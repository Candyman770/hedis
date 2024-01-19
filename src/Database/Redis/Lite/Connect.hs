{-# LANGUAGE TupleSections       #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE TypeApplications    #-}
{-# LANGUAGE RecordWildCards     #-}

module Database.Redis.Lite.Connect (
  connectCluster, connect, disconnect, runRedis
) where

import Prelude
import qualified Data.HashMap.Strict as HM
import qualified Data.List as L
import Control.Concurrent.MVar (MVar, newMVar, readMVar)
import Data.IORef (newIORef)
import Control.Exception (throwIO)
import Control.Monad (when, void)
import Control.Monad.Reader (runReaderT)
import Control.Monad.Extra (whenJust, mapMaybeM)
import Data.Maybe (fromMaybe)
import Data.Either.Extra (eitherToMaybe)
-- import System.IO (BufferMode(..), hGetBuffering, hSetBuffering)
import GHC.Conc (killThread, threadDelay)
import Control.Concurrent.Async (race)
import System.Clock (Clock(..), getTime)

import qualified Database.Redis.ConnectionContext as CC
import qualified Database.Redis.Cluster as CL
import qualified Database.Redis.Cluster.Command as CMD
import qualified Database.Redis.Lite.Cluster as Cluster
import qualified Database.Redis.Lite.NonCluster as NonCluster
import qualified Database.Redis.Connection as Conn
import qualified Database.Redis.ProtocolPipelining as PP
import qualified Database.Redis.Core as Core
import Database.Redis.Protocol (Reply(..))
import Database.Redis.Lite.Types

connectCluster :: ConnectInfo -> IO Connection
connectCluster connInfo = do
  conn <- Conn.createConnection connInfo
  slotsResponse <- Core.runRedisInternal conn Conn.clusterSlots
  (shardMapVar, shardMap) <- case slotsResponse of
    Left e -> throwIO $ Conn.ClusterConnectError e
    Right slots -> do
        shardMap <- Conn.shardMapFromClusterSlotsResponse slots
        (, shardMap) <$> (newMVar shardMap)
  commandInfos <- Core.runRedisInternal conn Conn.command
  case commandInfos of
    Left e -> throwIO $ Conn.ClusterConnectError e
    Right infos -> do
      let nodes = Cluster.getNodes shardMap
      nodeMap <- connectNodes connInfo nodes
      nodeMapState <- newMVar nodeMap
      clusterChannel <- Cluster.initiateClusterWorkers nodeMapState (\nodeId nMapState -> do
        shrdMap <- refreshShardMapAction connInfo shardMapVar
        refreshNodeMapAction shrdMap connInfo nodeId nMapState) (connectKeepAlive connInfo) (fromMaybe 1 $ requestTimeout connInfo)
      pure $ Cluster $ ClusterConnection clusterChannel nodeMapState shardMapVar (CMD.newInfoMap infos) connInfo

connect :: ConnectInfo -> IO Connection
connect connInfo = do
  conn <- connectInstance connInfo
  connState <- newMVar conn
  nonClusterChannel <-
    NonCluster.initiateWorkers connState (refreshConnectionAction connInfo) (fromMaybe 1 $ pipelineBatchSize connInfo) (connectKeepAlive connInfo) (fromMaybe 1 $ requestTimeout connInfo)
  pure $ NonCluster $ NonClusterConnection nonClusterChannel connState connInfo

connectInstance :: ConnectInfo -> IO NodeConnection
connectInstance connInfo = do
  pCtx <- Conn.createConnection connInfo
  ref <- newIORef Nothing
  -- case ctx of
  --   CC.NormalHandle h -> hSetBuffering h (BlockBuffering (Just 10240))
  --   _ -> pure ()
  when (connectReadOnly connInfo) $ do
    PP.beginReceiving pCtx
    void $ Core.runRedisInternal pCtx Conn.readOnly
  createdAt <- getTime Monotonic
  return (CL.NodeConnection (PP.toCtx pCtx) ref createdAt mempty)

connectNode :: ConnectInfo -> Node -> IO NodeConnection
connectNode connectInfo (Node n _ host port) = do
  (NodeConnection ctx ref timeSpec _) <- connectInstance $ connectInfo{connectHost = host, connectPort = CC.PortNumber $ toEnum port}
  pure $ NodeConnection ctx ref timeSpec n

connectNodes :: ConnectInfo -> [CL.Node] -> IO NodeMap
connectNodes connectInfo nodes = do
  nodeConns <- mapMaybeM (\node@(Node _ nodeRole _ _) ->
    case nodeRole of
      Master -> Just <$> connectNode connectInfo node
      Slave -> eitherToMaybe <$> (Cluster.tryWithUncaughtExc $ connectNode connectInfo node)
    ) nodes
  pure $ NodeMap $ foldl (\acc nodeConn@(NodeConnection _ _ _ nodeId) ->
    HM.insert nodeId nodeConn acc
    ) HM.empty nodeConns

disconnect :: Connection -> IO ()
disconnect connection = case connection of
  Cluster (ClusterConnection (RedisLiteChannel _ workers) nodeMapState _ _ _) -> do
    nodeMap <- CL.hasLocked $ readMVar nodeMapState
    Cluster.disconnectMap nodeMap
    ChanWorkers{..} <- readMVar workers
    killThread writer >> killThread reader >> killThread connRefresher >> killThread timeoutHandler
  NonCluster (NonClusterConnection (RedisLiteChannel _ workers) connState _) -> do
    conn <- CL.hasLocked $ readMVar connState
    Cluster.disconnect conn
    ChanWorkers{..} <- readMVar workers
    killThread writer >> killThread reader >> killThread connRefresher >> killThread timeoutHandler

refreshConnectionAction :: ConnectInfo -> Bool -> ConnectionState -> IO ()
refreshConnectionAction connectInfo closeOldConn connState =
  void $ Cluster.modifyMVarNoBlocking connState $ \oldConn -> do
    newConn <- connectInstance connectInfo
    when closeOldConn (void $ Cluster.tryWithUncaughtExc $ Cluster.disconnect oldConn)
    pure newConn

refreshNodeMapAction :: ShardMap -> ConnectInfo -> Maybe NodeID -> NodeMapState -> IO ()
refreshNodeMapAction shardMap connectInfo nodeIDM nodeMapVar = do
  void $ Cluster.modifyMVarNoBlocking nodeMapVar $ \_oldNodeMap@(NodeMap oldMap) -> do
    let nodes = Cluster.getNodes shardMap
    -- putStrLn $ "Nodes: " <> (show nodes)
    case nodeIDM of
      Nothing -> do
        -- refreshing entire nodeMap
        newNodeMap <- connectNodes connectInfo nodes
        -- void $ try @SomeException $ Cluster.disconnectMap oldNodeMap
        pure newNodeMap
      Just nodeId ->
        case (L.find (\(Node nId _ _ _) -> nId == nodeId) nodes) of
          Just node -> do
            -- refreshing single node
            nodeConn <- connectNode connectInfo node
            -- putStrLn "Before hClose"
            whenJust (HM.lookup nodeId oldMap) (void . Cluster.tryWithUncaughtExc . Cluster.disconnect)
            -- putStrLn "After hClose"
            pure $ NodeMap (HM.insert nodeId nodeConn oldMap)
          Nothing -> do
            -- node has been removed from cluster
            whenJust (HM.lookup nodeId oldMap) (void . Cluster.tryWithUncaughtExc . Cluster.disconnect)
            pure $ NodeMap (HM.delete nodeId oldMap)

refreshShardMapAction :: ConnectInfo -> MVar (ShardMap) -> IO CL.ShardMap
refreshShardMapAction connectInfo shardMapVar = do
  Cluster.modifyMVarNoBlocking shardMapVar $ \_ -> do
    conn <- Conn.createConnection connectInfo
    let timeout = fromMaybe 1000 $ (round . (* 1000000) <$> (requestTimeout connectInfo))
    raceResult <- race (threadDelay timeout) (Cluster.tryWithUncaughtExc $ Conn.refreshShardMapWithConn conn True) -- racing with delay of default 1 ms 
    res <- case raceResult of
      Left () ->
        throwIO $ CL.TimeoutException "ClusterSlots Timeout"
      Right eiShardMapResp ->
        case eiShardMapResp of
          Right shardMap -> pure shardMap 
          Left _ -> do
            throwIO $ Conn.ClusterConnectError (Error "Couldn't refresh shardMap due to connection error")
    PP.disconnect conn
    pure res

runRedis :: Connection -> Redis a -> IO a
runRedis connection redis =
  case connection of
    Cluster conn ->
      runReaderT redis (ClusteredEnv (ClusterEnv conn refreshShardMapAction refreshNodeMapAction))
    NonCluster conn ->
      runReaderT redis (NonClusteredEnv (NonClusterEnv conn refreshConnectionAction))