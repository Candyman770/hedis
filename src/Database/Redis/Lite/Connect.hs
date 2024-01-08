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
import Control.Exception (SomeException, throwIO, try)
import Control.Monad (when, void)
import Control.Monad.Reader (runReaderT)
import Control.Monad.Extra (whenJust)
import Data.Maybe (fromMaybe)
-- import System.IO (BufferMode(..), hGetBuffering, hSetBuffering)
import GHC.Conc (killThread, threadDelay)
import Control.Concurrent.Async (race)

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
        refreshNodeMapAction shrdMap connInfo nodeId nMapState) (connectKeepAlive connInfo)
      pure $ Cluster $ ClusterConnection clusterChannel nodeMapState shardMapVar (CMD.newInfoMap infos) connInfo

connect :: ConnectInfo -> IO Connection
connect connInfo = do
  conn <- connectInstance connInfo
  connState <- newMVar conn
  nonClusterChannel <- NonCluster.initiateWorkers connState (refreshConnectionAction connInfo) (fromMaybe 1 $ pipelineBatchSize connInfo) (connectKeepAlive connInfo)
  pure $ NonCluster $ NonClusterConnection nonClusterChannel connState connInfo

disconnect :: Connection -> IO ()
disconnect connection = case connection of
  Cluster (ClusterConnection (ClusterChannel _ ChanWorkers{..}) nodeMapState _ _ _) -> do
    nodeMap <- CL.hasLocked $ readMVar nodeMapState
    Cluster.disconnectMap nodeMap
    killThread writer >> killThread reader >> killThread connRefresher
  NonCluster (NonClusterConnection (NonClusterChannel _ ChanWorkers{..}) connState _) -> do
    conn <- CL.hasLocked $ readMVar connState
    Cluster.disconnect conn
    killThread writer >> killThread reader >> killThread connRefresher

refreshConnectionAction :: ConnectInfo -> ConnectionState -> IO ()
refreshConnectionAction connectInfo connState =
  void $ Cluster.modifyMVarNoBlocking connState $ \_ ->
    connectInstance connectInfo

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
  return (CL.NodeConnection (PP.toCtx pCtx) ref mempty)

connectNode :: ConnectInfo -> Node -> IO NodeConnection
connectNode connectInfo (Node n _ host port) = do
  (NodeConnection ctx ref _) <- connectInstance $ connectInfo{connectHost = host, connectPort = CC.PortNumber $ toEnum port}
  pure $ NodeConnection ctx ref n

connectNodes :: ConnectInfo -> [CL.Node] -> IO NodeMap
connectNodes connectInfo nodes = do
  nodeConns <- mapM (connectNode connectInfo) nodes
  pure $ NodeMap $ foldl (\acc nodeConn@(NodeConnection _ _ nodeId) ->
    HM.insert nodeId nodeConn acc
    ) HM.empty nodeConns

refreshNodeMapAction :: ShardMap -> ConnectInfo -> Maybe NodeID -> NodeMapState -> IO ()
refreshNodeMapAction shardMap connectInfo nodeIDM nodeMapVar = do
  void $ Cluster.modifyMVarNoBlocking nodeMapVar $ \oldNodeMap@(NodeMap oldMap) -> do
    let nodes = Cluster.getNodes shardMap
    case nodeIDM of
      Nothing -> do
        -- refreshing entire nodeMap
        newNodeMap <- connectNodes connectInfo nodes
        void $ try @SomeException $ Cluster.disconnectMap oldNodeMap
        pure newNodeMap
      Just nodeId ->
        case (L.find (\(Node nId _ _ _) -> nId == nodeId) nodes) of
          Just node -> do
            -- refreshing single node
            nodeConn <- connectNode connectInfo node
            whenJust (HM.lookup nodeId oldMap) (void . try @SomeException . Cluster.disconnect)
            pure $ NodeMap (HM.insert nodeId nodeConn oldMap)
          Nothing -> do
            -- node has been removed from cluster
            whenJust (HM.lookup nodeId oldMap) (void . try @SomeException . Cluster.disconnect)
            pure $ NodeMap (HM.delete nodeId oldMap)

refreshShardMapAction :: ConnectInfo -> MVar (ShardMap) -> IO CL.ShardMap
refreshShardMapAction connectInfo shardMapVar = do
  Cluster.modifyMVarNoBlocking shardMapVar $ \_ -> do
    conn <- Conn.createConnection connectInfo
    let timeout = fromMaybe 1000 $ (round . (* 1000000) <$> (requestTimeout connectInfo))
    raceResult <- race (threadDelay timeout) (try $ Conn.refreshShardMapWithConn conn True) -- racing with delay of default 1 ms 
    case raceResult of
      Left () ->
        throwIO $ CL.TimeoutException "ClusterSlots Timeout"
      Right eiShardMapResp ->
        case eiShardMapResp of
          Right shardMap -> pure shardMap 
          Left (_ :: SomeException) -> do
            throwIO $ Conn.ClusterConnectError (Error "Couldn't refresh shardMap due to connection error")

runRedis :: Connection -> Redis a -> IO a
runRedis connection redis =
  case connection of
    Cluster conn ->
      runReaderT redis (ClusteredEnv (ClusterEnv conn refreshShardMapAction refreshNodeMapAction))
    NonCluster conn ->
      runReaderT redis (NonClusteredEnv (NonClusterEnv conn refreshConnectionAction))