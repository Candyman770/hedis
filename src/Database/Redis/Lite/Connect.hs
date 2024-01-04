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
import Control.Concurrent.MVar (MVar, newMVar, readMVar)
import Data.IORef (newIORef)
import Control.Exception (SomeException, throwIO, try)
import Control.Monad (void)
import Control.Monad.Reader (runReaderT)
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
  -- putStrLn $ "CLUSTER CHAN CONNECT"
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
        let
          isConnectionReadOnly = connectReadOnly connInfo
          nodes = Cluster.getNodes shardMap
        nodeMap <- connectNodes connInfo nodes
        nodeMapState <- newMVar nodeMap
        clusterChannel <- Cluster.initiateClusterWorkers nodeMapState (nodeMapRefresher shardMapVar connInfo (connectNodes connInfo))
        pure $ Cluster $ ClusterConnection clusterChannel shardMapVar (CMD.newInfoMap infos) connInfo

    -- clusterConnect :: Bool -> IO CL.NodeConnection -> IO CL.NodeConnection
    -- clusterConnect readOnlyConnection connection = do
    --   nodeConn@(CL.NodeConnection ctx _ _) <- connection
    --   pCtx <- PP.fromCtx ctx
    --   when readOnlyConnection $ do
    --     PP.beginReceiving pCtx
    --     void $ Core.runRedisInternal pCtx readOnly
    --   return nodeConn

-- TODO changes to only refresh single faulty node connection
-- NodeMap Refresher for Buffer-Writer
nodeMapRefresher :: MVar CL.ShardMap -> ConnectInfo -> ([CL.Node] -> IO NodeMap) -> NodeMapState -> IO ()
nodeMapRefresher shardMapVar connectInfo getNodeConns nodeMapState = do
  void $ Cluster.modifyMVarNoBlocking nodeMapState $ \oldNodeMap -> do
    shardMap <- refreshShardMapAction connectInfo shardMapVar
    newNodeMap <- getNodeConns (Cluster.getNodes shardMap)
    void $ try @SomeException $ Cluster.disconnectMap oldNodeMap
    pure newNodeMap

connect :: ConnectInfo -> IO Connection
connect connInfo = do
  conn <- connectInstance connInfo
  connState <- newMVar conn
  nonClusterChannel <- NonCluster.initiateWorkers connState (refreshConnectionAction connInfo) (fromMaybe 1 $ pipelineBatchSize connInfo)
  pure $ NonCluster $ NonClusterConnection nonClusterChannel connInfo

disconnect :: Connection -> IO ()
disconnect connection = case connection of
  Cluster (ClusterConnection (ClusterChannel _ nodeMapState ChanWorkers{..}) _ _ _) -> do
    nodeMap <- CL.hasLocked $ readMVar nodeMapState
    Cluster.disconnectMap nodeMap
    killThread writer 
    killThread reader
  NonCluster (NonClusterConnection (NonClusterChannel _ connState ChanWorkers{..}) _) -> do
    conn <- CL.hasLocked $ readMVar connState
    Cluster.disconnect conn
    killThread writer 
    killThread reader

refreshConnectionAction :: ConnectInfo -> ConnectionState -> IO ()
refreshConnectionAction connectInfo connState =
  void $ Cluster.modifyMVarNoBlocking connState $ \_ ->
    connectInstance connectInfo

connectInstance :: ConnectInfo -> IO NodeConnection
connectInstance connInfo = do
  ctx <- PP.toCtx <$> Conn.createConnection connInfo
  ref <- newIORef Nothing
  -- case ctx of
  --   CC.NormalHandle h -> hSetBuffering h (BlockBuffering (Just 10240))
  --   _ -> pure ()
  return (CL.NodeConnection ctx ref mempty)

connectNode :: ConnectInfo -> Node -> IO NodeConnection
connectNode connectInfo (Node n _ host port) = do
  let connInfo = connectInfo{connectHost = host, connectPort = CC.PortNumber $ toEnum port}
  ctx <- PP.toCtx <$> Conn.createConnection connInfo
  ref <- newIORef Nothing
  -- case ctx of
  --   CC.NormalHandle h -> hSetBuffering h (BlockBuffering (Just 10240))
  --   _ -> pure ()
  return (CL.NodeConnection ctx ref n)

connectNodes :: ConnectInfo -> [CL.Node] -> IO NodeMap
connectNodes connectInfo nodes = do
  nodeConns <- mapM (connectNode connectInfo) nodes
  pure $ NodeMap $ foldl (\acc nodeConn@(NodeConnection _ _ nodeId) ->
    HM.insert nodeId nodeConn acc
    ) HM.empty nodeConns

-- TODO changes to only refresh single faulty node connection
-- NodeMap Refresher for User Threads
refreshNodeMapAction :: ShardMap -> ConnectInfo -> NodeMapState -> IO ()
refreshNodeMapAction shardMap connectInfo nodeMapVar = do
  void $ Cluster.modifyMVarNoBlocking nodeMapVar $ \oldNodeMap -> do
    newNodeMap <- connectNodes connectInfo (Cluster.getNodes shardMap)
    void $ try @SomeException $ Cluster.disconnectMap oldNodeMap
    pure newNodeMap

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