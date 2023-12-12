{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings #-}

module Database.Redis.ClusterChan.ClusterChanConnect where

import Prelude
import qualified Data.HashMap.Strict as HM
import qualified Data.Pool as DP
import qualified Data.Time as Time
import Control.Concurrent.MVar (MVar, newMVar, readMVar, tryTakeMVar, putMVar)
import Data.IORef (newIORef)
import Control.Exception (throwIO)
import Control.Monad (void, replicateM)
import Control.Monad.Reader (liftIO)
import Data.Maybe (fromMaybe)
import System.IO (BufferMode(..), hGetBuffering, hSetBuffering)

import qualified Database.Redis.ConnectionContext as CC
import qualified Database.Redis.Cluster as CL
import qualified Database.Redis.Cluster.Command as CMD
import qualified Database.Redis.ClusterChan.ClusterChan as CLChan
import qualified Database.Redis.Connection as Conn
import qualified Database.Redis.ProtocolPipelining as PP
import qualified Database.Redis.Core as Core
import Database.Redis.Commands (clusterSlots, command, auth, readOnly)

connectClusterChan :: Conn.ConnectInfo -> CLChan.ConnectChanInfo -> IO CLChan.ChanConnection
connectClusterChan bootstrapConnInfo connChanInfo = do
  -- putStrLn $ "CLUSTER CHAN CONNECT"
  let timeoutOptUs =
        round . (1000000 *) <$> Conn.connectTimeout bootstrapConnInfo
  connectionForShardMap <- DP.createPool (Conn.createConnection bootstrapConnInfo) (CC.disconnect . PP.toCtx) 1 (Conn.connectMaxIdleTime bootstrapConnInfo) 1
  slotsResponse <- DP.withResource connectionForShardMap $ \conn -> Core.runRedisInternal conn clusterSlots
  (shardMapVar, shardMap) <- case slotsResponse of
      Left e -> throwIO $ Conn.ClusterConnectError e
      Right slots -> do
          shardMap <- Conn.shardMapFromClusterSlotsResponse slots
          (, shardMap) <$> (newMVar shardMap)
  commandInfos <- DP.withResource connectionForShardMap $ \conn -> Core.runRedisInternal conn command
  case commandInfos of
      Left e -> throwIO $ Conn.ClusterConnectError e
      Right infos -> do
        let
          isConnectionReadOnly = Conn.connectReadOnly bootstrapConnInfo
          nodes = CLChan.getNodes shardMap
        nodeMapVars <- replicateM (CLChan.channelCount connChanInfo) (do
          pool <- getNodeMapPool timeoutOptUs (Conn.connectMaxIdleTime bootstrapConnInfo) nodes
          DP.withResource pool $ \_ -> pure ()
          newMVar pool
          )
        let reqTimeout = fromMaybe 500000 (round . (1000000 *) <$> Conn.requestTimeout bootstrapConnInfo)
        clusterChannels <- CLChan.createClusterChannels nodeMapVars (refreshNodeMap shardMapVar connectionForShardMap (Conn.requestTimeout bootstrapConnInfo) (getNodeMapPool timeoutOptUs (Conn.connectMaxIdleTime bootstrapConnInfo))) reqTimeout
        -- clusterChannelMap <- do
        --   nodeMap@(CLChan.NodeMap nMp) <- connectNodes withAuth timeoutOptUs nodes
        --   mapM_ (\(CL.NodeConnection ctx _ _) ->
        --     case ctx of
        --       CC.NormalHandle h -> hSetBuffering h (BlockBuffering (Just 10240))
        --       _ -> pure ()
        --     ) (HM.elems nMp)
        --   CLChan.createClusterChannels nodeMap (refreshNodeMap shardMapVar connectionForShardMap (Conn.requestTimeout bootstrapConnInfo) (getNodeMapPool timeoutOptUs (Conn.connectMaxIdleTime bootstrapConnInfo))) (CLChan.pipelineBatchSize connChanInfo) reqTimeout
        pure $ CLChan.ChanConnection clusterChannels shardMapVar connectionForShardMap (CMD.newInfoMap infos) bootstrapConnInfo
  where
    withAuth host port timeout = do
      conn <- PP.connect host port timeout
      conn' <- case Conn.connectTLSParams bootstrapConnInfo of
                Nothing -> return conn
                Just tlsParams -> PP.enableTLS tlsParams conn
      PP.beginReceiving conn'

      Core.runRedisInternal conn' $ do
          -- AUTH
        case Conn.connectAuth bootstrapConnInfo of
          Nothing   -> return ()
          Just pass -> do
            resp <- auth pass
            case resp of
              Left r -> liftIO $ throwIO $ Conn.ConnectAuthError r
              _      -> return ()
      return $ PP.toCtx conn'

    getNodeMapPool timeoutOptUs idleTimeout nodes =
      DP.createPool (connectNodes withAuth timeoutOptUs nodes) CLChan.disconnectMap 1 idleTimeout 1

    -- clusterConnect :: Bool -> IO CL.NodeConnection -> IO CL.NodeConnection
    -- clusterConnect readOnlyConnection connection = do
    --   nodeConn@(CL.NodeConnection ctx _ _) <- connection
    --   pCtx <- PP.fromCtx ctx
    --   when readOnlyConnection $ do
    --     PP.beginReceiving pCtx
    --     void $ Core.runRedisInternal pCtx readOnly
    --   return nodeConn

connectNode :: (String -> CC.PortID -> Maybe Int -> IO CC.ConnectionContext) -> Maybe Int -> CL.Node -> IO CL.NodeConnection
connectNode withAuth timeoutOpt (CL.Node n _ host port) = do
  ctx <- withAuth host (CC.PortNumber $ toEnum port) timeoutOpt
  ref <- newIORef Nothing
  -- case ctx of
  --   CC.NormalHandle h -> hSetBuffering h (BlockBuffering Nothing)
  --   _ -> pure ()
  return (CL.NodeConnection ctx ref n)

connectNodes :: (String -> CC.PortID -> Maybe Int -> IO CC.ConnectionContext) -> Maybe Int -> [CL.Node] -> IO CLChan.NodeMap
connectNodes withAuth timeoutOpt nodes = do
  nodeConns <- mapM (connectNode withAuth timeoutOpt) nodes
  pure $ CLChan.NodeMap $ foldl (\acc nodeConn@(CL.NodeConnection _ _ nodeId) ->
    HM.insert nodeId nodeConn acc
    ) HM.empty nodeConns

refreshNodeMap :: MVar CL.ShardMap -> DP.Pool PP.Connection -> Maybe Time.NominalDiffTime -> ([CL.Node] -> IO (DP.Pool CLChan.NodeMap)) -> CLChan.NodeMapMVar -> IO ()
refreshNodeMap shardMapVar connP timeoutOpts getNodeConns nodeMapVar = do
  void $ CLChan.modifyMVarNoBlocking nodeMapVar $ \_ -> do
    shardMap <- CLChan.refreshShardMap connP shardMapVar timeoutOpts
    connPool <- getNodeConns (CLChan.getNodes shardMap)
    DP.withResource connPool $ \_ -> pure ()
    pure connPool
    -- DP.destroyAllResources nodeMapPOld