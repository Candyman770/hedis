{-# LANGUAGE TupleSections #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Database.Redis.Connection where

import Control.Exception
import qualified Control.Monad.Catch as Catch
import Control.Monad.IO.Class(liftIO, MonadIO)
import Control.Monad(when)
import Control.Concurrent.MVar(MVar, newMVar)
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as Char8
import Data.Functor(void)
import qualified Data.IntMap.Strict as IntMap
import Data.Pool(Pool, withResource, createPool, destroyAllResources)
import Data.Typeable
import qualified Data.HashMap.Strict as HM
import System.Random (randomRIO)
import System.Environment (lookupEnv)
import Data.Maybe (fromMaybe)
import Text.Read (readMaybe)

import qualified Database.Redis.ProtocolPipelining as PP
import Database.Redis.Core(Redis, RedisCtx(..), sendRequest, runRedisInternal, runRedisClusteredInternal)
import Database.Redis.Protocol(Reply(..))
import Database.Redis.Cluster(ShardMap(..), Node, Shard(..))
import Database.Redis.Types (Status, encode)
import qualified Database.Redis.Cluster as Cluster
import qualified Database.Redis.Cluster.Command as CMD
import qualified Database.Redis.ConnectionContext as CC
import Database.Redis.Lite.Types (ConnectInfo(..))
import           Control.Concurrent (threadDelay)
import           Control.Concurrent.Async (race)
--import qualified Database.Redis.Cluster.Pipeline as ClusterPipeline

import Database.Redis.Commands
    ( ClusterSlotsResponse(..)
    , ClusterSlotsResponseEntry(..)
    , ClusterSlotsNode(..))

--------------------------------------------------------------------------------
-- Connection
--

-- |A threadsafe pool of network connections to a Redis server. Use the
--  'connect' function to create one.
data Connection
    = NonClusteredConnection (Pool PP.Connection)
    | ClusteredConnection (MVar ShardMap) (Pool Cluster.Connection)

-- |Information for connnecting to a Redis server.
--
-- It is recommended to not use the 'ConnInfo' data constructor directly.
-- Instead use 'defaultConnectInfo' and update it with record syntax. For
-- example to connect to a password protected Redis server running on localhost
-- and listening to the default port:
--
-- @
-- myConnectInfo :: ConnectInfo
-- myConnectInfo = defaultConnectInfo {connectAuth = Just \"secret\"}
-- @
--

data ConnectError = ConnectAuthError Reply
                  | ConnectSelectError Reply
    deriving (Eq, Show, Typeable)

instance Exception ConnectError

-- |Default information for connecting:
--
-- @
--  connectHost           = \"localhost\"
--  connectPort           = PortNumber 6379 -- Redis default port
--  connectAuth           = Nothing         -- No password
--  connectDatabase       = 0               -- SELECT database 0
--  connectMaxConnections = 50              -- Up to 50 connections
--  connectMaxIdleTime    = 30              -- Keep open for 30 seconds
--  connectTimeout        = Nothing         -- Don't add timeout logic
--  connectTLSParams      = Nothing         -- Do not use TLS
-- @
--
defaultConnectInfo :: ConnectInfo
defaultConnectInfo = ConnInfo
    { connectHost           = "localhost"
    , connectPort           = CC.PortNumber 6379
    , connectAuth           = Nothing
    , connectReadOnly       = False
    , connectDatabase       = 0
    , connectMaxConnections = 50
    , connectMaxIdleTime    = 30
    , connectTimeout        = Nothing
    , connectTLSParams      = Nothing
    , requestTimeout        = Nothing
    , pipelineBatchSize     = Nothing
    , connectKeepAlive      = 60
    }

auth
    :: RedisCtx m f
    => B.ByteString -- ^ password
    -> m (f Status)
auth password = sendRequest ["AUTH", password]

-- the select command. used in 'connect'.
select
    :: RedisCtx m f
    => Integer -- ^ index
    -> m (f Status)
select ix = sendRequest ["SELECT", encode ix]

-- the ping command. used in 'checkedconnect'.
ping
    :: (RedisCtx m f)
    => m (f Status)
ping  = sendRequest (["PING"] )

command :: (RedisCtx m f) => m (f [CMD.CommandInfo])
command = sendRequest ["COMMAND"]

readOnly :: (RedisCtx m f) => m (f Status)
readOnly = sendRequest ["READONLY"]

clusterSlots
    :: (RedisCtx m f)
    => m (f ClusterSlotsResponse)
clusterSlots = sendRequest $ ["CLUSTER", "SLOTS"]

createConnection :: ConnectInfo -> IO PP.Connection
createConnection ConnInfo{..} = do
    let timeoutOptUs =
          round . (1000000 *) <$> connectTimeout
    conn <- PP.connect connectHost connectPort timeoutOptUs
    conn' <- case connectTLSParams of
               Nothing -> return conn
               Just tlsParams -> PP.enableTLS tlsParams conn
    PP.beginReceiving conn'

    runRedisInternal conn' $ do
        -- AUTH
        case connectAuth of
            Nothing   -> return ()
            Just pass -> do
              resp <- auth pass
              case resp of
                Left r -> liftIO $ throwIO $ ConnectAuthError r
                _      -> return ()
        -- SELECT
        when (connectDatabase /= 0) $ do
          resp <- select connectDatabase
          case resp of
              Left r -> liftIO $ throwIO $ ConnectSelectError r
              _      -> return ()
    return conn'

-- |Constructs a 'Connection' pool to a Redis server designated by the
--  given 'ConnectInfo'. The first connection is not actually established
--  until the first call to the server.
connect :: ConnectInfo -> IO Connection
connect cInfo@ConnInfo{..} = NonClusteredConnection <$>
    createPool (createConnection cInfo) PP.disconnect 1 connectMaxIdleTime connectMaxConnections

-- |Constructs a 'Connection' pool to a Redis server designated by the
--  given 'ConnectInfo', then tests if the server is actually there.
--  Throws an exception if the connection to the Redis server can't be
--  established.
checkedConnect :: ConnectInfo -> IO Connection
checkedConnect connInfo = do
    conn <- connect connInfo
    runRedis conn $ void ping
    return conn

-- |Destroy all idle resources in the pool.
disconnect :: Connection -> IO ()
disconnect (NonClusteredConnection pool) = destroyAllResources pool
disconnect (ClusteredConnection _ pool) = destroyAllResources pool

-- | Memory bracket around 'connect' and 'disconnect'.
withConnect :: (Catch.MonadMask m, MonadIO m) => ConnectInfo -> (Connection -> m c) -> m c
withConnect connInfo = Catch.bracket (liftIO $ connect connInfo) (liftIO . disconnect)

-- | Memory bracket around 'checkedConnect' and 'disconnect'
withCheckedConnect :: ConnectInfo -> (Connection -> IO c) -> IO c
withCheckedConnect connInfo = bracket (checkedConnect connInfo) disconnect

-- |Interact with a Redis datastore specified by the given 'Connection'.
--
--  Each call of 'runRedis' takes a network connection from the 'Connection'
--  pool and runs the given 'Redis' action. Calls to 'runRedis' may thus block
--  while all connections from the pool are in use.
runRedis :: Connection -> Redis a -> IO a
runRedis (NonClusteredConnection pool) redis =
  withResource pool $ \conn -> runRedisInternal conn redis
runRedis (ClusteredConnection _ pool) redis =
    withResource pool $ \conn -> runRedisClusteredInternal conn (refreshShardMap conn) redis

newtype ClusterConnectError = ClusterConnectError Reply
    deriving (Eq, Show, Typeable)

instance Exception ClusterConnectError

-- |Constructs a 'ShardMap' of connections to clustered nodes. The argument is
-- a 'ConnectInfo' for any node in the cluster
--
-- Some Redis commands are currently not supported in cluster mode
-- - CONFIG, AUTH
-- - SCAN
-- - MOVE, SELECT
-- - PUBLISH, SUBSCRIBE, PSUBSCRIBE, UNSUBSCRIBE, PUNSUBSCRIBE, RESET
connectCluster :: ConnectInfo -> IO Connection
connectCluster bootstrapConnInfo = do
    let timeoutOptUs =
          round . (1000000 *) <$> connectTimeout bootstrapConnInfo
    conn <- createConnection bootstrapConnInfo
    slotsResponse <- runRedisInternal conn clusterSlots
    shardMapVar <- case slotsResponse of
        Left e -> throwIO $ ClusterConnectError e
        Right slots -> do
            shardMap <- shardMapFromClusterSlotsResponse slots
            newMVar shardMap
    commandInfos <- runRedisInternal conn command
    case commandInfos of
        Left e -> throwIO $ ClusterConnectError e
        Right infos -> do
            let
                isConnectionReadOnly = connectReadOnly bootstrapConnInfo
                clusterConnection = Cluster.connect withAuth infos shardMapVar timeoutOptUs isConnectionReadOnly refreshShardMapWithNodeConn
            pool <- createPool (clusterConnect isConnectionReadOnly clusterConnection) Cluster.disconnect 1 (connectMaxIdleTime bootstrapConnInfo) (connectMaxConnections bootstrapConnInfo)
            return $ ClusteredConnection shardMapVar pool
    where
      withAuth host port timeout = do
        conn <- PP.connect host port timeout
        conn' <- case connectTLSParams bootstrapConnInfo of
                  Nothing -> return conn
                  Just tlsParams -> PP.enableTLS tlsParams conn
        PP.beginReceiving conn'

        runRedisInternal conn' $ do
            -- AUTH
            case connectAuth bootstrapConnInfo of
                Nothing   -> return ()
                Just pass -> do
                  resp <- auth pass
                  case resp of
                    Left r -> liftIO $ throwIO $ ConnectAuthError r
                    _      -> return ()
        return $ PP.toCtx conn'

      clusterConnect :: Bool -> IO Cluster.Connection -> IO Cluster.Connection
      clusterConnect readOnlyConnection connection = do
          clusterConn@(Cluster.Connection nodeMap _ _ _ _) <- connection
          nodesConns <-  sequence $ ( PP.fromCtx . (\(Cluster.NodeConnection ctx _ _ _) -> ctx ) . snd) <$> (HM.toList nodeMap)
          when readOnlyConnection $
                  mapM_ (\conn -> do
                          PP.beginReceiving conn
                          runRedisInternal conn readOnly
                      ) nodesConns
          return clusterConn

shardMapFromClusterSlotsResponse :: ClusterSlotsResponse -> IO ShardMap
shardMapFromClusterSlotsResponse ClusterSlotsResponse{..} = ShardMap <$> foldr mkShardMap (pure IntMap.empty)  clusterSlotsResponseEntries where
    mkShardMap :: ClusterSlotsResponseEntry -> IO (IntMap.IntMap Shard) -> IO (IntMap.IntMap Shard)
    mkShardMap ClusterSlotsResponseEntry{..} accumulator = do
        accumulated <- accumulator
        let master = nodeFromClusterSlotNode True clusterSlotsResponseEntryMaster
        -- let replicas = map (nodeFromClusterSlotNode False) clusterSlotsResponseEntryReplicas
        let shard = Shard master []
        let slotMap = IntMap.fromList $ map (, shard) [clusterSlotsResponseEntryStartSlot..clusterSlotsResponseEntryEndSlot]
        return $ IntMap.union slotMap accumulated
    nodeFromClusterSlotNode :: Bool -> ClusterSlotsNode -> Node
    nodeFromClusterSlotNode isMaster ClusterSlotsNode{..} =
        let hostname = Char8.unpack clusterSlotsNodeIP
            role = if isMaster then Cluster.Master else Cluster.Slave
        in
            Cluster.Node clusterSlotsNodeID role hostname (toEnum clusterSlotsNodePort)

refreshShardMap :: Cluster.Connection -> IO ShardMap
refreshShardMap (Cluster.Connection nodeConns _ _ _ _) =
    refreshShardMapWithNodeConn (HM.elems nodeConns)

refreshShardMapWithNodeConn :: [Cluster.NodeConnection] -> IO ShardMap
refreshShardMapWithNodeConn [] = throwIO $ ClusterConnectError (Error "Couldn't refresh shardMap due to connection error")
refreshShardMapWithNodeConn nodeConnsList = do
    selectedIdx <- randomRIO (0, (length nodeConnsList) - 1)
    let (Cluster.NodeConnection ctx _ _ _) = nodeConnsList !! selectedIdx
    pipelineConn <- PP.fromCtx ctx
    envTimeout <- fromMaybe (10 ^ (3 :: Int)) . (>>= readMaybe) <$> lookupEnv "REDIS_CLUSTER_SLOTS_TIMEOUT"
    raceResult <- race (threadDelay envTimeout) (try $ refreshShardMapWithConn pipelineConn True) -- racing with delay of default 1 ms 
    case raceResult of
        Left () -> do
            print $ "TimeoutForConnection " <> show ctx 
            throwIO $ Cluster.TimeoutException "ClusterSlots Timeout"
        Right eiShardMapResp -> 
            case eiShardMapResp of
                Right shardMap -> pure shardMap 
                Left (err :: SomeException) -> do
                    print $ "ShardMapRefreshError-" <> show err 
                    throwIO $ ClusterConnectError (Error "Couldn't refresh shardMap due to connection error")

refreshShardMapWithConn :: PP.Connection -> Bool -> IO ShardMap
refreshShardMapWithConn pipelineConn _ = do
    _ <- PP.beginReceiving pipelineConn
    slotsResponse <- runRedisInternal pipelineConn clusterSlots
    case slotsResponse of
        Left e -> throwIO $ ClusterConnectError e
        Right slots -> case clusterSlotsResponseEntries slots of 
            [] -> throwIO $ ClusterConnectError $ SingleLine "empty slotsResponse"
            _ -> shardMapFromClusterSlotsResponse slots
