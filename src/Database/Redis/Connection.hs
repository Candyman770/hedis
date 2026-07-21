{-# LANGUAGE TupleSections #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE NamedFieldPuns #-}
module Database.Redis.Connection where

import Control.Exception
import qualified Control.Monad.Catch as Catch
import Control.Monad.IO.Class(liftIO, MonadIO)
import Control.Monad(when,foldM)

import Control.Concurrent.MVar(modifyMVar, readMVar)
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as Char8
import Data.Functor(void)
import qualified Data.IntMap.Strict as IntMap
import Data.Pool(Pool, withResource, createPool, destroyAllResources)
import Data.Typeable
import Data.List (nub)
import qualified Data.Time as Time
import Network.TLS (ClientParams)
import qualified Network.Socket as NS
import qualified Data.HashMap.Strict as HM
import System.Random (randomRIO)
import System.Environment (lookupEnv)
import Data.Maybe (fromMaybe, listToMaybe, mapMaybe)
import Text.Read (readMaybe)

import qualified Database.Redis.ProtocolPipelining as PP
import Database.Redis.Core(Redis, runRedisInternal, runRedisClusteredInternal, sendRequest)
import Database.Redis.Protocol(Reply(..))
import Database.Redis.Cluster(ShardMap(..), Node(..), Shard(..), NodeConnectionMap)
import qualified Database.Redis.Cluster as Cluster
import qualified Database.Redis.ConnectionContext as CC
import qualified System.Timeout as T
import Data.IORef(IORef, readIORef)
import Database.Redis.Commands
    ( ping
    , select
    , auth
    , clusterSlots
    , command
    , readOnly
    , ClusterSlotsResponse(..)
    , ClusterSlotsResponseEntry(..)
    , ClusterSlotsNode(..))

--------------------------------------------------------------------------------
-- Connection
--

-- |A threadsafe pool of network connections to a Redis server. Use the
--  'connect' function to create one.
data Connection
    = NonClusteredConnection (Pool PP.Connection)
    | ClusteredConnection ConnectInfo Cluster.Connection

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
data ConnectInfo = ConnInfo
    { connectHost           :: NS.HostName
    , connectPort           :: CC.PortID
    , connectAuth           :: Maybe ConnectAuth
    , connectReadOnly       :: Bool
    -- ^ When the server is protected by a password, set 'connectAuth' to 'Just'
    --   the password. Each connection will then authenticate by the 'auth'
    --   command.
    , connectDatabase       :: Integer
    -- ^ Each connection will 'select' the database with the given index.
    , connectMaxConnections :: Int
    -- ^ Maximum number of connections to keep open. The smallest acceptable
    --   value is 1.
    , connectMaxIdleTime    :: Time.NominalDiffTime
    -- ^ Amount of time for which an unused connection is kept open. The
    --   smallest acceptable value is 0.5 seconds. If the @timeout@ value in
    --   your redis.conf file is non-zero, it should be larger than
    --   'connectMaxIdleTime'.
    , connectTimeout        :: Maybe Time.NominalDiffTime
    -- ^ Optional timeout until connection to Redis gets
    --   established. 'ConnectTimeoutException' gets thrown if no socket
    --   get connected in this interval of time.
    , connectTLSParams      :: Maybe ClientParams
    -- ^ Optional TLS parameters. TLS will be enabled if this is provided.
    , requestTimeout        :: Maybe Double
    -- ^ timeout for a redis command request in seconds example: 0.5 seconds (500 milliseconds)
    -- post requestTimeout, TimeoutException will be thrown. This is now only applicable to cluster redis.
    -- TODO add for non cluster redis also
    , tryAgainDelay         :: Maybe Double
    -- ^ retry delay for a redis command request when TRYAGAIN error is received during cluster slot migration
    -- default value is 100 ms
    -- 
    } deriving Show

data ConnectAuth = Static B.ByteString | Dynamic ShowableIORefByteString
        deriving Show

newtype ShowableIORefByteString = ShowableIORefByteString (IORef B.ByteString)

instance Show ShowableIORefByteString where
    show _ = "ioref bytestring" :: String

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
    , tryAgainDelay         = Nothing
    }

defaultClusterConnectInfo :: ConnectInfo
defaultClusterConnectInfo = ConnInfo
    { connectHost           = "localhost"
    , connectPort           = CC.PortNumber 30001
    , connectAuth           = Nothing
    , connectReadOnly       = False
    , connectDatabase       = 0
    , connectMaxConnections = 50
    , connectMaxIdleTime    = 30
    , connectTimeout        = Nothing
    , connectTLSParams      = Nothing
    , requestTimeout        = Nothing
    , tryAgainDelay         = Nothing
    }

createConnection :: ConnectInfo -> IO PP.Connection
createConnection ConnInfo{..} = do
    let timeoutOptUs =
          round . (1000000 *) <$> connectTimeout
    conn <- PP.connect connectHost connectPort timeoutOptUs
    conn' <- case connectTLSParams of
               Nothing -> return conn
               Just tlsParams -> PP.enableTLS tlsParams conn
    PP.beginReceiving conn'
    connectAuth' <- getConnectionAuthByteString connectAuth

    runRedisInternal conn' $ do
        -- AUTH
        case connectAuth' of
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
disconnect (ClusteredConnection _ conn) = Cluster.destroyNodeResources conn

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
runRedis (ClusteredConnection bootstrapConnInfo conn) redis =
    runRedisClusteredInternal conn (refreshShardMap bootstrapConnInfo conn) redis

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
connectCluster bootstrapConnInfo@ConnInfo{connectMaxConnections,connectMaxIdleTime,requestTimeout,tryAgainDelay} = do
    conn <- createConnection bootstrapConnInfo
    epochReply <- runRedisInternal conn (sendRequest ["CLUSTER", "INFO"])
    let epoch = either (const 0) parseClusterEpoch epochReply
    slotsResponse <- runRedisInternal conn clusterSlots
    shardMap <- case slotsResponse of
        Left e -> throwIO $ ClusterConnectError e
        Right slots -> shardMapFromClusterSlotsResponse epoch slots
    commandInfos <- runRedisInternal conn command
    case commandInfos of
        Left e -> throwIO $ ClusterConnectError e
        Right infos -> do
            let withAuth = connectWithAuth bootstrapConnInfo
            clusterConnection <- Cluster.createClusterConnectionPools withAuth connectMaxConnections connectMaxIdleTime infos shardMap requestTimeout tryAgainDelay
            return $ ClusteredConnection bootstrapConnInfo clusterConnection

connectWithAuth :: ConnectInfo -> Cluster.Host -> CC.PortID -> IO CC.ConnectionContext
connectWithAuth ConnInfo{connectTLSParams,connectAuth,connectReadOnly,connectTimeout} host port = do
    conn <- PP.connect host port $ clusterConnectTimeoutinUs <$> connectTimeout
    conn' <- case connectTLSParams of
                Nothing -> return conn
                Just tlsParams -> PP.enableTLS tlsParams conn
    PP.beginReceiving conn'
    connectAuth' <- getConnectionAuthByteString connectAuth
    runRedisInternal conn' $ do
        -- AUTH
        case connectAuth' of
            Nothing   -> return ()
            Just pass -> do
                resp <- auth pass
                case resp of
                    Left r -> liftIO $ throwIO $ ConnectAuthError r
                    _      -> return ()
    when connectReadOnly $ do
        runRedisInternal conn' readOnly >> return()
    return $ PP.toCtx conn'

clusterConnectTimeoutinUs :: Time.NominalDiffTime -> Int
clusterConnectTimeoutinUs = round . (1000000 *) 

shardMapFromClusterSlotsResponse :: Int -> ClusterSlotsResponse -> IO ShardMap
shardMapFromClusterSlotsResponse epoch ClusterSlotsResponse{..} = do
    slots <- foldr mkShardMap (pure IntMap.empty) clusterSlotsResponseEntries
    return ShardMap { shardMapSlots = slots, shardMapEpoch = epoch }
  where
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

-- Parse cluster_current_epoch from the CLUSTER INFO bulk-string response.
-- CLUSTER INFO returns lines like "cluster_current_epoch:6\r\n".
parseClusterEpoch :: Reply -> Int
parseClusterEpoch (Bulk (Just bs)) =
    let ls = Char8.lines bs
        prefix = "cluster_current_epoch:"
        found = listToMaybe $ mapMaybe (fmap Char8.unpack . Char8.stripPrefix prefix) ls
    in case found >>= readMaybe of
        Just n  -> n
        Nothing -> 0
parseClusterEpoch _ = 0

refreshShardMap :: ConnectInfo -> Cluster.Connection -> Maybe Cluster.NodeConnection -> IO (ShardMap, NodeConnectionMap)
refreshShardMap connectInfo@ConnInfo{connectMaxConnections,connectMaxIdleTime} (Cluster.Connection shardNodeVar _ _) nodeConn = do
    -- Do the blocking CLUSTER INFO + CLUSTER SLOTS round-trips OUTSIDE the MVar.
    -- We only take a snapshot of the existing node connections (readMVar does not
    -- hold the lock) to reach a node, so routing threads that merely readMVar the
    -- shardNodeVar are not blocked for the duration of the network fetch.
    (_, nodeConnSnapshot) <- readMVar shardNodeVar
    newShardMap <- refreshShardMapWithNodeConn nodeConn (HM.elems nodeConnSnapshot)
    -- The critical section below is now non-blocking: compare epochs against the
    -- freshest current ShardMap and swap. updateNodeConnections only allocates
    -- resource-pool structures (createPool opens sockets lazily, on demand), so it
    -- holds the lock for negligible time.
    modifyMVar shardNodeVar $ \(currentShardMap, oldNodeConnMap) -> do
        -- Epoch guard: Redis cluster gossip is eventually consistent.
        -- A CLUSTER SLOTS response from a gossip-lagged node carries a lower
        -- cluster_current_epoch. Writing it back would overwrite a correct
        -- post-failover ShardMap with a stale one.
        -- We skip the guard when either epoch is 0 (parse failed / old node)
        -- so that a missing CLUSTER INFO never permanently blocks refreshes.
        let isStale = shardMapEpoch currentShardMap > 0
                   && shardMapEpoch newShardMap    > 0
                   && shardMapEpoch newShardMap    < shardMapEpoch currentShardMap
        if isStale
            then return ((currentShardMap, oldNodeConnMap), (currentShardMap, oldNodeConnMap))
            else do
                newNodeConnMap <- updateNodeConnections newShardMap oldNodeConnMap
                return ((newShardMap, newNodeConnMap), (newShardMap, newNodeConnMap))
    where
        withAuth :: Cluster.Host -> CC.PortID -> IO CC.ConnectionContext
        withAuth = connectWithAuth connectInfo
        updateNodeConnections :: ShardMap -> HM.HashMap Cluster.NodeID Cluster.NodeConnection -> IO (HM.HashMap Cluster.NodeID Cluster.NodeConnection)
        updateNodeConnections newShardMap oldNodeConnMap = do
            foldM (\acc node@(Cluster.Node nodeid _ _ _) -> 
                case HM.lookup nodeid oldNodeConnMap of
                    Just nodeconn -> return $ HM.insert nodeid nodeconn acc
                    Nothing       -> do
                        (_,nodeConnPool) <- Cluster.createNodePool withAuth connectMaxConnections connectMaxIdleTime node
                        return $ HM.insert nodeid nodeConnPool acc
                 ) HM.empty (nub $ Cluster.nodes newShardMap)

refreshShardMapWithNodeConn :: Maybe Cluster.NodeConnection -> [Cluster.NodeConnection] -> IO ShardMap
refreshShardMapWithNodeConn _ [] = throwIO $ ClusterConnectError (Error "Couldn't refresh shardMap due to connection error")
refreshShardMapWithNodeConn maybeNodeConn nodeConnsList = do
    let numOfNodes = length nodeConnsList
    selectedIdx <- randomRIO (0, length nodeConnsList - 1)
    let (Cluster.NodeConnection pool _) = fromMaybe (nodeConnsList !! selectedIdx) maybeNodeConn
    eresp <- try $ refreshShardMapWithPool pool
    case eresp of 
        Left  (_::SomeException) ->  do                 -- retry on other node
            let otherSelectedIdx                        = (selectedIdx + 1) `mod` numOfNodes
                (Cluster.NodeConnection otherPool _)    = maybe (nodeConnsList !! otherSelectedIdx) 
                                                                (\nc -> if nc /= nodeConnsList !! selectedIdx then nodeConnsList !! selectedIdx else nodeConnsList !! otherSelectedIdx) 
                                                                (maybeNodeConn)
            refreshShardMapWithPool otherPool
        Right shardMap -> return shardMap
    where 
        refreshShardMapWithPool pool = withResource pool $ 
                \(ctx,_) -> do
                    pipelineConn <- PP.fromCtx ctx
                    envTimeout <- fromMaybe (10 ^ (5 :: Int)) . (>>= readMaybe) <$> lookupEnv "REDIS_CLUSTER_SLOTS_TIMEOUT"
                    eresp <- T.timeout envTimeout (try $ refreshShardMapWithConn pipelineConn True) -- racing with delay of default 100 ms 
                    case eresp of
                        Nothing -> do
                            print $ "TimeoutForConnection " <> show ctx 
                            throwIO $ Cluster.TimeoutException "ClusterSlots Timeout"
                        Just eiShardMapResp -> 
                            case eiShardMapResp of
                                Right shardMap -> pure shardMap 
                                Left (err :: SomeException) -> do
                                    print $ "ShardMapRefreshError-" <> show err 
                                    throwIO $ ClusterConnectError (Error $ Char8.pack ("Couldn't refresh shardMap due to error - " <> show err))

refreshShardMapWithConn :: PP.Connection -> Bool -> IO ShardMap
refreshShardMapWithConn pipelineConn _ = do
    _ <- PP.beginReceiving pipelineConn
    -- Fetch cluster_current_epoch from CLUSTER INFO so we can guard against
    -- stale overwrites: a ShardMap fetched from a gossip-lagged node will have
    -- a lower epoch and will never be allowed to overwrite a newer one.
    epochReply  <- runRedisInternal pipelineConn (sendRequest ["CLUSTER", "INFO"])
    let epoch = either (const 0) parseClusterEpoch epochReply
    slotsResponse <- runRedisInternal pipelineConn clusterSlots
    case slotsResponse of
        Left e -> throwIO $ ClusterConnectError e
        Right slots -> case clusterSlotsResponseEntries slots of
            [] -> throwIO $ ClusterConnectError $ SingleLine "empty slotsResponse"
            _ -> shardMapFromClusterSlotsResponse epoch slots

-- This function gets the byteString from the connectAuth
getConnectionAuthByteString :: Maybe ConnectAuth -> IO (Maybe B.ByteString)
getConnectionAuthByteString connectAuth = 
    case connectAuth of
            Just (Dynamic (ShowableIORefByteString authTokenRef)) -> do
                authToken <- readIORef authTokenRef
                return $ Just authToken
            Just (Static pass) -> return $ Just pass
            _ -> return Nothing