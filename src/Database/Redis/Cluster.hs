{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Database.Redis.Cluster
  ( Connection(..)
  , NodeRole(..)
  , NodeConnection(..)
  , Node(..)
  , ShardMap(..)
  , NodeConnectionMap
  , Host
  , HashSlot
  , Shard(..)
  , TimeoutException(..)
  , NodeID
  , Pipeline(..)
  , PipelineState(..)
  , createClusterConnectionPools
  , destroyNodeResources
  , requestPipelined
  , requestMasterNodes
  , nodes
  , createNodePool
) where

import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as Char8
import qualified Data.IORef as IOR
import Data.Maybe(mapMaybe, fromMaybe)
import Data.List(sortBy, find)
import Data.List.Extra (nubOrd)
import Data.Map(fromListWith, assocs)
import Data.Function(on)
import Control.Exception(Exception, SomeException, throwIO, BlockedIndefinitelyOnMVar(..), catches, Handler(..), try, fromException)
import Data.Pool(Pool, createPool, withResource, destroyAllResources)
import Control.Concurrent.MVar(MVar, newMVar, readMVar, modifyMVar)
import Control.Monad(zipWithM, replicateM)
import Database.Redis.Cluster.HashSlot(HashSlot(..), keyToSlot, numHashSlots)
import qualified Database.Redis.ConnectionContext as CC
import qualified Data.HashMap.Strict as HM
import qualified Data.IntMap.Strict as IntMap
import qualified Data.Time as Time
import           Data.Typeable
import qualified Scanner
import System.Environment (lookupEnv)
import System.IO.Unsafe(unsafeInterleaveIO)
import Text.Read (readMaybe)
import Control.Monad.Extra (loopM, fromMaybeM)
import Database.Redis.Protocol(Reply(Error), renderRequest, reply)
import qualified Database.Redis.Cluster.Command as CMD
import System.Timeout (timeout)
import System.Random (randomRIO)
import Control.Applicative((<|>))

-- This module implements a clustered connection whilst maintaining
-- compatibility with the original Hedis codebase. In particular it still
-- performs implicit pipelining using `unsafeInterleaveIO` as the single node
-- codebase does. To achieve this each connection carries around with it a
-- pipeline of commands. Every time `sendRequest` is called the command is
-- added to the pipeline and an IO action is returned which will, upon being
-- evaluated, execute the entire pipeline. If the pipeline is already executed
-- then it just looks up it's response in the executed pipeline.

-- | A connection to a redis cluster, it is composed of a map from Node IDs to
-- | 'NodeConnection's, a 'Pipeline', and a 'ShardMap'

data Connection = Connection (MVar (ShardMap, NodeConnectionMap)) CMD.InfoMap ClusterConfig

-- | A connection to a single node in the cluster, similar to 'ProtocolPipelining.Connection'
data NodeConnection = NodeConnection (Pool (CC.ConnectionContext, IOR.IORef (Maybe B.ByteString))) NodeID

instance Show NodeConnection where
    show (NodeConnection _ id1) = "nodeId: " <> show id1

instance Eq NodeConnection where
    (NodeConnection _ id1) == (NodeConnection _ id2) = id1 == id2

instance Ord NodeConnection where
    compare (NodeConnection _ id1) (NodeConnection _ id2) = compare id1 id2

data PipelineState =
      -- Nothing in the pipeline has been evaluated yet so nothing has been
      -- sent
      Pending [[B.ByteString]]
      -- This pipeline has been executed, the replies are contained within it
    | Executed [Reply]
      -- We're in a MULTI-EXEC transaction. All commands in the transaction
      -- should go to the same node, but we won't know what node that is until
      -- we see a command with a key. We're storing these transactions and will
      -- send them all together when we see an EXEC.
    | TransactionPending [[B.ByteString]]
-- A pipeline has an MVar for the current state, this state is actually always
-- `Pending` because the first thing the implementation does when executing a
-- pipeline is to take the current pipeline state out of the MVar and replace
-- it with a new `Pending` state. The executed state is held on to by the
-- replies within it.

newtype Pipeline = Pipeline (MVar PipelineState)

data NodeRole = Master | Slave deriving (Show, Eq, Ord)

type Host = String
type Port = Int
type NodeID = B.ByteString
-- Represents a single node, note that this type does not include the 
-- connection to the node because the shard map can be shared amongst multiple
-- connections
data Node = Node NodeID NodeRole Host Port deriving (Show, Eq, Ord)

type MasterNode = Node
type SlaveNode = Node

-- A 'shard' is a master node and 0 or more slaves, (the 'master', 'slave'
-- terminology is unfortunate but I felt it better to follow the documentation
-- until it changes).
data Shard = Shard MasterNode [SlaveNode] deriving (Show, Eq, Ord)

-- A map from hashslot to shards
newtype ShardMap = ShardMap (IntMap.IntMap Shard) deriving (Show)

type NodeConnectionMap = HM.HashMap NodeID NodeConnection

-- Object for storing connection Info which will be used when cluster is refreshed
data ClusterConfig = ClusterConfig
    { 
        requestTimeout  :: Int -- in microseconds
    } deriving Show

newtype MissingNodeException = MissingNodeException [B.ByteString] deriving (Show, Typeable)
instance Exception MissingNodeException

newtype UnsupportedClusterCommandException = UnsupportedClusterCommandException [B.ByteString] deriving (Show, Typeable)
instance Exception UnsupportedClusterCommandException

newtype CrossSlotException = CrossSlotException [[B.ByteString]] deriving (Show, Typeable)
instance Exception CrossSlotException

data NoNodeException = NoNodeException  deriving (Show, Typeable)
instance Exception NoNodeException

data TimeoutException = TimeoutException String deriving (Show, Typeable)
instance Exception TimeoutException

createClusterConnectionPools :: (Host -> CC.PortID -> IO CC.ConnectionContext) -> Int -> Time.NominalDiffTime -> [CMD.CommandInfo] -> ShardMap -> Maybe Double -> IO Connection
createClusterConnectionPools withAuth maxResources idleTime commandInfos shardMap requestTimeoutSeconds = do
        nodeConns <- nodeConnections
        shardNodeVar <- newMVar (shardMap, nodeConns)
        nodeRequestTimeout <- round . (\x -> (x :: Time.NominalDiffTime) * 1000000) . realToFrac . fromMaybe (5 :: Double) . (requestTimeoutSeconds <|> ). (>>= readMaybe) <$> lookupEnv "REDIS_REQUEST_NODE_TIMEOUT"
        let clusterConfig = ClusterConfig {
                    requestTimeout = nodeRequestTimeout
                }
        return $ Connection shardNodeVar (CMD.newInfoMap commandInfos) clusterConfig where
    nodeConnections :: IO (HM.HashMap NodeID NodeConnection)
    nodeConnections = do
      nodeConnectionsList <- mapM (createNodePool withAuth maxResources idleTime) (nubOrd $ nodes shardMap)
      return $ HM.fromList nodeConnectionsList

createNodePool :: (Host -> CC.PortID -> IO CC.ConnectionContext) -> Int -> Time.NominalDiffTime -> Node -> IO (NodeID, NodeConnection)
createNodePool withAuth maxResources idleTime (Node nodeid _ host port) = do
    connectionPool <- createPool (do 
                                    connectionContext <- withAuth host (CC.PortNumber $ toEnum port)
                                    ref <- IOR.newIORef Nothing
                                    return (connectionContext,ref)) (CC.disconnect . fst) 1 idleTime maxResources
    return (nodeid, NodeConnection connectionPool nodeid)

destroyNodeResources :: Connection -> IO ()
destroyNodeResources (Connection shardNodeVar _ _) =
    hasLocked $ readMVar shardNodeVar
    >>= (\(_, nodeConnMap) -> (mapM_ disconnectNode $ HM.elems nodeConnMap))
    where
        disconnectNode (NodeConnection nodePool _) = destroyAllResources nodePool

-- Add a request to the current pipeline for this connection. The pipeline will
-- be executed implicitly as soon as any result returned from this function is
-- evaluated.
requestPipelined :: (Maybe NodeConnection -> IO (ShardMap, NodeConnectionMap)) -> Connection -> [B.ByteString] -> MVar Pipeline -> IO Reply
requestPipelined refreshShardmapAction conn nextRequest pipelineVar = modifyMVar pipelineVar $ \(Pipeline stateVar) -> do
    (newStateVar, repliesIndex) <- hasLocked $ modifyMVar stateVar $ \case
        Pending requests | isMulti nextRequest -> do
            replies <- evaluatePipeline refreshShardmapAction conn requests
            s' <- newMVar $ TransactionPending [nextRequest]
            return (Executed replies, (s', 0))
        Pending requests | length requests > 1000 -> do
            replies <- evaluatePipeline refreshShardmapAction conn (nextRequest:requests)
            return (Executed replies, (stateVar, length requests))
        Pending requests ->
            return (Pending (nextRequest:requests), (stateVar, length requests))
        TransactionPending requests ->
            if isExec nextRequest then do
              replies <- evaluateTransactionPipeline refreshShardmapAction conn (nextRequest:requests)
              return (Executed replies, (stateVar, length requests))
            else
              return (TransactionPending (nextRequest:requests), (stateVar, length requests))
        e@(Executed _) -> do
            s' <- newMVar $
                    if isMulti nextRequest then
                        TransactionPending [nextRequest]
                    else
                        Pending [nextRequest]
            return (e, (s', 0))
    evaluateAction <- unsafeInterleaveIO $ do
        replies <- hasLocked $ modifyMVar newStateVar $ \case
            Executed replies ->
                return (Executed replies, replies)
            Pending requests-> do
                replies <- evaluatePipeline refreshShardmapAction conn requests
                return (Executed replies, replies)
            TransactionPending requests-> do
                replies <- evaluateTransactionPipeline refreshShardmapAction conn requests
                return (Executed replies, replies)
        return $ replies !! repliesIndex
    return (Pipeline newStateVar, evaluateAction)

isMulti :: [B.ByteString] -> Bool
isMulti ("MULTI" : _) = True
isMulti _ = False

isExec :: [B.ByteString] -> Bool
isExec ("EXEC" : _) = True
isExec _ = False

data PendingRequest = PendingRequest Int [B.ByteString]
data CompletedRequest = CompletedRequest Int [B.ByteString] Reply

rawRequest :: PendingRequest -> [B.ByteString]
rawRequest (PendingRequest _ r) =  r

responseIndex :: CompletedRequest -> Int
responseIndex (CompletedRequest i _ _) = i

rawResponse :: CompletedRequest -> Reply
rawResponse (CompletedRequest _ _ r) = r

-- The approach we take here is similar to that taken by the redis-py-cluster
-- library, which is described at https://redis-py-cluster.readthedocs.io/en/master/pipelines.html
--
-- Essentially we group all the commands by node (based on the current shardmap)
-- and then execute a pipeline for each node (maintaining the order of commands
-- on a per node basis but not between nodes). Once we've done this, if any of
-- the commands have resulted in a MOVED error we refresh the shard map, then
-- we run through all the responses and retry any MOVED or ASK errors. This retry
-- step is not pipelined, there is a request per error. This is probably
-- acceptable in most cases as these errors should only occur in the case of
-- cluster reconfiguration events, which should be rare.
evaluatePipeline :: (Maybe NodeConnection -> IO (ShardMap, NodeConnectionMap)) -> Connection -> [[B.ByteString]] -> IO [Reply]
evaluatePipeline refreshShardmapAction conn@(Connection shardNodeVar infoMap (ClusterConfig reqTimeout)) requests = do
        (shardMap, nodesConn) <- hasLocked $ readMVar shardNodeVar
        erequestsByNode <- try $ getRequestsByNode shardMap nodesConn
        requestsByNode <- case erequestsByNode of
            Right reqByNode -> pure reqByNode
            Left (_ :: MissingNodeException) -> do
                (newShardMap, newNodeConn) <- hasLocked $ refreshShardmapAction Nothing
                getRequestsByNode newShardMap newNodeConn
        -- catch the exception thrown at each node level
        -- send the command to random node.
        -- merge the current responses with new responses.
        eresps <- mapM (try . uncurry executeRequests) requestsByNode
        -- take a random connection where there are no exceptions.
        -- PERF_CONCERN: Since usually we send only one request at time, this won't be
        -- heavy perf issue. but still should be evaluated and figured out with complete rewrite.

        -- throwing exception for timeouts thus closing the connection instead of retrying.
        -- otherwise if there is any response in the connection buffer it'll get forwarded to other requests that are reusing the same connection.
        -- leading to jumbled up responses
        resps <- concat <$>
          mapM (\(resp, (nc, r)) -> do
                responses <-  case resp of
                                Right v -> return v
                                Left (err :: SomeException) ->
                                    case fromException err of
                                        Just (er :: TimeoutException) -> hasLocked $ refreshShardmapAction Nothing >> throwIO er
                                        _ -> getRandomConnection nc conn >>= (`executeRequests` r)
                refreshedShardMapAndNodeConnsIORef <- IOR.newIORef Nothing
                mapM (\completedRequest@(CompletedRequest index request response) -> 
                    case response of
                        (Error errString) | (B.isPrefixOf "MOVED" errString || B.isPrefixOf "TRYAGAIN" errString) -> CompletedRequest index request <$> refreshShardMapAndRetryRequest refreshedShardMapAndNodeConnsIORef (hasLocked $ refreshShardmapAction (Just nc)) request
                        (askingRedirection -> Just (host, port)) -> do 
                                refreshedShardMapAndNodeConns <- fromMaybeM (hasLocked $ refreshShardmapAction (Just nc)) $ IOR.readIORef refreshedShardMapAndNodeConnsIORef
                                maybeAskNode <- nodeConnWithHostAndPort refreshedShardMapAndNodeConns host port
                                case maybeAskNode of
                                    Just askNode -> CompletedRequest index request <$> (head <$> tail <$> requestNode reqTimeout askNode (["ASKING"] : [request]))
                                    Nothing -> do
                                        CompletedRequest index request <$> refreshShardMapAndRetryRequest refreshedShardMapAndNodeConnsIORef (hasLocked $ refreshShardmapAction (Just nc)) request
                        _ -> return completedRequest
                    ) responses
            ) (zip eresps requestsByNode)
        return $ map rawResponse $ sortBy (on compare responseIndex) resps
  where
    getRequestsByNode :: ShardMap -> NodeConnectionMap -> IO [(NodeConnection, [PendingRequest])]
    getRequestsByNode shardMap nodeConnMap = do
        commandsWithNodes <- zipWithM (requestWithNodes shardMap nodeConnMap) (reverse [0..(length requests - 1)]) requests
        return $ assocs $ fromListWith (++) (mconcat commandsWithNodes)
    requestWithNodes :: ShardMap -> NodeConnectionMap -> Int -> [B.ByteString] -> IO [(NodeConnection, [PendingRequest])]
    requestWithNodes shardMap nodeConnMap index request = do
        nodeConns <- nodeConnectionForCommand shardMap nodeConnMap infoMap request
        return $ (, [PendingRequest index request]) <$> nodeConns
    executeRequests :: NodeConnection -> [PendingRequest] -> IO [CompletedRequest]
    executeRequests nodeConn nodeRequests = do
        replies <- requestNode reqTimeout nodeConn $ map rawRequest nodeRequests
        return $ zipWith (curry (\(PendingRequest i r, rep) -> CompletedRequest i r rep)) nodeRequests replies
    refreshShardMapAndRetryRequest :: IOR.IORef (Maybe (ShardMap, NodeConnectionMap)) -> IO (ShardMap, NodeConnectionMap) -> [B.ByteString] -> IO Reply
    refreshShardMapAndRetryRequest refreshedShardMapAndNodeConnsIORef refreshShardmap request =  do 
        (newShardMap, newNodeConn) <- fromMaybeM (hasLocked refreshShardmap >>= (\new -> IOR.writeIORef refreshedShardMapAndNodeConnsIORef (Just new) >> return new )) $ 
                                        IOR.readIORef refreshedShardMapAndNodeConnsIORef
        nodeConns <- nodeConnectionForCommand newShardMap newNodeConn infoMap request
        head <$> requestNode reqTimeout (head nodeConns) [request]

--fix multi exec
-- Like `evaluateOnPipeline`, except we expect to be able to run all commands
-- on a single shard. Failing to meet this expectation is an error.
evaluateTransactionPipeline :: (Maybe NodeConnection -> IO (ShardMap, NodeConnectionMap)) -> Connection -> [[B.ByteString]] -> IO [Reply]
evaluateTransactionPipeline refreshShardmapAction conn requests' = do
    let requests = reverse requests'
    let (Connection shardNodeVar infoMap (ClusterConfig reqTimeout)) = conn
    keys <- mconcat <$> mapM (requestKeys infoMap) requests
    -- In cluster mode Redis expects commands in transactions to all work on the
    -- same hashslot. We find that hashslot here.
    -- We could be more permissive and allow transactions that touch multiple
    -- hashslots, as long as those hashslots are on the same node. This allows
    -- a new failure case though: if some of the transactions hashslots are
    -- moved to a different node we could end up in a situation where some of
    -- the commands in a transaction are applied and some are not. Better to
    -- fail early.
    hashSlot <- hashSlotForKeys (CrossSlotException requests) keys
    (ShardMap shardMap, nodeConns) <- hasLocked $ readMVar shardNodeVar
    nodeConn <- nodeConnForHashSlot (ShardMap shardMap, nodeConns) ("evaluateTransactionPipeline" : head requests) hashSlot
    -- catch the exception thrown, send the command to random node.
    -- This change is required to handle the cluster topology change.
    eresps <- try $ requestNode reqTimeout nodeConn requests
    resps <-
      case eresps of
        Right v -> return v
        Left (err :: SomeException) -> do
            refreshedShardMapAndNodeConns <- hasLocked $ refreshShardmapAction (Just nodeConn)
            case fromException err of
                Just (er :: TimeoutException) -> throwIO er
                _ -> do
                    newNodeConn <- nodeConnForHashSlot refreshedShardMapAndNodeConns ("evaluateTransactionPipeline" : head requests) hashSlot
                    requestNode reqTimeout newNodeConn requests
    -- The Redis documentation has the following to say on the effect of
    -- resharding on multi-key operations:
    --
    --     Multi-key operations may become unavailable when a resharding of the
    --     hash slot the keys belong to is in progress.
    --
    --     More specifically, even during a resharding the multi-key operations
    --     targeting keys that all exist and all still hash to the same slot
    --     (either the source or destination node) are still available.
    --
    --     Operations on keys that don't exist or are - during the resharding -
    --     split between the source and destination nodes, will generate a
    --     -TRYAGAIN error. The client can try the operation after some time,
    --     or report back the error.
    --
    --     https://redis.io/topics/cluster-spec#multiple-keys-operations
    --
    -- An important take-away here is that MULTI..EXEC transactions can fail
    -- with a redirect in which case we need to repeat the full transaction on
    -- the node we're redirected too.
    --
    -- A second important takeway is that MULTI..EXEC transactions might
    -- temporarily fail during resharding with a -TRYAGAIN error. We can only
    -- make arbitrary decisions about how long to paus before the retry and how
    -- often to retry, so instead we'll propagate the error to the library user
    -- and let them decide how they would like to handle the error.
    loopM (\case 
        heads:tails   -> if moved heads then do
                            refreshedShardMapAndNodeConns <- hasLocked $ refreshShardmapAction (Just nodeConn)
                            newNodeConn <- nodeConnForHashSlot refreshedShardMapAndNodeConns ("Error in evaluateTransactionPipeline moved retry" : head requests) hashSlot
                            Right <$> requestNode reqTimeout newNodeConn requests
                        else 
                            case askingRedirection heads of
                                Just (host,port) -> do 
                                        refreshedShardMapAndNodeConns <- hasLocked $ refreshShardmapAction (Just nodeConn)
                                        maybeAskNode <- nodeConnWithHostAndPort refreshedShardMapAndNodeConns host port
                                        case maybeAskNode of
                                            Just askNode -> Right . tail <$> requestNode reqTimeout askNode (["ASKING"] : requests)
                                            Nothing -> do
                                                refreshedShardMapAndNodeConns' <- hasLocked $ refreshShardmapAction (Just nodeConn)
                                                newNodeConn <- nodeConnForHashSlot refreshedShardMapAndNodeConns' ("Error in evaluateTransactionPipeline ASK retry" : head requests) hashSlot
                                                Right <$> requestNode reqTimeout newNodeConn requests
                                Nothing -> return $ Left tails
        []          -> return $ Right resps) resps

nodeConnForHashSlot :: (ShardMap, NodeConnectionMap) -> [B.ByteString] -> HashSlot -> IO NodeConnection
nodeConnForHashSlot (ShardMap shardMap, nodeConnsMap) errInfo hashSlot = do
    node <-
        case IntMap.lookup (fromEnum hashSlot) shardMap of
            Nothing -> throwIO $ MissingNodeException ("HashSlot lookup failed in nodeConnForHashSlot" : errInfo)
            Just (Shard master _) -> return master
    case HM.lookup (nodeId node) nodeConnsMap of
        Nothing -> do
            throwIO $ MissingNodeException ("NodeId lookup failed in nodeConnForHashSlot" : errInfo)
        Just nodeConn' -> return nodeConn'

hashSlotForKeys :: Exception e => e -> [B.ByteString] -> IO HashSlot
hashSlotForKeys exception keys =
    case nubOrd (keyToSlot <$> keys) of
        -- If none of the commands contain a key we can send them to any
        -- node. Let's pick the first one.
        [] -> HashSlot <$> randomRIO (0, numHashSlots - 1)
        [hashSlot] -> return hashSlot
        _ -> throwIO $ exception

requestKeys :: CMD.InfoMap -> [B.ByteString] -> IO [B.ByteString]
requestKeys infoMap request =
    case CMD.keysForRequest infoMap request of
        Nothing -> throwIO $ UnsupportedClusterCommandException request
        Just k -> return k

askingRedirection :: Reply -> Maybe (Host, Port)
askingRedirection (Error errString) = case Char8.words errString of
    ["ASK", _, hostport] -> case Char8.split ':' hostport of
       [host, portString] -> case Char8.readInt portString of
         Just (port,"") -> Just (Char8.unpack host, port)
         _ -> Nothing
       _ -> Nothing
    _ -> Nothing
askingRedirection _ = Nothing

moved :: Reply -> Bool
moved (Error errString) = case Char8.words errString of
    "MOVED":_ -> True
    _ -> False
moved _ = False

nodeConnWithHostAndPort :: (ShardMap, NodeConnectionMap)  -> Host -> Port -> IO (Maybe NodeConnection)
nodeConnWithHostAndPort (shardMap, nodeConns) host port = do
    case nodeWithHostAndPort shardMap host port of
        Nothing -> return Nothing
        Just node -> return (HM.lookup (nodeId node) nodeConns)

nodeConnectionForCommand :: ShardMap -> NodeConnectionMap -> CMD.InfoMap -> [B.ByteString] -> IO [NodeConnection]
nodeConnectionForCommand (ShardMap shardMap) nodeConns infoMap request =
    case request of
        ("FLUSHALL" : _) -> allNodes
        ("FLUSHDB" : _) -> allNodes
        ("QUIT" : _) -> allNodes
        ("UNWATCH" : _) -> allNodes
        _ -> do
            keys <- requestKeys infoMap request
            hashSlot <- hashSlotForKeys (CrossSlotException [request]) keys
            node <- case IntMap.lookup (fromEnum hashSlot) shardMap of
                Nothing -> throwIO $ MissingNodeException ("HashSlot lookup failed in nodeConnectionForCommand" : request)
                Just (Shard master _) -> return master
            maybe (throwIO $ MissingNodeException ("NodeId lookup failed in nodeConnectionForCommand" : request)) (return . return) (HM.lookup (nodeId node) nodeConns)
    where
        allNodes = do
            maybeNodes <- allMasterNodes (ShardMap shardMap) nodeConns
            case maybeNodes of
                Nothing -> throwIO $ MissingNodeException ("Master node lookup failed" : request)
                Just allNodes' -> return allNodes'

allMasterNodes :: ShardMap -> NodeConnectionMap -> IO (Maybe [NodeConnection])
allMasterNodes (ShardMap shardMap) nodeConns = do
    return $ mapM (flip HM.lookup nodeConns) onlyMasterNodeIds
  where
    onlyMasterNodeIds = nubOrd $ (\(Shard master _) -> nodeId master) <$> (IntMap.elems shardMap)

requestNode :: Int -> NodeConnection -> [[B.ByteString]] -> IO [Reply]
requestNode requestNodeTimeout (NodeConnection pool _) requests = withResource pool $ \(ctx, lastRecvRef) -> do
    mayberesp <- timeout requestNodeTimeout $ requestNodeImpl ctx lastRecvRef
    case mayberesp of
      Just a    -> return a
      Nothing   -> putStrLn "timeout happened" *> throwIO (TimeoutException "Request Timeout")
    where
    requestNodeImpl :: CC.ConnectionContext -> IOR.IORef (Maybe B.ByteString) -> IO [Reply]
    requestNodeImpl ctx lastRecvRef = do
        mapM_ (sendNode ctx . renderRequest) requests
        _ <- CC.flush ctx
        replicateM (length requests) $ recvNode ctx lastRecvRef
    sendNode :: CC.ConnectionContext -> B.ByteString -> IO ()
    sendNode = CC.send
    recvNode :: CC.ConnectionContext -> IOR.IORef (Maybe B.ByteString) -> IO Reply
    recvNode ctx lastRecvRef = do
        maybeLastRecv <- IOR.readIORef lastRecvRef
        scanResult <- case maybeLastRecv of
            Just lastRecv -> Scanner.scanWith (CC.recv ctx) reply lastRecv
            Nothing -> Scanner.scanWith (CC.recv ctx) reply B.empty

        case scanResult of
            Scanner.Fail{}       -> CC.errConnClosed
            Scanner.More{}    -> error "Hedis: parseWith returned Partial"
            Scanner.Done rest' r -> do
                IOR.writeIORef lastRecvRef (Just rest')
                return r

{-# INLINE nodes #-}
nodes :: ShardMap -> [Node]
nodes (ShardMap shardMap) = concatMap snd $ IntMap.toList $ fmap shardNodes shardMap where
    shardNodes :: Shard -> [Node]
    shardNodes (Shard master slaves) = master:slaves


nodeWithHostAndPort :: ShardMap -> Host -> Port -> Maybe Node
nodeWithHostAndPort shardMap host port = find (\(Node _ _ nodeHost nodePort) -> port == nodePort && host == nodeHost) (nodes shardMap)

nodeId :: Node -> NodeID
nodeId (Node theId _ _ _) = theId

hasLocked :: IO a -> IO a
hasLocked action =
  action `catches`
  [ Handler $ \exc@BlockedIndefinitelyOnMVar -> throwIO exc
  ]


requestMasterNodes :: Connection -> [B.ByteString] -> IO [Reply]
requestMasterNodes conn@(Connection _ _ (ClusterConfig reqTimeout)) req = do
    masterNodeConns <- masterNodes conn
    concat <$> mapM (flip (requestNode reqTimeout) [req]) masterNodeConns

masterNodes :: Connection -> IO [NodeConnection]
masterNodes (Connection shardNodeVar _ _) = do
    (ShardMap shardMap, nodeConns) <- hasLocked $ readMVar shardNodeVar
    let masterNodeIds = map (\(Shard m _) -> nodeId m) $ IntMap.elems shardMap
    return $ mapMaybe (`HM.lookup` nodeConns) (nubOrd masterNodeIds)

getRandomConnection :: NodeConnection -> Connection -> IO NodeConnection
getRandomConnection nc conn = do
  let (Connection shardNodeVar _ _) = conn
  (_, nodeConns) <- hasLocked $ readMVar shardNodeVar
  let conns = HM.elems nodeConns
  return $ fromMaybe (head conns) $ find (nc /= ) conns
