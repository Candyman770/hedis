{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE TypeApplications #-}

module Database.Redis.ClusterChan.ClusterChan where

import System.Random (randomRIO)
import qualified Data.List as L
import qualified Data.HashMap.Strict as HM
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as Char8
import qualified Data.Pool as DP
import qualified Data.IntMap.Strict as IntMap
import qualified Data.Time as Time
import qualified Control.Concurrent.Chan.Unagi as Chan
import GHC.Conc (ThreadStatus(..), threadStatus)
import Control.Monad (replicateM_, replicateM, forever, void, when)
import Control.Concurrent.Async(race, AsyncCancelled(..))
import Control.Concurrent (ThreadId, forkIO, threadDelay, killThread)
import Control.Concurrent.MVar (MVar, readMVar, newEmptyMVar, putMVar, tryTakeMVar, takeMVar)
import Data.IORef (IORef, newIORef, readIORef, writeIORef)
import Control.Monad.Reader (ReaderT, runReaderT, ask, liftIO, lift)
import Control.Monad.Catch (catchAll)
import Control.Exception (SomeException, Exception, throwIO, try, mask, evaluate, onException, throwTo)
import Data.Maybe (maybeToList, fromMaybe)
import Data.Typeable (Typeable)
import Scanner

import qualified Database.Redis.Cluster as CL
import qualified Database.Redis.Cluster.Command as CMD
import qualified Database.Redis.ConnectionContext as CC
import qualified Database.Redis.Connection as Conn
import Database.Redis.Protocol (Reply(..), renderRequest, reply)
import qualified Database.Redis.ProtocolPipelining as PP
import Database.Redis.Types (RedisArg(..), RedisResult(..), Status(..))

data ChanConnection = ChanConnection ClusterChannels (MVar CL.ShardMap) (DP.Pool PP.Connection) CMD.InfoMap Conn.ConnectInfo

-- data ChanConnection = ChanConnection ClusterChannelMap (MVar CL.ShardMap) (DP.Pool PP.Connection) CMD.InfoMap Conn.ConnectInfo

newtype NodeMap = NodeMap (HM.HashMap CL.NodeID CL.NodeConnection)

type NodeMapMVar = MVar (DP.Pool NodeMap)

data ClusterChannel = ClusterChannel (Chan.InChan ChanRequest) NodeMapMVar ClusterChanWorkers 

type ClusterChannels = [ClusterChannel]

data RedisChanException = ConsumerKilled | RequestTimeout | MissingNode | ConnectionLost SomeException | ConnectionFailure SomeException deriving (Show, Typeable)
instance Exception RedisChanException

type ClusterChannelMap = HM.HashMap CL.NodeID ClusterChannel

data ClusterChanWorkers =
  ClusterChanWorkers
    { writer :: ThreadId
    , reader :: ThreadId
    }

-- data ChanRequest =
--   = Transaction [[B.ByteString]]
--   | Normal [B.ByteString]
--   deriving (Show)

data ChanRequest =
  ChanRequest
  { request :: [B.ByteString]
  , nodeID :: B.ByteString
  , mVar :: MVar (Either RedisChanException Reply)
  }

data ChanRawRequest
  = Transaction [B.ByteString]
  | Normal B.ByteString

data ConnectChanInfo =
  ConnectChanInfo
    { channelCount :: Int
    , pipelineBatchSize :: Int
    }
  deriving (Show, Eq)

data ClusterChanEnv =
  ClusterChanEnv
    { chanConnection :: ChanConnection
    , clusterChanIORef :: IORef Reply
    }

type RedisChan = ReaderT ClusterChanEnv IO

createClusterChannels :: [NodeMapMVar] -> (NodeMapMVar -> IO ()) -> Int -> IO ClusterChannels
createClusterChannels nodeMapVars nodeMapRefresher requestTimeout = do
  clusterChannels <- mapM (\nodeMapVar -> do
    (inChanReq, outChanReq) <- Chan.newChan
    (inChanRes, outChanRes) <- Chan.newChan
    writerThread <- forkIO $ consumerWriter outChanReq nodeMapVar nodeMapRefresher inChanRes
    readerThread <- forkIO $ consumerReader outChanRes nodeMapVar nodeMapRefresher requestTimeout
    pure $ ClusterChannel inChanReq nodeMapVar (ClusterChanWorkers writerThread readerThread)
    ) nodeMapVars
  pure clusterChannels

-- createClusterChannels :: NodeMap -> (NodeMapMVar -> IO ()) -> Int -> Int -> IO ClusterChannelMap
-- createClusterChannels (NodeMap nodeMap) nodeMapRefresher pipelineBatchSize requestTimeout = do
--   clusterChannels <- mapM (\(nodeId, nodeConn) -> do
--     (inChanReq, outChanReq) <- Chan.newChan
--     (inChanRes, outChanRes) <- Chan.newChan
--     nodeMapVar <- newEmptyMVar
--     writerThread <- forkIO $ consumerWriter2 outChanReq [] nodeConn inChanRes pipelineBatchSize
--     readerThread <- forkIO $ consumerReader outChanRes nodeMapVar nodeMapRefresher requestTimeout
--     pure $ (nodeId, ClusterChannel inChanReq nodeMapVar (ClusterChanWorkers writerThread readerThread))
--     ) (HM.toList nodeMap)
--   pure $ HM.fromList clusterChannels

consumerWriter :: Chan.OutChan ChanRequest -> NodeMapMVar -> (NodeMapMVar -> IO ()) -> Chan.InChan (CL.NodeConnection, Int, MVar (Either RedisChanException Reply)) -> IO ()
consumerWriter outChanReq nodeMapMVar nodeMapRefresher inChanRes =
  forever $ do
    ChanRequest{..} <- Chan.readChan outChanReq
    nodeMapP <- CL.hasLocked $ readMVar nodeMapMVar
    DP.withResource nodeMapP $ \(NodeMap nodeMap) -> do
      case HM.lookup nodeID nodeMap of
        Nothing -> putMVar mVar (Left MissingNode)
        Just (nodeConn@(CL.NodeConnection ctx _ _)) -> do
          res <- try $ do
            mapM_ (CC.send ctx) request
            CC.flush ctx
          case res of
            Left (err :: SomeException) -> do -- retry ?
              void $ try @SomeException $ CC.disconnect ctx
              foreverRetry (nodeMapRefresher nodeMapMVar) "ConsumerWriter NodeMapRefreshErr"
              putMVar mVar (Left (ConnectionLost err))
            Right _ ->
              Chan.writeChan inChanRes (nodeConn, length request, mVar)

consumerWriter2 :: Chan.OutChan ChanRequest -> [(Chan.Element ChanRequest, IO ChanRequest)] -> CL.NodeConnection -> Chan.InChan (CL.NodeConnection, Int, MVar (Either RedisChanException Reply)) -> Int -> IO ()
consumerWriter2 outChanReq currReqList nodeConn@(CL.NodeConnection ctx _ _) inChanRes pipelineBatchSize = do
  ((_, blockingRead) : newReqList) <- (currReqList ++) <$> replicateM (pipelineBatchSize - (length currReqList)) (Chan.tryReadChan outChanReq)
  fstElement <- blockingRead
  (readyList, remReqList) <- spanM newReqList []
  res <- try $ do
    mapM_ (\(ChanRequest request _ _) -> mapM_ (CC.send ctx) request) (fstElement : readyList)
    CC.flush ctx
  case res of
    Left (err :: SomeException) -> do
      void $ try @SomeException $ CC.disconnect ctx
      -- get new connection
      let newNodeConn = undefined
      mapM_ (\(ChanRequest _ _ mVar) -> putMVar mVar (Left (ConnectionLost err))) (fstElement : readyList)
      consumerWriter2 outChanReq remReqList newNodeConn inChanRes pipelineBatchSize
    Right _ -> do
      mapM_ (\(ChanRequest request _ mVar) -> Chan.writeChan inChanRes (nodeConn, length request, mVar)) (fstElement : readyList)
      consumerWriter2 outChanReq remReqList nodeConn inChanRes pipelineBatchSize
  where
    spanM :: [(Chan.Element ChanRequest, IO ChanRequest)] -> [ChanRequest] -> IO ([ChanRequest], [(Chan.Element ChanRequest, IO ChanRequest)])
    spanM [] xs = pure (reverse xs, [])
    spanM rq@((element, _) : rest) xs = do
      reqM <- Chan.tryRead element
      case reqM of
        Nothing -> pure (reverse xs, rq)
        Just req -> spanM rest (req : xs)


consumerReader :: Chan.OutChan (CL.NodeConnection, Int, MVar (Either RedisChanException Reply)) -> NodeMapMVar -> (NodeMapMVar -> IO ()) -> Int -> IO ()
consumerReader outChan nodeMapMVar nodeMapRefresher requestTimeout =
  forever $ do
    (nodeConn@(CL.NodeConnection ctx _ _), noOfResp, mVar) <- Chan.readChan outChan
    -- eresp <- race (threadDelay requestTimeout) (try $ do
    --   replicateM_ (noOfResp - 1) (recvNode nodeConn)
    --   recvNode nodeConn)
    eresp <- Right <$> (try $ do
      replicateM_ (noOfResp - 1) (recvNode nodeConn)
      recvNode nodeConn)
    case eresp of
      Left _ -> do
        void $ try @SomeException $ CC.disconnect ctx
        -- foreverRetry (nodeMapRefresher nodeMapMVar) "ConsumerReader NodeMapRefreshErr"
        putMVar mVar (Left RequestTimeout)
      Right res ->
        case res of
          Left (err :: SomeException) -> do -- retry ?
            void $ try @SomeException $ CC.disconnect ctx
            -- foreverRetry (nodeMapRefresher nodeMapMVar) "ConsumerReader NodeMapRefreshErr"
            putMVar mVar (Left (ConnectionLost err))
          Right val -> do
            putMVar mVar (Right val)
  where
    recvNode :: CL.NodeConnection -> IO Reply
    recvNode (CL.NodeConnection ctx lastRecvRef _) = do
      maybeLastRecv <- readIORef lastRecvRef
      scanResult <- case maybeLastRecv of
          Just lastRecv -> Scanner.scanWith (CC.recv ctx) reply lastRecv
          Nothing -> Scanner.scanWith (CC.recv ctx) reply B.empty
      case scanResult of
        Scanner.Fail{}       -> CC.errConnClosed
        Scanner.More{}    -> error "Hedis: parseWith returned Partial"
        Scanner.Done rest' r -> do
          writeIORef lastRecvRef (Just rest')
          return r

foreverRetry :: IO a -> String -> IO a
foreverRetry action errMsg = do
  eRes <- try action
  case eRes of
    Left (err :: SomeException) -> do
      putStrLn $ errMsg <> " - " <> (show err)
      threadDelay 1000000
      foreverRetry action errMsg
    Right res -> pure res

nodeIdForCommand :: CMD.InfoMap -> CL.ShardMap -> [[B.ByteString]] -> IO CL.NodeID
nodeIdForCommand infoMap (CL.ShardMap shardMap) requests = do
  keys <- concat <$> mapM (CL.requestKeys infoMap) requests
  hashSlot <- CL.hashSlotForKeys (CL.CrossSlotException requests) keys
  case IntMap.lookup (fromEnum hashSlot) shardMap of
    Nothing -> throwIO $ CL.MissingNodeException (head requests)
    Just (CL.Shard (CL.Node nodeId _ _ _) _) -> return nodeId

nodeWithHostAndPort :: CL.ShardMap -> String -> Int -> Maybe CL.Node
nodeWithHostAndPort shardMap host port =
  L.find (\(CL.Node _ _ nHost nPort) -> (nHost == host) && (nPort == port)) (getNodes shardMap)

runRedisChan :: ChanConnection -> RedisChan a -> IO a
runRedisChan chanConnection redis = do
  ref <- newIORef (SingleLine "nobody will ever see this")
  r <- runReaderT redis (ClusterChanEnv chanConnection ref) 
  readIORef ref >>= (`seq` return ())
  return r

sendRequest :: (RedisResult a)
    => [B.ByteString] -> RedisChan (Either Reply a)
sendRequest req = do
  ClusterChanEnv (ChanConnection channels shardMapVar connForShardMapRefresh infoMap connectInfo) ioRef <- ask
  r <- liftIO $ do
    idx <- randomRIO (0, (length channels) - 1) 
    pushRequestAndGetReply connForShardMapRefresh shardMapVar infoMap [req] (channels !! idx) connectInfo 2
  lift (writeIORef ioRef r)
  pure $ decode r

pushRequestAndGetReply :: DP.Pool PP.Connection -> MVar CL.ShardMap -> CMD.InfoMap -> [[B.ByteString]] -> ClusterChannel -> Conn.ConnectInfo -> Int -> IO Reply
pushRequestAndGetReply connForShardMapRefresh shardMapVar infoMap requests clusterChannel@(ClusterChannel inChan nodeMapVar worker) connectInfo retryCount = do
  shardMap <- CL.hasLocked $ readMVar shardMapVar
  nodeId <- nodeIdForCommand infoMap shardMap requests
  -- (ClusterChannel inChan nodeMapVar worker) <-
  --   case HM.lookup nodeId clusterChannel of
  --     Just channel -> pure channel
  --     Nothing -> throwIO $ CL.MissingNodeException (head requests)
  mVar <- newEmptyMVar
  Chan.writeChan inChan (ChanRequest (renderRequest <$> requests) nodeId mVar)
  resp <- race (threadDelay 5000000) (readMVar mVar)
  -- resp <- Right <$> readMVar mVar
  case resp of
    Right eReply ->
      case eReply of
        Right rply ->
          case rply of
            (moveRedirection -> Just (slot, host, port)) ->
              case retryCount of
                0 -> pure rply
                _ -> do
                  (CL.ShardMap shardMapNew) <- CL.hasLocked $ readMVar shardMapVar
                  case (IntMap.lookup slot shardMapNew) of
                    Just (CL.Shard (CL.Node _ _ nHost nPort) _) | ((nHost == host) && (nPort == port)) -> pure ()
                    _ -> void $ refreshShardMap connForShardMapRefresh shardMapVar (Conn.requestTimeout connectInfo)
                  pushRequestAndGetReply connForShardMapRefresh shardMapVar infoMap requests clusterChannel connectInfo (retryCount-1)  
            (askingRedirection -> Just (host, port)) -> do
              shardMapNew <- CL.hasLocked $ readMVar shardMapVar
              let maybeAskNode = nodeWithHostAndPort shardMapNew host port
              case maybeAskNode of
                Just _ -> pushRequestAndGetReply connForShardMapRefresh shardMapVar infoMap (["ASKING"] : requests) clusterChannel connectInfo (retryCount-1)
                Nothing -> case retryCount of
                  0 -> throwIO $ CL.MissingNodeException (head requests)
                  _ -> do
                    void $ refreshShardMap connForShardMapRefresh shardMapVar (Conn.requestTimeout connectInfo)
                    pushRequestAndGetReply connForShardMapRefresh shardMapVar infoMap requests clusterChannel connectInfo (retryCount-1)
            _ -> pure rply
        Left err | (retryCount <= 0) -> throwIO err
        Left err ->
          case err of
            RequestTimeout -> throwIO err
            MissingNode -> do
              shardMapNew <- CL.hasLocked $ readMVar shardMapVar
              refreshNodeMap shardMapNew connectInfo nodeMapVar
              pushRequestAndGetReply connForShardMapRefresh shardMapVar infoMap requests clusterChannel connectInfo (retryCount-1)
            _ ->
              pushRequestAndGetReply connForShardMapRefresh shardMapVar infoMap requests clusterChannel connectInfo (retryCount-1)
    Left _ ->
      throwIO $ CL.TimeoutException "Reply MVar Wait Timeout"

compete :: IO a -> IO b -> IO (Either a b)
compete left right = do
  done <- newEmptyMVar
  mask $ \restore -> do
    lid <- forkIO $
      restore (left >>= putMVar done . Right . Left)
        `catchAll` (putMVar done . Left)
    rid <- forkIO $
      restore (right >>= putMVar done . Right . Right)
        `catchAll` (putMVar done . Left)
    eResp <- takeMVar done
    throwTo lid AsyncCancelled
    throwTo rid AsyncCancelled
    case eResp of
      Left err -> throwIO err
      Right res -> pure res

moveRedirection :: Reply -> Maybe (Int, String, Int)
moveRedirection (Error errString) = case Char8.words errString of
  ["MOVED", slotStr, hostport] ->
    let (host, portString) = Char8.breakEnd (== ':') hostport
    in case Char8.readInt portString of
        Just (port,"") -> case Char8.readInt slotStr of
          Just (slot, "") -> Just (slot, Char8.unpack $ Char8.init host, port)
          _ -> Nothing
        _ -> Nothing
  _ -> Nothing
moveRedirection _ = Nothing

askingRedirection :: Reply -> Maybe (String, Int)
askingRedirection (Error errString) = case Char8.words errString of
    ["ASK", _, hostport] ->
      let (host, portString) = Char8.breakEnd (== ':') hostport
      in case Char8.readInt portString of
          Just (port,"") -> Just (Char8.unpack $ Char8.init host, port)
          _ -> Nothing
    _ -> Nothing
askingRedirection _ = Nothing

refreshNodeMap :: CL.ShardMap -> Conn.ConnectInfo -> NodeMapMVar -> IO ()
refreshNodeMap shardMap connectInfo nodeMapVar = do
  void $ modifyMVarNoBlocking nodeMapVar $ \nodeMapPOld -> do
    nodeMapPool <- DP.createPool (connectNodes connectInfo (getNodes shardMap)) disconnectMap 1 (Conn.connectMaxIdleTime connectInfo) 1
    DP.withResource nodeMapPool $ \_ -> pure ()
    pure nodeMapPool
    -- DP.destroyAllResources nodeMapPOld

connectNode :: Conn.ConnectInfo -> CL.Node -> IO CL.NodeConnection
connectNode connectInfo (CL.Node n _ host port) = do
  let connInfo = connectInfo{Conn.connectHost = host, Conn.connectPort = CC.PortNumber $ toEnum port}
  conn <- Conn.createConnection connInfo
  ref <- newIORef Nothing
  return (CL.NodeConnection (PP.toCtx conn) ref n)

connectNodes :: Conn.ConnectInfo -> [CL.Node] -> IO NodeMap
connectNodes connectInfo nodes = do
  nodeConns <- mapM (connectNode connectInfo) nodes
  pure $ NodeMap $ foldl (\acc nodeConn@(CL.NodeConnection _ _ nodeId) ->
    HM.insert nodeId nodeConn acc
    ) HM.empty nodeConns

disconnect :: CL.NodeConnection -> IO ()
disconnect (CL.NodeConnection ctx _ _) = CC.disconnect ctx

disconnectMap :: NodeMap -> IO ()
disconnectMap (NodeMap nodeMap) = mapM_ (\(CL.NodeConnection ctx _ _) -> CC.disconnect ctx) $ HM.elems nodeMap

refreshShardMap :: DP.Pool PP.Connection -> MVar (CL.ShardMap) -> Maybe Time.NominalDiffTime -> IO CL.ShardMap
refreshShardMap connP shardMapVar timeoutOpts = do
  modifyMVarNoBlocking shardMapVar $ \_ -> do
    DP.withResource connP $ \conn -> do
      let timeout = fromMaybe 1000 $ (round . (* 1000000) <$> timeoutOpts)
      raceResult <- race (threadDelay timeout) (try $ Conn.refreshShardMapWithConn conn True) -- racing with delay of default 1 ms 
      case raceResult of
        Left () ->
          throwIO $ CL.TimeoutException "ClusterSlots Timeout"
        Right eiShardMapResp ->
          case eiShardMapResp of
            Right shardMap -> pure shardMap 
            Left (_ :: SomeException) -> do
              throwIO $ Conn.ClusterConnectError (Error "Couldn't refresh shardMap due to connection error")

getNodes :: CL.ShardMap -> [CL.Node]
getNodes (CL.ShardMap shardMap) =
  IntMap.foldl (\acc (CL.Shard (node@(CL.Node nodeId _ _ _)) _) ->
    case (L.find (\(CL.Node nId _ _ _) -> nId == nodeId) acc) of
      Just _ -> acc
      Nothing -> node : acc
    ) [] shardMap

modifyMVarNoBlocking :: MVar a -> (a -> IO a) -> IO a
modifyMVarNoBlocking mVar io = do
  aM <- tryTakeMVar mVar
  case aM of
    Nothing -> readMVar mVar
    Just a -> mask $ \restore -> do
      a' <- restore (io a >>= evaluate) `onException` putMVar mVar a
      putMVar mVar a'
      pure a'


-- get, set, xadd, xdel, sadd, srem, sismember, expire, 

data TrimOpts = NoArgs | Maxlen Integer | ApproxMaxlen Integer

xaddOpts ::
    B.ByteString -- ^ key
    -> B.ByteString -- ^ id
    -> [(B.ByteString, B.ByteString)] -- ^ (field, value)
    -> TrimOpts
    -> RedisChan (Either Reply B.ByteString)
xaddOpts key entryId fieldValues opts = sendRequest $
    ["XADD", key] ++ optArgs ++ [entryId] ++ fieldArgs
    where
        fieldArgs = concatMap (\(x,y) -> [x,y]) fieldValues
        optArgs = case opts of
            NoArgs -> []
            Maxlen mx -> ["MAXLEN", encode mx]
            ApproxMaxlen mx -> ["MAXLEN", "~", encode mx]

xadd ::
    B.ByteString -- ^ stream
    -> B.ByteString -- ^ id
    -> [(B.ByteString, B.ByteString)] -- ^ (field, value)
    -> RedisChan (Either Reply B.ByteString)
xadd key entryId fieldValues = xaddOpts key entryId fieldValues NoArgs

xdel ::
    B.ByteString -- ^ stream
    -> [B.ByteString] -- ^ message IDs
    -> RedisChan (Either Reply Integer)
xdel stream messageIds = sendRequest $ ["XDEL", stream] ++ messageIds

set ::
    B.ByteString -- ^ key
    -> B.ByteString -- ^ value
    -> RedisChan (Either Reply Status)
set key value = sendRequest ["SET", key, value]


data Condition = Nx | Xx deriving (Show, Eq)


instance RedisArg Condition where
  encode Nx = "NX"
  encode Xx = "XX"


data SetOpts = SetOpts
  { setSeconds      :: Maybe Integer
  , setMilliseconds :: Maybe Integer
  , setCondition    :: Maybe Condition
  } deriving (Show, Eq)


setOpts ::
    B.ByteString -- ^ key
    -> B.ByteString -- ^ value
    -> SetOpts
    -> RedisChan (Either Reply Status)
setOpts key value SetOpts{..} =
    sendRequest $ concat [["SET", key, value], ex, px, condition]
  where
    ex = maybe [] (\s -> ["EX", encode s]) setSeconds
    px = maybe [] (\s -> ["PX", encode s]) setMilliseconds
    condition = map encode $ maybeToList setCondition

get ::
    B.ByteString -- ^ key
    -> RedisChan (Either Reply (Maybe B.ByteString))
get key = sendRequest (["GET"] ++ [encode key] )

sadd ::
    B.ByteString -- ^ key
    -> [B.ByteString] -- ^ member
    -> RedisChan (Either Reply Integer)
sadd key member = sendRequest (["SADD"] ++ [encode key] ++ map encode member )

srem ::
    B.ByteString -- ^ key
    -> [B.ByteString] -- ^ member
    -> RedisChan (Either Reply Integer)
srem key member = sendRequest (["SREM"] ++ [encode key] ++ map encode member )

expire ::
    B.ByteString -- ^ key
    -> Integer -- ^ seconds
    -> RedisChan (Either Reply Bool)
expire key seconds = sendRequest (["EXPIRE"] ++ [encode key] ++ [encode seconds] )

sismember ::
    B.ByteString -- ^ key
    -> B.ByteString -- ^ member
    -> RedisChan (Either Reply Bool)
sismember key member = sendRequest (["SISMEMBER"] ++ [encode key] ++ [encode member] )