{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE DataKinds #-}

module Database.Redis.Lite.Cluster where

import qualified Data.List as L
import qualified Data.HashMap.Strict as HM
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as Char8
import qualified Data.IntMap.Strict as IntMap
import qualified Control.Concurrent.Chan.Unagi as Chan
import qualified Data.Time as Time
import Control.Monad (replicateM, forever, void, foldM)
import Control.Monad.Extra (whenJust)
import Control.Concurrent.Async (race)
import Control.Concurrent (ThreadId, forkIO, threadDelay, killThread)
import Control.Concurrent.MVar (MVar, newMVar, readMVar, newEmptyMVar, putMVar, tryPutMVar, tryTakeMVar, modifyMVar_)
import Data.IORef (readIORef, writeIORef)
import Control.Exception (SomeException, AsyncException(ThreadKilled), throwIO, try, mask, evaluate, onException, fromException)
-- import Data.Maybe (fromMaybe)
import Data.List.Extra (firstJust)
import Scanner
import System.Clock as SC (TimeSpec(..), Clock(..), getTime)

import qualified Database.Redis.Cluster as CL
import qualified Database.Redis.Cluster.Command as CMD
import qualified Database.Redis.ConnectionContext as CC
import Database.Redis.Protocol (Reply(..), renderRequest, reply)
import Database.Redis.Lite.Types

initiateClusterWorkers :: NodeMapState -> (Maybe NodeID -> NodeMapState -> IO ()) -> Time.NominalDiffTime -> Time.NominalDiffTime -> IO RedisLiteChannel
initiateClusterWorkers nodeMapState nodeMapRefresher keepAlive requestTime = do
  (inChanReq, outChanReq) <- Chan.newChan
  (inChanRes, outChanRes) <- Chan.newChan
  dupOutChanRes <- Chan.dupChan inChanRes
  let keepAliveTime = round . (1000000 *) $ keepAlive
      reqTimeout = round . (1000000 *) $ requestTime
  writerThread <- forkIO $ bufferWriter outChanReq nodeMapState nodeMapRefresher inChanRes
  readerThread <- forkIO $ bufferReader outChanRes
  oldNodeMapState <- newMVar (NodeMap HM.empty)
  workers <- newEmptyMVar
  connRefresherThread <- forkIO $ connectionRefresher (nodeMapRefresher Nothing nodeMapState) keepAliveTime oldNodeMapState
  timeoutHandlerThread <- forkIO $ bufferTimeoutHandler dupOutChanRes nodeMapState nodeMapRefresher (forkIO $ bufferReader outChanRes) workers reqTimeout oldNodeMapState
  putMVar workers (ChanWorkers writerThread readerThread connRefresherThread timeoutHandlerThread)
  pure $ RedisLiteChannel inChanReq workers

bufferWriter :: Chan.OutChan ChanRequest -> NodeMapState -> (Maybe NodeID -> NodeMapState -> IO ()) -> Chan.InChan (NodeConnection, Int, MVar (Either RedisException [Reply])) -> IO ()
bufferWriter outChanReq nodeMapState nodeMapRefresher inChanRes =
  forever $ do
    (ChanRequest request nodeID reponseMVar) <- Chan.readChan outChanReq
    (NodeMap nodeMap) <- CL.hasLocked $ readMVar nodeMapState
    case HM.lookup nodeID nodeMap of
      Nothing -> putMVar reponseMVar (Left MissingNode)
      Just (nodeConn@(NodeConnection ctx _ _ _)) -> do
        res <- tryWithUncaughtExc $ do
          mapM_ (CC.send ctx) request
          CC.flush ctx
        case res of
          Left err -> do
            void $ tryWithUncaughtExc $ CC.disconnect ctx
            foreverRetry (nodeMapRefresher (Just nodeID) nodeMapState) "ConsumerWriter NodeMapRefreshErr"
            putMVar reponseMVar (Left (ConnectionFailure err))
          Right _ ->
            Chan.writeChan inChanRes (nodeConn, length request, reponseMVar)

bufferReader :: Chan.OutChan (NodeConnection, Int, MVar (Either RedisException [Reply])) -> IO ()
bufferReader outChan =
  forever $ do
    (nodeConn@(NodeConnection ctx _ _ _), noOfResp, mVar) <- Chan.readChan outChan
    eResp <- tryWithUncaughtExc $
      replicateM noOfResp (recvNode nodeConn)
    case eResp of
      Left err -> do
        void $ tryWithUncaughtExc $ CC.disconnect ctx
        tryPutMVar mVar (Left (ConnectionFailure err))
      Right val -> do
        tryPutMVar mVar (Right val)
  where
    recvNode :: NodeConnection -> IO Reply
    recvNode (NodeConnection ctx lastRecvRef _ _) = do
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

-- This worker helps in tackling timeout cases and gracefully closing old connections
bufferTimeoutHandler ::
    Chan.OutChan (NodeConnection, Int, MVar (Either RedisException [Reply]))
  -> NodeMapState
  -> (Maybe NodeID -> NodeMapState -> IO ()) -- nodeMap refresher
  -> IO ThreadId -- fork bufferReader
  -> MVar ChanWorkers
  -> Int -- requestTimeout (in microsec)
  -> NodeMapState -- old nodeConnections
  -> IO ()
bufferTimeoutHandler outChan nodeMapState nodeMapRefresher forkReader workers reqTimeout oldNodeMapState =
  forever $ do
    (nodeConn@(NodeConnection _ _ timeSpec nodeId), _, replyMVar) <- Chan.readChan outChan
    raceRes <- race (threadDelay reqTimeout) (readMVar replyMVar)
    -- raceRes <- maybe (race (threadDelay reqTimeout) (readMVar replyMVar)) (pure . Right) =<< tryReadMVar replyMVar
    case raceRes of
      Left _ -> do
        (ChanWorkers _ readerId _ _) <- CL.hasLocked $ readMVar workers
        -- we kill reader thread before nodeMapRefresher so that its is able to close the faulty connection
        -- putStrLn "Before Kill Thread"
        killThread readerId
        -- putStrLn "After Kill Thread"
        nodeMapRefresher (Just nodeId) nodeMapState
        void $ tryWithUncaughtExc $ disconnect nodeConn -- its possible that the faulty connection is not present in current NodeMapState
        void $ modifyMVarNoBlocking workers $ \ChanWorkers{..} -> do
          newReaderId <- forkReader
          pure $ ChanWorkers{reader = newReaderId, ..}
        void $ tryPutMVar replyMVar (Left $ Timeout "Request Timeout")
      Right _ -> do
        (NodeMap nodeMap) <- CL.hasLocked $ readMVar nodeMapState
        modifyMVar_ oldNodeMapState $ \(NodeMap oldNodeMap) ->
          case (HM.lookup nodeId nodeMap) of
            Just (NodeConnection _ _ currTimeSpec _) ->
              if currTimeSpec > timeSpec
                then do
                  -- we store the old connections in oldNodeMapState
                  case (HM.lookup nodeId oldNodeMap) of
                    Just (NodeConnection ctx _ oldTimeSpec _) | oldTimeSpec < timeSpec ->
                      void $ tryWithUncaughtExc $ CC.disconnect ctx
                    _ -> pure ()
                  pure $ NodeMap (HM.insert nodeId nodeConn oldNodeMap)
                else do
                  -- currTimeSpec == timeSpec means bufferReader no longer needs to read responses from old connection, thus it can be closed
                  whenJust (HM.lookup nodeId oldNodeMap) (void . tryWithUncaughtExc . disconnect)
                  pure $ NodeMap (HM.delete nodeId oldNodeMap)
            Nothing -> do
              -- because current NodeMap doesn't have this NodeID we won't know when to close it.
              -- so we let connectionRefresher worker close this when "keepAlive" time has passed
              case (HM.lookup nodeId oldNodeMap) of
                Just (NodeConnection ctx _ oldTimeSpec _) | oldTimeSpec < timeSpec ->
                  void $ tryWithUncaughtExc $ CC.disconnect ctx
                _ -> pure ()
              pure $ NodeMap (HM.insert nodeId nodeConn oldNodeMap)

-- We refresh nodeMap at intervals of "keepAliveTime" but don't close the old connections (its handled by timeoutHandler thread)
-- ideally when this worker reads oldNodeMapState it'll be empty
-- but for cases where a node is removed from cluster or no new requests were made after nodeMapRefresh
-- its possible that the connection won't be closed by timeoutHandler thread, so we rely on below worker for that
connectionRefresher :: IO () -> Int -> NodeMapState -> IO ()
connectionRefresher nodeMapRefresher keepAliveTime oldNodeMapState = do
  forever $ do
    threadDelay keepAliveTime
    currTimeSpec <- getTime Monotonic
    modifyMVar_ oldNodeMapState $ \(NodeMap oldNodeMap) ->
      NodeMap <$> foldM (\hm nodeConn@(NodeConnection _ _ timeSpec nodeId) -> do
        let diff = currTimeSpec - timeSpec
            diffinUs = ((SC.sec diff) * 1000000) + ((SC.nsec diff) `div` 1000)
        if diffinUs >= (fromIntegral keepAliveTime)
          then do
            void $ tryWithUncaughtExc $ disconnect nodeConn
            pure hm
          else pure $ HM.insert nodeId nodeConn hm 
        ) HM.empty (HM.elems oldNodeMap)
    void $ tryWithUncaughtExc $ nodeMapRefresher

foreverRetry :: IO a -> String -> IO a
foreverRetry action errLabel = do
  eRes <- tryWithUncaughtExc action
  case eRes of
    Left err -> do
      putStrLn $ errLabel <> " : " <> (show err)
      threadDelay 1000000
      foreverRetry action errLabel
    Right res -> pure res

clusterRequestHandler :: ClusterEnv -> [[B.ByteString]] -> Int -> IO Reply
clusterRequestHandler env@(ClusterEnv (ClusterConnection (RedisLiteChannel inChan _) nodeMapState shardMapVar infoMap connectInfo) refreshShardMap refreshNodeMap) requests retryCount = do
  shardMap <- CL.hasLocked $ readMVar shardMapVar
  nodeId <- nodeIdForCommand infoMap shardMap requests
  mVar <- newEmptyMVar
  -- putStrLn "Sending Request"
  Chan.writeChan inChan (ChanRequest (renderRequest <$> requests) nodeId mVar)
  resp <- CL.hasLocked $ readMVar mVar
  case resp of
    Right rply ->
      case rply of
        ((firstJust moveRedirection) -> Just (slot, host, port)) ->
          case retryCount of
            0 -> pure (L.last rply)
            _ -> do
              (ShardMap shardMapNew) <- CL.hasLocked $ readMVar shardMapVar
              case (IntMap.lookup slot shardMapNew) of
                Just (Shard (Node _ _ nHost nPort) _) | ((nHost == host) && (nPort == port)) -> pure ()
                _ -> void $ refreshShardMap connectInfo shardMapVar
              clusterRequestHandler env requests (retryCount-1)  
        ((firstJust askingRedirection) -> Just (host, port)) -> do
          shardMapNew <- CL.hasLocked $ readMVar shardMapVar
          let maybeAskNode = nodeWithHostAndPort shardMapNew host port
          case maybeAskNode of
            Just _ -> clusterRequestHandler env (["ASKING"] : requests) (retryCount-1)
            Nothing -> case retryCount of
              0 -> throwIO $ CL.MissingNodeException (head requests)
              _ -> do
                void $ refreshShardMap connectInfo shardMapVar
                clusterRequestHandler env requests (retryCount-1)
        _ -> pure (L.last rply)
    Left err | (retryCount <= 0) -> throwIO err
    Left err ->
      -- putStrLn $ "Got Error: " <> show err
      case err of
        Timeout _ -> throwIO err
        MissingNode -> do
          shardMapNew <- CL.hasLocked $ readMVar shardMapVar
          refreshNodeMap shardMapNew connectInfo (Just nodeId) nodeMapState
          clusterRequestHandler env requests (retryCount-1)
        _ ->
          clusterRequestHandler env requests (retryCount-1)

nodeIdForCommand :: CMD.InfoMap -> ShardMap -> [[B.ByteString]] -> IO NodeID
nodeIdForCommand infoMap (ShardMap shardMap) requests = do
  keys <- concat <$> mapM (CL.requestKeys infoMap) requests
  hashSlot <- CL.hashSlotForKeys (CL.CrossSlotException requests) keys
  case IntMap.lookup (fromEnum hashSlot) shardMap of
    Nothing -> throwIO $ CL.MissingNodeException (head requests)
    Just (Shard (Node nodeId _ _ _) _) -> return nodeId

nodeWithHostAndPort :: ShardMap -> String -> Int -> Maybe Node
nodeWithHostAndPort shardMap host port =
  L.find (\(Node _ _ nHost nPort) -> (nHost == host) && (nPort == port)) (getNodes shardMap)

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

disconnect :: NodeConnection -> IO ()
disconnect (NodeConnection ctx _ _ _) = CC.disconnect ctx

disconnectMap :: NodeMap -> IO ()
disconnectMap (NodeMap nodeMap) = mapM_ (\(NodeConnection ctx _ _ _) -> CC.disconnect ctx) $ HM.elems nodeMap

getNodes :: ShardMap -> [Node]
getNodes (ShardMap shardMap) =
  let shards = IntMap.foldl (\acc shard@(Shard (Node nodeId _ _ _) _) ->
        case (L.find (\(Shard (Node nId _ _ _) _) -> nId == nodeId) acc) of
          Just _ -> acc
          Nothing -> shard : acc
        ) [] shardMap
  in concat $ map (\(Shard master slaves) -> master : slaves) shards

modifyMVarNoBlocking :: MVar a -> (a -> IO a) -> IO a
modifyMVarNoBlocking mVar io = do
  aM <- tryTakeMVar mVar
  case aM of
    Nothing -> readMVar mVar
    Just a -> mask $ \restore -> do
      a' <- restore (io a >>= evaluate) `onException` putMVar mVar a
      putMVar mVar a'
      pure a'

tryWithUncaughtExc :: IO a -> IO (Either SomeException a)
tryWithUncaughtExc io = do
  r <- try io
  case r of
    Right _ -> pure r
    Left err ->
      case fromException err of
        Just ThreadKilled -> throwIO ThreadKilled
        _ -> pure r