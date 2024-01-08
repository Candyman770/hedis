{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE TypeApplications #-}

module Database.Redis.Lite.Cluster where

import qualified Data.List as L
import qualified Data.HashMap.Strict as HM
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as Char8
import qualified Data.IntMap.Strict as IntMap
import qualified Control.Concurrent.Chan.Unagi as Chan
import qualified Data.Time as Time
import Control.Monad (replicateM_, forever, void)
import Control.Concurrent.Async (race)
import Control.Concurrent (forkIO, threadDelay)
import Control.Concurrent.MVar (MVar, readMVar, newEmptyMVar, putMVar, tryTakeMVar)
import Data.IORef (readIORef, writeIORef)
import Control.Exception (SomeException, throwIO, try, mask, evaluate, onException)
import Data.Maybe (fromMaybe)
import Scanner

import qualified Database.Redis.Cluster as CL
import qualified Database.Redis.Cluster.Command as CMD
import qualified Database.Redis.ConnectionContext as CC
import Database.Redis.Protocol (Reply(..), renderRequest, reply)
import Database.Redis.Lite.Types

initiateClusterWorkers :: NodeMapState -> (Maybe NodeID -> NodeMapState -> IO ()) -> Time.NominalDiffTime -> IO ClusterChannel
initiateClusterWorkers nodeMapState nodeMapRefresher keepAlive = do
  (inChanReq, outChanReq) <- Chan.newChan
  (inChanRes, outChanRes) <- Chan.newChan
  let keepAliveTime = round . (1000000 *) $ keepAlive 
  writerThread <- forkIO $ bufferWriter outChanReq nodeMapState nodeMapRefresher inChanRes
  readerThread <- forkIO $ bufferReader outChanRes
  connRefresherThread <- forkIO $
    forever $ do
      threadDelay keepAliveTime
      void $ try @SomeException $ nodeMapRefresher Nothing nodeMapState
  pure $ ClusterChannel inChanReq (ChanWorkers writerThread readerThread connRefresherThread)

bufferWriter :: Chan.OutChan ClusterChanRequest -> NodeMapState -> (Maybe NodeID -> NodeMapState -> IO ()) -> Chan.InChan (NodeConnection, Int, MVar (Either RedisException Reply)) -> IO ()
bufferWriter outChanReq nodeMapState nodeMapRefresher inChanRes =
  forever $ do
    (ClusterChanRequest request nodeID reponseMVar) <- Chan.readChan outChanReq
    (NodeMap nodeMap) <- CL.hasLocked $ readMVar nodeMapState
    case HM.lookup nodeID nodeMap of
      Nothing -> putMVar reponseMVar (Left MissingNode)
      Just (nodeConn@(NodeConnection ctx _ _)) -> do
        res <- try $ do
          mapM_ (CC.send ctx) request
          CC.flush ctx
        case res of
          Left (err :: SomeException) -> do
            void $ try @SomeException $ CC.disconnect ctx
            foreverRetry (nodeMapRefresher (Just nodeID) nodeMapState) "ConsumerWriter NodeMapRefreshErr"
            putMVar reponseMVar (Left (ConnectionFailure err))
          Right _ ->
            Chan.writeChan inChanRes (nodeConn, length request, reponseMVar)

bufferReader :: Chan.OutChan (NodeConnection, Int, MVar (Either RedisException Reply)) -> IO ()
bufferReader outChan =
  forever $ do
    (nodeConn@(NodeConnection ctx _ _), noOfResp, mVar) <- Chan.readChan outChan
    eResp <- try $ do
      replicateM_ (noOfResp - 1) (recvNode nodeConn)
      recvNode nodeConn
    case eResp of
      Left (err :: SomeException) -> do
        void $ try @SomeException $ CC.disconnect ctx
        putMVar mVar (Left (ConnectionFailure err))
      Right val -> do
        putMVar mVar (Right val)
  where
    recvNode :: NodeConnection -> IO Reply
    recvNode (NodeConnection ctx lastRecvRef _) = do
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
foreverRetry action errLabel = do
  eRes <- try action
  case eRes of
    Left (err :: SomeException) -> do
      putStrLn $ errLabel <> " : " <> (show err)
      threadDelay 1000000
      foreverRetry action errLabel
    Right res -> pure res

clusterRequestHandler :: ClusterEnv -> [[B.ByteString]] -> Int -> IO Reply
clusterRequestHandler env@(ClusterEnv (ClusterConnection (ClusterChannel inChan _) nodeMapState shardMapVar infoMap connectInfo) refreshShardMap refreshNodeMap) requests retryCount = do
  shardMap <- CL.hasLocked $ readMVar shardMapVar
  nodeId <- nodeIdForCommand infoMap shardMap requests
  mVar <- newEmptyMVar
  Chan.writeChan inChan (ClusterChanRequest (renderRequest <$> requests) nodeId mVar)
  let reqTimeout = fromMaybe 1000000 $ round . (1000000 *) <$> (requestTimeout connectInfo)
  resp <- race (threadDelay reqTimeout) (readMVar mVar)
  case resp of
    Right eReply ->
      case eReply of
        Right rply ->
          case rply of
            (moveRedirection -> Just (slot, host, port)) ->
              case retryCount of
                0 -> pure rply
                _ -> do
                  (ShardMap shardMapNew) <- CL.hasLocked $ readMVar shardMapVar
                  case (IntMap.lookup slot shardMapNew) of
                    Just (Shard (Node _ _ nHost nPort) _) | ((nHost == host) && (nPort == port)) -> pure ()
                    _ -> void $ refreshShardMap connectInfo shardMapVar
                  clusterRequestHandler env requests (retryCount-1)  
            (askingRedirection -> Just (host, port)) -> do
              shardMapNew <- CL.hasLocked $ readMVar shardMapVar
              let maybeAskNode = nodeWithHostAndPort shardMapNew host port
              case maybeAskNode of
                Just _ -> clusterRequestHandler env (["ASKING"] : requests) (retryCount-1)
                Nothing -> case retryCount of
                  0 -> throwIO $ CL.MissingNodeException (head requests)
                  _ -> do
                    void $ refreshShardMap connectInfo shardMapVar
                    clusterRequestHandler env requests (retryCount-1)
            _ -> pure rply
        Left err | (retryCount <= 0) -> throwIO err
        Left err ->
          case err of
            MissingNode -> do
              shardMapNew <- CL.hasLocked $ readMVar shardMapVar
              refreshNodeMap shardMapNew connectInfo (Just nodeId) nodeMapState
              clusterRequestHandler env requests (retryCount-1)
            _ ->
              clusterRequestHandler env requests (retryCount-1)
    Left _ ->
      if retryCount > 0
        then do
          shardMapNew <- CL.hasLocked $ readMVar shardMapVar
          refreshNodeMap shardMapNew connectInfo (Just nodeId) nodeMapState
          clusterRequestHandler env requests (retryCount-1)
        else throwIO $ CL.TimeoutException "Reply MVar Wait Timeout"

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
disconnect (NodeConnection ctx _ _) = CC.disconnect ctx

disconnectMap :: NodeMap -> IO ()
disconnectMap (NodeMap nodeMap) = mapM_ (\(NodeConnection ctx _ _) -> CC.disconnect ctx) $ HM.elems nodeMap

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