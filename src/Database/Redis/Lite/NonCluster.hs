{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module Database.Redis.Lite.NonCluster where

import qualified Data.ByteString as B
import qualified Control.Concurrent.Chan.Unagi as Chan
import Control.Concurrent.MVar (MVar, readMVar, putMVar, newEmptyMVar)
import Control.Exception (SomeException, throwIO, try)
import Control.Concurrent (forkIO, threadDelay)
import Control.Monad (replicateM, void)
import Control.Concurrent.Async (race)
import Data.Maybe (fromMaybe)

import Database.Redis.Lite.Types
import Database.Redis.Protocol (Reply(..), renderRequest)
import qualified Database.Redis.Lite.Cluster as Cluster
import qualified Database.Redis.Cluster as CL
import qualified Database.Redis.ConnectionContext as CC

initiateWorkers :: ConnectionState -> (ConnectionState -> IO ()) -> Int -> IO NonClusterChannel
initiateWorkers connState connRefresher batchSize = do
  (inChanReq, outChanReq) <- Chan.newChan
  (inChanRes, outChanRes) <- Chan.newChan
  writerThread <- forkIO $ bufferWriter outChanReq [] connState connRefresher batchSize inChanRes
  readerThread <- forkIO $ Cluster.bufferReader outChanRes
  pure $ NonClusterChannel inChanReq connState (ChanWorkers writerThread readerThread)

bufferWriter :: Chan.OutChan ChanRequest -> [(Chan.Element ChanRequest, IO ChanRequest)] -> ConnectionState -> (ConnectionState -> IO ()) -> Int -> Chan.InChan (NodeConnection, Int, MVar (Either RedisException Reply)) -> IO ()
bufferWriter outChanReq currReqList connState connRefresher batchSize inChanRes = do
  ((_, blockingRead) : newReqList) <- (currReqList ++) <$> replicateM (batchSize - (length currReqList)) (Chan.tryReadChan outChanReq)
  fstElement <- blockingRead
  nodeConn@(NodeConnection ctx _ _) <- CL.hasLocked $ readMVar connState
  (readyList, remReqList) <- spanM newReqList []
  res <- try $ do
    mapM_ (\(ChanRequest requests _) -> mapM_ (CC.send ctx) requests) (fstElement : readyList)
    CC.flush ctx
  case res of
    Left (err :: SomeException) -> do
      void $ try @SomeException $ CC.disconnect ctx
      Cluster.foreverRetry (connRefresher connState) "ConsumerWriter ConnRefreshError"
      mapM_ (\(ChanRequest _ mVar) -> putMVar mVar (Left (ConnectionFailure err))) (fstElement : readyList)
      bufferWriter outChanReq remReqList connState connRefresher batchSize inChanRes
    Right _ -> do
      mapM_ (\(ChanRequest requests mVar) -> Chan.writeChan inChanRes (nodeConn, length requests, mVar)) (fstElement : readyList)
      bufferWriter outChanReq remReqList connState connRefresher batchSize inChanRes
  where
    spanM :: [(Chan.Element ChanRequest, IO ChanRequest)] -> [ChanRequest] -> IO ([ChanRequest], [(Chan.Element ChanRequest, IO ChanRequest)])
    spanM [] xs = pure (reverse xs, [])
    spanM rq@((element, _) : rest) xs = do
      reqM <- Chan.tryRead element
      case reqM of
        Nothing -> pure (reverse xs, rq)
        Just req -> spanM rest (req : xs)

requestHandler :: NonClusterEnv -> [[B.ByteString]] -> Int -> IO Reply
requestHandler env@(NonClusterEnv (NonClusterConnection (NonClusterChannel inChan connState _) connectInfo) refreshConn) requests retryCount = do
  mVar <- newEmptyMVar
  Chan.writeChan inChan (ChanRequest (renderRequest <$> requests) mVar)
  let reqTimeout = fromMaybe 1000000 $ round . (1000000 *) <$> (requestTimeout connectInfo)
  resp <- race (threadDelay reqTimeout) (readMVar mVar)
  case resp of
    Right eReply ->
      case eReply of
        Right reply -> pure reply
        Left err | (retryCount <= 0) -> throwIO err
        Left _ -> do
          refreshConn connectInfo connState
          requestHandler env requests (retryCount-1)
    Left _ ->
      if retryCount > 0
        then do
          refreshConn connectInfo connState
          requestHandler env requests (retryCount-1)
        else throwIO $ CL.TimeoutException "Reply MVar Wait Timeout"