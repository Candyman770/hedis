{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE RecordWildCards #-}

module Database.Redis.Lite.NonCluster where

import qualified Data.ByteString as B
import qualified Control.Concurrent.Chan.Unagi as Chan
import qualified Data.Time as Time
import qualified Data.List as L
import Control.Concurrent.MVar (MVar, newMVar, readMVar, putMVar, newEmptyMVar, modifyMVar_, tryPutMVar)
import Control.Exception (throwIO)
import Control.Concurrent (ThreadId, forkIO, threadDelay, killThread)
import Control.Monad (replicateM, void, forever)
import Control.Monad.Extra (whenJust)
import Control.Concurrent.Async (race)
-- import Data.Maybe (fromMaybe)
import System.Clock as SC (TimeSpec(..), Clock(..), getTime)

import Database.Redis.Lite.Types
import Database.Redis.Protocol (Reply(..), renderRequest)
import qualified Database.Redis.Lite.Cluster as Cluster
import qualified Database.Redis.Cluster as CL
import qualified Database.Redis.ConnectionContext as CC

initiateWorkers :: ConnectionState -> (Bool -> ConnectionState -> IO ()) -> Int -> Time.NominalDiffTime -> Time.NominalDiffTime -> IO RedisLiteChannel
initiateWorkers connState connRefresher batchSize keepAlive requestTime = do
  (inChanReq, outChanReq) <- Chan.newChan
  (inChanRes, outChanRes) <- Chan.newChan
  dupOutChanRes <- Chan.dupChan inChanRes
  let keepAliveTime = round . (1000000 *) $ keepAlive
      reqTimeout = round . (1000000 *) $ requestTime
  writerThread <- forkIO $ bufferWriter outChanReq [] connState (connRefresher True) batchSize inChanRes
  readerThread <- forkIO $ Cluster.bufferReader outChanRes
  oldConnState <- newMVar Nothing
  workers <- newEmptyMVar
  connRefresherThread <- forkIO $ connectionRefresher (connRefresher False connState) keepAliveTime oldConnState
  timeoutHandlerThread <- forkIO $ bufferTimeoutHandler dupOutChanRes connState connRefresher (forkIO $ Cluster.bufferReader outChanRes) workers reqTimeout oldConnState
  putMVar workers (ChanWorkers writerThread readerThread connRefresherThread timeoutHandlerThread)
  pure $ RedisLiteChannel inChanReq workers

bufferWriter :: Chan.OutChan ChanRequest -> [(Chan.Element ChanRequest, IO ChanRequest)] -> ConnectionState -> (ConnectionState -> IO ()) -> Int -> Chan.InChan (NodeConnection, Int, MVar (Either RedisException [Reply])) -> IO ()
bufferWriter outChanReq currReqList connState connRefresher batchSize inChanRes = do
  ((_, blockingRead) : newReqList) <- (currReqList ++) <$> replicateM (batchSize - (length currReqList)) (Chan.tryReadChan outChanReq)
  fstElement <- blockingRead
  nodeConn@(NodeConnection ctx _ _ _) <- CL.hasLocked $ readMVar connState
  (readyList, remReqList) <- spanM newReqList []
  res <- Cluster.tryWithUncaughtExc $ do
    mapM_ (\(ChanRequest requests _ _) -> mapM_ (CC.send ctx) requests) (fstElement : readyList)
    CC.flush ctx
  case res of
    Left err -> do
      void $ Cluster.tryWithUncaughtExc $ CC.disconnect ctx
      Cluster.foreverRetry (connRefresher connState) "ConsumerWriter ConnRefreshError"
      mapM_ (\(ChanRequest _ _ mVar) -> putMVar mVar (Left (ConnectionFailure err))) (fstElement : readyList)
      bufferWriter outChanReq remReqList connState connRefresher batchSize inChanRes
    Right _ -> do
      mapM_ (\(ChanRequest requests _ mVar) -> Chan.writeChan inChanRes (nodeConn, length requests, mVar)) (fstElement : readyList)
      bufferWriter outChanReq remReqList connState connRefresher batchSize inChanRes
  where
    spanM :: [(Chan.Element ChanRequest, IO ChanRequest)] -> [ChanRequest] -> IO ([ChanRequest], [(Chan.Element ChanRequest, IO ChanRequest)])
    spanM [] xs = pure (reverse xs, [])
    spanM rq@((element, _) : rest) xs = do
      reqM <- Chan.tryRead element
      case reqM of
        Nothing -> pure (reverse xs, rq)
        Just req -> spanM rest (req : xs)

bufferTimeoutHandler ::
  Chan.OutChan (NodeConnection, Int, MVar (Either RedisException [Reply]))
  -> ConnectionState
  -> (Bool -> ConnectionState -> IO ()) -- nodeMap refresher
  -> IO ThreadId -- fork bufferReader
  -> MVar ChanWorkers
  -> Int -- requestTimeout (in microsec)
  -> MVar (Maybe NodeConnection) -- old nodeConnections
  -> IO ()
bufferTimeoutHandler outChan connState connRefresher forkReader workers reqTimeout oldConnState = do
  (nodeConn@(NodeConnection _ _ timeSpec _), _, replyMVar) <- Chan.readChan outChan
  raceRes <- race (threadDelay reqTimeout) (readMVar replyMVar)
  -- raceRes <- maybe (race (threadDelay reqTimeout) (readMVar replyMVar)) (pure . Right) =<< tryReadMVar replyMVar
  case raceRes of
    Left _ -> do
      (ChanWorkers _ readerId _ _) <- CL.hasLocked $ readMVar workers
      -- we kill reader thread before connRefresher so that its is able to close the faulty connection
      -- putStrLn "Before Kill Thread"
      killThread readerId
      -- putStrLn "After Kill Thread"
      connRefresher True connState
      void $ Cluster.modifyMVarNoBlocking workers $ \ChanWorkers{..} -> do
        newReaderId <- forkReader
        pure $ ChanWorkers{reader = newReaderId, ..}
      void $ tryPutMVar replyMVar (Left $ Timeout "Request Timeout")
    Right _ -> do
      (NodeConnection _ _ currTimeSpec _) <- CL.hasLocked $ readMVar connState
      modifyMVar_ oldConnState $ \oldConn ->
        if currTimeSpec > timeSpec
          then do
            case oldConn of
              Just (NodeConnection ctx _ oldTimeSpec _) | oldTimeSpec < timeSpec ->
                void $ Cluster.tryWithUncaughtExc $ CC.disconnect ctx
              _ -> pure ()
            pure $ Just nodeConn
          else do
            whenJust oldConn (void . Cluster.tryWithUncaughtExc . Cluster.disconnect)
            pure Nothing

connectionRefresher :: IO () -> Int -> MVar (Maybe NodeConnection) -> IO ()
connectionRefresher connRefresher keepAliveTime oldConnState = do
  forever $ do
    threadDelay keepAliveTime
    currTimeSpec <- getTime Monotonic
    modifyMVar_ oldConnState $ \oldConn ->
      case oldConn of
        Just (NodeConnection ctx _ timeSpec _) -> do
          let diff = currTimeSpec - timeSpec
              diffInUs = ((SC.sec diff) * 1000000) + ((SC.nsec diff) `div` 1000)
          if diffInUs >= (fromIntegral keepAliveTime)
            then do
              void $ Cluster.tryWithUncaughtExc $ CC.disconnect ctx
              pure Nothing
            else pure oldConn
        Nothing -> pure Nothing
    void $ Cluster.tryWithUncaughtExc $ connRefresher

requestHandler :: NonClusterEnv -> [[B.ByteString]] -> Int -> IO Reply
requestHandler env@(NonClusterEnv (NonClusterConnection (RedisLiteChannel inChan _) connState connectInfo) refreshConn) requests retryCount = do
  mVar <- newEmptyMVar
  Chan.writeChan inChan (ChanRequest (renderRequest <$> requests) mempty mVar)
  resp <- CL.hasLocked $ readMVar mVar
  case resp of
    Right reply -> pure (L.last reply)
    Left (Timeout errStr) -> throwIO (Timeout errStr)
    Left err | (retryCount <= 0) -> throwIO err
    Left _ -> do
      refreshConn connectInfo True connState
      requestHandler env requests (retryCount-1)