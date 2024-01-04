{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE FlexibleInstances #-}

module Database.Redis.Lite.Core where

import Data.ByteString (ByteString)
import Control.Monad.Reader (ask, liftIO)

import Database.Redis.Lite.Types
import Database.Redis.Lite.Cluster as Cluster
import Database.Redis.Lite.NonCluster as NonCluster
import Database.Redis.Types (RedisResult(..))
import Database.Redis.Protocol (Reply)

class RedisCtx m f | m -> f where
  sendRequest :: RedisResult a => [ByteString] -> m (f a)

instance RedisCtx Redis (Either Reply) where
  sendRequest req = do
    env <- ask
    reply <-
      liftIO $ case env of
        ClusteredEnv clusterEnv ->
          Cluster.clusterRequestHandler clusterEnv [req] 2
        NonClusteredEnv nonClusterEnv ->
          NonCluster.requestHandler nonClusterEnv [req] 2
    return $ decode reply

sendRequestMulti :: [[ByteString]] -> Redis Reply
sendRequestMulti reqList = do
  env <- ask
  liftIO $ case env of
    ClusteredEnv clusterEnv ->
      Cluster.clusterRequestHandler clusterEnv reqList 2
    NonClusteredEnv nonClusterEnv ->
      NonCluster.requestHandler nonClusterEnv reqList 2