{-# LANGUAGE CPP #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE RecordWildCards #-}

module Database.Redis.Core.Internal where
#if __GLASGOW_HASKELL__ > 711 && __GLASGOW_HASKELL__ < 808
import Control.Monad.Fail (MonadFail)
#endif
import Control.Monad.Reader
import Data.IORef
import Database.Redis.Protocol
import Control.Concurrent.MVar (MVar)
import Control.Monad.IO.Unlift (MonadUnliftIO)
import qualified Database.Redis.ProtocolPipelining as PP
import qualified Database.Redis.Cluster as Cluster

-- |Context for normal command execution, outside of transactions. Use
--  'runRedis' to run actions of this type.
--
--  In this context, each result is wrapped in an 'Either' to account for the
--  possibility of Redis returning an 'Error' reply.
newtype Redis a =
  Redis (ReaderT RedisEnv IO a)
  deriving (Monad, MonadIO, Functor, Applicative, MonadUnliftIO)
#if __GLASGOW_HASKELL__ > 711
deriving instance MonadFail Redis
#endif
data RedisEnv
    = NonClusteredEnv { envConn :: PP.Connection, nonClusteredLastReply :: IORef Reply }
    | ClusteredEnv
        { refreshAction :: Maybe Cluster.NodeConnection -> IO (Cluster.ShardMap,Cluster.NodeConnectionMap)
        , connection :: Cluster.Connection
        , clusteredLastReply :: IORef Reply
        , pipeline :: MVar Cluster.Pipeline
        }

envLastReply :: RedisEnv -> IORef Reply
envLastReply NonClusteredEnv{..} = nonClusteredLastReply
envLastReply ClusteredEnv{..} = clusteredLastReply
