{-# LANGUAGE DuplicateRecordFields #-}

module Database.Redis.Lite.Types where

import qualified Data.Time as Time
import qualified Data.ByteString as B
import qualified Data.IntMap.Strict as IntMap
import qualified Data.HashMap.Strict as HM
import qualified Control.Concurrent.Chan.Unagi as Chan
import qualified Network.Socket as NS
import Data.IORef (IORef)
import Network.TLS (ClientParams)
import Control.Concurrent (ThreadId)
import Control.Concurrent.MVar (MVar)
import Control.Monad.Reader (ReaderT)
import Control.Exception (SomeException, Exception)
import Data.Typeable (Typeable)

import qualified Database.Redis.ConnectionContext as CC
import qualified Database.Redis.Cluster.Command as CMD
import Database.Redis.Protocol(Reply(..))

data ConnectInfo = ConnInfo
    { connectHost           :: NS.HostName
    , connectPort           :: CC.PortID
    , connectAuth           :: Maybe B.ByteString
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
    , requestTimeout        :: Maybe Time.NominalDiffTime
    , pipelineBatchSize     :: Maybe Int
    -- max number of requests that can be pipelined. Used in non-cluster mode
    , connectKeepAlive      :: Time.NominalDiffTime
    -- Amount of time for which a connection is kept open.
    } deriving Show




-- | A connection to a single node in the cluster, similar to 'ProtocolPipelining.Connection'
data NodeConnection = NodeConnection CC.ConnectionContext (IORef (Maybe B.ByteString)) NodeID

instance Show NodeConnection where
    show (NodeConnection _ _ id1) = "nodeId: " <> show id1

instance Eq NodeConnection where
    (NodeConnection _ _ id1) == (NodeConnection _ _ id2) = id1 == id2

instance Ord NodeConnection where
    compare (NodeConnection _ _ id1) (NodeConnection _ _ id2) = compare id1 id2

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

-- A 'shard' is a master node and 0 or more slaves
data Shard = Shard MasterNode [SlaveNode] deriving (Show, Eq, Ord)

-- A map from hashslot to shards
newtype ShardMap = ShardMap (IntMap.IntMap Shard) deriving (Show)




data ClusterConnection = ClusterConnection ClusterChannel NodeMapState (MVar ShardMap) CMD.InfoMap ConnectInfo

data NonClusterConnection = NonClusterConnection NonClusterChannel ConnectionState ConnectInfo

newtype NodeMap = NodeMap (HM.HashMap NodeID NodeConnection)

type NodeMapState = MVar NodeMap

type ConnectionState = MVar NodeConnection

data ClusterChannel = ClusterChannel (Chan.InChan ClusterChanRequest) ChanWorkers

data NonClusterChannel = NonClusterChannel (Chan.InChan ChanRequest) ChanWorkers

data RedisException = MissingNode | ConnectionFailure SomeException deriving (Show, Typeable)
instance Exception RedisException

data ChanWorkers =
  ChanWorkers
    { writer :: ThreadId
    , reader :: ThreadId
    , connRefresher :: ThreadId
    }

data ClusterChanRequest =
  ClusterChanRequest
  { request :: [B.ByteString]
  , nodeID :: B.ByteString
  , responseMVar :: MVar (Either RedisException Reply)
  }

data ChanRequest =
  ChanRequest
  { request :: [B.ByteString]
  , reponseMVar :: MVar (Either RedisException Reply)
  }

data RedisEnv
  = ClusteredEnv ClusterEnv
  | NonClusteredEnv NonClusterEnv

data ClusterEnv =
  ClusterEnv
    { connection :: ClusterConnection
    , refreshShardMap :: (ConnectInfo -> MVar (ShardMap) -> IO ShardMap)
    , refreshNodeMap :: (ShardMap -> ConnectInfo -> Maybe NodeID -> NodeMapState -> IO ())
    }

data NonClusterEnv =
  NonClusterEnv
    { connection :: NonClusterConnection
    , refreshConnection :: (ConnectInfo -> ConnectionState -> IO ())
    }

data Connection
  = Cluster ClusterConnection
  | NonCluster NonClusterConnection

type Redis = ReaderT RedisEnv IO