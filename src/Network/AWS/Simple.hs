{-# LANGUAGE OverloadedStrings #-}

module Network.AWS.Simple
  ( connectAWS,
    fromEnv,
    AWSHandle,
    AWS.Region (..),

    -- * Logging
    AWS.LogLevel (..),
    LogFun,

    -- * S3
    AWSFileReadability (..),
    s3Upload,
    s3Download,
    s3Delete,
    s3CopyInBucket,
    s3MetaData,

    -- * SQS
    sqsGetQueue,
    AWSQueue,
    sqsSendMessage,
    sqsGetMessage,
    GetMessageCfg (..),
    SqsMessage (..),
    MessageHandle,
    sqsAckMessage,
    sqsChangeMessageTimeout,
  )
where

import qualified Amazonka as AWS
import qualified Amazonka.S3 as S3
import qualified Amazonka.S3.GetObject as S3
import qualified Amazonka.S3.HeadObject as S3
import qualified Amazonka.S3.PutObject as S3
import qualified Amazonka.SQS as SQS
import qualified Amazonka.SQS.GetQueueUrl as SQS
import qualified Amazonka.SQS.ReceiveMessage as SQS
import qualified Amazonka.SQS.Types as SQS
import Control.Lens
import Control.Monad
import Control.Monad.Trans.Resource
import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder as BSB
import qualified Data.ByteString.Lazy as BSL
import Data.Conduit
import Data.HashMap.Strict (HashMap)
import Data.Int
import Data.Maybe
import qualified Data.Text as T
import Data.Time.TimeSpan

data AWSHandle = AWSHandle
  { a_cfg :: !AWS.Env
  }

type LogFun = AWS.LogLevel -> BS.ByteString -> IO ()

fromEnv :: AWS.Env -> AWSHandle
fromEnv = AWSHandle

connectAWS :: AWS.Region -> LogFun -> IO AWSHandle
connectAWS reg logF =
  AWSHandle <$> hdl
  where
    hdl =
      do
        x <- AWS.newEnv AWS.discover
        pure $ x {AWS.envLogger = mkLogFun logF, AWS.envRegion = reg}

mkLogFun :: LogFun -> AWS.Logger
mkLogFun f ll logBuilder =
  f ll (BSL.toStrict $ BSB.toLazyByteString logBuilder)

runAWS :: ResourceT IO a -> IO a
runAWS action =
  runResourceT action

data AWSFileReadability
  = AWSFilePublicRead
  | AWSFilePrivate
  deriving (Show, Eq, Enum, Bounded)

s3MetaData ::
  AWSHandle ->
  T.Text ->
  T.Text ->
  IO (HashMap T.Text T.Text)
s3MetaData hdl bucket objName =
  runAWS $
    do
      rs <- AWS.send (a_cfg hdl) ho
      pure $ view S3.headObjectResponse_metadata rs
  where
    ho = S3.newHeadObject (S3.BucketName bucket) (S3.ObjectKey objName)

s3Upload ::
  AWSHandle ->
  AWSFileReadability ->
  HashMap T.Text T.Text ->
  T.Text ->
  T.Text ->
  Int64 ->
  ConduitT () BS.ByteString (ResourceT IO) () ->
  IO ()
s3Upload hdl readability metaData bucket objName size fileSource =
  runAWS $
    do
      contentHash <- runConduit $ fileSource .| AWS.sinkSHA256
      let body = AWS.HashedStream contentHash (fromIntegral size) fileSource
          po =
            (S3.newPutObject (S3.BucketName bucket) (S3.ObjectKey objName) (AWS.Hashed body))
              & S3.putObject_metadata .~ metaData
          poACL =
            case readability of
              AWSFilePrivate -> po
              AWSFilePublicRead -> po & S3.putObject_acl .~ (Just S3.ObjectCannedACL_Public_read)
      _ <- AWS.send (a_cfg hdl) poACL
      pure ()

s3Download ::
  AWSHandle ->
  T.Text ->
  T.Text ->
  (ConduitT () BS.ByteString (ResourceT IO) () -> ResourceT IO a) ->
  IO a
s3Download hdl bucket objName handleOutput =
  runAWS $
    do
      rs <- AWS.send (a_cfg hdl) (S3.newGetObject (S3.BucketName bucket) (S3.ObjectKey objName))
      handleOutput $ AWS._streamBody $ view S3.getObjectResponse_body rs

s3Delete :: AWSHandle -> T.Text -> T.Text -> IO ()
s3Delete hdl bucket objName =
  runAWS $
    void $ AWS.send (a_cfg hdl) (S3.newDeleteObject (S3.BucketName bucket) (S3.ObjectKey objName))

s3CopyInBucket :: AWSHandle -> T.Text -> T.Text -> T.Text -> IO ()
s3CopyInBucket hdl bucket objName newName =
  runAWS $
    void $
      AWS.send (a_cfg hdl) $
        S3.newCopyObject (S3.BucketName bucket) (bucket <> "/" <> objName) (S3.ObjectKey newName)

newtype AWSQueue = AWSQueue {_unAWSQueue :: T.Text} -- queue url

sqsGetQueue :: AWSHandle -> T.Text -> IO AWSQueue
sqsGetQueue hdl name =
  runAWS $
    AWSQueue <$> view SQS.getQueueUrlResponse_queueUrl <$> AWS.send (a_cfg hdl) (SQS.newGetQueueUrl name)

sqsSendMessage :: AWSHandle -> AWSQueue -> T.Text -> IO ()
sqsSendMessage hdl (AWSQueue q) payload =
  runAWS $
    void $ AWS.send (a_cfg hdl) (SQS.newSendMessage q payload)

data GetMessageCfg = GetMessageCfg
  { -- | how long should the message be hidden from other consumers until 'sqsAckMessage' is called
    -- maximum: 12 hours
    gmc_ackTimeout :: !TimeSpan,
    -- | how many messages should be pulled at once. Between 1 and 10
    gmc_messages :: !Int,
    -- | how long should one polling request wait for the next message? Between 0 and 20 seconds.
    gmc_waitTime :: !TimeSpan
  }

data SqsMessage = SqsMessage
  { sm_handle :: !MessageHandle,
    sm_payload :: !T.Text
  }

-- | Amazon SQS receipt handle id
newtype MessageHandle = MessageHandle {_unMessageHandle :: T.Text}

wrapMessage :: SQS.Message -> Maybe SqsMessage
wrapMessage msg =
  do
    hdl <- MessageHandle <$> msg ^. SQS.message_receiptHandle
    body <- msg ^. SQS.message_body
    pure $ SqsMessage hdl body

sqsGetMessage :: AWSHandle -> AWSQueue -> GetMessageCfg -> IO [SqsMessage]
sqsGetMessage hdl (AWSQueue q) gmc =
  runAWS $
    do
      ms <-
        AWS.send (a_cfg hdl) $
          SQS.newReceiveMessage q
            & SQS.receiveMessage_waitTimeSeconds ?~ round (toSeconds (gmc_waitTime gmc))
            & SQS.receiveMessage_visibilityTimeout ?~ round (toSeconds (gmc_ackTimeout gmc))
            & SQS.receiveMessage_maxNumberOfMessages ?~ gmc_messages gmc
      return (mapMaybe wrapMessage $ fromMaybe [] $ ms ^. SQS.receiveMessageResponse_messages)

sqsAckMessage :: AWSHandle -> AWSQueue -> MessageHandle -> IO ()
sqsAckMessage hdl (AWSQueue q) (MessageHandle rh) =
  runAWS $
    void $ AWS.send (a_cfg hdl) (SQS.newDeleteMessage q rh)

sqsChangeMessageTimeout :: AWSHandle -> AWSQueue -> MessageHandle -> TimeSpan -> IO ()
sqsChangeMessageTimeout hdl (AWSQueue q) (MessageHandle rh) t =
  runAWS $
    void $ AWS.send (a_cfg hdl) $ SQS.newChangeMessageVisibility q rh (round $ toSeconds t)
