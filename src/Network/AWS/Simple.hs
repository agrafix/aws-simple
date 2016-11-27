{-# LANGUAGE OverloadedStrings #-}
module Network.AWS.Simple
       ( connectAWS, AWSHandle
         -- * Logging
       , AWS.LogLevel (..), LogFun
         -- * S3
       , AWSFileReadability(..)
       , s3Upload, s3Download, s3Delete, s3CopyInBucket
         -- * SQS
       , sqsGetQueue, AWSQueue
       , sqsSendMessage
       , sqsGetMessage, GetMessageCfg(..), SqsMessage(..), MessageHandle
       , sqsAckMessage
       )
where

import Control.Lens
import Control.Monad
import Control.Monad.Trans
import Control.Monad.Trans.Resource
import Data.Conduit
import Data.Int
import Data.Maybe
import Data.Monoid
import qualified Blaze.ByteString.Builder as BSB
import qualified Data.ByteString as BS
import qualified Data.Text as T
import qualified Network.AWS as AWS
import qualified Network.AWS.Data.Body as AWS
import qualified Network.AWS.S3 as S3
import qualified Network.AWS.SQS as SQS

data AWSHandle =
    AWSHandle
    { a_cfg :: !AWS.Env
    }

type LogFun = AWS.LogLevel -> BS.ByteString -> IO ()

connectAWS :: LogFun -> IO AWSHandle
connectAWS logF =
    AWSHandle <$> hdl
    where
        hdl =
            do x <- AWS.newEnv AWS.Frankfurt AWS.Discover
               pure (x & AWS.envLogger .~ mkLogFun logF)

mkLogFun :: LogFun -> AWS.Logger
mkLogFun f ll logBuilder=
    f ll (BSB.toByteString logBuilder)

runAWS :: AWSHandle -> AWS.AWS a -> IO a
runAWS aws action =
    runResourceT $ AWS.runAWS (a_cfg aws) action

data AWSFileReadability
    = AWSFilePublicRead
    | AWSFilePrivate
    deriving (Show, Eq, Enum, Bounded)

s3Upload ::
    AWSHandle
    -> AWSFileReadability
    -> T.Text
    -> T.Text
    -> Int64
    -> Source (ResourceT IO) BS.ByteString -> IO ()
s3Upload hdl readability bucket objName size fileSource =
    runAWS hdl $
    do contentHash <- lift $ fileSource $$ AWS.sinkSHA256
       let body = AWS.HashedStream contentHash (fromIntegral size) fileSource
           po = S3.putObject (S3.BucketName bucket) (S3.ObjectKey objName) (AWS.Hashed body)
           poACL =
               case readability of
                 AWSFilePrivate -> po
                 AWSFilePublicRead -> po & S3.poACL .~ (Just S3.OPublicRead)
       _ <- AWS.send poACL
       pure ()

s3Download ::
    AWSHandle -> T.Text -> T.Text
    -> (ResumableSource (ResourceT IO) BS.ByteString -> ResourceT IO a)
    -> IO a
s3Download hdl bucket objName handleOutput =
    runAWS hdl $
    do rs <- AWS.send (S3.getObject (S3.BucketName bucket) (S3.ObjectKey objName))
       lift $ handleOutput $ AWS._streamBody $ view S3.gorsBody rs

s3Delete :: AWSHandle -> T.Text -> T.Text -> IO ()
s3Delete hdl bucket objName =
    runAWS hdl $
    void $ AWS.send (S3.deleteObject (S3.BucketName bucket) (S3.ObjectKey objName))

s3CopyInBucket :: AWSHandle -> T.Text -> T.Text -> T.Text -> IO ()
s3CopyInBucket hdl bucket objName newName =
    runAWS hdl $
    void $ AWS.send $
    S3.copyObject (S3.BucketName bucket) (bucket <> "/" <> objName) (S3.ObjectKey newName)

newtype AWSQueue =
    AWSQueue { _unAWSQueue :: T.Text } -- queue url


sqsGetQueue :: AWSHandle -> T.Text -> IO AWSQueue
sqsGetQueue hdl name =
    runAWS hdl $
    AWSQueue <$> view SQS.gqursQueueURL <$> AWS.send (SQS.getQueueURL name)


sqsSendMessage :: AWSHandle -> AWSQueue -> T.Text -> IO ()
sqsSendMessage hdl (AWSQueue q) payload =
    runAWS hdl $
    void $ AWS.send (SQS.sendMessage q payload)

data GetMessageCfg
   = GetMessageCfg
   { gmc_ackTimeout :: !Int
   , gmc_messages :: !Int -- ^ maximum is 10
   , gmc_waitTime :: !Int
   }

data SqsMessage
   = SqsMessage
   { sm_handle :: !MessageHandle
   , sm_payload :: !T.Text
   }

-- | Amazon SQS receipt handle id
newtype MessageHandle =
    MessageHandle { _unMessageHandle :: T.Text }

wrapMessage :: SQS.Message -> Maybe SqsMessage
wrapMessage msg =
    do hdl <- MessageHandle <$> msg ^. SQS.mReceiptHandle
       body <- msg ^. SQS.mBody
       pure $ SqsMessage hdl body

sqsGetMessage :: AWSHandle -> AWSQueue -> GetMessageCfg -> IO [SqsMessage]
sqsGetMessage hdl (AWSQueue q) gmc =
    runAWS hdl $
    do ms <-
           AWS.send $
           SQS.receiveMessage q
           & SQS.rmWaitTimeSeconds ?~ gmc_waitTime gmc
           & SQS.rmVisibilityTimeout ?~ gmc_ackTimeout gmc
           & SQS.rmMaxNumberOfMessages ?~ gmc_messages gmc
       return (mapMaybe wrapMessage $ ms ^. SQS.rmrsMessages)

sqsAckMessage :: AWSHandle -> AWSQueue -> MessageHandle -> IO ()
sqsAckMessage hdl (AWSQueue q) (MessageHandle rh) =
    runAWS hdl $
    void $ AWS.send (SQS.deleteMessage q rh)
