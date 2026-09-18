use aws_sdk_s3::operation::get_object::GetObjectOutput;
use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output;
use std::str::FromStr;
use std::time::Duration;

use crate::near_indexer_primitives::CryptoHash;
use crate::types::{FetchedMessage, MessageMetadata};

pub trait S3Client {
    async fn get_object(
        &self,
        bucket: &str,
        prefix: &str,
    ) -> Result<
        GetObjectOutput,
        aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::get_object::GetObjectError>,
    >;

    async fn list_objects(
        &self,
        bucket: &str,
        start_after: &str,
    ) -> Result<
        ListObjectsV2Output,
        aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Error>,
    >;
}

#[derive(Clone, Debug)]
pub struct LakeS3Client {
    s3: aws_sdk_s3::Client,
}

impl LakeS3Client {
    pub const fn new(s3: aws_sdk_s3::Client) -> Self {
        Self { s3 }
    }
}

impl S3Client for LakeS3Client {
    async fn get_object(
        &self,
        bucket: &str,
        prefix: &str,
    ) -> Result<
        GetObjectOutput,
        aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::get_object::GetObjectError>,
    > {
        self.s3
            .get_object()
            .bucket(bucket)
            .key(prefix)
            .request_payer(aws_sdk_s3::types::RequestPayer::Requester)
            .send()
            .await
    }

    async fn list_objects(
        &self,
        bucket: &str,
        start_after: &str,
    ) -> Result<
        ListObjectsV2Output,
        aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Error>,
    > {
        self.s3
            .list_objects_v2()
            .max_keys(1000) // 1000 is the default and max value for this parameter
            .delimiter("/".to_string())
            .start_after(start_after)
            .request_payer(aws_sdk_s3::types::RequestPayer::Requester)
            .bucket(bucket)
            .send()
            .await
    }
}

/// Queries the list of the objects in the bucket, grouped by "/" delimiter.
/// Returns the list of block heights that can be fetched
pub async fn list_block_heights<C: S3Client + Send + Sync>(
    lake_s3_client: &C,
    s3_bucket_name: &str,
    start_from_block_height: crate::types::BlockHeight,
) -> Result<Vec<crate::types::BlockHeight>, crate::types::LakeError> {
    tracing::debug!(
        target: crate::LAKE_FRAMEWORK,
        "Fetching block heights from S3, after #{}...",
        start_from_block_height
    );
    let response = lake_s3_client
        .list_objects(s3_bucket_name, &format!("{start_from_block_height:0>12}"))
        .await?;

    Ok(response
        .common_prefixes
        .map_or_else(Vec::new, |common_prefixes| {
            common_prefixes
                .into_iter()
                .filter_map(|common_prefix| common_prefix.prefix)
                .filter_map(|prefix_string| {
                    prefix_string
                        .split_once('/')
                        .map(|(first, _)| first)
                        .map(u64::from_str)
                        .and_then(|num| num.ok())
                })
                .collect()
        }))
}

#[cfg(not(test))]
const INITIAL_RETRY_DELAY: Duration = Duration::from_millis(200);
#[cfg(test)]
const INITIAL_RETRY_DELAY: Duration = Duration::from_millis(1);
const MAX_RETRY_DELAY: Duration = Duration::from_secs(10);

async fn fetch_object_bytes_with_retry<C: S3Client + Send + Sync>(
    lake_s3_client: &C,
    s3_bucket_name: &str,
    object_key: &str,
) -> Result<bytes::Bytes, crate::types::LakeError> {
    let mut retry_delay = INITIAL_RETRY_DELAY;

    loop {
        match lake_s3_client.get_object(s3_bucket_name, object_key).await {
            Ok(response) => match response.body.collect().await {
                Ok(body) => return Ok(body.into_bytes()),
                Err(err) => {
                    let delay = jittered_retry_delay(retry_delay);
                    tracing::debug!(
                        target: crate::LAKE_FRAMEWORK,
                        "Failed to read {object_key}. Retrying in {delay:?}.\n{err:#?}",
                    );
                    tokio::time::sleep(delay).await;
                }
            },
            Err(err) if is_retryable_get_object_error(&err) => {
                let delay = jittered_retry_delay(retry_delay);
                tracing::debug!(
                    target: crate::LAKE_FRAMEWORK,
                    "Failed to fetch {object_key}. Retrying in {delay:?}.\n{err:#?}",
                );
                tokio::time::sleep(delay).await;
            }
            Err(err) => return Err(err.into()),
        }

        retry_delay = retry_delay.saturating_mul(2).min(MAX_RETRY_DELAY);
    }
}

fn is_retryable_get_object_error(
    error: &aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::get_object::GetObjectError>,
) -> bool {
    use aws_sdk_s3::error::SdkError;

    match error {
        SdkError::TimeoutError(_) | SdkError::ResponseError(_) => true,
        SdkError::DispatchFailure(error) => !error.is_user(),
        SdkError::ServiceError(error) => {
            error.err().is_no_such_key()
                || matches!(error.raw().status().as_u16(), 408 | 429 | 500..=599)
                || matches!(
                    error.err().meta().code(),
                    Some(
                        "InternalError"
                            | "NoSuchKey"
                            | "NotFound"
                            | "RequestTimeout"
                            | "RequestTimeoutException"
                            | "ServiceUnavailable"
                            | "SlowDown"
                            | "Throttling"
                            | "ThrottlingException"
                    )
                )
        }
        _ => false,
    }
}

fn jittered_retry_delay(max_delay: Duration) -> Duration {
    let max_nanos = u64::try_from(max_delay.as_nanos()).unwrap_or(u64::MAX);
    let min_nanos = max_nanos / 2;
    let jitter_range = max_nanos - min_nanos;
    let random_nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos() as u64)
        .unwrap_or_default();

    Duration::from_nanos(min_nanos + random_nanos % (jitter_range + 1))
}

/// By the given block height gets the objects:
/// - block.json
/// - shard_N.json
///   Combines the raw JSON content of the objects into a single JSON message.
///   Returns the raw JSON payload with its parsed block metadata.
pub async fn fetch_raw_message<C: S3Client + Send + Sync>(
    lake_s3_client: &C,
    s3_bucket_name: &str,
    block_height: crate::types::BlockHeight,
) -> Result<FetchedMessage<Vec<u8>>, crate::types::LakeError> {
    #[derive(serde::Deserialize)]
    struct BlockLayout {
        chunks: Vec<ChunkLayout>,
    }

    #[derive(serde::Deserialize)]
    struct ChunkLayout {
        shard_id: u64,
    }

    let block_bytes = fetch_object_bytes_with_retry(
        lake_s3_client,
        s3_bucket_name,
        &format!("{block_height:0>12}/block.json"),
    )
    .await?;

    let layout: BlockLayout = serde_json::from_slice(&block_bytes)?;

    let shards = futures::future::try_join_all(layout.chunks.into_iter().map(|chunk| async move {
        fetch_object_bytes_with_retry(
            lake_s3_client,
            s3_bucket_name,
            &format!("{block_height:0>12}/shard_{}.json", chunk.shard_id),
        )
        .await
    }))
    .await?;

    let capacity = block_bytes.len()
        + shards.iter().map(bytes::Bytes::len).sum::<usize>()
        + shards.len().saturating_sub(1)
        + 22;
    let mut message = Vec::with_capacity(capacity);

    message.extend_from_slice(b"{\"block\":");
    message.extend_from_slice(&block_bytes);
    message.extend_from_slice(b",\"shards\":[");

    for (index, shard) in shards.iter().enumerate() {
        if index != 0 {
            message.push(b',');
        }
        message.extend_from_slice(shard);
    }

    message.extend_from_slice(b"]}");

    Ok(attach_raw_message_metadata(message)?)
}

#[derive(serde::Deserialize)]
struct MinimalBlockHeader {
    height: crate::types::BlockHeight,
    hash: CryptoHash,
    prev_hash: CryptoHash,
}

#[derive(serde::Deserialize)]
struct MinimalBlock {
    header: MinimalBlockHeader,
}

#[derive(serde::Deserialize)]
struct MinimalStreamerMessage {
    block: MinimalBlock,
}

fn attach_raw_message_metadata(
    message: Vec<u8>,
) -> Result<FetchedMessage<Vec<u8>>, serde_json::Error> {
    let parsed: MinimalStreamerMessage = serde_json::from_slice(&message)?;
    let header = parsed.block.header;

    Ok(FetchedMessage {
        message,
        metadata: MessageMetadata {
            height: header.height,
            hash: header.hash,
            prev_hash: header.prev_hash,
        },
    })
}

/// By the given block height gets the objects:
/// - block.json
/// - shard_N.json
///   Reads the content of the objects and parses as a JSON.
///   Returns the result in `near_indexer_primitives::StreamerMessage`
pub async fn fetch_streamer_message<C: S3Client + Send + Sync>(
    lake_s3_client: &C,
    s3_bucket_name: &str,
    block_height: crate::types::BlockHeight,
) -> Result<near_lake_primitives::StreamerMessage, crate::types::LakeError> {
    let block_bytes = fetch_object_bytes_with_retry(
        lake_s3_client,
        s3_bucket_name,
        &format!("{block_height:0>12}/block.json"),
    )
    .await?;
    let block_view =
        serde_json::from_slice::<crate::near_indexer_primitives::views::BlockView>(&block_bytes)?;

    let fetch_shards_futures = block_view.chunks.iter().map(|chunk| {
        fetch_shard_or_retry(
            lake_s3_client,
            s3_bucket_name,
            block_height,
            chunk.shard_id.into(),
        )
    });

    let shards = futures::future::try_join_all(fetch_shards_futures).await?;

    Ok(near_lake_primitives::StreamerMessage {
        block: block_view,
        shards,
    })
}

/// Fetches the shard data JSON from AWS S3 and returns the `IndexerShard`
async fn fetch_shard_or_retry<C: S3Client + Send + Sync>(
    lake_s3_client: &C,
    s3_bucket_name: &str,
    block_height: crate::types::BlockHeight,
    shard_id: u64,
) -> Result<near_lake_primitives::IndexerShard, crate::types::LakeError> {
    let body_bytes = fetch_object_bytes_with_retry(
        lake_s3_client,
        s3_bucket_name,
        &format!("{block_height:0>12}/shard_{shard_id}.json"),
    )
    .await?;

    Ok(serde_json::from_slice::<near_lake_primitives::IndexerShard>(&body_bytes)?)
}

#[cfg(test)]
mod test {
    use super::*;

    use aws_sdk_s3::operation::get_object::builders::GetObjectOutputBuilder;
    use aws_sdk_s3::operation::list_objects_v2::builders::ListObjectsV2OutputBuilder;
    use aws_sdk_s3::primitives::ByteStream;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use aws_smithy_types::body::SdkBody;

    #[derive(Clone, Debug)]
    pub struct LakeS3Client {}

    impl S3Client for LakeS3Client {
        async fn get_object(
            &self,
            _bucket: &str,
            prefix: &str,
        ) -> Result<
            GetObjectOutput,
            aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::get_object::GetObjectError>,
        > {
            let path = format!("{}/blocks/{}", env!("CARGO_MANIFEST_DIR"), prefix);
            let file_bytes = tokio::fs::read(path).await.unwrap();
            let stream = ByteStream::new(SdkBody::from(file_bytes));
            Ok(GetObjectOutputBuilder::default().body(stream).build())
        }

        async fn list_objects(
            &self,
            _bucket: &str,
            _start_after: &str,
        ) -> Result<
            ListObjectsV2Output,
            aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Error>,
        > {
            Ok(ListObjectsV2OutputBuilder::default().build())
        }
    }

    #[derive(Debug)]
    struct RetryTestS3Client {
        attempts: AtomicUsize,
        permanent_failure: bool,
    }

    #[derive(Debug)]
    struct RawMessageTestS3Client {
        block: Vec<u8>,
        shard: Option<Vec<u8>>,
    }

    impl S3Client for RawMessageTestS3Client {
        async fn get_object(
            &self,
            _bucket: &str,
            prefix: &str,
        ) -> Result<
            GetObjectOutput,
            aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::get_object::GetObjectError>,
        > {
            let bytes = if prefix.ends_with("block.json") {
                self.block.clone()
            } else {
                self.shard.clone().expect("test shard must be configured")
            };
            let stream = ByteStream::new(SdkBody::from(bytes));

            Ok(GetObjectOutputBuilder::default().body(stream).build())
        }

        async fn list_objects(
            &self,
            _bucket: &str,
            _start_after: &str,
        ) -> Result<
            ListObjectsV2Output,
            aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Error>,
        > {
            Ok(ListObjectsV2OutputBuilder::default().build())
        }
    }

    impl S3Client for RetryTestS3Client {
        async fn get_object(
            &self,
            _bucket: &str,
            _prefix: &str,
        ) -> Result<
            GetObjectOutput,
            aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::get_object::GetObjectError>,
        > {
            let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);

            if self.permanent_failure {
                return Err(aws_sdk_s3::error::SdkError::construction_failure(
                    std::io::Error::other("invalid request"),
                ));
            }

            if attempt == 0 {
                return Err(aws_sdk_s3::error::SdkError::timeout_error(
                    std::io::Error::new(std::io::ErrorKind::TimedOut, "request timed out"),
                ));
            }

            let stream = ByteStream::new(SdkBody::from(b"object bytes".to_vec()));
            Ok(GetObjectOutputBuilder::default().body(stream).build())
        }

        async fn list_objects(
            &self,
            _bucket: &str,
            _start_after: &str,
        ) -> Result<
            ListObjectsV2Output,
            aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Error>,
        > {
            Ok(ListObjectsV2OutputBuilder::default().build())
        }
    }

    #[tokio::test]
    async fn retries_transient_get_object_errors() {
        let lake_client = RetryTestS3Client {
            attempts: AtomicUsize::new(0),
            permanent_failure: false,
        };

        let bytes = fetch_object_bytes_with_retry(&lake_client, "bucket", "object")
            .await
            .unwrap();

        assert_eq!(bytes.as_ref(), b"object bytes");
        assert_eq!(lake_client.attempts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn returns_permanent_get_object_errors() {
        let lake_client = RetryTestS3Client {
            attempts: AtomicUsize::new(0),
            permanent_failure: true,
        };

        let error = fetch_object_bytes_with_retry(&lake_client, "bucket", "object")
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            crate::types::LakeError::AwsGetObjectError {
                error: aws_sdk_s3::error::SdkError::ConstructionFailure(_),
            }
        ));
        assert_eq!(lake_client.attempts.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn does_not_retry_user_dispatch_errors() {
        let error =
            aws_sdk_s3::error::SdkError::dispatch_failure(aws_sdk_s3::error::ConnectorError::user(
                Box::new(std::io::Error::other("invalid request")),
            ));

        assert!(!is_retryable_get_object_error(&error));
    }

    #[tokio::test]
    async fn fetches_raw_message() {
        let lake_client = LakeS3Client {};

        let fetched_message = fetch_raw_message(&lake_client, "near-lake-data-mainnet", 879765)
            .await
            .unwrap();
        let streamer_message: near_lake_primitives::StreamerMessage =
            serde_json::from_slice(&fetched_message.message).unwrap();

        assert_eq!(streamer_message.block.header.height, 879765);
        assert_eq!(streamer_message.shards.len(), 1);
        assert_eq!(fetched_message.metadata.height, 879765);
        assert_eq!(
            fetched_message.metadata.hash,
            streamer_message.block.header.hash
        );
        assert_eq!(
            fetched_message.metadata.prev_hash,
            streamer_message.block.header.prev_hash
        );
    }

    #[tokio::test]
    async fn rejects_raw_message_with_missing_header() {
        let lake_client = RawMessageTestS3Client {
            block: br#"{"chunks":[]}"#.to_vec(),
            shard: None,
        };
        let error = fetch_raw_message(&lake_client, "bucket", 1)
            .await
            .unwrap_err();

        assert!(matches!(error, crate::types::LakeError::ParseError { .. }));
    }

    #[tokio::test]
    async fn rejects_raw_message_with_malformed_shard_json() {
        let lake_client = RawMessageTestS3Client {
            block: br#"{
                "header": {
                    "height": 879765,
                    "hash": "95K8Je1iAVqieVU8ZuGgSdbvYs8T9rL6ER1XnRekMGbj",
                    "prev_hash": "9Da84RTsubZPcLxzK1K6JkCnDnMn4DxaSRzJPtnYJXUM"
                },
                "chunks": [{"shard_id": 0}]
            }"#
            .to_vec(),
            shard: Some(b"{".to_vec()),
        };
        let error = fetch_raw_message(&lake_client, "bucket", 879765)
            .await
            .unwrap_err();

        assert!(matches!(error, crate::types::LakeError::ParseError { .. }));
    }

    #[tokio::test]
    async fn deserializes_meta_transactions() {
        let lake_client = LakeS3Client {};

        let streamer_message =
            fetch_streamer_message(&lake_client, "near-lake-data-mainnet", 879765)
                .await
                .unwrap();

        let delegate_action = &streamer_message.shards[0]
            .chunk
            .as_ref()
            .unwrap()
            .transactions[0]
            .transaction
            .actions[0];

        assert_eq!(
            serde_json::to_value(delegate_action).unwrap(),
            serde_json::json!({
                "Delegate": {
                    "delegate_action": {
                        "sender_id": "test.near",
                        "receiver_id": "test.near",
                        "actions": [
                          {
                            "AddKey": {
                              "public_key": "ed25519:CnQMksXTTtn81WdDujsEMQgKUMkFvDJaAjDeDLTxVrsg",
                              "access_key": {
                                "nonce": 0,
                                "permission": "FullAccess"
                              }
                            }
                          }
                        ],
                        "nonce": 879546,
                        "max_block_height": 100,
                        "public_key": "ed25519:8Rn4FJeeRYcrLbcrAQNFVgvbZ2FCEQjgydbXwqBwF1ib"
                    },
                    "signature": "ed25519:25uGrsJNU3fVgUpPad3rGJRy2XQum8gJxLRjKFCbd7gymXwUxQ9r3tuyBCD6To7SX5oSJ2ScJZejwqK1ju8WdZfS"
                }
            })
        );
    }
}
