#![doc(issue_tracker_base_url = "https://github.com/lazureykis/mongo-lock-async/issues")]

//! Distributed mutually exclusive locks in MongoDB.
//!
//! This crate contains only async implementation.
//! If you need a synchronous version, use [`mongo-lock`](https://crates.io/crates/mongo-lock) crate.
//!
//! This implementation relies on system time. Ensure that NTP clients on your servers are configured properly.
//!
//! Usage:
//! ```rust
//! #[tokio::main]
//! async fn main() {
//!     let mongo = mongodb::Client::with_uri_str("mongodb://localhost").await.unwrap();
//!
//!     // We need to ensure that mongodb collection has a proper index.
//!     mongo_lock_async::prepare_database(&mongo).await.unwrap();
//!
//!     if let Ok(Some(lock)) =
//!         mongo_lock_async::Lock::try_acquire(
//!             &mongo,
//!             "my-key",
//!             std::time::Duration::from_secs(30)
//!         ).await
//!     {
//!         println!("Lock acquired.");
//!
//!         // Release the lock before ttl expires to allow others to acquire it.
//!         lock.release().await.ok();
//!     }
//! }
//! ```

mod error;
mod util;

pub use error::Error;
use mongodb::bson::{doc, Document};
use mongodb::error::{ErrorKind, WriteError, WriteFailure};
use mongodb::options::IndexOptions;
use mongodb::{Client, Collection, IndexModel};
use std::time::Duration;

const COLLECTION_NAME: &str = "locks";
const DEFAULT_DB_NAME: &str = "mongo-lock";

#[inline]
fn collection(mongo: &Client) -> Collection<Document> {
    mongo
        .default_database()
        .unwrap_or_else(|| mongo.database(DEFAULT_DB_NAME))
        .collection(COLLECTION_NAME)
}

/// Distributed mutex lock.
pub struct Lock {
    mongo: Client,
    id: String,
}

impl Lock {
    /// Tries to acquire the lock with the given key.
    pub async fn try_acquire(
        mongo: &Client,
        key: &str,
        ttl: Duration,
    ) -> Result<Option<Lock>, Error> {
        let (now, expires_at) = util::now_and_expires_at(ttl);

        // Update expired locks if MongoDB didn't clean them yet.
        let query = doc! {
            "_id": key,
            "expiresAt": {"$lte": now},
        };

        let update = doc! {
            "$set": {
                "expiresAt": expires_at,
            },
            "$setOnInsert": {
                "_id": key,
            },
        };

        match collection(mongo)
            .update_one(query, update)
            .upsert(true)
            .await
        {
            Ok(result) => {
                if result.upserted_id.is_some() || result.modified_count == 1 {
                    Ok(Some(Lock {
                        mongo: mongo.clone(),
                        id: key.to_string(),
                    }))
                } else {
                    Ok(None)
                }
            }
            Err(err) => {
                if let ErrorKind::Write(WriteFailure::WriteError(WriteError {
                    code: 11000, ..
                })) = *err.kind
                {
                    Ok(None)
                } else {
                    Err(err.into())
                }
            }
        }
    }

    /// Tries to acquire the lock with the given key.
    /// If the lock is already acquired, waits for it to be released
    /// up to `lock_wait_timeout` time checking every `lock_poll_interval`.
    pub async fn try_acquire_with_timeout(
        mongo: &Client,
        key: &str,
        key_ttl: Duration,
        lock_wait_timeout: Duration,
        lock_poll_interval: Duration,
    ) -> Result<Option<Lock>, Error> {
        let start = std::time::Instant::now();
        loop {
            match Self::try_acquire(mongo, key, key_ttl).await {
                Ok(Some(lock)) => return Ok(Some(lock)),
                Ok(None) => {
                    if start.elapsed() > lock_wait_timeout {
                        return Ok(None);
                    }
                    tokio::time::sleep(lock_poll_interval).await;
                }
                Err(err) => return Err(err),
            }

            if start.elapsed() > lock_wait_timeout {
                return Err("Cannot acquire lock".into());
            }
        }
    }

    /// Releases the lock.
    pub async fn release(&self) -> Result<bool, Error> {
        let result = collection(&self.mongo)
            .delete_one(doc! {"_id": &self.id})
            .await?;

        Ok(result.deleted_count == 1)
    }
}

/// Prepares MongoDB collection to store locks.
///
/// Creates TTL index to remove old records after they expire.
///
/// The [Lock] itself does not relies on this index,
/// because MongoDB can remove documents with some significant delay.
pub async fn prepare_database(mongo: &Client) -> Result<(), Error> {
    let options = IndexOptions::builder()
        .expire_after(Some(Duration::from_secs(0)))
        .build();

    let model = IndexModel::builder()
        .keys(doc! {"expiresAt": 1})
        .options(options)
        .build();

    collection(mongo).create_index(model).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use tokio::time::Instant;

    use super::*;

    fn gen_random_key() -> String {
        use rand::{distributions::Alphanumeric, thread_rng, Rng};
        thread_rng()
            .sample_iter(&Alphanumeric)
            .take(30)
            .map(char::from)
            .collect()
    }

    #[tokio::test]
    async fn simple_locks() {
        let mongo = mongodb::Client::with_uri_str("mongodb://localhost")
            .await
            .unwrap();

        prepare_database(&mongo).await.unwrap();

        let key1 = gen_random_key();
        let key2 = gen_random_key();

        let lock1 = Lock::try_acquire(&mongo, &key1, Duration::from_secs(5))
            .await
            .unwrap();
        assert!(lock1.is_some());

        let lock1_dup = Lock::try_acquire(&mongo, &key1, Duration::from_secs(5))
            .await
            .unwrap();
        assert!(lock1_dup.is_none());

        let released1 = lock1.unwrap().release().await.unwrap();
        assert!(released1);

        let lock1 = Lock::try_acquire(&mongo, &key1, Duration::from_secs(5))
            .await
            .unwrap();
        assert!(lock1.is_some());

        let lock2 = Lock::try_acquire(&mongo, &key2, Duration::from_secs(5))
            .await
            .unwrap();
        assert!(lock2.is_some());

        lock1.unwrap().release().await.unwrap();
        lock2.unwrap().release().await.unwrap();
    }

    #[tokio::test]
    async fn with_ttl() {
        let mongo = Client::with_uri_str("mongodb://localhost").await.unwrap();

        prepare_database(&mongo).await.unwrap();

        let key = gen_random_key();

        assert!(Lock::try_acquire(&mongo, &key, Duration::from_secs(1))
            .await
            .unwrap()
            .is_some());

        assert!(Lock::try_acquire(&mongo, &key, Duration::from_secs(1))
            .await
            .unwrap()
            .is_none());

        tokio::time::sleep(Duration::from_secs(1)).await;

        assert!(Lock::try_acquire(&mongo, &key, Duration::from_secs(1))
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn wait_for_lock() {
        let mongo = Client::with_uri_str("mongodb://localhost").await.unwrap();

        prepare_database(&mongo).await.unwrap();

        let key = gen_random_key();

        assert!(Lock::try_acquire(&mongo, &key, Duration::from_secs(3))
            .await
            .unwrap()
            .is_some());

        let now = Instant::now();
        assert!(Lock::try_acquire_with_timeout(
            &mongo,
            &key,
            Duration::from_secs(3),
            Duration::from_secs(5),
            Duration::from_millis(100)
        )
        .await
        .unwrap()
        .is_some());

        assert!(now.elapsed() > Duration::from_secs(2));

        assert!(Lock::try_acquire(&mongo, &key, Duration::from_secs(1))
            .await
            .unwrap()
            .is_none());
    }
}
