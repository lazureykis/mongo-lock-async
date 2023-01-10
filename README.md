# Distributed mutually exclusive locks in MongoDB.
[![Crates.io](https://img.shields.io/crates/v/mongo-lock-async.svg)](https://crates.io/crates/mongo-lock-async) [![docs.rs](https://docs.rs/mongo-lock-async/badge.svg)](https://docs.rs/mongo-lock-async)


This crate contains only async implementation.
If you need a synchronous version, use [`mongo-lock`](https://crates.io/crates/mongo-lock) crate.

This implementation relies on system time. Ensure that NTP clients on your servers are configured properly.

## Installation
Add this crate to `Cargo.toml`

```toml
[dependencies]
mongo_lock_async = "0"
```

## Usage
```rust
#[tokio::main]
async fn main() {
    let mongo = mongodb::Client::with_uri_str("mongodb://localhost").await.unwrap();

    // We need to ensure that mongodb collection has a proper index.
    mongo_lock_async::prepare_database(&mongo).await.unwrap();

    if let Ok(Some(lock)) =
        mongo_lock_async::Lock::try_acquire(
            &mongo,
            "my-key",
            std::time::Duration::from_secs(30)
        ).await
    {
        println!("Lock acquired.");

        // Release the lock before ttl expires to allow others to acquire it.
        lock.release().await.ok();
    }
}
```
