use mongodb::bson::DateTime;
use std::time::{Duration, SystemTime};

#[inline]
pub fn now_and_expires_at(ttl: Duration) -> (DateTime, DateTime) {
    let now = SystemTime::now();
    let expires_at = now + ttl;

    let now = DateTime::from_system_time(now);
    let expires_at = DateTime::from_system_time(expires_at);

    (now, expires_at)
}
