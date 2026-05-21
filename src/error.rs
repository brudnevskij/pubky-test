use thiserror::Error;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("redis error: {0}")]
    Redis(#[from] redis::RedisError),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("task join error: {0}")]
    Join(#[from] tokio::task::JoinError),
}
