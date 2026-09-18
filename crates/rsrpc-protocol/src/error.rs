//! Library error type: every fallible public API returns
//! [`Result`] with [`RsrpcError`] instead of a boxed opaque error, so
//! callers can match on failure kinds instead of parsing strings.
//!
//! Wire-visible messages keep their historical text.

use thiserror::Error;

/// Alias for fallible library operations.
pub type Result<T> = std::result::Result<T, RsrpcError>;

/// Every failure mode of this library.
#[derive(Debug, Error)]
pub enum RsrpcError {
  /// Caller-supplied JSON (database, overrides) failed to parse.
  /// Keeps the serde error as `source` (line/column/classification)
  /// instead of only its `Display` text.
  #[error("invalid JSON provided to RPCServer: {source}")]
  InvalidJson {
    #[source]
    source: serde_json::Error,
  },

  /// Database file missing or unreadable.
  #[error("rpcserver could not find file {0:?}: {1}", path, source)]
  UnreadableFile {
    path: std::path::PathBuf,
    source: std::io::Error,
  },

  /// A poisoned mutex was encountered (a previous panic while holding
  /// the lock). Carries which lock, for diagnostics.
  #[error("{0} lock poisoned: {1}")]
  Poisoned(&'static str, String),

  /// A transport was configured with an unusable value (zero bound, ...).
  /// Caller bug by construction: fix the config, not the error.
  #[error("invalid transport config: {0}")]
  InvalidConfig(&'static str),

  /// A scan was requested while another scan is still running
  /// (re-entrant trigger racing the scan thread).
  #[error("scanning already in progress")]
  ScanInProgress,

  /// WebSocket server bind failed on every candidate port without a
  /// capturable OS error (empty range / listener-only failures).
  #[error("websocket bind failed on ports {start}-{end}: all in use")]
  WsExhausted { start: u16, end: u16 },

  /// Bridge listener bind failed on every candidate port.
  #[error("bridge {name} launch failed on ports {start}-{end}: all in use")]
  BridgeBind {
    name: &'static str,
    start: u16,
    end: u16,
  },

  /// I/O failures (procfs reads, file writes).
  #[error(transparent)]
  Io(#[from] std::io::Error),

  /// JSON (de)serialization failures.
  #[error(transparent)]
  Json(#[from] serde_json::Error),

  /// HTTP fetch failures (database, exclusions). The client error stays
  /// opaque — this crate owns no HTTP dependency — while the source
  /// chain and message remain available for diagnostics.
  #[error("http request failed: {0}")]
  Http(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),

  /// IPC socket/pipe bind failed on every candidate index (Discord,
  /// arRPC or another rsRPC already holds them all).
  #[error("ipc bind failed after {attempts} attempts")]
  IpcBind {
    attempts: u8,
    #[source]
    source: std::io::Error,
  },

  /// WebSocket bridge bind failed on every candidate port. Keeps the
  /// last `io::Error` so callers can match `AddrInUse` vs permission
  /// vs other failures.
  #[error("websocket bind failed on ports {start}-{end}: all in use")]
  WsBind {
    start: u16,
    end: u16,
    #[source]
    source: std::io::Error,
  },
}

impl RsrpcError {
  /// Wrap an HTTP client error opaquely, preserving its source chain.
  #[must_use]
  pub fn http(source: impl std::error::Error + Send + Sync + 'static) -> Self {
    Self::Http(Box::new(source))
  }
}
