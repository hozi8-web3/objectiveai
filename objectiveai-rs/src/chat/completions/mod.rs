//! Chat completions request and response types.

pub mod request;
pub mod response;
mod upstream;

pub use upstream::*;

#[cfg(feature = "http")]
mod http;

#[cfg(feature = "http")]
pub use http::*;
