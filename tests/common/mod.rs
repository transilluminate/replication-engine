//! Shared test utilities for integration and chaos tests.
//!
//! This module provides:
//! - Redis testcontainer setup
//! - Mock SyncEngineRef for recording calls
//! - CDC event helpers

pub mod containers;
pub mod mock_sync;

pub use containers::*;
pub use mock_sync::*;
