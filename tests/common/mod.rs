// Copyright (c) 2025-2026 Adrian Robinson. Licensed under the AGPL-3.0.
// See LICENSE file in the project root for full license text.

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
