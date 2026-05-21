//! Public surface of the rektifier binary crate. The actual `main`
//! entry point lives in `main.rs`; this lib re-exports the modules
//! whose internals integration tests need to reach (notably the
//! auth-wiring builder).

pub mod auth_wiring;
