//! # BoolCache
//!
//! A Redis-like in-memory cache engine with AOF+RDB persistence.
//!
//! ## Quick start
//!
//! ```rust,no_run
//! use boolcache::db::{Config, Db};
//! use boolcache::commands::string::{set, get, SetOptions};
//!
//! let db = Db::open(Config::default()).unwrap();
//! set(&db, "greeting", b"hello".to_vec(), SetOptions::default()).unwrap();
//! let val = get(&db, "greeting").unwrap();
//! println!("{:?}", val);
//! ```

pub mod commands;
pub mod db;
pub mod replication;
pub mod error;
pub mod persistence;
pub mod store;
pub mod types;

pub use db::{Config, Db};
pub use error::{Error, Result};
