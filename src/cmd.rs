// src/cmd.rs
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum Command {
    Set { key: String, value: String },
    Get { key: String },
    Remove { key: String },
    Compact,
}