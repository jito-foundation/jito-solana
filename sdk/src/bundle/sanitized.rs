#![cfg(feature = "full")]

use {solana_sdk::transaction::SanitizedTransaction, uuid::Uuid};

#[derive(Clone, Debug)]
pub struct SanitizedBundle {
    pub transactions: Vec<SanitizedTransaction>,
    pub uuid: Uuid,
}
