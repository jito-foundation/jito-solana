use solana_sdk::{account::AccountSharedData, pubkey::Pubkey, sysvar};
use std::collections::HashMap;

/// Encapsulates overridden accounts, typically used for transaction simulations
#[derive(Default)]
pub struct AccountOverrides {
    pub slot_history: Option<AccountSharedData>,
    pub cached_accounts_with_rent: HashMap<Pubkey, AccountSharedData>,
}

pub enum AccountWithRentInfo {
    Zero(AccountSharedData),
    SubtractRent(AccountSharedData),
}

impl AccountOverrides {
    /// Sets in the slot history
    ///
    /// Note: no checks are performed on the correctness of the contained data
    pub fn set_slot_history(&mut self, slot_history: Option<AccountSharedData>) {
        self.slot_history = slot_history;
    }

    /// Gets the account if it's found in the list of overrides
    pub fn get_ignore_rent_type(&self, pubkey: &Pubkey) -> Option<&AccountSharedData> {
        if pubkey == &sysvar::slot_history::id() {
            self.slot_history.as_ref()
        } else {
            self.cached_accounts_with_rent.get(pubkey)
        }
    }

    /// Gets the account info with info on whether rent should be subtracted or not
    pub fn get(&self, pubkey: &Pubkey) -> Option<AccountWithRentInfo> {
        if pubkey == &sysvar::slot_history::id() {
            if self.slot_history.is_some() {
                Some(AccountWithRentInfo::Zero(
                    self.slot_history.as_ref().unwrap().clone(),
                ))
            } else {
                None
            }
        } else {
            match self.cached_accounts_with_rent.get(pubkey) {
                None => None,
                Some(acc) => Some(AccountWithRentInfo::SubtractRent(acc.clone())),
            }
        }
    }

    pub fn put(&mut self, pubkey: Pubkey, data: AccountSharedData) {
        self.cached_accounts_with_rent.insert(pubkey, data);
    }
}
