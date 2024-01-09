use solana_accounts_db::account_overrides::AccountOverrides;

use {
    solana_runtime::bank::Bank,
    solana_sdk::{account::AccountSharedData, pubkey::Pubkey},
};

pub(crate) fn get_account_from_overwrites_or_bank(
    pubkey: &Pubkey,
    bank: &Bank,
    account_overrides: Option<&AccountOverrides>,
) -> Option<AccountSharedData> {
    account_overrides
        .and_then(|accounts| accounts.get(pubkey).cloned())
        .or_else(|| bank.get_account(pubkey))
}
