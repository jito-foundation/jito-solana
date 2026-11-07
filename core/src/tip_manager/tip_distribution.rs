use {
    borsh::BorshDeserialize,
    solana_account::{AccountSharedData, ReadableAccount},
    solana_clock::Epoch,
    solana_pubkey::Pubkey,
    thiserror::Error,
};

#[derive(Debug, PartialEq, Clone, Eq, Error)]
pub enum TipDistributionError {
    #[error("Invalid account owner")]
    InvalidAccountOwner,
    #[error("Invalid discriminator")]
    InvalidDiscriminator,
    #[error("Deserialization error")]
    DeserializationError,
}

pub type TipDistributionResult<T> = std::result::Result<T, TipDistributionError>;

pub struct TipDistributionAccount;

impl TipDistributionAccount {
    pub(crate) fn find_program_address(
        program_id: &Pubkey,
        vote_pubkey: &Pubkey,
        epoch: Epoch,
    ) -> (Pubkey, u8) {
        Pubkey::find_program_address(
            &[
                b"TIP_DISTRIBUTION_ACCOUNT",
                vote_pubkey.to_bytes().as_ref(),
                epoch.to_le_bytes().as_ref(),
            ],
            program_id,
        )
    }
}

pub struct InitializeTipDistributionConfigInstruction;

impl InitializeTipDistributionConfigInstruction {
    const DISCRIMINATOR: &'static [u8] = &[175, 175, 109, 31, 13, 152, 155, 237];

    pub(crate) fn to_instruction_data(
        authority: Pubkey,
        expired_funds_account: Pubkey,
        num_epochs_valid: u64,
        max_validator_commission_bps: u16,
        bump: u8,
    ) -> Vec<u8> {
        let mut data = Vec::with_capacity(Self::DISCRIMINATOR.len() + 75);
        data.extend_from_slice(Self::DISCRIMINATOR);
        data.extend_from_slice(authority.as_ref());
        data.extend_from_slice(expired_funds_account.as_ref());
        data.extend_from_slice(&num_epochs_valid.to_le_bytes());
        data.extend_from_slice(&max_validator_commission_bps.to_le_bytes());
        data.push(bump);
        data
    }
}

pub struct InitializeTipDistributionAccountInstruction;

impl InitializeTipDistributionAccountInstruction {
    const DISCRIMINATOR: &'static [u8] = &[120, 191, 25, 182, 111, 49, 179, 55];

    pub(crate) fn to_instruction_data(
        merkle_root_upload_authority: Pubkey,
        validator_commission_bps: u16,
        bump: u8,
    ) -> Vec<u8> {
        let mut data = Vec::with_capacity(Self::DISCRIMINATOR.len() + 35);
        data.extend_from_slice(Self::DISCRIMINATOR);
        data.extend_from_slice(merkle_root_upload_authority.as_ref());
        data.extend_from_slice(&validator_commission_bps.to_le_bytes());
        data.push(bump);
        data
    }
}

#[allow(unused)]
#[derive(BorshDeserialize)]
pub struct JitoTipDistributionConfig {
    /// Account with authority over this PDA.
    authority: Pubkey,

    /// We want to expire funds after some time so that validators can be refunded the rent.
    /// Expired funds will get transferred to this account.
    expired_funds_account: Pubkey,

    /// Specifies the number of epochs a merkle root is valid for before expiring.
    num_epochs_valid: u64,

    /// The maximum commission a validator can set on their distribution account.
    max_validator_commission_bps: u16,

    /// The bump used to generate this account
    bump: u8,
}

#[allow(unused)]
impl JitoTipDistributionConfig {
    const DISCRIMINATOR: &'static [u8] = &[155, 12, 170, 224, 30, 250, 204, 130];

    pub(crate) fn from_account_shared_data(
        account_shared_data: &AccountSharedData,
        program_id: &Pubkey,
    ) -> TipDistributionResult<Self> {
        if account_shared_data.owner() != program_id {
            return Err(TipDistributionError::InvalidAccountOwner);
        }

        if &account_shared_data.data()[0..8] != Self::DISCRIMINATOR {
            return Err(TipDistributionError::InvalidDiscriminator);
        }

        JitoTipDistributionConfig::try_from_slice(&account_shared_data.data()[8..83]).map_err(|e| {
            error!("Error deserializing tip distribution config account: {e}");
            TipDistributionError::DeserializationError
        })
    }

    pub(crate) fn find_program_address(program_id: &Pubkey) -> (Pubkey, u8) {
        Pubkey::find_program_address(&[b"CONFIG_ACCOUNT"], program_id)
    }

    pub(crate) fn authority(&self) -> Pubkey {
        self.authority
    }

    pub(crate) fn expired_funds_account(&self) -> Pubkey {
        self.expired_funds_account
    }

    pub(crate) fn num_epochs_valid(&self) -> u64 {
        self.num_epochs_valid
    }

    pub(crate) fn max_validator_commission_bps(&self) -> u16 {
        self.max_validator_commission_bps
    }
}
