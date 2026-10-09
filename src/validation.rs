use crate::transaction::Transaction;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValidationError {
    EmptyPayload,
    PayloadTooLarge,
}

pub fn validate(tx: &Transaction) -> Result<(), ValidationError> {
    if tx.payload.is_empty() {
        return Err(ValidationError::EmptyPayload);
    }

    if tx.payload.len() > 1024 {
        return Err(ValidationError::PayloadTooLarge);
    }

    Ok(())
}
