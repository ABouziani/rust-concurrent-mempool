#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Transaction {
    pub id: u64,
    pub payload: Vec<u8>,
}

impl Transaction {
    pub fn new(id: u64, payload: impl Into<Vec<u8>>) -> Self {
        Self {
            id,
            payload: payload.into(),
        }
    }
}
