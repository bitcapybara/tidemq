use std::fmt;

use serde::{Deserialize, Serialize};
use tidemq_protocol::Id;

use super::{Generations, Rejection, Result};

pub const DELIVERY_TOKEN_BYTES: usize = 101;
const TOKEN_VERSION: u8 = 1;
const TOKEN_BODY_BYTES: usize = DELIVERY_TOKEN_BYTES - 32;
const TOKEN_DOMAIN: &[u8] = b"tidemq.delivery-token.v1\0";

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "[u8; 32]", into = "[u8; 32]")]
pub struct SigningKey([u8; 32]);

impl TryFrom<[u8; 32]> for SigningKey {
    type Error = Rejection;

    fn try_from(bytes: [u8; 32]) -> Result<Self> {
        if bytes.iter().all(|byte| *byte == 0) {
            return Err(Rejection::InvalidToken);
        }
        Ok(Self(bytes))
    }
}

impl From<SigningKey> for [u8; 32] {
    fn from(key: SigningKey) -> Self {
        key.0
    }
}

impl fmt::Debug for SigningKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SigningKey([REDACTED])")
    }
}

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "Vec<u8>", into = "Vec<u8>")]
pub struct DeliveryToken(Vec<u8>);

impl TryFrom<Vec<u8>> for DeliveryToken {
    type Error = Rejection;

    fn try_from(bytes: Vec<u8>) -> Result<Self> {
        if bytes.len() != DELIVERY_TOKEN_BYTES {
            return Err(Rejection::InvalidToken);
        }
        Ok(Self(bytes))
    }
}

impl From<DeliveryToken> for Vec<u8> {
    fn from(token: DeliveryToken) -> Self {
        token.0
    }
}

impl fmt::Debug for DeliveryToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliveryToken([REDACTED])")
    }
}

impl DeliveryToken {
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Claims {
    pub topic_id: Id,
    pub partition_id: u32,
    pub subscription_id: Id,
    pub generations: Generations,
    pub offset: u64,
    pub attempt: u32,
}

impl SigningKey {
    pub(super) fn issue(&self, claims: Claims) -> DeliveryToken {
        let mut bytes = Vec::with_capacity(DELIVERY_TOKEN_BYTES);
        bytes.push(TOKEN_VERSION);
        bytes.extend_from_slice(claims.topic_id.as_bytes());
        bytes.extend_from_slice(&claims.partition_id.to_be_bytes());
        bytes.extend_from_slice(claims.subscription_id.as_bytes());
        bytes.extend_from_slice(&claims.generations.cursor.to_be_bytes());
        bytes.extend_from_slice(&claims.generations.assignment.to_be_bytes());
        bytes.extend_from_slice(&claims.offset.to_be_bytes());
        bytes.extend_from_slice(&u64::from(claims.attempt).to_be_bytes());
        let mac = self.mac(&bytes);
        bytes.extend_from_slice(mac.as_bytes());
        DeliveryToken(bytes)
    }

    pub(super) fn verify(&self, token: &DeliveryToken) -> Result<Claims> {
        let bytes = token.as_bytes();
        let body = bytes
            .get(..TOKEN_BODY_BYTES)
            .ok_or(Rejection::InvalidToken)?;
        let supplied = blake3::Hash::from_slice(
            bytes
                .get(TOKEN_BODY_BYTES..)
                .ok_or(Rejection::InvalidToken)?,
        )
        .map_err(|_| Rejection::InvalidToken)?;
        if self.mac(body) != supplied || bytes.first().copied() != Some(TOKEN_VERSION) {
            return Err(Rejection::InvalidToken);
        }
        decode_claims(body)
    }

    fn mac(&self, body: &[u8]) -> blake3::Hash {
        let mut hasher = blake3::Hasher::new_keyed(&self.0);
        hasher.update(TOKEN_DOMAIN);
        hasher.update(body);
        hasher.finalize()
    }
}

fn decode_claims(body: &[u8]) -> Result<Claims> {
    let mut fields = body.get(1..).ok_or(Rejection::InvalidToken)?;
    let topic_id = Id::from_slice(take(&mut fields, 16)?).map_err(|_| Rejection::InvalidToken)?;
    let partition_id = u32::from_be_bytes(array(take(&mut fields, 4)?)?);
    let subscription_id =
        Id::from_slice(take(&mut fields, 16)?).map_err(|_| Rejection::InvalidToken)?;
    let cursor = u64::from_be_bytes(array(take(&mut fields, 8)?)?);
    let assignment = u64::from_be_bytes(array(take(&mut fields, 8)?)?);
    let offset = u64::from_be_bytes(array(take(&mut fields, 8)?)?);
    let attempt = u32::try_from(u64::from_be_bytes(array(take(&mut fields, 8)?)?))
        .map_err(|_| Rejection::InvalidToken)?;
    if !fields.is_empty() || cursor == 0 || assignment == 0 || attempt == 0 {
        return Err(Rejection::InvalidToken);
    }
    Ok(Claims {
        topic_id,
        partition_id,
        subscription_id,
        generations: Generations { cursor, assignment },
        offset,
        attempt,
    })
}

fn take<'a>(remaining: &mut &'a [u8], count: usize) -> Result<&'a [u8]> {
    let field = remaining.get(..count).ok_or(Rejection::InvalidToken)?;
    *remaining = remaining.get(count..).ok_or(Rejection::InvalidToken)?;
    Ok(field)
}

fn array<const N: usize>(bytes: &[u8]) -> Result<[u8; N]> {
    bytes.try_into().map_err(|_| Rejection::InvalidToken)
}
