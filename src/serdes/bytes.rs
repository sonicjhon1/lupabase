use crate::{Deserialize, Error, Result, Serialize};

/// Provides bytes Serialization / Deserialization
pub trait BytesSerde {
    /// Format used for Serializing / Deserializing
    const FORMAT: &str;

    /// Attempts to serialize the provided data as bytes
    ///
    /// # Errors
    /// - Parsing failure
    fn try_serialize_as_bytes<S: Serialize>(data: S) -> Result<Vec<u8>>;

    /// Attempts to deserialize the provided data from bytes
    ///
    /// # Errors
    /// - Parsing failure
    fn try_deserialize_from_bytes<'de, O: Deserialize<'de>>(bytes: &'de [u8]) -> Result<O>;
}

#[cfg(feature = "cbor")]
pub use cbor::*;

#[cfg(feature = "cbor")]
mod cbor {
    use super::*;

    pub struct CborSerde;

    impl BytesSerde for CborSerde {
        const FORMAT: &str = "cbor";

        fn try_serialize_as_bytes<S: Serialize>(data: S) -> Result<Vec<u8>> {
            minicbor_serde::to_vec(&data).map_err(|e| Error::SerializationFailure(Box::new(e)))
        }

        fn try_deserialize_from_bytes<'de, O: Deserialize<'de>>(bytes: &'de [u8]) -> Result<O> {
            minicbor_serde::from_slice(bytes)
                .map_err(|e| Error::DeserializationFailure(Box::new(e)))
        }
    }
}

#[cfg(feature = "json")]
pub use json::*;

#[cfg(feature = "json")]
mod json {
    use super::*;

    pub struct JsonSerde;

    impl BytesSerde for JsonSerde {
        const FORMAT: &str = "json";

        fn try_serialize_as_bytes<S: Serialize>(data: S) -> Result<Vec<u8>> {
            serde_json::to_vec(&data).map_err(|e| Error::SerializationFailure(Box::new(e)))
        }

        fn try_deserialize_from_bytes<'de, O: Deserialize<'de>>(bytes: &'de [u8]) -> Result<O> {
            serde_json::from_slice(bytes).map_err(|e| Error::DeserializationFailure(Box::new(e)))
        }
    }
}
