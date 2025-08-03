//! Macro for defining strongly-typed ID types based on ULID

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Ident};

pub fn define_id_type(input: TokenStream) -> TokenStream {
    let type_name = parse_macro_input!(input as Ident);

    let output = quote! {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
        #[serde(transparent)]
        pub struct #type_name(ulid::Ulid);

        impl #type_name {
            /// Create a new instance with a fresh ULID
            pub fn new() -> Self {
                Self(ulid::Ulid::new())
            }

            /// Get the underlying ULID
            pub fn as_ulid(&self) -> &ulid::Ulid {
                &self.0
            }

            /// Create from a string representation
            pub fn from_string(s: &str) -> Result<Self, ulid::DecodeError> {
                Ok(Self(ulid::Ulid::from_string(s)?))
            }

            /// Convert to string representation
            pub fn to_string(&self) -> String {
                self.0.to_string()
            }

            /// Create from a UUID (for database compatibility)
            pub fn from_uuid(uuid: uuid::Uuid) -> Self {
                Self(ulid::Ulid::from(uuid))
            }

            /// Convert to UUID (for database compatibility)
            pub fn to_uuid(&self) -> uuid::Uuid {
                self.0.into()
            }
        }

        impl Default for #type_name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl std::fmt::Display for #type_name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.0.fmt(f)
            }
        }

        impl std::str::FromStr for #type_name {
            type Err = ulid::DecodeError;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Self::from_string(s)
            }
        }

        impl From<ulid::Ulid> for #type_name {
            fn from(ulid: ulid::Ulid) -> Self {
                Self(ulid)
            }
        }

        impl From<#type_name> for ulid::Ulid {
            fn from(id: #type_name) -> Self {
                id.0
            }
        }

        impl From<uuid::Uuid> for #type_name {
            fn from(uuid: uuid::Uuid) -> Self {
                Self::from_uuid(uuid)
            }
        }

        impl From<#type_name> for uuid::Uuid {
            fn from(id: #type_name) -> Self {
                id.to_uuid()
            }
        }

        // sqlx implementations
        impl sqlx::Type<sqlx::Postgres> for #type_name {
            fn type_info() -> sqlx::postgres::PgTypeInfo {
                <uuid::Uuid as sqlx::Type<sqlx::Postgres>>::type_info()
            }
        }

        impl sqlx::Encode<'_, sqlx::Postgres> for #type_name {
            fn encode_by_ref(&self, buf: &mut sqlx::postgres::PgArgumentBuffer) -> sqlx::encode::IsNull {
                <uuid::Uuid as sqlx::Encode<sqlx::Postgres>>::encode_by_ref(&self.to_uuid(), buf)
            }
        }

        impl sqlx::Decode<'_, sqlx::Postgres> for #type_name {
            fn decode(value: sqlx::postgres::PgValueRef<'_>) -> Result<Self, sqlx::error::BoxDynError> {
                let uuid = <uuid::Uuid as sqlx::Decode<sqlx::Postgres>>::decode(value)?;
                Ok(Self::from_uuid(uuid))
            }
        }

        impl<'q> sqlx::encode::Encode<'q, sqlx::Postgres> for &'q #type_name {
            fn encode_by_ref(&self, buf: &mut <sqlx::Postgres as sqlx::database::HasArguments<'q>>::ArgumentBuffer) -> sqlx::encode::IsNull {
                <uuid::Uuid as sqlx::Encode<sqlx::Postgres>>::encode_by_ref(&self.to_uuid(), buf)
            }
        }

        // AsRef trait for convenience
        impl AsRef<ulid::Ulid> for #type_name {
            fn as_ref(&self) -> &ulid::Ulid {
                &self.0
            }
        }
    };

    TokenStream::from(output)
}
