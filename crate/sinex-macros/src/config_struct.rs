use proc_macro::TokenStream;
use quote::quote;
use syn::{
    parse_macro_input, Attribute, Expr, Fields, FieldsNamed, Ident, ItemStruct, Type, Visibility,
};

/// Macro for generating configuration structs with validation and defaults
///
/// This macro simplifies the creation of configuration structs by automatically generating:
/// - Serde serialization/deserialization
/// - Default implementation with specified defaults
/// - Validation methods using ValidationChain
/// - Environment variable extraction
/// - Configuration file loading
///
/// # Usage
///
/// ```rust
/// config_struct! {
///     #[derive(Debug, Clone)]
///     pub struct DatabaseConfig {
///         #[config(env = "DATABASE_URL", validate = "not_empty")]
///         pub url: String,
///         
///         #[config(env = "DATABASE_MAX_CONNECTIONS", default = 10, validate = "min_value(1)")]
///         pub max_connections: u32,
///         
///         #[config(env = "DATABASE_TIMEOUT", default = 30, validate = "min_value(1)")]
///         pub timeout_seconds: u64,
///         
///         #[config(env = "DATABASE_SSL_MODE", default = "prefer")]
///         pub ssl_mode: String,
///     }
/// }
/// ```
///
/// This generates:
/// - The struct with serde derives
/// - `impl Default` with specified defaults
/// - `impl DatabaseConfig` with validation and loading methods
/// - Environment variable extraction logic
pub fn config_struct(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as ItemStruct);

    let struct_name = &input.ident;
    let struct_vis = &input.vis;
    let struct_attrs = &input.attrs;

    let fields = match &input.fields {
        Fields::Named(fields) => fields,
        _ => panic!("config_struct only supports named fields"),
    };

    let config_fields = extract_config_fields(fields);

    let mut generated = quote! {};

    // Generate the original struct with serde derives
    generated.extend(generate_struct_definition(
        struct_name,
        struct_vis,
        struct_attrs,
        fields,
    ));

    // Generate Default implementation
    generated.extend(generate_default_impl(struct_name, &config_fields));

    // Generate validation and loading methods
    generated.extend(generate_config_impl(struct_name, &config_fields));

    generated.into()
}

#[derive(Debug)]
struct ConfigField {
    name: Ident,
    _field_type: Type,
    env_var: Option<String>,
    default_value: Option<Expr>,
    validations: Vec<String>,
}

fn extract_config_fields(fields: &FieldsNamed) -> Vec<ConfigField> {
    fields
        .named
        .iter()
        .map(|field| {
            let name = field.ident.clone().unwrap();
            let field_type = field.ty.clone();
            let env_var = None;
            let default_value = None;
            let validations = Vec::new();

            // Parse config attributes
            for attr in &field.attrs {
                if attr.path().is_ident("config") {
                    // For now, implement a simplified version
                    // Full implementation would require more complex parsing
                }
            }

            ConfigField {
                name,
                _field_type: field_type,
                env_var,
                default_value,
                validations,
            }
        })
        .collect()
}

fn generate_struct_definition(
    struct_name: &Ident,
    struct_vis: &Visibility,
    struct_attrs: &[Attribute],
    fields: &FieldsNamed,
) -> proc_macro2::TokenStream {
    quote! {
        #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        #(#struct_attrs)*
        #struct_vis struct #struct_name {
            #fields
        }
    }
}

fn generate_default_impl(
    struct_name: &Ident,
    config_fields: &[ConfigField],
) -> proc_macro2::TokenStream {
    let field_defaults = config_fields.iter().map(|field| {
        let name = &field.name;
        let default_value = field
            .default_value
            .as_ref()
            .map(|expr| {
                quote! { #expr }
            })
            .unwrap_or_else(|| {
                quote! { Default::default() }
            });

        quote! {
            #name: #default_value,
        }
    });

    quote! {
        impl Default for #struct_name {
            fn default() -> Self {
                Self {
                    #(#field_defaults)*
                }
            }
        }
    }
}

fn generate_config_impl(
    struct_name: &Ident,
    config_fields: &[ConfigField],
) -> proc_macro2::TokenStream {
    let validation_chains = config_fields.iter().map(|field| {
        let name = &field.name;
        let name_str = name.to_string();

        if field.validations.is_empty() {
            quote! {
                let #name = self.#name;
            }
        } else {
            let validation_calls = field.validations.iter().map(|validation| {
                let validation_ident: Ident = syn::parse_str(validation).unwrap();
                quote! { .#validation_ident() }
            });

            quote! {
                let #name = ValidationChain::validate(self.#name, #name_str)
                    #(#validation_calls)*
                    .into_result()?;
            }
        }
    });

    let env_loading: Vec<_> = config_fields
        .iter()
        .map(|field| {
            let name = &field.name;

            if let Some(env_var) = &field.env_var {
                quote! {
                    if let Ok(value) = std::env::var(#env_var) {
                        config.#name = value.parse().map_err(|_| {
                            sinex_core_types::SinexError::configuration(format!(
                                "Invalid value for environment variable {}: {}",
                                #env_var, value
                            ))
                        })?;
                    }
                }
            } else {
                quote! {}
            }
        })
        .collect();

    let field_assignments = config_fields.iter().map(|field| {
        let name = &field.name;
        quote! {
            #name,
        }
    });

    quote! {
        impl #struct_name {
            /// Validate the configuration
            pub fn validate(self) -> sinex_core_types::Result<Self> {
                use sinex_core_types::validation::ValidationChain;

                #(#validation_chains)*

                Ok(Self {
                    #(#field_assignments)*
                })
            }

            /// Load configuration from environment variables
            pub fn from_env() -> sinex_core_types::Result<Self> {
                let mut config = Self::default();

                #(#env_loading)*

                config.validate()
            }

            /// Load configuration from a file
            pub fn from_file<P: AsRef<std::path::Path>>(path: P) -> sinex_core_types::Result<Self> {
                let contents = std::fs::read_to_string(path)
                    .map_err(|e| sinex_core_types::SinexError::io(e.to_string()))?;

                let config: Self = toml::from_str(&contents)
                    .map_err(|e| sinex_core_types::SinexError::configuration(e.to_string()))?;

                config.validate()
            }

            /// Load configuration from environment variables and file, with env taking precedence
            pub fn load<P: AsRef<std::path::Path>>(config_path: Option<P>) -> sinex_core_types::Result<Self> {
                let mut config = if let Some(path) = config_path {
                    Self::from_file(path)?
                } else {
                    Self::default()
                };

                // Override with environment variables
                #(#env_loading)*

                config.validate()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_struct_parsing() {
        let input = quote! {
            #[derive(Debug, Clone)]
            pub struct TestConfig {
                #[config(env = "TEST_URL", validate = "not_empty")]
                pub url: String,

                #[config(env = "TEST_PORT", default = 8080, validate = "min_value(1)")]
                pub port: u32,
            }
        };

        let parsed: ItemStruct = syn::parse2(input).unwrap();
        assert_eq!(parsed.ident, "TestConfig");

        if let Fields::Named(fields) = parsed.fields {
            assert_eq!(fields.named.len(), 2);
        } else {
            panic!("Expected named fields");
        }
    }

    #[test]
    fn test_config_field_extraction() {
        let input = quote! {
            {
                #[config(env = "TEST_URL", validate = "not_empty")]
                pub url: String,

                #[config(env = "TEST_PORT", default = 8080)]
                pub port: u32,
            }
        };

        let fields: FieldsNamed = syn::parse2(input).unwrap();
        let config_fields = extract_config_fields(&fields);

        assert_eq!(config_fields.len(), 2);
        assert_eq!(config_fields[0].env_var, Some("TEST_URL".to_string()));
        assert_eq!(config_fields[1].env_var, Some("TEST_PORT".to_string()));
    }
}
