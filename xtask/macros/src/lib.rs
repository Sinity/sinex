//! Procedural macros for Sinex test infrastructure
//!
//! This macro provides sophisticated test infrastructure for Sinex tests, including:
//! - Automatic Sandbox creation and cleanup
//! - Proptest integration with async runtime bridging
//! - Smart timeout handling based on test patterns
//! - Progress indicators for long-running tests
//! - Rich error reporting with timing information

use proc_macro::TokenStream;
use quote::{ToTokens, quote};
use syn::parse::Parser;
use syn::{
    Error, Expr, ExprLit, FnArg, ItemFn, Lit, Meta, MetaNameValue, Pat, PatType, Type, TypePath,
    parse_macro_input, punctuated::Punctuated, spanned::Spanned, token::Comma,
};

/// Configuration parsed from `sinex_test` attributes
#[derive(Debug)]
struct SinexTestConfig {
    timeout: Option<u64>,
    trace: bool,
    serial: bool,
}

/// Known `sinex_test` attribute names for error reporting.
const KNOWN_SINEX_TEST_ATTRS: &[&str] = &["timeout", "trace", "serial"];

fn parse_u64_lit(lit: &Lit, field_name: &str) -> Result<u64, Error> {
    match lit {
        Lit::Int(lit_int) => lit_int.base10_parse::<u64>().map_err(|error| {
            Error::new(
                lit_int.span(),
                format!("invalid `{field_name}` value: {error}"),
            )
        }),
        _ => Err(Error::new(
            lit.span(),
            format!("`{field_name}` must be an integer literal"),
        )),
    }
}

fn parse_bool_lit(lit: &Lit, field_name: &str) -> Result<bool, Error> {
    match lit {
        Lit::Bool(lit_bool) => Ok(lit_bool.value()),
        _ => Err(Error::new(
            lit.span(),
            format!("`{field_name}` must be a boolean literal"),
        )),
    }
}

fn lit_from_expr<'a>(expr: &'a Expr, field_name: &str) -> Result<&'a Lit, Error> {
    if let Expr::Lit(expr_lit) = expr {
        Ok(&expr_lit.lit)
    } else {
        Err(Error::new(
            expr.span(),
            format!("`{field_name}` must be a literal"),
        ))
    }
}

/// Parse `sinex_test` attributes.
/// Supports: timeout = 30, trace = true, serial = true
///
/// Returns a compile error for unknown attribute names, preventing
/// silent typo bugs like `#[sinex_test(timout = 30)]`.
fn parse_sinex_test_attrs_tokens(
    attr_tokens: proc_macro2::TokenStream,
) -> Result<SinexTestConfig, Error> {
    let mut config = SinexTestConfig {
        timeout: None,
        trace: false,
        serial: false,
    };

    if attr_tokens.is_empty() {
        return Ok(config);
    }

    if let Ok(parsed) = Punctuated::<Meta, Comma>::parse_terminated.parse2(attr_tokens.clone()) {
        for meta in parsed {
            match meta {
                Meta::NameValue(MetaNameValue { path, value, .. }) if path.is_ident("timeout") => {
                    let lit = match &value {
                        Expr::Lit(ExprLit { lit, .. }) => lit,
                        _ => {
                            let error =
                                Error::new(value.span(), "`timeout` must be an integer literal");
                            return Err(error);
                        }
                    };
                    match parse_u64_lit(lit, "timeout") {
                        Ok(timeout) => {
                            config.timeout = Some(timeout);
                        }
                        Err(error) => return Err(error),
                    }
                }
                Meta::NameValue(MetaNameValue { path, value, .. }) if path.is_ident("trace") => {
                    let lit = match &value {
                        Expr::Lit(ExprLit { lit, .. }) => lit,
                        _ => {
                            let error =
                                Error::new(value.span(), "`trace` must be a boolean literal");
                            return Err(error);
                        }
                    };
                    match parse_bool_lit(lit, "trace") {
                        Ok(trace) => {
                            config.trace = trace;
                        }
                        Err(error) => return Err(error),
                    }
                }
                Meta::NameValue(MetaNameValue { path, value, .. }) if path.is_ident("serial") => {
                    let lit = match &value {
                        Expr::Lit(ExprLit { lit, .. }) => lit,
                        _ => {
                            let error =
                                Error::new(value.span(), "`serial` must be a boolean literal");
                            return Err(error);
                        }
                    };
                    match parse_bool_lit(lit, "serial") {
                        Ok(serial) => {
                            config.serial = serial;
                        }
                        Err(error) => return Err(error),
                    }
                }
                Meta::Path(path) if path.is_ident("trace") => {
                    config.trace = true;
                }
                Meta::Path(path) if path.is_ident("serial") => {
                    config.serial = true;
                }
                other => {
                    let name = match &other {
                        Meta::Path(p) | Meta::NameValue(MetaNameValue { path: p, .. }) => {
                            p.to_token_stream().to_string()
                        }
                        Meta::List(l) => l.path.to_token_stream().to_string(),
                    };
                    let err = Error::new(
                        other.span(),
                        format!(
                            "unknown sinex_test attribute `{name}`. \
                             Known attributes: {}",
                            KNOWN_SINEX_TEST_ATTRS.join(", ")
                        ),
                    );
                    return Err(err);
                }
            }
        }
        return Ok(config);
    }

    // Also try simple name=value parsing
    if let Ok(Meta::NameValue(nv)) = syn::parse2::<Meta>(attr_tokens) {
        if nv.path.is_ident("timeout") {
            match lit_from_expr(&nv.value, "timeout").and_then(|lit| parse_u64_lit(lit, "timeout"))
            {
                Ok(timeout) => {
                    config.timeout = Some(timeout);
                }
                Err(error) => return Err(error),
            }
        } else if nv.path.is_ident("trace") {
            match lit_from_expr(&nv.value, "trace").and_then(|lit| parse_bool_lit(lit, "trace")) {
                Ok(trace) => {
                    config.trace = trace;
                }
                Err(error) => return Err(error),
            }
        } else if nv.path.is_ident("serial") {
            match lit_from_expr(&nv.value, "serial").and_then(|lit| parse_bool_lit(lit, "serial")) {
                Ok(serial) => {
                    config.serial = serial;
                }
                Err(error) => return Err(error),
            }
        } else {
            let name = nv.path.to_token_stream().to_string();
            let err = Error::new(
                nv.path.span(),
                format!(
                    "unknown sinex_test attribute `{name}`. \
                     Known attributes: {}",
                    KNOWN_SINEX_TEST_ATTRS.join(", ")
                ),
            );
            return Err(err);
        }
        return Ok(config);
    }

    Err(Error::new(
        proc_macro2::Span::call_site(),
        "failed to parse sinex_test attributes — expected e.g. #[sinex_test(timeout = 30, trace, serial)]",
    ))
}

fn parse_sinex_test_attrs(attr: TokenStream) -> std::result::Result<SinexTestConfig, TokenStream> {
    parse_sinex_test_attrs_tokens(proc_macro2::TokenStream::from(attr))
        .map_err(|error| error.to_compile_error().into())
}

// ---------------------------------------------------------------------------
// sinex_prop attribute macro
// ---------------------------------------------------------------------------

#[proc_macro_attribute]
pub fn sinex_prop(attr: TokenStream, item: TokenStream) -> TokenStream {
    use proc_macro2::TokenStream as TS;
    use quote::quote;

    #[derive(Default)]
    struct PropOpts {
        cases: Option<u32>,
        timeout_secs: Option<u64>,
        trace: bool,
        seed: Option<u64>,
        max_shrink_time_ms: Option<u64>,
    }

    fn parse_u32(lit: &Lit) -> Result<u32, Error> {
        match lit {
            Lit::Int(i) => i.base10_parse::<u32>().map_err(|e| Error::new(i.span(), e)),
            _ => Err(Error::new(lit.span(), "expected integer")),
        }
    }

    fn parse_u64(lit: &Lit) -> Result<u64, Error> {
        match lit {
            Lit::Int(i) => i.base10_parse::<u64>().map_err(|e| Error::new(i.span(), e)),
            _ => Err(Error::new(lit.span(), "expected integer")),
        }
    }

    fn parse_timeout(lit: &Lit) -> Result<u64, Error> {
        match lit {
            Lit::Int(i) => i.base10_parse::<u64>().map_err(|e| Error::new(i.span(), e)),
            Lit::Str(s) => {
                let raw = s.value();
                if let Some(num) = raw.trim().strip_suffix('s') {
                    num.parse::<u64>().map_err(|e| Error::new(s.span(), e))
                } else {
                    raw.trim()
                        .parse::<u64>()
                        .map_err(|e| Error::new(s.span(), e))
                }
            }
            _ => Err(Error::new(lit.span(), r#"expected seconds or "30s""#)),
        }
    }

    fn lit_from_expr(expr: &Expr) -> Result<&Lit, Error> {
        if let Expr::Lit(expr_lit) = expr {
            Ok(&expr_lit.lit)
        } else {
            Err(Error::new(expr.span(), "expected literal"))
        }
    }

    fn is_context(ty: &Type) -> bool {
        match ty {
            Type::Reference(r) => is_context(&r.elem),
            Type::Path(TypePath { path, .. }) => path
                .segments
                .last()
                .is_some_and(|seg| seg.ident == "Sandbox" || seg.ident == "TestContext"),
            _ => false,
        }
    }

    let attr_tokens = proc_macro2::TokenStream::from(attr);
    let opts = (|| -> Result<PropOpts, Error> {
        let mut o = PropOpts::default();
        if attr_tokens.is_empty() {
            return Ok(o);
        }

        let parsed = Punctuated::<Meta, Comma>::parse_terminated.parse2(attr_tokens.clone())?;

        for meta in parsed {
            match meta {
                Meta::NameValue(MetaNameValue { path, value, .. }) if path.is_ident("cases") => {
                    let lit = lit_from_expr(&value)?;
                    o.cases = Some(parse_u32(lit)?);
                }
                Meta::NameValue(MetaNameValue { path, value, .. }) if path.is_ident("timeout") => {
                    let lit = lit_from_expr(&value)?;
                    o.timeout_secs = Some(parse_timeout(lit)?);
                }
                Meta::Path(p) if p.is_ident("trace") => {
                    o.trace = true;
                }
                Meta::NameValue(MetaNameValue { path, value, .. }) if path.is_ident("seed") => {
                    let lit = lit_from_expr(&value)?;
                    o.seed = Some(parse_u64(lit)?);
                }
                Meta::NameValue(MetaNameValue { path, value, .. })
                    if path.is_ident("max_shrink_time_ms") =>
                {
                    let lit = lit_from_expr(&value)?;
                    o.max_shrink_time_ms = Some(parse_u64(lit)?);
                }
                other => return Err(Error::new(other.span(), "unknown sinex_prop option")),
            }
        }
        Ok(o)
    })();
    let opts = match opts {
        Ok(v) => v,
        Err(e) => return e.to_compile_error().into(),
    };

    let input = parse_macro_input!(item as ItemFn);
    let fn_name = &input.sig.ident;
    let fn_vis = &input.vis;
    let user_body = &input.block;
    let is_async = input.sig.asyncness.is_some();

    // Collect parameters
    struct Param {
        pat: Pat,
        ty: Option<Type>,
        strat: TS,
    }

    let mut ctx_param: Option<(Pat, Type)> = None;
    let mut params: Vec<Param> = Vec::new();

    for (idx, arg) in input.sig.inputs.iter().enumerate() {
        let FnArg::Typed(PatType { pat, ty, attrs, .. }) = arg else {
            return Error::new_spanned(arg, "methods are not supported in #[sinex_prop]")
                .to_compile_error()
                .into();
        };

        if is_context(ty.as_ref()) {
            if idx != 0 {
                return Error::new_spanned(arg, "Sandbox must appear first")
                    .to_compile_error()
                    .into();
            }
            if ctx_param.is_some() {
                return Error::new_spanned(arg, "duplicate Sandbox parameter")
                    .to_compile_error()
                    .into();
            }
            if !matches!(ty.as_ref(), Type::Reference(_)) {
                return Error::new_spanned(ty, "Sandbox parameter must be a reference")
                    .to_compile_error()
                    .into();
            }
            ctx_param = Some(((**pat).clone(), ty.as_ref().clone()));
            // Context parameter doesn't need a strategy - skip to next param
            continue;
        }

        let mut strategy = None;
        for a in attrs {
            if a.path().is_ident("strategy") {
                strategy = Some(match a.parse_args::<TS>() {
                    Ok(ts) => ts,
                    Err(e) => return e.to_compile_error().into(),
                });
                break;
            }
        }
        let Some(strat) = strategy else {
            return Error::new_spanned(pat, "each parameter needs #[strategy(...)]")
                .to_compile_error()
                .into();
        };

        params.push(Param {
            pat: (**pat).clone(),
            ty: Some(ty.as_ref().clone()),
            strat,
        });
    }

    if params.is_empty() {
        return Error::new_spanned(
            &input.sig,
            "#[sinex_prop] requires at least one #[strategy] parameter",
        )
        .to_compile_error()
        .into();
    }

    if ctx_param.is_some() && !is_async {
        return Error::new_spanned(
            input.sig.fn_token,
            "Sandbox requires async #[sinex_prop] tests",
        )
        .to_compile_error()
        .into();
    }

    let timeout_secs = opts.timeout_secs.unwrap_or(if is_async { 60 } else { 20 }); // Increased from 30/10
    let cases = opts.cases.unwrap_or(256);
    let trace_stmt = if opts.trace {
        quote!( ::xtask::sandbox::Sandbox::init_tracing("debug"); )
    } else {
        quote!()
    };

    let seed_stmt = opts.seed.map_or_else(
        || {
            quote! {
                if let Some(seed_env) = std::env::var("SINEX_PROPTEST_SEED")
                    .ok()
                    .and_then(|s| s.parse::<u64>().ok())
                {
                    cfg.rng_algorithm = ::proptest::test_runner::RngAlgorithm::ChaCha;
                    cfg.rng_seed = ::proptest::test_runner::RngSeed::Fixed(seed_env);
                }
            }
        },
        |seed| {
            quote! {
                cfg.rng_algorithm = ::proptest::test_runner::RngAlgorithm::ChaCha;
                cfg.rng_seed = ::proptest::test_runner::RngSeed::Fixed(#seed);
            }
        },
    );

    let shrink_stmt = opts.max_shrink_time_ms.map_or_else(
        || quote!(),
        |ms| {
            quote! {
                let shrink = (#ms).min(u32::MAX as u64) as u32;
                cfg.max_shrink_time = shrink.max(1);
            }
        },
    );

    let strategy_expr = if params.len() == 1 {
        params[0].strat.clone()
    } else {
        let parts = params.iter().map(|p| &p.strat);
        quote!( ( #( #parts ),* ) )
    };

    let arg_idents: Vec<_> = (0..params.len())
        .map(|idx| syn::Ident::new(&format!("__prop_arg{idx}"), proc_macro2::Span::call_site()))
        .collect();

    let destructures: Vec<TS> = params
        .iter()
        .enumerate()
        .map(|(idx, param)| {
            let ident = &arg_idents[idx];
            let pat = &param.pat;
            let ty = param
                .ty
                .as_ref()
                .map(|ty| quote!( : #ty ))
                .unwrap_or_default();
            quote!( let #pat #ty = #ident; )
        })
        .collect();

    let tuple_unpack = if params.len() == 1 {
        let ident = &arg_idents[0];
        quote!( let #ident = value; )
    } else {
        quote!( let ( #( #arg_idents ),* ) = value; )
    };

    let ctx_binding = ctx_param.as_ref().map_or_else(
        || quote!(),
        |(pat, ty)| {
            quote! {
                let ctx_ref: #ty = ctx_holder.as_ref().expect("Sandbox available");
                let #pat = ctx_ref;
            }
        },
    );
    let expects_ctx = ctx_param.is_some();

    let runner_setup = quote! {
        use ::proptest::prelude::*;
        let mut cfg = ::xtask::sandbox::sinex_prop_runner_config(#cases, module_path!(), test_name);
        #seed_stmt
        #shrink_stmt
        let mut runner = ::proptest::test_runner::TestRunner::new(cfg);
    };

    let async_body = {
        quote! {
            #trace_stmt
            let test_name = stringify!(#fn_name);
            let start = std::time::Instant::now();
            eprintln!("🔄 {} [prop, timeout: {}s, cases: {}]", test_name.replace('_', " "), #timeout_secs, #cases);
            let _test_temp_env = ::xtask::sandbox::prepare_test_temp_env(test_name)?;
            let ctx_holder = if #expects_ctx {
                Some(::xtask::sandbox::Sandbox::with_name(test_name).await?)
            } else {
                None
            };
            #runner_setup
            let strategy = #strategy_expr;
            let __sinex_prop_handle = tokio::runtime::Handle::current();
            let result = runner.run(&strategy, |value| {
                let handle = __sinex_prop_handle.clone();
                match handle.runtime_flavor() {
                    tokio::runtime::RuntimeFlavor::MultiThread => tokio::task::block_in_place(|| {
                        handle.block_on(async {
                            let fut = async {
                                #tuple_unpack
                                #ctx_binding
                                #( #destructures )*
                                #user_body
                            };
                            match tokio::time::timeout(
                                std::time::Duration::from_secs(#timeout_secs),
                                fut,
                            ).await {
                                Ok(Ok(())) => Ok(()),
                                Ok(Err(err)) => Err(::proptest::test_runner::TestCaseError::fail(format!("{err:?}"))),
                                Err(_) => Err(::proptest::test_runner::TestCaseError::fail(format!("case timed out after {}s", #timeout_secs))),
                            }
                        })
                    }),
                    _ => {
                        let _guard = handle.enter();
                        futures::executor::block_on(async {
                            let fut = async {
                                #tuple_unpack
                                #ctx_binding
                                #( #destructures )*
                                #user_body
                            };
                            match tokio::time::timeout(
                                std::time::Duration::from_secs(#timeout_secs),
                                fut,
                            ).await {
                                Ok(Ok(())) => Ok(()),
                                Ok(Err(err)) => Err(::proptest::test_runner::TestCaseError::fail(format!("{err:?}"))),
                                Err(_) => Err(::proptest::test_runner::TestCaseError::fail(format!("case timed out after {}s", #timeout_secs))),
                            }
                        })
                    }
                }
            });
            let ctx_snapshot_ref = ctx_holder.as_ref();
            let elapsed = start.elapsed();
            match result {
                Ok(_) => {
                    eprintln!("✅ {} ({:.1?})", test_name.replace('_', " "), elapsed);
                    Ok(())
                }
                Err(err) => {
                    eprintln!("❌ {} ({:.1?})", test_name.replace('_', " "), elapsed);
                    let failure_ctx = ctx_snapshot_ref
                        .map(|ctx| ::xtask::sandbox::snapshot_helper::FailureContext::Borrowed(ctx))
                        .unwrap_or(::xtask::sandbox::snapshot_helper::FailureContext::None);
                    ::xtask::sandbox::snapshot_helper::persist_failure(
                        test_name,
                        format!("{err:?}"),
                        failure_ctx,
                    );
                    Err(::color_eyre::eyre::eyre!("{err}"))
                }
            }
        }
    };

    let sync_body = {
        quote! {
            #trace_stmt
            let test_name = stringify!(#fn_name);
            let start = std::time::Instant::now();
            eprintln!("🔄 {} [prop, cases: {}]", test_name.replace('_', " "), #cases);
            #runner_setup
            let strategy = #strategy_expr;
            let result = runner.run(&strategy, |value| {
                #tuple_unpack
                #( #destructures )*
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| { #user_body }))
                .map_err(|_| ::proptest::test_runner::TestCaseError::fail("panic in property"))
                .and_then(|res| {
                    res.map_err(|err| ::proptest::test_runner::TestCaseError::fail(format!("{err:?}")))
                })
            });
            let elapsed = start.elapsed();
            match result {
                Ok(_) => {
                    eprintln!("✅ {} ({:.1?})", test_name.replace('_', " "), elapsed);
                    Ok(())
                }
                Err(err) => {
                    eprintln!("❌ {} ({:.1?})", test_name.replace('_', " "), elapsed);
                    ::xtask::sandbox::snapshot_helper::persist_failure(
                        test_name,
                        format!("{err:?}"),
                        ::xtask::sandbox::snapshot_helper::FailureContext::None,
                    );
                    Err(::color_eyre::eyre::eyre!("{err}"))
                }
            }
        }
    };

    let expanded = if is_async {
        quote! {
            #fn_vis
            #[tokio::test(flavor = "multi_thread")]
            async fn #fn_name() -> ::xtask::sandbox::TestResult<()> {
                #async_body
            }
        }
    } else {
        quote! {
            #fn_vis
            #[test]
            fn #fn_name() -> ::xtask::sandbox::TestResult<()> {
                #sync_body
            }
        }
    };

    expanded.into()
}

// ---------------------------------------------------------------------------
// sinex_proptest block macro
// ---------------------------------------------------------------------------

#[proc_macro]
pub fn sinex_proptest(input: TokenStream) -> TokenStream {
    use proc_macro2::TokenStream as TS;
    use quote::quote;
    use syn::{
        Attribute, Expr, Ident, Pat, Result, Token, Type,
        parse::{Parse, ParseStream},
    };

    #[derive(Default)]
    struct Defaults {
        cases: Option<TS>,
        timeout: Option<TS>,
        trace: bool,
        seed: Option<TS>,
    }

    struct TestCase {
        attrs: Vec<Attribute>,
        name: Ident,
        ctx: Option<(Pat, Type)>,
        params: Vec<(Pat, Option<Type>, Expr)>,
        body: syn::Block,
    }

    struct BlockDecl {
        defaults: Defaults,
        tests: Vec<TestCase>,
    }

    impl Parse for BlockDecl {
        fn parse(input: ParseStream<'_>) -> Result<Self> {
            let mut defaults = Defaults::default();
            for attr in input.call(Attribute::parse_inner)? {
                let path = attr.path();
                if path.is_ident("cases") {
                    defaults.cases = Some(attr.parse_args()?);
                } else if path.is_ident("timeout") {
                    defaults.timeout = Some(attr.parse_args()?);
                } else if path.is_ident("trace") {
                    defaults.trace = true;
                } else if path.is_ident("seed") {
                    defaults.seed = Some(attr.parse_args()?);
                } else {
                    return Err(syn::Error::new_spanned(attr, "unknown block attribute"));
                }
            }

            let mut tests = Vec::new();
            while !input.is_empty() {
                let attrs = input.call(Attribute::parse_outer)?;
                input.parse::<Token![fn]>()?;
                let name: Ident = input.parse()?;
                let content;
                syn::parenthesized!(content in input);
                let mut ctx = None;
                let mut params = Vec::new();
                while !content.is_empty() {
                    let pat: Pat = content.call(Pat::parse_single)?;
                    let ty = if content.peek(Token![:]) {
                        content.parse::<Token![:]>()?;
                        Some(content.parse::<Type>()?)
                    } else {
                        None
                    };
                    if content.peek(Token![in]) {
                        content.parse::<Token![in]>()?;
                        let strat: Expr = content.parse()?;
                        params.push((pat, ty, strat));
                    } else {
                        if ctx.is_some() {
                            return Err(syn::Error::new_spanned(
                                &pat,
                                "only one context parameter supported",
                            ));
                        }
                        let Some(ty) = ty else {
                            return Err(syn::Error::new_spanned(
                                &pat,
                                "context parameter needs a type",
                            ));
                        };
                        ctx = Some((pat, ty));
                    }
                    if content.peek(Token![,]) {
                        content.parse::<Token![,]>()?;
                    }
                }

                if input.peek(Token![->]) {
                    input.parse::<Token![->]>()?;
                    input.parse::<Type>()?;
                }

                let body: syn::Block = input.parse()?;
                tests.push(TestCase {
                    attrs,
                    name,
                    ctx,
                    params,
                    body,
                });
            }

            Ok(BlockDecl { defaults, tests })
        }
    }

    let block = match syn::parse::<BlockDecl>(input) {
        Ok(b) => b,
        Err(e) => return e.to_compile_error().into(),
    };

    let mut out = Vec::<TS>::new();
    for test in block.tests {
        let TestCase {
            attrs,
            name,
            ctx,
            params,
            body,
        } = test;

        let mut meta_entries = Vec::<TS>::new();
        if let Some(c) = &block.defaults.cases {
            meta_entries.push(quote!(cases = #c));
        }
        if let Some(t) = &block.defaults.timeout {
            meta_entries.push(quote!(timeout = #t));
        }
        if block.defaults.trace {
            meta_entries.push(quote!(trace));
        }
        if let Some(s) = &block.defaults.seed {
            meta_entries.push(quote!(seed = #s));
        }

        let mut passthrough_attrs = Vec::new();
        for attr in &attrs {
            if attr.path().is_ident("cases") {
                let lit: TS = attr.parse_args().unwrap();
                meta_entries.push(quote!(cases = #lit));
            } else if attr.path().is_ident("timeout") {
                let lit: TS = attr.parse_args().unwrap();
                meta_entries.push(quote!(timeout = #lit));
            } else if attr.path().is_ident("trace") {
                meta_entries.push(quote!(trace));
            } else if attr.path().is_ident("seed") {
                let lit: TS = attr.parse_args().unwrap();
                meta_entries.push(quote!(seed = #lit));
            } else {
                passthrough_attrs.push(attr.clone());
            }
        }

        let meta_tokens = if meta_entries.is_empty() {
            quote!()
        } else {
            quote!(( #(#meta_entries),* ))
        };

        let mut param_defs = Vec::<TS>::new();
        let mut destructures = Vec::<TS>::new();
        for (idx, (pat, ty, strat)) in params.iter().enumerate() {
            let ident = syn::Ident::new(&format!("__arg{idx}"), proc_macro2::Span::call_site());
            let ty_tokens = ty.as_ref().map_or_else(|| quote!(_), |t| quote!(#t));
            param_defs.push(quote!( #[strategy(#strat)] #ident: #ty_tokens ));
            let ty_ann = ty.as_ref().map(|t| quote!( : #t )).unwrap_or_default();
            destructures.push(quote!( let #pat #ty_ann = #ident; ));
        }

        let ctx_tokens = ctx
            .as_ref()
            .map_or_else(|| quote!(), |(pat, ty)| quote!( #pat: #ty, ));

        out.push(quote! {
            #(#passthrough_attrs)*
            #[::xtask::sandbox::sinex_prop #meta_tokens]
            fn #name( #ctx_tokens #( #param_defs ),* ) -> ::xtask::sandbox::TestResult<()> {
                #( #destructures )*
                #body
            }
        });
    }

    quote!( #( #out )* ).into()
}

#[proc_macro_attribute]
pub fn sinex_test(attr: TokenStream, item: TokenStream) -> TokenStream {
    let config = match parse_sinex_test_attrs(attr) {
        Ok(c) => c,
        Err(err) => return err,
    };
    let input = parse_macro_input!(item as ItemFn);
    expand_sinex_test(config, input)
}

#[proc_macro_attribute]
pub fn sinex_serial_test(attr: TokenStream, item: TokenStream) -> TokenStream {
    let mut config = match parse_sinex_test_attrs(attr) {
        Ok(c) => c,
        Err(err) => return err,
    };
    config.serial = true;
    let input = parse_macro_input!(item as ItemFn);
    expand_sinex_test(config, input)
}

/// Classify function attributes into rstest, case, test-control, and other categories.
struct ClassifiedAttrs {
    case_attrs: Vec<syn::Attribute>,
    other_attrs: Vec<syn::Attribute>,
    test_attrs: Vec<syn::Attribute>,
    has_rstest_cases: bool,
    has_rstest_attr: bool,
}

fn classify_attrs(input: &ItemFn) -> ClassifiedAttrs {
    let mut result = ClassifiedAttrs {
        case_attrs: Vec::new(),
        other_attrs: Vec::new(),
        test_attrs: Vec::new(),
        has_rstest_cases: false,
        has_rstest_attr: false,
    };

    for attr in &input.attrs {
        let is_case = attr
            .path()
            .segments
            .last()
            .is_some_and(|seg| seg.ident == "case");

        if is_case {
            result.has_rstest_cases = true;
            result.case_attrs.push(attr.clone());
        } else if attr.path().is_ident("rstest") {
            result.has_rstest_attr = true;
            result.has_rstest_cases = true;
            result.other_attrs.push(attr.clone());
        } else if attr.path().is_ident("ignore") || attr.path().is_ident("should_panic") {
            result.test_attrs.push(attr.clone());
            result.other_attrs.push(attr.clone());
        } else {
            result.other_attrs.push(attr.clone());
        }
    }

    // Also check if any parameters have #[case] attribute
    for arg in &input.sig.inputs {
        if let syn::FnArg::Typed(pat_type) = arg {
            for attr in &pat_type.attrs {
                let is_case = attr
                    .path()
                    .segments
                    .last()
                    .is_some_and(|seg| seg.ident == "case");
                if is_case {
                    result.has_rstest_cases = true;
                }
            }
        }
    }

    result
}

/// Check if a type is Sandbox/TestContext (handles both `Type::Path` and `Type::Reference`).
fn is_context_type(ty: &syn::Type) -> bool {
    match ty {
        syn::Type::Reference(r) => is_context_type(&r.elem),
        syn::Type::Path(type_path) => type_path
            .path
            .segments
            .last()
            .is_some_and(|seg| seg.ident == "Sandbox" || seg.ident == "TestContext"),
        _ => false,
    }
}

/// Check if function return type is Result<()> or `TestResult`<T>.
fn has_result_return_type(output: &syn::ReturnType) -> bool {
    if let syn::ReturnType::Type(_, ty) = output
        && let syn::Type::Path(type_path) = ty.as_ref()
    {
        return type_path.path.segments.last().is_some_and(|seg| {
            let ident = &seg.ident;
            ident == "Result" || ident == "TestResult"
        });
    }
    false
}

/// Generate the serial guard token stream (empty if serial is disabled).
fn serial_guard_tokens(enable_serial: bool) -> proc_macro2::TokenStream {
    if enable_serial {
        quote! {
            let _serial_guard = ::xtask::sandbox::acquire_pool_test_guard().await?;
        }
    } else {
        quote! {}
    }
}

/// Generate rstest-compatible test output.
fn expand_rstest_variant(
    input: &ItemFn,
    attrs: &ClassifiedAttrs,
    timeout_secs: u64,
    enable_serial: bool,
    enable_tracing: bool,
) -> TokenStream {
    let fn_name = &input.sig.ident;
    let fn_vis = &input.vis;
    let fn_body = &input.block;

    // Build the function signature without Sandbox (if present)
    let mut filtered_inputs = Vec::new();
    let mut has_ctx_param = false;

    for arg in &input.sig.inputs {
        if let syn::FnArg::Typed(pat_type) = arg
            && let syn::Type::Path(type_path) = pat_type.ty.as_ref()
            && type_path
                .path
                .segments
                .last()
                .is_some_and(|seg| seg.ident == "Sandbox")
        {
            has_ctx_param = true;
            continue;
        }
        filtered_inputs.push(arg.clone());
    }

    let mut new_sig = input.sig.clone();
    new_sig.inputs = filtered_inputs.into_iter().collect();

    let serial_guard = serial_guard_tokens(enable_serial);
    let tracing_block = if enable_tracing {
        quote! { ::xtask::Sandbox::init_tracing("debug"); }
    } else {
        quote! {}
    };

    let future_body = if has_ctx_param {
        quote! {
            #serial_guard
            #tracing_block
            let _test_temp_env = ::xtask::sandbox::prepare_test_temp_env(test_name)?;
            let ctx = ::xtask::Sandbox::with_name(test_name).await?;
            async { #fn_body }.await
        }
    } else {
        quote! {
            #serial_guard
            #tracing_block
            let _test_temp_env = ::xtask::sandbox::prepare_test_temp_env(test_name)?;
            async { #fn_body }.await
        }
    };

    let rstest_attr = if attrs.has_rstest_attr {
        quote! {}
    } else {
        quote! { #[::rstest::rstest] }
    };

    let case_attrs = &attrs.case_attrs;
    let other_attrs = &attrs.other_attrs;

    quote! {
        #rstest_attr
        #(#case_attrs)*
        #(#other_attrs)*
        #[tokio::test]
        #fn_vis #new_sig {
            let test_name = stringify!(#fn_name);
            let start = ::std::time::Instant::now();
            eprintln!("🔄 {} [rstest case, timeout: {}s]", test_name.replace('_', " "), #timeout_secs);

            let result = tokio::time::timeout(
                std::time::Duration::from_secs(#timeout_secs),
                async { #future_body }
            ).await
            .map_err(|_| ::color_eyre::eyre::eyre!("Test timed out after {} seconds", #timeout_secs))?;

            let elapsed = start.elapsed();
            match &result {
                Ok(_) => {
                    eprintln!("✅ {} ({:.1?})", test_name.replace('_', " "), elapsed);
                }
                Err(err) => {
                    eprintln!("❌ {} ({:.1?})", test_name.replace('_', " "), elapsed);
                    ::xtask::sandbox::snapshot_helper::persist_failure(
                        test_name,
                        format!("{err:?}"),
                        ::xtask::sandbox::snapshot_helper::FailureContext::None,
                    );
                }
            }
            result
        }
    }.into()
}

/// Generate async test with Sandbox context.
fn expand_async_context_test(
    input: &ItemFn,
    test_attrs: &[syn::Attribute],
    fn_body: &syn::Block,
    timeout_secs: u64,
    enable_serial: bool,
) -> proc_macro2::TokenStream {
    let fn_name = &input.sig.ident;
    let fn_vis = &input.vis;
    let serial_guard = serial_guard_tokens(enable_serial);

    quote! {
        #(#test_attrs)*
        #[tokio::test]
        #fn_vis async fn #fn_name() -> ::xtask::sandbox::TestResult<()> {
            let test_future = async {
                let test_name = stringify!(#fn_name);
                let start = std::time::Instant::now();
                eprintln!("🔄 {} [timeout: {}s]", test_name.replace('_', " "), #timeout_secs);
                #serial_guard
                let _test_temp_env = ::xtask::sandbox::prepare_test_temp_env(test_name)?;

                let ctx = ::xtask::Sandbox::with_name(test_name).await?;
                let ctx_failure_snapshot = ctx.failure_snapshot();

                // Progress reporter: only spawn for genuinely long timeouts.
                // The first tick fires after 10s, so tests under 10s never see it.
                let result: ::xtask::sandbox::TestResult<()> = if #timeout_secs > 30 {
                    let progress_task = tokio::spawn(async {
                        let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
                        interval.tick().await; // first tick is immediate, skip it
                        let mut elapsed_secs = 0u64;
                        loop {
                            interval.tick().await;
                            elapsed_secs += 10;
                            eprintln!("  ⏳ {} still running... ({}s elapsed)", test_name.replace('_', " "), elapsed_secs);
                            if elapsed_secs >= #timeout_secs - 10 {
                                break;
                            }
                        }
                    });

                    let test_result = async { #fn_body }.await;
                    progress_task.abort();
                    test_result
                } else {
                    async { #fn_body }.await
                };

                let elapsed = start.elapsed();
                match &result {
                    Ok(_) => {
                        eprintln!("✅ {} ({:.1?})", test_name.replace('_', " "), elapsed);
                    }
                    Err(err) => {
                        eprintln!("❌ {} ({:.1?})", test_name.replace('_', " "), elapsed);
                        ::xtask::sandbox::snapshot_helper::persist_failure(
                            test_name,
                            format!("{err:?}"),
                            ::xtask::sandbox::snapshot_helper::FailureContext::Snapshot(
                                ctx_failure_snapshot.clone(),
                            ),
                        );
                    }
                }
                result
            };

            tokio::time::timeout(
                std::time::Duration::from_secs(#timeout_secs),
                test_future
            )
            .await
            .map_err(|_| ::color_eyre::eyre::eyre!("Test timed out after {} seconds", #timeout_secs))?
        }
    }
}

/// Generate simple async test (no Sandbox context).
fn expand_simple_async_test(
    input: &ItemFn,
    test_attrs: &[syn::Attribute],
    fn_body: &syn::Block,
    timeout_secs: u64,
    enable_serial: bool,
) -> proc_macro2::TokenStream {
    let fn_name = &input.sig.ident;
    let fn_vis = &input.vis;
    let serial_guard = serial_guard_tokens(enable_serial);

    quote! {
        #(#test_attrs)*
        #[tokio::test]
        #fn_vis async fn #fn_name() -> ::xtask::sandbox::TestResult<()> {
            let test_future = async {
                let test_name = stringify!(#fn_name);
                let start = std::time::Instant::now();
                eprintln!("🔄 {} [simple, timeout: {}s]", test_name.replace('_', " "), #timeout_secs);
                #serial_guard
                let _test_temp_env = ::xtask::sandbox::prepare_test_temp_env(test_name)?;

                let result = async {
                    #fn_body
                }.await;

                let elapsed = start.elapsed();
                match &result {
                    Ok(_) => {
                        eprintln!("✅ {} ({:.1?})", test_name.replace('_', " "), elapsed);
                    }
                    Err(err) => {
                        eprintln!("❌ {} ({:.1?})", test_name.replace('_', " "), elapsed);
                        ::xtask::sandbox::snapshot_helper::persist_failure(
                            test_name,
                            format!("{err:?}"),
                            ::xtask::sandbox::snapshot_helper::FailureContext::None,
                        );
                    }
                }
                result
            };

            tokio::time::timeout(
                std::time::Duration::from_secs(#timeout_secs),
                test_future
            ).await
            .map_err(|_| ::color_eyre::eyre::eyre!("Test timed out after {} seconds", #timeout_secs))?
        }
    }
}

fn expand_sinex_test(config: SinexTestConfig, input: ItemFn) -> TokenStream {
    // All sinex_test functions must be async
    if input.sig.asyncness.is_none() {
        return syn::Error::new_spanned(
            input.sig.fn_token,
            "sinex_test requires async functions — use `async fn` instead of `fn`",
        )
        .to_compile_error()
        .into();
    }

    let attrs = classify_attrs(&input);

    const DEFAULT_TIMEOUT: u64 = 120;
    let timeout_secs = config.timeout.unwrap_or(DEFAULT_TIMEOUT);

    // Validate return type
    if !has_result_return_type(&input.sig.output) {
        return syn::Error::new_spanned(
            &input.sig.output,
            "sinex_test functions must return Result<()> or Result<T>",
        )
        .to_compile_error()
        .into();
    }

    let takes_context = input.sig.inputs.iter().any(|arg| {
        if let syn::FnArg::Typed(pat_type) = arg {
            return is_context_type(pat_type.ty.as_ref());
        }
        false
    });

    // Dispatch to the appropriate code generation variant
    if attrs.has_rstest_cases {
        return expand_rstest_variant(&input, &attrs, timeout_secs, config.serial, config.trace);
    }

    let fn_body = *input.block.clone();

    let output = if takes_context {
        expand_async_context_test(
            &input,
            &attrs.test_attrs,
            &fn_body,
            timeout_secs,
            config.serial,
        )
    } else {
        expand_simple_async_test(
            &input,
            &attrs.test_attrs,
            &fn_body,
            timeout_secs,
            config.serial,
        )
    };

    output.into()
}

#[derive(Clone, Copy)]
enum BenchMode {
    Auto,
    Integration,
    Micro,
}

fn parse_bench_mode(lit: &Lit) -> syn::Result<BenchMode> {
    if let Lit::Str(mode) = lit {
        match mode.value().to_lowercase().as_str() {
            "integration" => Ok(BenchMode::Integration),
            "micro" => Ok(BenchMode::Micro),
            other => Err(syn::Error::new(
                lit.span(),
                format!("unknown bench mode '{other}'"),
            )),
        }
    } else {
        Err(syn::Error::new(
            lit.span(),
            "mode must be a string literal (\"micro\" or \"integration\")",
        ))
    }
}

#[proc_macro_attribute]
pub fn sinex_bench(attr: TokenStream, item: TokenStream) -> TokenStream {
    // When building tests (not benchmarks), just remove the function entirely
    // by returning it wrapped in #[cfg(all(test, feature = "bench"))]
    // This prevents divan errors during test compilation

    let input = parse_macro_input!(item as ItemFn);
    let fn_name = &input.sig.ident;

    let attr_tokens = proc_macro2::TokenStream::from(attr);
    let mut bench_mode = BenchMode::Auto;
    let mut args_tokens: Option<proc_macro2::TokenStream> = None;

    if !attr_tokens.is_empty() {
        let parsed = match Punctuated::<Meta, Comma>::parse_terminated.parse2(attr_tokens) {
            Ok(list) => list,
            Err(err) => return err.to_compile_error().into(),
        };

        for meta in parsed {
            match meta {
                Meta::NameValue(nv) if nv.path.is_ident("mode") => {
                    if let Expr::Lit(expr_lit) = nv.value {
                        bench_mode = match parse_bench_mode(&expr_lit.lit) {
                            Ok(mode) => mode,
                            Err(err) => return err.to_compile_error().into(),
                        };
                    } else {
                        return syn::Error::new_spanned(
                            nv.value,
                            "mode attribute expects a string literal",
                        )
                        .to_compile_error()
                        .into();
                    }
                }
                Meta::NameValue(nv) if nv.path.is_ident("args") => {
                    args_tokens = Some(nv.value.into_token_stream());
                }
                other => {
                    return syn::Error::new_spanned(
                        other,
                        "supported attributes: mode = \"micro\"|\"integration\", args = [...]",
                    )
                    .to_compile_error()
                    .into();
                }
            }
        }
    }

    // Remove async validation - benchmarks should be synchronous since the macro handles async internally

    // Check function parameters
    let mut takes_context = false;
    let mut takes_args = false;
    let mut arg_type = None;

    for (i, arg) in input.sig.inputs.iter().enumerate() {
        if let syn::FnArg::Typed(pat_type) = arg
            && let syn::Type::Path(type_path) = pat_type.ty.as_ref()
            && let Some(last_segment) = type_path.path.segments.last()
        {
            if last_segment.ident == "BenchContext" && i == 0 {
                takes_context = true;
            } else if i == 1 || (i == 0 && !takes_context) {
                takes_args = true;
                arg_type = Some(pat_type.ty.clone());
            }
        }
    }

    let fn_vis = &input.vis;
    let fn_body = &input.block;

    let resolved_mode = match bench_mode {
        BenchMode::Auto => {
            if takes_context {
                BenchMode::Integration
            } else {
                BenchMode::Micro
            }
        }
        other => other,
    };

    let output = match resolved_mode {
        BenchMode::Integration => {
            if !takes_context {
                return syn::Error::new_spanned(
                    fn_name,
                    "BenchContext parameter required for integration benchmarks",
                )
                .to_compile_error()
                .into();
            }

            if takes_args {
                let args_tokens = match args_tokens {
                    Some(tokens) => tokens,
                    None => {
                        return syn::Error::new_spanned(
                            fn_name,
                            "integration benchmarks with args require `args = [...]`",
                        )
                        .to_compile_error()
                        .into();
                    }
                };
                quote! {
                    #[cfg(feature = "bench")]
                    #[divan::bench(args = #args_tokens)]
                    #fn_vis fn #fn_name(bencher: divan::Bencher, arg: #arg_type) {
                        use ::xtask::sandbox::bench::BENCH_CONTEXT;
                        let ctx = &*BENCH_CONTEXT;

                        bencher.bench_local(|| {
                            ctx.runtime.block_on(async {
                                let result: ::xtask::sandbox::TestResult<()> = async {
                                    let ctx = ctx;
                                    let arg = arg;
                                    #fn_body
                                }.await;
                                result.unwrap()
                            })
                        });
                    }
                }
            } else {
                quote! {
                    #[cfg(feature = "bench")]
                    #[divan::bench]
                    #fn_vis fn #fn_name(bencher: divan::Bencher) {
                        use ::xtask::sandbox::bench::BENCH_CONTEXT;
                        let ctx = &*BENCH_CONTEXT;

                        bencher.bench_local(|| {
                            ctx.runtime.block_on(async {
                                let result: ::xtask::sandbox::TestResult<()> = async {
                                    let ctx = ctx;
                                    #fn_body
                                }.await;
                                result.unwrap()
                            })
                        });
                    }
                }
            }
        }
        BenchMode::Micro => {
            if takes_context {
                return syn::Error::new_spanned(
                    fn_name,
                    "BenchContext is not available in micro benchmarks (use `mode = \"integration\"`)",
                )
                .to_compile_error()
                .into();
            }

            if takes_args {
                let args_tokens = match args_tokens {
                    Some(tokens) => tokens,
                    None => {
                        return syn::Error::new_spanned(
                            fn_name,
                            "micro benchmarks with args require `args = [...]`",
                        )
                        .to_compile_error()
                        .into();
                    }
                };
                quote! {
                    #[cfg(feature = "bench")]
                    #[divan::bench(args = #args_tokens)]
                    #fn_vis fn #fn_name(bencher: divan::Bencher, arg: #arg_type) {
                        let runtime = tokio::runtime::Runtime::new().unwrap();

                        bencher.bench_local(|| {
                            runtime.block_on(async {
                                let result: ::xtask::sandbox::TestResult<()> = async {
                                    let arg = arg;
                                    #fn_body
                                }.await;
                                result.unwrap()
                            })
                        });
                    }
                }
            } else {
                quote! {
                    #[cfg(feature = "bench")]
                    #[divan::bench]
                    #fn_vis fn #fn_name(bencher: divan::Bencher) {
                        let runtime = tokio::runtime::Runtime::new().unwrap();

                        bencher.bench_local(|| {
                            runtime.block_on(async {
                                let result: ::xtask::sandbox::TestResult<()> = async {
                                    #fn_body
                                }.await;
                                result.unwrap()
                            })
                        });
                    }
                }
            }
        }
        BenchMode::Auto => unreachable!("bench mode should be fully resolved"),
    };

    output.into()
}

#[cfg(test)]
mod tests {
    use super::{
        expand_async_context_test, expand_simple_async_test, parse_sinex_test_attrs_tokens,
        serial_guard_tokens,
    };
    use quote::quote;
    use syn::{ItemFn, parse2};

    fn parse_ok(tokens: proc_macro2::TokenStream) -> super::SinexTestConfig {
        parse_sinex_test_attrs_tokens(tokens).expect("attributes should parse")
    }

    fn parse_err(tokens: proc_macro2::TokenStream) -> String {
        parse_sinex_test_attrs_tokens(tokens)
            .expect_err("attributes should fail")
            .to_string()
    }

    #[test]
    fn sinex_test_attrs_parse_valid_timeout_and_flags() {
        let config = parse_ok(quote!(timeout = 45, trace = true, serial));

        assert_eq!(config.timeout, Some(45));
        assert!(config.trace);
        assert!(config.serial);
    }

    #[test]
    fn sinex_test_attrs_reject_invalid_timeout_literal() {
        let error = parse_err(quote!(timeout = "fast"));
        assert!(error.contains("timeout"));
        assert!(error.contains("integer literal"));
    }

    #[test]
    fn sinex_test_attrs_reject_invalid_trace_literal() {
        let error = parse_err(quote!(trace = "yes"));
        assert!(error.contains("trace"));
        assert!(error.contains("boolean literal"));
    }

    #[test]
    fn sinex_test_attrs_reject_unknown_attribute() {
        let error = parse_err(quote!(timout = 30));
        assert!(error.contains("unknown sinex_test attribute"));
        assert!(error.contains("timout"));
    }

    fn parse_item_fn(tokens: proc_macro2::TokenStream) -> ItemFn {
        parse2(tokens).expect("test function should parse")
    }

    fn count_occurrences(haystack: &str, needle: &str) -> usize {
        haystack.match_indices(needle).count()
    }

    #[test]
    fn serial_guard_tokens_propagate_lock_acquisition_failures() {
        let rendered = serial_guard_tokens(true).to_string();
        assert!(rendered.contains("acquire_pool_test_guard"));
        assert!(
            rendered.contains(". await ?"),
            "rendered tokens: {rendered}"
        );
    }

    #[test]
    fn async_context_expansion_keeps_single_serial_guard_inside_timed_future() {
        let input = parse_item_fn(quote! {
            async fn serial_context_test(ctx: ::xtask::sandbox::TestContext) -> ::xtask::sandbox::TestResult<()> {
                let _ = ctx;
                Ok(())
            }
        });

        let rendered = expand_async_context_test(&input, &[], &input.block, 30, true).to_string();
        assert_eq!(
            count_occurrences(&rendered, "acquire_pool_test_guard"),
            1,
            "rendered tokens: {rendered}"
        );
        assert!(
            rendered.contains("let test_future = async"),
            "rendered tokens: {rendered}"
        );
        assert!(rendered.contains("timeout"), "rendered tokens: {rendered}");
    }

    #[test]
    fn simple_async_expansion_keeps_single_serial_guard_inside_timed_future() {
        let input = parse_item_fn(quote! {
            async fn serial_simple_test() -> ::xtask::sandbox::TestResult<()> {
                Ok(())
            }
        });

        let rendered = expand_simple_async_test(&input, &[], &input.block, 30, true).to_string();
        assert_eq!(
            count_occurrences(&rendered, "acquire_pool_test_guard"),
            1,
            "rendered tokens: {rendered}"
        );
        assert!(
            rendered.contains("let test_future = async"),
            "rendered tokens: {rendered}"
        );
        assert!(rendered.contains("timeout"), "rendered tokens: {rendered}");
    }
}
