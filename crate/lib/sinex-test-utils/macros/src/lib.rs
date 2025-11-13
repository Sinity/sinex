//! Procedural macros for Sinex test infrastructure
//!
//! This macro provides sophisticated test infrastructure for Sinex tests, including:
//! - Automatic TestContext creation and cleanup
//! - Proptest integration with async runtime bridging
//! - Smart timeout handling based on test patterns
//! - Progress indicators for long-running tests
//! - Rich error reporting with timing information

use proc_macro::TokenStream;
use quote::{quote, ToTokens};
use syn::{
    braced, parenthesized,
    parse::{Parse, ParseStream, Parser},
    parse_macro_input, parse_quote,
    punctuated::Punctuated,
    spanned::Spanned,
    token::Comma,
    Attribute, Block, Expr, FnArg, Ident, ItemFn, Lit, Meta, Pat, PatIdent, ReturnType, Token,
    Type, Visibility,
};

/// Configuration parsed from sinex_test attributes
struct SinexTestConfig {
    timeout: Option<u64>,
    trace: bool,
}

/// Parse sinex_test attributes
/// Supports: timeout = 30, trace = true
fn parse_sinex_test_attrs(attr: TokenStream) -> SinexTestConfig {
    let mut config = SinexTestConfig {
        timeout: None,
        trace: false,
    };

    if attr.is_empty() {
        return config;
    }

    // Try to parse as a simple integer first (legacy timeout support)
    let attr_str = attr.to_string();
    if let Ok(timeout) = attr_str.trim().parse::<u64>() {
        config.timeout = Some(timeout);
        return config;
    }

    // Parse comma-separated attributes
    if let Ok(meta_list) = syn::parse::<syn::MetaList>(attr.clone()) {
        for nested in meta_list.tokens {
            if let Ok(Meta::NameValue(nv)) = syn::parse2::<Meta>(quote! { #nested }) {
                if nv.path.is_ident("timeout") {
                    if let Expr::Lit(expr_lit) = &nv.value {
                        if let Lit::Int(lit_int) = &expr_lit.lit {
                            config.timeout = lit_int.base10_parse().ok();
                        }
                    }
                } else if nv.path.is_ident("trace") {
                    if let Expr::Lit(expr_lit) = &nv.value {
                        if let Lit::Bool(lit_bool) = &expr_lit.lit {
                            config.trace = lit_bool.value();
                        }
                    }
                }
            }
        }
    }

    // Also try simple name=value parsing
    if let Ok(Meta::NameValue(nv)) = syn::parse::<Meta>(attr) {
        if nv.path.is_ident("timeout") {
            if let Expr::Lit(expr_lit) = &nv.value {
                if let Lit::Int(lit_int) = &expr_lit.lit {
                    config.timeout = lit_int.base10_parse().ok();
                }
            }
        } else if nv.path.is_ident("trace") {
            if let Expr::Lit(expr_lit) = &nv.value {
                if let Lit::Bool(lit_bool) = &expr_lit.lit {
                    config.trace = lit_bool.value();
                }
            }
        }
    }

    config
}

#[derive(Clone, Default)]
struct SinexPropConfig {
    timeout: Option<u64>,
    trace: bool,
    cases: Option<u32>,
    seed: Option<u64>,
    max_shrink_time_ms: Option<u64>,
}

fn parse_sinex_prop_attrs(attr: TokenStream) -> syn::Result<SinexPropConfig> {
    if attr.is_empty() {
        return Ok(SinexPropConfig::default());
    }

    let metas = Punctuated::<Meta, Comma>::parse_terminated.parse(attr)?;
    let mut config = SinexPropConfig::default();

    for meta in metas {
        match meta {
            Meta::NameValue(nv) if nv.path.is_ident("timeout") => {
                if let Expr::Lit(expr_lit) = nv.value {
                    config.timeout = Some(parse_timeout_literal(&expr_lit.lit)?);
                } else {
                    return Err(syn::Error::new(
                        nv.value.span(),
                        "timeout expects an integer literal or \"30s\"",
                    ));
                }
            }
            Meta::NameValue(nv) if nv.path.is_ident("cases") => {
                if let Expr::Lit(expr_lit) = nv.value {
                    config.cases = Some(parse_u32_literal(&expr_lit.lit)?);
                } else {
                    return Err(syn::Error::new(
                        nv.value.span(),
                        "cases expects an integer literal",
                    ));
                }
            }
            Meta::NameValue(nv) if nv.path.is_ident("seed") => {
                if let Expr::Lit(expr_lit) = nv.value {
                    config.seed = Some(parse_u64_literal(&expr_lit.lit)?);
                } else {
                    return Err(syn::Error::new(
                        nv.value.span(),
                        "seed expects an integer literal",
                    ));
                }
            }
            Meta::NameValue(nv) if nv.path.is_ident("max_shrink_time_ms") => {
                if let Expr::Lit(expr_lit) = nv.value {
                    config.max_shrink_time_ms = Some(parse_u64_literal(&expr_lit.lit)?);
                } else {
                    return Err(syn::Error::new(
                        nv.value.span(),
                        "max_shrink_time_ms expects an integer literal",
                    ));
                }
            }
            Meta::Path(path) if path.is_ident("trace") => {
                config.trace = true;
            }
            other => {
                return Err(syn::Error::new(
                    other.span(),
                    "supported options: timeout=.., trace, cases=.., seed=.., max_shrink_time_ms=..",
                ));
            }
        }
    }

    Ok(config)
}

fn merge_prop_configs(base: &SinexPropConfig, overrides: &SinexPropConfig) -> SinexPropConfig {
    SinexPropConfig {
        timeout: overrides.timeout.or(base.timeout),
        trace: overrides.trace || base.trace,
        cases: overrides.cases.or(base.cases),
        seed: overrides.seed.or(base.seed),
        max_shrink_time_ms: overrides.max_shrink_time_ms.or(base.max_shrink_time_ms),
    }
}

fn config_to_attr_tokens(config: &SinexPropConfig) -> proc_macro2::TokenStream {
    let mut entries = Vec::new();
    if let Some(timeout) = config.timeout {
        entries.push(quote! { timeout = #timeout });
    }
    if config.trace {
        entries.push(quote! { trace });
    }
    if let Some(cases) = config.cases {
        entries.push(quote! { cases = #cases });
    }
    if let Some(seed) = config.seed {
        entries.push(quote! { seed = #seed });
    }
    if let Some(ms) = config.max_shrink_time_ms {
        entries.push(quote! { max_shrink_time_ms = #ms });
    }

    if entries.is_empty() {
        quote! {}
    } else {
        quote! { (#(#entries),*) }
    }
}

fn parse_u32_literal(lit: &Lit) -> syn::Result<u32> {
    match lit {
        Lit::Int(int) => int
            .base10_parse::<u32>()
            .map_err(|e| syn::Error::new(int.span(), e)),
        _ => Err(syn::Error::new(lit.span(), "expected integer literal")),
    }
}

fn type_is_test_context(ty: &Type) -> bool {
    if let Type::Path(type_path) = ty {
        return type_path
            .path
            .segments
            .last()
            .map(|seg| seg.ident == "TestContext")
            .unwrap_or(false);
    }
    false
}

fn is_test_context_reference(ty: &Type) -> bool {
    if let Type::Reference(reference) = ty {
        return type_is_test_context(&reference.elem);
    }
    false
}

#[proc_macro_attribute]
pub fn sinex_prop(attr: TokenStream, item: TokenStream) -> TokenStream {
    match expand_sinex_prop(attr, item) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

struct PropParam {
    ident: Ident,
    strategy: proc_macro2::TokenStream,
}

fn expand_sinex_prop(
    attr: TokenStream,
    item: TokenStream,
) -> syn::Result<proc_macro2::TokenStream> {
    let input: ItemFn = syn::parse(item)?;
    let config = parse_sinex_prop_attrs(attr)?;

    let fn_name = input.sig.ident.clone();
    let fn_vis = input.vis.clone();
    let is_async = input.sig.asyncness.is_some();
    let attrs = input.attrs.clone();

    let has_result_return = match &input.sig.output {
        ReturnType::Type(_, ty) => {
            if let Type::Path(type_path) = ty.as_ref() {
                type_path
                    .path
                    .segments
                    .last()
                    .map(|seg| {
                        let ident = &seg.ident;
                        ident == "Result" || ident == "TestResult"
                    })
                    .unwrap_or(false)
            } else {
                false
            }
        }
        ReturnType::Default => false,
    };

    if !has_result_return {
        return Err(syn::Error::new_spanned(
            &input.sig.output,
            "sinex_prop functions must return Result<()> or Result<T>",
        ));
    }

    let mut ctx_param: Option<Ident> = None;
    let mut prop_params = Vec::<PropParam>::new();

    let mut inner_inputs = input.sig.inputs.clone();
    for arg in inner_inputs.iter_mut() {
        match arg {
            FnArg::Receiver(_) => {
                return Err(syn::Error::new_spanned(
                    arg,
                    "sinex_prop does not support self receivers",
                ));
            }
            FnArg::Typed(pat_ty) => {
                if is_test_context_reference(&pat_ty.ty) {
                    if ctx_param.is_some() {
                        return Err(syn::Error::new_spanned(
                            &pat_ty.ty,
                            "multiple TestContext parameters are not supported",
                        ));
                    }
                    if let Pat::Ident(PatIdent { ident, .. }) = &*pat_ty.pat {
                        ctx_param = Some(ident.clone());
                    } else {
                        return Err(syn::Error::new_spanned(
                            &pat_ty.pat,
                            "TestContext parameter must be an identifier",
                        ));
                    }
                    continue;
                }

                let mut strategy = None;
                let mut cleaned_attrs = Vec::new();
                for attr in &pat_ty.attrs {
                    if attr.path().is_ident("strategy") {
                        strategy = Some(attr.parse_args::<proc_macro2::TokenStream>()?);
                    } else {
                        cleaned_attrs.push(attr.clone());
                    }
                }
                if strategy.is_none() {
                    return Err(syn::Error::new_spanned(
                        &pat_ty.pat,
                        "each parameter must have #[strategy(...)]",
                    ));
                }
                if let Pat::Ident(PatIdent { ident, .. }) = &*pat_ty.pat {
                    prop_params.push(PropParam {
                        ident: ident.clone(),
                        strategy: strategy.unwrap(),
                    });
                } else {
                    return Err(syn::Error::new_spanned(
                        &pat_ty.pat,
                        "parameter pattern must be an identifier",
                    ));
                }
                pat_ty.attrs = cleaned_attrs;
            }
        }
    }

    if prop_params.is_empty() {
        return Err(syn::Error::new(
            fn_name.span(),
            "sinex_prop requires at least one #[strategy] parameter",
        ));
    }

    let mut inner_fn = input.clone();
    inner_fn.attrs.clear();
    inner_fn.vis = syn::Visibility::Inherited;
    let inner_ident = Ident::new(&format!("__sinex_prop_impl_{}", fn_name), fn_name.span());
    inner_fn.sig.ident = inner_ident.clone();
    inner_fn.sig.inputs = inner_inputs;

    let timeout_secs = config.timeout.unwrap_or(30);
    let cases_expr = config
        .cases
        .map(|v| quote! { Some(#v) })
        .unwrap_or_else(|| quote! { None });
    let seed_expr = config
        .seed
        .map(|v| quote! { Some(#v) })
        .unwrap_or_else(|| quote! { None });
    let shrink_expr = config
        .max_shrink_time_ms
        .map(|v| quote! { Some(#v) })
        .unwrap_or_else(|| quote! { None });

    let tracing_block = if config.trace {
        quote! { sinex_test_utils::TestContext::init_tracing("debug"); }
    } else {
        quote! {}
    };

    let strategy_expr = if prop_params.len() == 1 {
        let strat = &prop_params[0].strategy;
        quote! { #strat }
    } else {
        let pieces = prop_params.iter().map(|p| &p.strategy);
        quote! { ( #(#pieces),* ) }
    };

    let destructure = if prop_params.len() == 1 {
        let ident = &prop_params[0].ident;
        quote! { let #ident = value; }
    } else {
        let idents = prop_params.iter().map(|p| &p.ident);
        quote! { let (#(#idents),*) = value; }
    };

    let has_ctx = ctx_param.is_some();

    let mut call_args = Vec::new();
    if ctx_param.is_some() {
        call_args.push(quote! { ctx_ref.expect("TestContext available") });
    }
    for param in &prop_params {
        let ident = &param.ident;
        call_args.push(quote!(#ident));
    }

    let mut call_expr = quote! { #inner_ident( #(#call_args),* ) };
    if is_async {
        call_expr = quote! { #call_expr.await };
    }

    let ctx_ref_stmt = if has_ctx {
        quote! { let ctx_ref = ctx_holder.as_ref(); }
    } else {
        quote! {}
    };

    let harness = quote! {
        #(#attrs)*
        #[tokio::test(flavor = "multi_thread")]
        #fn_vis async fn #fn_name() -> color_eyre::eyre::Result<()> {
            #tracing_block
            let test_name = stringify!(#fn_name);
            let start = std::time::Instant::now();
            eprintln!("🔄 {} [property, timeout: {}s]", test_name.replace('_', " "), #timeout_secs);

            let timeout = std::time::Duration::from_secs(#timeout_secs);

            let (run_result, telemetry) = tokio::time::timeout(
                timeout,
                async {
                    let mut ctx_holder = if #has_ctx {
                        Some(sinex_test_utils::TestContext::with_name(test_name).await?)
                    } else {
                        None
                    };

                    let telemetry = ctx_holder
                        .as_ref()
                        .map(|ctx| sinex_test_utils::runlog::TelemetryHandle::from(ctx.telemetry_handle()));
                    #ctx_ref_stmt

                    let overrides = sinex_test_utils::property_testing::RunnerOverrides {
                        cases: #cases_expr,
                        seed: #seed_expr,
                        max_shrink_time_ms: #shrink_expr,
                    };
                    let mut runner = sinex_test_utils::property_testing::make_runner(overrides);
                    let strategy = #strategy_expr;

                    let run = runner.run(&strategy, |value| {
                        #destructure
                        let fut = async { #call_expr };
                        futures::executor::block_on(fut)
                            .map_err(|err| proptest::test_runner::TestCaseError::fail(format!("{err:?}")))
                    });

                    drop(ctx_holder);
                    Ok::<_, color_eyre::eyre::Report>((run, telemetry))
                },
            )
            .await
            .map_err(|_| color_eyre::eyre::eyre!(format!("Test timed out after {} seconds", #timeout_secs)))??;

            let result = run_result
                .map_err(|err| color_eyre::eyre::eyre!(format!("Property failure: {err}")));

            let elapsed = start.elapsed();
            if result.is_ok() {
                eprintln!("✅ {} ({:.1?})", test_name.replace('_', " "), elapsed);
            } else {
                eprintln!("❌ {} ({:.1?})", test_name.replace('_', " "), elapsed);
            }

            sinex_test_utils::runlog::record_async(
                test_name,
                elapsed,
                telemetry.as_ref(),
                &result,
            )
            .await;

            result
        }
    };

    Ok(quote! {
        #inner_fn
        #harness
    })
}

fn parse_u64_literal(lit: &Lit) -> syn::Result<u64> {
    match lit {
        Lit::Int(int) => int
            .base10_parse::<u64>()
            .map_err(|e| syn::Error::new(int.span(), e)),
        _ => Err(syn::Error::new(lit.span(), "expected integer literal")),
    }
}

fn parse_timeout_literal(lit: &Lit) -> syn::Result<u64> {
    match lit {
        Lit::Int(int) => int
            .base10_parse::<u64>()
            .map_err(|e| syn::Error::new(int.span(), e)),
        Lit::Str(s) => {
            let value = s.value();
            if let Some(stripped) = value.trim().strip_suffix('s') {
                stripped
                    .parse::<u64>()
                    .map_err(|e| syn::Error::new(s.span(), e))
            } else {
                value
                    .trim()
                    .parse::<u64>()
                    .map_err(|e| syn::Error::new(s.span(), e))
            }
        }
        _ => Err(syn::Error::new(
            lit.span(),
            r#"expected integer seconds or string literal like "30s""#,
        )),
    }
}

struct ProptestBlock {
    defaults: SinexPropConfig,
    cases: Vec<ProptestCase>,
}

struct ProptestCase {
    attrs: Vec<Attribute>,
    overrides: SinexPropConfig,
    vis: Visibility,
    async_token: Option<Token![async]>,
    name: Ident,
    ctx_param: Option<BlockCtxParam>,
    prop_params: Vec<BlockPropParamSpec>,
    output: ReturnType,
    body: Block,
}

struct BlockCtxParam {
    ident: Ident,
    ty: Type,
}

struct BlockPropParamSpec {
    ident: Ident,
    ty: Type,
    strategy: Expr,
}

impl Parse for ProptestBlock {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let content;
        braced!(content in input);

        let mut defaults = SinexPropConfig::default();
        let mut cases = Vec::new();

        while !content.is_empty() {
            if content.peek(Token![#]) && content.peek2(Token![!]) {
                let attrs = content.call(Attribute::parse_inner)?;
                for attr in attrs {
                    if !apply_prop_control_attr(&attr, &mut defaults)? {
                        return Err(syn::Error::new(
                            attr.span(),
                            "unsupported block attribute; allowed: cases, timeout, trace, seed, max_shrink_time_ms",
                        ));
                    }
                }
                continue;
            }

            cases.push(content.parse::<ProptestCase>()?);
        }

        Ok(Self { defaults, cases })
    }
}

impl Parse for ProptestCase {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let attrs = input.call(Attribute::parse_outer)?;
        let (overrides, passthrough) = split_control_attrs(attrs)?;

        let vis: Visibility = input.parse()?;
        let async_token = if input.peek(Token![async]) {
            Some(input.parse()?)
        } else {
            None
        };

        let _fn_token: Token![fn] = input.parse()?;
        let name: Ident = input.parse()?;

        if input.peek(Token![<]) {
            return Err(syn::Error::new(
                input.span(),
                "generics are not supported in sinex_proptest! declarations",
            ));
        }

        let content;
        parenthesized!(content in input);
        let (ctx_param, prop_params) = parse_block_params(&content)?;

        if prop_params.is_empty() {
            return Err(syn::Error::new(
                name.span(),
                "sinex_proptest! cases require at least one parameter with `in <strategy>`",
            ));
        }

        if input.peek(Token![where]) {
            return Err(syn::Error::new(
                input.span(),
                "where clauses are not supported in sinex_proptest! declarations",
            ));
        }

        let output = if input.peek(Token![->]) {
            let arrow: Token![->] = input.parse()?;
            let ty: Type = input.parse()?;
            ReturnType::Type(arrow, Box::new(ty))
        } else {
            ReturnType::Default
        };

        let body: Block = input.parse()?;

        Ok(Self {
            attrs: passthrough,
            overrides,
            vis,
            async_token,
            name,
            ctx_param,
            prop_params,
            output,
            body,
        })
    }
}

fn parse_block_params(
    input: ParseStream,
) -> syn::Result<(Option<BlockCtxParam>, Vec<BlockPropParamSpec>)> {
    let mut ctx_param = None;
    let mut props = Vec::new();

    while !input.is_empty() {
        let ident: Ident = input.parse()?;

        input.parse::<Token![:]>()?;
        let ty: Type = input.parse()?;

        if input.peek(Token![in]) {
            input.parse::<Token![in]>()?;
            let strategy: Expr = input.parse()?;
            props.push(BlockPropParamSpec {
                ident,
                ty,
                strategy,
            });
        } else {
            if ctx_param.is_some() {
                return Err(syn::Error::new(
                    ident.span(),
                    "only one non-strategy parameter is allowed (use it for &TestContext)",
                ));
            }
            if !is_test_context_reference(&ty) {
                return Err(syn::Error::new(
                    ty.span(),
                    "parameters must use `name: Type in <strategy>` unless they are &TestContext",
                ));
            }
            ctx_param = Some(BlockCtxParam { ident, ty });
        }

        if input.peek(Token![,]) {
            input.parse::<Token![,]>()?;
            if input.is_empty() {
                break;
            }
        }
    }

    Ok((ctx_param, props))
}

fn split_control_attrs(attrs: Vec<Attribute>) -> syn::Result<(SinexPropConfig, Vec<Attribute>)> {
    let mut overrides = SinexPropConfig::default();
    let mut passthrough = Vec::new();

    for attr in attrs {
        if apply_prop_control_attr(&attr, &mut overrides)? {
            continue;
        }
        passthrough.push(attr);
    }

    Ok((overrides, passthrough))
}

fn apply_prop_control_attr(attr: &Attribute, cfg: &mut SinexPropConfig) -> syn::Result<bool> {
    match attr.meta.clone() {
        Meta::NameValue(nv) if nv.path.is_ident("cases") => {
            if let Expr::Lit(expr_lit) = nv.value {
                cfg.cases = Some(parse_u32_literal(&expr_lit.lit)?);
                return Ok(true);
            }
            return Err(syn::Error::new(
                nv.value.span(),
                "cases attribute expects an integer literal",
            ));
        }
        Meta::NameValue(nv) if nv.path.is_ident("timeout") => {
            if let Expr::Lit(expr_lit) = nv.value {
                cfg.timeout = Some(parse_timeout_literal(&expr_lit.lit)?);
                return Ok(true);
            }
            return Err(syn::Error::new(
                nv.value.span(),
                "timeout attribute expects an integer or \"30s\" literal",
            ));
        }
        Meta::NameValue(nv) if nv.path.is_ident("seed") => {
            if let Expr::Lit(expr_lit) = nv.value {
                cfg.seed = Some(parse_u64_literal(&expr_lit.lit)?);
                return Ok(true);
            }
            return Err(syn::Error::new(
                nv.value.span(),
                "seed attribute expects an integer literal",
            ));
        }
        Meta::NameValue(nv) if nv.path.is_ident("max_shrink_time_ms") => {
            if let Expr::Lit(expr_lit) = nv.value {
                cfg.max_shrink_time_ms = Some(parse_u64_literal(&expr_lit.lit)?);
                return Ok(true);
            }
            return Err(syn::Error::new(
                nv.value.span(),
                "max_shrink_time_ms expects an integer literal",
            ));
        }
        Meta::Path(path) if path.is_ident("trace") => {
            cfg.trace = true;
            Ok(true)
        }
        _ => Ok(false),
    }
}

#[proc_macro]
pub fn sinex_proptest(input: TokenStream) -> TokenStream {
    let block = parse_macro_input!(input as ProptestBlock);
    match expand_sinex_proptest(block) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

fn expand_sinex_proptest(block: ProptestBlock) -> syn::Result<proc_macro2::TokenStream> {
    if block.cases.is_empty() {
        return Err(syn::Error::new(
            proc_macro2::Span::call_site(),
            "sinex_proptest! requires at least one test case",
        ));
    }

    let mut expanded = Vec::new();
    for case in block.cases {
        let config = merge_prop_configs(&block.defaults, &case.overrides);
        let attr_tokens = config_to_attr_tokens(&config);
        let prop_attr = if attr_tokens.is_empty() {
            quote! { #[sinex_test_utils::sinex_prop] }
        } else {
            quote! { #[sinex_test_utils::sinex_prop #attr_tokens] }
        };

        let async_token = case.async_token;
        let name = case.name;
        let vis = case.vis;
        let attrs = case.attrs;
        let body = case.body;

        let ctx_param = case.ctx_param.map(|param| {
            let ident = param.ident;
            let ty = param.ty;
            quote! { #ident: #ty }
        });

        let prop_params: Vec<_> = case
            .prop_params
            .into_iter()
            .map(|param| {
                let ident = param.ident;
                let ty = param.ty;
                let strategy = param.strategy;
                quote! { #[strategy(#strategy)] #ident: #ty }
            })
            .collect();

        let output = match case.output {
            ReturnType::Default => parse_quote!(-> sinex_test_utils::TestResult<()>),
            other => other,
        };

        let params = if let Some(ctx) = ctx_param {
            quote! { #ctx, #(#prop_params),* }
        } else {
            quote! { #(#prop_params),* }
        };

        expanded.push(quote! {
            #prop_attr
            #(#attrs)*
            #vis #async_token fn #name(#params) #output {
                #body
            }
        });
    }

    Ok(quote! { #(#expanded)* })
}

#[proc_macro_attribute]
pub fn sinex_test(attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);
    let fn_name = &input.sig.ident;

    // Check if function is async or sync
    let is_async = input.sig.asyncness.is_some();

    // Check for rstest integration and preserve important attributes:
    // 1. Look for #[case] attributes on the function
    // 2. Look for #[case] attributes on parameters
    // 3. Preserve #[ignore] and #[should_panic] attributes
    let mut case_attrs = Vec::new();
    let mut other_attrs = Vec::new();
    let mut test_attrs = Vec::new(); // For #[ignore], #[should_panic], etc.
    let mut has_rstest_cases = false;

    // Separate #[case] attributes from others, preserve test attributes
    for attr in &input.attrs {
        if attr.path().is_ident("case") {
            has_rstest_cases = true;
            case_attrs.push(attr.clone());
        } else if attr.path().is_ident("ignore") || attr.path().is_ident("should_panic") {
            test_attrs.push(attr.clone());
            other_attrs.push(attr.clone());
        } else {
            other_attrs.push(attr.clone());
        }
    }

    // Also check if any parameters have #[case] attribute
    for arg in &input.sig.inputs {
        if let syn::FnArg::Typed(pat_type) = arg {
            for attr in &pat_type.attrs {
                if attr.path().is_ident("case") {
                    has_rstest_cases = true;
                }
            }
        }
    }

    // Default timeout constants
    const DEFAULT_SYNC_TIMEOUT: u64 = 10; // 10 seconds for sync tests
    const DEFAULT_ASYNC_TIMEOUT: u64 = 30; // 30 seconds for async tests

    // Parse sinex_test configuration
    let config = parse_sinex_test_attrs(attr);
    let timeout_secs = config.timeout.unwrap_or({
        if is_async {
            DEFAULT_ASYNC_TIMEOUT
        } else {
            DEFAULT_SYNC_TIMEOUT
        }
    });
    let enable_tracing = config.trace;

    let fn_body = *input.block.clone();

    let fn_vis = &input.vis;

    // Check return type - must be Result<()> or Result<T>
    let has_result_return = if let syn::ReturnType::Type(_, ref ty) = input.sig.output {
        if let syn::Type::Path(type_path) = ty.as_ref() {
            type_path
                .path
                .segments
                .last()
                .map(|seg| {
                    let ident = &seg.ident;
                    ident == "Result" || ident == "TestResult"
                })
                .unwrap_or(false)
        } else {
            false
        }
    } else {
        false
    };

    if !has_result_return {
        return syn::Error::new_spanned(
            &input.sig.output,
            "sinex_test functions must return Result<()> or Result<T>",
        )
        .to_compile_error()
        .into();
    }

    // Check if function takes TestContext parameter
    let takes_context = input.sig.inputs.iter().any(|arg| {
        if let syn::FnArg::Typed(pat_type) = arg {
            if let syn::Type::Path(type_path) = pat_type.ty.as_ref() {
                if let Some(last_segment) = type_path.path.segments.last() {
                    return last_segment.ident == "TestContext";
                }
            }
        }
        false
    });

    // Sync tests can't use TestContext (it requires async)
    if takes_context && !is_async {
        return syn::Error::new_spanned(input.sig.fn_token, "TestContext requires async functions")
            .to_compile_error()
            .into();
    }

    // If we have rstest cases, generate rstest-compatible output
    if has_rstest_cases {
        // For rstest integration, we need to:
        // 1. Add #[rstest] attribute
        // 2. Include #[case] attributes
        // 3. Handle TestContext specially - it gets created per test case

        let fn_vis = &input.vis;
        let _fn_sig = &input.sig;
        let fn_body = &input.block;

        // Build the function signature without TestContext (if present)
        // since we'll create it inside each test case
        let mut filtered_inputs = Vec::new();
        let mut has_ctx_param = false;

        for arg in &input.sig.inputs {
            if let syn::FnArg::Typed(pat_type) = arg {
                if let syn::Type::Path(type_path) = pat_type.ty.as_ref() {
                    if let Some(last_segment) = type_path.path.segments.last() {
                        if last_segment.ident == "TestContext" {
                            has_ctx_param = true;
                            continue; // Skip TestContext parameter
                        }
                    }
                }
            }
            filtered_inputs.push(arg.clone());
        }

        // Create new signature without TestContext
        let mut new_sig = input.sig.clone();
        new_sig.inputs = filtered_inputs.into_iter().collect();

        let tracing_block = if enable_tracing {
            quote! {
                TestContext::init_tracing("debug");
            }
        } else {
            quote! {}
        };

        let future_body = if has_ctx_param {
            quote! {
                #tracing_block
                let ctx = TestContext::with_name(test_name).await?;
                let telemetry = sinex_test_utils::runlog::TelemetryHandle::from(ctx.telemetry_handle());
                let inner = async { #fn_body }.await;
                (inner, Some(telemetry))
            }
        } else {
            quote! {
                #tracing_block
                let inner = async { #fn_body }.await;
                (inner, None::<sinex_test_utils::runlog::TelemetryHandle>)
            }
        };

        return quote! {
            #[::rstest::rstest]
            #(#case_attrs)*
            #(#other_attrs)*
            #[tokio::test]
            #fn_vis #new_sig {
                use std::time::Instant;

                let test_name = stringify!(#fn_name);
                let start = Instant::now();
                eprintln!("🔄 {} [rstest case, timeout: {}s]", test_name.replace('_', " "), #timeout_secs);

                let (result, telemetry) = tokio::time::timeout(
                    std::time::Duration::from_secs(#timeout_secs),
                    async { #future_body }
                ).await
                .map_err(|_| color_eyre::eyre::eyre!("Test timed out after {} seconds", #timeout_secs))?;

                let elapsed = start.elapsed();
                if result.is_ok() {
                    eprintln!("✅ {} ({:.1?})", test_name.replace('_', " "), elapsed);
                } else {
                    eprintln!("❌ {} ({:.1?})", test_name.replace('_', " "), elapsed);
                }

                if let Some(handle) = telemetry.as_ref() {
                    sinex_test_utils::runlog::record_async(
                        test_name,
                        elapsed,
                        Some(handle.as_ref()),
                        &result,
                    )
                    .await;
                } else {
                    sinex_test_utils::runlog::record_async(
                        test_name,
                        elapsed,
                        None,
                        &result,
                    )
                    .await;
                }

                result
            }
        }.into();
    }

    let output = if !is_async {
        // Sync test handling
        quote! {
            #(#test_attrs)*
            #[test]
            #fn_vis fn #fn_name() -> color_eyre::eyre::Result<()> {
                use std::thread;
                use std::time::{Duration, Instant};

                let test_name = stringify!(#fn_name);
                let start = Instant::now();
                eprintln!("🔄 {} [sync, timeout: {}s]", test_name.replace('_', " "), #timeout_secs);

                // Start progress thread for longer tests
                let progress_handle = if #timeout_secs > 5 {
                    let test_name_clone = test_name.to_string();
                    let timeout = #timeout_secs;
                    Some(thread::spawn(move || {
                        let mut elapsed = 5;
                        loop {
                            thread::park_timeout(Duration::from_secs(5));
                            eprintln!("  ⏳ {} still running... ({}s elapsed)",
                                     test_name_clone.replace('_', " "), elapsed);
                            elapsed += 5;
                            if elapsed >= timeout - 5 {
                                break;
                            }
                        }
                    }))
                } else {
                    None
                };

                // Run the test
                let result: color_eyre::eyre::Result<()> = (|| {
                    #fn_body
                })();

                // Clean up progress thread
                if let Some(handle) = progress_handle {
                    // Thread will exit on its own, just don't wait for it
                    drop(handle);
                }

                let elapsed = start.elapsed();
                if result.is_ok() {
                    eprintln!("✅ {} ({:.1?})", test_name.replace('_', " "), elapsed);
                } else {
                    eprintln!("❌ {} ({:.1?})", test_name.replace('_', " "), elapsed);
                }

                // Check if we exceeded timeout (soft warning only)
                if elapsed.as_secs() > #timeout_secs {
                    eprintln!("⚠️  {} exceeded timeout of {}s",
                             test_name.replace('_', " "), #timeout_secs);
                }

                sinex_test_utils::runlog::record_sync(
                    test_name,
                    elapsed,
                    &result,
                );

                result
            }
        }
    } else if takes_context {
        // Regular database test using universal pool system with proper cleanup
        quote! {
            #(#test_attrs)*
            #[tokio::test]
            #fn_vis async fn #fn_name() -> color_eyre::eyre::Result<()> {
                let test_future = async {
                    let test_name = stringify!(#fn_name);
                    let start = std::time::Instant::now();
                    eprintln!("🔄 {} [timeout: {}s]", test_name.replace('_', " "), #timeout_secs);

                    let ctx = TestContext::with_name(test_name).await?;
                    let telemetry = ctx.telemetry_handle();

                    let result: color_eyre::eyre::Result<()> = if #timeout_secs > 10 {
                        let progress_task = tokio::spawn(async {
                            let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
                            interval.tick().await;
                            let mut elapsed_secs = 5;
                            loop {
                                interval.tick().await;
                                eprintln!("  ⏳ {} still running... ({}s elapsed)", test_name.replace('_', " "), elapsed_secs);
                                elapsed_secs += 5;
                                if elapsed_secs >= #timeout_secs - 5 {
                                    break;
                                }
                            }
                        });

                        let test_result = async { #fn_body }.await;
                        if !progress_task.is_finished() {
                            progress_task.abort();
                            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        }
                        test_result
                    } else {
                        async { #fn_body }.await
                    };

                    let elapsed = start.elapsed();
                    if result.is_ok() {
                        eprintln!("✅ {} ({:.1?})", test_name.replace('_', " "), elapsed);
                    } else {
                        eprintln!("❌ {} ({:.1?})", test_name.replace('_', " "), elapsed);
                    }

                    sinex_test_utils::runlog::record_async(
                        test_name,
                        elapsed,
                        Some(&telemetry),
                        &result,
                    )
                    .await;

                    result
                };

                tokio::time::timeout(
                    std::time::Duration::from_secs(#timeout_secs),
                    test_future
                )
                .await
                .map_err(|_| color_eyre::eyre::eyre!("Test timed out after {} seconds", #timeout_secs))?
            }
        }
    } else {
        // Simple test - just timeout wrapper
        quote! {
            #(#test_attrs)*
            #[tokio::test]
            #fn_vis async fn #fn_name() -> color_eyre::eyre::Result<()> {
                let test_name = stringify!(#fn_name);
                let start = std::time::Instant::now();
                eprintln!("🔄 {} [simple, timeout: {}s]", test_name.replace('_', " "), #timeout_secs);

                let result = tokio::time::timeout(
                    std::time::Duration::from_secs(#timeout_secs),
                    async { #fn_body }
                ).await
                .map_err(|_| color_eyre::eyre::eyre!("Test timed out after {} seconds", #timeout_secs))?;

                let elapsed = start.elapsed();
                if result.is_ok() {
                    eprintln!("✅ {} ({:.1?})", test_name.replace('_', " "), elapsed);
                } else {
                    eprintln!("❌ {} ({:.1?})", test_name.replace('_', " "), elapsed);
                }

                sinex_test_utils::runlog::record_async(
                    test_name,
                    elapsed,
                    None,
                    &result,
                )
                .await;

                result
            }
        }
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
                format!("unknown bench mode '{}'", other),
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
        if let syn::FnArg::Typed(pat_type) = arg {
            if let syn::Type::Path(type_path) = pat_type.ty.as_ref() {
                if let Some(last_segment) = type_path.path.segments.last() {
                    if last_segment.ident == "BenchContext" && i == 0 {
                        takes_context = true;
                    } else if i == 1 || (i == 0 && !takes_context) {
                        // This is the args parameter
                        takes_args = true;
                        arg_type = Some(pat_type.ty.clone());
                    }
                }
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
                    #[divan::bench(#args_tokens)]
                    #fn_vis fn #fn_name(bencher: divan::Bencher, arg: #arg_type) {
                        use sinex_test_utils::bench::BENCH_CONTEXT;
                        let ctx = &*BENCH_CONTEXT;

                        bencher.bench_local(|| {
                            ctx.runtime.block_on(async {
                                let result: color_eyre::eyre::Result<()> = async {
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
                        use sinex_test_utils::bench::BENCH_CONTEXT;
                        let ctx = &*BENCH_CONTEXT;

                        bencher.bench_local(|| {
                            ctx.runtime.block_on(async {
                                let result: color_eyre::eyre::Result<()> = async {
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
                    #[divan::bench(#args_tokens)]
                    #fn_vis fn #fn_name(bencher: divan::Bencher, arg: #arg_type) {
                        let runtime = tokio::runtime::Runtime::new().unwrap();

                        bencher.bench_local(|| {
                            runtime.block_on(async {
                                let result: color_eyre::eyre::Result<()> = async {
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
                                let result: color_eyre::eyre::Result<()> = async {
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
