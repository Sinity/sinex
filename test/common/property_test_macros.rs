/// Macros for property-based testing in Sinex
///
/// These macros integrate proptest with our test infrastructure to make
/// property testing more ergonomic and consistent.

/// Create a property test with database support
///
/// Usage:
/// ```
/// sinex_proptest! {
///     #[sinex_test]
///     async fn test_name(
///         event in arbitrary_event(),
///         count in 1usize..10
///     ) {
///         // Test body with database access
///         let pool = ctx.db_pool();
///         // ...
///     }
/// }
/// ```
#[macro_export]
macro_rules! sinex_proptest {
    (
        #[sinex_test]
        async fn $name:ident(
            $($param:ident in $strategy:expr),* $(,)?
        ) $body:block
    ) => {
        #[sinex_test]
        async fn $name(ctx: TestContext) {
            use proptest::prelude::*;
            
            let config = ProptestConfig::with_cases(100);
            let mut runner = TestRunner::new(config);
            
            let strategy = ($($strategy,)*);
            
            runner.run(&strategy, |($($param,)*)| {
                let test_future = async {
                    $body
                };
                
                // Run the async test
                let runtime = tokio::runtime::Runtime::new().unwrap();
                runtime.block_on(test_future);
                
                Ok(())
            }).unwrap();
        }
    };
}

/// Create a synchronous property test
///
/// Usage:
/// ```
/// sinex_proptest_sync! {
///     fn test_name(
///         ulid in arbitrary_ulid(),
///         size in 0usize..1000
///     ) {
///         // Synchronous test body
///         assert!(ulid != Ulid::nil());
///     }
/// }
/// ```
#[macro_export]
macro_rules! sinex_proptest_sync {
    (
        fn $name:ident(
            $($param:ident in $strategy:expr),* $(,)?
        ) $body:block
    ) => {
        #[test]
        fn $name() {
            use proptest::prelude::*;
            
            proptest!(|($($param in $strategy),*)| {
                $body
            });
        }
    };
}

/// Create a property test that generates test cases based on invariants
///
/// Usage:
/// ```
/// property_invariant! {
///     name: ulid_ordering,
///     given: (a: Ulid, b: Ulid),
///     invariant: |a, b| {
///         if a < b {
///             assert!(a.to_string() < b.to_string())
///         }
///     }
/// }
/// ```
#[macro_export]
macro_rules! property_invariant {
    (
        name: $name:ident,
        given: ($($param:ident : $type:ty),* $(,)?),
        invariant: $check:expr $(,)?
    ) => {
        #[test]
        fn $name() {
            use proptest::prelude::*;
            
            proptest!(|($(
                $param: $type,
            )*)|{
                let check_fn = $check;
                check_fn($($param),*);
            });
        }
    };
}

/// Create a property test with custom configuration
///
/// Usage:
/// ```
/// configured_proptest! {
///     #[cases(1000)]
///     #[max_shrink_iters(50)]
///     fn test_name(
///         events in arbitrary_event_batch()
///     ) {
///         assert!(!events.is_empty());
///     }
/// }
/// ```
#[macro_export]
macro_rules! configured_proptest {
    (
        #[cases($cases:expr)]
        $(#[max_shrink_iters($shrink:expr)])?
        fn $name:ident(
            $($param:ident in $strategy:expr),* $(,)?
        ) $body:block
    ) => {
        #[test]
        fn $name() {
            use proptest::prelude::*;
            
            let mut config = ProptestConfig::with_cases($cases);
            $(config.max_shrink_iters = $shrink;)?
            
            let mut runner = TestRunner::new(config);
            let strategy = ($($strategy,)*);
            
            runner.run(&strategy, |($($param,)*)| {
                $body
                Ok(())
            }).unwrap();
        }
    };
}

/// Create a stateful property test that maintains state across operations
///
/// Usage:
/// ```
/// stateful_proptest! {
///     name: queue_operations,
///     state: VecDeque<Event>,
///     operations: [
///         push_front(event: Event) => {
///             state.push_front(event);
///             assert!(state.len() > 0);
///         },
///         pop_back() => {
///             let old_len = state.len();
///             state.pop_back();
///             assert_eq!(state.len(), old_len.saturating_sub(1));
///         }
///     ]
/// }
/// ```
#[macro_export]
macro_rules! stateful_proptest {
    (
        name: $name:ident,
        state: $state_type:ty,
        operations: [
            $(
                $op_name:ident($($param:ident : $param_type:ty),* $(,)?) => $op_body:block
            ),* $(,)?
        ] $(,)?
    ) => {
        #[test]
        fn $name() {
            use proptest::prelude::*;
            
            #[derive(Debug, Clone)]
            enum Operation {
                $(
                    $op_name { $($param: $param_type),* },
                )*
            }
            
            fn arbitrary_operation() -> impl Strategy<Value = Operation> {
                prop_oneof![
                    $(
                        any::<($($param_type,)*)>().prop_map(|($($param,)*)| {
                            Operation::$op_name { $($param),* }
                        }),
                    )*
                ]
            }
            
            proptest!(|(ops in proptest::collection::vec(arbitrary_operation(), 0..100))| {
                let mut state: $state_type = Default::default();
                
                for op in ops {
                    match op {
                        $(
                            Operation::$op_name { $($param),* } => {
                                $op_body
                            }
                        )*
                    }
                }
            });
        }
    };
}

/// Create a property test that checks multiple related properties
///
/// Usage:
/// ```
/// property_suite! {
///     name: event_properties,
///     given: arbitrary_event(),
///     properties: {
///         has_valid_id: |event| {
///             assert_ne!(event.id, Ulid::nil());
///         },
///         has_source: |event| {
///             assert!(!event.source.is_empty());
///         },
///         has_type: |event| {
///             assert!(!event.event_type.is_empty());
///         }
///     }
/// }
/// ```
#[macro_export]
macro_rules! property_suite {
    (
        name: $suite_name:ident,
        given: $strategy:expr,
        properties: {
            $(
                $prop_name:ident : $check:expr
            ),* $(,)?
        } $(,)?
    ) => {
        mod $suite_name {
            use super::*;
            use proptest::prelude::*;
            
            $(
                #[test]
                fn $prop_name() {
                    proptest!(|(value in $strategy)| {
                        let check_fn = $check;
                        check_fn(value);
                    });
                }
            )*
        }
    };
}

/// Create a regression test from a failing property test case
///
/// Usage:
/// ```
/// regression_test! {
///     name: specific_ulid_case,
///     // This value caused a failure in property testing
///     input: Ulid::from_string("01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
///     test: |ulid| {
///         assert_eq!(ulid.to_string().len(), 26);
///     }
/// }
/// ```
#[macro_export]
macro_rules! regression_test {
    (
        name: $name:ident,
        input: $input:expr,
        test: $test:expr $(,)?
    ) => {
        #[test]
        fn $name() {
            let input = $input;
            let test_fn = $test;
            test_fn(input);
        }
    };
}

/// Create a property test that compares two implementations
///
/// Usage:
/// ```
/// differential_proptest! {
///     name: json_parsing,
///     input: arbitrary_json_string(),
///     implementations: {
///         serde: |s| serde_json::from_str::<Value>(s),
///         custom: |s| custom_json_parser::parse(s),
///     }
/// }
/// ```
#[macro_export]
macro_rules! differential_proptest {
    (
        name: $name:ident,
        input: $strategy:expr,
        implementations: {
            $impl1:ident : $fn1:expr,
            $impl2:ident : $fn2:expr $(,)?
        } $(,)?
    ) => {
        #[test]
        fn $name() {
            use proptest::prelude::*;
            
            proptest!(|(input in $strategy)| {
                let result1 = $fn1(&input);
                let result2 = $fn2(&input);
                
                match (result1, result2) {
                    (Ok(v1), Ok(v2)) => {
                        assert_eq!(v1, v2, 
                                   "Implementations {} and {} should produce same result",
                                   stringify!($impl1), stringify!($impl2));
                    }
                    (Err(_), Err(_)) => {
                        // Both failed - consistent
                    }
                    _ => {
                        panic!("Implementations {} and {} disagree on validity",
                               stringify!($impl1), stringify!($impl2));
                    }
                }
            });
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::prelude::*;
    use crate::common::property_builders::*;
    
    // Example usage of the macros
    
    sinex_proptest_sync! {
        fn example_ulid_property(
            ulid in arbitrary_ulid()
        ) {
            assert_ne!(ulid, Ulid::nil());
            assert_eq!(ulid.to_string().len(), 26);
        }
    }
    
    property_invariant! {
        name: example_invariant,
        given: (a: u32, b: u32),
        invariant: |a, b| {
            assert_eq!(a + b, b + a); // Commutative property
        }
    }
    
    configured_proptest! {
        #[cases(50)]
        fn example_configured(
            events in arbitrary_event_batch()
        ) {
            assert!(events.len() <= 50);
        }
    }
    
    property_suite! {
        name: example_suite,
        given: arbitrary_event(),
        properties: {
            has_id: |event| {
                assert_ne!(event.id, Ulid::nil());
            },
            has_source: |event| {
                assert!(!event.source.is_empty());
            }
        }
    }
}