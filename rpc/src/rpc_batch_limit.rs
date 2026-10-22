use {
    jsonrpc_core::{
        Call, Error, Failure, Metadata, Middleware, Output, Request, Response,
        futures::future::Either,
        middleware::{NoopCallFuture, NoopFuture},
    },
    std::future::Future,
};

/// Maximum number of `simulateBundle` calls executed per JSON-RPC batch request.
pub(crate) const MAX_SIMULATE_BUNDLE_CALLS_PER_BATCH: usize = 16;

const SIMULATE_BUNDLE: &str = "simulateBundle";

/// Rejects `simulateBundle` calls beyond the first [`MAX_SIMULATE_BUNDLE_CALLS_PER_BATCH`] in a
/// JSON-RPC batch request. The rest of the batch runs as usual.
#[derive(Clone, Debug, Default)]
pub(crate) struct SimulateBundleBatchLimit;

impl<M: Metadata> Middleware<M> for SimulateBundleBatchLimit {
    type Future = NoopFuture;
    type CallFuture = NoopCallFuture;

    fn on_request<F, X>(&self, request: Request, meta: M, next: F) -> Either<Self::Future, X>
    where
        F: Fn(Request, M) -> X + Send + Sync,
        X: Future<Output = Option<Response>> + Send + 'static,
    {
        let Request::Batch(calls) = request else {
            return Either::Right(next(request, meta));
        };

        // One entry per output in request order: `None` for an output from `next`, `Some` for a
        // rejected call. Notifications produce no output.
        let mut outputs = Vec::with_capacity(calls.len());
        let mut allowed = Vec::with_capacity(calls.len());
        let mut simulate_bundle_calls = 0usize;
        for call in calls {
            match call {
                Call::MethodCall(method_call) if method_call.method == SIMULATE_BUNDLE => {
                    simulate_bundle_calls += 1;
                    if simulate_bundle_calls > MAX_SIMULATE_BUNDLE_CALLS_PER_BATCH {
                        outputs.push(Some(Output::Failure(Failure {
                            jsonrpc: method_call.jsonrpc,
                            error: Error::invalid_params(format!(
                                "Too many simulateBundle calls in batch; max \
                                 {MAX_SIMULATE_BUNDLE_CALLS_PER_BATCH}"
                            )),
                            id: method_call.id,
                        })));
                    } else {
                        outputs.push(None);
                        allowed.push(Call::MethodCall(method_call));
                    }
                }
                notification @ Call::Notification(_) => allowed.push(notification),
                other => {
                    outputs.push(None);
                    allowed.push(other);
                }
            }
        }

        let response = next(Request::Batch(allowed), meta);
        if simulate_bundle_calls <= MAX_SIMULATE_BUNDLE_CALLS_PER_BATCH {
            return Either::Right(response);
        }
        Either::Left(Box::pin(async move {
            let mut processed = match response.await {
                Some(Response::Batch(processed)) => processed,
                Some(Response::Single(output)) => vec![output],
                None => Vec::new(),
            }
            .into_iter();
            let outputs = outputs
                .into_iter()
                .filter_map(|output| output.or_else(|| processed.next()))
                .collect();
            Some(Response::Batch(outputs))
        }))
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        jsonrpc_core::{MetaIoHandler, Value},
        std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    const MAX: usize = MAX_SIMULATE_BUNDLE_CALLS_PER_BATCH;

    /// Returns a handler along with the number of times its `simulateBundle` method has run.
    fn io() -> (
        MetaIoHandler<(), SimulateBundleBatchLimit>,
        Arc<AtomicUsize>,
    ) {
        let simulations = Arc::new(AtomicUsize::default());
        let mut io = MetaIoHandler::with_middleware(SimulateBundleBatchLimit);
        io.add_method("ping", |_| async { Ok(Value::from("pong")) });
        let counter = simulations.clone();
        io.add_method(SIMULATE_BUNDLE, move |_| {
            counter.fetch_add(1, Ordering::Relaxed);
            async { Ok(Value::from("simulated")) }
        });
        (io, simulations)
    }

    fn handle(io: &MetaIoHandler<(), SimulateBundleBatchLimit>, request: Value) -> Value {
        let response = io
            .handle_request_sync(&request.to_string(), ())
            .expect("response");
        serde_json::from_str(&response).expect("response deserialization")
    }

    fn call(id: usize, method: &str) -> Value {
        json!({"jsonrpc": "2.0", "id": id, "method": method})
    }

    fn success(id: usize, result: &str) -> Value {
        json!({"jsonrpc": "2.0", "result": result, "id": id})
    }

    fn rejected(id: usize) -> Value {
        json!({
            "jsonrpc": "2.0",
            "error": {
                "code": -32602,
                "message": format!("Too many simulateBundle calls in batch; max {MAX}"),
            },
            "id": id,
        })
    }

    #[test]
    fn test_other_methods_are_not_limited() {
        let (io, _simulations) = io();
        let batch: Vec<_> = (0..2 * MAX).map(|id| call(id, "ping")).collect();
        let expected: Vec<_> = (0..2 * MAX).map(|id| success(id, "pong")).collect();
        assert_eq!(handle(&io, Value::from(batch)), Value::from(expected));
    }

    #[test]
    fn test_simulate_bundle_calls_at_limit_are_processed() {
        let (io, simulations) = io();
        let mut batch: Vec<_> = (0..MAX).map(|id| call(id, SIMULATE_BUNDLE)).collect();
        batch.push(call(MAX, "ping"));
        let mut expected: Vec<_> = (0..MAX).map(|id| success(id, "simulated")).collect();
        expected.push(success(MAX, "pong"));

        assert_eq!(handle(&io, Value::from(batch)), Value::from(expected));
        assert_eq!(simulations.load(Ordering::Relaxed), MAX);
    }

    #[test]
    fn test_excess_simulate_bundle_calls_are_rejected_in_place() {
        let (io, simulations) = io();
        let mut batch = vec![call(0, "ping")];
        batch.extend((1..=MAX).map(|id| call(id, SIMULATE_BUNDLE)));
        batch.push(json!({"jsonrpc": "2.0", "method": "ping"})); // notification, no output
        batch.push(call(MAX + 1, SIMULATE_BUNDLE));
        batch.push(json!({"id": MAX + 2})); // invalid entry
        batch.push(call(MAX + 3, SIMULATE_BUNDLE));
        batch.push(call(MAX + 4, "ping"));

        let mut expected = vec![success(0, "pong")];
        expected.extend((1..=MAX).map(|id| success(id, "simulated")));
        expected.push(rejected(MAX + 1));
        expected.push(json!({
            "jsonrpc": "2.0",
            "error": {"code": -32600, "message": "Invalid request"},
            "id": MAX + 2,
        }));
        expected.push(rejected(MAX + 3));
        expected.push(success(MAX + 4, "pong"));

        assert_eq!(handle(&io, Value::from(batch)), Value::from(expected));
        assert_eq!(simulations.load(Ordering::Relaxed), MAX);
    }

    #[test]
    fn test_simulate_bundle_notifications_are_not_counted() {
        let (io, simulations) = io();
        let mut batch: Vec<_> = (0..MAX)
            .map(|_| json!({"jsonrpc": "2.0", "method": SIMULATE_BUNDLE}))
            .collect();
        batch.extend((0..MAX).map(|id| call(id, SIMULATE_BUNDLE)));
        let expected: Vec<_> = (0..MAX).map(|id| success(id, "simulated")).collect();

        assert_eq!(handle(&io, Value::from(batch)), Value::from(expected));
        assert_eq!(simulations.load(Ordering::Relaxed), MAX);
    }

    #[test]
    fn test_single_request_is_processed() {
        let (io, simulations) = io();
        assert_eq!(
            handle(&io, call(1, SIMULATE_BUNDLE)),
            success(1, "simulated")
        );
        assert_eq!(simulations.load(Ordering::Relaxed), 1);
    }
}
