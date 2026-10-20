use {
    jsonrpc_core::{
        Error, ErrorCode, Metadata, Middleware, Request, Response, Version,
        futures::future::{self, Either},
        middleware::{NoopCallFuture, NoopFuture},
    },
    std::future::Future,
};

/// Maximum number of calls in a single JSON-RPC batch request.
pub(crate) const MAX_BATCH_SIZE: usize = 16;

/// Rejects JSON-RPC batch requests with more than [`MAX_BATCH_SIZE`] calls.
#[derive(Clone, Debug, Default)]
pub(crate) struct BatchSizeLimit;

impl<M: Metadata> Middleware<M> for BatchSizeLimit {
    type Future = NoopFuture;
    type CallFuture = NoopCallFuture;

    fn on_request<F, X>(&self, request: Request, meta: M, next: F) -> Either<Self::Future, X>
    where
        F: Fn(Request, M) -> X + Send + Sync,
        X: Future<Output = Option<Response>> + Send + 'static,
    {
        match request {
            Request::Batch(calls) if calls.len() > MAX_BATCH_SIZE => {
                let error = Error {
                    code: ErrorCode::InvalidRequest,
                    message: format!("Batch request too large; max {MAX_BATCH_SIZE} calls"),
                    data: None,
                };
                Either::Left(Box::pin(future::ready(Some(Response::from(
                    error,
                    Some(Version::V2),
                )))))
            }
            request => Either::Right(next(request, meta)),
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        jsonrpc_core::{MetaIoHandler, Value},
    };

    fn io() -> MetaIoHandler<(), BatchSizeLimit> {
        let mut io = MetaIoHandler::with_middleware(BatchSizeLimit);
        io.add_method("ping", |_| async { Ok(Value::String("pong".to_string())) });
        io
    }

    fn handle_batch(len: usize) -> Value {
        let request: Vec<_> = (0..len)
            .map(|id| json!({"jsonrpc": "2.0", "id": id, "method": "ping"}))
            .collect();
        let response = io()
            .handle_request_sync(&Value::from(request).to_string(), ())
            .expect("response");
        serde_json::from_str(&response).expect("response deserialization")
    }

    #[test]
    fn test_batch_at_limit_is_processed() {
        let response = handle_batch(MAX_BATCH_SIZE);
        let outputs = response.as_array().expect("batch response");
        assert_eq!(outputs.len(), MAX_BATCH_SIZE);
        assert!(outputs.iter().all(|output| output["result"] == "pong"));
    }

    #[test]
    fn test_batch_over_limit_is_rejected() {
        assert_eq!(
            handle_batch(MAX_BATCH_SIZE + 1),
            json!({
                "jsonrpc": "2.0",
                "error": {
                    "code": -32600,
                    "message": format!("Batch request too large; max {MAX_BATCH_SIZE} calls"),
                },
                "id": null,
            }),
        );
    }

    #[test]
    fn test_single_request_is_processed() {
        let response = io()
            .handle_request_sync(r#"{"jsonrpc":"2.0","id":1,"method":"ping"}"#, ())
            .expect("response");
        assert_eq!(
            serde_json::from_str::<Value>(&response).unwrap(),
            json!({"jsonrpc": "2.0", "result": "pong", "id": 1}),
        );
    }
}
