use {
    crate::banking_stage::BankingControlMsg, agave_scheduling_utils::handshake, std::path::Path,
    tokio::sync::mpsc,
};

pub(crate) fn spawn(
    path: &Path,
    session_sender: mpsc::Sender<BankingControlMsg>,
    validate: impl Fn() -> Result<(), &'static str> + Send + 'static,
) {
    // NB: Panic on start if we can't bind.
    let _ = std::fs::remove_file(path);
    let mut listener = handshake::server::Server::new(path).unwrap();

    std::thread::Builder::new()
        .name("solBindingSrv".to_string())
        .spawn(move || {
            loop {
                match listener.accept_with_validation(&validate) {
                    Ok(session) => {
                        if session_sender
                            .blocking_send(BankingControlMsg::External { session })
                            .is_err()
                        {
                            break;
                        }
                    }
                    Err(err) => {
                        error!("External scheduler handshake failed; err={err}")
                    }
                };
            }
        })
        .unwrap();
}
