use {
    crate::handshake::{
        ClientHandshakeError, ClientLogon, ClientSession, ClientWorkerSession, JitoClientSession,
        JitoClientWorkerSession, logon_flags,
        shared::{LOGON_FAILURE, LOGON_SUCCESS, MAX_WORKERS, VERSION},
    },
    agave_scheduler_bindings::{CheckWorkerToPackMessage, PackToCheckWorkerMessage},
    libc::CMSG_LEN,
    nix::sys::socket::{self, ControlMessageOwned, MsgFlags, UnixAddr},
    rts_alloc::Allocator,
    std::{
        fs::File,
        io::{IoSliceMut, Write},
        os::{
            fd::{AsRawFd, FromRawFd},
            unix::net::UnixStream,
        },
        path::Path,
        time::Duration,
    },
};

/// Number of global shared memory objects (in addition to per worker objects).
const GLOBAL_SHMEM: usize = 5;

/// The maximum size in bytes of the control message containing the queues assuming [`MAX_WORKERS`]
/// is respected.
///
/// Each FD is 4 bytes so we simply multiply the number of shmem objects by 4 to get the control
/// message buffer size.
const CMSG_MAX_SIZE: usize = (GLOBAL_SHMEM + 3 + MAX_WORKERS * 4) * 4;

/// Connects to the scheduler server on the given IPC path.
///
/// # Timeout
///
/// Timeout is enforced at the syscall level. In the typical case, this function will do two
/// syscalls, one to send the logon message and one to receive the response. However, if for
/// whatever reason the OS does not accept 1024 bytes in a single syscall, then multiple writes
/// could be needed. As such this timeout is meant to guard against a broken server but not
/// necessarily ensure this function always returns before the timeout (this is somewhat in line
/// with typical timeouts because you have no guarantee of being rescheduled).
pub fn connect(
    path: impl AsRef<Path>,
    logon: ClientLogon,
    timeout: Duration,
) -> Result<ClientSession, ClientHandshakeError> {
    connect_path(path.as_ref(), logon, timeout)
}

fn connect_path(
    path: &Path,
    logon: ClientLogon,
    timeout: Duration,
) -> Result<ClientSession, ClientHandshakeError> {
    // NB: Technically this connect call can block indefinitely if the receiver's connection queue
    // is full. In practice this should almost never happen. If it does work arounds are:
    //
    // - Users can spawn off a thread to handle the connect call and then just poll that thread
    //   exiting.
    // - This library could drop to raw unix sockets and use select/poll to enforce a timeout on the
    //   IO operation.
    let mut stream = UnixStream::connect(path)?;
    stream.set_read_timeout(Some(timeout))?;
    stream.set_write_timeout(Some(timeout))?;

    // Send the logon message to the server.
    send_logon(&mut stream, logon)?;

    // Receive the server's response & on success the files for the newly allocated shared memory.
    let files = recv_response(&mut stream)?;

    // Join the shared memory regions.
    let session = setup_session(&logon, files)?;

    Ok(session)
}

fn send_logon(stream: &mut UnixStream, logon: ClientLogon) -> Result<(), ClientHandshakeError> {
    // Send the logon message.
    let mut buf = [0; 1024];
    buf[..8].copy_from_slice(&VERSION.to_le_bytes());
    const LOGON_END: usize = 8 + core::mem::size_of::<ClientLogon>();
    let ptr = buf[8..LOGON_END].as_mut_ptr().cast::<ClientLogon>();
    // SAFETY:
    // - `buf` is valid for writes.
    // - `buf.len()` has enough space for logon's size in memory.
    unsafe {
        core::ptr::write_unaligned(ptr, logon);
    }
    stream.write_all(&buf)?;

    Ok(())
}

fn recv_response(stream: &mut UnixStream) -> Result<Vec<File>, ClientHandshakeError> {
    // Receive the requested FDs.
    let mut buf = [0; 1024];
    let mut iov = [IoSliceMut::new(&mut buf)];
    // SAFETY: CMSG_LEN is always safe (const expression).
    let mut cmsgs = [0u8; unsafe { CMSG_LEN(CMSG_MAX_SIZE as u32) as usize }];
    let msg = socket::recvmsg::<UnixAddr>(
        stream.as_raw_fd(),
        &mut iov,
        Some(&mut cmsgs),
        MsgFlags::empty(),
    )?;

    let truncated = msg.flags.contains(MsgFlags::MSG_CTRUNC);
    // Own every received descriptor immediately, including malformed responses.
    // This ensures that errors close descriptors rather than leaking them.
    let mut files = Vec::new();
    for cmsg in msg.cmsgs()? {
        if let ControlMessageOwned::ScmRights(fds) = cmsg {
            files.extend(fds.into_iter().map(|fd| {
                // SAFETY: the descriptor was just received from SCM_RIGHTS.
                unsafe { File::from_raw_fd(fd) }
            }));
        }
    }
    let bytes_read = msg.bytes;
    let buf = msg
        .iovs()
        .next()
        .ok_or(ClientHandshakeError::ProtocolViolation)?;
    if truncated || bytes_read == 0 {
        return Err(ClientHandshakeError::ProtocolViolation);
    }
    match buf[0] {
        LOGON_FAILURE => {
            if bytes_read < 2 {
                return Err(ClientHandshakeError::ProtocolViolation);
            }
            let end = 2usize
                .checked_add(usize::from(buf[1]))
                .ok_or(ClientHandshakeError::ProtocolViolation)?;
            if end > bytes_read {
                return Err(ClientHandshakeError::ProtocolViolation);
            }
            let reason = std::str::from_utf8(&buf[2..end])
                .map_err(|_| ClientHandshakeError::ProtocolViolation)?;
            return Err(ClientHandshakeError::Rejected(reason.to_string()));
        }
        LOGON_SUCCESS if !files.is_empty() => {}
        _ => return Err(ClientHandshakeError::ProtocolViolation),
    }

    Ok(files)
}

pub fn setup_session(
    logon: &ClientLogon,
    files: Vec<File>,
) -> Result<ClientSession, ClientHandshakeError> {
    if logon.flags != 0 && logon.flags != logon_flags::JITO {
        return Err(ClientHandshakeError::ProtocolViolation);
    }
    let jito_enabled = logon.flags == logon_flags::JITO;
    let worker_file_count = if jito_enabled { 4usize } else { 2 };
    let worker_end = logon
        .worker_count
        .checked_mul(worker_file_count)
        .and_then(|count| GLOBAL_SHMEM.checked_add(count))
        .ok_or(ClientHandshakeError::ProtocolViolation)?;
    let expected_files = worker_end
        .checked_add(if jito_enabled { 3 } else { 0 })
        .ok_or(ClientHandshakeError::ProtocolViolation)?;
    if logon.worker_count == 0 || files.len() != expected_files {
        return Err(ClientHandshakeError::ProtocolViolation);
    }
    let (global_files, rest) = files.split_at(GLOBAL_SHMEM);
    let (worker_files, jito_files) = rest.split_at(worker_end.checked_sub(GLOBAL_SHMEM).unwrap());
    let [
        allocator_file,
        tpu_to_pack_file,
        progress_tracker_file,
        pack_to_check_worker_file,
        check_worker_to_pack_file,
    ] = global_files
    else {
        unreachable!();
    };

    // Setup requested allocators.
    let allocators = (0..logon.allocator_handles)
        .map(|_| Allocator::join(allocator_file))
        .collect::<Result<Vec<_>, _>>()?;

    let jito = if jito_enabled {
        let [ingress, completion, progress] = jito_files else {
            return Err(ClientHandshakeError::ProtocolViolation);
        };
        // SAFETY: the addon version and exact descriptor count were validated;
        // the trusted server creates these queues in this order with these types.
        Some(unsafe {
            JitoClientSession {
                ingress: shaq::spsc::Consumer::join(ingress)?,
                completion: shaq::spsc::Producer::join(completion)?,
                progress: shaq::spsc::Consumer::join(progress)?,
            }
        })
    } else {
        None
    };

    // NB: After creating & mapping the queues we are fine to drop the files as mmap will keep the
    // underlying object alive until process exit or munmap.
    let session = ClientSession {
        jito,
        allocators,
        tpu_to_pack: unsafe { shaq::spsc::Consumer::join(tpu_to_pack_file)? },
        progress_tracker: unsafe { shaq::spsc::Consumer::join(progress_tracker_file)? },
        // SAFETY: the server initialized this FD as a matching MPMC consumer.
        pack_to_check_worker: unsafe {
            shaq::mpmc::Producer::<PackToCheckWorkerMessage>::join(pack_to_check_worker_file)?
        },
        // SAFETY: the server initialized this FD as a matching MPMC producer.
        check_worker_to_pack: unsafe {
            shaq::mpmc::Consumer::<CheckWorkerToPackMessage>::join(check_worker_to_pack_file)?
        },
        workers: worker_files
            .chunks_exact(worker_file_count)
            .map(|window| {
                let jito = if jito_enabled {
                    // SAFETY: descriptor order and addon version match the server.
                    Some(unsafe {
                        JitoClientWorkerSession {
                            request: shaq::spsc::Producer::join(&window[2])?,
                            response: shaq::spsc::Consumer::join(&window[3])?,
                        }
                    })
                } else {
                    None
                };
                Ok(ClientWorkerSession {
                    jito,
                    // SAFETY: unchanged upstream descriptor order and message types.
                    pack_to_worker: unsafe { shaq::spsc::Producer::join(&window[0])? },
                    worker_to_pack: unsafe { shaq::spsc::Consumer::join(&window[1])? },
                })
            })
            .collect::<Result<_, ClientHandshakeError>>()?,
    };

    // Drop the file handles now that mmaps are completed.
    drop(files);

    Ok(session)
}

impl From<nix::Error> for ClientHandshakeError {
    fn from(value: nix::Error) -> Self {
        Self::Io(value.into())
    }
}
