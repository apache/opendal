mod descriptor_utils;
mod filesystem;
mod fuse;
mod passthrough;
mod server;

#[macro_use]
extern crate log;

use libc::EFD_NONBLOCK;
use log::*;
use std::convert::{self, TryInto};
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::{env, error, fmt, io, process};

use vhost::vhost_user::message::*;
use vhost::vhost_user::Error::Disconnected;
use vhost::vhost_user::{Backend, Listener};
use vhost_user_backend::Error::HandleRequest;
use vhost_user_backend::{VhostUserBackend, VhostUserDaemon, VringMutex, VringState, VringT};
use virtio_bindings::bindings::virtio_config::*;
use virtio_bindings::bindings::virtio_ring::{
    VIRTIO_RING_F_EVENT_IDX, VIRTIO_RING_F_INDIRECT_DESC,
};
use virtio_queue::{DescriptorChain, QueueOwnedT};
use vm_memory::{
    ByteValued, GuestAddressSpace, GuestMemoryAtomic, GuestMemoryLoadGuard, GuestMemoryMmap, Le32,
};
use vmm_sys_util::epoll::EventSet;
use vmm_sys_util::eventfd::EventFd;

use crate::descriptor_utils::{Error as VufDescriptorError, Reader, Writer};
use crate::filesystem::FileSystem;
use crate::passthrough::ovfs_filesystem::*;
use crate::server::{Error as VhostUserFsError, Server};

const QUEUE_SIZE: usize = 32768;
const REQUEST_QUEUES: u32 = 1;
const NUM_QUEUES: usize = REQUEST_QUEUES as usize + 1;
const HIPRIO_QUEUE_EVENT: u16 = 0;
const REQ_QUEUE_EVENT: u16 = 1;
const MAX_TAG_LEN: usize = 36;

type Result<T> = std::result::Result<T, Error>;
type VhostUserBackendResult<T> = std::result::Result<T, std::io::Error>;

#[allow(dead_code)]
#[derive(Debug)]
enum Error {
    CreateKillEventFd(io::Error),
    CreateThreadPool(io::Error),
    HandleEventNotEpollIn,
    HandleEventUnknownEvent,
    IterateQueue,
    NoMemoryConfigured,
    ProcessQueue(VhostUserFsError),
    QueueReader(VufDescriptorError),
    QueueWriter(VufDescriptorError),
    UnshareCloneFs(io::Error),
    InvalidTag,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use self::Error::UnshareCloneFs;
        match self {
            UnshareCloneFs(error) => {
                write!(
                    f,
                    "The unshare(CLONE_FS) syscall failed with '{error}'. \
                    If running in a container please check that the container \
                    runtime seccomp policy allows unshare."
                )
            }
            Self::InvalidTag => write!(
                f,
                "The tag may not be empty or longer than {MAX_TAG_LEN} bytes (encoded as UTF-8)."
            ),
            _ => write!(f, "{self:?}"),
        }
    }
}

impl error::Error for Error {}

impl convert::From<Error> for io::Error {
    fn from(e: Error) -> Self {
        io::Error::new(io::ErrorKind::Other, e)
    }
}

struct VhostUserFsThread<F: FileSystem + Send + Sync + 'static> {
    mem: Option<GuestMemoryAtomic<GuestMemoryMmap>>,
    kill_evt: EventFd,
    server: Arc<Server<F>>,
    vu_req: Option<Backend>,
    event_idx: bool,
}

impl<F: FileSystem + Send + Sync + 'static> Clone for VhostUserFsThread<F> {
    fn clone(&self) -> Self {
        VhostUserFsThread {
            mem: self.mem.clone(),
            kill_evt: self.kill_evt.try_clone().unwrap(),
            server: self.server.clone(),
            vu_req: self.vu_req.clone(),
            event_idx: self.event_idx,
        }
    }
}

impl<F: FileSystem + Send + Sync + 'static> VhostUserFsThread<F> {
    fn new(fs: F) -> Result<Self> {
        Ok(VhostUserFsThread {
            mem: None,
            kill_evt: EventFd::new(EFD_NONBLOCK).map_err(Error::CreateKillEventFd)?,
            server: Arc::new(Server::new(fs)),
            vu_req: None,
            event_idx: false,
        })
    }

    fn return_descriptor(
        vring_state: &mut VringState,
        head_index: u16,
        event_idx: bool,
        len: usize,
    ) {
        let used_len: u32 = match len.try_into() {
            Ok(l) => l,
            Err(_) => panic!("Invalid used length, can't return used descritors to the ring"),
        };

        if vring_state.add_used(head_index, used_len).is_err() {
            warn!("Couldn't return used descriptors to the ring");
        }

        if event_idx {
            match vring_state.needs_notification() {
                Err(_) => {
                    warn!("Couldn't check if queue needs to be notified");
                    vring_state.signal_used_queue().unwrap();
                }
                Ok(needs_notification) => {
                    if needs_notification {
                        vring_state.signal_used_queue().unwrap();
                    }
                }
            }
        } else {
            vring_state.signal_used_queue().unwrap();
        }
    }

    fn process_queue_serial(&self, vring_state: &mut VringState) -> Result<bool> {
        let mut used_any = false;
        let mem = match &self.mem {
            Some(m) => m.memory(),
            None => return Err(Error::NoMemoryConfigured),
        };

        let avail_chains: Vec<DescriptorChain<GuestMemoryLoadGuard<GuestMemoryMmap>>> = vring_state
            .get_queue_mut()
            .iter(mem.clone())
            .map_err(|_| Error::IterateQueue)?
            .collect();

        for chain in avail_chains {
            used_any = true;

            let head_index = chain.head_index();

            let reader = Reader::new(&mem, chain.clone())
                .map_err(Error::QueueReader)
                .unwrap();
            let writer = Writer::new(&mem, chain.clone())
                .map_err(Error::QueueWriter)
                .unwrap();

            let len = self
                .server
                .handle_message(reader, writer)
                .map_err(Error::ProcessQueue)
                .unwrap();

            Self::return_descriptor(vring_state, head_index, self.event_idx, len);
        }

        Ok(used_any)
    }

    fn handle_event_serial(
        &self,
        device_event: u16,
        vrings: &[VringMutex],
    ) -> VhostUserBackendResult<()> {
        let mut vring_state = match device_event {
            HIPRIO_QUEUE_EVENT => {
                debug!("HIPRIO_QUEUE_EVENT");
                vrings[0].get_mut()
            }
            REQ_QUEUE_EVENT => {
                debug!("QUEUE_EVENT");
                vrings[1].get_mut()
            }
            _ => return Err(Error::HandleEventUnknownEvent.into()),
        };

        if self.event_idx {
            loop {
                vring_state.disable_notification().unwrap();
                self.process_queue_serial(&mut vring_state)?;
                if !vring_state.enable_notification().unwrap() {
                    break;
                }
            }
        } else {
            self.process_queue_serial(&mut vring_state)?;
        }

        Ok(())
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
struct VirtioFsConfig {
    tag: [u8; MAX_TAG_LEN],
    num_request_queues: Le32,
}

impl Default for VirtioFsConfig {
    fn default() -> Self {
        Self {
            tag: [0; MAX_TAG_LEN],
            num_request_queues: Le32::default(),
        }
    }
}

unsafe impl ByteValued for VirtioFsConfig {}

struct VhostUserFsBackend<F: FileSystem + Send + Sync + 'static> {
    thread: RwLock<VhostUserFsThread<F>>,
    tag: Option<String>,
}

impl<F: FileSystem + Send + Sync + 'static> VhostUserFsBackend<F> {
    fn new(fs: F, tag: Option<String>) -> Result<Self> {
        let thread = RwLock::new(VhostUserFsThread::new(fs)?);
        Ok(VhostUserFsBackend { thread, tag })
    }
}

impl<F: FileSystem + Send + Sync + 'static> VhostUserBackend for VhostUserFsBackend<F> {
    type Bitmap = ();
    type Vring = VringMutex;

    fn num_queues(&self) -> usize {
        NUM_QUEUES
    }

    fn max_queue_size(&self) -> usize {
        QUEUE_SIZE
    }

    fn features(&self) -> u64 {
        1 << VIRTIO_F_VERSION_1
            | 1 << VIRTIO_RING_F_INDIRECT_DESC
            | 1 << VIRTIO_RING_F_EVENT_IDX
            | VhostUserVirtioFeatures::PROTOCOL_FEATURES.bits()
    }

    fn protocol_features(&self) -> VhostUserProtocolFeatures {
        let mut protocol_features = VhostUserProtocolFeatures::MQ
            | VhostUserProtocolFeatures::BACKEND_REQ
            | VhostUserProtocolFeatures::BACKEND_SEND_FD
            | VhostUserProtocolFeatures::REPLY_ACK
            | VhostUserProtocolFeatures::CONFIGURE_MEM_SLOTS;

        if self.tag.is_some() {
            protocol_features |= VhostUserProtocolFeatures::CONFIG;
        }

        protocol_features
    }

    fn get_config(&self, offset: u32, size: u32) -> Vec<u8> {
        let tag = self.tag.as_ref().expect("Did not expect read of config if tag is not set. We do not advertise F_CONFIG in that case!");
        assert!(tag.len() <= MAX_TAG_LEN, "too long tag length");
        assert!(!tag.is_empty(), "tag should not be empty");
        let mut fixed_len_tag = [0; MAX_TAG_LEN];
        fixed_len_tag[0..tag.len()].copy_from_slice(tag.as_bytes());

        let config = VirtioFsConfig {
            tag: fixed_len_tag,
            num_request_queues: Le32::from(REQUEST_QUEUES),
        };

        let offset = offset as usize;
        let size = size as usize;
        let mut result: Vec<_> = config
            .as_slice()
            .iter()
            .skip(offset)
            .take(size)
            .copied()
            .collect();
        result.resize(size, 0);
        result
    }

    fn set_event_idx(&self, enabled: bool) {
        self.thread.write().unwrap().event_idx = enabled;
    }

    fn update_memory(&self, mem: GuestMemoryAtomic<GuestMemoryMmap>) -> VhostUserBackendResult<()> {
        self.thread.write().unwrap().mem = Some(mem);
        Ok(())
    }

    fn handle_event(
        &self,
        device_event: u16,
        evset: EventSet,
        vrings: &[VringMutex],
        _thread_id: usize,
    ) -> VhostUserBackendResult<()> {
        if evset != EventSet::IN {
            return Err(Error::HandleEventNotEpollIn.into());
        }

        let thread = self.thread.read().unwrap();

        thread.handle_event_serial(device_event, vrings)
    }

    fn exit_event(&self, _thread_index: usize) -> Option<EventFd> {
        Some(self.thread.read().unwrap().kill_evt.try_clone().unwrap())
    }

    fn set_backend_req_fd(&self, vu_req: Backend) {
        self.thread.write().unwrap().vu_req = Some(vu_req);
    }
}

fn set_signal_handlers() {
    use vmm_sys_util::signal;

    extern "C" fn handle_signal(_: libc::c_int, _: *mut libc::siginfo_t, _: *mut libc::c_void) {
        unsafe { libc::_exit(1) };
    }
    let signals = vec![libc::SIGHUP, libc::SIGTERM];
    for s in signals {
        if let Err(e) = signal::register_signal_handler(s, handle_signal) {
            error!("Setting signal handlers: {}", e);
            process::exit(1);
        }
    }
}

fn main() {
    env::set_var("RUST_LOG", LevelFilter::Debug.to_string());
    env_logger::init();

    set_signal_handlers();

    let listener = {
        let socket = "/tmp/vhostqemu";

        let socket_parent_dir = Path::new(socket).parent().unwrap_or_else(|| {
            error!("Invalid socket file name");
            process::exit(1);
        });

        if !socket_parent_dir.as_os_str().is_empty() && !socket_parent_dir.exists() {
            error!(
                "{} does not exist or is not a directory",
                socket_parent_dir.to_string_lossy()
            );
            process::exit(1);
        }

        let listener = Listener::new(socket, true).unwrap_or_else(|error| {
            error!("Error creating listener: {}", error);
            process::exit(1);
        });

        listener
    };

    let fs = OVFSFileSystem::new();

    let fs_backend = Arc::new(VhostUserFsBackend::new(fs, None).unwrap_or_else(|error| {
        error!("Error creating vhost-user backend: {}", error);
        process::exit(1)
    }));

    let mut daemon = VhostUserDaemon::new(
        String::from("virtiofsd-backend"),
        fs_backend.clone(),
        GuestMemoryAtomic::new(GuestMemoryMmap::new()),
    )
    .unwrap();

    info!("Waiting for vhost-user socket connection...");

    if let Err(e) = daemon.start(listener) {
        error!("Failed to start daemon: {:?}", e);
        process::exit(1);
    }

    info!("Client connected, servicing requests");

    if let Err(e) = daemon.wait() {
        match e {
            HandleRequest(Disconnected) => info!("Client disconnected, shutting down"),
            _ => error!("Waiting for daemon failed: {:?}", e),
        }
    }

    let kill_evt = fs_backend
        .thread
        .read()
        .unwrap()
        .kill_evt
        .try_clone()
        .unwrap();
    if let Err(e) = kill_evt.write(1) {
        error!("Error shutting down worker thread: {:?}", e)
    }
}
