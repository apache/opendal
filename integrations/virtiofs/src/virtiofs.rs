// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::io;
use std::sync::Arc;
use std::sync::RwLock;

use log::warn;
use opendal::Operator;
use vhost::vhost_user::message::VhostUserProtocolFeatures;
use vhost::vhost_user::message::VhostUserVirtioFeatures;
use vhost::vhost_user::Backend;
use vhost::vhost_user::Listener;
use vhost_user_backend::VhostUserBackend;
use vhost_user_backend::VhostUserDaemon;
use vhost_user_backend::VringMutex;
use vhost_user_backend::VringState;
use vhost_user_backend::VringT;
use virtio_bindings::bindings::virtio_config::VIRTIO_F_VERSION_1;
use virtio_bindings::bindings::virtio_ring::VIRTIO_RING_F_EVENT_IDX;
use virtio_bindings::bindings::virtio_ring::VIRTIO_RING_F_INDIRECT_DESC;
use virtio_queue::DescriptorChain;
use virtio_queue::QueueOwnedT;
use vm_memory::GuestAddressSpace;
use vm_memory::GuestMemoryAtomic;
use vm_memory::GuestMemoryLoadGuard;
use vm_memory::GuestMemoryMmap;
use vmm_sys_util::epoll::EventSet;
use vmm_sys_util::eventfd::EventFd;

use crate::error::*;
use crate::filesystem::Filesystem;
use crate::virtiofs_util::Reader;
use crate::virtiofs_util::Writer;

/// Marks an event from the high priority queue.
const HIPRIO_QUEUE_EVENT: u16 = 0;
/// Marks an event from the request queue.
const REQ_QUEUE_EVENT: u16 = 1;
/// The maximum queue size supported.
const QUEUE_SIZE: usize = 32768;
/// The number of request queues supported.
/// The vitrofs spec allows for multiple request queues, but we'll only support one.
const REQUEST_QUEUES: usize = 1;
/// In addition to request queues there is one high priority queue.
const NUM_QUEUES: usize = REQUEST_QUEUES + 1;

/// VhostUserFsThread represents the actual worker process used to handle file system requests from VMs.
struct VhostUserFsThread {
    core: Filesystem,
    mem: Option<GuestMemoryAtomic<GuestMemoryMmap>>,
    vu_req: Option<Backend>,
    event_idx: bool,
    kill_event_fd: EventFd,
}

impl VhostUserFsThread {
    fn new(core: Filesystem) -> Result<VhostUserFsThread> {
        let event_fd = EventFd::new(libc::EFD_NONBLOCK).map_err(|err| {
            new_unexpected_error("failed to create kill eventfd", Some(err.into()))
        })?;
        Ok(VhostUserFsThread {
            core,
            mem: None,
            vu_req: None,
            event_idx: false,
            kill_event_fd: event_fd,
        })
    }

    /// This is used when the backend has processed a request and needs to notify the frontend.
    fn return_descriptor(
        vring_state: &mut VringState,
        head_index: u16,
        event_idx: bool,
        len: usize,
    ) {
        if vring_state.add_used(head_index, len as u32).is_err() {
            warn!("Failed to add used to used queue.");
        };
        // Check if the used queue needs to be signaled.
        if event_idx {
            match vring_state.needs_notification() {
                Ok(needs_notification) => {
                    if needs_notification && vring_state.signal_used_queue().is_err() {
                        warn!("Failed to signal used queue.");
                    }
                }
                Err(_) => {
                    if vring_state.signal_used_queue().is_err() {
                        warn!("Failed to signal used queue.");
                    };
                }
            }
        } else if vring_state.signal_used_queue().is_err() {
            warn!("Failed to signal used queue.");
        }
    }

    /// Process filesystem requests one at a time in a serialized manner.
    fn handle_event_serial(&self, device_event: u16, vrings: &[VringMutex]) -> Result<()> {
        let mut vring_state = match device_event {
            HIPRIO_QUEUE_EVENT => vrings[0].get_mut(),
            REQ_QUEUE_EVENT => vrings[1].get_mut(),
            _ => return Err(new_unexpected_error("failed to handle unknown event", None)),
        };
        if self.event_idx {
            // If EVENT_IDX is enabled, we could keep calling process_queue()
            // until it stops finding new request on the queue.
            loop {
                if vring_state.disable_notification().is_err() {
                    warn!("Failed to disable used queue notification.");
                }
                self.process_queue_serial(&mut vring_state)?;
                if let Ok(has_more) = vring_state.enable_notification() {
                    if !has_more {
                        break;
                    }
                } else {
                    warn!("Failed to enable used queue notification.");
                }
            }
        } else {
            // Without EVENT_IDX, a single call is enough.
            self.process_queue_serial(&mut vring_state)?;
        }
        Ok(())
    }

    /// Forwards filesystem messages to specific functions and
    /// returns the filesystem request execution result.
    fn process_queue_serial(&self, vring_state: &mut VringState) -> Result<bool> {
        let mut used_any = false;
        let mem = match &self.mem {
            Some(m) => m.memory(),
            None => return Err(new_unexpected_error("no memory configured", None)),
        };
        let avail_chains: Vec<DescriptorChain<GuestMemoryLoadGuard<GuestMemoryMmap>>> = vring_state
            .get_queue_mut()
            .iter(mem.clone())
            .map_err(|_| new_unexpected_error("iterating through the queue failed", None))?
            .collect();
        for chain in avail_chains {
            used_any = true;
            let head_index = chain.head_index();
            let reader = Reader::new(&mem, chain.clone())
                .map_err(|_| new_unexpected_error("creating a queue reader failed", None))
                .unwrap();
            let writer = Writer::new(&mem, chain.clone())
                .map_err(|_| new_unexpected_error("creating a queue writer failed", None))
                .unwrap();
            let len = self
                .core
                .handle_message(reader, writer)
                .map_err(|_| new_unexpected_error("processing a queue request failed", None))
                .unwrap();
            VhostUserFsThread::return_descriptor(vring_state, head_index, self.event_idx, len);
        }
        Ok(used_any)
    }
}

/// VhostUserFsBackend is a structure that implements the VhostUserBackend trait
/// and implements concrete services for the vhost user backend server.
struct VhostUserFsBackend {
    thread: RwLock<VhostUserFsThread>,
}

impl VhostUserFsBackend {
    fn new(core: Filesystem) -> Result<VhostUserFsBackend> {
        let thread = RwLock::new(VhostUserFsThread::new(core)?);
        Ok(VhostUserFsBackend { thread })
    }

    fn kill(&self) -> Result<()> {
        self.thread
            .read()
            .unwrap()
            .kill_event_fd
            .write(1)
            .map_err(|err| {
                new_unexpected_error("failed to write to kill eventfd", Some(err.into()))
            })
    }
}

impl VhostUserBackend for VhostUserFsBackend {
    type Bitmap = ();
    type Vring = VringMutex;

    /// Get number of queues supported.
    fn num_queues(&self) -> usize {
        NUM_QUEUES
    }

    /// Get maximum queue size supported.
    fn max_queue_size(&self) -> usize {
        QUEUE_SIZE
    }

    /// Get available virtio features.
    fn features(&self) -> u64 {
        // Align to the virtiofsd's features here.
        (1 << VIRTIO_F_VERSION_1)
            | (1 << VIRTIO_RING_F_INDIRECT_DESC)
            | (1 << VIRTIO_RING_F_EVENT_IDX)
            | VhostUserVirtioFeatures::PROTOCOL_FEATURES.bits()
    }

    /// Get available vhost protocol features.
    fn protocol_features(&self) -> VhostUserProtocolFeatures {
        // Align to the virtiofsd's protocol features here.
        VhostUserProtocolFeatures::MQ
            | VhostUserProtocolFeatures::BACKEND_REQ
            | VhostUserProtocolFeatures::BACKEND_SEND_FD
            | VhostUserProtocolFeatures::REPLY_ACK
            | VhostUserProtocolFeatures::CONFIGURE_MEM_SLOTS
    }

    /// Enable or disabled the virtio EVENT_IDX feature.
    fn set_event_idx(&self, enabled: bool) {
        self.thread.write().unwrap().event_idx = enabled;
    }

    /// Update guest memory regions.
    fn update_memory(&self, mem: GuestMemoryAtomic<GuestMemoryMmap>) -> io::Result<()> {
        self.thread.write().unwrap().mem = Some(mem);
        Ok(())
    }

    /// Set handler for communicating with the frontend by the backend communication channel.
    fn set_backend_req_fd(&self, vu_req: Backend) {
        self.thread.write().unwrap().vu_req = Some(vu_req);
    }

    /// Provide an optional exit EventFd for the specified worker thread.
    fn exit_event(&self, _thread_index: usize) -> Option<EventFd> {
        Some(
            self.thread
                .read()
                .unwrap()
                .kill_event_fd
                .try_clone()
                .unwrap(),
        )
    }

    /// Handle IO events for backend registered file descriptors.
    fn handle_event(
        &self,
        device_event: u16,
        evset: EventSet,
        vrings: &[Self::Vring],
        _thread_id: usize,
    ) -> io::Result<()> {
        if evset != EventSet::IN {
            return Err(new_unexpected_error(
                "failed to handle event other than input event",
                None,
            )
            .into());
        }
        let thread = self.thread.read().unwrap();
        thread
            .handle_event_serial(device_event, vrings)
            .map_err(|err| err.into())
    }
}

/// VirtioFS is a structure that represents the virtiofs service.
/// It is used to run the virtiofs service with the given operator and socket path.
/// The operator is used to interact with the backend storage system.
/// The socket path is used to communicate with the QEMU and VMs.
pub struct VirtioFs {
    socket_path: String,
    filesystem_backend: Arc<VhostUserFsBackend>,
}

impl VirtioFs {
    pub fn new(core: Operator, socket_path: &str) -> Result<VirtioFs> {
        let filesystem_core = Filesystem::new(core);
        let filesystem_backend = Arc::new(VhostUserFsBackend::new(filesystem_core).unwrap());
        Ok(VirtioFs {
            socket_path: socket_path.to_string(),
            filesystem_backend,
        })
    }

    // Run the virtiofs service.
    pub fn run(&self) -> Result<()> {
        let listener = Listener::new(&self.socket_path, true)
            .map_err(|_| new_unexpected_error("failed to create listener", None))?;
        let mut daemon = VhostUserDaemon::new(
            String::from("virtiofs-backend"),
            self.filesystem_backend.clone(),
            GuestMemoryAtomic::new(GuestMemoryMmap::new()),
        )
        .unwrap();
        if daemon.start(listener).is_err() {
            return Err(new_unexpected_error("failed to start daemon", None));
        }
        if daemon.wait().is_err() {
            return Err(new_unexpected_error("failed to wait daemon", None));
        }
        Ok(())
    }

    // Kill the virtiofs service.
    pub fn kill(&self) -> Result<()> {
        if self.filesystem_backend.kill().is_err() {
            return Err(new_unexpected_error("failed to kill backend", None));
        }
        Ok(())
    }
}
