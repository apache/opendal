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
use std::sync::RwLock;

use vhost::vhost_user::message::{VhostUserProtocolFeatures, VhostUserVirtioFeatures};
use vhost::vhost_user::Backend;
use vhost_user_backend::{VhostUserBackend, VringMutex, VringState, VringT};
use virtio_bindings::bindings::virtio_config::VIRTIO_F_VERSION_1;
use virtio_bindings::bindings::virtio_ring::{
    VIRTIO_RING_F_EVENT_IDX, VIRTIO_RING_F_INDIRECT_DESC,
};
use vm_memory::{ByteValued, GuestMemoryAtomic, GuestMemoryMmap, Le32};
use vmm_sys_util::epoll::EventSet;
use vmm_sys_util::eventfd::EventFd;

use crate::error::*;

/// Marks an event from the high priority queue.
const HIPRIO_QUEUE_EVENT: u16 = 0;
/// Marks an event from the request queue.
const REQ_QUEUE_EVENT: u16 = 1;
/// The maximum number of bytes in VirtioFsConfig tag field.
const MAX_TAG_LEN: usize = 36;
/// The maximum queue size supported.
const QUEUE_SIZE: usize = 32768;
/// The number of request queues supported.
/// The vitrofs spec allows for mutiple request queues, but we'll only support one.
const REQUEST_QUEUES: u32 = 1;
/// In addition to the request queues there is one high priority queue.
const NUM_QUEUES: usize = REQUEST_QUEUES as usize + 1;

/// VhostUserFsThread will be serialized and used as
/// the return value of get_config function in the VhostUserBackend trait.
struct VhostUserFsThread {
    mem: Option<GuestMemoryAtomic<GuestMemoryMmap>>,
    vu_req: Option<Backend>,
    event_idx: bool,
    kill_event_fd: EventFd,
}

#[allow(dead_code)]
impl VhostUserFsThread {
    fn new() -> Result<VhostUserFsThread> {
        let event_fd = EventFd::new(libc::EFD_NONBLOCK).map_err(|err| {
            new_unexpected_error("failed to create kill eventfd", Some(err.into()))
        })?;
        Ok(VhostUserFsThread {
            mem: None,
            vu_req: None,
            event_idx: false,
            kill_event_fd: event_fd,
        })
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
                vring_state.disable_notification().unwrap();
                self.process_queue_serial(&mut vring_state)?;
                if !vring_state.enable_notification().unwrap() {
                    break;
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
    fn process_queue_serial(&self, _vring_state: &mut VringState) -> Result<bool> {
        unimplemented!()
    }
}

/// VhostUserFsBackend is a structure that implements the VhostUserBackend trait
/// and implements concrete services for the vhost user backend server.
pub struct VhostUserFsBackend {
    tag: Option<String>,
    thread: RwLock<VhostUserFsThread>,
}

#[allow(dead_code)]
impl VhostUserFsBackend {
    pub fn new(tag: Option<String>) -> Result<VhostUserFsBackend> {
        let thread = RwLock::new(VhostUserFsThread::new()?);
        Ok(VhostUserFsBackend { thread, tag })
    }
}

/// VirtioFsConfig will be serialized and used as
/// the return value of get_config function in the VhostUserBackend trait.
#[repr(C)]
#[derive(Clone, Copy)]
struct VirtioFsConfig {
    tag: [u8; MAX_TAG_LEN],
    num_request_queues: Le32,
}

unsafe impl ByteValued for VirtioFsConfig {}

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
        1 << VIRTIO_F_VERSION_1
            | 1 << VIRTIO_RING_F_INDIRECT_DESC
            | 1 << VIRTIO_RING_F_EVENT_IDX
            | VhostUserVirtioFeatures::PROTOCOL_FEATURES.bits()
    }

    /// Get available vhost protocol features.
    fn protocol_features(&self) -> VhostUserProtocolFeatures {
        // Align to the virtiofsd's protocol features here.
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

    /// Enable or disabled the virtio EVENT_IDX feature.
    fn set_event_idx(&self, enabled: bool) {
        self.thread.write().unwrap().event_idx = enabled;
    }

    /// Get virtio device configuration.
    fn get_config(&self, offset: u32, size: u32) -> Vec<u8> {
        let tag = self.tag.as_ref().expect("Did not expect read of config if tag is not set.");
        let mut fixed_len_tag = [0; MAX_TAG_LEN];
        fixed_len_tag[0..tag.len()].copy_from_slice(tag.as_bytes());
        let config = VirtioFsConfig {
            tag: fixed_len_tag,
            num_request_queues: Le32::from(REQUEST_QUEUES),
        };
        let mut result: Vec<_> = config
            .as_slice()
            .iter()
            .skip(offset as usize)
            .take(size as usize)
            .copied()
            .collect();
        result.resize(size as usize, 0);
        result
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
                "failed to handle handle event other than input event",
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
