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

use vhost::vhost_user::message::*;
use vhost::vhost_user::Backend;
use vhost_user_backend::{VhostUserBackend, VringMutex, VringState, VringT};
use virtio_bindings::bindings::virtio_config::*;
use virtio_bindings::bindings::virtio_ring::{
    VIRTIO_RING_F_EVENT_IDX, VIRTIO_RING_F_INDIRECT_DESC,
};
use vm_memory::{ByteValued, GuestMemoryAtomic, GuestMemoryMmap, Le32};
use vmm_sys_util::epoll::EventSet;
use vmm_sys_util::eventfd::EventFd;

use crate::error::*;

const QUEUE_SIZE: usize = 32768;
const REQUEST_QUEUES: u32 = 1;
const NUM_QUEUES: usize = REQUEST_QUEUES as usize + 1;
const HIPRIO_QUEUE_EVENT: u16 = 0;
const REQ_QUEUE_EVENT: u16 = 1;
const MAX_TAG_LEN: usize = 36;

struct VhostUserFsThread {
    mem: Option<GuestMemoryAtomic<GuestMemoryMmap>>,
    vu_req: Option<Backend>,
    event_idx: bool,
    kill_event_fd: EventFd,
}

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

    fn process_queue_serial(&self, _vring_state: &mut VringState) -> Result<bool> {
        unimplemented!()
    }

    fn handle_event_serial(&self, device_event: u16, vrings: &[VringMutex]) -> Result<()> {
        let mut vring_state = match device_event {
            HIPRIO_QUEUE_EVENT => vrings[0].get_mut(),
            REQ_QUEUE_EVENT => vrings[1].get_mut(),
            _ => return Err(new_unexpected_error("failed to handle unknown event", None)),
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

unsafe impl ByteValued for VirtioFsConfig {}

struct VhostUserFsBackend {
    thread: RwLock<VhostUserFsThread>,
    tag: Option<String>,
}

impl VhostUserFsBackend {
    fn new(tag: Option<String>) -> Result<Self> {
        let thread = RwLock::new(VhostUserFsThread::new()?);
        Ok(VhostUserFsBackend { thread, tag })
    }
}

impl VhostUserBackend for VhostUserFsBackend {
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

    fn update_memory(&self, mem: GuestMemoryAtomic<GuestMemoryMmap>) -> io::Result<()> {
        self.thread.write().unwrap().mem = Some(mem);
        Ok(())
    }

    fn handle_event(
        &self,
        device_event: u16,
        evset: EventSet,
        vrings: &[Self::Vring],
        _thread_id: usize,
    ) -> io::Result<()> {
        if evset != EventSet::IN {
            return Err(new_unexpected_error("failed to handle handle event other than input event", None).into());
        }
        let thread = self.thread.read().unwrap();
        thread
            .handle_event_serial(device_event, vrings)
            .map_err(|err| err.into())
    }

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

    fn set_backend_req_fd(&self, vu_req: Backend) {
        self.thread.write().unwrap().vu_req = Some(vu_req);
    }
}
