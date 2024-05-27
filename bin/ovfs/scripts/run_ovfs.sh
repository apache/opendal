#!/usr/bin/env bash

# This shell file should not be run directly and commands here can be used directly in Ubuntu 22.04
exit 0

## Preapre
sudo apt-get update
sudo apt install libcap-ng-dev libseccomp-dev
sudo apt-get install qemu qemu-kvm

git clone https://gitlab.com/virtio-fs/virtiofsd.git
cargo build --release

# The kernel version of Ubuntu 20.04 is exactly 5.4, which meets the minimum requirements of virtiofs
wget https://releases.ubuntu.com/20.04/ubuntu-20.04.6-live-server-amd64.iso
qemu-img create -f qcow2 ubuntu.qcow2 20G
qemu-system-x86_64 -enable-kvm -smp 8 -m 16G -cdrom ubuntu-20.04.6-live-server-amd64.iso -drive file=ubuntu.qcow2,format=qcow2 -boot d

## Run
host# ./run_s3_with_minio.sh

host# ./target/release/ovfs --socket-path=/tmp/vhostqemu

host# qemu-system-x86_64 -M pc -cpu host --enable-kvm -smp 8 \
     -m 16G -object memory-backend-file,id=mem,size=4G,mem-path=/dev/shm,share=on -numa node,memdev=mem \
     -chardev socket,id=char0,path=/tmp/vhostqemu -device vhost-user-fs-pci,queue-size=1024,chardev=char0,tag=myfs \
     -chardev stdio,mux=on,id=mon -mon chardev=mon,mode=readline -device virtio-serial-pci -device virtconsole,chardev=mon -vga none -display none \
     -drive if=virtio,file=ubuntu.qcow2

guest# sudo mount -t virtiofs myfs /mnt
