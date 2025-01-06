# File Write on Fill Disk

Reported by [The `FsBackend` only writes partial bytes and doesn't raise any errors](https://github.com/apache/opendal/issues/3052).

Setup:

```shell
fallocate -l 512K disk.img 
mkfs disk.img 
mkdir ./td
sudo mount -o loop td.img ./td
chmod a+wr ./td
```

