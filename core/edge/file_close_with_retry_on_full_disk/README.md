# File Close with Retry on Full Disk

Reported by [fs::Writer::poll_close can't be retried multiple times when error occurs](https://github.com/apache/opendal/issues/4058).

Setup:

```shell
fallocate -l 512K disk.img 
mkfs disk.img 
mkdir ./td
sudo mount -o loop td.img ./td
chmod a+wr ./td
```

