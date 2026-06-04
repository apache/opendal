#define __NR_io_uring_setup 425
#define __NR_io_uring_enter 426
#define __NR_io_uring_register 427

typedef signed int __s32;
typedef signed long long __s64;
typedef unsigned char __u8;
typedef unsigned short __u16;
typedef unsigned int __u32;
typedef unsigned long long __u64;
typedef int __kernel_rwf_t;

#define __aligned_u64 __u64 __attribute__((aligned(8)))
#define __DECLARE_FLEX_ARRAY(TYPE, NAME) \
	struct { \
		struct { } __empty_ ## NAME; \
		TYPE NAME[]; \
	}
#define SPLICE_F_FD_IN_FIXED (1U << 31)

struct __kernel_timespec {
	__s64 tv_sec;
	__s64 tv_nsec;
};

struct open_how {
	__u64 flags;
	__u64 mode;
	__u64 resolve;
};

#define UAPI_LINUX_IO_URING_H_SKIP_LINUX_TIME_TYPES_H
#define OPENDAL_IO_URING_BINDGEN_TYPES
#include "linux/io_uring.h"

#define FUTEX_WAITV_MAX 128
struct futex_waitv {
	__u64 val;
	__u64 uaddr;
	__u32 flags;
	__u32 __reserved;
};
