#
# Build the example client
#

RPATH=$(PWD)/../../../target/debug
CFLAGS = -I../include
LDFLAGS = -L$(RPATH) -Wl,-rpath,$(RPATH)
LIBS = -lopendal

all: build

build:
	$(CC) hello.c -o hello $(CFLAGS) $(LDFLAGS) $(LIBS)
