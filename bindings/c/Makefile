#
# Build the example client
#

RPATH=$(PWD)/../../target/debug
CFLAGS = -I./include
LDFLAGS = -L$(RPATH) -Wl,-rpath,$(RPATH)
LIBS = -lopendal
OBJ_DIR=obj

all: $(OBJ_DIR) build test

$(OBJ_DIR):
	mkdir -p $@

build:
	cargo build

test:
	$(CC) tests/hello.c -o $(OBJ_DIR)/hello $(CFLAGS) $(LDFLAGS) $(LIBS)

clean:
	cargo clean
	rm -rf $(OBJ_DIR)
