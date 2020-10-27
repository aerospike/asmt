# Makes asmt (Aerospike shared memory tool).

DIR_TARGET = target
DIR_OBJ = $(DIR_TARGET)/obj
DIR_BIN = $(DIR_TARGET)/bin

SRC_DIRS = src
OBJ_DIRS = $(SRC_DIRS:%=$(DIR_OBJ)/%)

ASMT_SRC = asmt.c hardware.c

ASMT_SOURCES = $(ASMT_SRC:%=src/%)

ASMT_OBJECTS = $(ASMT_SOURCES:%.c=$(DIR_OBJ)/%.o)

ASMT_BINARY = $(DIR_BIN)/asmt

ALL_OBJECTS = $(ASMT_OBJECTS)
ALL_DEPENDENCIES = $(ALL_OBJECTS:%.o=%.d)

COMMIT_H = src/commit.h

CC = gcc
CFLAGS = -g -fno-common -std=gnu99 -D_REENTRANT -D_FILE_OFFSET_BITS=64 -Wall
CFLAGS += -D_GNU_SOURCE -MMD
LDFLAGS = $(CFLAGS)
INCLUDES = -Isrc -I/usr/include
LIBRARIES = -lpthread -lrt -lz
COMMIT = $(shell git describe --always 2>/dev/null)

default: all

all: asmt

$(COMMIT_H):
	echo 'static const char g_commit[] = "$(COMMIT)";' > $(COMMIT_H)

target_dir:
	@/bin/mkdir -p $(DIR_BIN) $(OBJ_DIRS)

asmt: target_dir $(COMMIT_H) $(ASMT_OBJECTS)
	@echo "Linking $@"
	$(CC) $(LDFLAGS) -o $(ASMT_BINARY) $(ASMT_OBJECTS) $(LIBRARIES)
	rm -f $(COMMIT_H)

# For now we only clean everything.
clean:
	/bin/rm -f $(COMMIT_H)
	/bin/rm -rf $(DIR_TARGET)

-include $(ALL_DEPENDENCIES)

$(DIR_OBJ)/%.o: %.c
	@echo "Building $@"
	$(CC) $(CFLAGS) -o $@ -c $(INCLUDES) $<

