# Aerospike Shared Memory Tool (ASMT)

ASMT provides a program for backing up and restoring the primary index
of an Aerospike Database Enterpise Edition (EE).

* [Overview](#overview)
  * [What Does ASMT Do?](#what-does-asmt-do)
* [Building from Source](#building-from-source)
  * [Getting Started](#getting-started)
    * [Clone the Repo](#clone-the-repo)
    * [Download the Source](#download-the-source)
  * [Install the Dependencies](#install-the-dependencies)
    * [For CentOS](#for-centos)
    * [For Debian or Ubuntu](#for-debian-or-ubuntu)
  * [Compile](#compile)
* [Using ASMT](#using-asmt)
  * [Taking a Backup of the Primary Index](#taking-a-backup-of-the-primary-index)
  * [Restoring the Primary Index from an ASMT Backup](#restoring-the-primary-index-from-an-asmt-backup)
  * [ASMT Options](#asmt-options)
  * [Common Errors](#common-errors)
* [License](#license)

## Overview

After an Aerospike Database server node has been shut down cleanly, ASMT may
be used to save the server node's primary index from shared memory to files in
the file system. The server node (host machine) may then be rebooted, after
which ASMT can be used to restore the index from the file system to shared
memory. This in turn enables the Aerospike Database server to
[fast restart](https://www.aerospike.com/docs/operations/manage/aerospike/fast_start/index.html),
even though it was rebooted.

**Note:** ASMT cannot be used while the Aerospike Database server is running.

### What Does ASMT Do?

With ASMT, shared memory segments corresponding
to an Aerospike Database primary index are backed up (copied from shared
memory to files in the file system) and restored (copied from files in the file
system to shared memory).

Once the primary index has been copied to shared memory by ASMT,
the Aerospike Database node may be restarted, in which case Aerospike Database
will find the primary index in shared memory and avoid reconstructing it.
Reconstructing the primary index without reading it from shared memory requires
scanning all of the records in the database, which can be time-consuming
(a [cold start](https://www.aerospike.com/docs/operations/manage/aerospike/cold_start/index.html)).

## Building from Source

You can use a compiled binary associated with one of the
[releases](https://github.com/aerospike/asmt/releases). The `asmt` binary is
statically built on CentOS 7, and should work for most x86-64 Linux distros.

Alternatively you can compile `asmt` yourself.

### Getting Started

There are two options for getting the source code:

#### Clone the Repo

```
$ git clone https://github.com/aerospike/asmt.git
```

This creates an `asmt` directory.

#### Download the Source

Alternatively, you can download the ZIP or TAR file from the git repository.
When you unpack/untar the file, it creates an /aerospike-asmt-\<version\>
directory.

### Install the Dependencies

Before you can build ASMT, you must install certain libraries.

#### For CentOS
```
$ sudo yum install make gcc zlib-devel
```

#### For Debian or Ubuntu
```
$ sudo apt-get install make gcc libc6-dev zlib1g-dev
```

### Compile

```
$ cd asmt
$ make
```

This will create a binary in a `target/bin` directory: `target/bin/asmt`

## Using ASMT

Copy the asmt binary (as an executable) to wherever is most convenient on the
relevant Aerospike Database server node, and change to that directory.

### Taking a Backup of the Primary Index
After checking that the server node is not running, run the asmt binary to
back up the primary index, for example:

```
$ ./asmt -b -v -p /path/to/index/backup
```

where:

```
 -b - back up (as opposed to restore - must specify one or the other)
 -v - verbose output (optional but recommended)
 -p <backup path> - mandatory directory path specifying where your index backup
files will be saved
```

The above example will back up all relevant namespaces' indexes for server
instance 0, the most common usage.

To back up a different server instance, use `-i` to specify the instance index
(see below).

To back up a specific namespace only, use `-n` to specify the namespace name
(see below). **Note:** A comma-separated list of namespace names may be supplied,
e.g., `-n foo,bar,test`.

To compress the files while backing them up, use the `-z` option.

For other back up options, use `-h` or see the list below.

**Note:** ASMT must be run with the same user and group that was used to run the
Aerospike database server. If you ran the Aerospike database server as user
root, group root, you must run ASMT as user root, group root. The sudo command
can facilitate this.

When the back up is complete, there will be files in the specified directory
corresponding to all the relevant shared memory blocks. The file names are the
same as the shared memory keys. Uncompressed file names end in '.dat' while
compressed files end in '.dat.gz'. Note that the base file for a namespace
is never compressed as ASMT must examine its contents prior to restoring any
files for its namespace.

Note that if files with the relevant names already exist in the directory,
the backup will not start, i.e., it will not overwrite existing files.

If the back up was successful, the host machine may then be rebooted. The index
shared memory blocks are lost, but ASMT will enable the primary index to be
restored after reboot.

### Restoring the Primary Index from an ASMT Backup
After reboot, but before restarting the Aerospike Database server, run the asmt
binary to restore the primary index, for example:

```
$ ./asmt -r -v -p /path/to/index/backup
```

where:

```
 -r - restore (as opposed to back up - must specify one or the other)
 -v - verbose output (optional but recommended)
 -p <backup path> - mandatory directory path specifying where your index backup
files have been saved
```

This example usage will restore all saved namespaces' indexes for server
instance 0.

To restore a different server instance, use `-i` to specify the instance index.

To restore a specific namespace only, use `-n` to specify the namespace name.
(**Note:** A comma-separated list of namespace names may be supplied, e.g.,
`-n foo,bar,test`.)

There is no need to specify the `-z` option when restoring, even if the files
are compressed (i.e., they were created using the `-z` option).

For other restore options, use `-h` or see the list below.

**Note:** ASMT must be run with the same user and group that was used to run the
Aerospike database server. If you ran the Aerospike database server as user
root, group root, you must run ASMT as user root, group root. The sudo command
can facilitate this.

### ASMT Options

```
usage: asmt [-a] [-b] [-c] [-h] [-i <instance>] [-n <name>[,<name>...]]
            -p <pathdir> [-r] [-t <threads>] [-v] [-z]

-a analyze (advisory - goes with '-b' or '-r')
-b back up (operation or advisory with '-a')
-c compare crc32 values of segments and segment files
-h help
-i filter by instance (default is instance 0)
-n filter by namespace name (default is all namespaces)
-p path of directory (mandatory)
-r restore (operation or advisory with '-a')
-t maximum number of threads for I/O
-v verbose output
-z compress files on backup
```

These options have the following meanings:

`-a`	used to analyze whether a back up or restore can be performed without
	    actually performing the back up or restore.

`-b`	perform a back up operation, to copy Aerospike Database's primary index
	    from shared memory to files in the file system. May be combined with
	    '-a' to check whether a back up may be performed without performing the
	    back up.

`-c`	compute a standard 32-bit cyclic redundancy check (CRC-32) to ensure
	    that the contents of the primary index were not corrupted during a
    	back up operation, during a restore operation, or while resident in
	    the file system. Without the `-z` option (see below), computing the
	    CRC-32 may be computationally expensive. When used with `-z`, the cost
    	is much lower, and may be considered negligible.

`-h`	show information on how to use ASMT.

`-i`	select a particular Aerospike Database instance, e.g., `-i 1`. The
	    default instance is 0.

`-n`	select a particular Aerospike Database namespace, e.g., `-n foo`.
	    If no value is specified, all namespaces for the given instance are
	    backed up or restored. Multiple namespaces may be specified as a
	    comma-separated list, e.g., `-n foo,bar,test`.

`-p`	specify the path to which the Aerospike Database primary index should
	    be backed up, or the path from which the Aerospike Database primary
	    index should be restored, e.g., `-p backup/asd`.

`-r`	perform a restore operation, to copy Aerospike Database's primary
	    index from files in the file system to shared memory. May be combined
	    with `-a` to check whether a restore may be performed without performing
	    the restore.

`-t`	specify the maximum number of threads to use for performing I/O needed
	    for a back up or restore operation. Normally, ASMT creates one I/O thread
	    per CPU core, but this may be decreased by the user. As there would be
	    no benefit to increasing this number past the number of CPU cores, to
	    attempt to do so has no effect.

`-v`	specifies whether ASMT should produce verbose output. Recommended.

`-z`	compress primary index on backup. This can result in files that are
	    15-30% smaller and 15-30% quicker to write, at the cost of a small
    	amount of computation. There is no need to specify this option when
	    restoring a primary index that was backed up using this option. (Files
	    compressed by back up are automatically decompressed by restore.)

**Note:** ASMT must be run with the same user and group that was used to run the
Aerospike database server. If you ran the Aerospike database server as user
root, group root, you must run ASMT as user root, group root. The sudo command
can facilitate this.


### Common Errors

ASMT cannot be used while Aerospike Database is running. If an attempt is
made to back up the primary index while Aerospike Database is running, ASMT
will complain and fail. In that case, wait for Aerospike Database to shut
down cleanly and remove any files created by ASMT before running ASMT.

If Aerospike Database is restarted before ASMT has completed restoring the
primary index, Aerospike Database will fail as it attempts to delete shared
memory segments. In that case, remove any shared memory segments created
by ASMT and retry the restore operation.

**Note:** ASMT must be run with the same user and group that was used to run the
Aerospike database server. If you ran the Aerospike database server as user
root, group root, you must run ASMT as user root, group root. The sudo command
can facilitate this.

The following command may be used to delete Aerospike Database files that
were backed up to the file system:

```
rm -rf pathname
```

where: pathname is the path name specified with the `-p` option to ASMT.

**Note:** The above command is somewhat **dangerous** as it will delete all files
in the pathname directory. If you stored files in addition to Aerospike
Database primary index files in that location, this command will delete
them, too!

The following command may be used to delete Aerospike Database shared memory
segments:

```
ipcs | grep ^0xae | awk '{print $1}' | xargs -i ipcrm -M {}
```

## License

ASMT is supplied under an [Apache 2.0 license](LICENSE).

