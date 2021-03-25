# Aerospike Shared Memory Tool (ASMT)

### Overview
------------

ASMT provides a program for backing up and restoring the primary index
of an Aerospike Database Enterpise Edition (EE).

After an Aerospike Database server node has been shut down cleanly, ASMT may
be used to save the server node's primary index from shared memory to files in
the file system. The server node (host machine) may then be rebooted, after
which ASMT can be used to restore the index from the file system to shared
memory. This in turn enables the Aerospike Database server to fast restart,
even though it was rebooted.

**Note** ASMT cannot be used while the Aerospike Database server is running.

#### License
------------

ASMT is supplied under an [Apache 2.0 license](LICENSE-2.0.txt).

#### What ASMT Does
-------------------

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

### Getting Started
--------------------

**Download the ASMT package using git:**

```
$ git clone https://github.com/aerospike/asmt.git
```
This creates an /asmt directory.

Alternatively, you can download the ZIP or TAR file from the git repository.
When you unpack/untar the file, it creates an /aerospike-asmt-\<version\>
directory.

**Install the Required Libraries**

Before you can build ASMT, you must install certain libraries.

For CentOS:
```
$ sudo yum install make gcc zlib-devel
```

For Debian or Ubuntu:
```
$ sudo apt-get install make gcc libc6-dev zlib1g-dev
```

**Build the package.**

```
$ cd asmt
$ make
```

This will create a binary in a target/bin directory: target/bin/asmt

#### Using ASMT
---------------

Copy the asmt binary (as an executable) to wherever is most convenient on the
relevant Aerospike Database server node, and change to that directory.

After checking that the server node is not running, run the asmt binary to
backup the primary index, for example:

```
$ sudo ./asmt -b -v -p index-backup
```

where:

```
 -b - backup (as opposed to restore - must specify one or the other)
 -v - verbose output (optional but recommended)
 -p <backup path> - mandatory directory path specifying where your index backup
files will be saved
```

The above example will backup all relevant namespaces' indexes for server
instance 0, the most common usage.

To backup a different server instance, use '-i' to specify the instance index
(see below).

To backup a specific namespace only, use '-n' to specify the namespace name
(see below). Note: A comma-separated list of namespace names may be supplied,
e.g., '-n foo,bar,test'.

To compress the files while backing them up, use the '-z' option.

Note: ASMT must be run as uid 0 and gid 0. The sudo command allows this.

For other backup options, use '-h' or see the list below.

When the backup is complete, there will be files in the specified directory
corresponding to all the relevant shared memory blocks. The file names are the
same as the shared memory keys. Uncompressed file names end in '.dat' while
compressed files end in '.dat.gz'. Note that the base file for a namespace
is never compressed as ASMT must examine its contents prior to restoring any
files for its namespace.

Note that if files with the relevant names already exist in the directory,
the backup will not start, i.e., it will not overwrite existing files.

If the backup was successful, the host machine may then be rebooted. The index
shared memory blocks are lost, but ASMT will enable the primary index to be
restored after reboot.

After reboot, but before restarting the Aerospike Database server, run the asmt
binary to restore the primary index, for example:

```
$ sudo ./asmt -r -v -p index-backup
```

where:

```
 -r - restore (as opposed to backup - must specify one or the other)
 -v - verbose output (optional but recommended)
 -p <backup path> - mandatory directory path specifying where your index backup
files have been saved
```

This example usage will restore all saved namespaces' indexes for server
instance 0.

To restore a different server instance, use -i to specify the instance index.

To restore a specific namespace only, use -n to specify the namespace name.
(Note: A comma-separated list of namespace names may be supplied, e.g.,
'-n foo,bar,test'.)

There is no need to specify the -z option when restoring, even if the files
are compressed (i.e., they were created using the -z option).

For other restore options, use -h or see the list below.

#### ASMT Options
-----------------

```
usage: asmt [-a] [-b] [-c] [-h] [-i <instance>] [-n <name>[,<name>...]]
            [-p <pathdir>] [-r] [-t <threads>] [-v] [-z]

-a analyze (advisory - goes with '-b' or '-r')
-b backup (operation or advisory with '-a')
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

-a	used to analyze whether a backup or restore can be performed without
	actually performing the backup or restore.

-b	perform a backup operation, to copy Aerospike Database's primary index
	from shared memory to files in the file system. May be combined with
	'-a' to check whether a backup may be performed without performing the
	backup.

-c	compute a standard 32-bit cyclic redundancy check (CRC-32) to ensure
	that the contents of the primary index were not corrupted during a
	backup operation, during a restore operation, or while resident in
	the file system. Without the '-z' option (see below), computing the
	CRC-32 may be computationally expensive. When used with '-z', the cost
	is much lower, and may be considered negligible.

-h	show information on how to use ASMT.

-i	select a particular Aerospike Database instance, e.g., "-i 1". The
	default instance is 0.

-n	select a particular Aerospike Database namespace, e.g., "-n foo".
	If no value is specified, all namespaces for the given instance are
	backed up or restored. Multiple namespaces may be specified as a
	comma-separated list, e.g., "-n foo,bar,test".

-p	specify the path to which the Aerospike Database primary index should
	be backed up, or the path from which the Aerospike Database primary
	index should be restored, e.g., "-p backup/asd".

-r	perform a restore operation, to copy Aerospike Database's primary
	index from files in the file system to shared memory. May be combined
	with '-a' to check whether a restore may be performed without performing
	the restore.

-t	specify the maximum number of threads to use for performing I/O needed
	for a backup or restore operation. Normally, ASMT creates one I/O thread
	per CPU core, but this may be decreased by the user. As there would be
	no benefit to increasing this number past the number of CPU cores, to
	attempt to do so has no effect.

-v	specifies whether ASMT should produce verbose output. Recommended.

-z	compress primary index on backup. This can result in files that are
	15-30% smaller and 15-30% quicker to write, at the cost of a small
	amount of computation. There is no need to specify this option when
	restoring a primary index that was backed up using this option. (Files
	compressed by backup are automatically decompressed by restore.)

#### Common Errors
------------------

ASMT cannot be used while Aerospike Database is running. If an attempt is
made to backup the primary index while Aerospike Database is running, ASMT
will complain and fail. In that case, wait for Aerospike Database to shut
down cleanly and remove any files created by ASMT before running ASMT.

If Aerospike Database is restarted before ASMT has completed restoring the
primary index, Aerospike Database will fail as it attempts to delete shared
memory segments. In that case, remove any shared memory segments created
by ASMT and retry the restore operation.

The following command may be used to delete Aerospike Database files that
were backed up to the file system:

```
sudo rm -rf pathname
```

where: pathname is the path name specified with the '-p' option to ASMT.

Note: The above command is somewhat **dangerous** as it will delete all files
in the pathname directory. If you stored files in addition to Aerospike
Database primary index files in that location, this command will delete
them, too!

The following command may be used to delete Aerospike Database shared memory
segments:

```
ipcs | grep ^0xae | awk '{print $1}' | xargs -i sudo ipcrm -M {}
```
