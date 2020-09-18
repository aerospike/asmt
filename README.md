# Aerospike Shared Memory Tool (ASMT)

This project is maintained by [Aerospike](http://www.aerospike.com)

### Overview
------------

ASMT provides a program for backing up or restoring an Aerospike Database's
primary index.

After an Aerospike Database server node is shut down cleanly, ASMT can be used
to save the server node's primary index from shared memory to file. The host
machine may then be rebooted, after which ASMT can be used to restore the index
from file to shared memory. This in turn enables the Aerospike Database server
node to fast restart, even though it was rebooted.

Note - ASMT cannot be used while the Aerospike Database server is running.

#### What ASMT Does
-------------------

TBD.

### Getting Started
--------------------

**Download the ASMT package through git:**

```
$ git clone https://github.com/aerospike/asmt.git
```
This creates an /asmt directory.

Alternately you can download the ZIP or TAR file from the links at the left.
When you unpack/untar the file, it creates an /aerospike-asmt-<version>
directory.

**Install the Required Libraries**

Before you can build ASMT, you need to install some libraries.

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
$ cd asmt    OR    cd /aerospike-asmt-<version>
$ make
```

This will create a binary in a target/bin directory: target/bin/asmt

#### Using ASMT
---------------

Copy (as executable) the asmt binary to wherever is most convenient on the
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

This example usage will backup all relevant namespaces' indexes for server
instance 0.

To backup a different server instance, use -i to specify the instance index.

To backup a specific namespace only, use -n to specify the namespace name.

To compress the files while backing them up, use the -z option.

For other backup options, use -h or see the list below.

When the backup is complete, there will be files in the specified directory
corresponding to all the relevant shared memory blocks. The file names are the
same as the shared memory keys. Uncompressed file names end in '.dat' while
compressed files end in '.dat.gz'. Note that the base file for a namespace
is never compressed as ASMT must examine its contents prior to restoring any
files for its namespace.

Note - if files of the relevant names already exist in the directory, the backup
will not start, i.e., it will not overwrite the files.

If the backup was successful, the host machine may then be rebooted. The index
shared memory blocks are lost, but the files will allow them to be restored
after reboot.

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

There is no need to specify the -z option when restoring, even if the files
are compressed (i.e., they were created using the -z option).

For other restore options, use -h or see the list below.

#### ASMT Options
-----------------

TBD (for now, use -h).
