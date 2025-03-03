/*
 * asmt.c
 *
 * Copyright (C) 2022-2024 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * This program is free software: you can redistribute it and/or modify it under
 * the terms of the GNU Affero General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses/
 */

//==========================================================
// Includes.
//
#include <assert.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <libgen.h>
#include <limits.h>
#include <pwd.h>
#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <zlib.h>

#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "hardware.h"
#include "warnings.h"

//=========================================================
// Nomenclature.
//
// Segment pointers:
//		sp		segment pointer
// 		pbsp	primary base segment pointer
// 		ptsp	primary treex segment pointer
// 		pssps	primary stage segment pointers
// 		n_pssps	number of primary stage segment pointers
// 		smsp	secondary meta segment pointer
// 		sssps	secondary stage segment pointers
// 		n_sssps	number of secondary stage segment pointers
// 		dsps	data segment pointers
// 		n_dsps	number of data segment pointers
// 		pbsp	primary base segment pointer
//
//		fp		file pointer
// 		pbfp	primary base file pointer
// 		ptfp	primary treex file pointer
// 		psfps	primary stage file pointers
// 		n_psfps	number of primary stage file pointers
// 		smfp	secondary meta file pointer
// 		ssfps	secondary stage file pointers
// 		n_ssfps	number of secondary stage file pointers
// 		dfps	data file pointers
// 		n_dfps	number of data file pointers

//==========================================================
// Typedefs & constants.
//

// Types of segments / segment files.

typedef enum {
	TYPE_BASE, TYPE_TREEX, TYPE_META, TYPE_PRI_STAGE,
	TYPE_SEC_STAGE, TYPE_DAT_STAGE,
} as_type;

// Information about a segment.

typedef struct as_segment_s {
	key_t key;
	int shmid;
	uid_t uid;
	gid_t gid;
	unsigned int mode;
	shmatt_t natt;
	size_t segsz;
	uint32_t stage;
	uint32_t inst;
	uint32_t nsid;
	char* nsnm;
	as_type type;
	uLong crc32;
} as_segment_t;

// Information about a segment file.

typedef struct as_file_s {
	key_t key;
	uid_t uid;
	gid_t gid;
	unsigned int mode;
	size_t filsz;
	size_t segsz;
	bool compress;
	uint32_t stage;
	uint32_t inst;
	uint32_t nsid;
	char* nsnm;
	as_type type;
} as_file_t;

// Information about a file I/O.

typedef struct as_io_s {
	key_t key;
	int fd;
	bool write;
	void* memptr;
	size_t filsz;
	size_t segsz;
	bool compress;
	uLong crc32;
	int shmid;
	uid_t uid;
	gid_t gid;
	mode_t mode;
} as_io_t;

// Information about a compressed file.

typedef struct as_cmp_s {
	uint32_t magic;
	uint32_t version;
	size_t segsz;
	uLong crc32;
} __attribute__((packed)) as_cmp_t;

//==========================================================
// Globals.
//

// Constant globals.

static const char g_fullname[] = "Aerospike Shared Memory Tool";
static const char g_version[] = "Version 2.1.4";
static const char g_copyright[] = "Copyright (C) 2022-2024 Aerospike, Inc.";
static const char g_all_rights[] = "All rights reserved.";

static const char* FILE_EXTENSION = ".dat";
static const char* FILE_EXTENSION_CMP = ".dat.gz";

static const key_t AS_XMEM_KEY_TYPE_MASK = (key_t)0xFF000000;
static const key_t AS_XMEM_PRI_KEY = (key_t)0xAE000000;
static const key_t AS_XMEM_SEC_KEY = (key_t)0xA2000000;
static const key_t AS_XMEM_DAT_KEY = (key_t)0xAD000000;
static const key_t AS_XMEM_TREEX_KEY = (key_t)0x00000001;
static const key_t AS_XMEM_ARENA_KEY = (key_t)0x00000100;

static const int AS_XMEM_INSTANCE_KEY_SHIFT = 20;
static const int AS_XMEM_NS_KEY_SHIFT = 12;

static const unsigned int DEFAULT_MODE = 0600;
static const unsigned int DEFAULT_MODE_DIR = 0700;
static const unsigned int MODE_MASK = 0x1ff;

static const int SHMGET_FLAGS_CREATE_ONLY = IPC_CREAT | IPC_EXCL | 0666;

// Instead of #defines: Advantage? Use the symbol table, so easier debugging.

// For string formatting.
enum {
	MAX_BUFFER = 1024
};

// For screen formatting.
enum {
	NUM_BLANKS = 2
};


// Minimum acceptable value.
enum {
	MIN_THREADS = 1
};

// Maximum acceptable value.
enum {
	MAX_THREADS = 1024
};

// Any unacceptable value.
enum {
	INV_THREADS = 65535
};

// Minimum acceptable value.
enum {
	MIN_INST = 0
};

// Maximum acceptable value.
enum {
	MAX_INST = 15
};

// Any unacceptable value.
enum {
	INV_INST = 65535
};

// Minimum acceptable value.
enum {
	MIN_NSID = 1
};

// Maximum acceptable value.
enum {
	MAX_NSID = 32
};

// Minimum acceptable value.
enum {
	MIN_ARENA = 0x100
};

// Maximum acceptable value.
enum {
	MAX_ARENA = 0x8FF
};

// Any unacceptable value.
enum {
	INV_ARENA = 0xffff
};

// Offset of version in base segment.
enum {
	BASEVER_OFF = 0
};

// Size of version in base segment.
enum {
	BASEVER_LEN = sizeof(uint32_t)
};

// Minimum acceptable version of base segment,
enum {
	BASEVER_MIN = 10
};

// Maximum acceptable version of base segment.
enum {
	BASEVER_MAX = 12
};

// Shutdown status offset in base segment.
enum {
	BASESHUT_OFF = sizeof(uint32_t)
};

// Shutdown status length in base segment.
enum {
	BASESHUT_LEN = sizeof(uint32_t)
};

// Offset of namespace in base segment.
enum {
	BASE_NAMESPACE_OFF = 1024
};

// Offset of namespace in data segments.
enum {
	DATA_NAMESPACE_OFF = 12
};

// Length of namespace name in all segments.
enum {
	NAMESPACE_LEN = 32
};

// Offset of n_arenas in base segment.
enum {
	N_ARENAS_PRI_OFF = 2152
};

// Offset of n_arenas in meta segment.
enum {
	N_ARENAS_SEC_OFF = 20
};

// Length of n_arenas field.
enum {
	N_ARENAS_LEN = sizeof(uint32_t)
};

// Offset of header in compressed file.
enum {
	CMPHDR_OFF = 0
};

// Length of header in compressed file.
enum {
	CMPHDR_LEN = sizeof(as_cmp_t)
};

// asmt magic number ('TMSA' in ASCII).
enum {
	CMPHDR_MAG1 = 0X41534D54
};

// asmt magic number ('ASMT' in ASCII).
enum {
	CMPHDR_MAG2 = 0X544D5341
};

// asmt header current version
enum {
	CMPHDR_VER = 1
};

// Compression chunk size.
enum {
	CMPCHUNK = 1048576
};

// Compression start chunk size
enum {
	CMPCHUNK_START = 1024
};

// Maximum number of primary stages.
enum {
	MAX_PRI_STAGES = 2048
};

// Maximum number of secondary stages.
enum {
	MAX_SEC_STAGES = 2048
};

enum {
	MAX_DATA_STAGES = 128
};

// General globals.

static char* g_pathdir = NULL;
static char* g_progname = NULL;
static char* g_nsnm = NULL;
static char* g_nsnm_base = NULL;
static char** g_nsnm_array = NULL;
static uint32_t g_nsnm_count = 0;
static uint32_t g_inst = 0; // Default is instance 0.
static bool g_analyze = false;
static bool g_backup = false;
static bool g_compress = false;
static bool g_crc32 = false;
static bool g_restore = false;
static bool g_verbose = false;
static uint32_t g_max_threads = INV_THREADS; // Default is num_cpus().
static uLong g_crc32_init;

// File I/O related globals.

static as_io_t* g_ios;
static bool g_ios_ok;
static pthread_mutex_t g_io_mutex;
static uint32_t g_n_ios;
static uint32_t g_next_io;
static uint64_t g_total_to_transfer;
static uint64_t g_total_transferred;
static uint32_t g_decile_transferred;
static struct timespec g_io_start_time;

//==========================================================
// Forward declarations.
//

static void usage(bool verbose);
static void print_newline_and_blanks(size_t n_blanks);
static int init_nsnm_list(void);
static void exit_nsnm_list(void);
static bool analyze(void);
static bool analyze_backup(void);
static bool check_dir(const char* pathname, bool is_write, bool create);
static bool list_segments(as_segment_t** segments, uint32_t* n_segments,
		int* error);
static bool stat_segment(int shmid, as_segment_t** segment, int* error);
static int qsort_compare_segments(const void* left, const void* right);
static bool analyze_backup_candidate(as_segment_t* segments,
		uint32_t n_segments, uint32_t base_ix);
static void display_segments(as_segment_t* pbsp, as_segment_t* ptsp,
		as_segment_t pssps[], uint32_t n_pssps, as_segment_t* smsp,
		as_segment_t sssps[], uint32_t n_sssps,
		as_segment_t dsps[], uint32_t n_dsps);
static char *get_segment_nsnm(as_segment_t* sp);
static bool is_file_compressed(key_t key);
static char *get_file_nsnm(key_t key);
static char *get_file_nsnm_compressed(key_t key);
static bool analyze_backup_sanity(as_segment_t* pbsp, as_segment_t* ptsp,
		as_segment_t pssps[], uint32_t n_pssps, as_segment_t* smsp,
		as_segment_t sssps[], uint32_t n_sssps,
		as_segment_t dsps[], uint32_t n_dsps);
static bool backup_candidate(as_segment_t* pbsp, as_segment_t* ptsp,
		as_segment_t pssps[], uint32_t n_pssps, as_segment_t* smsp,
		as_segment_t sssps[], uint32_t n_sssps, as_segment_t dsps[], uint32_t n_dsps);
static bool backup_candidate_file(as_segment_t* sp, as_io_t* io, as_io_t ios[],
		as_segment_t* pbsp, as_segment_t* ptsp, as_segment_t pssps[],
		uint32_t n_pssps, as_segment_t* smsp, as_segment_t sssps[],
		uint32_t n_sssps, as_segment_t dsps[], uint32_t n_dsps);
static bool backup_candidate_check_crc32(as_io_t ios[], as_segment_t* pbsp,
		as_segment_t* ptsp, as_segment_t pssps[], uint32_t n_pssps,
		as_segment_t* smsp, as_segment_t sssps[], uint32_t n_sssps,
		as_segment_t dsps[], uint32_t n_dsps);
static void backup_candidate_cleanup(as_io_t ios[], as_segment_t* pbsp,
		as_segment_t* ptsp, as_segment_t pssps[], uint32_t n_pssps,
		as_segment_t* smsp, as_segment_t sssps[], uint32_t n_sssps,
		as_segment_t dsps[], uint32_t n_dsps,
		bool remove_files);
static bool start_io(as_io_t ios[], uint32_t n_ios);
static void* run_io(void* args);
static bool write_file(int fd, const void* buf, size_t segsz, mode_t mode,
		uid_t uid, gid_t gid, bool compress, uLong* crc);
static bool pwrite_file(int fd, const void* buf, size_t segsz, mode_t mode,
		uid_t uid, gid_t gid, uLong* crc);
static bool zwrite_file(int fd, const void* buf, size_t segsz, mode_t mode,
		uid_t uid, gid_t gid, uLong* crc);
static bool read_file(int fd, void* buf, size_t filsz, size_t segsz, int shmid,
		mode_t mode, uid_t uid, gid_t gid, bool compress, uLong* crc);
static bool pread_file(int fd, void* buf, size_t segsz, int shmid, mode_t mode,
		uid_t uid, gid_t gid, uLong* crc);
static bool zread_file(int fd, void* buf, size_t filsz, size_t segsz, int shmid,
		mode_t mode, uid_t uid, gid_t gid, uLong* crc);
static bool analyze_restore(void);
static bool analyze_restore_candidate(as_file_t* fps, uint32_t n_fps,
		uint32_t base_ix);
static bool analyze_restore_sanity(as_file_t* pbfp, as_file_t* ptfp,
		as_file_t psfps[], uint32_t n_psfps, as_file_t* smfp,
		as_file_t ssfps[], uint32_t n_ssfps, as_file_t dfps[], uint32_t n_dfps);
static void display_files(as_file_t* pbfp, as_file_t* ptfp, as_file_t psfps[],
		uint32_t n_psfps, as_file_t* smfp, as_file_t ssfps[], uint32_t n_ssfps,
		as_file_t dfps[], uint32_t n_dfps);
static bool restore_candidate(as_file_t* pbfp, as_file_t* ptfp, as_file_t psfps[],
		uint32_t n_psfps, as_file_t* smfp, as_file_t ssfps[], uint32_t n_ssfps,
		as_file_t dfps[], uint32_t n_dfps);
static void restore_candidate_cleanup(as_io_t ios[], uint32_t n_ios,
		bool remove_segments);
static bool restore_candidate_segment(as_file_t *fp, as_io_t *io, as_io_t ios[],
		uint32_t n_ios, as_file_t *pbfp, as_file_t *ptfp, as_file_t psfps[],
		uint32_t n_psfps, as_file_t *smfp, as_file_t ssfps[], uint32_t n_ssfps,
		as_file_t dfps[], uint32_t n_dfps);
static bool restore_candidate_check_crc32(as_io_t ios[], uint32_t n_ios);
static bool validate_file_name(const char* pathname, as_file_t* fp);
static bool list_files(as_file_t** fps, uint32_t* n_fps, int* error);
static int qsort_compare_files(const void* left, const void* right);
static int qsort_compare_segments(const void* left, const void* right);
static void draw_table(char** table, uint32_t n_rows, uint32_t n_cols);
static char* strfmt_width(char* string, uint32_t width, uint32_t n_blanks,
		bool dashes);
static char* strtime_diff_eta(struct timespec* start, struct timespec* end,
		uint32_t decile);
static void gettime_hmst(struct timespec* time, time_t* hours, time_t* minutes,
		time_t* seconds, time_t* tenths);

//==========================================================
// Aerospike shared memory tool entry point.
//

int
main(int argc, char* argv[])
{
	// Save the basename of the first argument as the program name.

	g_progname = basename(argv[0]);

	// Scan through command line options.

	int opt;

	while ((opt = getopt(argc, argv, "abchi:n:p:rt:vz")) != -1) {
		switch (opt) {

		case 'a':
			// Perform analyze operation.
			g_analyze = true;
			break;

		case 'b':
			// Perform backup operation (or advisory for analyze operation).
			g_backup = true;
			break;

		case 'c':
			// Compare crc32 values.
			g_crc32 = true;
			break;

		case 'h':
			// Provide usage information to user.
			usage(true);
			exit(EXIT_SUCCESS);
			break;

		case 'i':
			// Filter by instance number (default is 0).
			g_inst = (uint32_t)atoi(optarg);
			break;

		case 'n':
			// Filter by namespace name (default is any).
			g_nsnm = optarg;
			break;

		case 'p':
			// Set path directory for segment files (no default).
			g_pathdir = optarg;
			break;

		case 'r':
			// Perform restore operation (or advisory for analyze operation).
			g_restore = true;
			break;

		case 't':
			// Set the maximum number of threads for backup/restore I/Os.
			// Default is number of CPUs.
			g_max_threads = (uint32_t)atoi(optarg);
			break;

		case 'v':
			// Request verbose output.
			g_verbose = true;
			break;

		case 'z':
			// Request compressed backup.
			g_compress = true;
			break;

		default:
			// Unknown command line option.
			usage(true);
			exit(EXIT_FAILURE);
			break;
		}
	}

	// If there are arguments past the command line options, bark.

	if (optind < argc) {
		usage(true);
		exit(EXIT_FAILURE);
	}

	// Did user specify exactly one command to perform?

	if ((!g_backup && !g_restore) || (g_backup && g_restore)) {
		printf("Must specify exactly one of backup ('-b')"
				" or restore ('-r').\n\n");
		usage(false);
		exit(EXIT_FAILURE);
	}

	// User must specify the path of the directory containing (or to contain)
	// Aerospike database segment files.

	if (g_pathdir == NULL) {
		printf("Must specify pathname of file directory (use '-p').\n\n");
		usage(false);
		exit(EXIT_FAILURE);
	}

	// Don't need to specify compress with restore.

	if (g_restore && g_compress) {
		printf("Unnecessary to specify compress ('-z') with restore ('-r').\n\n");
	}

	// Can't specify an instance number outside the valid range.
	// Note: Instance can be 0.

	if (g_inst != INV_INST && g_inst > MAX_INST) {
		printf("Instance must be from %d..%d (use '-i').\n\n", MIN_INST,
				MAX_INST);
		usage(false);
		exit(EXIT_FAILURE);
	}

	// Determine maximum number of threads--use num_cpus() if not specified.

	if (g_max_threads == INV_THREADS) {
		g_max_threads = num_cpus();
	}
	else if (g_max_threads < MIN_THREADS || g_max_threads > MAX_THREADS) {
		printf("Max threads must be in the range %d..%d (use '-t').\n\n",
				MIN_THREADS, MAX_THREADS);
		usage(false);
		exit(EXIT_FAILURE);
	}

	// If we haven't printed usage (and verbose), print copyright info.

	if (g_verbose) {
		printf("%s, %s", g_fullname, g_version);
		printf("\n");
		printf("%s  %s\n", g_copyright, g_all_rights);
		printf("\n");
	}

	// Print command as issued.

	if (g_verbose) {
		printf("%s", g_progname);
		for (int i = 1; i < argc; i++) {
			printf(" %s", argv[i]);
		}
		printf("\n");
	}

	// Decide which operation to perform.

	if (g_verbose) {
		printf("\n");
		if (g_analyze) {
			printf("Performing analyze operation");
			if (g_backup) {
				printf(" with backup option");
			}
			else {
				printf(" with restore option");
			}
			printf(".\n");
		}
		else if (g_backup) {
			printf("Performing backup operation");
			if (g_crc32 && !g_compress) {
				printf(" with crc32 checking");
			}
			else if (g_compress && !g_crc32) {
				printf(" with compression");
			}
			else if (g_compress && g_crc32) {
				printf(" with compression and crc32 checking");
			}
			printf(".\n");
		}
		else {
			printf("Performing restore operation");
			if (g_crc32) {
				printf(" with crc32 checking");
			}
			printf(".\n");
		}
	}

	// Initialize the CRC32 initialization constant.

	g_crc32_init = g_crc32 ? crc32(0L, Z_NULL, 0) : 0;

	// Get the list of namespace names over which to operate.

	int ret = init_nsnm_list();

	if (ret <= 0) {
		printf("Failed to extract namespace names from list.\n");
		exit(EXIT_FAILURE);
	}

	// Operate over each namespace name provided (if any).

	bool success;

	if (g_nsnm_count == 0) {
		// No namespace name provided.
		printf("Did not provide list of namespace names.\n");
		exit_nsnm_list();

		exit(EXIT_FAILURE);
	}
	else {
		// List of namespace names provided.
		success = true;
		uint32_t nsnm_count = 0;

		for (uint32_t i = 0; i < g_nsnm_count; i++) {
			g_nsnm = g_nsnm_array[i];

			if (strcmp(g_nsnm, "") == 0) {
				continue;
			}

			nsnm_count++;

			if (!analyze()) {
				success = false;
			}
		}

		if (success && nsnm_count != g_nsnm_count) {
			printf("\nInvalid namespace name(s) provided.\n");
		}
	}

	exit_nsnm_list();

	exit(success ? EXIT_SUCCESS : EXIT_FAILURE);
}

//==========================================================
// Local helpers.
//

// Print usage information.

static void
usage(bool verbose)
{
	printf("%s, %s", g_fullname, g_version);
	printf("\n");
	printf("%s  %s\n", g_copyright, g_all_rights);
	printf("\n");

	char first_str[MAX_BUFFER];

	sprintf(first_str, "usage: %s", g_progname);

	size_t first_len = strlen(first_str);

	printf("%s", first_str);

	printf(" [-a]");
	printf(" [-b]");
	printf(" [-c]");
	printf(" [-h]");
	printf(" [-i <instance>]");
	printf(" -n <name>[,<name>...]");

	print_newline_and_blanks(first_len);

	printf(" -p <pathdir>");
	printf(" [-r]");
	printf(" [-t <threads>]");
	printf(" [-v]");
	printf(" [-z]");

	printf("\n\n");

	printf("-a analyze (advisory - goes with '-b' or '-r')\n");
	printf("-b backup (operation or advisory with '-a')\n");
	printf("-c compare crc32 values of segments and segment files\n");
	printf("-h help\n");
	printf("-i filter by instance (default is instance 0)\n");
	printf("-n filter by namespace name\n");
	printf("-p path of directory (mandatory)\n");
	printf("-r restore (operation or advisory with '-a')\n");
	printf("-t maximum number of threads for I/O (default is #CPUs,"
			" in this case %u)\n", num_cpus());
	printf("-v verbose output\n");
	printf("-z compress files on backup\n");

	printf("\n");

	printf("Notes:\n");

	printf("\n");

	printf("1. The '-c' option has a significant performance cost.\n");
	printf("2. However, this is reduced when combined with the '-z' option.\n");
	printf("3. Should be run in verbose mode ('-v') if possible.\n");
	printf("4. A comma-separated list of namespace names may be provided.\n");

	if (!verbose) {
		return;
	}

	printf("\n");

	printf("Possible primary option combinations:\n");

	printf("\n");

	printf("-b     Perform backup operation ('-p' required).\n");
	printf("-r     Perform restore operation ('-p' required).\n");
	printf("-ba    Analyze backup operation ('-p' required).\n");
	printf("-ra    Analyze restore operation ('-p' required).\n");

	printf("\n");

	printf("Examples:\n");

	printf("\n");

	char buffer[MAX_BUFFER];

	sprintf(buffer, "%s -b -p /home/aerospike/backups -n foo", g_progname);
	printf("%s\n", buffer);

	printf("\n");

	printf("    Backs up all Aerospike database segments with instance 0\n");
	printf("    and namespace \'foo\' to the directory /home/aerospike/backups.\n");

	printf("\n");

	sprintf(buffer, "%s -b -p /home/aerospike/backups -zc -n bar", g_progname);
	printf("%s\n", buffer);

	printf("\n");

	printf("    Backs up all Aerospike database segments with instance 0\n");
	printf("    and namespace \'bar\' to the directory /home/aerospike/backups.\n");
	printf("    Requests that file compression be applied and crc32 checks\n");
	printf("    be made on all backups.\n");

	printf("\n");

	sprintf(buffer, "%s -ba -i2 -p /home/aerospike/backups -v -n test", g_progname);
	printf("%s\n", buffer);

	printf("\n");

	printf("    Analyzes whether any Aerospike database segments with\n");
	printf("    instance 2 and namespace \'test\' can be backed up to the directory\n");
	printf("    /home/aerospike/backups. Requests verbose output.\n");

	printf("\n");

	sprintf(buffer, "%s -r -i3 -n bar -p /home/aerospike/backups -cv -t 128",
			g_progname);
	printf("%s\n", buffer);

	printf("\n");

	printf("    Restores all Aerospike database segment files with instance 3\n");
	printf("    and namespace \'bar\' from the directory /home/aerospike/backups.\n");
	printf("    Requests that crc32 checks be made on all restorations.\n");
	printf("    Requests verbose output. Uses no more than 128 threads\n");
	printf("    for file I/O. Any compressed files will be decompressed.\n");

	printf("\n");
}

// Print a newline followed by a number of blanks.

static void
print_newline_and_blanks(size_t n_blanks)
{
	char buffer[MAX_BUFFER];

	memset(buffer, ' ', n_blanks);
	buffer[n_blanks] = '\0';
	printf("\n%s", buffer);
}

static int
init_nsnm_list(void)
{
	assert(g_nsnm_base == NULL);
	assert(g_nsnm_array == NULL);
	assert(g_nsnm_count == 0);

	if (g_nsnm == NULL) {
		return 0;
	}

	char* list = strdup(g_nsnm);

	if (list == NULL) {
		return 0;
	}

	// Save the original namespace name list.

	g_nsnm_base = g_nsnm;

	// Extract namespace names from the list.

	char* tmp_list = list;

	while (tmp_list != NULL) {

		// Find next element in list.

		char* tmp_elmt = strchr(tmp_list, ',');

		if (tmp_elmt != NULL) {
			*tmp_elmt = '\0';
		}

		// Add element to array.

		g_nsnm_count++;

		char** new_array = (char**)realloc(g_nsnm_array,
				g_nsnm_count * sizeof(char*));
		assert(new_array != NULL);
		g_nsnm_array = new_array;

		g_nsnm_array[g_nsnm_count - 1] = strdup(tmp_list);

		// Go to next element (if any).

		tmp_list = tmp_elmt == NULL ? NULL : ++tmp_elmt;
	}

	free(list);
	g_nsnm = NULL;

	return (int)g_nsnm_count;
}

static void
exit_nsnm_list(void)
{
	if (g_nsnm_array == NULL) {
		return;
	}

	for (uint32_t i = 0; i < g_nsnm_count; i++) {
		assert(g_nsnm_array[i] != NULL);
		free(g_nsnm_array[i]);
		g_nsnm_array[i] = NULL;
	}

	free(g_nsnm_array);

	g_nsnm_array = NULL;
	g_nsnm = g_nsnm_base;
	g_nsnm_base = NULL;
}

// Analyze (and perform?) which operations (backup/restore) can be performed.

static bool
analyze(void)
{
	return g_backup ? analyze_backup() : analyze_restore();
}

// Analyze whether backup operations can be performed (and perform?).

static bool
analyze_backup(void)
{
	as_segment_t* segments;
	int error;

	// First, see if we can access the backup directory for writing.
	// Do not create if only analyzing.

	if (!check_dir(g_pathdir, true, !g_analyze)) {
		if (g_verbose) {
			printf("Cannot write to directory \'%s\'", g_pathdir);
			if (g_analyze) {
				printf(": either it does not exist,"
						" we don't have write permission,"
						" or we're running with \'-a\'.\n");
			} else {
				printf(": either it does not exist"
						" or we don't have write permission.\n");
			}
		}

		return false;
	}

	// Get the list of segments that passed the instance / namespace filter.

	uint32_t n_segments;

	if (!list_segments(&segments, &n_segments, &error) || n_segments == 0) {
		// Note: n_segments and error are valid even if list_segments() returned false.

		if (g_verbose) {
			printf("\nDid not find any suitable Aerospike database segments");
			printf(", instance %u", g_inst);

			if (g_nsnm != NULL) {
				printf(", namespace \'%s\'", g_nsnm);
			}

			if (error != 0) {
				char errbuff[MAX_BUFFER];
				char *errout = strerror_r(errno, errbuff, MAX_BUFFER);

				printf(": error was %d: %s", error, errout);
			}

			printf(".\n");
		}

		return false;
	}

	// Look for segments that can be backed up:
	//
	// Must have one base and treex segment and and one or more primary stage
	// segments. May have one meta segment and one or more secondary stage
	// segments. Will handle multiple namespaces if requested and no failures.

	bool candidates = false;

	for (uint32_t ix= 0; ix < n_segments; ix++) {
		as_segment_t* segment = &segments[ix];

		if (segment->type == TYPE_BASE) {
			candidates = true;

			if (!analyze_backup_candidate(segments, n_segments, ix)) {
				for (uint32_t jx = 0; jx < n_segments; jx++) {
					as_segment_t* sp = &segments[jx];

					if (sp->nsnm != NULL) {
// FIXME				free(sp->nsnm);
						sp->nsnm = NULL;
					}
				}

// FIXME		free(segments);
				segments = NULL;

				return false;
			}
		}
	}

	if (!candidates) {
	    // Look for orphaned data segments in the g_nsnm namespace.

	    as_segment_t dsps[MAX_DATA_STAGES] = { 0 };
	    uint32_t n_dsps = 0;

	    for (uint32_t ix = 0; ix < n_segments; ix++) {
			as_segment_t* sp = &segments[ix];

			if (sp->type != TYPE_DAT_STAGE ||
				sp->inst != g_inst) {
			    continue;
			}

			sp->nsnm = get_segment_nsnm(sp);

			if (strcmp(g_nsnm, sp->nsnm) != 0) {
				continue;
			}

			// Record this segment for backup.

			dsps[n_dsps++] = *sp;
	    }

	    if (n_dsps) {
			qsort((void*)dsps, (size_t)n_dsps, sizeof(as_segment_t),
					qsort_compare_segments);

			if (g_verbose) {
				printf("\n");
				display_segments(NULL, NULL, NULL, 0, NULL, NULL, 0, dsps, n_dsps);
				printf("\n");
			}

			if (!g_analyze) {
			    backup_candidate(NULL, NULL, NULL, 0, NULL, NULL, 0, dsps, n_dsps);
			}

			candidates = true;
	    }
	}

	if (!candidates) {
		if (g_verbose) {
			printf("\nDid not find any unattached Aerospike database segments");
			printf(", instance %u", g_inst);

			if (g_nsnm != NULL) {
				printf(", namespace \'%s\'", g_nsnm);
			}

			printf(".\n");
		}
	}

	for (uint32_t jx = 0; jx < n_segments; jx++) {
		as_segment_t* sp = &segments[jx];

		if (sp->nsnm != NULL) {
			//FIXME: free(sp->nsnm);
			sp->nsnm = NULL;
		}
	}
	free(segments);
	segments = NULL;

	return true;
}

// Check whether a directory exists and is accessible by us.
// Note: If create is set, we will try to create the directory.
// Note: We check permissions based on the write parameter,
// Note: If we create the directory, we do not unwind after failure.

static bool
check_dir(const char* pathname, bool is_write, bool create)
{
	struct stat statbuf;

	// Does the directory exit?

	if (stat(pathname, &statbuf) != 0) {
		// If not and we've been asked (create is true), can we create it?

		if (create) {
			if (mkdir(pathname, DEFAULT_MODE_DIR) != 0) {
				return false;
			}
			if (g_verbose) {
				printf("\nCreated backup directory \'%s\'.\n", pathname);
			}
		}
		else {
			return false;
		}

		// We created it. Try again to get status.

		if (stat(pathname, &statbuf) != 0) {
			return false;
		}
	}

	// Is it a directory? (Will be if we just created it!)

	if (!S_ISDIR(statbuf.st_mode)) {
		return false;
	}

	// Can we access it? Standard UNIX rules.

	uint32_t my_uid = getuid();
	uint32_t my_gid = getgid();

	if (is_write) {
		if (my_uid == 0 || my_gid == 0
				|| (statbuf.st_uid == my_uid && (statbuf.st_mode & S_IWUSR))
				|| (statbuf.st_gid == my_gid && (statbuf.st_mode & S_IWGRP))
				|| (statbuf.st_mode & S_IWOTH)) {
			return true;
		}
	}
	else {
		if (my_uid == 0 || my_gid == 0
				|| (statbuf.st_uid == my_uid && (statbuf.st_mode & S_IRUSR))
				|| (statbuf.st_gid == my_gid && (statbuf.st_mode & S_IRGRP))
				|| (statbuf.st_mode & S_IROTH)) {
			return true;
		}
	}

	// Can't access.

	return false;
}

// Get information about all Aerospike database segments.
// Note: *n_segments and *error are valid even if list_segments() returns false.

static bool
list_segments(as_segment_t** segments, uint32_t* n_segments, int* error)
{
	// Start with no segments.

	*n_segments = 0;
	*error = 0;

	// Get info on all shared memory segments..

	struct shmid_ds dummy; // Dummy, needed by shmctl(3).

	int rc = shmctl(0, SHM_INFO, &dummy);

	if (rc < 0) {
		*error = errno;
		return false;
	}

	int max_shmid = rc; // Range of shmids: (0..max_shmid) (inclusive).

	*segments = NULL; // Table is initially empty.

	// Try each shmid in the range. Some may correspond to Aerospike segments.

	for (int ix = 0; ix <= max_shmid; ix++) {
		as_segment_t* segment;

		// Get information about segment.

		if (!stat_segment(ix, &segment, error)) {
			continue;
		}

		// Check whether the segment is attached.

		if (segment->natt != 0) {
			if (segment->nsnm != NULL) {
				free(segment->nsnm);
				segment->nsnm = NULL;
			}

			free(segment);
			segment = NULL;
			continue;
		}

		// Check whether the instance is a match (if specified).

		if (g_inst != INV_INST && segment->inst != g_inst) {
			if (segment->nsnm != NULL) {
				free(segment->nsnm);
				segment->nsnm = NULL;
			}

			free(segment);
			segment = NULL;
			continue;
		}

		// Check whether the namespace name is a match.

		if (segment->type == TYPE_BASE && g_nsnm != NULL) {
			if (segment->nsnm == NULL) {
				free(segment);
				segment = NULL;
				continue;
			}
			else if (strcmp(segment->nsnm, g_nsnm) != 0) {
				free(segment->nsnm);
				segment->nsnm = NULL;
				free(segment);
				segment = NULL;
				continue;
			}
		}

		// Found a valid, unattached Aerospike database segment; append it to table.

		(*n_segments)++;

		*segments = realloc(*segments,
				(uint32_t)*n_segments * sizeof(as_segment_t));
		assert(*segments != NULL);

		memcpy(*segments + *n_segments - 1, segment, sizeof(as_segment_t));

		free(segment);
		segment = NULL;
		// Do not free segment->nsnm: It is still in use!
	}

	return true;
}

// Get information about a single shared memory segment by shmid.
// Validates whether a segment is an Aerospike database segment.

static bool
stat_segment(int shmid, as_segment_t** segment, int* error)
{
	// Get info on shared memory segment with ID of shmid.

	struct shmid_ds ds;

	int rc = shmctl(shmid, SHM_STAT, &ds);

	if (rc == -1) {
		*error = errno;
		return false;
	}

	// Extract key from shmid_ds structure.

	key_t key = ds.shm_perm.__key;

	// Check if this is an Aerospike primary, secondary, or data key.

	bool primary = false;
	bool secondary = false;

	switch(key & AS_XMEM_KEY_TYPE_MASK) {
		case AS_XMEM_PRI_KEY:
			primary = true;
			break;
		case AS_XMEM_SEC_KEY:
			secondary = true;
			break;
		case AS_XMEM_DAT_KEY:
			break;
		default:
			*error = EINVAL;
			return false;
			break;
	}

	// Found a valid Aerospike database segment; create segment entry.

	*segment = malloc(sizeof(as_segment_t));
	assert(*segment != NULL);

	as_segment_t* sp = *segment;

	// Populate segment info.

	sp->key = key;
	sp->shmid = rc;
	sp->uid = ds.shm_perm.uid;
	sp->gid = ds.shm_perm.gid;
	sp->mode = ds.shm_perm.mode;
	sp->natt = ds.shm_nattch;
	sp->segsz = ds.shm_segsz;

	// Extract the key base from the key.

	key = key & ~AS_XMEM_KEY_TYPE_MASK;

	// Determine instance from key base.

	sp->inst = (uint32_t)key >> AS_XMEM_INSTANCE_KEY_SHIFT;

	if (sp->inst > MAX_INST) {
		// Note: instance can be zero.
		free(*segment);
		*segment = NULL;
		*error = ENOENT;
		return false;
	}

	// Determine namespace ID from key base.

	key = key & ~(0xf << AS_XMEM_INSTANCE_KEY_SHIFT);

	sp->nsid = (uint32_t)(key & (0xff << AS_XMEM_NS_KEY_SHIFT))
			>> AS_XMEM_NS_KEY_SHIFT;

	if (sp->nsid < MIN_NSID || sp->nsid > MAX_NSID) {
		free(*segment);
		*segment = NULL;
		*error = ENOENT;
		return false;
	}

	// Extract segment type from key base.

	key = key & ~(0xff << AS_XMEM_NS_KEY_SHIFT);

	if (key >= AS_XMEM_ARENA_KEY) {
		if (primary) {
			sp->type = TYPE_PRI_STAGE;
		}
		else if (secondary) {
			sp->type = TYPE_SEC_STAGE;
		}
	}
	else if (key == AS_XMEM_TREEX_KEY) {
		if (primary) {
			sp->type = TYPE_TREEX;
		}
	}
	else {
		if (primary) {
			sp->type = TYPE_BASE ;
		}
		else if (secondary) {
			sp->type = TYPE_META ;
		}
		else {
			sp->type = TYPE_DAT_STAGE;
		}
	}

	// Extract stage number from key.

	if (sp->type == TYPE_PRI_STAGE ||
		sp->type == TYPE_SEC_STAGE) {
		if ((uint32_t)key < MIN_ARENA || (uint32_t)key > MAX_ARENA) {
			free(*segment);
			*segment = NULL;
			*error = ENOENT;
			return false;
		}

		sp->stage = (uint32_t)key;
	}
	else if (sp->type == TYPE_DAT_STAGE) {
		sp->stage = (uint32_t)key;
	}
	else {
		sp->stage = 0;
	}

	// Get namespace name for segment.

	sp->nsnm = get_segment_nsnm(sp);

	// If g_crc32 option is selected, compute crc32.

	if (g_crc32) {
		void* memptr = shmat(sp->shmid, NULL, SHM_RDONLY);

		if (memptr == (void*)-1) {
			*error = errno;

			if (sp->nsnm != NULL) {
				free(sp->nsnm);
				sp->nsnm = NULL;
			}

			free(*segment);
			*segment = NULL;
			return false;
		}

		sp->crc32 = crc32(g_crc32_init, memptr, (uInt)sp->segsz);
		shmdt(memptr);
	}
	else {
		sp->crc32 = g_crc32_init;
	}

	*error = 0;
	return true;
}

static char *
get_segment_nsnm(as_segment_t* sp)
{
	void* memptr = shmat(sp->shmid, NULL, SHM_RDONLY);

	if (memptr == (void*)-1) {
		return NULL;
	}

	char nsnm[NAMESPACE_LEN + 1] = {0};

	if (sp->type == TYPE_BASE) {
		memcpy(nsnm, memptr + BASE_NAMESPACE_OFF, NAMESPACE_LEN);
	}
	else if (sp->type == TYPE_DAT_STAGE) {
		memcpy(nsnm, memptr + DATA_NAMESPACE_OFF, NAMESPACE_LEN);
	}
	else {
		shmdt(memptr);

		return NULL;
	}

	shmdt(memptr);

	nsnm[NAMESPACE_LEN] = '\0';

	return nsnm[0] == '\0' ? NULL : strdup(nsnm);
}

static char *
get_file_nsnm(key_t key)
{
	// Is file compressed?

	bool compressed = is_file_compressed(key);

	// Generate the pathname for the file.

	char pathname[PATH_MAX + 1];

	const char* extension = compressed ?  FILE_EXTENSION_CMP : FILE_EXTENSION;

	sprintf(pathname, "%s/%08x%s", g_pathdir, key, extension);

	if (compressed) {
		// Handle compressed file.
		return get_file_nsnm_compressed(key);
	}
		
	// Open the (uncompressed) file.

	int rc = open(pathname, O_RDONLY, DEFAULT_MODE);

	if (rc < 0) {
		if (g_verbose) {
			char errbuff[MAX_BUFFER];
			char* errout = strerror_r(errno, errbuff, MAX_BUFFER);

			printf("Could not open segment file \'%s\'"
					": error was %d: %s.\n", pathname, errno, errout);
		}

		return NULL;
	}

	int fd = rc;

	// Seek in the file to the namespace name field.

	if (lseek(fd, (off_t)DATA_NAMESPACE_OFF, SEEK_SET) != (off_t)DATA_NAMESPACE_OFF) {
		if (g_verbose) {
			char errbuff[MAX_BUFFER];
			char* errout = strerror_r(errno, errbuff, MAX_BUFFER);

			printf("Could not seek to namespace name in data file header \'%s\'"
					": error was %d: %s.\n", pathname, errno, errout);
		}

		return NULL;
	}

	// Read the namespace name fromm the file.

	char nsnm[NAMESPACE_LEN + 1] = {0};

	if (read(fd, (void*)&nsnm, NAMESPACE_LEN) != (size_t)NAMESPACE_LEN) {
		if (g_verbose) {
			char errbuff[MAX_BUFFER];
			char* errout = strerror_r(errno, errbuff, MAX_BUFFER);

			printf("Could not read data file header \'%s\'"
					": error was %d: %s.\n", pathname, errno, errout);
		}

		return NULL;
	}

	close(fd);

	// NUL-terminate nsnm.

	nsnm[NAMESPACE_LEN] = '\0';

	return strdup(nsnm);
}

static bool
is_file_compressed(key_t key)
{
	char pathname[PATH_MAX + 1] = {0};

	// Try compressed.

	sprintf(pathname, "%s/%08x%s", g_pathdir, key, FILE_EXTENSION_CMP);

	// Get status of file.

	struct stat statbuf;

	if (stat(pathname, &statbuf) < 0) {
			return false;
	}

	// Try uncompressed.

	sprintf(pathname, "%s/%08x%s", g_pathdir, key, FILE_EXTENSION);

	// Get status of file.

	if (stat(pathname, &statbuf) < 0) {
			return true;
	}

	assert(false);
}

static char*
get_file_nsnm_compressed(key_t key)
{
	bool compressed = is_file_compressed(key);

	// Generate the pathname for the file.

	char pathname[PATH_MAX + 1];

	const char* extension = compressed ?  FILE_EXTENSION_CMP : FILE_EXTENSION;

	sprintf(pathname, "%s/%08x%s", g_pathdir, key, extension);

	// Open the compressed file.

	int rc = open(pathname, O_RDONLY, DEFAULT_MODE);

	if (rc < 0) {
		if (g_verbose) {
			char errbuff[MAX_BUFFER];
			char* errout = strerror_r(errno, errbuff, MAX_BUFFER);

			printf("Could not open compressed segment file \'%s\'"
					": error was %d: %s.\n", pathname, errno, errout);
		}

		return NULL;
	}

	int fd = rc;

	// Read compressed file header.

	as_cmp_t header;

	if (lseek(fd, (off_t)CMPHDR_OFF, SEEK_SET) != (off_t)CMPHDR_OFF) {
		if (g_verbose) {
			printf("Could not seek to header in compressed file.\n");
		}

		return NULL;
	}

	if (read(fd, (void*)&header, CMPHDR_LEN) != (size_t)CMPHDR_LEN) {
		if (g_verbose) {
			printf("Could not read header from compressed file.\n");
		}

		return NULL;
	}

	// Sanity check header.

	if (header.magic != CMPHDR_MAG1 && header.magic != CMPHDR_MAG2) {
		if (g_verbose) {
			printf("Compressed file header bad magic number:"
					" expecting 0x%08x, found 0x%08x.\n", CMPHDR_MAG2, header.magic);
		}

		return NULL;
	}

	if (header.version != CMPHDR_VER) {
		if (g_verbose) {
			printf("Compressed file header bad version number:"
					" expecting 0x%08x, found 0x%08x.\n", CMPHDR_VER, header.version);
		}

		return NULL;
	}

	// Set up compression engine.

	z_stream infstream;

	infstream.zalloc = Z_NULL;
	infstream.zfree = Z_NULL;
	infstream.opaque = Z_NULL;
	infstream.avail_in = 0;
	infstream.next_in = Z_NULL;

	// Specify gzip only.

	int windowBits = 15 + 32; // Use maximum memory and zlib or gzip algorithm.
	int ret = inflateInit2(&infstream, windowBits);

	if (ret != Z_OK) {
		if (g_verbose) {
			printf("Unable to initialize compression engine.\n");
		}

		return NULL;
	}

	// Allocate memory for compression engine buffer.

	uint8_t* cmp_buf = (uint8_t*)malloc(CMPCHUNK_START);

	if (cmp_buf == NULL) {
		if (g_verbose) {
			printf("Unable to allocate memory for compression engine.\n");
		}

		(void)inflateEnd(&infstream);
		return NULL;
	}

	// Decompress start of file (one chunk). Assumes compression < 20X.

	size_t have_bytes = 0;
	void* buf = malloc(CMPCHUNK_START * 20);
	void* my_buf = buf;

	if (buf == NULL) {
		if (g_verbose) {
			printf("Could not allocate buffer while extracting NSNM from compressed data file.\n");
		}
	
		(void)inflateEnd(&infstream);
		return NULL;
	}

	// Read a chunk of the file into cmp_buf.

	ssize_t bytes_read = read(fd, (void*)cmp_buf, CMPCHUNK_START);

	if (bytes_read < 0) {
		if (g_verbose) {
			printf("Error while reading compressed file.\n");
		}

		(void)inflateEnd(&infstream);
		free(cmp_buf);
		cmp_buf = NULL;
		free(buf);

		return NULL;
	}

	// If read(2) returned no bytes, we're done.

	if (bytes_read == 0) {
		return NULL;
	}

	infstream.avail_in = (uInt)bytes_read;
	infstream.next_in = cmp_buf;

	infstream.avail_out = CMPCHUNK_START;
	my_buf += have_bytes;
	infstream.next_out = my_buf;

	ret = inflate(&infstream, Z_SYNC_FLUSH);

	switch (ret) {
	case Z_ERRNO:
	case Z_NEED_DICT:
	case Z_DATA_ERROR:
	case Z_MEM_ERROR:
	case Z_STREAM_ERROR:

		if (g_verbose) {
			printf("Error while decompressing file");
		}

		switch (ret) {

		case Z_ERRNO:
			if (g_verbose) {
				printf(": error reading compressed file");
			}

			break;

		case Z_STREAM_ERROR:
			if (g_verbose) {
				printf(": invalid compression level");
			}

			break;

		case Z_DATA_ERROR:
			if (g_verbose) {
				printf(": invalid or incomplete deflate data");
			}

			break;

		case Z_MEM_ERROR:
			if (g_verbose) {
				printf(": out of memory");
			}

			break;

		case Z_VERSION_ERROR:
			if (g_verbose) {
				printf(": zlib version mismatch");
			}

			break;

		default:
			if (g_verbose) {
				printf(": unknown error (%d)", ret);
			}

			break;
		}

		if (g_verbose) {
			printf(" (%lu bytes into file).\n",
					infstream.total_in + CMPHDR_LEN);
		}

		(void)inflateEnd(&infstream);
		free(cmp_buf);
		cmp_buf = NULL;
		free(buf);

		return NULL;
	}

	(void)inflateEnd(&infstream);

	// Extract nsnm from buffer.

	char nsnm[NAMESPACE_LEN + 1] = {0};

	memcpy(nsnm, buf + DATA_NAMESPACE_OFF, NAMESPACE_LEN);

	nsnm[NAMESPACE_LEN] ='\0';

	free(cmp_buf);
	cmp_buf = NULL;
	free(buf);

	return strdup(nsnm);
}

// Determine whether a candidate base segment can be backed up.

static bool
analyze_backup_candidate(as_segment_t* segments, uint32_t n_segments,
		uint32_t base_ix)
{
	// Shortcut for primary base segment.

	as_segment_t* pbsp = &segments[base_ix];

	assert(pbsp->type == TYPE_BASE);
	assert(pbsp->nsnm != NULL);

	// Shortcuts for instance and namespace values.

	uint32_t inst = pbsp->inst;
	uint32_t nsid = pbsp->nsid;
	char* nsnm = pbsp->nsnm;

	// Find the primary treex segment.

	as_segment_t* ptsp = NULL;
	uint32_t n_ptsps = 0;

	for (uint32_t ix = 0; ix < n_segments; ix++) {
		as_segment_t* sp = &segments[ix];

		if (sp->type == TYPE_TREEX && sp->nsid == nsid && sp->inst == inst) {
			ptsp = sp;
			ptsp->nsnm = strdup(nsnm);
			n_ptsps++;
		}
	}

	if (n_ptsps != 1) {
		if (g_verbose) {
			printf("Missing treex segment for instance %u"
					", namespace \'%s\' (nsid %d).\n", inst, nsnm, nsid);
		}

		// TODO: if (n_ptsps > 1) will leak ptsp->nsnm.

		return false;
	}

	// Find the primary stage segments.

	as_segment_t pssps[MAX_PRI_STAGES] = { 0 };
	uint32_t n_pssps = 0;

	for (uint32_t ix = 0; ix < n_segments; ix++) {
		as_segment_t* sp = &segments[ix];

		if (sp->type == TYPE_PRI_STAGE && sp->nsid == nsid
				&& sp->inst == inst) {
			pssps[n_pssps] = *sp;
			pssps[n_pssps].nsnm = strdup(nsnm);
			n_pssps++;
		}
	}

	if (n_pssps < 1) {
		if (g_verbose) {
			printf("Missing primary stage segment(s) for instance %u"
					", namespace \'%s\' (nsid %d).\n", inst, nsnm, nsid);
		}

		if (ptsp != NULL && ptsp->nsnm != NULL) {
			free(ptsp->nsnm);
			ptsp->nsnm = NULL;
		}

		return false;
	}

	// Sort the primary stage segments by key.

	if (n_pssps > 1) {
		qsort((void*)pssps, (size_t)n_pssps, sizeof(as_segment_t),
				qsort_compare_segments);
	}

	// Check that all primary stages are present.

	for (uint32_t ix = 0; ix < n_pssps; ix++) {
		bool found = false;

		for (uint32_t jx = 0; jx < n_pssps; jx++) {
			as_segment_t* sp = &pssps[jx];

			if (sp->stage == ix + (uint32_t)AS_XMEM_ARENA_KEY) {
				found = true;
				break;
			}
		}

		if (!found) {
			if (g_verbose) {
				printf("Missing primary stage segment %03x for instance %u"
						", namespace \'%s\' (nsid %d).\n",
						ix + (uint32_t)AS_XMEM_ARENA_KEY, inst, nsnm, nsid);
			}

			if (ptsp != NULL && ptsp->nsnm != NULL) {
				free(ptsp->nsnm);
				ptsp->nsnm = NULL;
			}

			for (uint32_t jx = 0; jx < n_pssps; jx++) {
				if (pssps[jx].nsnm != NULL) {
					free(pssps[jx].nsnm);
					pssps[jx].nsnm = NULL;
				}
			}

			return false;
		}
	}

	// Find the meta segment (if any).

	as_segment_t* smsp = NULL;
	uint32_t n_smsps = 0;

	for (uint32_t ix = 0; ix < n_segments; ix++) {
		as_segment_t* sp = &segments[ix];

		if (sp->type == TYPE_META && sp->nsid == nsid && sp->inst == inst) {
			smsp = sp;
			smsp->nsnm = strdup(nsnm);
			n_smsps++;
		}
	}

	if (n_smsps > 1) {
		if (g_verbose) {
			printf("Too many meta segments for instance %u"
					", namespace \'%s\' (nsid %d).\n", inst, nsnm, nsid);
		}

		if (ptsp != NULL && ptsp->nsnm != NULL) {
			free(ptsp->nsnm);
			ptsp->nsnm = NULL;
		}

		for (uint32_t jx= 0; jx < n_pssps; jx++) {
			if (pssps[jx].nsnm != NULL) {
				free(pssps[jx].nsnm);
				pssps[jx].nsnm = NULL;
			}
		}

		// TODO: If (n_smsps > 1) will leak smsp->nsnm.

		return false;
	}

	as_segment_t sssps[MAX_SEC_STAGES] = { 0 };
	uint32_t n_sssps = 0;

	// Found a meta segment?

	if (n_smsps == 1) {

		// Find the secondary stage segments.

		for (uint32_t ix = 0; ix < n_segments; ix++) {
			as_segment_t* sp = &segments[ix];

			if (sp->type == TYPE_SEC_STAGE && sp->nsid == nsid
					&& sp->inst == inst) {

				sssps[n_sssps] = *sp;
				sssps[n_sssps].nsnm = strdup(nsnm);
				n_sssps++;
			}
		}

		if (n_sssps < 1) {
			if (g_verbose) {
				printf("No secondary stage segments for instance %u"
						", namespace \'%s\' (nsid %d).\n", inst, nsnm, nsid);
			}

			if (ptsp != NULL && ptsp->nsnm != NULL) {
				free(ptsp->nsnm);
				ptsp->nsnm = NULL;
			}

			for (uint32_t jx= 0; jx < n_pssps; jx++) {
				if (pssps[jx].nsnm != NULL) {
					free(pssps[jx].nsnm);
					pssps[jx].nsnm = NULL;
				}
			}

			if (n_sssps > 0) {
				if (smsp != NULL && smsp->nsnm != NULL) {
					free(smsp->nsnm);
					smsp->nsnm = NULL;
				}
			}


			return false;
		}

		// Sort the secondary stage segments by key.

		if (n_sssps > 1) {
			qsort((void*)sssps, (size_t)n_sssps, sizeof(as_segment_t),
					qsort_compare_segments);
		}

		// Check that all secondary stages are present.

		for (uint32_t ix = 0; ix < n_sssps; ix++) {
			bool found = false;

			for (uint32_t jx = 0; jx < n_sssps; jx++) {
				as_segment_t* sp = &sssps[jx];

				if (sp->stage == ix + (uint32_t)AS_XMEM_ARENA_KEY) {
					found = true;
				}
			}

			if (!found) {
				if (g_verbose) {
					printf(
							"Missing secondary stage segment %03x for instance %u"
									", namespace \'%s\' (nsid %d).\n",
							ix + (uint32_t)AS_XMEM_ARENA_KEY, inst, nsnm,
							nsid);
				}

				if (ptsp != NULL && ptsp->nsnm != NULL) {
					free(ptsp->nsnm);
					ptsp->nsnm = NULL;
				}

				for (uint32_t jx= 0; jx < n_pssps; jx++) {
					if (pssps[jx].nsnm != NULL) {
						free(pssps[jx].nsnm);
						pssps[jx].nsnm = NULL;
					}
				}

				if (smsp != NULL && smsp->nsnm != NULL) {
					free(smsp->nsnm);
					smsp->nsnm = NULL;
				}

				for (uint32_t jx= 0; jx < n_sssps; jx++) {
					if (sssps[jx].nsnm != NULL) {
						free(sssps[jx].nsnm);
						sssps[jx].nsnm = NULL;
					}
				}

				return false;
			}
		}
	}

	// Find the data segments, if any, associated with the base segment's namespace.

	as_segment_t dsps[MAX_DATA_STAGES] = { 0 };
	uint32_t n_dsps = 0;

	for (uint32_t ix = 0; ix < n_segments; ix++) {
		as_segment_t* sp = &segments[ix];

		// Remember data segments with the correct instance and namespace name.

		if (sp->type == TYPE_DAT_STAGE && sp->inst == inst) {
			// Check whether this data segment matches the base segment's namespace name.

			if (strcmp(pbsp->nsnm, sp->nsnm) == 0) {
				dsps[n_dsps++] = *sp;
			}
		}
	}

	// Sort the data segments (if any).

	if (n_dsps > 1) {
		qsort((void*)dsps, (size_t)n_dsps, sizeof(as_segment_t),
				qsort_compare_segments);
	}

	// If verbose, display a list of segments to be backed up.

	if (g_verbose) {
		printf("\n");
		display_segments(pbsp, ptsp, pssps, n_pssps, smsp, sssps, n_sssps, dsps, n_dsps);
		printf("\n");
	}

	// Sanity-check backup candidate,

	if (!analyze_backup_sanity(pbsp, ptsp, pssps, n_pssps, smsp, sssps, n_sssps, dsps, n_dsps)) {
		if (g_verbose && !g_analyze) {
			printf("Failed backup sanity check for instance %u"
					", namespace \'%s\' (nsid %d).\n", inst, nsnm, nsid);
		}

		if (ptsp != NULL && ptsp->nsnm != NULL) {
			free(ptsp->nsnm);
			ptsp->nsnm = NULL;
		}

		for (uint32_t jx= 0; jx < n_pssps; jx++) {\
			if (pssps[jx].nsnm != NULL) {
				free(pssps[jx].nsnm);
				pssps[jx].nsnm = NULL;
			}
		}

		if (smsp != NULL && smsp->nsnm != NULL) {
			free(smsp->nsnm);
			smsp->nsnm = NULL;
		}

		for (uint32_t jx= 0; jx < n_sssps; jx++) {
			if (sssps[jx].nsnm != NULL) {
				free(sssps[jx].nsnm);
				sssps[jx].nsnm = NULL;
			}
		}

		for (uint32_t jx = 0; jx < n_dsps; jx++) {
			if (dsps[jx].nsnm != NULL) {
				free(dsps[jx].nsnm);
				dsps[jx].nsnm = NULL;
			}
		}

		return false;
	}

	// Determine whether to merely analyze or actually backup.

	if (g_analyze) {
		if (g_verbose) {
			// Print command to backup these segments.

			printf("%s -b", g_progname);
			printf(" -i %u", inst);
			printf(" -p %s", g_pathdir);

			printf(" -n ");

			for (uint32_t ix = 0; ix < g_nsnm_count; ix++) {
				if (ix == g_nsnm_count - 1) {
					printf("%s", g_nsnm_array[ix]);
				}
				else {
					printf("%s,", g_nsnm_array[ix]);
				}
			}

			if (g_compress) {
				printf(" -z");
			}
			if (g_crc32) {
				printf(" -c");
			}
			printf("\n");
		}

		if (ptsp->nsnm != NULL) {
			free(ptsp->nsnm);
			ptsp->nsnm = NULL;
		}

		for (uint32_t jx= 0; jx < n_pssps; jx++) {
			if (pssps[jx].nsnm != NULL) {
				free(pssps[jx].nsnm);
				pssps[jx].nsnm = NULL;
			}
		}

		if (smsp != NULL && smsp->nsnm != NULL) {
			free(smsp->nsnm);
			smsp->nsnm = NULL;
		}

		for (uint32_t jx= 0; jx < n_sssps; jx++) {
			if (sssps[jx].nsnm != NULL) {
				free(sssps[jx].nsnm);
				sssps[jx].nsnm = NULL;
			}
		}

		for (uint32_t jx = 0; jx < n_dsps; jx++) {
			if (dsps[jx].nsnm != NULL) {
				free(dsps[jx].nsnm);
				dsps[jx].nsnm = NULL;
			}
		}

		return true;
	}

	// Actually perform backup...

//	if (g_verbose) {
//		printf("\n");
//		display_segments(pbsp, ptsp, pssps, n_pssps, smsp, sssps, n_sssps, dsps, n_dsps);
//		printf("\n");
//	}

	bool success =  backup_candidate(pbsp, ptsp, pssps, n_pssps, smsp, sssps, n_sssps,
						dsps, n_dsps);

	if (ptsp != NULL && ptsp->nsnm != NULL) {
		free(ptsp->nsnm);
		ptsp->nsnm = NULL;
	}

	for (uint32_t jx = 0; jx < n_pssps; jx++) {
		if (pssps[jx].nsnm != NULL) {
			free(pssps[jx].nsnm);
			pssps[jx].nsnm = NULL;
		}
	}

	if (n_sssps > 0) {
		if (smsp != NULL && smsp->nsnm != NULL) {
			free(smsp->nsnm);
			smsp->nsnm = NULL;
		}

		for (uint32_t jx = 0; jx < n_sssps; jx++) {
			if (sssps[jx].nsnm != NULL) {
				free(sssps[jx].nsnm);
				sssps[jx].nsnm = NULL;
			}
		}
	}

	if (n_dsps > 0) {
		for (uint32_t jx = 0; jx < n_dsps; jx++) {
			if (dsps[jx].nsnm != NULL) {
				free(dsps[jx].nsnm);
				dsps[jx].nsnm = NULL;
			}
		}
	}

	return success;
}

// qsort(3) comparison routine for shared memory segments.

static int
qsort_compare_segments(const void* left, const void* right)
{
	return (int)((uint32_t)((as_segment_t*)left)->key
			- (uint32_t)((as_segment_t*)right)->key);
}

// Display a table of all segments to be backed up.

static void
display_segments(as_segment_t* pbsp, as_segment_t* ptsp,
		as_segment_t pssps[], uint32_t n_pssps, as_segment_t* smsp,
		as_segment_t sssps[], uint32_t n_sssps,
		as_segment_t dsps[], uint32_t n_dsps)
{
	uint32_t n_rows;

	if (pbsp) {
		n_rows = 1 + 2 + n_pssps + (n_sssps > 0 ? (1 + n_sssps) : 0) + n_dsps;
	}
	else {
		n_rows = 1 + n_dsps;
	}

	char* table[n_rows][g_crc32 ? 13 : 12];

	// Fill in table header.

	table[0][0] = strdup("key");
	table[0][1] = strdup("shmid");
	table[0][2] = strdup("user");
	table[0][3] = strdup("group");
	table[0][4] = strdup("mode");
	table[0][5] = strdup("natt");
	table[0][6] = strdup("segsz");
	table[0][7] = strdup("inst");
	table[0][8] = strdup("nsid");
	table[0][9] = strdup("name");
	table[0][10] = strdup("type");
	table[0][11] = strdup("stage");
	if (g_crc32) {
		table[0][12] = strdup("crc32");
	}

	char buffer[MAX_BUFFER];

	// Fill in table body.

	for (uint32_t i = 1; i < n_rows; i++) {
		as_segment_t* sp;

		if (pbsp == NULL) {
			sp = &dsps[i - 1];
		}
		else if (i == 1) {
			sp = pbsp;
		}
		else if (i == 2) {
			sp = ptsp;
		}
		else if (i == 3 && smsp != NULL) {
			sp = smsp;
		}
		else if (i <= 3 + n_pssps - (uint32_t)(smsp == NULL)) {
			sp = &pssps[i - (4 - (uint32_t)(smsp == NULL))];
		}
		else if (i <= 3 + n_pssps + n_sssps - (uint32_t)(smsp == NULL)) {
			sp = &sssps[i - (4 + n_pssps - (uint32_t)(smsp == NULL))];
		}
		else {
			sp = &dsps[i - (4 + n_pssps + n_sssps - (uint32_t)(smsp == NULL))];
		}

		sprintf(buffer, "0x%08x", sp->key);
		table[i][0] = strdup(buffer);

		sprintf(buffer, "%d", sp->shmid);
		table[i][1] = strdup(buffer);

		struct passwd* pw;

		if ((pw = getpwuid(sp->uid)) == NULL) {
			sprintf(buffer, "%d", sp->uid);
		}
		else {
			sprintf(buffer, "%s", pw->pw_name);
		}
		table[i][2] = strdup(buffer);

		struct group* gr;

		if ((gr = getgrgid(sp->gid)) == NULL) {
			sprintf(buffer, "%d", sp->gid);
		}
		else {
			sprintf(buffer, "%s", gr->gr_name);
		}
		table[i][3] = strdup(buffer);

		sprintf(buffer, "0%o", sp->mode);
		table[i][4] = strdup(buffer);

		sprintf(buffer, "%lu", sp->natt);
		table[i][5] = strdup(buffer);

		sprintf(buffer, "%lu", sp->segsz);
		table[i][6] = strdup(buffer);

		sprintf(buffer, "%u", sp->inst);
		table[i][7] = strdup(buffer);

		sprintf(buffer, "%u", sp->nsid);
		table[i][8] = strdup(buffer);

		sprintf(buffer, "%s", sp->nsnm == NULL ? "-" : sp->nsnm);

		table[i][9] = strdup(buffer);

		switch (sp->type) {
		case TYPE_BASE:
			sprintf(buffer, "pi-base");
			break;
		case TYPE_TREEX:
			sprintf(buffer, "pi-treex");
			break;
		case TYPE_META:
			sprintf(buffer, "si-meta");
			break;
		case TYPE_PRI_STAGE:
			sprintf(buffer, "pi-stage");
			break;
		case TYPE_SEC_STAGE:
			sprintf(buffer, "si-stage");
			break;
		case TYPE_DAT_STAGE:
			sprintf(buffer, "data-stage");
			break;
		default:
			printf("unknown segment type is %d.\n", sp->type);
			assert(false);
		}
		table[i][10] = strdup(buffer);

		if (sp->type == TYPE_PRI_STAGE
				|| sp->type == TYPE_SEC_STAGE
				|| sp->type == TYPE_DAT_STAGE) {
			sprintf(buffer, "0x%03x", sp->stage);
		}
		else {
			sprintf(buffer, "-");
		}
		table[i][11] = strdup(buffer);

		if (g_crc32) {
			sprintf(buffer, "0x%08lx", sp->crc32);
			table[i][12] = strdup(buffer);
		}
	}

	// Draw the table. Frees all allocated elements.

	draw_table(&table[0][0], n_rows, g_crc32 ? 13 : 12);
}

// Perform a final sanity check on this backup candidate.

static bool
analyze_backup_sanity(as_segment_t* pbsp, as_segment_t* ptsp,
		as_segment_t pssps[], uint32_t n_pssps, as_segment_t* smsp,
		as_segment_t sssps[], uint32_t n_sssps,
		as_segment_t dsps[], uint32_t n_dsps)
{
	(void)ptsp;
	(void)pssps;
	(void)sssps;
	(void)dsps;
	(void)n_dsps;

	// Extract primary stage count from the base segment.

	if (pbsp->segsz < N_ARENAS_PRI_OFF + N_ARENAS_LEN) {
		if (g_verbose) {
			printf("Base segment 0x%08x is too small.\n", pbsp->key);
		}

		return false;
	}

	void* memptr = shmat(pbsp->shmid, NULL, SHM_RDONLY);

	if (memptr == (void*)-1) {
		if (g_verbose) {
			printf("Could not access base segment 0x%08x.\n", pbsp->key);
		}

		return false;
	}

	// Check the base segment version number.

	uint32_t base_ver = *(uint32_t*)(memptr + BASEVER_OFF);

	if (base_ver < BASEVER_MIN || base_ver > BASEVER_MAX) {
		if (g_verbose) {
			printf("Invalid version number in base segment 0x%08x:"
					" expecting version in range %u to %u"
					", found version %u.\n", pbsp->key, BASEVER_MIN, BASEVER_MAX,
					base_ver);
		}

		shmdt(memptr);

		return false;
	}

	// Check the base segment shutdown status.

	uint32_t base_shut = *(uint32_t*)(memptr + BASESHUT_OFF);

	if (base_shut != 1) {
		if (g_verbose) {
			printf("Shutdown status in base segment 0x%08x:"
					" expecting status 1"
					", found status %u.\n", pbsp->key, base_shut);
		}

		shmdt(memptr);

		return false;
	}

	// Actually read the number of stages from the base segment.

	uint32_t n_pri_arenas = *(uint32_t*)(memptr + N_ARENAS_PRI_OFF);

	shmdt(memptr);

	// Check that we have the full complement of primary stages to back up.

	if (n_pri_arenas != n_pssps) {
		if (g_verbose) {
			printf("Wrong number of primary arena stages"
					": expecting %u, found %u.\n", n_pri_arenas, n_pssps);
		}

		return false;
	}

	// Check sanity of the meta segment (if any).

	if (n_sssps > 0) {

		// Check the size of the meta segment.

		if (smsp->segsz < N_ARENAS_SEC_OFF + N_ARENAS_LEN) {
			if (g_verbose) {
				printf("Meta segment 0x%08x is too small.\n", smsp->key);
			}

			return false;
		}

		memptr = shmat(smsp->shmid, NULL, SHM_RDONLY);

		if (memptr == (void*)-1) {
			if (g_verbose) {
				printf("Could not access meta segment 0x%08x.\n", smsp->key);
			}

			return false;
		}

		// Actually read the number of stages from the base segment.

		uint32_t n_sec_arenas = *(uint32_t*)(memptr + N_ARENAS_SEC_OFF);

		shmdt(memptr);

		// Check that we have the full complement of secondary stages to back up.

		if (n_sec_arenas != n_sssps) {
			if (g_verbose) {
				printf("Wrong number of secondary arena stages"
						": expecting %u, found %u.\n", n_sec_arenas, n_sssps);
			}

			return false;
		}
	}

	// Check that the destination has no files for this namespace and instance.

	DIR* dir = opendir(g_pathdir);

	if (dir == NULL) {
		return true;
	}

	struct dirent* dirent;
	as_file_t aerospike_file;

	bool found = false;
	while ((dirent = readdir(dir)) != NULL) {
		// Skip "." and ".." entries.

		if (strcmp(dirent->d_name, ".") == 0
				|| strcmp(dirent->d_name, "..") == 0) {
			continue;
		}

		// Validate the file name.

		if (!validate_file_name(dirent->d_name, &aerospike_file)) {
			continue;
		}

		// Check whether the file is for this namespace and instance.

		if (aerospike_file.inst == g_inst && aerospike_file.nsid == pbsp->nsid) {
			found = true;

			if (g_verbose && !g_analyze) {
				printf("Found existing Aerospike file \'%s/%s\' with instance %u"
								", namespace \'%s\' (nsid %u)"
								": cannot back up associated segment.\n",
						g_pathdir, dirent->d_name, g_inst, pbsp->nsnm,
						pbsp->nsid);
			}

			continue;
		}
	}

	closedir(dir);

	return !found;
}

// Actually back up identified segments.

static bool
backup_candidate(as_segment_t* pbsp, as_segment_t* ptsp,
		as_segment_t pssps[], uint32_t n_pssps, as_segment_t* smsp,
		as_segment_t sssps[], uint32_t n_sssps,
		as_segment_t dsps[], uint32_t n_dsps)
{
	// Create list of file I/O requests.
	// Note: Assumes that ulimit (number of open files) is big enough.

	uint32_t n_files = n_pssps + !!pbsp + !!ptsp;

	if (n_sssps > 0) {
		n_files += 1 + n_sssps;
	}

	if (n_dsps > 0) {
		n_files += n_dsps;
	}

	as_io_t ios[n_files];
	uint32_t n_ios = 0;

	if (pbsp) {
	    if (!backup_candidate_file(pbsp, &ios[n_ios++], ios, pbsp, ptsp, pssps, n_pssps,
				       smsp, sssps, n_sssps, dsps, n_dsps)) {
		return false;
	    }
	}

	if (ptsp) {
	    if (!backup_candidate_file(ptsp, &ios[n_ios++], ios, pbsp, ptsp, pssps, n_pssps,
				       smsp, sssps, n_sssps, dsps, n_dsps)) {
		return false;
	    }
	}

	for (uint32_t i = 0; i < n_pssps; i++) {
		if (!backup_candidate_file(&pssps[i], &ios[n_ios++], ios, pbsp, ptsp, pssps,
				n_pssps, smsp, sssps, n_sssps, dsps, n_dsps)) {
			return false;
		}
	}

	if (n_sssps > 0) {
		if (!backup_candidate_file(smsp, &ios[n_ios++], ios, pbsp, ptsp, pssps,
				n_pssps, smsp, sssps, n_sssps, dsps, n_dsps)) {
			return false;
		}

		for (uint32_t i = 0; i < n_sssps; i++) {
			if (!backup_candidate_file(&sssps[i], &ios[n_ios++], ios, pbsp, ptsp,
					pssps, n_pssps, smsp, sssps, n_sssps, dsps, n_dsps)) {
				return false;
			}
		}
	}

	for (uint32_t i = 0; i < n_dsps; i++) {
		if (!backup_candidate_file(&dsps[i], &ios[n_ios++], ios, pbsp, ptsp,
				pssps, n_pssps, smsp, sssps, n_sssps, dsps, n_dsps)) {
			return false;
		}
	}

	assert(n_files == n_ios);

	// Hand the file I/O requests in for processing.

	bool success = start_io(&ios[0], n_ios);

	// I/O requests were processed. Now post-process.

	if (success && g_crc32) {
		if (!backup_candidate_check_crc32(ios, pbsp, ptsp, pssps, n_pssps, smsp,
				sssps, n_sssps, dsps, n_dsps)) {
			success = false;
		}
	}

	// Notify user of success or failure.

	if (g_verbose) {
		printf("%s",
				success ? "\nSuccessfully backed up" : "\nFailed to back up");
		printf(" %u Aerospike database segments.\n", n_files);
	}

	// Clean up all intermediate operations.

	backup_candidate_cleanup(ios, pbsp, ptsp, pssps, n_pssps, smsp, sssps, n_sssps,
			dsps, n_dsps, !success);

	return success;
}

static bool
backup_candidate_file(as_segment_t* sp, as_io_t* io, as_io_t ios[],
		as_segment_t* pbsp, as_segment_t* ptsp, as_segment_t pssps[],
		uint32_t n_pssps, as_segment_t* smsp, as_segment_t sssps[],
		uint32_t n_sssps, as_segment_t dsps[], uint32_t n_dsps)
{
	// Attach to the segment (for reading).

	void* memptr = shmat(sp->shmid, NULL, SHM_RDONLY);

	if (memptr == (void*)-1) {
		char errbuff[MAX_BUFFER];
		char* errout = strerror_r(errno, errbuff, MAX_BUFFER);

		printf("Could not attach segment 0x%08x"
				": error was %d: %s.\n", sp->key,
		errno, errout);

		// Clean up all intermediate operations.

		backup_candidate_cleanup(ios, pbsp, ptsp, pssps, n_pssps, smsp, sssps, n_sssps,
		dsps, n_dsps, true);

		return false;
	}

	// Create an I/O request for the segment.

	io->key = sp->key;
	io->write = true;
	io->memptr = memptr;
	io->filsz = 0;
	io->segsz = sp->segsz;
	io->mode = sp->mode;
	io->uid = sp->uid;
	io->gid = sp->gid;
	io->crc32 = g_crc32_init;

	// Construct the filename extension.

	const char* extension;

	if (sp->type != TYPE_BASE && sp->type != TYPE_META && g_compress) {
		io->compress = true;
		extension = FILE_EXTENSION_CMP;
	}
	else {
		io->compress = false;
		extension = FILE_EXTENSION;
	}

	// Construct the filename for the segment file.

	char pathname[PATH_MAX + 1];

	sprintf(pathname, "%s/%08x%s", g_pathdir, sp->key, extension);

	// Open (create) the segment file.

	int rc = open(pathname, O_CREAT | O_RDWR | O_EXCL, DEFAULT_MODE);

	if (rc < 0) {
		char errbuff[MAX_BUFFER];
		char* errout = strerror_r(errno, errbuff, MAX_BUFFER);

		if (g_verbose) {
			printf("Could not create segment file \'%s\'"
					": error was %d: %s.\n", pathname, errno, errout);
		}

		// Clean up all intermediate operations.

		backup_candidate_cleanup(ios, pbsp, ptsp, pssps, n_pssps, smsp, sssps, n_sssps,
			dsps, n_dsps, true);

		return false;
	}

	// Complete creation of I/O request.

	io->fd = rc;

	if (!io->compress) {
		// Allocate storage space for the data to be written to the file.

		rc = posix_fallocate(io->fd, 0, (off_t)sp->segsz);

		if (rc < 0) {
			char errbuff[MAX_BUFFER];
			char* errout = strerror_r(errno, errbuff, MAX_BUFFER);

			if (g_verbose) {
				printf("Could not allocate storage for segment file \'%s\'"
						": error was %d: %s.\n", pathname, errno, errout);
			}

			// Clean up all intermediate operations.

			backup_candidate_cleanup(ios, pbsp, ptsp, pssps, n_pssps, smsp, sssps,
					n_sssps, dsps, n_dsps, true);

			return false;
		}
	}

	return true;
}

static bool
backup_candidate_check_crc32(as_io_t ios[], as_segment_t* pbsp,
		as_segment_t* ptsp, as_segment_t pssps[], uint32_t n_pssps,
		as_segment_t* smsp, as_segment_t sssps[], uint32_t n_sssps,
		as_segment_t dsps[], uint32_t n_dsps)
{
	if (pbsp && pbsp->crc32 != ios[0].crc32) {
		if (g_verbose) {
			printf("crc32-check failed for base segment.\n");
		}

		return false;
	}

	if (ptsp && ptsp->crc32 != ios[1].crc32) {
		if (g_verbose) {
			printf("crc32-check failed for tree-x segment.\n");
		}

		return false;
	}

	for (uint32_t ix = 0; ix < n_pssps; ix++) {
		if (pssps[ix].crc32 != ios[2 + ix].crc32) {
			if (g_verbose) {
				printf("crc32-check failed for primary stage segment.\n");
			}

			return false;
		}
	}

	if (n_sssps > 0) {
		if (smsp && smsp->crc32 != ios[2 + n_pssps].crc32) {
			if (g_verbose) {
				printf("crc32-check failed for secondary meta segment.\n");
			}

			return false;
		}

		for (uint32_t ix = 0; ix < n_sssps; ix++) {
			if (sssps[ix].crc32 != ios[2 + n_pssps + 1 + ix].crc32) {
				if (g_verbose) {
					printf("crc32-check failed for secondary stage segment.\n");
				}

				return false;
			}
		}
	}

	if (!pbsp) {
		for (uint32_t ix = 0; ix < n_dsps; ix++) {
			if (dsps[ix].crc32 != ios[ix].crc32) {
				if (g_verbose) {
					printf("crc32-check failed for data stage segment.\n");
				}

				return false;
			}
		}
	}
	else {
		for (uint32_t ix = 0; ix < n_dsps; ix++) {
			if (dsps[ix].crc32 != ios[2 + n_pssps + n_sssps + !!smsp + ix].crc32) {
				if (g_verbose) {
					printf("crc32-check failed for data stage segment.\n");
				}

				return false;
			}
		}
	}

	return true;
}

// Cleanup from backup_candidate().
// If remove is set, all files created should be removed.

static void
backup_candidate_cleanup(as_io_t ios[], as_segment_t* pbsp,
		as_segment_t* ptsp, as_segment_t pssps[], uint32_t n_pssps,
		as_segment_t* smsp, as_segment_t sssps[], uint32_t n_sssps,
		as_segment_t dsps[], uint32_t n_dsps,
		bool remove_files)
{
	(void)dsps;
	(void)n_dsps;

	uint32_t n_objects = n_pssps + !!pbsp + !!ptsp;

	if (n_sssps > 0) {
		n_objects += 1 + n_sssps;
	}

	// Detach all (possibly) attached segments.

	for (uint32_t ix = 0; ix < n_objects; ix++) {
		shmdt(ios[ix].memptr);
	}

	// Close all (possibly) opened files.

	for (uint32_t ix = 0; ix < n_objects; ix++) {
		close(ios[ix].fd);
	}

	// Remove all created files (only on failure case).

	if (!remove_files) {
		return;
	}

	char pathname[PATH_MAX + 1];
	const char* extension;
	as_segment_t* sp;

	if (pbsp) {
	    sp = pbsp;
	    extension = FILE_EXTENSION;
	    sprintf(pathname, "%s/%08x%s", g_pathdir, sp->key, extension);
	    unlink(pathname);
	}

	if (ptsp) {
	    sp = ptsp;
	    extension = g_compress ? FILE_EXTENSION_CMP : FILE_EXTENSION;
	    sprintf(pathname, "%s/%08x%s", g_pathdir, sp->key, extension);
	    unlink(pathname);
	}

	for (uint32_t ix = 0; ix < n_pssps; ix++) {
		sp = &pssps[ix];

		extension = g_compress ? FILE_EXTENSION_CMP : FILE_EXTENSION;
		sprintf(pathname, "%s/%08x%s", g_pathdir, sp->key, extension);
		unlink(pathname);
	}

	if (n_sssps > 0) {
		sp = smsp;
		extension = FILE_EXTENSION;
		sprintf(pathname, "%s/%08x%s", g_pathdir, sp->key, extension);
		unlink(pathname);

		for (uint32_t ix = 0; ix < n_sssps; ix++) {
			sp = &sssps[ix];

			extension = g_compress ? FILE_EXTENSION_CMP : FILE_EXTENSION;
			sprintf(pathname, "%s/%08x%s", g_pathdir, sp->key, extension);
			unlink(pathname);
		}
	}
}

// Actually start the I/Os in the list.

static bool
start_io(as_io_t ios[], uint32_t n_ios)
{
	// Number of threads to start, based on g_max_threads and n_ios.

	uint32_t n_threads = n_ios > g_max_threads ? g_max_threads : n_ios;

	// Set global file I/O variables.

	g_ios = ios;
	g_n_ios = n_ios;
	g_next_io = 0;
	g_ios_ok = true;

	// How much data will be transferred (total)?

	g_total_to_transfer = 0;

	for (uint32_t i = 0; i < g_n_ios; i++) {
		g_total_to_transfer += (uint64_t)ios[i].segsz;
	}

	g_total_transferred = 0;
	g_decile_transferred = 0;

	// Remember start time.

	int rc = clock_gettime(CLOCK_MONOTONIC, &g_io_start_time);

	if (rc != 0) {
		if (g_verbose) {
			printf("Could not determine I/O start time.\n");
		}

		return false;
	}

	// Initialize global mutex.

	pthread_mutex_init(&g_io_mutex, NULL);

	// Thread table.

	pthread_t threads[n_threads];

	// Actually start threads.

	uint32_t i;
	for (i = 0; i < n_threads; i++) {
		rc = pthread_create(&threads[i], NULL, run_io, NULL);

		// If creating thread failed, notify successfully created threads
		// to end and wait.

		if (rc != 0) {
			pthread_mutex_lock(&g_io_mutex);
			g_ios_ok = false;
			pthread_mutex_unlock(&g_io_mutex);

			break;
		}
	}

	// Wait for all threads to exit.

	for (uint32_t j = 0; j < i; j++) {
		pthread_join(threads[j], NULL);
	}

	struct timespec io_end_time;

	rc = clock_gettime(CLOCK_MONOTONIC, &io_end_time);

	if (rc != 0) {
		if (g_verbose) {
			printf("Could not determine I/O end time.\n");
		}
	}
	else {
		if (g_verbose && g_decile_transferred != 10) {
			char* time_str = strtime_diff_eta(&g_io_start_time, &io_end_time,
					g_decile_transferred);

			printf("Total I/O time was %s.\n", time_str);
			free(time_str);
			time_str = NULL;
		}
	}

	// Return success or failure.

	return g_ios_ok;
}

// Process individual file I/O requests by individual threads.

static void*
run_io(void* args)
{
	(void)args;

	while (true) {
		// Get the next I/O request.

		uint32_t next;

		pthread_mutex_lock(&g_io_mutex);

		// Is everything running okay?

		bool ok = g_ios_ok;

		// If so, get next I/O operation.

		if (ok) {
			next = g_next_io++;
		}

		pthread_mutex_unlock(&g_io_mutex);

		// If there are no more requests or one or more failed, quit.

		if (!ok || next >= g_n_ios) {
			break;
		}

		// Extract this I/O request.

		as_io_t* io = &g_ios[next];

		bool success;

		if (io->write) {
			success = write_file(io->fd, io->memptr, io->segsz, io->mode,
					io->uid, io->gid, io->compress, &io->crc32);
			(void)fsync(io->fd);
		}
		else {
			success = read_file(io->fd, io->memptr, io->filsz, io->segsz,
					io->shmid, io->mode, io->uid, io->gid, io->compress,
					&io->crc32);
		}

		// If this request failed, stop the other threads.

		if (!success) {
			pthread_mutex_lock(&g_io_mutex);
			g_ios_ok = false;
			pthread_mutex_unlock(&g_io_mutex);
			break;
		}
		else {
			pthread_mutex_lock(&g_io_mutex);

			g_total_transferred += io->segsz;

			// if we've reached a notable decile point, notify the user.
			// Note: This is the only point at which output is done under
			// a mutex, so it isn't stepped on by other threads.

			if (g_verbose) {
				uint32_t decile_transferred = (uint32_t)((g_total_transferred
						* 10UL) / g_total_to_transfer);

				if (g_decile_transferred != decile_transferred) {
					g_decile_transferred = decile_transferred;

					if (g_verbose) {
						printf("Transferred %3d%% of data",
								g_decile_transferred * 10);
					}

					struct timespec io_end_time;
					int rc = clock_gettime(CLOCK_MONOTONIC, &io_end_time);

					if (rc != 0) {
						if (g_verbose) {
							printf(".\n");
						}
					}
					else {
						char* time_str = strtime_diff_eta(&g_io_start_time,
								&io_end_time, g_decile_transferred);

						if (g_verbose) {
							printf(" in %s.\n", time_str);
						}

						free(time_str);
						time_str = NULL;
					}
				}
			}

			pthread_mutex_unlock(&g_io_mutex);
		}
	}

	return NULL;
}

// Write a complete file (compressed if requested). Compute crc32 if requested.

static bool
write_file(int fd, const void* buf, size_t segsz, mode_t mode,
		uid_t uid, gid_t gid, bool compress, uLong* crc)
{
	if (compress) {
		return zwrite_file(fd, buf, segsz, mode, uid, gid, crc);
	}
	else {
		return pwrite_file(fd, buf, segsz, mode, uid, gid, crc);
	}
}

// Write a complete file (compressed). Retrieve crc32 if requested.

static bool
zwrite_file(int fd, const void* buf, size_t segsz, mode_t mode,
		uid_t uid, gid_t gid, uLong* crc)
{
	// Set up and write initial compressed file header.

	as_cmp_t header;

	header.magic = CMPHDR_MAG2;
	header.version = CMPHDR_VER;
	header.crc32 = g_crc32_init;
	header.segsz = segsz;

	if (lseek(fd, (off_t)CMPHDR_OFF, SEEK_SET) != (off_t)CMPHDR_OFF) {
		if (g_verbose) {
			printf("Could not write compressed file header to file.\n");
		}

		return false;
	}

	if (write(fd, (void*)&header, CMPHDR_LEN) != (size_t)CMPHDR_LEN) {
		if (g_verbose) {
			printf("Could not write compressed file header to file.\n");
		}

		return false;
	}

	// Allocate buffer for compression intermediate results.

	uint8_t* cmp_buf = (uint8_t*)malloc(CMPCHUNK);

	if (cmp_buf == NULL) {
		if (g_verbose) {
			printf("Could not allocate memory to compress file.\n");
		}

		return false;
	}

	// Set up the compression.

	z_stream defstream;

	defstream.zalloc = Z_NULL;
	defstream.zfree = Z_NULL;
	defstream.opaque = Z_NULL;

	// Use gzip compression; as a side effect, get crc32 for free.

	int windowBits = 15 + 16; // Use max memory and use gzip algorithm.
	int memLevel = 9; // Use maximum memory level.

	if (deflateInit2(&defstream, Z_BEST_SPEED, Z_DEFLATED, windowBits, memLevel,
			Z_DEFAULT_STRATEGY) != Z_OK) {
		if (g_verbose) {
			printf("Did not initialize compression engine while writing"
					" segment to file.\n");
		}

		free(cmp_buf);
		cmp_buf = NULL;
		return false;
	}

	// Whole segment is available in buf.

	defstream.avail_in = (uInt)segsz;
	defstream.next_in = (Bytef*)buf;

	// Actually compress the segment.

	int ret;

	do {
		// Compress one chunk at a time.

		defstream.avail_out = (uInt)CMPCHUNK;
		defstream.next_out = (Bytef*)cmp_buf;

		ret = deflate(&defstream, Z_FINISH);
		if (ret == Z_STREAM_ERROR) {
			if (g_verbose) {
				printf("Could not compress file.\n");
			}

			free(cmp_buf);
			cmp_buf = NULL;
			return false;
		}

		size_t have_bytes = CMPCHUNK - defstream.avail_out;

		// Write this chunk to output file.

		if (write(fd, (void*)cmp_buf, have_bytes) != (ssize_t)have_bytes) {
			if (g_verbose) {
				printf("Could not write to compressed file.\n");
			}

			(void)deflateEnd(&defstream);
			free(cmp_buf);
			cmp_buf = NULL;
			return false;
		}
	} while (defstream.avail_out == 0);

	// Finished compressing. Was it successful?

	if (defstream.avail_in != 0) {
		if (g_verbose) {
			printf("Failed to compress file.\n");
		}

		(void)deflateEnd(&defstream);
		free(cmp_buf);
		cmp_buf = NULL;
		return false;
	}

	// Shut down compression engine.

	deflateEnd(&defstream);

	// Free intermediate compression buffer.

	free(cmp_buf);
	cmp_buf = NULL;

	// Ensure compression engine finished happily.

	if (ret != Z_STREAM_END) {
		if (g_verbose) {
			printf("Did not finish compressing segment to file.\n");
		}

		return false;
	}

	// Should we retrieve crc32?

	*crc = g_crc32 ? defstream.adler : g_crc32_init;

	// Go back and write compressed file header (ALWAYS).

	header.magic = CMPHDR_MAG2;
	header.version = CMPHDR_VER;
	header.segsz = segsz;
	header.crc32 = defstream.adler;

	if (lseek(fd, (off_t)CMPHDR_OFF, SEEK_SET) != (off_t)CMPHDR_OFF) {
		if (g_verbose) {
			printf("Could not seek to compressed file header in file.\n");
		}

		return false;
	}

	if (write(fd, (void*)&header, CMPHDR_LEN) != (size_t)CMPHDR_LEN) {
		if (g_verbose) {
			printf("Could not write compressed file header to file.\n");
		}

		return false;
	}

	// Set file ownership.

	if (fchown(fd, uid, gid) == -1) {
		char errbuff[MAX_BUFFER];
		char* errout = strerror_r(errno, errbuff, MAX_BUFFER);

		if (g_verbose) {
			printf("Unable to set uid or gid for file"
					": error was %d: %s\n", errno, errout);
		}

		return false;
	}

	// Set file mode.

	if (fchmod(fd, mode) == -1) {
		char errbuff[MAX_BUFFER];
		char* errout = strerror_r(errno, errbuff, MAX_BUFFER);

		if (g_verbose) {
			printf("Unable to set mode for file"
					": error was %d: %s\n", errno, errout);
		}

		return false;
	}

	return true;
}

// Write a complete file (uncompressed). Compute crc32 if requested.

static bool
pwrite_file(int fd, const void* buf, size_t segsz, mode_t mode,
		uid_t uid, gid_t gid, uLong* crc)
{
	// newsize is running size, as pwrite(2) progresses.

	ssize_t newsize = (ssize_t)segsz;

	// bytes_written is result of individual pwrite(2) operation.

	ssize_t bytes_written;

	// Initially, offset is start of segment / segment file.

	off_t offset = 0;

	// Write chunks of segment, as large as possible.

	while ((bytes_written = pwrite(fd, buf, (size_t)newsize, offset)) != newsize) {
		if (bytes_written == 0) {
			continue;
		}
		else if (bytes_written < 0) {
			char errbuff[MAX_BUFFER];
			char* errout = strerror_r(errno, errbuff, MAX_BUFFER);

			if (g_verbose) {
				printf("Unable to pwrite(2) file"
						": error was %d: %s\n", errno, errout);
			}

			return false;
		}

		// Should we compute crc32? If so, apply to this chunk.

		if (g_crc32) {
			*crc = crc32(*crc, buf, (uInt)bytes_written);
		}

		// If only partial write, set up next chunk.

		buf += bytes_written;
		offset += bytes_written;
		newsize -= bytes_written;
	}

	// Finish crc32 computation on last chunk, if incomplete.

	if (bytes_written >= 0 && g_crc32) {
		*crc = crc32(*crc, buf, (uInt)bytes_written);
	}

	// Set file ownership.

	if (fchown(fd, uid, gid) == -1) {
		char errbuff[MAX_BUFFER];
		char* errout = strerror_r(errno, errbuff, MAX_BUFFER);

		if (g_verbose) {
			printf("Unable to set uid or gid for file"
					": error was %d: %s\n", errno, errout);
		}

		return false;
	}

	// Set file mode.

	if (fchmod(fd, mode) == -1) {
		char errbuff[MAX_BUFFER];
		char* errout = strerror_r(errno, errbuff, MAX_BUFFER);

		if (g_verbose) {
			printf("Unable to set mode for file"
					": error was %d: %s\n", errno, errout);
		}

		return false;
	}

	return true;
}

// Read a complete file (compressed if requested). Compute crc32 if requested.

static bool
read_file(int fd, void* buf, size_t filsz, size_t segsz, int shmid,
		mode_t mode, uid_t uid, gid_t gid, bool compress, uLong* crc)
{
	if (compress) {
		return zread_file(fd, buf, filsz, segsz, shmid, mode, uid, gid, crc);
	}
	else {
		return pread_file(fd, buf, segsz, shmid, mode, uid, gid, crc);
	}
}

// Read a complete file (compressed). Compute crc32 if requested.

static bool
zread_file(int fd, void* buf, size_t filsz, size_t segsz, int shmid,
		mode_t mode, uid_t uid, gid_t gid, uLong* crc)
{
	(void)filsz;

	// Read compressed file header.

	as_cmp_t header;

	if (lseek(fd, (off_t)CMPHDR_OFF, SEEK_SET) != (off_t)CMPHDR_OFF) {
		if (g_verbose) {
			printf("Could not seek to header in compressed file.\n");
		}

		return false;
	}

	if (read(fd, (void*)&header, CMPHDR_LEN) != (size_t)CMPHDR_LEN) {
		if (g_verbose) {
			printf("Could not read header from compressed file.\n");
		}

		return false;
	}

	// Sanity check header.

	if (header.magic != CMPHDR_MAG1 && header.magic != CMPHDR_MAG2) {
		if (g_verbose) {
			printf("Compressed file header bad magic number:"
					" expecting 0x%08x, found 0x%08x;", CMPHDR_MAG2, header.magic);
		}

		return false;
	}

	if (header.version != CMPHDR_VER) {
		if (g_verbose) {
			printf("Compressed file header bad version number:"
					" expecting 0x%08x, found 0x%08x;", CMPHDR_VER, header.version);
		}

		return false;
	}

	if (segsz != header.segsz) {
		if (g_verbose) {
			printf("Compressed file header segment size mismatch:"
					" expecting %lu, found %lu.\n", segsz, header.segsz);
		}

		return false;
	}

	// Set up compression engine.

	z_stream infstream;

	infstream.zalloc = Z_NULL;
	infstream.zfree = Z_NULL;
	infstream.opaque = Z_NULL;
	infstream.avail_in = 0;
	infstream.next_in = Z_NULL;

	// Specify gzip only.

	int windowBits = 15 + 32; // Use maximum memory and zlib or gzip algorithm.
	int ret = inflateInit2(&infstream, windowBits);

	if (ret != Z_OK) {
		if (g_verbose) {
			printf("Unable to initialize compression engine.\n");
		}

		return false;
	}

	// Allocate memory for compression engine buffer.

	uint8_t* cmp_buf = (uint8_t*)malloc(CMPCHUNK);

	if (cmp_buf == NULL) {
		if (g_verbose) {
			printf("Unable to allocate memory for compression engine.\n");
		}

		(void)inflateEnd(&infstream);
		return false;
	}

	// Decompress file one chunk at a time.

	size_t have_bytes = 0;
	void* my_buf = buf;

	do {

		// Read a chunk of the file into cmp_buf.

		ssize_t bytes_read = read(fd, (void*)cmp_buf, CMPCHUNK);

		if (bytes_read < 0) {
			if (g_verbose) {
				printf("Error while reading compressed file.\n");
			}

			(void)inflateEnd(&infstream);
			free(cmp_buf);
			cmp_buf = NULL;
			return false;
		}

		// If read(2) returned no bytes, we're done.

		if (bytes_read == 0) {
			break;
		}

		infstream.avail_in = (uInt)bytes_read;
		infstream.next_in = cmp_buf;

		do {
			infstream.avail_out = CMPCHUNK;
			my_buf += have_bytes;
			infstream.next_out = my_buf;

			ret = inflate(&infstream, Z_SYNC_FLUSH);

			switch (ret) {
			case Z_ERRNO:
			case Z_NEED_DICT:
			case Z_DATA_ERROR:
			case Z_MEM_ERROR:
			case Z_STREAM_ERROR:

				if (g_verbose) {
					printf("Error while decompressing file");
				}

				switch (ret) {

				case Z_ERRNO:
					if (g_verbose) {
						printf(": error reading compressed file");
					}

					break;

				case Z_STREAM_ERROR:
					if (g_verbose) {
						printf(": invalid compression level");
					}

					break;

				case Z_DATA_ERROR:
					if (g_verbose) {
						printf(": invalid or incomplete deflate data");
					}

					break;

				case Z_MEM_ERROR:
					if (g_verbose) {
						printf(": out of memory");
					}

					break;

				case Z_VERSION_ERROR:
					if (g_verbose) {
						printf(": zlib version mismatch");
					}

					break;

				default:
					if (g_verbose) {
						printf(": unknown error (%d)", ret);
					}

					break;
				}

				if (g_verbose) {
					printf(" (%lu bytes into file).\n",
							infstream.total_in + CMPHDR_LEN);
				}

				(void)inflateEnd(&infstream);
				free(cmp_buf);
				cmp_buf = NULL;
				return false;
			}

			have_bytes = CMPCHUNK - infstream.avail_out;
		} while (infstream.avail_out == 0);
	} while (ret != Z_STREAM_END);

	(void)inflateEnd(&infstream);

	free(cmp_buf);
	cmp_buf = NULL;

	// Retrieve crc32, if requested.

	*crc = g_crc32 ? infstream.adler : g_crc32_init;

	// Set segment ownership

	struct shmid_ds shmid_ds = { .shm_perm.uid = uid, .shm_perm.gid = gid,
			.shm_perm.mode = (short unsigned)(mode & MODE_MASK), };

	if (shmctl(shmid, IPC_SET, &shmid_ds) == -1) {
		char errbuff[MAX_BUFFER];
		char* errout = strerror_r(errno, errbuff, MAX_BUFFER);

		if (g_verbose) {
			printf("Unable to set uid, gid, or mode for shared memory segment"
					": error was %d: %s\n", errno, errout);
		}

		return false;
	}

	return (ret == Z_STREAM_END || ret == Z_OK) ? true : false;
}

// Read a complete file (uncompressed). Compute crc32 if requested.

static bool
pread_file(int fd, void* buf, size_t segsz, int shmid, mode_t mode,
		uid_t uid, gid_t gid, uLong* crc)
{
	// newsize is running size, as pread(2) progresses.

	ssize_t newsize = (ssize_t)segsz;

	// bytes_read is result of individual pread(2) operation.

	ssize_t bytes_read;

	// Initially, offset is start of segment / segment file.

	off_t offset = 0;

	while ((bytes_read = pread(fd, buf, (size_t)newsize, offset)) != newsize) {
		if (bytes_read == 0) {
			continue;
		}
		else if (bytes_read < 0) {
			char errbuff[MAX_BUFFER];
			char* errout = strerror_r(errno, errbuff, MAX_BUFFER);

			if (g_verbose) {
				printf("Unable to pread(2) file"
						": error was %d: %s\n", errno, errout);
			}

			return false;
		}

		// Should we compute crc32? If so, apply to this chunk.

		if (g_crc32) {
			*crc = crc32(*crc, buf, (uInt)bytes_read);
		}

		// If only partial read, set up next chunk.

		buf += bytes_read;
		offset += bytes_read;
		newsize -= bytes_read;
	}

	if (bytes_read > 0) {
		// Finish crc32 computation on last chunk, if incomplete.

		if (g_crc32) {
			*crc = crc32(*crc, buf, (uInt)bytes_read);
		}

		// Set segment ownership.

		struct shmid_ds shmid_ds = { .shm_perm.uid = uid, .shm_perm.gid = gid,
				.shm_perm.mode = (short unsigned)(mode & MODE_MASK), };

		if (shmctl(shmid, IPC_SET, &shmid_ds) == -1) {
			char errbuff[MAX_BUFFER];
			char* errout = strerror_r(errno, errbuff, MAX_BUFFER);

			if (g_verbose) {
				printf("Unable to set uid, gid, or mode for shared memory segment"
						": error was %d: %s\n", errno, errout);
			}

			return false;
		}
	}

	return true;
}

// Analyze restore operation.

static bool
analyze_restore(void)
{
	as_file_t* fps = NULL;
	int error;

	// First, see if we can access the backup directory for reading.
	// Do not create.

	if (!check_dir(g_pathdir, false, false)) {
		if (g_verbose) {
			printf("Cannot read from directory \'%s\'", g_pathdir);
			printf(": either it does not exist"
					" or we don't have read permission.\n");
		}
		return false;
	}

	// Get the list of Aerospike database segment files that passed the filter.

	uint32_t n_fps;

	if (!list_files(&fps, &n_fps, &error) || n_fps == 0) {
		// Note: n_fps and error are valid even if list_files() returned false.

		if (g_verbose) {
			printf("\nDid not find any Aerospike database segment files");

			if (g_inst != INV_INST) {
				printf(", instance %u", g_inst);
			}

			if (g_nsnm != NULL) {
				printf(", namespace \'%s\'", g_nsnm);
			}

			if (error != 0) {
				char errbuff[MAX_BUFFER];
				char *errout = strerror_r(errno, errbuff, MAX_BUFFER);

				printf(": error was %d: %s", error, errout);
			}

			printf(".\n");
		}

		if (n_fps > 0) {
			for (uint32_t j = 0; j < n_fps; j++) {
				as_file_t* fp = &fps[j];

				if (fp->type == TYPE_BASE && fp->nsnm != NULL) {
					free(fp->nsnm);
					fp->nsnm = NULL;
				}
			}

			if (fps != NULL) {
				free(fps);
				fps = NULL;
			}
		}

		return false;
	}

	// Look for files that can be restored.
	// Must have one base segment file, one treex segment file,
	// and at least one primary stage segment file.
	// May have one meta file and if so, at least one secondary stage segment file.

	bool candidates = false;

	for (uint32_t ix = 0; ix < n_fps; ix++) {
		as_file_t* fp = &fps[ix];

		if (fp->type == TYPE_BASE) {
			candidates = true;

			if (!analyze_restore_candidate(fps, n_fps, ix)) {

				for (uint32_t j = 0; j < n_fps; j++) {
					as_file_t* fp2 = &fps[j];

					if (fp2->type == TYPE_BASE && fp2->nsnm != NULL) {
						free(fp2->nsnm);
						fp2->nsnm = NULL;
					}
				}

				if (fp != NULL) {
					free(fps);
					fps = NULL;
				}

				return false;
			}
		}
	}

	if (!candidates) {
	    // Look for orphaned data segments in the g_nsnm namespace.

	    as_file_t dfps[MAX_DATA_STAGES] = { 0 };
	    uint32_t n_dfps = 0;

	    for (uint32_t ix = 0; ix < n_fps; ix++) {
			as_file_t* fp = &fps[ix];

			if (fp->type != TYPE_DAT_STAGE ||
				fp->inst != g_inst) {
			    continue;
			}

			fp->nsnm = get_file_nsnm(fp->key);

			if (strcmp(g_nsnm, fp->nsnm) != 0) {
				continue;
			}

			// Record this segment for backup.

			dfps[n_dfps++] = *fp;
	    }

	    if (n_dfps) {
			qsort((void*)dfps, (size_t)n_dfps, sizeof(as_file_t),
					qsort_compare_files);

			if (g_verbose) {
				printf("\n");
				display_files(NULL, NULL, NULL, 0, NULL, NULL, 0, dfps, n_dfps);
				printf("\n");
			}

			if (!g_analyze) {
			    restore_candidate(NULL, NULL, NULL, 0, NULL, NULL, 0, dfps, n_dfps);
			}

			candidates = true;
	    }
	}


	if (!candidates) {
		if (g_verbose) {
			printf("\nDid not find any Aerospike database segment files");
			if (g_inst != INV_INST) {
				printf(", instance %u", g_inst);
			}

			if (g_nsnm != NULL) {
				printf(", namespace \'%s\'", g_nsnm);
			}

			printf(".\n");
		}
	}

	// Free table created by list_files().

	for (uint32_t jx = 0; jx < n_fps; jx++) {
		as_file_t* fp = &fps[jx];

		if (fp->type == TYPE_BASE && fp->nsnm != NULL) {
			free(fp->nsnm);
			fp->nsnm = NULL;
		}
	}

	if (fps != NULL) {
		free(fps);
		fps = NULL;
	}

	return true;
}

// Analyze whether to restore a candidate set of segment files.

static bool
analyze_restore_candidate(as_file_t* fps, uint32_t n_fps, uint32_t base_ix)
{
	// Extract the base segment file.

	as_file_t* pbfp = &fps[base_ix];

	assert(pbfp->type == TYPE_BASE);
	assert(pbfp->nsnm != NULL);

	uint32_t inst = pbfp->inst;
	uint32_t nsid = pbfp->nsid;
	char* nsnm = pbfp->nsnm;

	// Find the corresponding treex segment file.

	as_file_t* ptfp = NULL;
	uint32_t n_ptfps = 0;

	for (uint32_t ix = 0; ix < n_fps; ix++) {
		if (ix == base_ix) {
			continue;
		}

		as_file_t* fp = &fps[ix];

		if (fp->type == TYPE_TREEX && fp->inst == inst && fp->nsid == nsid) {
			ptfp = fp;
			ptfp->nsnm = strdup(nsnm);
			n_ptfps++;
		}
	}

	if (n_ptfps != 1) {
		if (g_verbose) {
			printf("Missing treex segment file.\n");
		}

		// TODO: If (n_ptfps > 1) will leak ptfp->nsnm;

		return false;
	}

	// Find the primary stage segment files.

	as_file_t psfps[MAX_PRI_STAGES] = { 0 };
	uint32_t n_psfps = 0;

	for (uint32_t ix = 0; ix < n_fps; ix++) {
		as_file_t* fp = &fps[ix];

		if (fp->type == TYPE_PRI_STAGE && fp->nsid == nsid
				&& fp->inst == inst) {
			psfps[n_psfps] = *fp;
			psfps[n_psfps].nsnm = strdup(nsnm);
			n_psfps++;
		}
	}

	if (n_psfps < 1) {
		if (g_verbose) {
			printf("Missing primary stage segment file(s) for instance %u"
					", namespace \'%s\' (nsid %d).\n", inst, nsnm, nsid);
		}

		if (ptfp != NULL && ptfp->nsnm != NULL) {
			free(ptfp->nsnm);
			ptfp->nsnm = NULL;
		}

		return false;
	}

	// Sort the primary stage segment files by key.

	if (n_psfps > 1) {
		qsort((void*)psfps, (size_t)n_psfps, sizeof(as_file_t),
				qsort_compare_files);
	}

	// Check that all primary stage files are present.

	for (uint32_t ix = 0; ix < n_psfps; ix++) {
		bool found = false;

		for (uint32_t jx = 0; jx < n_psfps; jx++) {
			as_file_t* fp = &psfps[jx];

			if (fp->stage == ix + (uint32_t)AS_XMEM_ARENA_KEY) {
				found = true;
				break;
			}
		}

		if (!found) {
			if (g_verbose) {
				printf("Missing primary stage segment file %03x for instance %u"
						", namespace \'%s\' (nsid %d).\n",
						ix + (uint32_t)AS_XMEM_ARENA_KEY, inst, nsnm, nsid);
			}

			if (ptfp != NULL && ptfp->nsnm != NULL) {
				free(ptfp->nsnm);
				ptfp->nsnm = NULL;
			}

			for (uint32_t jx = 0; jx < n_psfps; jx++) {
				if (psfps[jx].nsnm != NULL) {
					free(psfps[jx].nsnm);
					psfps[jx].nsnm = NULL;
				}
			}

			return false;
		}
	}

	// Find the meta segment file (if any).

	as_file_t* smfp = NULL;
	uint32_t n_smfps = 0;

	for (uint32_t ix = 0; ix < n_fps; ix++) {
		as_file_t* fp = &fps[ix];

		if (fp->type == TYPE_META && fp->nsid == nsid && fp->inst == inst) {
			smfp = fp;
			smfp->nsnm = strdup(nsnm);
			n_smfps++;
		}
	}

	if (n_smfps > 1) {
		if (g_verbose) {
			printf("Too many meta segment files for instance %u"
					", namespace \'%s\' (nsid %d).\n", inst, nsnm, nsid);
		}

		if (ptfp != NULL && ptfp->nsnm != NULL) {
			free(ptfp->nsnm);
			ptfp->nsnm = NULL;
		}

		for (uint32_t jx= 0; jx < n_psfps; jx++) {
			if (psfps[jx].nsnm != NULL) {
				free(psfps[jx].nsnm);
				psfps[jx].nsnm = NULL;
			}
		}

		// TODO: If (n_smfps > 1) will leak smfp->nsnm;

		return false;
	}

	as_file_t ssfps[MAX_SEC_STAGES] = { 0 };
	uint32_t n_ssfps = 0;

	// Found a meta segment file?

	if (n_smfps == 1) {

		// Find the secondary stage segment files.

		for (uint32_t ix = 0; ix < n_fps; ix++) {
			as_file_t* fp = &fps[ix];

			if (fp->type == TYPE_SEC_STAGE && fp->nsid == nsid
					&& fp->inst == inst) {
				ssfps[n_ssfps] = *fp;
				ssfps[n_ssfps].nsnm = strdup(nsnm);
				n_ssfps++;
			}
		}

		if (n_ssfps < 1) {
			if (g_verbose) {
				printf("No secondary stage segment files for instance %u"
						", namespace \'%s\' (nsid %d).\n", inst, nsnm, nsid);
			}

			if (ptfp != NULL && ptfp->nsnm != NULL) {
				free(ptfp->nsnm);
				ptfp->nsnm = NULL;
			}

			for (uint32_t jx= 0; jx < n_psfps; jx++) {
				if (psfps[jx].nsnm != NULL) {
					free(psfps[jx].nsnm);
					psfps[jx].nsnm = NULL;
				}
			}

			if (smfp != NULL && smfp->nsnm != NULL) {
				free(smfp->nsnm);
				smfp->nsnm = NULL;
			}

			return false;
		}

		// Sort the secondary stage segment files by key.

		if (n_ssfps > 1) {
			qsort((void*)ssfps, (size_t)n_ssfps, sizeof(as_file_t),
					qsort_compare_files);
		}

		// Check that all secondary stage files are present.

		for (uint32_t ix = 0; ix < n_ssfps; ix++) {
			bool found = false;

			for (uint32_t jx = 0; jx < n_ssfps; jx++) {
				as_file_t* fp = &ssfps[jx];

				if (fp->stage == ix + (uint32_t)AS_XMEM_ARENA_KEY) {
					found = true;
				}
			}

			if (!found) {
				if (g_verbose) {
					printf(
							"Missing secondary stage segment file %03x for instance %u"
									", namespace \'%s\' (nsid %d).\n",
							ix + (uint32_t)AS_XMEM_ARENA_KEY, inst, nsnm, nsid);
				}

				if (ptfp != NULL && ptfp->nsnm != NULL) {
					free(ptfp->nsnm);
					ptfp->nsnm = NULL;
				}

				for (uint32_t jx= 0; jx < n_psfps; jx++) {
					if (psfps[jx].nsnm != NULL) {
						free(psfps[jx].nsnm);
						psfps[jx].nsnm = NULL;
					}
				}

				if (smfp != NULL && smfp->nsnm != NULL) {
					free(smfp->nsnm);
					smfp->nsnm = NULL;
				}

				for (uint32_t jx = 0; jx < n_ssfps; jx++) {
					if (ssfps[jx].nsnm != NULL) {
						free(ssfps[jx].nsnm);
						ssfps[jx].nsnm = NULL;
					}
				}

				return false;
			}
		}
	}

	as_file_t dfps[MAX_DATA_STAGES] = { 0 };
	uint32_t n_dfps = 0;

	for (uint32_t ix = 0; ix < n_fps; ix++) {
		as_file_t* fp = &fps[ix];

		if (fp->type == TYPE_DAT_STAGE && fp->inst == inst) {
			dfps[n_dfps] = *fp;
			dfps[n_dfps].nsnm = get_file_nsnm(fp->key);
			if (strcmp(dfps[n_dfps].nsnm, nsnm) == 0) {
				n_dfps++;
			}
		}
	}

	// If verbose, display a list of matching segment files.

	if (g_verbose) {
		printf("\n");
		display_files(pbfp, ptfp, psfps, n_psfps, smfp, ssfps, n_ssfps,
			dfps, n_dfps);
		printf("\n");
	}

	// Perform a final sanity check on this restore candidate.

	if (!analyze_restore_sanity(pbfp, ptfp, psfps, n_psfps, smfp, ssfps, n_ssfps,
			dfps, n_dfps)) {
		if (g_verbose) {
			printf("Failed restore sanity check for instance %u"
					", namespace \'%s\' (nsid %d).\n", fps[base_ix].inst,
					fps[base_ix].nsnm, fps[base_ix].nsid);
		}

		if (ptfp != NULL && ptfp->nsnm != NULL) {
			free(ptfp->nsnm);
			ptfp->nsnm = NULL;
		}

		for (uint32_t jx= 0; jx < n_psfps; jx++) {
			if (psfps[jx].nsnm != NULL) {
				free(psfps[jx].nsnm);
				psfps[jx].nsnm = NULL;
			}
		}

		if (smfp != NULL && smfp->nsnm != NULL) {
			free(smfp->nsnm);
			smfp->nsnm = NULL;
		}

		for (uint32_t jx = 0; jx < n_ssfps; jx++) {
			if (ssfps[jx].nsnm != NULL) {
				free(ssfps[jx].nsnm);
				ssfps[jx].nsnm = NULL;
			}
		}

		return false;
	}

	// Determine whether to analyze or actually restore.

	if (g_analyze) {
		if (g_verbose) {
			// Print command to restore these segment files.

			printf("%s -r", g_progname);
			printf(" -i %u", inst);
			printf(" -p %s", g_pathdir);

			printf(" -n ");

			for (uint32_t ix = 0; ix < g_nsnm_count; ix++) {
				if (ix == g_nsnm_count - 1) {
					printf("%s", g_nsnm_array[ix]);
				}
				else {
					printf("%s,", g_nsnm_array[ix]);
				}
			}

			if (g_crc32) {
				printf(" -c");
			}

			printf("\n");
		}

		if (ptfp != NULL && ptfp->nsnm != NULL) {
			free(ptfp->nsnm);
			ptfp->nsnm = NULL;
		}

		for (uint32_t jx= 0; jx < n_psfps; jx++) {
			if (psfps[jx].nsnm != NULL) {
				free(psfps[jx].nsnm);
				psfps[jx].nsnm = NULL;
			}
		}

		if (smfp != NULL && smfp->nsnm != NULL) {
			free(smfp->nsnm);
			smfp->nsnm = NULL;
		}

		for (uint32_t jx = 0; jx < n_ssfps; jx++) {
			if (ssfps[jx].nsnm != NULL) {
				free(ssfps[jx].nsnm);
				ssfps[jx].nsnm = NULL;
			}
		}

		return true;
	}
	else {
		// Actually perform restores...

		bool success = restore_candidate(pbfp, ptfp, psfps, n_psfps, smfp, ssfps,
				n_ssfps, dfps, n_dfps);

		if (ptfp != NULL && ptfp->nsnm != NULL) {
			free(ptfp->nsnm);
			ptfp->nsnm = NULL;
		}

		for (uint32_t jx= 0; jx < n_psfps; jx++) {
			if (psfps[jx].nsnm != NULL) {
				free(psfps[jx].nsnm);
				psfps[jx].nsnm = NULL;
			}
		}

		if (smfp != NULL && smfp->nsnm != NULL) {
			free(smfp->nsnm);
			smfp->nsnm = NULL;
		}

		for (uint32_t jx = 0; jx < n_ssfps; jx++) {
			if (ssfps[jx].nsnm != NULL) {
				free(ssfps[jx].nsnm);
				ssfps[jx].nsnm = NULL;
			}
		}

		for (uint32_t jx = 0; jx < n_dfps; jx++) {
			if (dfps[jx].nsnm != NULL) {
				free(dfps[jx].nsnm);
				dfps[jx].nsnm = NULL;
			}
		}

		return success;
	}
}

// Display a list of segment files to be restored.

static void
display_files(as_file_t* pbfp, as_file_t* ptfp, as_file_t psfps[], uint32_t n_psfps,
		as_file_t* smfp, as_file_t ssfps[], uint32_t n_ssfps, as_file_t dfps[], uint32_t n_dfps)
{
	// The table to be displayed.

	uint32_t n_rows;

	if (pbfp) {
		n_rows = 1 + 2 + n_psfps + (n_ssfps > 0 ? (1 + n_ssfps) : 0) + n_dfps;
	}
	else {
		n_rows = 1 + n_dfps;
	}

	char* table[n_rows][11];

	// Create the table header.

	table[0][0] = strdup("key");
	table[0][1] = strdup("user");
	table[0][2] = strdup("group");
	table[0][3] = strdup("mode");
	table[0][4] = strdup("filsz");
	table[0][5] = strdup("segsz");
	table[0][6] = strdup("inst");
	table[0][7] = strdup("nsid");
	table[0][8] = strdup("name");
	table[0][9] = strdup("type");
	table[0][10] = strdup("stage");

	// Create the table body.

	char buffer[MAX_BUFFER];

	for (uint32_t i = 1; i < n_rows; i++) {
		as_file_t* fp;

		if (pbfp == NULL) {
			fp = &dfps[i - 1];
		}
		else if (i == 1) {
			fp = pbfp;
		}
		else if (i == 2) {
			fp = ptfp;
		}
		else if (i == 3 && smfp != NULL) {
			fp = smfp;
		}
		else if (i <= 3 + n_psfps - (uint32_t)(smfp == NULL)) {
			fp = &psfps[i - (4 - (uint32_t)(smfp == NULL))];
		}
		else if (i <= 3 + n_psfps + n_ssfps - (uint32_t)(smfp == NULL)) {
			fp = &ssfps[i - (4 + n_psfps - (uint32_t)(smfp == NULL))];
		}
		else {
			fp = &dfps[i - (4 + n_psfps + n_ssfps - (uint32_t)(smfp == NULL))];
		}

		sprintf(buffer, "0x%08x", fp->key);
		table[i][0] = strdup(buffer);

		struct passwd* pw;

		if ((pw = getpwuid(fp->uid)) == NULL) {
			sprintf(buffer, "%d", fp->uid);
		}
		else {
			sprintf(buffer, "%s", pw->pw_name);
		}
		table[i][1] = strdup(buffer);

		struct group* gr;

		if ((gr = getgrgid(fp->gid)) == NULL) {
			sprintf(buffer, "%d", fp->gid);
		}
		else {
			sprintf(buffer, "%s", gr->gr_name);
		}

		table[i][2] = strdup(buffer);

		sprintf(buffer, "0%o", fp->mode);
		table[i][3] = strdup(buffer);

		sprintf(buffer, "%lu", fp->filsz);
		table[i][4] = strdup(buffer);

		sprintf(buffer, "%lu", fp->segsz);
		table[i][5] = strdup(buffer);

		sprintf(buffer, "%u", fp->inst);
		table[i][6] = strdup(buffer);

		sprintf(buffer, "%u", fp->nsid);
		table[i][7] = strdup(buffer);

		sprintf(buffer, "%s", fp->nsnm == NULL ? "-" : fp->nsnm);
		table[i][8] = strdup(buffer);

		switch (fp->type) {
		case TYPE_BASE:
			sprintf(buffer, "pi-base");
			break;
		case TYPE_META:
			sprintf(buffer, "si-meta");
			break;
		case TYPE_TREEX:
			sprintf(buffer, "pi-treex");
			break;
		case TYPE_PRI_STAGE:
			sprintf(buffer, "pi-stage");
			break;
		case TYPE_SEC_STAGE:
			sprintf(buffer, "si-stage");
			break;
		case TYPE_DAT_STAGE:
			sprintf(buffer, "data");
			break;
		default:
			assert(false);
		}

		table[i][9] = strdup(buffer);

		if (fp->type == TYPE_PRI_STAGE ||
			fp->type == TYPE_SEC_STAGE ||
			fp->type == TYPE_DAT_STAGE) {
			sprintf(buffer, "0x%03x", fp->stage);
		}
		else {
			sprintf(buffer, "-");
		}

		table[i][10] = strdup(buffer);
	}

	// Draw the table. All allocated entries will be freed.

	draw_table(&table[0][0], n_rows, 11);
}

static bool
analyze_restore_sanity(as_file_t* pbfp, as_file_t* ptfp, as_file_t psfps[],
		uint32_t n_psfps, as_file_t* smfp, as_file_t ssfps[], uint32_t n_ssfps,
		as_file_t dfps[], uint32_t n_dfps)
{
	(void)ptfp;
	(void)psfps;
	(void)smfp;
	(void)ssfps;
	(void)n_ssfps;
	(void)dfps;
	(void)n_dfps;

	// Check that the number of stages is valid.

	char pathname[PATH_MAX + 1];

	sprintf(pathname, "%s/%08x%s", g_pathdir, pbfp->key, FILE_EXTENSION);

	// Extract arena stage count name from file.

	int rc = open(pathname, O_RDONLY);

	if (rc < 0) {
		if (g_verbose) {
			printf("Could not extract number of arena stages from base segment"
					" file \'%s\'.\n", pathname);
		}

		return false;
	}

	int fd = rc;

	// Check the base_ix segment version number.

	if (lseek(fd, (off_t)BASEVER_OFF, SEEK_SET) != (off_t)BASEVER_OFF) {
		close(fd);

		if (g_verbose) {
			printf("Could not extract version number from base segment"
					" file \'%s\'.\n", pathname);
		}

		return false;
	}

	// Read the version number from base segment file.

	union {
		uint32_t base_ver;
		uint8_t bytes[sizeof(uint32_t)];
	} u1;

	if (read(fd, (void*)u1.bytes, BASEVER_LEN) != BASEVER_LEN) {
		close(fd);

		if (g_verbose) {
			printf("Could not extract version number from base segment"
					" file \'%s\'.\n", pathname);
		}

		return false;
	}

	// Check version number.

	if (u1.base_ver < BASEVER_MIN || u1.base_ver > BASEVER_MAX) {
		close(fd);

		if (g_verbose) {
			printf("Invalid version number in base segment file \'%s\'"
					": expecting version in range %u to %u"
					", found version %u.\n", pathname, BASEVER_MIN, BASEVER_MAX,
					u1.base_ver);
		}

		return false;
	}

	// Read the number of arena stages from the base segment file.

	if (lseek(fd, (off_t)N_ARENAS_PRI_OFF, SEEK_SET)
			!= (off_t)N_ARENAS_PRI_OFF) {
		close(fd);

		if (g_verbose) {
			printf("Could not extract number of arena stages from base segment"
					" file \'%s\'.\n", pathname);
		}

		return false;
	}

	// Read the namespace name.

	union {
		uint32_t n_arenas;
		uint8_t bytes[sizeof(uint32_t)];
	} u2;

	if (read(fd, (void*)u2.bytes, N_ARENAS_LEN) != N_ARENAS_LEN) {
		close(fd);

		if (g_verbose) {
			printf("Could not extract number of arena stages from base segment"
					" file \'%s\'.\n", pathname);
		}

		return false;
	}

	close(fd);

	if (u2.n_arenas != n_psfps) {
		if (g_verbose) {
			printf("Incorrect number of arena stages found"
					": expecting %u, found %u.\n", u2.n_arenas, n_psfps);
		}

		return false;
	}

	// Check that there are no segments with the same namespace and instance.
	// Get info on all shared memory segments.

	struct shmid_ds dummy; // Dummy, needed by shmctl(3).

	rc = shmctl(0, SHM_INFO, &dummy);

	if (rc < 0) {
		if (g_verbose) {
			printf("Could not enumerate shared memory segments.\n");
		}

		return false;
	}

	int max_shmid = rc; // Range of shmids: 0..max_shmid (inclusive).

	// Try each shmid in the range. Some may correspond to segments.

	bool found = false;

	for (int i = 0; i <= max_shmid; i++) {
		// Get information about segment.

		struct shmid_ds ds;

		if (shmctl(i, SHM_STAT, &ds) == -1) {
			continue;
		}

		// Extract key from shmid_ds structure.

		key_t key = ds.shm_perm.__key;

		// Check if this is an Aerospike primary segment key.

		if ((key & AS_XMEM_KEY_TYPE_MASK) == AS_XMEM_PRI_KEY) {

			// Found a valid Aerospike database primary segment.
			// Extract the base from the key.

			key = key & ~AS_XMEM_PRI_KEY;

			// Determine instance from key base.

			uint32_t inst = (uint32_t) key >> AS_XMEM_INSTANCE_KEY_SHIFT;

			// Determine namespace ID from key base.

			key = key & ~(0xf << AS_XMEM_INSTANCE_KEY_SHIFT);

			uint32_t nsid = (uint32_t) (key & (0xff << AS_XMEM_NS_KEY_SHIFT))
					>> AS_XMEM_NS_KEY_SHIFT;

			// Check whether instance and namespace match.

			if (nsid == pbfp->nsid && inst == pbfp->inst) {
				if (g_verbose) {
					printf("Found existing Aerospike primary index segment 0x%08x with"
							" instance %u, namespace \'%s\' (nsid %u)"
							": cannot restore associated file.\n",
							ds.shm_perm.__key, inst, pbfp->nsnm, nsid);
				}

				found = true;
			}
		}

		if ((key & AS_XMEM_KEY_TYPE_MASK) == AS_XMEM_SEC_KEY) {
			// Found a valid Aerospike database secondary segment.
			// Extract the base from the key.

			key = key & ~AS_XMEM_SEC_KEY;

			// Determine instance from key base.

			uint32_t inst = (uint32_t) key >> AS_XMEM_INSTANCE_KEY_SHIFT;

			// Determine namespace ID from key base.

			key = key & ~(0xf << AS_XMEM_INSTANCE_KEY_SHIFT);

			uint32_t nsid = (uint32_t) (key & (0xff << AS_XMEM_NS_KEY_SHIFT))
					>> AS_XMEM_NS_KEY_SHIFT;

			// Check whether instance and namespace match.

			if (nsid == pbfp->nsid && inst == pbfp->inst) {
				if (g_verbose) {
					printf("Found existing Aerospike secondary index segment 0x%08x with"
							" instance %u, namespace \'%s\' (nsid %u)"
							": cannot restore associated file.\n",
							ds.shm_perm.__key, inst, pbfp->nsnm, nsid);
				}

				found = true;
			}
		}

		if ((key & AS_XMEM_KEY_TYPE_MASK) == AS_XMEM_DAT_KEY) {
			// Found a valid Aerospike database data segment.
			// Extract the base from the key.

			key = key & ~AS_XMEM_DAT_KEY;

			// Determine instance from key base.

			uint32_t inst = (uint32_t) key >> AS_XMEM_INSTANCE_KEY_SHIFT;

			// Determine namespace ID from key base.

			key = key & ~(0xf << AS_XMEM_INSTANCE_KEY_SHIFT);

			uint32_t nsid = (uint32_t) (key & (0xff << AS_XMEM_NS_KEY_SHIFT))
					>> AS_XMEM_NS_KEY_SHIFT;

			// Check whether instance and namespace match.

			if (nsid == pbfp->nsid && inst == pbfp->inst) {
				if (g_verbose) {
					printf("Found existing Aerospike data segment 0x%08x with"
							" instance %u, namespace \'%s\' (nsid %u)"
							": cannot restore associated file.\n",
							ds.shm_perm.__key, inst, pbfp->nsnm, nsid);
				}

				found = true;
			}
		}
	}

	return !found;
}

// Actually restore candidate set of segment files.

static bool
restore_candidate(as_file_t* pbfp, as_file_t* ptfp, as_file_t psfps[],
		uint32_t n_psfps, as_file_t* smfp, as_file_t ssfps[], uint32_t n_ssfps,
		as_file_t dfps[], uint32_t n_dfps)
{
	// Create list of file I/O requests.

	uint32_t n_fps = n_psfps + !!pbfp + !!ptfp;

	if (n_ssfps > 0) {
		n_fps += 1 + n_ssfps;
	}

	if (n_dfps > 0) {
		n_fps += n_dfps;
	}

	as_io_t ios[n_fps];
	uint32_t n_ios = 0;

	if (pbfp) {
	    if (!restore_candidate_segment(pbfp, &ios[n_ios], ios, n_ios, pbfp, ptfp, psfps, n_psfps,
					   smfp, ssfps, n_ssfps, dfps, n_dfps)) {
		return false;
	    }
	    n_ios++;
	}

	if (ptfp) {
	    if (!restore_candidate_segment(ptfp, &ios[n_ios], ios, n_ios, pbfp, ptfp, psfps, n_psfps,
					   smfp, ssfps, n_ssfps, dfps, n_dfps)) {
		return false;
	    }
	    n_ios++;
	}

	for (uint32_t i = 0; i < n_psfps; i++) {
		if (!restore_candidate_segment(&psfps[i], &ios[n_ios], ios, n_ios, pbfp, ptfp, psfps,
				n_psfps, smfp, ssfps, n_ssfps, dfps, n_dfps)) {
			return false;
		}

		n_ios++;
	}

	if (n_ssfps > 0) {
		if (!restore_candidate_segment(smfp, &ios[n_ios], ios, n_ios, pbfp, ptfp, psfps,
				n_psfps, smfp, ssfps, n_ssfps, dfps, n_dfps)) {
			return false;
		}

		n_ios++;

		for (uint32_t i = 0; i < n_ssfps; i++) {
			if (!restore_candidate_segment(&ssfps[i], &ios[n_ios], ios, n_ios, pbfp, ptfp,
					psfps, n_psfps, smfp, ssfps, n_ssfps, dfps, n_dfps)) {
				return false;
			}

			n_ios++;
		}
	}

	if (n_dfps > 0) {
		for (uint32_t i = 0; i < n_dfps; i++) {
			if (!restore_candidate_segment(&dfps[i], &ios[n_ios], ios, n_ios, pbfp, ptfp,
					psfps, n_psfps, smfp, ssfps, n_ssfps, dfps, n_dfps)) {
				return false;
			}

			n_ios++;
		}
	}

	assert(n_fps == n_ios);

	// Hand the file I/O requests in for processing.

	bool success = start_io(ios, n_ios);

	// I/O requests were processed. Now post-process.

	if (success && g_crc32) {
		if (!restore_candidate_check_crc32(ios, n_ios)) {
			success = false;
		}
	}

	// Notify the user of success or failure.

	if (g_verbose) {
		printf("%s", success ?  "\nSuccessfully restored" : "\nFailed to restore");
		printf(" %u Aerospike database segment files.\n", n_fps);
	}

	// Clean up all intermediate operations.
	// On failure, will destroy all created segments.

	restore_candidate_cleanup(ios, n_ios, !success);

	return success;
}

static bool
restore_candidate_segment(as_file_t *fp, as_io_t *io, as_io_t ios[],
		uint32_t n_ios, as_file_t *pbfp, as_file_t *ptfp, as_file_t psfps[],
		uint32_t n_psfps, as_file_t *smfp, as_file_t ssfps[], uint32_t n_ssfps,
		as_file_t dfps[], uint32_t n_dfps)
{
	(void)pbfp;
	(void)ptfp;
	(void)psfps;
	(void)n_psfps;
	(void)smfp;
	(void)ssfps;
	(void)n_ssfps;
	(void)dfps;
	(void)n_dfps;

	// Try to create the segment.

	int shmid = shmget(fp->key, fp->segsz, SHMGET_FLAGS_CREATE_ONLY);

	if (shmid < 0) {
		int error = (errno == ENOENT) ? EEXIST : errno;
		char errbuff[MAX_BUFFER];
		char* errout = strerror_r(errno, errbuff, MAX_BUFFER);

		if (g_verbose) {
			printf("Could not create segment with key 0x%08x"
					": error was %d: %s.\n", fp->key, error, errout);
		}

		// Clean up all intermediate operations.

		restore_candidate_cleanup(ios, n_ios, true);

		return false;
	}

	// Attach to the segment (for writing).

	void* memptr = shmat(shmid, NULL, 0);

	// See if the segment was attached.
	// Can not operate on segments that are in use.

	if (memptr == (void*)-1) {
		char errbuff[MAX_BUFFER];
		char* errout = strerror_r(errno, errbuff, MAX_BUFFER);

		if (g_verbose) {
			printf("Could not attach segment with key 0x%08x"
					": error was %d: %s.\n", fp->key, errno, errout);
		}

		// Clean up all intermediate operations.

		restore_candidate_cleanup(ios, n_ios, true);

		return false;
	}

	// Create I/O request for segment file.

	io->key = fp->key;
	io->write = false;
	io->memptr = memptr;
	io->filsz = fp->filsz;
	io->segsz = fp->segsz;
	io->shmid = shmid;
	io->mode = fp->mode;
	io->uid = fp->uid;
	io->gid = fp->gid;
	io->crc32 = g_crc32_init;
	io->compress = fp->compress;

	// Construct the filename for the segment file.

	char pathname[PATH_MAX + 1];

	const char* extension =
			fp->type != TYPE_BASE && fp->compress ?
					FILE_EXTENSION_CMP : FILE_EXTENSION;

	sprintf(pathname, "%s/%08x%s", g_pathdir, fp->key, extension);

	// Open the segment file (for reading).

	int rc = open(pathname, O_RDONLY);

	if (rc < 0) {
		char errbuff[MAX_BUFFER];
		char* errout = strerror_r(errno, errbuff, MAX_BUFFER);

		if (g_verbose) {
			printf("Could not open segment file \'%s\'"
					": error was %d: %s.\n", pathname, errno, errout);
		}

		// Clean up all intermediate operations.

		restore_candidate_cleanup(ios, n_ios, true);

		return false;
	}

	// Complete I/O request.

	io->fd = rc;

	return true;
}

static bool
restore_candidate_check_crc32(as_io_t ios[], uint32_t n_ios)
{
	// Check crc32 for each restored segment.

	uint32_t i;
	for (i = 0; i < n_ios; i++) {
		as_io_t* io = &ios[i];

		// Get the shared memory ID of this segment.

		int shmid = shmget(ios[i].key, ios[i].segsz, 0);

		if (shmid < 0) {
			return false;
		}

		// Attach and get the memory location for this segment.

		void* memptr = shmat(shmid, NULL, SHM_RDONLY);

		if (memptr == (void*)-1) {
			return false;
		}

		// Actually compute the crc32 for this segment.

		uLong segment_crc32 = crc32(g_crc32_init, memptr, (uInt)ios[i].segsz);
		shmdt(memptr);

		// Compare the crc32 value just computed with the one computed
		// while reading the file.

		if (segment_crc32 != io->crc32) {
			if (g_verbose) {
				printf("crc32-check failed for restored segment.\n");
			}

			return false;
		}
	}

	return true;
}

// Cleanup from restore_candidate().
// If remove is set, all segments created should be removed.

static void
restore_candidate_cleanup(as_io_t ios[], uint32_t n_ios, bool remove_segments)
{
	// Close all opened files.

	for (uint32_t i = 0; i < n_ios; i++) {
		as_io_t *io = &ios[i];

		// Close the file.

		close(io->fd);
	}

	// Detach all attached segments.

	for (uint32_t i = 0; i < n_ios; i++) {
		as_io_t *io = &ios[i];

		// Detach this segment.

		shmdt(io->memptr);
	}

	if (!remove_segments) {
		return;
	}

	// Destroy all created segments in case of failure.

	for (uint32_t i = 0; i < n_ios; i++) {
		as_io_t *io = &ios[i];
		struct shmid_ds ds; // Dummy.

		// Destroy this segment.

		shmctl(io->shmid, IPC_RMID, &ds);
	}
}

// Validate whether this is an Aerospike database segment file.

static bool
validate_file_name(const char* pathname, as_file_t* fp)
{
	char* new_ptr = strdup(basename((char*)pathname));
	assert(new_ptr != NULL);

	char* old_ptr = new_ptr;
	char* dot_ptr = strchr(new_ptr, '.');

	if (dot_ptr == NULL) {
		free(old_ptr);
		old_ptr = NULL;
		return false;
	}

	// Ensure that file extension is ".dat" or ".dat.gz".

	if ((strcmp(dot_ptr, FILE_EXTENSION) != 0)
			&& (strcmp(dot_ptr, FILE_EXTENSION_CMP) != 0)) {
		free(old_ptr);
		old_ptr = NULL;
		return false;
	}

	// Lop off the file extension.

	*dot_ptr = '\0';

	int len = (int)strlen(new_ptr);

	// Ensure filename is right length.

	if (len != 8) {
		free(old_ptr);
		old_ptr = NULL;
		return false;
	}

	// Extract the key from the filename.

	key_t key = 0;

	for (int i = 0; i < len; i++) {
		char c = *new_ptr++;

		key <<= 4;

		if (c >= '0' && c <= '9') {
			key += c - '0';
		}
		else if (c >= 'a' && c <= 'f') {
			key += c - 'a' + 10;
		}
		else if (c >= 'A' && c <= 'F') {
			key += c - 'A' + 10;
		}
		else {
			if (g_verbose) {
				printf("Segment file name \'%s\'"
						" contains invalid characters.\n", pathname);
			}

			free(old_ptr);
			old_ptr = NULL;
			return false;
		}
	}

	// Check whether the key is valid (primary or secondary).

	fp->key = key;

	bool primary = false;
	bool secondary = false;
	bool data = false;

	if ((key & AS_XMEM_KEY_TYPE_MASK) == AS_XMEM_PRI_KEY) {
		primary = true;
		key = key & ~AS_XMEM_PRI_KEY;
	}
	else if ((key & AS_XMEM_KEY_TYPE_MASK) == AS_XMEM_SEC_KEY) {
		secondary = true;
		key = key & ~AS_XMEM_SEC_KEY;
	}
	else if ((key & AS_XMEM_KEY_TYPE_MASK) == AS_XMEM_DAT_KEY) {
		data = true;
		key = key & ~AS_XMEM_DAT_KEY;
	}

	if (!primary && !secondary && !data) {
		// Not an Aerospike database file.
		free(old_ptr);
		old_ptr = NULL;
		return false;
	}

	fp->inst = (uint32_t)key >> (uint32_t)AS_XMEM_INSTANCE_KEY_SHIFT;

	if (fp->inst > MAX_INST) {
		// Note: Instance can be zero.
		free(old_ptr);
		old_ptr = NULL;
		return false;
	}

	// Determine namespace ID from key base.

	key = key & ~(0xf << AS_XMEM_INSTANCE_KEY_SHIFT);
	fp->nsid = (uint32_t)(key & (0xff << AS_XMEM_NS_KEY_SHIFT))
			>> AS_XMEM_NS_KEY_SHIFT;

	if (fp->nsid < MIN_NSID || fp->nsid > MAX_NSID) {
		// Invalid nsid.
		free(old_ptr);
		old_ptr = NULL;
		return false;
	}

	// Extract file type from key base.

	key = key & ~(0xff << AS_XMEM_NS_KEY_SHIFT);

	if (key >= AS_XMEM_ARENA_KEY) {
		if (primary) {
			fp->type = TYPE_PRI_STAGE;
		} else if (secondary) {
			fp->type = TYPE_SEC_STAGE;
		}
		else if (data) {
			fp->type = TYPE_DAT_STAGE;
		}
		else {
			// Not a valid Aerospike file type.
			free(old_ptr);
			old_ptr = NULL;
			return false;
		}
	}
	else if (key == AS_XMEM_TREEX_KEY) {
		if (primary) {
			fp->type = TYPE_TREEX;
		}
	}
	else if (key > 0) {
		if (data) {
			fp->type = TYPE_DAT_STAGE;
		}
		else {
			// Not a valid Aerospike file type.
			free(old_ptr);
			old_ptr = NULL;
			return false;
		}
	}
	else if (key == 0) {
		if (primary) {
			fp->type = TYPE_BASE;
		}
		else if (secondary) {
			fp->type = TYPE_META;
		}
		else if (data) {
			fp->type = TYPE_DAT_STAGE;
		}
		else {
			// Not a valid Aerospike file type.
			free(old_ptr);
			old_ptr = NULL;
			return false;
		}
	}
	else {
		// Not a valid Aerospike file type.
		free(old_ptr);
		old_ptr = NULL;
		return false;
	}

	// Extract stage number from key (if this is a stage).

	fp->stage =
			(fp->type == TYPE_PRI_STAGE ||
			 fp->type == TYPE_SEC_STAGE ||
			 fp->type == TYPE_DAT_STAGE) ?
					(uint32_t)key : 0;

	if ((fp->type == TYPE_PRI_STAGE || fp->type == TYPE_SEC_STAGE)
			&& (fp->stage < MIN_ARENA || fp->stage > MAX_ARENA)) {
		// Not a valid stage.
		free(old_ptr);
		old_ptr = NULL;
		return false;
	}

	// Done.

	free(old_ptr);
	old_ptr = NULL;

	return true;
}

// Generate a list of Aerospike database segment files.
// Note: *n_fps and *error are valid even if list_files() returns false.

static bool
list_files(as_file_t** fps, uint32_t* n_fps, int* error)
{
	*n_fps = 0;
	*error = 0;

	DIR* dir = opendir(g_pathdir);

	if (dir == NULL) {
		*error = errno;
		if (g_verbose) {
			char errbuff[MAX_BUFFER];
			char *errout = strerror_r(errno, errbuff, MAX_BUFFER);
			printf("Cannot open directory \'%s\': error was %d: %s.\n",
					g_pathdir, *error, errout);
		}

		return false;
	}

	// *fps is array of file structures for Aerospike database segment files.

	*fps = NULL; // Table is initially empty.

	as_file_t valid_file;
	struct dirent* dirent;

	while ((dirent = readdir(dir)) != NULL) {
		// Skip "." and ".." entries.

		if (strcmp(dirent->d_name, ".") == 0
				|| strcmp(dirent->d_name, "..") == 0) {
			continue;
		}

		// Validate the file name.

		if (!validate_file_name(dirent->d_name, &valid_file)) {
			continue;
		}

		char pathname[PATH_MAX + 1];
		struct stat statbuf;

		sprintf(pathname, "%s/%s", g_pathdir, dirent->d_name);

		// Get status of file.

		if (stat(pathname, &statbuf) < 0) {
			if (g_verbose) {
				char errbuff[MAX_BUFFER];
				char *errout = strerror_r(errno, errbuff, MAX_BUFFER);

				printf("Did not find info for Aerospike database file"
						" \'%s\': error was %d: %s.\n", pathname, errno,
						errout);
			}

			continue;
		}

		// Check whether the instance number is a match.

		if (g_inst != INV_INST && valid_file.inst != g_inst) {
			continue;
		}

		// Extract namespace name from file (if this is a base segment file).

		if (valid_file.type == TYPE_BASE) {
			int rc = open(pathname, O_RDONLY);

			if (rc < 0) {
				continue;
			}

			int fd = rc;

			if (lseek(fd, (off_t)BASE_NAMESPACE_OFF, SEEK_SET)
					!= (off_t)BASE_NAMESPACE_OFF) {
				close(fd);
				continue;
			}

			// Read the namespace name.

			char nsnm[NAMESPACE_LEN + 1];

			if (read(fd, (void*)nsnm, NAMESPACE_LEN) != NAMESPACE_LEN) {
				close(fd);
				continue;
			}

			close(fd);
			nsnm[NAMESPACE_LEN] = '\0';
			valid_file.nsnm = strdup(nsnm);
		}
		else if (valid_file.type == TYPE_DAT_STAGE) {
			if (is_file_compressed(valid_file.key)) {
				valid_file.nsnm = get_file_nsnm_compressed(valid_file.key);
			}
			else {
				valid_file.nsnm = get_file_nsnm(valid_file.key);
			}
		}
		else {
			valid_file.nsnm = NULL;
		}

		// Check whether the namespace name is a match.

		if (valid_file.type == TYPE_BASE && g_nsnm != NULL) {
			assert(valid_file.nsnm != NULL);

			if (strcmp(valid_file.nsnm, g_nsnm) != 0) {
				free(valid_file.nsnm);
				valid_file.nsnm = NULL;
				continue;
			}
		}

		// Extract segment size for compressed files.

		size_t segsz;
		bool compress;

		if (valid_file.type != TYPE_BASE && valid_file.type != TYPE_META) {
			char* dot_ptr = strchr(dirent->d_name, '.');

			// Is this a compressed file?

			if (dot_ptr != NULL && strcmp(dot_ptr, FILE_EXTENSION_CMP) == 0) {

				int rc = open(pathname, O_RDONLY);

				if (rc < 0) {
					assert(valid_file.nsnm == NULL);
					continue;
				}

				int fd = rc;

				// Read the compressed file header.

				if (lseek(fd, (off_t)CMPHDR_OFF, SEEK_SET)
						!= (off_t)CMPHDR_OFF) {
					close(fd);
					assert(valid_file.nsnm == NULL);
					continue;
				}

				as_cmp_t header;

				if (read(fd, (void*)&header, CMPHDR_LEN) != CMPHDR_LEN) {
					close(fd);
					assert(valid_file.nsnm == NULL);
					continue;
				}

				close(fd);

				// Sanity check header.

				if (header.magic != CMPHDR_MAG1
						&& header.magic != CMPHDR_MAG2) {
					assert(valid_file.nsnm == NULL);
					continue;
				}

				if (header.version != CMPHDR_VER) {
					assert(valid_file.nsnm == NULL);
					continue;
				}

				segsz = header.segsz;
				compress = true;
			}
			else {
				segsz = (size_t)statbuf.st_size;
				compress = false;
			}
		}
		else {
			segsz = (size_t)statbuf.st_size;
			compress = false;
		}

		// Found a matching file. Add to list.

		(*n_fps)++;

		// Allocate store to hold an entry in file table.

		*fps = realloc(*fps, (size_t)*n_fps * sizeof(as_file_t));
		assert(*fps != NULL);

		as_file_t* fp = *fps + *n_fps - 1;

		fp->key = valid_file.key;
		fp->nsnm = valid_file.nsnm;
		fp->uid = statbuf.st_uid;
		fp->gid = statbuf.st_gid;
		fp->mode = statbuf.st_mode;
		fp->filsz = (size_t)statbuf.st_size;
		fp->segsz = segsz;
		fp->compress = compress;
		fp->stage = valid_file.stage;
		fp->inst = valid_file.inst;
		fp->nsid = valid_file.nsid;
		fp->type = valid_file.type;
	}

	closedir(dir);

	// Sort table by key (important!)

	if (*n_fps > 0) {
		qsort((void*)*fps, (size_t)*n_fps, sizeof(as_file_t),
				qsort_compare_files);
	}

	return true;
}

// qsort(3) comparison routine for shared memory file table.

static int
qsort_compare_files(const void* left, const void* right)
{
	return (int)((uint32_t)((as_segment_t*)left)->key
			- (uint32_t)((as_segment_t*)right)->key);
}

// Draw a table passed in as a n_rows x n_cols array of NUL-terminated
// character strings. The strings will be freed before returning.

static void
draw_table(char** table, uint32_t n_rows, uint32_t n_cols)
{
	// Array of column widths, initially zero.

	uint32_t colwidth[n_cols];

	memset(&colwidth, 0, (size_t)n_cols * sizeof(uint32_t));

	// For each column, find maximum length item.

	for (uint32_t i = 0; i < n_rows; i++) {
		for (uint32_t j = 0; j < n_cols; j++) {
			size_t itemlen = strlen(*(table + i * n_cols + j));

			if (itemlen > colwidth[j]) {
				colwidth[j] = (uint32_t)itemlen;
			}
		}
	}

	// Print header row followed by line with dashes.

	for (uint32_t j = 0; j < n_cols; j++) {
		char* field = strfmt_width(*(table + 0 * n_cols + j), colwidth[j],
				NUM_BLANKS, false);

		printf("%s", field);
		free(field);
		field = NULL;
	}

	printf("\n");

	for (uint32_t j = 0; j < n_cols; j++) {
		char* field = strfmt_width(*(table + 0 * n_cols + j), colwidth[j],
				NUM_BLANKS, true);

		printf("%s", field);
		free(field);
		field = NULL;
	}

	printf("\n");

	// Print table body.

	for (uint32_t i = 1; i < n_rows; i++) {
		for (uint32_t j = 0; j < n_cols; j++) {
			char* field = strfmt_width(*(table + i * n_cols + j), colwidth[j],
					NUM_BLANKS, false);

			printf("%s", field);
			free(field);
			field = NULL;
		}

		printf("\n");
	}

	// Free table elements.

	for (uint32_t i = 0; i < n_rows; i++) {
		for (uint32_t j = 0; j < n_cols; j++) {
			free((void*)*(table + i * n_cols + j));
			*(table + i * n_cols + j) = NULL;
		}
	}
}

// Sets a NUL-terminated character string with a fixed length.
// "dashes" parameter used to substitute dashes for string characters.

static char*
strfmt_width(char* string, uint32_t width, uint32_t n_blanks, bool dashes)
{
	char* buffer = malloc(MAX_BUFFER);
	assert(buffer != NULL);

	uint32_t len = (uint32_t)strnlen(string, width);
	uint32_t n_chars;

	if (dashes) {
		n_chars = width;
		memset(buffer, '-', n_chars);
	}
	else {
		n_chars = len < width ? len : width;
		strncpy(buffer, string, n_chars);
	}

	// Pad with blanks.

	memset(buffer + n_chars, ' ', width - n_chars + n_blanks);
	buffer[width + n_blanks] = '\0';

	return buffer;
}

#define ONE_BILLION 1000000000

static char*
strtime_diff_eta(struct timespec* start, struct timespec* end, uint32_t decile)
{
	(void)decile;

	char outbuff[256];
	char* outptr = outbuff;
	char* retptr = NULL;

	outbuff[0] = '\0';

	// Rationalize start and end.

	while (end->tv_nsec < start->tv_nsec) {
		end->tv_nsec += ONE_BILLION;
		end->tv_sec--;
	}

	// Compute diff.

	struct timespec diff;

	diff.tv_sec = end->tv_sec - start->tv_sec;
	diff.tv_nsec = end->tv_nsec - start->tv_nsec;

	// Format diff as printable string.

	time_t hours;
	time_t minutes;
	time_t seconds;
	time_t tenths;

	gettime_hmst(&diff, &hours, &minutes, &seconds, &tenths);

	if (hours < 0 || minutes < 0 || seconds < 0 || tenths < 0) {
		sprintf(outptr, "<null>");
		retptr = strdup(outbuff);
		return retptr;
	}

	if (hours != 0) {
		outptr += sprintf(outptr, "%ldh:%ldm:%ld.%lds", hours, minutes, seconds,
				tenths);
	}
	else if (minutes != 0) {
		outptr += sprintf(outptr, "%ldm:%ld.%lds", minutes, seconds, tenths);
	}
	else {
		outptr += sprintf(outptr, "%ld.%lds", seconds, tenths);
	}

	// TODO: Fix the ETA. It's hopelessly broken now.

//	if (decile < 1 || decile >= 10) {
//		retptr = strdup(outbuff);
//		return retptr;
//	}
//
//	// Add the ETA.
//
//	*outptr++ = ' ';
//	*outptr = '\0';
//
//	// Compute ETA, given diff and decile.
//
//	struct timespec eta = diff;
//
//	eta.tv_sec = (time_t)(10.0 / (double)decile * (double)diff.tv_sec);
//	eta.tv_nsec = (time_t)(10.0 / (double)decile * (double)diff.tv_nsec);
//	gettime_hmst(&eta, &hours, &minutes, &seconds, &tenths);
//
//	if (hours < 0 || minutes < 0 || seconds < 0 || tenths < 0) {
//		sprintf(outptr, "<null>");
//		retptr = strdup(outbuff);
//		return retptr;
//	}
//
//	if (hours != 0) {
//		sprintf(outptr, "(ETA: %ldh:%ldm:%ld.%lds)", hours, minutes, seconds,
//				tenths);
//	}
//	else if (minutes != 0) {
//		sprintf(outptr, "(ETA: %ldm:%ld.%lds)", minutes, seconds, tenths);
//	}
//	else {
//		sprintf(outptr, "(ETA: %ld.%lds)", seconds, tenths);
//	}
//
	retptr = strdup(outbuff);

	return retptr;
}

static void
gettime_hmst(struct timespec* time, time_t* hours, time_t* minutes,
		time_t* seconds, time_t* tenths)
{
	while (time->tv_nsec > ONE_BILLION) {
		time->tv_nsec -= ONE_BILLION;
		time->tv_sec++;
	}

	// Extract hours, minutes, seconds, and tenths.

	*hours = time->tv_sec / 3600;
	time->tv_sec -= *hours * 3600;
	*minutes = time->tv_sec / 60;
	time->tv_sec -= *minutes * 60;
	*seconds = time->tv_sec;
	*tenths = time->tv_nsec / (ONE_BILLION / 10);
}
