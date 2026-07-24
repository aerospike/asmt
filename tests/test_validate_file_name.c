/*
 * test_validate_file_name.c
 *
 * Copyright (C) 2026 Aerospike, Inc.
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
// Unit tests for validate_file_name().
//
// validate_file_name() is static and parses a segment file name (an 8-hex-digit
// System V shared-memory key plus a ".dat"/".dat.gz" extension) into an
// as_file_t, deciding the segment's type from the key. It touches no shared
// memory or file system, so it can be exercised directly. We reach the static
// function by including the translation unit and renaming its main().
//
// The key layout (8 hex digits) is: CC I NN KKK
//   CC  - class byte:  ae = primary, a2 = secondary, ad = data
//   I   - instance nibble (0..15)
//   NN  - namespace id  (0x01..0x20)
//   KKK - key base:     0 = base/meta, 1 = treex, >= 0x100 = arena stage
//
// The regression this guards (asmt PR #38 / #40): a data segment whose key base
// equals AS_XMEM_TREEX_KEY (1) used to fall through without a type being set,
// inheriting a stale TYPE_TREEX and producing "Missing treex segment file" on
// restore. It must now resolve to TYPE_DAT_STAGE.
//

int asmt_main_unused(int argc, char* argv[]);
#define main asmt_main_unused
#include "asmt.c"
#undef main

#include <stdio.h>
#include <string.h>

static int g_checks = 0;
static int g_failures = 0;

// basename() may modify its argument, so hand validate_file_name() a mutable
// copy rather than a string literal.
static bool
run(const char* file_name, as_file_t* fp)
{
	char buf[64];
	snprintf(buf, sizeof(buf), "%s", file_name);
	memset(fp, 0xEE, sizeof(*fp)); // poison, so an unset type would be caught
	return validate_file_name(buf, fp);
}

static void
expect_valid(const char* file_name, as_type want_type, uint32_t want_stage,
		uint32_t want_inst, const char* desc)
{
	as_file_t fp;
	g_checks++;

	if (! run(file_name, &fp)) {
		printf("FAIL  %-28s %s: expected valid, was rejected\n", file_name, desc);
		g_failures++;
		return;
	}

	if (fp.type != want_type) {
		printf("FAIL  %-28s %s: type=%d, expected %d\n", file_name, desc,
				(int)fp.type, (int)want_type);
		g_failures++;
		return;
	}

	if (fp.stage != want_stage) {
		printf("FAIL  %-28s %s: stage=0x%x, expected 0x%x\n", file_name, desc,
				fp.stage, want_stage);
		g_failures++;
		return;
	}

	if (fp.inst != want_inst) {
		printf("FAIL  %-28s %s: inst=%u, expected %u\n", file_name, desc,
				fp.inst, want_inst);
		g_failures++;
		return;
	}

	printf("ok    %-28s %s\n", file_name, desc);
}

static void
expect_invalid(const char* file_name, const char* desc)
{
	as_file_t fp;
	g_checks++;

	if (run(file_name, &fp)) {
		printf("FAIL  %-28s %s: expected rejected, was accepted (type=%d)\n",
				file_name, desc, (int)fp.type);
		g_failures++;
		return;
	}

	printf("ok    %-28s %s\n", file_name, desc);
}

int
main(void)
{
	printf("validate_file_name() tests\n");

	// Valid segments, instance 0, namespace id 1.
	expect_valid("ae001000.dat",    TYPE_BASE,      0,     0, "primary base");
	expect_valid("ae001001.dat",    TYPE_TREEX,     0,     0, "primary treex");
	expect_valid("ae001100.dat",    TYPE_PRI_STAGE, 0x100, 0, "primary arena stage");
	expect_valid("a2001000.dat",    TYPE_META,      0,     0, "secondary meta");
	expect_valid("a2001100.dat",    TYPE_SEC_STAGE, 0x100, 0, "secondary arena stage");
	expect_valid("ad001000.dat",    TYPE_DAT_STAGE, 0,     0, "data stage, key base 0");
	expect_valid("ad001002.dat",    TYPE_DAT_STAGE, 2,     0, "data stage, key base 2");
	expect_valid("ad001100.dat",    TYPE_DAT_STAGE, 0x100, 0, "data stage, arena-range key");

	// The regression: data segment whose key base == AS_XMEM_TREEX_KEY (1).
	expect_valid("ad001001.dat",    TYPE_DAT_STAGE, 1,     0, "data stage, key base 1 (regression)");

	// Non-zero instance nibble (including MAX_INST, 15).
	expect_valid("ae101001.dat",    TYPE_TREEX,     0,     1,  "primary treex, instance 1");
	expect_valid("a2301100.dat",    TYPE_SEC_STAGE, 0x100, 3,  "secondary arena stage, instance 3");
	expect_valid("adf01001.dat",    TYPE_DAT_STAGE, 1,     15, "data stage, key base 1, instance 15");

	// Compressed extension must be accepted too.
	expect_valid("ae001100.dat.gz", TYPE_PRI_STAGE, 0x100, 0, "primary arena stage, .gz");

	// Uppercase hex is accepted.
	expect_valid("AE001001.dat",    TYPE_TREEX,     0,     0, "primary treex, uppercase");

	// Invalid: recognized class but unrecognized key base.
	expect_invalid("ae001002.dat", "primary, key base 2 (not base/treex/arena)");
	expect_invalid("a2001001.dat", "secondary, key base 1 (not meta/arena)");

	// Invalid: primary arena stage out of range (> MAX_ARENA 0x8ff).
	expect_invalid("ae001900.dat", "primary arena stage out of range");

	// Invalid: not an Aerospike class byte.
	expect_invalid("12001000.dat", "non-Aerospike class byte");

	// Invalid: namespace id out of range.
	expect_invalid("ae000000.dat", "nsid 0 (< MIN_NSID)");
	expect_invalid("ae021000.dat", "nsid 33 (> MAX_NSID)");

	// Invalid: malformed names.
	expect_invalid("ae0010.dat",   "wrong length");
	expect_invalid("ae00100g.dat", "non-hex character");
	expect_invalid("ae001000.txt", "wrong extension");
	expect_invalid("ae001000",     "no extension");

	printf("\n%d/%d checks passed\n", g_checks - g_failures, g_checks);

	return g_failures == 0 ? 0 : 1;
}
