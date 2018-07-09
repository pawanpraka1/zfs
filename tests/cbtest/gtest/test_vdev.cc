/*
 * CDDL HEADER START
 *
 * The contents of this file are subject to the terms of the
 * Common Development and Distribution License (the "License").
 * You may not use this file except in compliance with the License.
 *
 * You can obtain a copy of the license at usr/src/OPENSOLARIS.LICENSE
 * or http://www.opensolaris.org/os/licensing.
 * See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * When distributing Covered Code, include this CDDL HEADER in each
 * file and include the License file at usr/src/OPENSOLARIS.LICENSE.
 * If applicable, add the following below this CDDL HEADER, with the
 * fields enclosed by brackets "[]" replaced with your own identifying
 * information: Portions Copyright [yyyy] [name of copyright owner]
 *
 * CDDL HEADER END
 */

/*
 * Copyright (c) 2018 CloudByte, Inc. All rights reserved.
 */

#include <gtest/gtest.h>
#include <stdio.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/types.h>
#include <linux/kdev_t.h>

#include <sys/vdev_impl.h>
#include <sys/spa_impl.h>
#include <uzfs_mgmt.h>

#include "gtest_utils.h"

#define	POOL_SIZE	(100 * 1024 * 1024)
#define	IO_SIZE		1024
#define	DISK_FILE	"/tmp/disk"
#define	DISK_DEV	"/dev/fake-disk"

using namespace GtestUtils;

vdev_ops_t *ops = &vdev_disk_ops;

class FakeVdev {
public:
	FakeVdev(std::string path) {
		bzero(&m_spa, sizeof (struct spa));
		m_spa.spa_mode = (FREAD|FWRITE);
		bzero(&m_vdev, sizeof (vdev_t));
		m_vdev.vdev_path = strdup(path.c_str());
		m_vdev.vdev_spa = &m_spa;
		m_vdev.vdev_ops = ops;
		m_vdev.vdev_state = VDEV_STATE_HEALTHY;
		m_opened = false;
	}

	~FakeVdev() {
		if (m_opened) {
			ops->vdev_op_close(&m_vdev);
			m_opened = false;
		}
		free(m_vdev.vdev_path);
	}

	vdev_t *get() {
		return (&m_vdev);
	}

	int open() {
		uint64_t psize = 0, max_psize = 0, ashift = 0;
		int rc;

		rc = ops->vdev_op_open(&m_vdev, &psize, &max_psize, &ashift);
		if (rc == 0) {
			m_opened = true;
		}
		return (rc);
	}

	void close() {
		ops->vdev_op_close(&m_vdev);
		m_opened = false;
	}

private:
	vdev_t m_vdev;
	struct spa m_spa;
	bool m_opened;
};

class FakeZio {
public:
	FakeZio(vdev_t *vdev, zio_type_t type, int cmd, int size) {
		m_finished = false;
		bzero(&m_zio, sizeof (zio_t));
		m_zio.io_type = type;
		m_zio.io_vd = vdev;
		m_zio.io_cmd = cmd;
		m_zio.io_size = size;
		m_zio.io_private = this;
		if (size > 0)
			m_zio.io_abd = abd_alloc_linear(size, B_FALSE);
	}

	~FakeZio() {
		if (m_zio.io_abd != NULL)
			abd_free(m_zio.io_abd);
	}

	zio_t *get() {
		return (&m_zio);
	}

	void done() {
		m_finished = true;
	}

	bool isDone() {
		return (m_finished);
	}
private:
	zio_t m_zio;
	bool m_finished;
};

/* Pointer updated from stub every time when zio finishes */
zio_t *finished_zio;
pthread_mutex_t finished_mtx;
pthread_cond_t finished_cv;

void
finish_zio(zio_t *zio)
{
	pthread_mutex_lock(&finished_mtx);
	finished_zio = zio;
	((FakeZio *)zio->io_private)->done();
	pthread_cond_broadcast(&finished_cv);
	pthread_mutex_unlock(&finished_mtx);
}

zio_t *
wait_for_zio()
{
	zio_t *zio;

	pthread_mutex_lock(&finished_mtx);
	while (finished_zio == NULL) {
		pthread_cond_wait(&finished_cv, &finished_mtx);
	}
	zio = finished_zio;
	finished_zio = NULL;
	pthread_mutex_unlock(&finished_mtx);

	return (zio);
}

/*
 * These two stub functions are called from vdev backend instead of real zfs
 * functions zio_execute and zio_interrupt. Why? See makefile.
 */
extern "C" {

void
mocked_zio_interrupt(zio_t *zio)
{
	finish_zio(zio);
}

void
mocked_zio_execute(zio_t *zio)
{
	finish_zio(zio);
}

// Grouping of IOs in vdev aio vdev backend
extern int aio_queue_high_wm;
extern int poll_sleep;

}	/* extern "C" */

TEST(Vdev, OpenEnoent) {
	uint64_t psize, max_psize, ashift;
	FakeVdev fakeVdev("/dev/doesnotexist");
	int rc;

	rc = ops->vdev_op_open(fakeVdev.get(), &psize, &max_psize, &ashift);
	EXPECT_EQ(rc, ENOENT);
	EXPECT_EQ(fakeVdev.get()->vdev_stat.vs_aux, VDEV_AUX_OPEN_FAILED);
}

TEST(Vdev, OpenAndClose) {
	uint64_t psize = 0, max_psize = 0, ashift = 0;
	FakeVdev fakeVdev(DISK_DEV);
	int rc;

	rc = ops->vdev_op_open(fakeVdev.get(), &psize, &max_psize, &ashift);
	ASSERT_EQ(rc, 0);
	EXPECT_EQ(psize, POOL_SIZE);
	EXPECT_EQ(max_psize, POOL_SIZE);
	EXPECT_GE(ashift, 8);	// block size at least 512
	ops->vdev_op_close(fakeVdev.get());
}

TEST(Vdev, WrongIoctl) {
	FakeVdev fakeVdev(DISK_DEV);
	FakeZio fakeZio(fakeVdev.get(), ZIO_TYPE_IOCTL, DKIOCGVTOC, 0);
	int rc;

	ASSERT_EQ(fakeVdev.open(), 0);
	ops->vdev_op_io_start(fakeZio.get());
	ASSERT_EQ(wait_for_zio(), fakeZio.get());
	EXPECT_EQ(fakeZio.get()->io_error, ENOTSUP);
}

TEST(Vdev, FlushIoctl) {
	FakeVdev fakeVdev(DISK_DEV);
	FakeZio fakeZio(fakeVdev.get(), ZIO_TYPE_IOCTL, DKIOCFLUSHWRITECACHE, 0);
	int rc;

	ASSERT_EQ(fakeVdev.open(), 0);
	ops->vdev_op_io_start(fakeZio.get());
	ASSERT_EQ(wait_for_zio(), fakeZio.get());
	EXPECT_EQ(fakeZio.get()->io_error, 0);
}

TEST(Vdev, WriteRead) {
	FakeVdev fakeVdev(DISK_DEV);
	FakeZio writeZio(fakeVdev.get(), ZIO_TYPE_WRITE, 0, IO_SIZE);
	FakeZio readZio(fakeVdev.get(), ZIO_TYPE_READ, 0, IO_SIZE);
	uint64_t offset = 2048;
	void *buf;
	int rc;

	ASSERT_EQ(fakeVdev.open(), 0);

	buf = abd_to_buf(writeZio.get()->io_abd);
	init_buf(buf, IO_SIZE, "OpenEBS");
	writeZio.get()->io_offset = offset;
	ops->vdev_op_io_start(writeZio.get());
	ASSERT_EQ(wait_for_zio(), writeZio.get());
	ASSERT_EQ(writeZio.get()->io_error, 0);

	readZio.get()->io_offset = offset;
	ops->vdev_op_io_start(readZio.get());
	ASSERT_EQ(wait_for_zio(), readZio.get());
	ASSERT_EQ(readZio.get()->io_error, 0);
	buf = abd_to_buf(readZio.get()->io_abd);
	rc = verify_buf(buf, IO_SIZE, "OpenEBS");
	EXPECT_EQ(rc, 0);
}

TEST(Vdev, WritePastEnd) {
	FakeVdev fakeVdev(DISK_DEV);
	FakeZio writeZio(fakeVdev.get(), ZIO_TYPE_WRITE, 0, IO_SIZE);

	ASSERT_EQ(fakeVdev.open(), 0);
	writeZio.get()->io_offset = POOL_SIZE;
	ops->vdev_op_io_start(writeZio.get());
	ASSERT_EQ(wait_for_zio(), writeZio.get());
	EXPECT_EQ(writeZio.get()->io_error, ENOSPC);
}

TEST(Vdev, ReadPastEnd) {
	FakeVdev fakeVdev(DISK_DEV);
	FakeZio readZio(fakeVdev.get(), ZIO_TYPE_READ, 0, IO_SIZE);

	ASSERT_EQ(fakeVdev.open(), 0);
	readZio.get()->io_offset = POOL_SIZE;
	ops->vdev_op_io_start(readZio.get());
	ASSERT_EQ(wait_for_zio(), readZio.get());
	EXPECT_EQ(readZio.get()->io_error, ENOSPC);
}

#define	OVERRIDE_COUNT	500

TEST(Vdev, BurstWrite) {
	FakeVdev fakeVdev(DISK_DEV);
	std::vector<FakeZio *> zios(OVERRIDE_COUNT);
	int i;

	ASSERT_EQ(fakeVdev.open(), 0);
	// no grouping of IOs
	aio_queue_high_wm = 1;

	// try to overwhelm vdev's input
	for (i = 0; i < OVERRIDE_COUNT; i++) {
		zios[i] = new FakeZio(fakeVdev.get(), ZIO_TYPE_WRITE, 0, IO_SIZE);
		zios[i]->get()->io_offset = 0;
		ops->vdev_op_io_start(zios[i]->get());
	}

	// wait for all IOs to finish before we continue
	do {
		usleep(100 * 1000);
		for (i = 0; i < OVERRIDE_COUNT; i++) {
			if (!zios[i]->isDone())
				break;
		}
	} while (i != OVERRIDE_COUNT);

	// check if any IO failed with EBUSY (it should)
	for (i = 0; i < OVERRIDE_COUNT; i++) {
		if (zios[i]->get()->io_error != 0) {
			EXPECT_EQ(zios[i]->get()->io_error, EBUSY);
			break;
		}
	}
	EXPECT_LT(i, OVERRIDE_COUNT);

	for (i = 0; i < OVERRIDE_COUNT; i++) {
		delete zios[i];
	}
}

#define	BATCH_COUNT	5
#define SLEEP_INTERVAL	(500 * 1000 * 1000)	// 1/2 second

TEST(Vdev, BurstWriteWithIOGrouping) {
	FakeVdev fakeVdev(DISK_DEV);
	FakeZio flushZio(fakeVdev.get(), ZIO_TYPE_IOCTL, DKIOCFLUSHWRITECACHE, 0);
	std::vector<FakeZio *> zios(OVERRIDE_COUNT);
	int i;

	// use batches of this many IOs
	aio_queue_high_wm = BATCH_COUNT;
	poll_sleep = SLEEP_INTERVAL;
	ASSERT_EQ(fakeVdev.open(), 0);

	// try to overwhelm vdev's input
	for (i = 0; i < 2 * BATCH_COUNT; i++) {
		zios[i] = new FakeZio(fakeVdev.get(), ZIO_TYPE_WRITE, 0, IO_SIZE);
		zios[i]->get()->io_offset = 0;
		ops->vdev_op_io_start(zios[i]->get());
	}

	// wait for at least one IO to fail with EBUSY
	do {
		for (i = 0; i < 2 * BATCH_COUNT; i++) {
			if (zios[i]->isDone() && zios[i]->get()->io_error == EBUSY)
				break;
		}
		usleep(1000);
	} while (i < 2 * BATCH_COUNT);

	// flush the disk cache with side effect of pushing all queued IOs
	// down the pipe
	ops->vdev_op_io_start(flushZio.get());
	while (!flushZio.isDone()) {
		usleep(1000);
	}
	EXPECT_EQ(flushZio.get()->io_error, 0);

	// flush notifies IO submitter thread and returns so IOs may still
	// not be finished, but they should finish relatively soon
	// (< SLEEP_INTERVAL)
	usleep(SLEEP_INTERVAL / 1000);

	// check that all IOs have been completed (with or without error)
	for (i = 0; i < 2 * BATCH_COUNT; i++) {
		ASSERT_TRUE(zios[i]->isDone());
		delete zios[i];
	}
}

void
cleanup()
{
	try {
		execStr("losetup -d " DISK_DEV);
	} catch (std::runtime_error &) {
		// just a best effort
	}
	unlink(DISK_DEV);
	unlink(DISK_FILE);
}

int
main(int argc, char** argv)
{
	dev_t dev = MKDEV(7, 201);
	struct stat st;
	int rc;
	int fd;

	::testing::InitGoogleTest(&argc, argv);

	pthread_cond_init(&finished_cv, NULL);
	pthread_mutex_init(&finished_mtx, NULL);
	if (uzfs_init() != 0) {
		fprintf(stderr, "Failed to initialize uzfs\n");
		return (-1);
	}

	fd = open(DISK_FILE, O_RDWR|O_CREAT, S_IRUSR|S_IWUSR);
	if (fd < 0) {
		perror("Cannot create disk file");
		return (-1);
	}
	close(fd);
	rc = truncate(DISK_FILE, POOL_SIZE);
	if (rc != 0) {
		perror("Cannot truncate disk file");
		return (-1);
	}
	if (stat(DISK_DEV, &st) == 0) {
		// Clean up after previous unclean shutdown
		cleanup();
	}
	rc = mknod(DISK_DEV, S_IFBLK|S_IRWXU|S_IRWXG|S_IROTH, dev);
	if (rc != 0) {
		perror("Cannot create device node");
		return (-1);
	}
	execStr("losetup " DISK_DEV " " DISK_FILE);

	/* Prepare a block device for testing */
	rc = RUN_ALL_TESTS();

	cleanup();
	pthread_cond_destroy(&finished_cv);
	pthread_mutex_destroy(&finished_mtx);
	return (rc);
}
