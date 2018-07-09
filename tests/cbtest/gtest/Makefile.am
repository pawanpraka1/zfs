include $(top_srcdir)/config/Rules.am

AM_CPPFLAGS += $(DEBUG_STACKFLAGS)

DEFAULT_INCLUDES += \
	-I$(top_srcdir)/include \
	-I$(top_srcdir)/lib/libspl/include

ZFS_LIBS = \
	$(top_builddir)/lib/libnvpair/libnvpair.la \
	$(top_builddir)/lib/libuutil/libuutil.la \
	$(top_builddir)/lib/libzpool/libzpool.la \
	$(top_builddir)/lib/libzrepl/libzrepl.la \
	$(top_builddir)/lib/libzfs/libzfs.la \
	$(top_builddir)/lib/libzfs_core/libzfs_core.la

sbin_PROGRAMS = test_uzfsserver test_uzfs test_zfs test_zrepl_prot test_vdev

test_uzfsserver_SOURCES = test_uzfsserver.cc
test_uzfs_SOURCES = test_uzfs.cc gtest_utils.cc
test_zrepl_prot_SOURCES = test_zrepl_prot.cc gtest_utils.cc
test_zfs_SOURCES = test_zfs.cc gtest_utils.cc
test_vdev_SOURCES = test_vdev.cc gtest_utils.cc

test_uzfs_LDADD = $(ZFS_LIBS)
test_uzfsserver_LDADD = $(ZFS_LIBS)
test_vdev_LDADD = $(ZFS_LIBS)

test_uzfsserver_CXXFLAGS = -std=c++11
test_uzfsserver_LDFLAGS = -pthread -lgtest -lgtest_main
test_uzfs_LDFLAGS = -pthread -lgtest -lgtest_main
test_zrepl_prot_LDFLAGS = -pthread -lgtest -lgtest_main
test_zfs_LDFLAGS = -pthread -lgtest -lgtest_main
test_vdev_LDFLAGS = -pthread -lgtest -lgtest_main
