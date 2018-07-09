AC_DEFUN([ZFS_AC_CONFIG_USER_MOCK], [
	AC_ARG_ENABLE(mock,
		AC_HELP_STRING([--enable-mock],
		[enable test mocking for unit testing [[default: no]]]),
		[],enable_mock=no)

	AS_IF([test "x$enable_mock" = xyes],
	[
		AC_DEFINE(MOCK_ENABLE, 1,
			[enable mocks in the code needed for some unit tests])
	])
])
