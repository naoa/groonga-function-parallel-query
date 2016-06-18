AM_CFLAGS =					\
	$(GROONGA_CFLAGS)

AM_LDFLAGS =					\
	-avoid-version				\
	-module					\
	-no-undefined

LIBS =						\
	$(GROONGA_LIBS)

functions_plugins_LTLIBRARIES =
functions_plugins_LTLIBRARIES += parallel_query.la

functions_plugins_SOURCE =			\
	parallel_query.c
