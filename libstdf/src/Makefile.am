noinst_HEADERS = dtc.h rec.h

# Internal macro to define internal libstdf stuff
AM_CFLAGS = -D__IN_LIBSTDF @DEBUG_CFLAGS@

INCLUDES = -I$(top_builddir)/include \
	@ZIP_CFLAGS@ @GZIP_CFLAGS@ @BZIP2_CFLAGS@

lib_LTLIBRARIES = libstdf.la

EXTRA_STDF_SOURCES :=
if HAVE_LZW
EXTRA_STDF_SOURCES += lzw.c
endif
libstdf_la_SOURCES = \
	libstdf.c \
	dtc.c \
	rec.c \
	$(EXTRA_STDF_SOURCES)
libstdf_la_LDFLAGS = -version-info 0:1:0

libstdf_la_LIBADD = @ZIP_LIBS@ @GZIP_LIBS@ @BZIP2_LIBS@ @DEBUG_LIBS@
