PHP_ARG_ENABLE([parallel],
  [whether to enable parallel support],
  [AS_HELP_STRING([--enable-parallel], [Enable parallel])],
  [no])

if test "$PHP_PARALLEL" != "no"; then
  AC_PATH_PROG(CARGO, cargo, no)
  if test "$CARGO" = "no"; then
    AC_MSG_ERROR([cargo not found. Install Rust via rustup.])
  fi

  PHP_NEW_EXTENSION(parallel, , $ext_shared)
  PHP_ADD_MAKEFILE_FRAGMENT([$ext_srcdir/pie/Makefile.frag], [$ext_srcdir/..])
fi