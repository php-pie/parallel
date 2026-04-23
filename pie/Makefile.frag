### Makefile fragment for extension parallel (Rust + ext-php-rs)
###
### $(PHP_EXT_SRCDIR) points to ./pie/ inside the extracted package.
### The Cargo project lives one directory above, so we build from $(CARGO_SRCDIR).
PHP_CONFIG_PATH=$(which php)
CARGO_SRCDIR = $(PHP_EXT_SRCDIR)/..
CARGO_TARGET_DIR = $(CARGO_SRCDIR)/target

# Detect host OS to pick the correct cdylib extension.
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
	CARGO_LIB_EXT = dylib
else
	CARGO_LIB_EXT = so
endif

CARGO_ARTIFACT = $(CARGO_TARGET_DIR)/release/libparallel.$(CARGO_LIB_EXT)
EXT_ARTIFACT   = $(phplibdir)/parallel.so

all: cargo_build

cargo_build:
	@echo "Building Rust extension with cargo..."
	cd $(CARGO_SRCDIR) && $(CARGO) build --release
	@mkdir -p $(phplibdir)
	cp $(CARGO_ARTIFACT) $(EXT_ARTIFACT)
	@echo "Built: $(EXT_ARTIFACT)"

install-modules: cargo_build

clean: cargo_clean

cargo_clean:
	cd $(CARGO_SRCDIR) && $(CARGO) clean

.PHONY: all cargo_build cargo_clean install-modules