EXTENSION_NAME = parallel
HERD_ROOT      = $(HOME)/Library/Application Support/Herd

# Detecta versão do PHP no PATH (ex: 84)
PHP_VERSION_SHORT ?= $(shell php -r 'echo PHP_MAJOR_VERSION . PHP_MINOR_VERSION;')

HERD_PHP_BIN    = $(HERD_ROOT)/bin/php$(PHP_VERSION_SHORT)
HERD_CONFIG_DIR = $(HERD_ROOT)/config/php/$(PHP_VERSION_SHORT)
HERD_PHP_INI    = $(HERD_CONFIG_DIR)/php.ini
HERD_EXT_DIR    = $(HERD_CONFIG_DIR)/extensions

CARGO_ARTIFACT  = target/release/lib$(EXTENSION_NAME).dylib
TARGET_SO       = $(HERD_EXT_DIR)/$(EXTENSION_NAME).so

.PHONY: build install-herd uninstall-herd clean

build:
	@test -x "$(HERD_PHP_BIN)" || (echo "❌ $(HERD_PHP_BIN) não encontrado"; exit 1)
	HERD_PHP_BIN="$(HERD_PHP_BIN)" \
	PATH="$(CURDIR)/.herd-shim:$$PATH" \
	cargo build --release

install-herd: build
	@mkdir -p "$(HERD_EXT_DIR)"
	@cp "$(CARGO_ARTIFACT)" "$(TARGET_SO)"
	@echo "✅ Instalado: $(TARGET_SO)"
	@if ! grep -qE "^extension_dir.*extensions" "$(HERD_PHP_INI)" 2>/dev/null || \
	    ! grep -qE "^extension[[:space:]]*=[[:space:]]*$(EXTENSION_NAME)" "$(HERD_PHP_INI)" 2>/dev/null; then \
		echo ""; \
		echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"; \
		echo "📝 Adicione ao php.ini:"; \
		echo "   $(HERD_PHP_INI)"; \
		echo ""; \
		echo "   extension_dir = \"$(HERD_EXT_DIR)\""; \
		echo "   extension=$(EXTENSION_NAME)"; \
		echo ""; \
		echo "   Reinicie o Herd depois."; \
		echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"; \
	else \
		echo "✅ php.ini já configurado."; \
	fi

uninstall-herd:
	@rm -f "$(TARGET_SO)"
	@echo "🗑  Removido: $(TARGET_SO)"

clean:
	cargo clean

switch-php: clean install-herd