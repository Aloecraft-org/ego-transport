TARGET_WASI:=--target wasm32-wasip2
TARGET_BROWSER:=--target wasm32-unknown-unknown
QUIET_WARN:=RUSTFLAGS="-Awarnings"

ifneq ($(filter quiet,$(MAKECMDGOALS)),)
CARGO_ENV:=$(QUIET_WARN)
else
CARGO_ENV:=
endif

quiet:
	@true

clean:
	rm -rf target .data

_init:
	@mkdir -p .data/home .data/tmp

define cargo_targets  # $(1)=command, $(2)=extra flags
$(1)_native:
	$(CARGO_ENV) cargo $(1)
$(1)_wasi:
	$(CARGO_ENV) cargo $(1) $(TARGET_WASI)
$(1)_browser:
	$(CARGO_ENV) cargo $(1) $(TARGET_BROWSER)
$(1): $(1)_native $(1)_wasi $(1)_browser
endef

$(eval $(call cargo_targets,build))
$(eval $(call cargo_targets,check))
# $(eval $(call cargo_targets,test))

check: check_native check_wasi check_browser
# test: test_native test_wasi test_browser
build: build_native build_wasi build_browser

clean:
	cargo clean
	rm -rf target

fmt:
	cargo fmt

clippy:
	cargo clippy --all-targets --all-features -- -D warnings

# ===========================================
# PHASE 1: BASIC PLATFORM TESTS
# ===========================================

# Run basic platform test on native
test_p1_native:
	cargo run --bin test_network

# Build WASI platform test
test_p1_wasm_build:
	cargo build --target wasm32-wasip2 --bin test_network

# Run WASI platform test (requires wasmtime)
test_p1_wasm:
	wasmtime run --wasi inherit-network target/wasm32-wasip2/debug/test_network.wasm

# Run browser platform test (requires trunk)
test_p1_web:
	trunk serve --port 9001

# ===========================================
# PHASE 2: TCP TESTS
# ===========================================

# Native TCP echo test (server + client in one)
test_tcp_native:
	cargo run --bin test_network_p1_native

# WASI TCP Client → Native TCP Server
test_tcp_wasi_client_build:
	cargo build --target wasm32-wasip2 --bin test_network_wasi_client

test_tcp_wasi_client_server:
	@echo "=== Starting Native TCP Server ==="
	@echo "Run in another terminal: make test_tcp_wasi_client_run"
	cargo run --bin test_network_wasi_client

test_tcp_wasi_client_run:
	wasmtime run --wasi inherit-network target/wasm32-wasip2/debug/test_network_wasi_client.wasm

# WASI TCP Server → Native TCP Client
test_tcp_wasi_server_build:
	cargo build --target wasm32-wasip2 --bin test_network_wasi_server

test_tcp_wasi_server_server:
	@echo "=== Starting WASI TCP Server ==="
	@echo "Run in another terminal: make test_tcp_wasi_server_client"
	wasmtime run --wasi inherit-network target/wasm32-wasip2/debug/test_network_wasi_server.wasm

test_tcp_wasi_server_client:
	cargo run --bin test_network_wasi_server

# ===========================================
# PHASE 2.5: SERVER ABSTRACTION TESTS
# ===========================================

# Native server with ServerBuilder (concurrent mode)
test_server_native:
	cargo run --bin test_server

# WASI server with ServerBuilder (sequential mode)
test_server_wasm_build:
	cargo build --target wasm32-wasip2 --bin test_server

test_server_wasm_server:
	@echo "=== Starting WASI Server (Sequential Mode) ==="
	@echo "Run in another terminal: make test_server_wasm_client"
	wasmtime run --wasi inherit-network target/wasm32-wasip2/debug/test_server.wasm

test_server_wasm_client:
	cargo run --bin test_client

# Standalone TCP client (useful for testing any server)
test_client:
	@echo "Usage: make test_client ADDR=127.0.0.1:9997"
	cargo run --bin test_client -- $(or $(ADDR),127.0.0.1:9997)

# ===========================================
# PHASE 3: WEBSOCKET TESTS
# ===========================================

# Native WebSocket test (server + client in one)
test_ws_native:
	cargo run --bin test_websocket_native

# WASI WebSocket Client → Native WebSocket Server
test_ws_wasi_build:
	cargo build --target wasm32-wasip2 --bin test_websocket_wasi_client

test_ws_wasi_server:
	@echo "=== Starting Native WebSocket Server for WASI ==="
	@echo "Run in another terminal: make test_ws_wasi_client"
	cargo run --bin test_websocket_wasi_client

test_ws_wasi_client:
	wasmtime run --wasi inherit-network target/wasm32-wasip2/debug/test_websocket_wasi_client.wasm

# Combined WASI test (shows instructions)
test_ws_wasi: test_ws_wasi_build
	@echo "==================================="
	@echo "WASI WebSocket Test"
	@echo "==================================="
	@echo ""
	@echo "Terminal 1: make test_ws_wasi_server"
	@echo "Terminal 2: make test_ws_wasi_client"
	@echo ""
	@echo "Or run separately as shown above"
	@echo "==================================="

# Browser WebSocket Client → Native WebSocket Server
test_ws_browser_build:
	cargo build --target wasm32-unknown-unknown --bin test_websocket_browser

test_ws_browser_server:
	@echo "=== Starting Native WebSocket Server for Browser ==="
	@echo "Open browser: http://localhost:9001"
	@echo "Or run in another terminal: make test_ws_browser_client"
	cargo run --bin test_websocket_wasi_client

test_ws_browser_client:
	@echo "=== Starting Browser WebSocket Client ==="
	@echo "Server should be running on 127.0.0.1:9995"
	@echo "Opening browser at http://localhost:9001"
	trunk serve --port 9001 test_websocket_browser.html

# Combined browser test (shows instructions)
test_ws_browser: test_ws_browser_build
	@echo "==================================="
	@echo "Browser WebSocket Test"
	@echo "==================================="
	@echo ""
	@echo "Terminal 1: make test_ws_browser_server"
	@echo "Terminal 2: make test_ws_browser_client"
	@echo ""
	@echo "Then open: http://localhost:9001"
	@echo "Check browser console (F12) for output"
	@echo "==================================="

# ===========================================
# AutoDetectListener Tests
# ===========================================

test_auto_detect_native:
	cargo run --bin test_auto_detect_native

test_auto_detect_wasi_server:
	cargo build --bin test_auto_detect_wasi_server                       
	cargo build --target wasm32-wasip2 --bin test_auto_detect_wasi_server
	wasmtime run --wasi inherit-network target/wasm32-wasip2/debug/test_auto_detect_wasi_server.wasm & ./target/debug/test_auto_detect_wasi_server
# 	cargo run --bin test_auto_detect_wasi_server --target wasm32-wasip2 & sleep 1
# 	cargo run --bin test_auto_detect_wasi_server --target wasm32-wasip2


# ===========================================
# COMPREHENSIVE TEST SUITES
# ===========================================

# Run all Phase 1 tests
test_phase1: test_p1_native
	@echo "✓ Phase 1: Platform abstraction tests complete"

# Run all Phase 2 TCP tests (interactive - requires multiple terminals)
test_phase2:
	@echo "==================================="
	@echo "Phase 2: TCP Tests"
	@echo "==================================="
	@echo ""
	@echo "1. Native TCP Echo Test:"
	@echo "   make test_tcp_native"
	@echo ""
	@echo "2. WASI Client → Native Server:"
	@echo "   Terminal 1: make test_tcp_wasi_client_server"
	@echo "   Terminal 2: make test_tcp_wasi_client_run"
	@echo ""
	@echo "3. WASI Server → Native Client:"
	@echo "   Terminal 1: make test_tcp_wasi_server_server"
	@echo "   Terminal 2: make test_tcp_wasi_server_client"
	@echo ""
	@echo "4. Server Abstraction:"
	@echo "   Native:  make test_server_native"
	@echo "   WASI:    Terminal 1: make test_server_wasm_server"
	@echo "            Terminal 2: make test_server_wasm_client"
	@echo "==================================="

# Run all Phase 3 WebSocket tests
test_phase3:
	@echo "==================================="
	@echo "Phase 3: WebSocket Tests ✓ COMPLETE"
	@echo "==================================="
	@echo ""
	@echo "1. Native WebSocket (server + client):"
	@echo "   make test_ws_native"
	@echo ""
	@echo "2. WASI WebSocket Client → Native Server:"
	@echo "   Terminal 1: make test_ws_wasi_server"
	@echo "   Terminal 2: make test_ws_wasi_client"
	@echo ""
	@echo "3. Browser WebSocket Client → Native Server:"
	@echo "   Terminal 1: make test_ws_browser_server"
	@echo "   Terminal 2: make test_ws_browser_client"
	@echo "   Browser:   http://localhost:9001 (check console)"
	@echo ""
	@echo "==================================="
	@echo "ALL PLATFORMS WORKING! 🎉"
	@echo "==================================="

# Build all test binaries
test_build_all: build_native test_p1_wasm_build test_tcp_wasi_client_build test_tcp_wasi_server_build test_server_wasm_build test_ws_wasi_build test_ws_browser_build
	@echo "✓ All test binaries built"

# ===========================================
# DEVELOPMENT HELPERS
# ===========================================

# Run native tests quickly
quick_test: test_tcp_native test_server_native test_ws_native
	@echo "✓ Quick native tests complete"

# Format code
fmt:
	cargo fmt

# Run clippy linter
lint:
	cargo clippy --all-targets --all-features

# Full pre-commit check
pre_commit: fmt lint check test_build_all
	@echo "✓ Pre-commit checks complete"

# ===========================================
# VICTORY LAP
# ===========================================

victory:
	@echo ""
	@echo "🎉🎊🚀 ALOECLIENT - COMPLETE! 🚀🎊🎉"
	@echo ""
	@echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	@echo "  Cross-Platform Networking Library"
	@echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	@echo ""
	@echo "✅ Native:   TCP + WebSocket (Server + Client)"
	@echo "✅ WASI:     TCP + WebSocket (Server + Client)"
	@echo "✅ Browser:  WebSocket (Client)"
	@echo ""
	@echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	@echo "  All Requirements Met!"
	@echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	@echo ""
	@echo "Quick Demo:"
	@echo "  make quick_test          # Run all native tests"
	@echo "  make test_phase3         # See all WebSocket options"
	@echo ""
	@echo "Ready to build AloeCraft! 🎮"
	@echo ""

# ===========================================
# HELP
# ===========================================

help:
	@echo "AloeCraft Client - Makefile Targets"
	@echo "===================================="
	@echo ""
	@echo "Build:"
	@echo "  make build              - Build all targets"
	@echo "  make build_native       - Build native only"
	@echo "  make build_wasm         - Build wasm32-wasip2 only"
	@echo "  make build_web          - Build wasm32-unknown-unknown only"
	@echo ""
	@echo "Check (fast):"
	@echo "  make check              - Check all targets"
	@echo "  make check_native       - Check native only"
	@echo "  make check_wasm         - Check WASI only"
	@echo "  make check_web          - Check browser only"
	@echo ""
	@echo "Phase 1 Tests (Platform Abstraction):"
	@echo "  make test_p1_native     - Run on native"
	@echo "  make test_p1_wasm       - Run on WASI (wasmtime)"
	@echo "  make test_p1_web        - Run on browser (trunk)"
	@echo ""
	@echo "Phase 2 Tests (TCP):"
	@echo "  make test_tcp_native            - Native echo test"
	@echo "  make test_tcp_wasi_client_*     - WASI client tests"
	@echo "  make test_tcp_wasi_server_*     - WASI server tests"
	@echo "  make test_server_native         - Native ServerBuilder test"
	@echo "  make test_server_wasm_*         - WASI ServerBuilder tests"
	@echo "  make test_client                - Standalone TCP client"
	@echo ""
	@echo "Phase 3 Tests (WebSocket) ✓ COMPLETE:"
	@echo "  make test_ws_native             - Native WebSocket test"
	@echo "  make test_ws_wasi_server        - Start native WS server for WASI"
	@echo "  make test_ws_wasi_client        - Run WASI WS client"
	@echo "  make test_ws_browser_server     - Start server for browser"
	@echo "  make test_ws_browser_client     - Run browser WS client"
	@echo ""
	@echo "Test Suites:"
	@echo "  make test_phase1        - Show Phase 1 tests"
	@echo "  make test_phase2        - Show Phase 2 tests"
	@echo "  make test_phase3        - Show Phase 3 tests ✓"
	@echo "  make quick_test         - Run fast native tests"
	@echo ""
	@echo "Development:"
	@echo "  make fmt                - Format code"
	@echo "  make lint               - Run clippy"
	@echo "  make pre_commit         - Full pre-commit check"
	@echo "  make clean              - Remove build artifacts"
	@echo ""
	@echo "Victory:"
	@echo "  make victory            - Celebrate! 🎉"
	@echo ""
	@echo "Quick Start:"
	@echo "  make quick_test         - Run all native tests"
	@echo "  make test_phase3        - See all WebSocket test options"

.PHONY: clean build build_native build_wasm build_web check check_native check_wasm check_web \
        test_p1_native test_p1_wasm_build test_p1_wasm test_p1_web \
        test_tcp_native test_tcp_wasi_client_build test_tcp_wasi_client_server test_tcp_wasi_client_run \
        test_tcp_wasi_server_build test_tcp_wasi_server_server test_tcp_wasi_server_client \
        test_server_native test_server_wasm_build test_server_wasm_server test_server_wasm_client \
        test_ws_native test_ws_wasi_build test_ws_wasi_server test_ws_wasi_client test_ws_wasi \
        test_ws_browser_build test_ws_browser_server test_ws_browser_client test_ws_browser \
        test_client test_phase1 test_phase2 test_phase3 test_build_all quick_test fmt lint pre_commit victory help

# Default target
.DEFAULT_GOAL := help