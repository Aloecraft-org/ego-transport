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
	cargo clean
	rm -rf .data

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
$(eval $(call cargo_targets,test))

fmt:
	cargo fmt

fmt_check:
	cargo fmt --all -- --check

clippy:
	cargo clippy --all-targets --all-features -- -D warnings
	cargo clippy --all-targets $(TARGET_WASI) -- -D warnings
	cargo clippy --all-targets $(TARGET_BROWSER) -- -D warnings

doc:
	cargo doc --no-deps --open

all: check test build

ci: fmt_check clippy check test

# ============================================================================
# STANDALONE TESTS
#
# Self-contained binaries: start servers and clients in the same process.
# No external dependencies. Run them all with `make test_standalone`.
# Each test has a timeout to prevent hanging.
# ============================================================================

test_SA_tcp_echo:
	@echo "━━━ TCP Echo (native) ━━━"
	@timeout 30 $(CARGO_ENV) cargo run --bin test_network_p1_native || \
		(echo "⏱️  TIMEOUT or FAIL: test_network_p1_native" && false)

test_SA_server_builder:
	@echo "━━━ ServerBuilder (native concurrent) ━━━"
	@timeout 30 $(CARGO_ENV) cargo run --bin test_server || \
		(echo "⏱️  TIMEOUT or FAIL: test_server" && false)

test_SA_ws_echo:
	@echo "━━━ WebSocket Echo (native) ━━━"
	@timeout 30 $(CARGO_ENV) cargo run --bin test_websocket_native || \
		(echo "⏱️  TIMEOUT or FAIL: test_websocket_native" && false)

test_SA_signaling:
	@echo "━━━ Signaling Protocol ━━━"
	@echo "⚠️  Known issue: may hang"
	@timeout 30 $(CARGO_ENV) cargo run --bin test_signaling || \
		(echo "⏱️  TIMEOUT or FAIL: test_signaling" && false)

test_SA_p2p:
	@echo "━━━ P2P Native-to-Native ━━━"
	@echo "⚠️  Known issue: may hang"
	@timeout 60 $(CARGO_ENV) cargo run --bin test_p2p || \
		(echo "⏱️  TIMEOUT or FAIL: test_p2p" && false)

test_SA_embedded_signaling:
	@echo "━━━ Embedded Signaling ━━━"
	@echo "⚠️  Known issue: may hang on WS echo test (Test 3)"
	@timeout 30 $(CARGO_ENV) cargo run --bin test_embedded_signaling || \
		(echo "⏱️  TIMEOUT or FAIL: test_embedded_signaling" && false)

test_SA_routed_signaling:
	@echo "━━━ Routed Signaling ━━━"
	@echo "⚠️  Known issue: may hang (relay forwarding)"
	@timeout 30 $(CARGO_ENV) cargo run --bin test_routed_signaling || \
		(echo "⏱️  TIMEOUT or FAIL: test_routed_signaling" && false)

test_SA_multihop_signaling:
	@echo "━━━ Multi-Hop Signaling ━━━"
	@echo "⚠️  Known issue: may hang (multi-hop relay)"
	@timeout 30 $(CARGO_ENV) cargo run --bin test_multihop_signaling || \
		(echo "⏱️  TIMEOUT or FAIL: test_multihop_signaling" && false)

# The reliable standalone tests, run in sequence (fails on first failure).
# The known-issue tests above (signaling/p2p, which can hang) are excluded;
# run them individually with their test_SA_* targets.
test_standalone: test_SA_tcp_echo test_SA_server_builder test_SA_ws_echo
	@echo "✅ Standalone tests passed"

# ============================================================================
# FIXTURE-BASED TESTS
#
# These require starting a server first, then running clients against it.
# They test cross-platform scenarios (WASI, browser).
# ============================================================================

fixture_signaling:
	@echo "━━━ Starting Signaling Server on 127.0.0.1:9995 (Ctrl+C to stop) ━━━"
	$(CARGO_ENV) cargo run --bin signaling_server

fixture_echo:
	@echo "━━━ Starting Echo Server on 127.0.0.1:9990 (TCP + WebSocket, Ctrl+C to stop) ━━━"
	$(CARGO_ENV) cargo run --bin test_auto_detect_native

client_p2p_native:
	@echo "━━━ Native P2P Peer (needs: fixture_signaling) ━━━"
	SIGNAL_URL=ws://127.0.0.1:9995 ROOM=test-rtc-room \
		$(CARGO_ENV) cargo run --bin test_p2p_native_peer

client_wasi_tcp:
	@echo "━━━ WASI TCP client (needs: fixture_echo) ━━━"
	$(CARGO_ENV) cargo run $(TARGET_WASI) --bin test_network_wasi_client

client_wasi_ws:
	@echo "━━━ WASI WS client (needs: fixture_echo) ━━━"
	$(CARGO_ENV) cargo run $(TARGET_WASI) --bin test_websocket_wasi_client

client_rtc_browser_build:
	@echo "━━━ Building browser WebRTC test (serve with: trunk serve test_rtc_browser.html) ━━━"
	$(CARGO_ENV) cargo build $(TARGET_BROWSER) --bin test_rtc_browser

client_ws_browser_build:
	@echo "━━━ Building browser WebSocket test (serve with: trunk serve test_websocket_browser.html) ━━━"
	$(CARGO_ENV) cargo build $(TARGET_BROWSER) --bin test_websocket_browser

help:
	@echo "ego-transport - Makefile Targets"
	@echo "================================"
	@echo ""
	@echo "Core (all three platforms unless suffixed):"
	@echo "  make build | check | test        - native + wasi + browser"
	@echo "  make <verb>_native|_wasi|_browser - single platform"
	@echo "  make fmt | fmt_check | clippy    - format / lint"
	@echo "  make ci                          - fmt_check + clippy + check + test"
	@echo "  Append 'quiet' to suppress rustc warnings (make test quiet)"
	@echo ""
	@echo "Standalone integration tests (self-contained, native):"
	@echo "  make test_standalone             - the reliable set"
	@echo "  make test_SA_*                   - individual tests (see Makefile)"
	@echo ""
	@echo "Fixtures & cross-platform clients:"
	@echo "  make fixture_signaling | fixture_echo"
	@echo "  make client_p2p_native | client_wasi_tcp | client_wasi_ws"
	@echo "  make client_rtc_browser_build | client_ws_browser_build"

.PHONY: quiet clean _init fmt fmt_check clippy doc all ci help \
        test_standalone test_SA_tcp_echo test_SA_server_builder test_SA_ws_echo \
        test_SA_signaling test_SA_p2p test_SA_embedded_signaling \
        test_SA_routed_signaling test_SA_multihop_signaling \
        fixture_signaling fixture_echo client_p2p_native client_wasi_tcp \
        client_wasi_ws client_rtc_browser_build client_ws_browser_build
