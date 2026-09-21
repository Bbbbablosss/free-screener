# Free Screener deployment (server 214)

This edition is isolated from the main Crypto Live installation:

- application: `/opt/free-screener`
- web: `127.0.0.1:8100`
- WebSocket gateway: `127.0.0.1:7100`
- Redis: `127.0.0.1:6381`
- temporary nginx entry: public port `8080`
- systemd prefix: `free-screener-*`

The existing `/opt/telegram-bots` directory and any non-prefixed services are
outside this deployment and must not be changed.

Before enabling nginx on port 80/443, inspect the live nginx configuration and
assign the intended domain. The temporary `8080` listener is intentionally safe
for first-run verification by IP.

Prerequisites on the host: `python3`, `redis-server`, `nginx`, and a Go toolchain
compatible with `goingest/go.mod`. Run `deploy/free/install.sh` only from the
final `/opt/free-screener` directory. It installs only `free-screener-*` units.
