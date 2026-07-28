# Xboard-Node

Node backend for [Xboard](https://github.com/cedar2025/Xboard). Supports `sing-box` / `xray-core` dual kernels.

> **Disclaimer**: This project is for educational and learning purposes only.

## Features

- Protocols: V2Ray family, Trojan, Shadowsocks, Hysteria2, TUIC, AnyTLS
- Sync: WebSocket push + REST polling dual channel
- User controls: speed limit, device limit, alive-IP tracking, hot update
- Deploy modes: node mode, machine mode, standalone mode
- Multi-instance: single process binding multiple panels / nodes

## Install

### Docker

```bash
docker run -d --restart=always --network=host \
  -e apiHost=https://panel.com -e apiKey=TOKEN -e nodeID=1 \
  ghcr.io/karllee830/xboard-node:latest
```

### Docker Compose

```bash
git clone -b compose --depth 1 https://github.com/karllee830/Xboard-Node.git
cd Xboard-Node
vim config/config.yml   # set panel.url / token / node_id
docker compose up -d
```

### Installer (Linux systemd)

```bash
# Node mode
curl -fsSL https://raw.githubusercontent.com/karllee830/Xboard-Node/dev/install.sh | \
  sudo bash -s -- --mode node --panel https://panel.example.com --token TOKEN --node-id 1

# Machine mode
curl -fsSL https://raw.githubusercontent.com/karllee830/Xboard-Node/dev/install.sh | \
  sudo bash -s -- --mode machine --panel https://panel.example.com --token TOKEN --machine-id 1
```

安装器与 `xbctl upgrade` 默认下载本仓库的 `dev` Release；可通过 `--version` 指定正式版本或 `latest`。

## xbctl

Run `xbctl` after installation for help. Common commands:

```bash
xbctl list                          # list all instances
xbctl status                        # running status
xbctl bind add-node --panel URL --token TOKEN --node-id 1
xbctl bind add-machine --panel URL --token TOKEN --machine-id 1
xbctl bind remove-node --panel URL --node-id 1
xbctl service restart
```

## Configuration

Legacy single-panel config is fully compatible. Appending bindings auto-migrates to `instances` format. See `config.yml.example`.

### Detailed traffic statistics (Sing-box only)

The one-command installer enables the collector by default for Sing-box and reports immutable one-minute multidimensional aggregates to the Xboard Statistics plugin without changing the billing report path. Pass `--disable-statistics` only when the panel plugin is not ready yet.

```yaml
statistics:
  enabled: true
  spool_path: "" # defaults to each node's isolated kernel directory
  # Legacy key names are retained for compatibility; limits apply per minute batch.
  max_hourly_dimensions: 200000
  max_domains_per_user_hour: 5000
  max_destination_ips_per_user_hour: 10000
  max_pending_batches: 10080
  request_timeout: 30
  disable_sniff: false
```

It records user/node association, source and reliable destination IP, normalized domain, TCP/UDP, sniffed application protocol, inbound/outbound and destination port. It never records User-Agent, URL paths, headers, cookies or payload contents. Pending gzip batches are persisted atomically and retried with exponential backoff.

## Extensions

- Custom routes: [docs-custom-routes.md](docs-custom-routes.md)
- Custom outbounds: [docs-custom-outbounds.md](docs-custom-outbounds.md)
- DNS providers (ACME DNS-01): [docs-dns-providers.md](docs-dns-providers.md)

## License

MPL-2.0.
