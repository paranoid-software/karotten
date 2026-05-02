# karotten

Docker → RabbitMQ log sink. A small Go daemon that forwards every
container's `stdout` / `stderr` on the host into a RabbitMQ topic
exchange — one AMQP message per log line, exactly as Docker emitted it.

Runs **outside** the Docker stack as a host-level process. Talks to the
Docker engine over its unix socket. Only attaches log streams for
containers that are currently `running`; stopped/exited ones are
ignored entirely. Reacts to Docker events to attach/detach as
containers come and go.

**Read-only against the daemon**: lists, inspects, log-streams,
events, `/_ping`. No `restart`, no `exec`, no modifications.

## Why a custom binary instead of an existing tool

Each existing option had a problem that mattered for our use case:

- **Vector `docker_logs` source** — solid in steady state, but locked
  itself in a tight reconnect loop when a stopped container existed at
  startup. Hit it in production.
- **Docker `--log-driver=syslog`** — robust, but breaks `docker logs`
  and requires a daemon restart to apply, neither of which were
  acceptable.
- **Filebeat / Fluent Bit / Promtail** — none have a native AMQP
  output; would need an extra hop.

`karotten` is ~500 lines of Go and a single non-stdlib dependency
([`amqp091-go`](https://github.com/rabbitmq/amqp091-go)).

## Routing-key contract

Routing key always has **three segments**, all derived from
Docker-level metadata only — the body is never parsed:

```
<project>.<service>.<stream>
```

| Segment | Source | Notes |
|---|---|---|
| `project` | `com.docker.compose.project` label | Falls back to `standalone` for non-compose containers. Any literal `.` becomes `-`. |
| `service` | `com.docker.compose.service` label | Falls back to the Docker container name when the container wasn't started by compose. Any literal `.` becomes `-`. |
| `stream` | Docker stdio stream | `stdout` or `stderr`. |

Replicas of the same service share the routing key — the `container_id`
and `container_name` fields in the body let consumers tell them apart
when needed.

## Message body

JSON envelope with these fields:

| Field | Type | Description |
|---|---|---|
| `timestamp` | RFC 3339 nano string | When `karotten` saw the line. |
| `project` | string | Same as routing-key segment. |
| `service` | string | Same as routing-key segment. |
| `container_name` | string | Docker container name (e.g. `my-looky-app-1`). Useful to differentiate replicas. |
| `container_id` | string | Short (12-char) Docker container ID. |
| `image` | string | Image reference of the container. |
| `stream` | string | `stdout` or `stderr`. |
| `message` | string | Raw line as Docker emitted it. **Never parsed** — JSON or plain text alike, the consumer decides. |

## Build

```bash
go build -o karotten .
```

Go 1.22 or newer.

## Configuration

Three environment variables:

| Var | Required | Default | Notes |
|---|---|---|---|
| `RABBITMQ_URL` | yes | — | e.g. `amqp://user:pass@host:5672/vhost` |
| `EXCHANGE` | no | `containers-logs` | Must already exist on the broker as `topic`, durable. `karotten` does a *passive* declare and won't create it. |
| `DOCKER_SOCK` | no | `/var/run/docker.sock` | Path to the Docker engine unix socket. |
| `DEBUG` | no | unset | If set, logs every published message. Very noisy; one-off debug only. |

The user running the binary needs read access to `DOCKER_SOCK` (i.e.
membership in the `docker` group on most distros).

## Deploying

Deployment is **operator-specific** and lives outside this repo: write
a systemd unit (or whatever supervisor you use) on the host that runs
the binary with the right env vars and `Restart=always`. A bare-bones
example for systemd:

```ini
# /etc/systemd/system/karotten.service
[Unit]
Description=karotten — Docker logs to RabbitMQ
After=docker.service network-online.target
Wants=docker.service network-online.target

[Service]
Type=simple
User=<service-user>           # member of the `docker` group
EnvironmentFile=/etc/default/karotten
ExecStart=/usr/local/bin/karotten
Restart=always
RestartSec=2
StandardOutput=journal
StandardError=journal
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true

[Install]
WantedBy=multi-user.target
```

with `/etc/default/karotten` (mode `0640`, `root:<service-user>`):

```
RABBITMQ_URL=amqp://user:pass@broker:5672/vhost
EXCHANGE=containers-logs
DOCKER_SOCK=/var/run/docker.sock
```

Then:

```bash
sudo install -o root -g root -m 0755 ./karotten /usr/local/bin/karotten
sudo systemctl daemon-reload
sudo systemctl enable --now karotten
```

## Resilience

Layered defenses against the failure modes seen during development:

1. **HTTP `ResponseHeaderTimeout` (15 s)** — a frozen daemon can't
   make initial GETs hang forever.
2. **`IdleConnTimeout` (60 s)** + `MaxIdleConnsPerHost=4` — pooled
   connections to the docker socket are discarded promptly so a
   daemon restart's stale FDs don't sit around in the pool.
3. **Events-stream watchdog (3 min)** — if no docker event arrives for
   3 minutes the events stream is force-reconnected. Catches the case
   where the kernel didn't propagate a daemon-side disconnect.
4. **Daemon `/_ping` health check (every 30 s, fatal after 3
   consecutive failures)** — backstop. If the daemon is genuinely
   gone, the process exits non-zero and the supervisor restarts it
   from a clean state.
5. **AMQP publisher reconnect with backoff** — broker bounces don't
   take the shipper down.
6. **Bounded in-memory publish buffer (8192 messages)** — drops on
   overflow rather than back-pressuring the docker log readers
   (which would throttle the daemon).

## What it does NOT do

- Does not declare or create queues. That's the consumer's job.
- Does not declare or create exchanges. The exchange must already
  exist on the broker; `karotten` does a *passive* declare on connect.
- Does not modify any container in any way (no restart, exec, kill,
  rm, anything).
- Does not parse log bodies. JSON or plain text, the line is forwarded
  as a string in the `message` field.
- Does not retry per individual message. If AMQP is down, messages are
  buffered up to 8192; beyond that they are dropped. Drop counter is
  logged every 60 s when non-zero.

## Operating

Once running under your supervisor:

```bash
sudo systemctl status karotten
sudo journalctl -u karotten -f
sudo systemctl restart karotten
```

Day-to-day container churn (`compose up`, `compose down`, `restart`,
adding new stacks) is handled automatically via Docker events — no
intervention needed. Even a `systemctl restart docker` recovers within
~1–2 min via the watchdogs and supervisor restart.
