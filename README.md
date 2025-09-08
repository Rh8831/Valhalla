# Valhalla

## Installation

1. Install [Docker](https://www.docker.com/) or [Podman](https://podman.io/):

   **Docker**

   ```sh
   curl -fsSL https://get.docker.com | sh
   ```

   **Podman**

   ```sh
   sudo apt-get -y install podman
   sudo apt install podman-compose
   ```

2. Run the setup script:

   ```sh
   sudo bash -c "$(curl -sL https://raw.githubusercontent.com/Rh8831/Valhalla/refs/heads/main/setup.sh)"
   ```

   The script pulls pre-built container images and starts them without building,
   so Podman works even when the source tree isn't present.

## Building and Running with Containers

### Docker

```sh
docker build -t valhalla .
docker run --rm -p 5000:5000 valhalla
docker compose up -d
```

### Podman

```sh
podman build -t valhalla .
podman run --rm -p 5000:5000 valhalla
podman compose up -d
```

**Podman prerequisites**

- Rootless Podman cannot bind ports below 1024 by default. Run `sudo podman`
  or set `net.ipv4.ip_unprivileged_port_start=0` to allow low ports.
- Install `podman-compose` if `podman compose` is unavailable:
  `sudo apt install podman-compose`.

**Troubleshooting Podman**

- Volume permissions: use `--userns keep-id` or ensure the host directory has
  matching ownership.
- SELinux denials: append `:Z` to volume mounts or disable labels with
  `--security-opt label=disable`.

## High-Concurrency Deployment

To handle a large number of concurrent connections, the application can run
Gunicorn with gevent workers. Set the `ASYNC_WORKERS` environment variable to
enable this mode:

```sh
ASYNC_WORKERS=100  # number of gevent workers
```

When `ASYNC_WORKERS` is defined, `start.sh` adds `--worker-class gevent` and uses
its value for the worker count. Ensure the `gevent` package is available in the
runtime environment.

In Docker Compose, expose the variable to the app service:

```yaml
environment:
  ASYNC_WORKERS: ${ASYNC_WORKERS}
```

This allows the service to scale with higher concurrency when needed.

## Subscription Fetch Caching

Remote panel queries for users and subscription links are cached in-memory to
reduce load. The cache lifetime defaults to 300 seconds and can be adjusted via
the `FETCH_CACHE_TTL` environment variable.
