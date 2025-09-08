# Deployment

This project uses [Gunicorn](https://gunicorn.org/) as its WSGI server.

## Container Deployment

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

- Install `podman-compose` if the `podman compose` plugin is not available.
- Rootless Podman cannot bind ports below 1024 by default. Use `sudo podman` or
  set `net.ipv4.ip_unprivileged_port_start=0` to allow low ports.

**Troubleshooting Podman**

- Volume permissions: use `--userns keep-id` or adjust host ownership.
- SELinux denials: add `:Z` to volume mounts or disable labels with
  `--security-opt label=disable`.

## Running with Gunicorn

Set up the environment variables in `.env` and start the server:

```bash
gunicorn --workers ${WORKERS:-$((2 * $(nproc) + 1))} \
         --timeout ${GUNICORN_TIMEOUT:-120} \
         --bind "${FLASK_HOST:-0.0.0.0}:${FLASK_PORT:-5000}" \
         app:app
```

`WORKERS` controls the number of worker processes. If unset, the startup
script calculates a sensible default of `2 * CPU cores + 1`.

`GUNICORN_TIMEOUT` sets the maximum time (in seconds) a worker can take to
handle a request. The default of `120` seconds accommodates slower upstream
panels.

When using Docker, Docker Compose, Podman, or `podman compose`, the container's
entrypoint runs the same command automatically. Adjust the worker count or
timeout by setting the `WORKERS` or `GUNICORN_TIMEOUT` environment variables.

### Enabling HTTPS

Provide paths to your SSL certificate and key via the `SSL_CERT_PATH` and
`SSL_KEY_PATH` environment variables. The startup script automatically passes
them to Gunicorn:

```bash
SSL_CERT_PATH=/app/certs/cert.pem \
SSL_KEY_PATH=/app/certs/key.pem \
FLASK_PORT=443 \
gunicorn --workers ${WORKERS:-$((2 * $(nproc) + 1))} \
         --timeout ${GUNICORN_TIMEOUT:-120} \
         --bind "${FLASK_HOST:-0.0.0.0}:${FLASK_PORT:-5000}" \
         --certfile "$SSL_CERT_PATH" --keyfile "$SSL_KEY_PATH" \
         app:app
```

When using Docker or Podman, mount the certificate files into the container and
set the environment variables accordingly. The `start.sh` script will detect
them and enable HTTPS automatically.

## Database connection pool

The application reuses database connections via a MySQL connection pool. The
pool size is controlled with the `MYSQL_POOL_SIZE` environment variable and
defaults to `5 × CPU cores`.

For deployments expecting heavy traffic, increase the pool size to allow more
concurrent requests. A common starting point is allocating roughly 5–10
connections per Gunicorn worker while staying within the MySQL server's
`max_connections` limit. The application logs an error when the pool is
exhausted; configure your monitoring to alert on this condition.
