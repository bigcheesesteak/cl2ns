# cl2ns

Lightweight Docker container that syncs Medtronic Carelink pump and sensor data to a [Nightscout](https://nightscout.github.io/) instance. No UI, no database, no web server -- just a single background daemon that polls Carelink and uploads to Nightscout.

## Features

- Syncs SGV (sensor glucose values) with trend direction and delta
- Uploads pump device status (battery, reservoir, IOB, suspend state)
- SHA-256 fingerprint deduplication to avoid duplicate Nightscout entries
- Adaptive polling: fast retries when waiting for new readings, then backs off
- Automatic OAuth token refresh with persistent token storage
- Timezone-aware timestamp conversion (auto-detected from Carelink or manually overridden)
- Runs entirely from container logs -- no UI, no ports exposed

## Prerequisites

You need a set of OAuth tokens from the Carelink mobile app flow. You can generate these using the bundled Python script in the `token` directory.

1. Navigate to the `token` directory: `cd token`
2. Install the required dependencies: `pip install -r requirements.txt`
3. Run the login script: `python carelink_carepartner_api_login.py` (add `--us` flag if you are in the US region)
4. A Firefox window will open temporarily to solve a Captcha. Once completed, a `logindata.json` file will be generated.

The `logindata.json` file will contain:
- `access_token` (JWT)
- `refresh_token`
- `client_id`
- `client_secret` (if applicable)
- `mag-identifier`

## Quick Start

You can run this easily using Docker Compose (or Portainer). Create a `docker-compose.yml` file:

```yaml
version: '3.8'

services:
  cl2ns:
    image: ghcr.io/bigcheesesteak/cl2ns:latest
    container_name: cl2ns
    restart: unless-stopped
    env_file:
      - .env
    volumes:
      - cl2ns_data:/data

volumes:
  cl2ns_data:
```

Next, create a `.env` file in the same directory (see **Configuration** below) with your credentials, and start the container:

```bash
docker compose up -d
docker compose logs -f cl2ns
```
## Configuration

All configuration is done via environment variables. Copy `.env.example` to `.env` and fill in your values.

| Variable | Required | Description |
|----------|----------|-------------|
| `CARELINK_TOKEN` | Yes | JWT access token from Carelink OAuth flow |
| `CARELINK_REFRESH_TOKEN` | Yes | Refresh token (single-use, auto-rotated) |
| `CARELINK_CLIENT_ID` | Yes | OAuth client ID |
| `CARELINK_CLIENT_SECRET` | No | OAuth client secret (if available) |
| `CARELINK_PATIENT_ID` | No | Required only for care partner accounts |
| `CARELINK_MAG_IDENTIFIER` | No | Legacy session identifier, usually not needed |
| `NIGHTSCOUT_URL` | Yes | Full URL of your Nightscout instance |
| `NIGHTSCOUT_API_SECRET` | Yes | Nightscout API secret |
| `SYNC_INTERVAL` | No | Base polling interval in seconds (default: `60`) |
| `LOG_LEVEL` | No | `DEBUG`, `INFO`, `WARNING`, or `ERROR` (default: `INFO`) |
| `TIMEZONE` | No | IANA timezone override (e.g. `Europe/Brussels`). Auto-detected from Carelink if not set. |

## How It Works

```
Carelink Cloud                    cl2ns                         Nightscout
     |                              |                              |
     |  <-- OAuth token refresh --> |                              |
     |  <-- POST /display/message   |                              |
     |      pump + sensor data -->  |                              |
     |                              |  --> POST /api/v1/entries    |
     |                              |  --> POST /api/v1/devicestatus
     |                              |                              |
     |         (sleep SYNC_INTERVAL)                               |
     |                              |                              |
```

1. Authenticates with the Carelink mobile API using OAuth bearer tokens
2. Fetches the latest pump/sensor data via the `/display/message` endpoint
3. Transforms SGV readings into Nightscout `entries` with trend direction and delta
4. Builds `devicestatus` with battery, reservoir, IOB, and suspend state
5. Deduplicates using SHA-256 fingerprints to avoid uploading the same readings twice
6. Sleeps and repeats. Uses fast retries (60s) when waiting for a new reading, then falls back to the configured interval.

## Log Output

Everything is reported through container stdout. Example output:

```
2026-08-13 09:34:11 [INFO] root: --- Sync Cycle [09:34:11 UTC] ---
2026-08-13 09:34:11 [INFO] root: Device Status: sensorState=NO_ERROR_MESSAGE | conduitInRange=True | pumpComm=True
2026-08-13 09:34:11 [INFO] root: Carelink Data: Glucose=124 mg/dL | Battery=75% | Reservoir=182.5 U
2026-08-13 09:34:11 [INFO] root: Nightscout Upload: SGV entries uploaded=1 (skipped=287)
2026-08-13 09:34:11 [INFO] root: New SGV reading received and synced!
2026-08-13 09:34:11 [INFO] root: Next sync check in 60s.
```

## Token Refresh

Carelink refresh tokens are **single-use**. After each refresh, the new token pair is persisted to `/data/carelink_tokens.json` inside the container volume. This means:

- The container survives restarts without re-authentication
- The initial env var tokens are only used on first boot
- If the volume is lost, you need to generate new tokens

## Troubleshooting

| Log Message | Meaning |
|-------------|---------|
| `sensorState=UNKNOWN` | Sensor is not active (warmup, expired, or not inserted) |
| `conduitInRange=False` | Phone/conduit cannot reach the pump |
| `systemStatus=BLUETOOTH_OFF` | Bluetooth is disabled on the pump |
| `Glucose=N/A` | No valid SG readings (all readings are 0 / sensor error) |
| `Carelink Auth Error` | Token expired and refresh failed. Generate new tokens. |

## Acknowledgments

This logic was built with the help of the following incredible community projects and sources:
- [Home-Assistant-Carelink by yo-han](https://github.com/yo-han/Home-Assistant-Carelink)
- [carelink_carepartner_api_login.py by yo-han](https://github.com/yo-han/Home-Assistant-Carelink/blob/develop/utils/carelink_carepartner_api_login.py)
- [carelink-python-client by ondrej1024](https://github.com/ondrej1024/carelink-python-client)
- [carelink-python-client by palmarci](https://github.com/palmarci/carelink-python-client)

## Development Note

This integration was developed with the assistance of AI. As an experienced developer, I utilized AI tools to accelerate the reverse-engineering of the intercepted API payloads and to scaffold the modern standalone Docker container architecture. All logic, architecture decisions, and final code implementations were manually directed, reviewed, and validated to ensure robustness and compliance with Python and Docker development standards.

## License

[Apache License 2.0](LICENSE)
