# adelaide-metro-data

Collects Adelaide Metro GTFS feeds:

- vehicle positions (realtime, polled every 5s, feed refreshes ~15s)
- trip updates (realtime, polled every 15s, feed refreshes ~60s)
- timetable (static GTFS zip, checked every 6h, downloaded only when `version.txt` changes)

Realtime snapshots are stored raw and only when the payload changes.

## Run

Create `config.toml` in the repo root (not tracked):

```toml
contact_email = "you@example.com"
data_dir = "/abs/path/data"
log_dir = "/abs/path/logs"

[vehicle_positions]
url = "https://gtfs.adelaidemetro.com.au/v1/realtime/vehicle_positions/"
interval = 5.0
timeout = [2.0, 3.0]

[trip_updates]
url = "https://gtfs.adelaidemetro.com.au/v1/realtime/trip_updates/"
interval = 15.0
timeout = [2.0, 5.0]

[static]
version_url = "https://gtfs.adelaidemetro.com.au/v1/static/latest/version.txt"
zip_url = "https://gtfs.adelaidemetro.com.au/v1/static/latest/google_transit.zip"
interval = 21600.0
timeout = [5.0, 120.0]
```

Then `uv run adelaide-metro-data` (or `--config path/to/config.toml`).

## Layout

```
data/vehicle_positions/YYYYMMDD/YYYYMMDDHHMMSS_<sha256[:12]>.pb
data/trip_updates/YYYYMMDD/...
data/gtfs_static/<version>_YYYYMMDD.zip
logs/<feed>/YYYYMMDD.jsonl      one line per poll
```

Dates are UTC.
