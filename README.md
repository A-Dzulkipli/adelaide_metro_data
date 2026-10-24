# adelaide-metro-data

Collects Adelaide Metro GTFS feeds:

- vehicle positions (realtime, polled every 5s, feed refreshes ~15s)
- trip updates (realtime, polled every 15s, feed refreshes ~60s)
- service alerts (realtime, polled every 60s)
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

[service_alerts]
url = "https://gtfs.adelaidemetro.com.au/v1/realtime/service_alerts/"
interval = 60.0
timeout = [2.0, 5.0]

[static]
version_url = "https://gtfs.adelaidemetro.com.au/v1/static/latest/version.txt"
zip_url = "https://gtfs.adelaidemetro.com.au/v1/static/latest/google_transit.zip"
interval = 21600.0
timeout = [5.0, 120.0]

[bundle]
remote = "gdrive:adelaide_metro"   # rclone remote:path
lag_days = 2
```

Then `uv run adelaide-metro-data` (or `--config path/to/config.toml`).

## Daily bundle

`uv run adelaide-metro-bundle` (run once a day) converts every UTC day at least `lag_days` old into one zstd
parquet per feed plus parquet logs, uploads them with `rclone copy`, then deletes that day's raw `.pb` files and
logs. Timetable zips are uploaded but kept locally. `--no-upload` writes parquet to `data/parquet/` and deletes
nothing. Requires [rclone](https://rclone.org) configured with the remote named in `config.toml`.

- `vehicle_positions/YYYYMMDD.parquet`: one row per vehicle per snapshot
- `trip_updates/YYYYMMDD.parquet`: one row per stop prediction (trip columns repeated)
- `service_alerts/YYYYMMDD.parquet`: one row per distinct alert, with `first_seen` / `gone_by` from fetch times
  (the feed's own header timestamp is ~10h ahead)
- `logs/<feed>/YYYYMMDD.parquet`: one row per poll

## Layout

```
data/vehicle_positions/YYYYMMDD/YYYYMMDDHHMMSS_<sha256[:12]>.pb
data/trip_updates/YYYYMMDD/...
data/service_alerts/YYYYMMDD/...
data/gtfs_static/<version>_YYYYMMDD.zip
logs/<feed>/YYYYMMDD.jsonl      one line per poll
```

Dates are UTC.
