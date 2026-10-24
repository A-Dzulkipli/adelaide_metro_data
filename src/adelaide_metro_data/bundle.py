import argparse
import shutil
import subprocess
import tomllib
from datetime import datetime, timedelta, timezone
from pathlib import Path

import polars as pl
from google.protobuf.descriptor import FieldDescriptor as FD
from google.transit import gtfs_realtime_pb2

# column name -> dotted field path, relative to the entity (vehicle / trip_update / stop_time_update)
VEHICLE_COLS = {
    "vehicle_id": "vehicle.id", "vehicle_label": "vehicle.label", "license_plate": "vehicle.license_plate",
    "trip_id": "trip.trip_id", "route_id": "trip.route_id", "direction_id": "trip.direction_id",
    "start_date": "trip.start_date", "start_time": "trip.start_time",
    "trip_schedule_relationship": "trip.schedule_relationship",
    "latitude": "position.latitude", "longitude": "position.longitude", "bearing": "position.bearing",
    "speed": "position.speed", "odometer": "position.odometer",
    "current_stop_sequence": "current_stop_sequence", "stop_id": "stop_id", "current_status": "current_status",
    "timestamp": "timestamp", "congestion_level": "congestion_level",
    "occupancy_status": "occupancy_status", "occupancy_percentage": "occupancy_percentage",
}
TRIP_COLS = {
    "trip_id": "trip.trip_id", "route_id": "trip.route_id", "direction_id": "trip.direction_id",
    "start_date": "trip.start_date", "start_time": "trip.start_time",
    "trip_schedule_relationship": "trip.schedule_relationship",
    "vehicle_id": "vehicle.id", "vehicle_label": "vehicle.label",
    "timestamp": "timestamp", "trip_delay": "delay",
}
STOP_COLS = {
    "stop_sequence": "stop_sequence", "stop_id": "stop_id",
    "arrival_time": "arrival.time", "arrival_delay": "arrival.delay", "arrival_uncertainty": "arrival.uncertainty",
    "departure_time": "departure.time", "departure_delay": "departure.delay",
    "departure_uncertainty": "departure.uncertainty",
    "stop_schedule_relationship": "schedule_relationship",
}


def get(msg, path):
    # None when the field is absent (proto2), enum values as their names
    for p in path.split("."):
        if not msg.HasField(p):
            return None
        field = msg.DESCRIPTOR.fields_by_name[p]
        msg = getattr(msg, p)
    return field.enum_type.values_by_number[msg].name if field.enum_type else msg


PB_DTYPES = {FD.TYPE_STRING: pl.String, FD.TYPE_ENUM: pl.String, FD.TYPE_FLOAT: pl.Float32,
             FD.TYPE_DOUBLE: pl.Float64, FD.TYPE_BOOL: pl.Boolean}


def schema(desc, cols):
    # fixed dtypes from the proto definition, so every day's parquet has the same schema
    out = {}
    for c, path in cols.items():
        d = desc
        for p in path.split("."):
            field = d.fields_by_name[p]
            d = field.message_type
        out[c] = PB_DTYPES.get(field.type, pl.Int64)
    return out


def read_feed(path):
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(path.read_bytes())
    return feed


def text(ts):
    return ts.translation[0].text if ts.translation else None


VEHICLE_SCHEMA = {"feed_ts": pl.Int64, "entity_id": pl.String, **schema(gtfs_realtime_pb2.VehiclePosition.DESCRIPTOR, VEHICLE_COLS)}
TRIP_SCHEMA = {"feed_ts": pl.Int64, "entity_id": pl.String, **schema(gtfs_realtime_pb2.TripUpdate.DESCRIPTOR, TRIP_COLS),
               **schema(gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.DESCRIPTOR, STOP_COLS)}


def vehicle_rows(feed):
    for e in feed.entity:
        yield {"feed_ts": feed.header.timestamp, "entity_id": e.id,
               **{c: get(e.vehicle, p) for c, p in VEHICLE_COLS.items()}}


def trip_rows(feed):
    # one row per stop_time_update; a trip without any keeps one row with empty stop columns
    for e in feed.entity:
        tu = e.trip_update
        trip = {"feed_ts": feed.header.timestamp, "entity_id": e.id, **{c: get(tu, p) for c, p in TRIP_COLS.items()}}
        for s in tu.stop_time_update or [None]:
            yield {**trip, **{c: None if s is None else get(s, p) for c, p in STOP_COLS.items()}}


ALERT_SCHEMA = {
    "alert_key": pl.Int64, "snapshot": pl.Int64, "cause": pl.String, "effect": pl.String,
    "header_text": pl.String, "description_text": pl.String, "url": pl.String,
    "active_periods": pl.List(pl.Struct({"start": pl.Int64, "end": pl.Int64})),
    "informed_entities": pl.List(pl.Struct({"agency_id": pl.String, "route_id": pl.String, "route_type": pl.Int64,
                                            "direction_id": pl.Int64, "trip_id": pl.String, "stop_id": pl.String})),
}


def alert_rows(feed, snapshot):
    for e in feed.entity:
        a = e.alert
        yield {
            # entity ids change every refresh, so identify an alert by its content
            "alert_key": hash(a.SerializeToString(deterministic=True)), "snapshot": snapshot,
            "cause": get(a, "cause"), "effect": get(a, "effect"),
            "header_text": text(a.header_text), "description_text": text(a.description_text), "url": text(a.url),
            "active_periods": [{"start": get(p, "start"), "end": get(p, "end")} for p in a.active_period],
            "informed_entities": [{"agency_id": get(i, "agency_id"), "route_id": get(i, "route_id"),
                                   "route_type": get(i, "route_type"), "direction_id": get(i, "direction_id"),
                                   "trip_id": get(i, "trip.trip_id"), "stop_id": get(i, "stop_id")}
                                  for i in a.informed_entity],
        }


def write_alerts(day_dir, out):
    # one row per distinct alert. Snapshots are only stored when the feed changes and the feed's header
    # timestamp is unreliable (~10h ahead), so timing comes from the fetch time in the snapshot filenames:
    # first_seen = first snapshot containing the alert, gone_by = first later snapshot without it (null if
    # still present at the end of the day)
    files = sorted(day_dir.glob("*.pb"))
    fetched = [int(datetime.strptime(f.name[:14], "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc).timestamp())
               for f in files] + [None]
    df = pl.DataFrame([r for i, f in enumerate(files) for r in alert_rows(read_feed(f), i)], schema=ALERT_SCHEMA)
    df = (df.group_by("alert_key", maintain_order=True)
            .agg(pl.col("snapshot").min().alias("first"), pl.col("snapshot").max().alias("last"), pl.all().first())
            .with_columns(first_seen=pl.col("first").map_elements(fetched.__getitem__, pl.Int64),
                          gone_by=(pl.col("last") + 1).map_elements(fetched.__getitem__, pl.Int64))
            .select("first_seen", "gone_by", *list(ALERT_SCHEMA)[2:]))
    df.write_parquet(out, compression="zstd")


FEEDS = {
    "vehicle_positions": (vehicle_rows, VEHICLE_SCHEMA),
    "trip_updates": (trip_rows, TRIP_SCHEMA),
}


def write_day(day_dir, rows_fn, schema, out):
    files = sorted(day_dir.glob("*.pb"))
    # write batches of snapshots as parts and stream them into one file, so memory stays flat on the large feeds
    parts = []
    for i in range(0, len(files), 50):
        parts.append(out.with_suffix(f".part{i}"))
        pl.DataFrame([r for f in files[i:i + 50] for r in rows_fn(read_feed(f))], schema=schema).write_parquet(parts[-1])
    pl.scan_parquet(parts).sink_parquet(out, compression="zstd")
    for part in parts:
        part.unlink()


def main():
    p = argparse.ArgumentParser(description="Bundle collected days into parquet and upload")
    p.add_argument("--config", type=Path, default=Path("config.toml"))
    p.add_argument("--no-upload", action="store_true", help="write parquet only; keep raw files")
    args = p.parse_args()
    with open(args.config, "rb") as f:
        cfg = tomllib.load(f)
    bc = cfg["bundle"]
    data_dir, log_dir = Path(cfg["data_dir"]), Path(cfg["log_dir"])
    out_dir = data_dir / "parquet"
    cutoff = (datetime.now(timezone.utc) - timedelta(days=bc["lag_days"])).strftime("%Y%m%d")

    days = sorted({d.name for feed in [*FEEDS, "service_alerts"] for d in (data_dir / feed).glob("[0-9]" * 8) if d.name <= cutoff}
                  | {f.stem for f in log_dir.glob("*/[0-9]*.jsonl") if f.stem <= cutoff})
    for day in days:
        done = []
        for feed in [*FEEDS, "service_alerts"]:
            day_dir = data_dir / feed / day
            if day_dir.exists():
                out = out_dir / feed / f"{day}.parquet"
                out.parent.mkdir(parents=True, exist_ok=True)
                write_alerts(day_dir, out) if feed == "service_alerts" else write_day(day_dir, *FEEDS[feed], out)
                done.append(day_dir)
                print(day, feed, pl.scan_parquet(out).select(pl.len()).collect().item(), "rows")
        for log in log_dir.glob(f"*/{day}.jsonl"):
            (out_dir / "logs" / log.parent.name).mkdir(parents=True, exist_ok=True)
            pl.read_ndjson(log).write_parquet(out_dir / "logs" / log.parent.name / f"{day}.parquet", compression="zstd")
            done.append(log)

        if args.no_upload:
            continue
        subprocess.run(["rclone", "copy", str(out_dir), bc["remote"]], check=True)
        for path in done:
            shutil.rmtree(path) if path.is_dir() else path.unlink()
        shutil.rmtree(out_dir)

    if not args.no_upload:
        # timetable zips stay local: the collector uses them to recognise known versions
        subprocess.run(["rclone", "copy", str(data_dir / "gtfs_static"), f"{bc['remote']}/gtfs_static"], check=True)


if __name__ == "__main__":
    main()
