import argparse
import hashlib
import json
import os
import threading
import time
import tomllib
import traceback
from datetime import datetime, timezone
from pathlib import Path

import requests
from google.protobuf.message import DecodeError
from google.transit import gtfs_realtime_pb2


def make_session(contact_email):
    s = requests.Session()
    s.headers.update({
        "User-Agent": f"Adelaide Metro Data Project, Contact: {contact_email}",
        "Accept-Encoding": "gzip",
    })
    return s


def fetch_realtime(session, url, timeout):
    try:
        r = session.get(url, timeout=timeout)
    except requests.RequestException as e:
        return "transport", type(e).__name__, None, None, None
    if r.status_code != 200:
        return "http", str(r.status_code), None, None, None
    feed = gtfs_realtime_pb2.FeedMessage()
    try:
        feed.ParseFromString(r.content)
    except DecodeError as e:
        return "content", str(e), None, None, None
    return "success", "", r.content, feed.header.timestamp, len(feed.entity)


def write_log(log_dir, fetched_at, record):
    day = datetime.fromtimestamp(fetched_at, timezone.utc).strftime("%Y%m%d")
    with open(log_dir / f"{day}.jsonl", "a") as f:
        f.write(json.dumps(record) + "\n")


def every(interval, step):
    next_tick = time.monotonic()
    while True:
        step()
        now = time.monotonic()
        while next_tick <= now:
            next_tick += interval
        time.sleep(next_tick - now)


def run_realtime(name, url, interval, timeout, cfg):
    session = make_session(cfg["contact_email"])
    data_dir = Path(cfg["data_dir"]) / name
    log_dir = Path(cfg["log_dir"]) / name
    log_dir.mkdir(parents=True, exist_ok=True)
    last_hash = None

    def step():
        nonlocal last_hash
        fetched_at = time.time()
        outcome, detail, body, header_ts, n_entities = fetch_realtime(session, url, timeout)
        stored = False
        if outcome == "success":
            h = hashlib.sha256(body).hexdigest()
            if h != last_hash:
                dt = datetime.fromtimestamp(fetched_at, timezone.utc)
                day_dir = data_dir / dt.strftime("%Y%m%d")
                day_dir.mkdir(parents=True, exist_ok=True)
                (day_dir / f"{dt.strftime('%Y%m%d%H%M%S')}_{h[:12]}.pb").write_bytes(body)
                last_hash = h
                stored = True
        write_log(log_dir, fetched_at, {
            "fetched_at": round(fetched_at, 3), "outcome": outcome, "detail": detail,
            "header_ts": header_ts, "entities": n_entities, "stored": stored,
        })

    every(interval, step)


def run_static(cfg):
    sc = cfg["static"]
    session = make_session(cfg["contact_email"])
    static_dir = Path(cfg["data_dir"]) / "gtfs_static"
    static_dir.mkdir(parents=True, exist_ok=True)
    log_dir = Path(cfg["log_dir"]) / "gtfs_static"
    log_dir.mkdir(parents=True, exist_ok=True)

    def step():
        fetched_at = time.time()
        version, stored, detail = None, False, ""
        try:
            r = session.get(sc["version_url"], timeout=tuple(sc["timeout"]))
            r.raise_for_status()
            version = r.text.strip()
            # known versions are the zips already on disk, named {version}_{YYYYMMDD}.zip
            if not any(static_dir.glob(f"{version}_*.zip")):
                z = session.get(sc["zip_url"], timeout=tuple(sc["timeout"]))
                z.raise_for_status()
                day = datetime.fromtimestamp(fetched_at, timezone.utc).strftime("%Y%m%d")
                tmp = static_dir / f"{version}_{day}.zip.part"
                tmp.write_bytes(z.content)
                tmp.rename(tmp.with_suffix(""))
                stored = True
        except requests.RequestException as e:
            detail = type(e).__name__
        write_log(log_dir, fetched_at, {
            "fetched_at": round(fetched_at, 3), "version": version, "stored": stored, "detail": detail,
        })

    every(sc["interval"], step)


def crash_on_error(f, *args):
    # a dead thread would otherwise silently stop one feed; take the whole process down instead
    try:
        f(*args)
    except BaseException:
        traceback.print_exc()
        os._exit(1)


def main():
    p = argparse.ArgumentParser(description="Adelaide Metro GTFS collector")
    p.add_argument("--config", type=Path, default=Path("config.toml"))
    args = p.parse_args()
    with open(args.config, "rb") as f:
        cfg = tomllib.load(f)
    Path(cfg["data_dir"]).mkdir(parents=True, exist_ok=True)
    Path(cfg["log_dir"]).mkdir(parents=True, exist_ok=True)

    jobs = [(run_static, cfg)]
    for name in ("vehicle_positions", "trip_updates", "service_alerts"):
        fc = cfg[name]
        jobs.append((run_realtime, name, fc["url"], fc["interval"], tuple(fc["timeout"]), cfg))
    threads = [threading.Thread(target=crash_on_error, args=job, daemon=True) for job in jobs]
    for t in threads:
        t.start()
    for t in threads:
        t.join()


if __name__ == "__main__":
    main()
