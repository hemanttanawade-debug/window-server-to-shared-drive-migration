#!/usr/bin/env python3
"""
gdrive_migrate_pure.py - Migrate a local folder to a Google Workspace Shared Drive using the Drive API
directly (NO rclone). Multi-threaded, resumable uploads, exponential backoff, idempotent re-runs.

Install:   pip install google-api-python-client google-auth

Auth:      Service account JSON key. If the account is itself a Shared Drive member, no --impersonate is needed.
           With Domain-Wide Delegation, pass --impersonate user@yourdomain.com (that user must be a member).
           Scope used: https://www.googleapis.com/auth/drive

Examples
  Scan locally (no Google calls, no credentials needed):
    python gdrive_migrate_pure.py --source "C:\\Users\\me\\Desktop\\important" --scan-only

  Dry run (reads Drive, writes nothing):
    python gdrive_migrate_pure.py --source "C:\\data" --shared-drive-id 0AXXXX --dest-path Migration/FS01 ^
        --service-account-file C:\\keys\\sa.json --impersonate admin@example.com --dry-run

  Real run / delta re-run (same command, only new or changed files upload):
    python gdrive_migrate_pure.py --source "C:\\data" --shared-drive-id 0AXXXX --dest-path Migration/FS01 ^
        --service-account-file C:\\keys\\sa.json --impersonate admin@example.com

Idempotency: a file is skipped when it exists in the same folder with the same size and the same modified time
(the script sets Drive's modifiedTime to the local mtime on upload). Use --checksum to compare MD5 instead.

Exit codes: 0 OK | 1 pre-flight failure | 2 some files failed | 4 quota/limit reached (re-run later)
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import fnmatch
import hashlib
import json
import logging
import os
import random
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Dict, Optional

SCOPES = ["https://www.googleapis.com/auth/drive"]
FOLDER_MIME = "application/vnd.google-apps.folder"
EXCLUDE_DIRS = {"$RECYCLE.BIN", "System Volume Information"}
EXCLUDE_FILES = ["Thumbs.db", "desktop.ini", "~$*"]
RATE_REASONS = {"userRateLimitExceeded", "rateLimitExceeded", "backendError", "internalError"}
FATAL_REASONS = {"storageQuotaExceeded", "teamDriveFileLimitExceeded", "numChildrenInNonRootLimitExceeded",
                 "dailyLimitExceeded", "uploadLimitExceeded", "teamDriveTeamMembersLimitExceeded"}

log = logging.getLogger("migrate")
stop_event = threading.Event()
tls = threading.local()


class QuotaStop(Exception):
    pass


# --------------------------------------------------------------------------- helpers
def fmt_bytes(n: float) -> str:
    for u in ("B", "KiB", "MiB", "GiB", "TiB"):
        if n < 1024:
            return f"{n:,.2f} {u}"
        n /= 1024
    return f"{n:,.2f} PiB"


def parse_size(s: str) -> int:
    s = s.strip().upper().rstrip("B").rstrip("I")
    mult = {"K": 1024, "M": 1024 ** 2, "G": 1024 ** 3, "T": 1024 ** 4}
    return int(float(s[:-1]) * mult[s[-1]]) if s and s[-1] in mult else int(s)


def md5_file(path: str) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8 * 1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def iso(ts: float) -> str:
    return dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def parse_iso(s: str) -> float:
    return dt.datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()


def excluded_file(name: str) -> bool:
    return any(fnmatch.fnmatch(name, p) for p in EXCLUDE_FILES)


def http_reason(e) -> str:
    try:
        return json.loads(e.content.decode())["error"]["errors"][0]["reason"]
    except Exception:
        return ""


# --------------------------------------------------------------------------- Drive access
def make_credentials(args):
    from google.oauth2 import service_account
    creds = service_account.Credentials.from_service_account_file(args.service_account_file, scopes=SCOPES)
    if args.impersonate:
        creds = creds.with_subject(args.impersonate)
    return creds


def drive(args, creds):
    """One service object per thread (httplib2 is not thread-safe)."""
    if not hasattr(tls, "svc"):
        from googleapiclient.discovery import build
        tls.svc = build("drive", "v3", credentials=creds, cache_discovery=False)
    return tls.svc


def with_backoff(fn, max_tries: int = 8):
    """Run fn(); retry on rate-limit / 5xx / network errors with exponential backoff + jitter."""
    from googleapiclient.errors import HttpError
    for i in range(max_tries):
        if stop_event.is_set():
            raise QuotaStop("stopped")
        try:
            return fn()
        except HttpError as e:
            status, reason = e.resp.status, http_reason(e)
            if reason in FATAL_REASONS:
                raise QuotaStop(reason)
            if status in (429, 500, 502, 503, 504) or (status == 403 and reason in RATE_REASONS):
                delay = min(120, 2 ** i) + random.random()
                log.warning("API %s/%s - retry %d/%d in %.1fs", status, reason, i + 1, max_tries, delay)
                time.sleep(delay)
                continue
            raise
        except OSError as e:  # timeouts, connection resets, DNS
            delay = min(120, 2 ** i) + random.random()
            log.warning("Network error %s - retry %d/%d in %.1fs", e, i + 1, max_tries, delay)
            time.sleep(delay)
    raise RuntimeError("retries exhausted")


class DriveTree:
    """Caches folder listings so we make ~1 list call per folder, not per file."""
    def __init__(self, args, creds, root_id: str, dry_run: bool):
        self.args, self.creds, self.root_id, self.dry = args, creds, root_id, dry_run
        self.lock = threading.RLock()
        self.folder_ids: Dict[str, Optional[str]] = {"": root_id}
        self.listings: Dict[str, Dict[str, dict]] = {}

    def children(self, folder_id: str) -> Dict[str, dict]:
        with self.lock:
            if folder_id in self.listings:
                return self.listings[folder_id]
            items: Dict[str, dict] = {}
            page = None
            while True:
                def call():
                    return drive(self.args, self.creds).files().list(
                        q=f"'{folder_id}' in parents and trashed=false",
                        corpora="drive", driveId=self.args.shared_drive_id,
                        includeItemsFromAllDrives=True, supportsAllDrives=True, pageSize=1000, pageToken=page,
                        fields="nextPageToken, files(id,name,mimeType,size,md5Checksum,modifiedTime)").execute()
                r = with_backoff(call)
                for f in r.get("files", []):
                    # prefer a folder when a folder and file share a name; otherwise first seen wins
                    items.setdefault(f["name"], f)
                page = r.get("nextPageToken")
                if not page:
                    break
            self.listings[folder_id] = items
            return items

    def ensure_folder(self, rel: str) -> Optional[str]:
        """rel = 'a/b/c' under the destination root. Returns None in dry-run if it doesn't exist yet."""
        with self.lock:
            if rel in self.folder_ids:
                return self.folder_ids[rel]
            parent_rel, _, name = rel.rpartition("/")
            parent_id = self.ensure_folder(parent_rel)
            if parent_id is None:
                self.folder_ids[rel] = None
                return None
            existing = self.children(parent_id).get(name)
            if existing and existing["mimeType"] == FOLDER_MIME:
                fid = existing["id"]
            elif self.dry:
                fid = None
            else:
                body = {"name": name, "mimeType": FOLDER_MIME, "parents": [parent_id]}
                fid = with_backoff(lambda: drive(self.args, self.creds).files().create(
                    body=body, fields="id", supportsAllDrives=True).execute())["id"]
                self.children(parent_id)[name] = {"id": fid, "name": name, "mimeType": FOLDER_MIME}
                self.listings[fid] = {}
            self.folder_ids[rel] = fid
            return fid


# --------------------------------------------------------------------------- upload
def upload_file(args, creds, local: str, folder_id: str, name: str, size: int, mtime: float, existing: Optional[dict]) -> int:
    from googleapiclient.errors import HttpError
    from googleapiclient.http import MediaFileUpload
    svc = drive(args, creds)
    body = {"name": name, "modifiedTime": iso(mtime)}
    fields = "id,size,md5Checksum"
    chunk = parse_size(args.chunk_size)
    resumable = size > 5 * 1024 * 1024  # empty/small files use simple upload (resumable fails on 0 bytes)
    media = MediaFileUpload(local, resumable=resumable, chunksize=chunk if resumable else -1)
    if existing and existing["mimeType"] != FOLDER_MIME:
        req = svc.files().update(fileId=existing["id"], body=body, media_body=media, fields=fields, supportsAllDrives=True)
    else:
        body["parents"] = [folder_id]
        req = svc.files().create(body=body, media_body=media, fields=fields, supportsAllDrives=True)

    resp, tries = None, 0
    while resp is None:
        if stop_event.is_set():
            raise QuotaStop("stopped")
        try:
            _, resp = req.next_chunk(num_retries=5) if resumable else (None, req.execute(num_retries=5))
        except HttpError as e:
            status, reason = e.resp.status, http_reason(e)
            if reason in FATAL_REASONS:
                raise QuotaStop(reason)
            tries += 1
            if tries <= 8 and (status in (429, 500, 502, 503, 504) or (status == 403 and reason in RATE_REASONS)):
                delay = min(120, 2 ** tries) + random.random()
                log.warning("Upload %s: API %s/%s - retry %d in %.1fs", name, status, reason, tries, delay)
                time.sleep(delay)
                continue
            raise
        except OSError as e:
            tries += 1
            if tries > 8:
                raise
            time.sleep(min(120, 2 ** tries) + random.random())
    if args.verify_md5 and size > 0:
        local_md5 = md5_file(local)
        if resp.get("md5Checksum") and resp["md5Checksum"].lower() != local_md5:
            raise RuntimeError(f"MD5 mismatch after upload: local {local_md5} vs drive {resp['md5Checksum']}")
    return size


# --------------------------------------------------------------------------- main run
def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Local folder -> Google Shared Drive (Drive API, no rclone)")
    p.add_argument("--source", default="", help="local folder to migrate (not needed with --list-drives)")
    p.add_argument("--shared-drive-id", default="")
    p.add_argument("--dest-path", default="")
    p.add_argument("--service-account-file", default="")
    p.add_argument("--impersonate", default="", help="DWD subject (must be a Shared Drive member)")
    p.add_argument("--threads", type=int, default=8, help="parallel uploads (keep modest to avoid 403 rate limits)")
    p.add_argument("--chunk-size", default="64M", help="resumable chunk, multiple of 256K (e.g. 64M)")
    p.add_argument("--max-bytes-per-run", default="", help="stop after this much data, e.g. 700G (750 GB/day limit)")
    p.add_argument("--checksum", action="store_true", help="skip decision by MD5 instead of size+mtime (reads local files)")
    p.add_argument("--verify-md5", action="store_true", help="re-hash each uploaded file and compare with Drive's MD5")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--scan-only", action="store_true", help="count local files only; no Google calls/credentials")
    p.add_argument("--list-drives", action="store_true", help="list Shared Drives (name + ID) visible to the account, then exit")
    p.add_argument("--log-dir", default=str(Path(__file__).resolve().parent / "logs"))
    args = p.parse_args(argv)

    run_id = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    run_dir = Path(args.log_dir) / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] [%(levelname)-7s] %(message)s", datefmt="%H:%M:%S",
                        handlers=[logging.StreamHandler(sys.stdout),
                                  logging.FileHandler(run_dir / "migration.log", encoding="utf-8")])

    src = os.path.abspath(args.source) if args.source else ""
    if not args.list_drives and (not args.source or not os.path.isdir(src)):
        log.error("--source is required and must be an existing folder (got: '%s').", args.source)
        return 1

    # ---- scan-only: no Google involvement
    if args.scan_only:
        files = total = dirs = 0
        for root, dn, fn in os.walk(src):
            dn[:] = [d for d in dn if d not in EXCLUDE_DIRS]
            dirs += 1
            for f in fn:
                if excluded_file(f):
                    continue
                try:
                    total += os.path.getsize(os.path.join(root, f))
                    files += 1
                except OSError as e:
                    log.warning("Cannot stat %s: %s", f, e)
        log.info("Scan: %s files, %s folders, %s", f"{files:,}", f"{dirs:,}", fmt_bytes(total))
        return 0

    # ---- pre-flight
    if not args.service_account_file or (not args.shared_drive_id and not args.list_drives):
        log.error("--service-account-file and --shared-drive-id are required (or use --scan-only / --list-drives).")
        return 1
    if args.shared_drive_id.startswith("YOUR_") or "ACTUAL" in args.shared_drive_id.upper():
        log.error("--shared-drive-id is still a placeholder. Run with --list-drives to find the real ID.")
        return 1
    if not Path(args.service_account_file).is_file():
        log.error("Service account key not found: %s", args.service_account_file)
        return 1
    try:
        import googleapiclient  # noqa: F401
        import google.oauth2  # noqa: F401
    except ImportError:
        log.error("Missing dependencies. Run: pip install google-api-python-client google-auth")
        return 1

    if args.list_drives:
        try:
            creds = make_credentials(args)
            found, page = [], None
            while True:
                r = with_backoff(lambda: drive(args, creds).drives().list(pageSize=100, pageToken=page,
                                                                          fields="nextPageToken, drives(id,name)").execute())
                found += r.get("drives", [])
                page = r.get("nextPageToken")
                if not page:
                    break
        except Exception as e:  # noqa: BLE001
            log.error("Could not list Shared Drives: %s", e)
            return 1
        if not found:
            log.warning("No Shared Drives visible. The impersonated user (or service account) is not a member of any. "
                        "Add them as Content manager/Manager on the target Shared Drive.")
        for d in found:
            log.info("Shared Drive: %-40s ID: %s", d["name"], d["id"])
        return 0

    try:
        creds = make_credentials(args)
        info = with_backoff(lambda: drive(args, creds).drives().get(driveId=args.shared_drive_id, fields="id,name").execute())
    except Exception as e:  # noqa: BLE001
        log.error("Cannot access Shared Drive %s: %s", args.shared_drive_id, e)
        log.error("Check: Drive API enabled, key valid, DWD scope authorised (if --impersonate), and the "
                  "service account / impersonated user is a MEMBER of the Shared Drive.")
        return 1
    log.info("Connected to Shared Drive '%s'. Source: %s  Dry run: %s", info["name"], src, args.dry_run)

    tree = DriveTree(args, creds, args.shared_drive_id, args.dry_run)
    dest_rel = args.dest_path.replace("\\", "/").strip("/")
    max_bytes = parse_size(args.max_bytes_per_run) if args.max_bytes_per_run else None

    stats = {"uploaded": 0, "skipped": 0, "failed": 0, "bytes": 0, "would_upload": 0, "would_bytes": 0}
    slock = threading.Lock()
    results = open(run_dir / "results.csv", "w", newline="", encoding="utf-8")
    rw = csv.writer(results, quoting=csv.QUOTE_ALL)
    rw.writerow(["Time", "Status", "Bytes", "Path", "Message"])

    def record(status: str, path: str, nbytes: int = 0, msg: str = "") -> None:
        with slock:
            rw.writerow([dt.datetime.now().isoformat(timespec="seconds"), status, nbytes, path, msg])

    def worker(local, rel_path, folder_id, name, size, mtime, existing):
        try:
            if stop_event.is_set():
                return
            if existing and existing["mimeType"] != FOLDER_MIME and int(existing.get("size", -1)) == size:
                if args.checksum:
                    same = size == 0 or existing.get("md5Checksum", "").lower() == md5_file(local)
                else:
                    same = abs(parse_iso(existing["modifiedTime"]) - mtime) <= 2
                if same:
                    with slock:
                        stats["skipped"] += 1
                    return
            if args.dry_run or folder_id is None:
                with slock:
                    stats["would_upload"] += 1
                    stats["would_bytes"] += size
                record("WOULD_UPLOAD", rel_path, size)
                return
            if max_bytes is not None:
                with slock:
                    if stats["bytes"] + size > max_bytes:
                        stop_event.set()
                        log.warning("--max-bytes-per-run reached; stopping. Re-run later to resume.")
                        return
            n = upload_file(args, creds, local, folder_id, name, size, mtime, existing)
            with slock:
                stats["uploaded"] += 1
                stats["bytes"] += n
            record("UPLOADED", rel_path, n)
        except QuotaStop as q:
            if str(q) != "stopped":
                log.error("Quota/limit reached (%s). Stopping; re-run later to resume.", q)
                stats["quota"] = str(q)
            stop_event.set()
        except Exception as e:  # noqa: BLE001
            with slock:
                stats["failed"] += 1
            log.error("FAILED %s: %s", rel_path, e)
            record("FAILED", rel_path, size, str(e))

    gate = threading.BoundedSemaphore(args.threads * 4)
    last_report = time.time()
    t0 = time.time()
    try:
        with ThreadPoolExecutor(max_workers=args.threads) as ex:
            for root, dirnames, filenames in os.walk(src):
                if stop_event.is_set():
                    break
                dirnames[:] = [d for d in dirnames if d not in EXCLUDE_DIRS]
                rel_dir = os.path.relpath(root, src).replace("\\", "/")
                rel_dir = "" if rel_dir == "." else rel_dir
                folder_rel = "/".join(x for x in (dest_rel, rel_dir) if x)
                try:
                    folder_id = tree.ensure_folder(folder_rel)
                    listing = tree.children(folder_id) if folder_id else {}
                except QuotaStop as q:
                    log.error("Stopping: %s", q)
                    stop_event.set()
                    break
                for fname in filenames:
                    if excluded_file(fname):
                        continue
                    local = os.path.join(root, fname)
                    try:
                        st = os.stat(local)
                    except OSError as e:
                        log.error("Cannot stat %s: %s", local, e)
                        stats["failed"] += 1
                        continue
                    gate.acquire()
                    fut = ex.submit(worker, local, f"{rel_dir}/{fname}".lstrip("/"), folder_id, fname,
                                    st.st_size, st.st_mtime, listing.get(fname))
                    fut.add_done_callback(lambda _f: gate.release())
                if time.time() - last_report > 30:
                    last_report = time.time()
                    el = max(1, time.time() - t0)
                    log.info("Progress: uploaded %s (%s), skipped %s, failed %s, %s/s", f"{stats['uploaded']:,}",
                             fmt_bytes(stats["bytes"]), f"{stats['skipped']:,}", stats["failed"], fmt_bytes(stats["bytes"] / el))
    except KeyboardInterrupt:
        log.error("Interrupted - waiting for in-flight uploads to stop...")
        stop_event.set()
    finally:
        results.close()

    elapsed = round(time.time() - t0, 1)
    code = 0
    status = "SUCCESS"
    if stats.get("quota") or (max_bytes is not None and stop_event.is_set()):
        code, status = 4, "QUOTA_OR_CAP_REACHED"
    elif stats["failed"]:
        code, status = 2, "COMPLETED_WITH_ERRORS"
    elif args.dry_run:
        status = "DRY_RUN_COMPLETE"
    summary = {"RunId": run_id, "Status": status, "ExitCode": code, "ElapsedSec": elapsed, "Source": src,
               "SharedDrive": info["name"], "DestPath": dest_rel, "DryRun": args.dry_run, **stats}
    (run_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    log.info("%s | uploaded %s (%s) | skipped %s | failed %s | would-upload %s (%s) | %ss",
             status, f"{stats['uploaded']:,}", fmt_bytes(stats["bytes"]), f"{stats['skipped']:,}", stats["failed"],
             f"{stats['would_upload']:,}", fmt_bytes(stats["would_bytes"]), elapsed)
    log.info("Logs: %s", run_dir)
    return code


if __name__ == "__main__":
    sys.exit(main())