#!/usr/bin/env python3
"""
download_xnat_subject_data.py

Download available data for a given SUBJECT_ID (within a PROJECT_ID) from XNAT
to a local directory. 

  - Downloading each resource as a ZIP then optionally extract.
  - If ZIP download fails, fall back to downloading files one-by-one.

Requires:
  pip install requests
Optional (for nicer retries):
  pip install urllib3

"""

from __future__ import annotations

import os
import re
import json
import time
import zipfile
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
from requests.auth import HTTPBasicAuth


# =========================
# USER CONFIG (edit these)
# =========================

USERNAME = os.environ.get("XNAT_USER", "")
PASSWORD = os.environ.get("XNAT_PASS", "")

BASE_URL = ""
PROJECT_ID = ""
SUBJECT_ID = ""

# Where to download everything
OUTPUT_ROOT = Path(r"D:\Downloads\XNAT_transfer_staging")

# If True: only download MR sessions (xnat:mrSessionData). If False: download all experiments.
ONLY_MR_SESSIONS = False

# Prefer resource ZIP downloads (recommended)
PREFER_RESOURCE_ZIPS = True

# If True: keep the downloaded ZIP file on disk
KEEP_ZIPS = False

# If True: extract ZIPs into folders (recommended for easy browsing)
EXTRACT_ZIPS = True

# Skip downloads if local file already exists with same size (when size is known)
SKIP_EXISTING_SAME_SIZE = True

# If a file exists but size differs (or remote size unknown), re-download it
REDOWNLOAD_IF_MISMATCH = True

# Networking
VERIFY_SSL = True
REQUEST_TIMEOUT = 300  # seconds for JSON calls
STREAM_CHUNK_BYTES = 1024 * 1024  # 1 MB
MAX_RETRIES = 4
RETRY_BACKOFF_SECONDS = 2

# =========================
# END USER CONFIG
# =========================


# -------------------------
# helpers
# -------------------------
def _norm_base_url(url: str) -> str:
    url = (url or "").rstrip("/")
    if url.endswith("/data"):
        url = url[:-5]
    return url


def _api(url_base: str, path: str) -> str:
    return _norm_base_url(url_base) + path


def _safe_name(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"[\\/:\*\?\"<>\|]+", "_", s)  # Windows-safe
    s = re.sub(r"\s+", "_", s)
    return s[:200] if len(s) > 200 else s


def _pretty_bytes(n: Optional[int]) -> str:
    if n is None:
        return "?"
    try:
        n = int(n)
    except Exception:
        return str(n)
    units = ["B", "KB", "MB", "GB", "TB"]
    x = float(n)
    i = 0
    while x >= 1024 and i < len(units) - 1:
        x /= 1024
        i += 1
    return f"{x:.1f} {units[i]}"


def _sleep_backoff(attempt: int) -> None:
    time.sleep(RETRY_BACKOFF_SECONDS * (attempt + 1))


# -------------------------
# XNAT REST helpers
# -------------------------
def xnat_get_json(sess: requests.Session, url: str, params: Optional[dict] = None) -> dict:
    last_err = None
    for attempt in range(MAX_RETRIES + 1):
        try:
            r = sess.get(url, params=params, timeout=REQUEST_TIMEOUT)
            if r.status_code >= 400:
                raise RuntimeError(f"GET failed {r.status_code}: {url}\n{r.text[:800]}")
            return r.json()
        except Exception as e:
            last_err = e
            if attempt < MAX_RETRIES:
                _sleep_backoff(attempt)
                continue
            raise RuntimeError(f"Failed to GET/parse JSON after retries: {url}\nLast error: {last_err}") from e


def _resultset_rows(data: dict) -> List[dict]:
    return data.get("ResultSet", {}).get("Result", []) or []


def _download_stream(
    sess: requests.Session,
    url: str,
    out_path: Path,
    expected_size: Optional[int] = None,
    extra_params: Optional[dict] = None,
) -> Tuple[bool, Optional[int], Optional[str]]:
    """
    Returns (ok, bytes_written, error_message).
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Skip-if-same-size logic
    if out_path.exists() and SKIP_EXISTING_SAME_SIZE:
        if expected_size is not None:
            try:
                local_size = out_path.stat().st_size
                if int(local_size) == int(expected_size):
                    return True, local_size, None
            except Exception:
                pass

    last_err = None
    for attempt in range(MAX_RETRIES + 1):
        try:
            with sess.get(url, params=extra_params, stream=True, timeout=REQUEST_TIMEOUT) as r:
                if r.status_code >= 400:
                    raise RuntimeError(f"Download failed {r.status_code}: {url}\n{r.text[:400]}")

                # If remote didn't provide size, try header
                if expected_size is None:
                    cl = r.headers.get("Content-Length")
                    if cl:
                        try:
                            expected_size = int(cl)
                        except Exception:
                            pass

                tmp_path = out_path.with_suffix(out_path.suffix + ".part")
                bytes_written = 0
                with tmp_path.open("wb") as f:
                    for chunk in r.iter_content(chunk_size=STREAM_CHUNK_BYTES):
                        if not chunk:
                            continue
                        f.write(chunk)
                        bytes_written += len(chunk)

                # If we have an expected size, validate
                if expected_size is not None and bytes_written != int(expected_size):
                    if REDOWNLOAD_IF_MISMATCH:
                        tmp_path.unlink(missing_ok=True)
                        raise RuntimeError(
                            f"Size mismatch for {out_path.name}: expected {expected_size}, got {bytes_written}"
                        )

                # Move into place
                tmp_path.replace(out_path)
                return True, bytes_written, None

        except Exception as e:
            last_err = e
            if attempt < MAX_RETRIES:
                _sleep_backoff(attempt)
                continue
            return False, None, str(last_err)

    return False, None, str(last_err)


def _paged_files_list(sess: requests.Session, files_url: str, page_limit: int = 2000) -> List[dict]:
    """
    Robust listing of resource files using paging (offset/limit).
    XNAT commonly supports: ?format=json&limit=N&offset=M
    """
    out: List[dict] = []
    offset = 0
    total = None

    while True:
        params = {"format": "json", "limit": str(page_limit), "offset": str(offset)}
        data = xnat_get_json(sess, files_url, params=params)
        rs = data.get("ResultSet", {}) or {}
        if total is None:
            try:
                total = int(rs.get("totalRecords")) if rs.get("totalRecords") is not None else None
            except Exception:
                total = None

        rows = _resultset_rows(data)
        if not rows:
            break

        out.extend(rows)
        offset += len(rows)

        # Stop conditions
        if total is not None and offset >= total:
            break
        if len(rows) < page_limit:
            break

    return out


def _file_row_info(row: dict) -> Tuple[str, Optional[int], Optional[str]]:
    """
    Returns (relative_path_within_resource, size_bytes, uri)
    """
    rel = (
        row.get("path")
        or row.get("Path")
        or row.get("Name")
        or row.get("name")
        or row.get("file")
        or row.get("filename")
        or ""
    )
    rel = str(rel).lstrip("/")

    size = row.get("Size") or row.get("size") or row.get("file_size") or row.get("fileSize")
    size_i = None
    if size is not None and str(size).strip() != "":
        try:
            size_i = int(float(size))
        except Exception:
            size_i = None

    uri = row.get("URI") or row.get("uri") or row.get("Url") or row.get("url")
    uri_s = str(uri) if uri else None

    return rel, size_i, uri_s


# -------------------------
# XNAT object listing
# -------------------------
def list_subject_experiments(sess: requests.Session, project: str, subject: str) -> List[dict]:
    url = _api(BASE_URL, f"/data/projects/{project}/subjects/{subject}/experiments")
    data = xnat_get_json(sess, url, params={"format": "json", "columns": "ID,label,xsiType,date"})
    rows = _resultset_rows(data)
    if ONLY_MR_SESSIONS:
        rows = [r for r in rows if str(r.get("xsiType", "")).strip() == "xnat:mrSessionData"]
    return rows


def list_experiment_scans(sess: requests.Session, exp_id: str) -> List[dict]:
    url = _api(BASE_URL, f"/data/experiments/{exp_id}/scans")
    data = xnat_get_json(sess, url, params={"format": "json", "columns": "ID,type"})
    return _resultset_rows(data)


def list_resources(sess: requests.Session, xnat_container_url: str) -> List[dict]:
    """
    xnat_container_url examples:
      /data/projects/{p}/subjects/{s}/resources
      /data/experiments/{id}/resources
      /data/experiments/{id}/scans/{scan}/resources
    """
    data = xnat_get_json(sess, xnat_container_url, params={"format": "json"})
    return _resultset_rows(data)


def _resource_label(row: dict) -> str:
    # XNAT returns label under different keys depending on endpoint/version
    for k in (
        "label",
        "xnat_abstractresource_label",
        "xnat:abstractResource/label",
        "xnat_abstractresource_label",
        "name",
    ):
        v = row.get(k)
        if v:
            return str(v)
    return str(row)


# -------------------------
# Download routines
# -------------------------
def download_resource(
    sess: requests.Session,
    resource_files_url: str,
    resource_folder: Path,
    resource_label: str,
    prefer_zip: bool = True,
) -> None:
    """
    resource_files_url should be the .../resources/{label}/files endpoint (no params).
    """
    resource_folder.mkdir(parents=True, exist_ok=True)

    # 1) Try ZIP
    if prefer_zip and PREFER_RESOURCE_ZIPS:
        zip_path = resource_folder / f"{_safe_name(resource_label)}.zip"
        zip_params = {"format": "zip"}  # common XNAT pattern for /files
        print(f"      - ZIP: {zip_path.name}")
        ok, nbytes, err = _download_stream(sess, resource_files_url, zip_path, expected_size=None, extra_params=zip_params)
        if ok:
            print(f"        downloaded {_pretty_bytes(nbytes)}")
            if EXTRACT_ZIPS:
                extract_dir = resource_folder / _safe_name(resource_label)
                extract_dir.mkdir(parents=True, exist_ok=True)
                try:
                    with zipfile.ZipFile(zip_path, "r") as zf:
                        zf.extractall(extract_dir)
                    print(f"        extracted -> {extract_dir}")
                except Exception as e:
                    print(f"        WARNING: ZIP extraction failed ({e}). Keeping ZIP.")
            if not KEEP_ZIPS:
                try:
                    zip_path.unlink(missing_ok=True)
                except Exception:
                    pass
            return
        else:
            print(f"        ZIP download failed; falling back to files. ({err})")

    # 2) File-by-file fallback
    print("      - files (fallback) ...")
    rows = _paged_files_list(sess, resource_files_url, page_limit=2000)
    if not rows:
        print("        (no files)")
        return

    # Save manifest for reproducibility
    manifest_path = resource_folder / f"{_safe_name(resource_label)}__manifest.json"
    try:
        with manifest_path.open("w", encoding="utf-8") as f:
            json.dump(rows, f, indent=2)
    except Exception:
        pass

    n_ok = 0
    n_fail = 0
    for i, row in enumerate(rows, start=1):
        rel, size_i, uri = _file_row_info(row)
        if not rel:
            # If no path is provided, invent something stable-ish
            rel = f"file_{i:05d}"

        local_path = resource_folder / _safe_name(resource_label) / Path(rel)
        if uri:
            file_url = _api(BASE_URL, uri if uri.startswith("/data") else f"/data{uri}" if uri.startswith("/") else f"/data/{uri}")
        else:
            # If no URI, attempt direct path under /files/{rel} (rare)
            file_url = resource_files_url.rstrip("/") + "/" + rel

        ok, nbytes, err = _download_stream(sess, file_url, local_path, expected_size=size_i, extra_params=None)
        if ok:
            n_ok += 1
        else:
            n_fail += 1
            print(f"        !! failed: {rel} ({err})")

        if i % 200 == 0:
            print(f"        progress: {i}/{len(rows)} files...")

    print(f"        done: ok={n_ok}, failed={n_fail}")


def main() -> int:
    out_subject_root = OUTPUT_ROOT / f"{_safe_name(PROJECT_ID)}__{_safe_name(SUBJECT_ID)}"
    out_subject_root.mkdir(parents=True, exist_ok=True)

    print("XNAT download")
    print(f"  Base:    {_norm_base_url(BASE_URL)}")
    print(f"  Project: {PROJECT_ID}")
    print(f"  Subject: {SUBJECT_ID}")
    print(f"  Output:  {out_subject_root}")
    print(f"  MR-only: {ONLY_MR_SESSIONS}")
    print()

    xnat = requests.Session()
    xnat.auth = HTTPBasicAuth(USERNAME, PASSWORD)
    xnat.verify = VERIFY_SSL

    # Auth sanity check
    ping = _api(BASE_URL, "/data/projects")
    r = xnat.get(ping, params={"format": "json"}, timeout=REQUEST_TIMEOUT)
    if r.status_code >= 400:
        print(f"ERROR: Cannot access {ping} ({r.status_code}). Check URL/credentials/SSL.\n{r.text[:400]}")
        return 3

    # -------------------------
    # Subject-level resources
    # -------------------------
    subj_res_url = _api(BASE_URL, f"/data/projects/{PROJECT_ID}/subjects/{SUBJECT_ID}/resources")
    print("Subject resources:")
    try:
        subj_resources = list_resources(xnat, subj_res_url)
    except Exception as e:
        print(f"  WARNING: could not list subject resources ({e})")
        subj_resources = []

    if subj_resources:
        subj_res_root = out_subject_root / "_subject_resources"
        subj_res_root.mkdir(parents=True, exist_ok=True)

        for rr in subj_resources:
            lbl = _resource_label(rr)
            lbl_s = _safe_name(lbl)
            print(f"  - {lbl}")
            files_url = _api(BASE_URL, f"/data/projects/{PROJECT_ID}/subjects/{SUBJECT_ID}/resources/{lbl}/files")
            download_resource(
                xnat,
                files_url,
                resource_folder=subj_res_root / lbl_s,
                resource_label=lbl,
                prefer_zip=True,
            )
    else:
        print("  (none)")
    print()

    # -------------------------
    # Experiments (sessions)
    # -------------------------
    print("Experiments:")
    exps = list_subject_experiments(xnat, PROJECT_ID, SUBJECT_ID)
    if not exps:
        print("  (none found)")
        return 0

    exp_root = out_subject_root / "experiments"
    exp_root.mkdir(parents=True, exist_ok=True)

    # Save experiments list
    try:
        with (out_subject_root / "experiments__list.json").open("w", encoding="utf-8") as f:
            json.dump(exps, f, indent=2)
    except Exception:
        pass

    for exp in exps:
        exp_id = str(exp.get("ID") or "").strip()
        exp_label = str(exp.get("label") or exp_id or "UNKNOWN").strip()
        exp_type = str(exp.get("xsiType") or "").strip()
        exp_date = str(exp.get("date") or "").strip()

        exp_dir = exp_root / f"{_safe_name(exp_label)}__{_safe_name(exp_id)}"
        exp_dir.mkdir(parents=True, exist_ok=True)

        print(f"  === {exp_label} ({exp_id}) [{exp_type}] {exp_date} ===")

        # Experiment-level resources
        print("    Experiment resources:")
        exp_res_url = _api(BASE_URL, f"/data/experiments/{exp_id}/resources")
        try:
            exp_resources = list_resources(xnat, exp_res_url)
        except Exception as e:
            print(f"      WARNING: could not list experiment resources ({e})")
            exp_resources = []

        if exp_resources:
            exp_res_root = exp_dir / "_experiment_resources"
            for rr in exp_resources:
                lbl = _resource_label(rr)
                print(f"      - {lbl}")
                files_url = _api(BASE_URL, f"/data/experiments/{exp_id}/resources/{lbl}/files")
                download_resource(
                    xnat,
                    files_url,
                    resource_folder=exp_res_root / _safe_name(lbl),
                    resource_label=lbl,
                    prefer_zip=True,
                )
        else:
            print("      (none)")

        # Scans + scan resources
        print("    Scans:")
        try:
            scans = list_experiment_scans(xnat, exp_id)
        except Exception as e:
            print(f"      WARNING: could not list scans ({e})")
            scans = []

        if not scans:
            print("      (none)")
            print()
            continue

        scans_root = exp_dir / "scans"
        scans_root.mkdir(parents=True, exist_ok=True)

        # Save scan list
        try:
            with (exp_dir / "scans__list.json").open("w", encoding="utf-8") as f:
                json.dump(scans, f, indent=2)
        except Exception:
            pass

        for sc in scans:
            scan_id = str(sc.get("ID") or "").strip()
            scan_type = str(sc.get("type") or "").strip()
            scan_dir = scans_root / f"{_safe_name(scan_id)}__{_safe_name(scan_type) if scan_type else 'NA'}"
            scan_dir.mkdir(parents=True, exist_ok=True)

            print(f"      - Scan {scan_id} ({scan_type or 'NA'})")
            scan_res_url = _api(BASE_URL, f"/data/experiments/{exp_id}/scans/{scan_id}/resources")
            try:
                scan_resources = list_resources(xnat, scan_res_url)
            except Exception as e:
                print(f"        WARNING: could not list scan resources ({e})")
                scan_resources = []

            if not scan_resources:
                print("        (no resources)")
                continue

            for rr in scan_resources:
                lbl = _resource_label(rr)
                print(f"        * {lbl}")
                files_url = _api(BASE_URL, f"/data/experiments/{exp_id}/scans/{scan_id}/resources/{lbl}/files")
                download_resource(
                    xnat,
                    files_url,
                    resource_folder=scan_dir / "resources" / _safe_name(lbl),
                    resource_label=lbl,
                    prefer_zip=True,
                )

        print()

    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
