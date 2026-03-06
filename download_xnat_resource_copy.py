#!/usr/bin/env python3
"""
download_xnat_resource.py

Download ONE specific XNAT resource (subject / experiment / scan level) to a local folder.

Usage 
---------------
1) Edit USER CONFIG below 
2) Run:
   python download_xnat_resource.py

Notes
-----
- For experiment-level / scan-level resources: XNAT experiment ID is used under /data/experiments/{ID}/...
- The script enforces UNIQUE matches for experiment/scan/resource when using substring matching.

Requires:
  pip install requests
Optional:
  pip install tqdm
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import sys
import time
import zipfile
from pathlib import Path
from typing import List, Optional, Tuple

import requests
from requests.auth import HTTPBasicAuth

try:
    from tqdm import tqdm  # type: ignore
except Exception:
    tqdm = None


# =========================
# USER CONFIG (edit these)
# =========================

USERNAME = os.environ.get("XNAT_USER", "")
PASSWORD = os.environ.get("XNAT_PASS", "")

BASE_URL = ""

PROJECT_ID = ""
SUBJECT_ID = ""  # will be prompted if empty
SESSION_ID = ""  # MR session label or experiment ID (resolved within PROJECT_ID + SUBJECT_ID)
SCAN_ID = ""     # scan numeric ID (e.g. "38")

# Resource identification
RESOURCE_LABEL = ""   # exact label preferred (e.g. "DICOM", "NIFTI", "T1w", "SNAPSHOTS", etc.)
RESOURCE_MATCH = ""   # substring match if you don't know exact label (must be unique)

# Where to download (root)
OUTPUT_ROOT = Path(r"")

# ZIP-first download (recommended)
PREFER_RESOURCE_ZIPS = True
KEEP_ZIPS = False
EXTRACT_ZIPS = True

# Skip downloads if local file already exists with same size (when size is known)
SKIP_EXISTING_SAME_SIZE = True
REDOWNLOAD_IF_MISMATCH = True

# Networking
VERIFY_SSL = True
REQUEST_TIMEOUT = 300
STREAM_CHUNK_BYTES = 1024 * 1024
MAX_RETRIES = 4
RETRY_BACKOFF_SECONDS = 2

# If True, when resolving SESSION_ID only consider MR sessions (xnat:mrSessionData)
ONLY_MR_SESSIONS = True

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


def _safe_rel_parts(posix_path: str) -> Path:
    parts = [p for p in posix_path.split("/") if p not in ("", ".", "..")]
    safe_parts = [_safe_name(p) for p in parts]
    return Path(*safe_parts)


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
    show_progress: bool = True,
) -> Tuple[bool, Optional[int], Optional[str]]:
    """
    Stream-download URL to out_path.

    Returns (ok, bytes_written, error_message).

    If tqdm is installed and show_progress=True, shows a byte-level progress bar.
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if out_path.exists() and SKIP_EXISTING_SAME_SIZE and expected_size is not None:
        try:
            if int(out_path.stat().st_size) == int(expected_size):
                return True, expected_size, None
        except Exception:
            pass

    last_err = None
    for attempt in range(MAX_RETRIES + 1):
        pbar = None
        try:
            with sess.get(url, params=extra_params, stream=True, timeout=REQUEST_TIMEOUT) as r:
                if r.status_code >= 400:
                    raise RuntimeError(f"Download failed {r.status_code}: {url}\n{r.text[:400]}")

                if expected_size is None:
                    cl = r.headers.get("Content-Length")
                    if cl:
                        try:
                            expected_size = int(cl)
                        except Exception:
                            expected_size = None

                use_pbar = (tqdm is not None) and show_progress
                if use_pbar:
                    pbar = tqdm(
                        total=expected_size,
                        unit="B",
                        unit_scale=True,
                        unit_divisor=1024,
                        desc=out_path.name,
                        leave=False,
                    )

                tmp_path = out_path.with_suffix(out_path.suffix + ".part")
                bytes_written = 0
                with tmp_path.open("wb") as f:
                    for chunk in r.iter_content(chunk_size=STREAM_CHUNK_BYTES):
                        if not chunk:
                            continue
                        f.write(chunk)
                        bytes_written += len(chunk)
                        if pbar is not None:
                            pbar.update(len(chunk))

                if pbar is not None:
                    pbar.close()
                    pbar = None

                if expected_size is not None and bytes_written != int(expected_size):
                    if REDOWNLOAD_IF_MISMATCH:
                        tmp_path.unlink(missing_ok=True)
                        raise RuntimeError(
                            f"Size mismatch for {out_path.name}: expected {expected_size}, got {bytes_written}"
                        )

                tmp_path.replace(out_path)
                return True, bytes_written, None

        except Exception as e:
            last_err = e
            try:
                if pbar is not None:
                    pbar.close()
            except Exception:
                pass

            if attempt < MAX_RETRIES:
                _sleep_backoff(attempt)
                continue
            return False, None, str(last_err)

    return False, None, str(last_err)


def _paged_files_list(sess: requests.Session, files_url: str, page_limit: int = 2000) -> List[dict]:
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

        if total is not None and offset >= total:
            break
        if len(rows) < page_limit:
            break

    return out


def _file_row_info(row: dict) -> Tuple[str, Optional[int], Optional[str]]:
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
# XNAT listing
# -------------------------
def list_subject_experiments(sess: requests.Session, base_url: str, project: str, subject: str) -> List[dict]:
    url = _api(base_url, f"/data/projects/{project}/subjects/{subject}/experiments")
    cols = "ID,label,xsiType,date"
    data = xnat_get_json(sess, url, params={"format": "json", "columns": cols})
    return _resultset_rows(data)


def list_experiment_scans(sess: requests.Session, base_url: str, exp_id: str) -> List[dict]:
    url = _api(base_url, f"/data/experiments/{exp_id}/scans")
    data = xnat_get_json(sess, url, params={"format": "json", "columns": "ID,type"})
    return _resultset_rows(data)


def list_scan_resources(sess: requests.Session, base_url: str, exp_id: str, scan_id: str) -> List[dict]:
    url = _api(base_url, f"/data/experiments/{exp_id}/scans/{scan_id}/resources")
    data = xnat_get_json(sess, url, params={"format": "json"})
    return _resultset_rows(data)


def _resource_label(row: dict) -> str:
    for k in ("label", "xnat_abstractresource_label", "xnat:abstractResource/label", "name"):
        v = row.get(k)
        if v:
            return str(v)
    return str(row)


def _pick_unique_match(candidates: List[Tuple[str, str]], query: str, what: str) -> str:
    q = (query or "").strip().lower()
    if not q:
        raise RuntimeError(f"{what}: no query provided.")

    exact = [cid for (disp, cid) in candidates if disp.lower() == q or cid.lower() == q]
    if len(exact) == 1:
        return exact[0]
    if len(exact) > 1:
        raise RuntimeError(f"{what}: exact match was not unique for '{query}'.")

    hits = [cid for (disp, cid) in candidates if q in disp.lower() or q in cid.lower()]
    if len(hits) == 1:
        return hits[0]
    if len(hits) == 0:
        preview = "\n".join([f"  - {disp} [{cid}]" for disp, cid in candidates[:50]])
        raise RuntimeError(f"{what}: no matches for '{query}'. Available (first 50):\n{preview}")
    preview = "\n".join([f"  - {disp} [{cid}]" for disp, cid in candidates[:50]])
    raise RuntimeError(f"{what}: match for '{query}' was ambiguous ({len(hits)} hits). Candidates (first 50):\n{preview}")


# -------------------------
# ZIP extraction: flatten to after /files/
# -------------------------
def _extract_xnat_zip_flat(zip_path: Path, dest_dir: Path) -> None:
    """
    Extract an XNAT resource ZIP into dest_dir, stripping any internal prefix up to the last '/files/'.
    Includes zip-slip protection.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(zip_path, "r") as zf:
        for info in zf.infolist():
            if info.is_dir():
                continue

            name = info.filename.replace("\\", "/")
            low = name.lower()

            cut = low.rfind("/files/")
            if cut != -1:
                rel_posix = name[cut + len("/files/") :]
            else:
                rel_posix = name.split("/")[-1]

            rel_path = _safe_rel_parts(rel_posix)
            if str(rel_path) in ("", "."):
                continue

            out_path = (dest_dir / rel_path).resolve()
            dest_root = dest_dir.resolve()

            if not str(out_path).startswith(str(dest_root)):
                continue

            out_path.parent.mkdir(parents=True, exist_ok=True)
            with zf.open(info, "r") as src, open(out_path, "wb") as dst:
                shutil.copyfileobj(src, dst)


def download_resource(
    sess: requests.Session,
    base_url: str,
    resource_files_url: str,
    resource_folder: Path,
    resource_label: str,
) -> None:
    resource_folder.mkdir(parents=True, exist_ok=True)

    # 1) ZIP first (byte progress bar)
    if PREFER_RESOURCE_ZIPS:
        zip_path = resource_folder / f"{_safe_name(resource_label)}.zip"
        print(f"  - ZIP: {zip_path.name}")
        ok, nbytes, err = _download_stream(
            sess,
            resource_files_url,
            zip_path,
            expected_size=None,
            extra_params={"format": "zip"},
            show_progress=True,
        )
        if ok:
            print(f"    downloaded {_pretty_bytes(nbytes)}")
            if EXTRACT_ZIPS:
                try:
                    _extract_xnat_zip_flat(zip_path, resource_folder)
                    print(f"    extracted -> {resource_folder}")
                except Exception as e:
                    print(f"    WARNING: ZIP extraction failed ({e}). Keeping ZIP.")
            if not KEEP_ZIPS:
                try:
                    zip_path.unlink(missing_ok=True)
                except Exception:
                    pass
            return
        else:
            print(f"    ZIP download failed; falling back to files. ({err})")

    # 2) fallback: file-by-file
    print("  - files (fallback) ...")
    rows = _paged_files_list(sess, resource_files_url, page_limit=2000)
    if not rows:
        print("    (no files)")
        return

    try:
        with (resource_folder / f"{_safe_name(resource_label)}__manifest.json").open("w", encoding="utf-8") as f:
            json.dump(rows, f, indent=2)
    except Exception:
        pass

    iterable = rows
    if tqdm is not None:
        iterable = tqdm(rows, desc="Downloading files", unit="file")

    n_ok = 0
    n_fail = 0
    for i, row in enumerate(iterable, start=1):
        rel, size_i, uri = _file_row_info(row)
        if not rel:
            rel = f"file_{i:05d}"

        local_rel = _safe_rel_parts(rel.replace("\\", "/"))
        local_path = resource_folder / local_rel
        local_path.parent.mkdir(parents=True, exist_ok=True)

        if uri:
            if uri.startswith("/data"):
                file_url = _api(base_url, uri)
            elif uri.startswith("/"):
                file_url = _api(base_url, "/data" + uri)
            else:
                file_url = _api(base_url, "/data/" + uri)
        else:
            file_url = resource_files_url.rstrip("/") + "/" + rel

        ok, _, err = _download_stream(
            sess,
            file_url,
            local_path,
            expected_size=size_i,
            extra_params=None,
            show_progress=False,
        )
        if ok:
            n_ok += 1
        else:
            n_fail += 1
            print(f"    !! failed: {rel} ({err})")

    print(f"    done: ok={n_ok}, failed={n_fail}")


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Download one scan-level XNAT resource using project + subject + session + scan.")
    p.add_argument("--base-url", default=None)
    p.add_argument("--project", default=None)
    p.add_argument("--subject", default=None)
    p.add_argument("--session", default=None, help="Session label or experiment ID (resolved within project+subject).")
    p.add_argument("--scan", default=None, help="Scan ID (e.g. 38).")
    p.add_argument("--resource", default=None, help="Exact resource label (preferred).")
    p.add_argument("--resource-match", default=None, help="Substring match for resource label.")
    p.add_argument("--out", default=None)

    p.add_argument("--no-zip", action="store_true")
    p.add_argument("--keep-zips", action="store_true")
    p.add_argument("--no-extract", action="store_true")
    p.add_argument("--insecure", action="store_true", help="Disable SSL verification.")
    p.add_argument("--all-experiments", action="store_true", help="Do not restrict session resolution to MR sessions.")
    return p


def _prompt_if_missing(label: str, current: str) -> str:
    if current.strip():
        return current.strip()
    try:
        v = input(f"Enter {label}: ").strip()
    except EOFError:
        v = ""
    if not v:
        raise RuntimeError(f"Missing {label}. Provide it in config or via CLI.")
    return v


def main() -> int:
    args = _build_parser().parse_args()

    base_url = args.base_url or BASE_URL
    project = (args.project or PROJECT_ID).strip()
    subject = (args.subject or SUBJECT_ID).strip()
    session_id = (args.session or SESSION_ID).strip()
    scan_id = (args.scan or SCAN_ID).strip()

    resource_label = (args.resource or RESOURCE_LABEL).strip()
    resource_match = (args.resource_match or RESOURCE_MATCH).strip()

    out_root = Path(args.out) if args.out else OUTPUT_ROOT

    global PREFER_RESOURCE_ZIPS, KEEP_ZIPS, EXTRACT_ZIPS, ONLY_MR_SESSIONS
    if args.no_zip:
        PREFER_RESOURCE_ZIPS = False
    if args.keep_zips:
        KEEP_ZIPS = True
    if args.no_extract:
        EXTRACT_ZIPS = False
    if args.all_experiments:
        ONLY_MR_SESSIONS = False

    verify_ssl = VERIFY_SSL and (not args.insecure)

    # Prompt for subject if missing (as requested)
    try:
        if not project:
            project = _prompt_if_missing("PROJECT_ID", project)
        if not subject:
            subject = _prompt_if_missing("SUBJECT_ID", subject)
        if not session_id:
            session_id = _prompt_if_missing("SESSION_ID (session label or experiment ID)", session_id)
        if not scan_id:
            scan_id = _prompt_if_missing("SCAN_ID", scan_id)
    except Exception as e:
        print(f"ERROR: {e}")
        return 2

    if not (resource_label or resource_match):
        print("ERROR: Set RESOURCE_LABEL or RESOURCE_MATCH (or CLI --resource/--resource-match).")
        return 2
    if not USERNAME or not PASSWORD:
        print("ERROR: Missing credentials. Set env vars XNAT_USER and XNAT_PASS (or edit config).")
        return 2

    print("XNAT targeted scan-resource download")
    print(f"  Base:    {_norm_base_url(base_url)}")
    print(f"  Project: {project}")
    print(f"  Subject: {subject}")
    print(f"  Session: {session_id}")
    print(f"  Scan:    {scan_id}")
    print()

    xnat = requests.Session()
    xnat.auth = HTTPBasicAuth(USERNAME, PASSWORD)
    xnat.verify = verify_ssl

    # Auth sanity check
    ping = _api(base_url, "/data/projects")
    r = xnat.get(ping, params={"format": "json"}, timeout=REQUEST_TIMEOUT)
    if r.status_code >= 400:
        print(f"ERROR: Cannot access {ping} ({r.status_code}). Check URL/credentials/SSL.\n{r.text[:400]}")
        return 3

    # Resolve SESSION_ID -> experiment ID within the SUBJECT (avoids collisions)
    exps = list_subject_experiments(xnat, base_url, project, subject)
    if ONLY_MR_SESSIONS:
        exps = [e for e in exps if str(e.get("xsiType", "")).strip() == "xnat:mrSessionData"]

    if not exps:
        print("ERROR: No experiments found for that subject (after filtering).")
        return 4

    exp_candidates: List[Tuple[str, str]] = []
    for e in exps:
        eid = str(e.get("ID") or "").strip()
        elab = str(e.get("label") or "").strip()
        disp = elab if elab else eid
        if eid:
            exp_candidates.append((disp, eid))

    try:
        exp_id = _pick_unique_match(exp_candidates, session_id, "Session (experiment)")
    except Exception as e:
        print(f"ERROR resolving session '{session_id}': {e}")
        return 4

    exp_label = None
    for disp, eid in exp_candidates:
        if eid == exp_id:
            exp_label = disp
            break

    print(f"Resolved session -> experiment ID: {exp_id} (label: {exp_label or exp_id})")

    # Resolve scan
    scans = list_experiment_scans(xnat, base_url, exp_id)
    if not scans:
        print("ERROR: No scans found in this session.")
        return 4

    scan_candidates: List[Tuple[str, str]] = []
    for s in scans:
        sid = str(s.get("ID") or "").strip()
        st = str(s.get("type") or "").strip()
        disp = f"{sid} ({st})" if st else sid
        if sid:
            scan_candidates.append((disp, sid))

    try:
        resolved_scan_id = _pick_unique_match(scan_candidates, scan_id, "Scan")
    except Exception as e:
        print(f"ERROR resolving scan '{scan_id}': {e}")
        return 4

    print(f"Resolved scan: {resolved_scan_id}")

    # List resources on the scan
    resources = list_scan_resources(xnat, base_url, exp_id, resolved_scan_id)
    if not resources:
        print("ERROR: No resources found under that scan.")
        return 5

    res_candidates = [(_resource_label(rr), _resource_label(rr)) for rr in resources]
    try:
        if resource_label:
            chosen_res = _pick_unique_match(res_candidates, resource_label, "Resource")
        else:
            chosen_res = _pick_unique_match(res_candidates, resource_match, "Resource")
    except Exception as e:
        print(f"ERROR resolving resource: {e}")
        return 5

    print(f"Selected resource: {chosen_res}")

    # XNAT download URL
    files_url = _api(base_url, f"/data/experiments/{exp_id}/scans/{resolved_scan_id}/resources/{chosen_res}/files")

    # Desired local layout:
    #   OUTPUT_ROOT / PROJECT_ID / SESSION_ID / SCAN_ID / RESOURCE
    dest_folder = out_root / _safe_name(project) / _safe_name(session_id) / _safe_name(resolved_scan_id) / _safe_name(chosen_res)
    dest_folder.mkdir(parents=True, exist_ok=True)

    # Write a tiny provenance file (since subject is not in the path)
    try:
        with (dest_folder / "_xnat_source.txt").open("w", encoding="utf-8") as f:
            f.write(f"base_url={_norm_base_url(base_url)}\n")
            f.write(f"project={project}\n")
            f.write(f"subject={subject}\n")
            f.write(f"session_input={session_id}\n")
            f.write(f"experiment_id={exp_id}\n")
            f.write(f"scan_id={resolved_scan_id}\n")
            f.write(f"resource={chosen_res}\n")
            f.write(f"files_url={files_url}\n")
    except Exception:
        pass

    print(f"Download URL: {files_url}")
    print(f"Destination:  {dest_folder}")

    download_resource(
        xnat,
        base_url=base_url,
        resource_files_url=files_url,
        resource_folder=dest_folder,
        resource_label=chosen_res,
    )

    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())