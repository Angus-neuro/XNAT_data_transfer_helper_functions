#!/usr/bin/env python3
"""
upload_resources_to_xnat.py

Upload local scan data into a SPECIFIC XNAT project / subject / session.

Modes:
  - MODE="dicom":     upload DICOM-like files to resource "DICOM"
  - MODE="resources": upload each resource subfolder (zip+extract)
  - MODE="both":      do both

Scan mapping:
  1) Folders WITH a leading scan number (for example "016 - ...") are treated as
     already-numbered locally and are NOT matched to existing XNAT scans by type.
  2) Folders WITHOUT a leading scan number may be matched to an existing XNAT scan
     by scan type.
  3) If no type match is made, optionally use a scan number embedded in the folder
     name, but only if that scan ID is not already used by an existing scan.
  4) Otherwise assign new IDs deterministically.

Preflight:
  - Prints whether each folder contains a scan number (prefix/suffix/anywhere) and the chosen candidate.

"""

from __future__ import annotations

import os
import re
import time
import tempfile
import zipfile
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
from requests.auth import HTTPBasicAuth

# =========================
# USER CONFIG
# =========================

USERNAME = os.environ.get("XNAT_USER", "")
PASSWORD = os.environ.get("XNAT_PASS", "")

BASE_URL = ""
PROJECT_ID = ""

# Explicit destination identifiers (user-editable)
SUBJECT_ID = ""
SESSION_LABEL = ""
SESSION_DATE: Optional[str] = None  # "YYYY-MM-DD" or None

# Local directory containing scan folders
INPUT_ROOT = Path(r"")

# Optional: only include scan folders whose names match this regex (None = all dirs)
SCAN_DIR_REGEX: Optional[re.Pattern] = None

# Mode: "dicom", "resources", or "both"
MODE = "dicom"

# Scan ID assignment for folders WITHOUT an existing-type match (or scan number)
SCAN_ID_START = 1

# If True: set scan "type" from folder name (and show that in XNAT UI) when creating NEW scans
SET_SCAN_TYPE_FROM_FOLDER = True

# -------- Preflight scan-number check --------
PREFLIGHT_SCAN_NUMBER_CHECK = True

# How to *interpret* scan numbers embedded in folder names:
#   - "suffix_or_prefix": prefer trailing digits; else leading digits
#   - "prefix_or_suffix": prefer leading digits; else trailing digits
#   - "last_group":       take the last digit group anywhere (useful when neither end is numeric)
SCAN_NUMBER_PICK_MODE = "suffix_or_prefix"

# If True, allow using embedded scan numbers as scan IDs when we cannot match to existing scans by type.
USE_SCAN_NUMBER_IF_NO_TYPE_MATCH = True

# -------------------------
# DICOM MODE SETTINGS
# -------------------------
DICOM_RESOURCE_LABEL = "DICOM"
DICOM_RESOURCE_FORMAT = "DICOM"
DICOM_RESOURCE_CONTENT = "RAW"

# Which files count as "dicom-like" for zipping
DICOM_EXTS = {".dcm", ".ima"}          # case-insensitive
DICOM_INCLUDE_EXTENSIONLESS = False    # set True if your exports have no extensions

# If True, skip DICOM upload if destination DICOM resource already has files
SKIP_IF_DICOM_NONEMPTY = True

# After DICOM upload, optionally call pullDataFromHeaders
PULL_HEADERS_AFTER_DICOM = True
PULL_HEADERS_LEVEL = "session"  # "scan" or "session"

# Post-upload verification/polling (helps avoid pullDataFromHeaders races)
POSTCHECK_DICOM = True
POSTCHECK_WAIT_SECONDS = 120          # poll up to this many seconds after upload
POSTCHECK_POLL_INTERVAL = 5           # seconds
POSTCHECK_LIST_FIRST_N = 20           # print first N filenames
POSTCHECK_MAX_FETCH_FILES = 5000      # safety cap; above this we don't fetch all names

# -------------------------
# RESOURCE MODE SETTINGS
# -------------------------
# Skip these resource folders (case-insensitive labels) during RESOURCE mode
EXCLUDE_RESOURCE_LABELS = {"DICOM", "TEST"}

# If True, skip uploading a resource if it already has >=1 file in XNAT
SKIP_IF_RESOURCE_NONEMPTY = True

# -------------------------
# UPLOAD SETTINGS
# -------------------------
DRY_RUN = False
OVERWRITE_ZIP_FILE = False
VERIFY_SSL = True
REQUEST_TIMEOUT = 300  # seconds

# =========================
# END USER CONFIG
# =========================


# -------------------------
# helpers (names / parsing)
# -------------------------
def _norm_base_url(url: str) -> str:
    url = url.rstrip("/")
    if url.endswith("/data"):
        url = url[:-5]
    return url


def _api(url_base: str, path: str) -> str:
    return _norm_base_url(url_base) + path


def _safe_resource_label(name: str) -> str:
    return re.sub(r"\s+", "_", name.strip())


def _norm_key(s: str) -> str:
    # stable matching: lower + spaces to underscore
    return re.sub(r"\s+", "_", (s or "").strip()).lower()


def _strip_leading_scan_id(folder_name: str) -> str:
    m = re.match(r"^\s*(\d+)(?:$|[_\-\s]+(.*))", folder_name)
    if not m:
        return folder_name
    tail = m.group(2)
    if tail is None or tail.strip() == "":
        return str(m.group(1))
    return tail.strip()


def _leading_scan_id(folder_name: str) -> Optional[int]:
    """
    Return a leading scan number only if the folder clearly begins with one,
    e.g. '016 - something' or '016_something' or just '016'.
    """
    m = re.match(r"^\s*(\d+)(?:$|[_\-\s]+.*)", folder_name)
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None


def _is_dicom_file(p: Path) -> bool:
    suf = p.suffix.lower()
    if suf in DICOM_EXTS:
        return True
    if DICOM_INCLUDE_EXTENSIONLESS and suf == "":
        return True
    return False


def _scan_number_info(name: str) -> Dict[str, object]:
    """
    Extract digit groups and report prefix/suffix candidates.
    Scan numbers are often 1 / 01 / 001 / 0001 etc, typically prefix or suffix.
    """
    s = (name or "").strip()
    prefix_m = re.match(r"^\s*(\d+)(?:$|[_\-\s].*)", s)
    suffix_m = re.match(r"^.*(?:[_\-\s])(\d+)\s*$", s) or re.match(r"^.*?(\d+)\s*$", s)
    groups = re.findall(r"\d+", s)

    prefix = int(prefix_m.group(1)) if prefix_m else None
    suffix = int(suffix_m.group(1)) if suffix_m else None
    last_group = int(groups[-1]) if groups else None

    chosen = None
    mode = (SCAN_NUMBER_PICK_MODE or "").strip().lower()
    if mode == "prefix_or_suffix":
        chosen = prefix if prefix is not None else suffix
    elif mode == "last_group":
        chosen = last_group
    else:  # default: suffix_or_prefix
        chosen = suffix if suffix is not None else prefix

    return {
        "name": s,
        "groups": groups,
        "prefix": prefix,
        "suffix": suffix,
        "last_group": last_group,
        "chosen": chosen,
    }


def _zip_files_sorted(files: List[Path], base_dir: Path, zip_path: Path) -> None:
    items: List[Tuple[str, Path]] = []
    for p in files:
        rel = p.relative_to(base_dir).as_posix()
        items.append((rel, p))
    items.sort(key=lambda t: t[0].lower())

    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for rel, p in items:
            zf.write(p, rel)


def _zip_dir_sorted(src_dir: Path, zip_path: Path) -> None:
    files: List[Tuple[str, Path]] = []
    for p in src_dir.rglob("*"):
        if p.is_file():
            rel = p.relative_to(src_dir).as_posix()
            files.append((rel, p))
    files.sort(key=lambda t: t[0].lower())

    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for rel, p in files:
            zf.write(p, rel)


# -------------------------
# XNAT REST helpers
# -------------------------
def xnat_get_json(sess: requests.Session, url: str, params: Optional[dict] = None) -> dict:
    r = sess.get(url, params=params, timeout=REQUEST_TIMEOUT)
    if r.status_code >= 400:
        raise RuntimeError(f"GET failed {r.status_code}: {url}\n{r.text[:500]}")
    try:
        return r.json()
    except Exception as e:
        raise RuntimeError(f"Failed to parse JSON from {url}: {e}\n{r.text[:500]}")


def xnat_exists(sess: requests.Session, url: str, params: Optional[dict] = None) -> bool:
    r = sess.get(url, params=params, timeout=REQUEST_TIMEOUT)
    if r.status_code == 404:
        return False
    if r.status_code >= 400:
        raise RuntimeError(f"GET failed {r.status_code}: {url}\n{r.text[:500]}")
    return True


def resolve_experiment_id(sess: requests.Session, project: str, experiment_label: str) -> Optional[str]:
    url = _api(BASE_URL, f"/data/projects/{project}/experiments")
    params = {
        "format": "json",
        "columns": "ID,label,xsiType,project",
        "xsiType": "xnat:mrSessionData",
        "label": experiment_label,
        "limit": "*",
    }
    data = xnat_get_json(sess, url, params=params)
    results = data.get("ResultSet", {}).get("Result", [])
    exact = [r for r in results if str(r.get("label", "")) == experiment_label]
    if not exact:
        return None
    for r in exact:
        if str(r.get("project", "")) == project:
            return str(r.get("ID"))
    return str(exact[0].get("ID"))


def list_scans_with_type(sess: requests.Session, experiment_id: str) -> List[Dict[str, str]]:
    """
    Returns list of dicts with keys: ID, type (if present).
    """
    url = _api(BASE_URL, f"/data/experiments/{experiment_id}/scans")
    data = xnat_get_json(sess, url, params={"format": "json", "columns": "ID,type"})
    results = data.get("ResultSet", {}).get("Result", [])
    out: List[Dict[str, str]] = []
    for r in results:
        sid = r.get("ID")
        if sid is None:
            continue
        out.append({"ID": str(sid), "type": str(r.get("type", "") or "")})
    return out


def ensure_subject(sess: requests.Session, project: str, subject_label: str) -> None:
    subj_url = _api(BASE_URL, f"/data/projects/{project}/subjects/{subject_label}")
    if xnat_exists(sess, subj_url, params={"format": "json"}):
        return

    if DRY_RUN:
        print(f"[DRY RUN] Would create subject: project={project} subject={subject_label}")
        return

    params = {"xsiType": "xnat:subjectData", "req_format": "qs"}
    r = sess.put(subj_url, params=params, timeout=REQUEST_TIMEOUT)
    if r.status_code >= 400:
        raise RuntimeError(f"Create subject failed {r.status_code}: {subj_url}\n{r.text[:500]}")


def ensure_session(sess: requests.Session, project: str, subject_label: str, session_label: str, session_date: Optional[str]) -> str:
    exp_id = resolve_experiment_id(sess, project, session_label)
    if exp_id:
        return exp_id

    if DRY_RUN:
        print(f"[DRY RUN] Would create session: project={project} subject={subject_label} session={session_label}")
        return f"DRYRUN_{session_label}"

    put_url = _api(BASE_URL, f"/data/projects/{project}/subjects/{subject_label}/experiments/{session_label}")
    params = {"xsiType": "xnat:mrSessionData", "req_format": "qs"}
    if session_date:
        params["xnat:mrSessionData/date"] = session_date

    r = sess.put(put_url, params=params, timeout=REQUEST_TIMEOUT)
    if r.status_code >= 400:
        raise RuntimeError(f"Create session failed {r.status_code}: {put_url}\n{r.text[:500]}")

    exp_id2 = resolve_experiment_id(sess, project, session_label)
    if not exp_id2:
        raise RuntimeError("Session creation returned but experiment_id could not be resolved.")
    return exp_id2


def ensure_scan(sess: requests.Session, experiment_id: str, scan_id: str, scan_type: Optional[str]) -> None:
    url = _api(BASE_URL, f"/data/experiments/{experiment_id}/scans/{scan_id}")
    r = sess.get(url, params={"format": "json"}, timeout=REQUEST_TIMEOUT)
    if r.status_code == 200:
        return
    if r.status_code != 404 and r.status_code >= 400:
        raise RuntimeError(f"Scan existence check failed {r.status_code}: {url}\n{r.text[:500]}")

    if DRY_RUN or str(experiment_id).startswith("DRYRUN_"):
        print(f"[DRY RUN] Would create scan {scan_id} in experiment {experiment_id} (type={scan_type or 'NA'})")
        return

    put_url = _api(BASE_URL, f"/data/experiments/{experiment_id}/scans/{scan_id}")
    params = {"xsiType": "xnat:mrScanData", "req_format": "qs"}
    if scan_type:
        params["xnat:mrScanData/type"] = scan_type

    rr = sess.put(put_url, params=params, timeout=REQUEST_TIMEOUT)
    if rr.status_code >= 400:
        raise RuntimeError(f"Create scan failed {rr.status_code}: {put_url}\n{rr.text[:500]}")


def ensure_resource_folder(
    sess: requests.Session,
    experiment_id: str,
    scan_id: str,
    resource_label: str,
    fmt: Optional[str] = None,
    content: Optional[str] = None,
) -> None:
    url = _api(BASE_URL, f"/data/experiments/{experiment_id}/scans/{scan_id}/resources/{resource_label}")
    r = sess.get(url, params={"format": "json"}, timeout=REQUEST_TIMEOUT)
    if r.status_code == 200:
        return
    if r.status_code != 404 and r.status_code >= 400:
        raise RuntimeError(f"GET resource check failed {r.status_code}: {url}\n{r.text[:500]}")

    if DRY_RUN or str(experiment_id).startswith("DRYRUN_"):
        print(f"[DRY RUN] Would create resource folder: scan={scan_id} res={resource_label} fmt={fmt} content={content}")
        return

    params: Dict[str, str] = {}
    if fmt:
        params["format"] = str(fmt)
    if content:
        params["content"] = str(content)

    rr = sess.put(url, params=params if params else None, timeout=REQUEST_TIMEOUT)
    if rr.status_code >= 400:
        raise RuntimeError(f"Create resource folder failed {rr.status_code}: {url}\n{r.text[:500]}")


def list_scan_resources(sess: requests.Session, experiment_id: str, scan_id: str) -> List[dict]:
    url = _api(BASE_URL, f"/data/experiments/{experiment_id}/scans/{scan_id}/resources")
    data = xnat_get_json(sess, url, params={"format": "json"})
    return data.get("ResultSet", {}).get("Result", []) or []


def list_resource_files(sess: requests.Session, experiment_id: str, scan_id: str, resource_label: str, limit: Optional[int] = None) -> List[dict]:
    url = _api(BASE_URL, f"/data/experiments/{experiment_id}/scans/{scan_id}/resources/{resource_label}/files")
    params = {"format": "json"}
    if limit is not None:
        params["limit"] = str(int(limit))
    data = xnat_get_json(sess, url, params=params)
    return data.get("ResultSet", {}).get("Result", []) or []


def resource_has_files(sess: requests.Session, experiment_id: str, scan_id: str, resource_label: str) -> bool:
    files = list_resource_files(sess, experiment_id, scan_id, resource_label, limit=1)
    return len(files) > 0


def _file_entry_name(r: dict) -> str:
    for k in ("Name", "name", "path", "Path", "URI", "uri"):
        v = r.get(k)
        if v:
            return str(v)
    return str(r)


def dicom_postcheck_summary(sess: requests.Session, experiment_id: str, scan_id: str) -> Dict[str, object]:
    """
    Return a summary similar to what you printed manually:
      - resource metadata file_count/format/content
      - /files list count and whether zip/non-zip present
      - first N filenames sorted
    """
    resources = list_scan_resources(sess, experiment_id, scan_id)
    dicom_meta = None
    for r in resources:
        lbl = str(r.get("label", r.get("xnat_abstractresource_label", "")) or "")
        if lbl == DICOM_RESOURCE_LABEL:
            dicom_meta = r
            break

    file_count = None
    fmt = None
    content = None
    if dicom_meta:
        for k in ("file_count", "filecount", "FileCount"):
            if k in dicom_meta:
                try:
                    file_count = int(str(dicom_meta.get(k)))
                except Exception:
                    file_count = dicom_meta.get(k)
                break
        fmt = dicom_meta.get("format")
        content = dicom_meta.get("content")

    fetch_limit = None
    if isinstance(file_count, int) and file_count > POSTCHECK_MAX_FETCH_FILES:
        fetch_limit = POSTCHECK_LIST_FIRST_N

    files = list_resource_files(sess, experiment_id, scan_id, DICOM_RESOURCE_LABEL, limit=fetch_limit)
    names = [_file_entry_name(r) for r in files]
    names_sorted = sorted(names, key=lambda s: s.lower())

    has_zip = any(n.lower().endswith(".zip") for n in names)
    non_zip = [n for n in names if not n.lower().endswith(".zip")]
    non_zip_count = len(non_zip)

    return {
        "file_count_meta": file_count,
        "format": fmt,
        "content": content,
        "files_listed": len(files),
        "contains_zip": bool(has_zip),
        "contains_nonzip": bool(non_zip_count > 0),
        "nonzip_count": non_zip_count,
        "first_names_sorted": names_sorted[:POSTCHECK_LIST_FIRST_N],
        "truncated_listing": fetch_limit is not None,
    }


def wait_for_dicom_extraction(sess: requests.Session, experiment_id: str, scan_id: str, expected_min_nonzip: int = 1) -> Dict[str, object]:
    """
    Poll until we see non-zip files in DICOM resource (or timeout).
    This reduces pullDataFromHeaders race conditions.
    """
    t0 = time.time()
    last = {}
    while True:
        try:
            last = dicom_postcheck_summary(sess, experiment_id, scan_id)
            nonzip_ok = bool(last.get("contains_nonzip", False)) and int(last.get("nonzip_count", 0)) >= expected_min_nonzip
            if last.get("truncated_listing", False):
                nonzip_ok = bool(last.get("contains_nonzip", False))
            if nonzip_ok:
                return last
        except Exception as e:
            last = {"error": str(e)}

        if time.time() - t0 >= POSTCHECK_WAIT_SECONDS:
            return last

        time.sleep(POSTCHECK_POLL_INTERVAL)


def upload_zip_as_resource(
    sess: requests.Session,
    experiment_id: str,
    scan_id: str,
    resource_label: str,
    zip_path: Path,
) -> None:
    filename = zip_path.name
    url = _api(
        BASE_URL,
        f"/data/experiments/{experiment_id}/scans/{scan_id}/resources/{resource_label}/files/{filename}",
    )

    if DRY_RUN or str(experiment_id).startswith("DRYRUN_"):
        print(f"[DRY RUN] Upload {zip_path} -> {url}?extract=true")
        return

    params = {"extract": "true"}
    with zip_path.open("rb") as f:
        files = {"file": (filename, f, "application/zip")}
        if OVERWRITE_ZIP_FILE:
            r = sess.post(url, params={**params, "overwrite": "true"}, files=files, timeout=REQUEST_TIMEOUT)
        else:
            r = sess.put(url, params=params, files=files, timeout=REQUEST_TIMEOUT)

    if r.status_code >= 400:
        raise RuntimeError(f"Upload failed {r.status_code}: {url}\n{r.text[:500]}")
    if r.text and r.text.strip():
        print(f"  XNAT response: {r.text.strip()[:200]}")


def pull_data_from_headers_scan(sess: requests.Session, experiment_id: str, scan_id: str) -> None:
    url = _api(BASE_URL, f"/data/experiments/{experiment_id}/scans/{scan_id}")
    if DRY_RUN or str(experiment_id).startswith("DRYRUN_"):
        print(f"[DRY RUN] Would pullDataFromHeaders scan={scan_id}")
        return
    r = sess.put(url, params={"pullDataFromHeaders": "true"}, timeout=REQUEST_TIMEOUT)
    if r.status_code >= 400:
        raise RuntimeError(f"pullDataFromHeaders (scan) failed {r.status_code}: {url}\n{r.text[:500]}")


def pull_data_from_headers_session(sess: requests.Session, experiment_id: str) -> None:
    url = _api(BASE_URL, f"/data/experiments/{experiment_id}")
    if DRY_RUN or str(experiment_id).startswith("DRYRUN_"):
        print(f"[DRY RUN] Would pullDataFromHeaders session={experiment_id}")
        return
    r = sess.put(url, params={"pullDataFromHeaders": "true"}, timeout=REQUEST_TIMEOUT)
    if r.status_code >= 400:
        raise RuntimeError(f"pullDataFromHeaders (session) failed {r.status_code}: {url}\n{r.text[:500]}")


# -------------------------
# local scan folder handling
# -------------------------
def iter_scan_dirs(root: Path) -> List[Path]:
    dirs: List[Path] = []
    for p in root.iterdir():
        if not p.is_dir():
            continue
        if SCAN_DIR_REGEX is not None and not SCAN_DIR_REGEX.search(p.name):
            continue
        dirs.append(p)
    return sorted(dirs, key=lambda p: p.name.lower())


def build_scan_id_map(
    scan_dirs: List[Path],
    existing_scans: List[Dict[str, str]],
) -> Dict[str, str]:
    """
    Option B behaviour:
      - If a local folder has a leading scan number, do NOT type-match it to an existing XNAT scan.
      - Only folders without a leading scan number are eligible for type-matching.
      - Unmatched folders may then use an embedded scan number if allowed and safe.
      - Remaining folders are assigned new IDs deterministically.
    """
    type_to_id: Dict[str, str] = {}
    dup_types: Dict[str, List[str]] = {}
    used_ids: set[int] = set()

    for s in existing_scans:
        sid = s.get("ID", "")
        stype = s.get("type", "") or ""
        try:
            used_ids.add(int(sid))
        except Exception:
            pass
        k = _norm_key(stype)
        if k:
            if k in type_to_id:
                dup_types.setdefault(k, []).extend([type_to_id[k], sid])
            else:
                type_to_id[k] = sid

    # Disable type matching for ambiguous duplicate types
    for k in list(dup_types.keys()):
        type_to_id.pop(k, None)

    scan_id_map: Dict[str, str] = {}
    taken_local: set[str] = set()
    taken_ids_by_local: Dict[str, str] = {}

    # Pass 1: type-match only folders that do NOT already have a leading scan number
    for sdir in scan_dirs:
        name = sdir.name
        if _leading_scan_id(name) is not None:
            continue

        k1 = _norm_key(name)
        k2 = _norm_key(_strip_leading_scan_id(name))
        match = None
        if k1 in type_to_id:
            match = type_to_id[k1]
        elif k2 in type_to_id:
            match = type_to_id[k2]

        if match:
            scan_id_map[name] = str(match)
            taken_local.add(name)
            taken_ids_by_local[name] = str(match)

    # Pass 2: use embedded scan number for anything still unmatched
    if USE_SCAN_NUMBER_IF_NO_TYPE_MATCH:
        for sdir in scan_dirs:
            name = sdir.name
            if name in taken_local:
                continue

            # For already-numbered folders, force use of the leading number
            lead_id = _leading_scan_id(name)
            if lead_id is not None:
                cid = lead_id
            else:
                info = _scan_number_info(name)
                chosen = info.get("chosen", None)
                if chosen is None:
                    continue
                try:
                    cid = int(chosen)
                except Exception:
                    continue

            # Safety: do not map onto an existing XNAT scan ID unless we matched by type
            if cid in used_ids:
                continue

            if str(cid) in taken_ids_by_local.values():
                continue

            scan_id_map[name] = str(cid)
            taken_local.add(name)
            taken_ids_by_local[name] = str(cid)

    # Pass 3: allocate new IDs deterministically for remaining
    remaining = [sdir.name for sdir in scan_dirs if sdir.name not in taken_local]
    remaining = sorted(remaining, key=lambda s: s.lower())

    next_id = max(int(SCAN_ID_START), 1)
    for name in remaining:
        while next_id in used_ids or str(next_id) in taken_ids_by_local.values():
            next_id += 1
        scan_id_map[name] = str(next_id)
        taken_ids_by_local[name] = str(next_id)
        used_ids.add(next_id)
        next_id += 1

    return scan_id_map


def _print_postcheck(exp_id: str, scan_id: str, summary: Dict[str, object]) -> None:
    if "error" in summary:
        print(f"  Postcheck ERROR: {summary['error']}")
        return

    print(f"  DICOM file_count: {summary.get('file_count_meta')}")
    print(f"  DICOM format:     {summary.get('format')}")
    print(f"  DICOM content:    {summary.get('content')}")
    print()
    print(f"  Files listed by /files?format=json: {summary.get('files_listed')}")
    print(f"  Contains .zip:                     {summary.get('contains_zip')}")
    print(f"  Contains non-zip files:            {summary.get('contains_nonzip')} (count={summary.get('nonzip_count')})")

    if summary.get("contains_nonzip"):
        print("  NOTE: Non-zip files are present (consistent with extraction having produced files).")
    else:
        print("  NOTE: No non-zip files yet (may still be extracting/cataloguing).")

    if summary.get("truncated_listing"):
        print("  NOTE: File listing truncated for safety (resource is large).")

    names = summary.get("first_names_sorted") or []
    if names:
        print()
        print(f"  First {len(names)} filenames (sorted):")
        for n in names:
            print(f"    - {n}")


def main() -> int:
    if not INPUT_ROOT.exists():
        print(f"ERROR: INPUT_ROOT does not exist: {INPUT_ROOT}")
        return 2

    if not SUBJECT_ID or not SESSION_LABEL:
        print("ERROR: SUBJECT_ID and SESSION_LABEL must be set in USER CONFIG.")
        return 2

    mode = (MODE or "").strip().lower()
    if mode not in {"dicom", "resources", "both"}:
        print("ERROR: MODE must be one of: dicom, resources, both")
        return 2

    base = _norm_base_url(BASE_URL)
    print(f"XNAT base:   {base}")
    print(f"Project:     {PROJECT_ID}")
    print(f"Subject ID:  {SUBJECT_ID}")
    print(f"Session:     {SESSION_LABEL}")
    print(f"SessionDate: {SESSION_DATE or 'None'}")
    print(f"Local:       {INPUT_ROOT}")
    print(f"Dry run:     {DRY_RUN}")
    print(f"Mode:        {mode}")
    print()

    scan_dirs = iter_scan_dirs(INPUT_ROOT)
    if not scan_dirs:
        msg = "No scan folders found under INPUT_ROOT"
        if SCAN_DIR_REGEX is not None:
            msg += f" matching SCAN_DIR_REGEX={SCAN_DIR_REGEX.pattern}"
        print(msg)
        return 1

    if PREFLIGHT_SCAN_NUMBER_CHECK:
        print("Preflight: scan-number presence in folder names")
        missing = 0
        for sdir in scan_dirs:
            info = _scan_number_info(sdir.name)
            groups = info["groups"]
            prefix = info["prefix"]
            suffix = info["suffix"]
            chosen = info["chosen"]
            lead = _leading_scan_id(sdir.name)
            if not groups:
                missing += 1
                print(f"  - {sdir.name}: NO DIGITS FOUND")
            else:
                print(
                    f"  - {sdir.name}: groups={groups} | prefix={prefix} | suffix={suffix} "
                    f"| chosen({SCAN_NUMBER_PICK_MODE})={chosen} | leading_scan_id={lead}"
                )
        if missing:
            print(f"WARNING: {missing} folder(s) had no digits at all; they will rely on type-match or new ID allocation.")
        print()

    xnat = requests.Session()
    xnat.auth = HTTPBasicAuth(USERNAME, PASSWORD)
    xnat.verify = VERIFY_SSL

    ping = _api(BASE_URL, "/data/projects")
    r = xnat.get(ping, params={"format": "json"}, timeout=REQUEST_TIMEOUT)
    if r.status_code >= 400:
        print(f"ERROR: Cannot access {ping} ({r.status_code}). Check URL/credentials/SSL.\n{r.text[:300]}")
        return 3

    try:
        ensure_subject(xnat, PROJECT_ID, SUBJECT_ID)
        exp_id = ensure_session(xnat, PROJECT_ID, SUBJECT_ID, SESSION_LABEL, SESSION_DATE)
    except Exception as e:
        print(f"ERROR: failed to ensure subject/session: {e}")
        return 4

    print(f"XNAT experiment ID: {exp_id}")

    existing_scans: List[Dict[str, str]] = []
    if not str(exp_id).startswith("DRYRUN_"):
        try:
            existing_scans = list_scans_with_type(xnat, exp_id)
        except Exception as e:
            print(f"WARNING: could not list existing scans ({e}); proceeding with empty existing-scan list.")
            existing_scans = []

    scan_id_map = build_scan_id_map(scan_dirs, existing_scans)

    print("Scan mapping (local folder -> XNAT scan ID):")
    for sdir in scan_dirs:
        print(f"  - {sdir.name} -> {scan_id_map[sdir.name]}")
    print()

    dicom_uploaded_any = False

    for sdir in scan_dirs:
        scan_folder_name = sdir.name
        scan_id = scan_id_map[scan_folder_name]

        scan_type = None
        if SET_SCAN_TYPE_FROM_FOLDER:
            scan_type = _strip_leading_scan_id(scan_folder_name)

        print(f"=== Scan folder {scan_folder_name} -> XNAT scan {scan_id} ===")

        try:
            ensure_scan(xnat, exp_id, scan_id, scan_type=scan_type)
        except Exception as e:
            print(f"  ERROR: could not ensure scan {scan_id}: {e}")
            print()
            continue

        # -------------------------
        # DICOM MODE
        # -------------------------
        if mode in {"dicom", "both"}:
            dicom_files = [p for p in sdir.rglob("*") if p.is_file() and _is_dicom_file(p)]
            dicom_files = sorted(dicom_files, key=lambda p: p.relative_to(sdir).as_posix().lower())

            if not dicom_files:
                print("  (No DICOM-like files found for DICOM mode)")
            else:
                try:
                    ensure_resource_folder(
                        xnat,
                        exp_id,
                        scan_id,
                        DICOM_RESOURCE_LABEL,
                        fmt=DICOM_RESOURCE_FORMAT,
                        content=DICOM_RESOURCE_CONTENT,
                    )
                except Exception as e:
                    print(f"  !! Could not ensure DICOM resource folder: {e}")

                skipped_due_to_nonempty = False
                if SKIP_IF_DICOM_NONEMPTY and not str(exp_id).startswith("DRYRUN_"):
                    try:
                        if resource_has_files(xnat, exp_id, scan_id, DICOM_RESOURCE_LABEL):
                            print(f"  * DICOM: resource already has files; skipping upload")
                            skipped_due_to_nonempty = True
                    except Exception as e:
                        print(f"  !! Could not check DICOM resource contents ({e}); will try upload anyway")

                if not skipped_due_to_nonempty:
                    with tempfile.TemporaryDirectory() as td:
                        zip_path = Path(td) / f"{DICOM_RESOURCE_LABEL}_scan{scan_id}.zip"
                        _zip_files_sorted(dicom_files, base_dir=sdir, zip_path=zip_path)
                        size_mb = zip_path.stat().st_size / 1e6
                        print(f"  * Uploading DICOM ({len(dicom_files)} files) to resource '{DICOM_RESOURCE_LABEL}' ({size_mb:.1f} MB)")
                        try:
                            upload_zip_as_resource(xnat, exp_id, scan_id, DICOM_RESOURCE_LABEL, zip_path)
                            dicom_uploaded_any = True
                        except Exception as e:
                            print(f"  !! DICOM upload failed for scan {scan_id}: {e}")

                if POSTCHECK_DICOM and not str(exp_id).startswith("DRYRUN_"):
                    try:
                        print("  Postcheck: DICOM resource status")
                        summary = wait_for_dicom_extraction(
                            xnat,
                            exp_id,
                            scan_id,
                            expected_min_nonzip=1,
                        )
                        _print_postcheck(exp_id, scan_id, summary)
                    except Exception as e:
                        print(f"  Postcheck WARNING: {e}")

                if PULL_HEADERS_AFTER_DICOM and PULL_HEADERS_LEVEL == "scan":
                    try:
                        pull_data_from_headers_scan(xnat, exp_id, scan_id)
                        print(f"  Called pullDataFromHeaders at scan level for scan {scan_id}")
                    except Exception as e:
                        print(f"  WARNING: pullDataFromHeaders (scan) failed: {e}")

        # -------------------------
        # RESOURCE MODE
        # -------------------------
        if mode in {"resources", "both"}:
            resource_dirs = [p for p in sdir.iterdir() if p.is_dir()]
            resource_dirs = sorted(resource_dirs, key=lambda p: p.name.lower())

            if not resource_dirs:
                print("  (No resource subfolders found for RESOURCE mode)")
            else:
                for rdir in resource_dirs:
                    resource_label_raw = rdir.name
                    if resource_label_raw.upper() in {x.upper() for x in EXCLUDE_RESOURCE_LABELS}:
                        continue

                    resource_label = _safe_resource_label(resource_label_raw)

                    if SKIP_IF_RESOURCE_NONEMPTY and not str(exp_id).startswith("DRYRUN_"):
                        try:
                            if resource_has_files(xnat, exp_id, scan_id, resource_label):
                                print(f"  * {resource_label_raw}: resource '{resource_label}' already has files; skipping")
                                continue
                        except Exception as e:
                            print(f"  !! Could not check resource '{resource_label}' contents ({e}); will try upload anyway")

                    try:
                        ensure_resource_folder(xnat, exp_id, scan_id, resource_label)
                    except Exception as e:
                        print(f"  !! Could not ensure resource folder '{resource_label}': {e}")

                    with tempfile.TemporaryDirectory() as td:
                        zip_path = Path(td) / f"{resource_label}.zip"
                        _zip_dir_sorted(rdir, zip_path)
                        size_mb = zip_path.stat().st_size / 1e6
                        print(f"  * Uploading '{resource_label_raw}' as resource '{resource_label}' ({size_mb:.1f} MB)")
                        try:
                            upload_zip_as_resource(xnat, exp_id, scan_id, resource_label, zip_path)
                        except Exception as e:
                            print(f"  !! Upload failed for scan {scan_id} resource {resource_label}: {e}")

        print()

    if mode in {"dicom", "both"} and PULL_HEADERS_AFTER_DICOM and PULL_HEADERS_LEVEL == "session":
        if dicom_uploaded_any and not str(exp_id).startswith("DRYRUN_"):
            try:
                pull_data_from_headers_session(xnat, exp_id)
                print(f"Called pullDataFromHeaders at session level for {exp_id}")
            except Exception as e:
                print(f"WARNING: pullDataFromHeaders (session) failed: {e}")

    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())