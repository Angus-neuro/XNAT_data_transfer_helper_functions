#!/usr/bin/env python3
"""
batch_upload_nifti_resources_to_xnat.py

Batch-upload NIFTI scan resources to XNAT for a whole folder of subjects.

Assumed local structure
-----------------------
INPUT_ROOT/
  SUBJECT001/
    scan_folder_A/
      NIFTI/
        ...
    scan_folder_B/
      NIFTI/
        ...
  SUBJECT002/
    scan_folder_A/
      NIFTI/
        ...

For each subject:
  - SUBJECT_ID   = subject folder name
  - SESSION_LABEL = f"{SUBJECT_ID}_MR_001"

Behaviour
---------
- Walk subject folders under INPUT_ROOT
- Create subject if missing
- Create MR session if missing
- Determine scan IDs using the same logic as your original script:
    * use existing type-match where possible
    * otherwise use embedded scan number if safe
    * otherwise allocate deterministically
- Upload the local NIFTI folder into the XNAT scan resource "NIFTI"
- Skip upload if destination NIFTI resource already contains files (optional)
- Print progress at subject / scan / resource level

This script is intentionally focused on resource upload for NIFTI folders.

Credentials
-----------
- Username and password are prompted at runtime via pop-up windows.
"""

from __future__ import annotations

import getpass
import re
import time
import tempfile
import zipfile
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
from requests.auth import HTTPBasicAuth

# Optional progress bars
try:
    from tqdm import tqdm  # type: ignore
except Exception:
    tqdm = None


# =========================
# USER CONFIG
# =========================

BASE_URL = ""
PROJECT_ID = ""

# Root folder containing subject folders
INPUT_ROOT = Path(r"")

# Subject folder filter (None = all subject folders)
SUBJECT_DIR_REGEX: Optional[re.Pattern] = None

# Optional: only include scan folders whose names match this regex (None = all dirs)
SCAN_DIR_REGEX: Optional[re.Pattern] = None

# Session label is derived as: SUBJECT_ID + SESSION_SUFFIX
SESSION_SUFFIX = ""
SESSION_DATE: Optional[str] = None  # "YYYY-MM-DD" or None

# Upload only this resource folder from each scan directory
TARGET_RESOURCE_DIR_NAME = "NIFTI"
TARGET_RESOURCE_LABEL = "NIFTI"

# If True: set XNAT scan type from folder name when creating a new scan
SET_SCAN_TYPE_FROM_FOLDER = True

# Scan ID assignment
SCAN_ID_START = 1

# -------- Preflight scan-number check --------
PREFLIGHT_SCAN_NUMBER_CHECK = True

# How to interpret scan numbers embedded in folder names:
#   - "suffix_or_prefix": prefer trailing digits; else leading digits
#   - "prefix_or_suffix": prefer leading digits; else trailing digits
#   - "last_group":       take the last digit group anywhere
SCAN_NUMBER_PICK_MODE = "suffix_or_prefix"

# If True, allow using embedded scan numbers as scan IDs when there is no type match
USE_SCAN_NUMBER_IF_NO_TYPE_MATCH = True

# -------------------------
# RESOURCE MODE SETTINGS
# -------------------------
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


class CredentialPromptCancelled(Exception):
    """Raised when the user cancels credential entry."""


# -------------------------
# credential prompt helpers
# -------------------------
def _prompt_credentials_gui(base_url: str) -> Tuple[str, str]:
    """
    Prompt for username/password using pop-up windows.
    """
    import tkinter as tk
    from tkinter import messagebox, simpledialog

    root = tk.Tk()
    root.withdraw()

    try:
        root.attributes("-topmost", True)
    except Exception:
        pass

    try:
        while True:
            username = simpledialog.askstring(
                title="XNAT Login",
                prompt=f"Enter username for:\n{base_url}",
                parent=root,
            )
            if username is None:
                raise CredentialPromptCancelled("Credential entry cancelled.")
            username = username.strip()
            if username:
                break
            messagebox.showerror("Missing username", "Username cannot be empty.", parent=root)

        while True:
            password = simpledialog.askstring(
                title="XNAT Login",
                prompt=f"Enter password for:\n{base_url}",
                parent=root,
                show="*",
            )
            if password is None:
                raise CredentialPromptCancelled("Credential entry cancelled.")
            if password:
                break
            messagebox.showerror("Missing password", "Password cannot be empty.", parent=root)

        return username, password

    finally:
        try:
            root.destroy()
        except Exception:
            pass


def prompt_credentials(base_url: str) -> Tuple[str, str]:
    """
    Ask for credentials. Uses a GUI popup when available, with a terminal fallback.
    """
    try:
        return _prompt_credentials_gui(base_url)
    except CredentialPromptCancelled:
        raise
    except Exception as e:
        print(f"[AUTH] GUI credential prompt unavailable: {e}. Falling back to terminal input.")

        username = input(f"Enter username for {base_url}: ").strip()
        if not username:
            raise CredentialPromptCancelled("Username entry cancelled/empty.")

        password = getpass.getpass(f"Enter password for {base_url}: ").strip()
        if not password:
            raise CredentialPromptCancelled("Password entry cancelled/empty.")

        return username, password


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


def _scan_number_info(name: str) -> Dict[str, object]:
    """
    Extract digit groups and report prefix/suffix candidates.
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


def _zip_dir_sorted(src_dir: Path, zip_path: Path, progress_label: Optional[str] = None) -> int:
    files: List[Tuple[str, Path]] = []
    for p in src_dir.rglob("*"):
        if p.is_file():
            rel = p.relative_to(src_dir).as_posix()
            files.append((rel, p))
    files.sort(key=lambda t: t[0].lower())

    if tqdm is not None and progress_label:
        iterator = tqdm(files, desc=progress_label, unit="file", leave=False)
    else:
        iterator = files

    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for rel, p in iterator:
            zf.write(p, rel)

    return len(files)


def _find_target_resource_dir(scan_dir: Path, target_name: str) -> Optional[Path]:
    target_lower = target_name.strip().lower()
    for p in scan_dir.iterdir():
        if p.is_dir() and p.name.strip().lower() == target_lower:
            return p
    return None


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
        print(f"    [DRY RUN] Would create subject: {subject_label}")
        return

    params = {"xsiType": "xnat:subjectData", "req_format": "qs"}
    r = sess.put(subj_url, params=params, timeout=REQUEST_TIMEOUT)
    if r.status_code >= 400:
        raise RuntimeError(f"Create subject failed {r.status_code}: {subj_url}\n{r.text[:500]}")


def ensure_session(
    sess: requests.Session,
    project: str,
    subject_label: str,
    session_label: str,
    session_date: Optional[str],
) -> str:
    exp_id = resolve_experiment_id(sess, project, session_label)
    if exp_id:
        return exp_id

    if DRY_RUN:
        print(f"    [DRY RUN] Would create session: {session_label}")
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
        print(f"    [DRY RUN] Would create scan {scan_id} (type={scan_type or 'NA'})")
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
) -> None:
    url = _api(BASE_URL, f"/data/experiments/{experiment_id}/scans/{scan_id}/resources/{resource_label}")
    r = sess.get(url, params={"format": "json"}, timeout=REQUEST_TIMEOUT)
    if r.status_code == 200:
        return
    if r.status_code != 404 and r.status_code >= 400:
        raise RuntimeError(f"GET resource check failed {r.status_code}: {url}\n{r.text[:500]}")

    if DRY_RUN or str(experiment_id).startswith("DRYRUN_"):
        print(f"    [DRY RUN] Would create resource folder: scan={scan_id} res={resource_label}")
        return

    rr = sess.put(url, timeout=REQUEST_TIMEOUT)
    if rr.status_code >= 400:
        raise RuntimeError(f"Create resource folder failed {rr.status_code}: {url}\n{rr.text[:500]}")


def list_resource_files(
    sess: requests.Session,
    experiment_id: str,
    scan_id: str,
    resource_label: str,
    limit: Optional[int] = None,
) -> List[dict]:
    url = _api(BASE_URL, f"/data/experiments/{experiment_id}/scans/{scan_id}/resources/{resource_label}/files")
    params = {"format": "json"}
    if limit is not None:
        params["limit"] = str(int(limit))
    data = xnat_get_json(sess, url, params=params)
    return data.get("ResultSet", {}).get("Result", []) or []


def resource_has_files(sess: requests.Session, experiment_id: str, scan_id: str, resource_label: str) -> bool:
    files = list_resource_files(sess, experiment_id, scan_id, resource_label, limit=1)
    return len(files) > 0


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
        print(f"    [DRY RUN] Upload {zip_path} -> {url}?extract=true")
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
        print(f"    XNAT response: {r.text.strip()[:200]}")


# -------------------------
# local folder handling
# -------------------------
def iter_subject_dirs(root: Path) -> List[Path]:
    dirs: List[Path] = []
    for p in root.iterdir():
        if not p.is_dir():
            continue
        if SUBJECT_DIR_REGEX is not None and not SUBJECT_DIR_REGEX.search(p.name):
            continue
        dirs.append(p)
    return sorted(dirs, key=lambda p: p.name.lower())


def iter_scan_dirs(subject_dir: Path) -> List[Path]:
    dirs: List[Path] = []
    for p in subject_dir.iterdir():
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
    Same logic as your original script:
      - folders with leading scan numbers are not type-matched
      - folders without leading scan numbers can be matched by type
      - unmatched folders may use embedded scan numbers if safe
      - remaining folders get new IDs deterministically
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

    for k in list(dup_types.keys()):
        type_to_id.pop(k, None)

    scan_id_map: Dict[str, str] = {}
    taken_local: set[str] = set()
    taken_ids_by_local: Dict[str, str] = {}

    # Pass 1: type-match only folders without leading scan number
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

    # Pass 2: use embedded scan number where safe
    if USE_SCAN_NUMBER_IF_NO_TYPE_MATCH:
        for sdir in scan_dirs:
            name = sdir.name
            if name in taken_local:
                continue

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

            if cid in used_ids:
                continue

            if str(cid) in taken_ids_by_local.values():
                continue

            scan_id_map[name] = str(cid)
            taken_local.add(name)
            taken_ids_by_local[name] = str(cid)

    # Pass 3: allocate new IDs
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


# -------------------------
# subject processing
# -------------------------
def process_subject(
    xnat: requests.Session,
    subject_dir: Path,
    subject_index: int,
    subject_total: int,
) -> Dict[str, object]:
    subject_id = subject_dir.name
    session_label = f"{subject_id}{SESSION_SUFFIX}"

    result = {
        "subject_id": subject_id,
        "status": "ok",
        "scans_total": 0,
        "scans_uploaded": 0,
        "scans_skipped_existing": 0,
        "scans_skipped_missing_resource": 0,
        "errors": [],
    }

    t0 = time.time()

    print("=" * 80)
    print(f"[SUBJECT {subject_index}/{subject_total}] {subject_id}")
    print(f"  Session label: {session_label}")
    print(f"  Local folder:   {subject_dir}")

    scan_dirs = iter_scan_dirs(subject_dir)
    if not scan_dirs:
        print("  No scan folders found under this subject; skipping.")
        result["status"] = "skipped_no_scans"
        return result

    result["scans_total"] = len(scan_dirs)

    if PREFLIGHT_SCAN_NUMBER_CHECK:
        print("  Preflight: scan-number check")
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
                print(f"    - {sdir.name}: NO DIGITS FOUND")
            else:
                print(
                    f"    - {sdir.name}: groups={groups} | prefix={prefix} | suffix={suffix} "
                    f"| chosen({SCAN_NUMBER_PICK_MODE})={chosen} | leading_scan_id={lead}"
                )
        if missing:
            print(f"  WARNING: {missing} scan folder(s) had no digits; they will rely on type-match or new ID allocation.")

    # Ensure subject and session
    try:
        ensure_subject(xnat, PROJECT_ID, subject_id)
        exp_id = ensure_session(xnat, PROJECT_ID, subject_id, session_label, SESSION_DATE)
    except Exception as e:
        msg = f"Failed to ensure subject/session: {e}"
        print(f"  ERROR: {msg}")
        result["status"] = "error"
        result["errors"].append(msg)
        return result

    print(f"  XNAT experiment ID: {exp_id}")

    # Existing scans
    existing_scans: List[Dict[str, str]] = []
    if not str(exp_id).startswith("DRYRUN_"):
        try:
            existing_scans = list_scans_with_type(xnat, exp_id)
        except Exception as e:
            print(f"  WARNING: could not list existing scans ({e}); proceeding with empty existing-scan list.")
            existing_scans = []

    scan_id_map = build_scan_id_map(scan_dirs, existing_scans)

    print("  Scan mapping:")
    for sdir in scan_dirs:
        print(f"    - {sdir.name} -> {scan_id_map[sdir.name]}")

    for scan_index, sdir in enumerate(scan_dirs, start=1):
        scan_folder_name = sdir.name
        scan_id = scan_id_map[scan_folder_name]

        scan_type = None
        if SET_SCAN_TYPE_FROM_FOLDER:
            scan_type = _strip_leading_scan_id(scan_folder_name)

        print()
        print(f"  [SCAN {scan_index}/{len(scan_dirs)}] {scan_folder_name} -> XNAT scan {scan_id}")

        try:
            ensure_scan(xnat, exp_id, scan_id, scan_type=scan_type)
        except Exception as e:
            msg = f"Could not ensure scan {scan_id}: {e}"
            print(f"    ERROR: {msg}")
            result["errors"].append(msg)
            result["status"] = "error"
            continue

        resource_dir = _find_target_resource_dir(sdir, TARGET_RESOURCE_DIR_NAME)
        if resource_dir is None:
            print(f"    No '{TARGET_RESOURCE_DIR_NAME}' folder found; skipping this scan.")
            result["scans_skipped_missing_resource"] += 1
            continue

        resource_label = _safe_resource_label(TARGET_RESOURCE_LABEL)

        try:
            ensure_resource_folder(xnat, exp_id, scan_id, resource_label)
        except Exception as e:
            msg = f"Could not ensure resource folder '{resource_label}': {e}"
            print(f"    ERROR: {msg}")
            result["errors"].append(msg)
            result["status"] = "error"
            continue

        if SKIP_IF_RESOURCE_NONEMPTY and not str(exp_id).startswith("DRYRUN_"):
            try:
                if resource_has_files(xnat, exp_id, scan_id, resource_label):
                    print(f"    Resource '{resource_label}' already has files; skipping upload.")
                    result["scans_skipped_existing"] += 1
                    continue
            except Exception as e:
                print(f"    WARNING: could not check resource contents ({e}); will try upload anyway.")

        try:
            with tempfile.TemporaryDirectory() as td:
                zip_path = Path(td) / f"{resource_label}_scan{scan_id}.zip"
                file_count = _zip_dir_sorted(
                    resource_dir,
                    zip_path,
                    progress_label=f"Zipping {subject_id} scan {scan_id}",
                )
                size_mb = zip_path.stat().st_size / 1e6
                print(f"    Uploading resource '{resource_label}' ({file_count} files, {size_mb:.1f} MB)")
                upload_zip_as_resource(xnat, exp_id, scan_id, resource_label, zip_path)
                result["scans_uploaded"] += 1
        except Exception as e:
            msg = f"Upload failed for scan {scan_id} resource {resource_label}: {e}"
            print(f"    ERROR: {msg}")
            result["errors"].append(msg)
            result["status"] = "error"
            continue

    elapsed = time.time() - t0
    print()
    print(f"  Subject summary for {subject_id}:")
    print(f"    scans total:               {result['scans_total']}")
    print(f"    scans uploaded:            {result['scans_uploaded']}")
    print(f"    scans skipped (existing):  {result['scans_skipped_existing']}")
    print(f"    scans skipped (no NIFTI):  {result['scans_skipped_missing_resource']}")
    print(f"    errors:                    {len(result['errors'])}")
    print(f"    elapsed:                   {elapsed:.1f} s")

    return result


# -------------------------
# main
# -------------------------
def main() -> int:
    if not INPUT_ROOT.exists():
        print(f"ERROR: INPUT_ROOT does not exist: {INPUT_ROOT}")
        return 2

    if not BASE_URL or not PROJECT_ID:
        print("ERROR: BASE_URL and PROJECT_ID must be set in USER CONFIG.")
        return 2

    try:
        username, password = prompt_credentials(BASE_URL)
    except CredentialPromptCancelled as e:
        print(f"ERROR: {e}")
        return 2
    except Exception as e:
        print(f"ERROR: failed to obtain credentials: {e}")
        return 2

    base = _norm_base_url(BASE_URL)

    print("Batch XNAT NIFTI upload")
    print("=" * 80)
    print(f"XNAT base:             {base}")
    print(f"Project:               {PROJECT_ID}")
    print(f"Input root:            {INPUT_ROOT}")
    print(f"Session suffix:        {SESSION_SUFFIX}")
    print(f"Target resource dir:   {TARGET_RESOURCE_DIR_NAME}")
    print(f"Target resource label: {TARGET_RESOURCE_LABEL}")
    print(f"Dry run:               {DRY_RUN}")
    print(f"Skip non-empty:        {SKIP_IF_RESOURCE_NONEMPTY}")
    print()

    subject_dirs = iter_subject_dirs(INPUT_ROOT)
    if not subject_dirs:
        msg = "No subject folders found under INPUT_ROOT"
        if SUBJECT_DIR_REGEX is not None:
            msg += f" matching SUBJECT_DIR_REGEX={SUBJECT_DIR_REGEX.pattern}"
        print(msg)
        return 1

    print(f"Found {len(subject_dirs)} subject folder(s).")
    print()

    xnat = requests.Session()
    xnat.auth = HTTPBasicAuth(username, password)
    xnat.verify = VERIFY_SSL

    ping = _api(BASE_URL, "/data/projects")
    r = xnat.get(ping, params={"format": "json"}, timeout=REQUEST_TIMEOUT)
    if r.status_code >= 400:
        print(f"ERROR: Cannot access {ping} ({r.status_code}). Check URL/credentials/SSL.\n{r.text[:300]}")
        return 3

    all_results: List[Dict[str, object]] = []

    total_uploaded = 0
    total_skipped_existing = 0
    total_skipped_missing = 0
    failed_subjects: List[str] = []

    overall_t0 = time.time()

    for i, subject_dir in enumerate(subject_dirs, start=1):
        try:
            res = process_subject(xnat, subject_dir, i, len(subject_dirs))
            all_results.append(res)

            total_uploaded += int(res["scans_uploaded"])
            total_skipped_existing += int(res["scans_skipped_existing"])
            total_skipped_missing += int(res["scans_skipped_missing_resource"])

            if res["status"] == "error":
                failed_subjects.append(str(res["subject_id"]))

        except KeyboardInterrupt:
            print("\nInterrupted by user.")
            return 130
        except Exception as e:
            subject_id = subject_dir.name
            print("=" * 80)
            print(f"[SUBJECT {i}/{len(subject_dirs)}] {subject_id}")
            print(f"  FATAL ERROR: {e}")
            failed_subjects.append(subject_id)
            all_results.append(
                {
                    "subject_id": subject_id,
                    "status": "error",
                    "scans_total": 0,
                    "scans_uploaded": 0,
                    "scans_skipped_existing": 0,
                    "scans_skipped_missing_resource": 0,
                    "errors": [str(e)],
                }
            )

    overall_elapsed = time.time() - overall_t0

    subjects_ok = sum(1 for r in all_results if r["status"] in {"ok", "skipped_no_scans"})
    subjects_error = sum(1 for r in all_results if r["status"] == "error")

    print()
    print("=" * 80)
    print("FINAL SUMMARY")
    print("=" * 80)
    print(f"Subjects found:                 {len(subject_dirs)}")
    print(f"Subjects completed:            {subjects_ok}")
    print(f"Subjects with errors:          {subjects_error}")
    print(f"Scans uploaded:                {total_uploaded}")
    print(f"Scans skipped (existing):      {total_skipped_existing}")
    print(f"Scans skipped (missing NIFTI): {total_skipped_missing}")
    print(f"Elapsed:                       {overall_elapsed:.1f} s")

    if failed_subjects:
        print()
        print("Subjects with errors:")
        for sid in failed_subjects:
            print(f"  - {sid}")

    print()
    print("Done.")
    return 0 if subjects_error == 0 else 5


if __name__ == "__main__":
    raise SystemExit(main())