#!/usr/bin/env python3
"""
upload_single_file_to_xnat.py

Upload ONE local file to a SPECIFIC XNAT scan resource and optional
target path within that resource.

Examples of destination paths inside the resource:
  data_resliced_bbr.nii.gz
  qc/overlay.png
  stats/run1/tmap.nii.gz

Notes
-----
- This uploads the file directly (no zip, no extract).
- If DEST_RESOURCE_PATH is None, the local filename is used.
- If OVERWRITE_EXISTING_FILE is False and the target file already exists,
  the script skips the upload safely.

Requires:
  pip install requests
"""

from __future__ import annotations

import mimetypes
import os
import re
from pathlib import Path
from typing import Dict, Optional
from urllib.parse import quote

import requests
from requests.auth import HTTPBasicAuth


# =========================
# USER CONFIG
# =========================

USERNAME = os.environ.get("XNAT_USER", "")
PASSWORD = os.environ.get("XNAT_PASS", "")

BASE_URL = ""
PROJECT_ID = ""

# Explicit destination identifiers
SUBJECT_ID = ""
SESSION_LABEL = ""
SESSION_DATE: Optional[str] = None   # e.g. "2026-02-26" or None

# Target scan/resource
SCAN_ID = ""
SCAN_TYPE: Optional[str] = None      # used only if a new scan must be created

RESOURCE_LABEL = ""
RESOURCE_FORMAT: Optional[str] = None
RESOURCE_CONTENT: Optional[str] = None

# Local file to upload
LOCAL_FILE = Path(r"")

# Target path INSIDE the resource.
# Examples:
#   None                          -> use local filename
#   "data_resliced_bbr.nii.gz"    -> upload at resource root
#   "qc/overlay.png"              -> upload into qc subfolder within resource
DEST_RESOURCE_PATH: Optional[str] = ""

# Creation behaviour
CREATE_SUBJECT_IF_MISSING = False
CREATE_SESSION_IF_MISSING = False
CREATE_SCAN_IF_MISSING = False
CREATE_RESOURCE_IF_MISSING = False

# Upload behaviour
OVERWRITE_EXISTING_FILE = True
DRY_RUN = False
VERIFY_SSL = True
REQUEST_TIMEOUT = 300  # seconds

# =========================
# END USER CONFIG
# =========================


# -------------------------
# helpers
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


def _normalise_resource_relpath(relpath: Optional[str], local_file: Path) -> str:
    if relpath is None or str(relpath).strip() == "":
        rel = local_file.name
    else:
        rel = str(relpath).replace("\\", "/").strip()

    rel = rel.lstrip("/")
    while "//" in rel:
        rel = rel.replace("//", "/")

    if rel in {"", ".", ".."}:
        raise ValueError("DEST_RESOURCE_PATH resolves to an invalid empty path.")

    return rel


def _guess_mime_type(path: Path) -> str:
    mime, _ = mimetypes.guess_type(str(path))
    return mime or "application/octet-stream"


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
    url = _api(BASE_URL, f"/data/projects/{quote(project, safe='')}/experiments")
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


def ensure_subject(sess: requests.Session, project: str, subject_label: str, create_if_missing: bool) -> None:
    subj_url = _api(
        BASE_URL,
        f"/data/projects/{quote(project, safe='')}/subjects/{quote(subject_label, safe='')}",
    )

    if xnat_exists(sess, subj_url, params={"format": "json"}):
        return

    if not create_if_missing:
        raise RuntimeError(f"Subject does not exist and CREATE_SUBJECT_IF_MISSING=False: {subject_label}")

    if DRY_RUN:
        print(f"[DRY RUN] Would create subject: project={project} subject={subject_label}")
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
    create_if_missing: bool,
) -> str:
    exp_id = resolve_experiment_id(sess, project, session_label)
    if exp_id:
        return exp_id

    if not create_if_missing:
        raise RuntimeError(f"Session does not exist and CREATE_SESSION_IF_MISSING=False: {session_label}")

    if DRY_RUN:
        print(f"[DRY RUN] Would create session: project={project} subject={subject_label} session={session_label}")
        return f"DRYRUN_{session_label}"

    put_url = _api(
        BASE_URL,
        f"/data/projects/{quote(project, safe='')}/subjects/{quote(subject_label, safe='')}/experiments/{quote(session_label, safe='')}",
    )
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


def ensure_scan(
    sess: requests.Session,
    experiment_id: str,
    scan_id: str,
    scan_type: Optional[str],
    create_if_missing: bool,
) -> None:
    url = _api(
        BASE_URL,
        f"/data/experiments/{quote(str(experiment_id), safe='')}/scans/{quote(str(scan_id), safe='')}",
    )

    r = sess.get(url, params={"format": "json"}, timeout=REQUEST_TIMEOUT)
    if r.status_code == 200:
        return
    if r.status_code != 404 and r.status_code >= 400:
        raise RuntimeError(f"Scan existence check failed {r.status_code}: {url}\n{r.text[:500]}")

    if not create_if_missing:
        raise RuntimeError(f"Scan does not exist and CREATE_SCAN_IF_MISSING=False: {scan_id}")

    if DRY_RUN or str(experiment_id).startswith("DRYRUN_"):
        print(f"[DRY RUN] Would create scan {scan_id} in experiment {experiment_id} (type={scan_type or 'NA'})")
        return

    params: Dict[str, str] = {"xsiType": "xnat:mrScanData", "req_format": "qs"}
    if scan_type:
        params["xnat:mrScanData/type"] = scan_type

    rr = sess.put(url, params=params, timeout=REQUEST_TIMEOUT)
    if rr.status_code >= 400:
        raise RuntimeError(f"Create scan failed {rr.status_code}: {url}\n{rr.text[:500]}")


def ensure_resource_folder(
    sess: requests.Session,
    experiment_id: str,
    scan_id: str,
    resource_label: str,
    fmt: Optional[str],
    content: Optional[str],
    create_if_missing: bool,
) -> None:
    resource_label = _safe_resource_label(resource_label)

    url = _api(
        BASE_URL,
        f"/data/experiments/{quote(str(experiment_id), safe='')}/scans/{quote(str(scan_id), safe='')}/resources/{quote(resource_label, safe='')}",
    )

    r = sess.get(url, params={"format": "json"}, timeout=REQUEST_TIMEOUT)
    if r.status_code == 200:
        return
    if r.status_code != 404 and r.status_code >= 400:
        raise RuntimeError(f"GET resource check failed {r.status_code}: {url}\n{r.text[:500]}")

    if not create_if_missing:
        raise RuntimeError(f"Resource does not exist and CREATE_RESOURCE_IF_MISSING=False: {resource_label}")

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
        raise RuntimeError(f"Create resource folder failed {rr.status_code}: {url}\n{rr.text[:500]}")


def xnat_file_exists(
    sess: requests.Session,
    experiment_id: str,
    scan_id: str,
    resource_label: str,
    resource_relpath: str,
) -> bool:
    resource_label = _safe_resource_label(resource_label)
    quoted_relpath = quote(resource_relpath, safe="/")

    url = _api(
        BASE_URL,
        f"/data/experiments/{quote(str(experiment_id), safe='')}/scans/{quote(str(scan_id), safe='')}/resources/{quote(resource_label, safe='')}/files/{quoted_relpath}",
    )

    r = sess.get(url, stream=True, timeout=REQUEST_TIMEOUT)
    try:
        if r.status_code == 404:
            return False
        if r.status_code >= 400:
            raise RuntimeError(f"Target file existence check failed {r.status_code}: {url}\n{r.text[:500]}")
        return True
    finally:
        r.close()


def upload_single_file(
    sess: requests.Session,
    experiment_id: str,
    scan_id: str,
    resource_label: str,
    local_file: Path,
    resource_relpath: str,
    overwrite: bool,
) -> None:
    resource_label = _safe_resource_label(resource_label)
    quoted_relpath = quote(resource_relpath, safe="/")

    url = _api(
        BASE_URL,
        f"/data/experiments/{quote(str(experiment_id), safe='')}/scans/{quote(str(scan_id), safe='')}/resources/{quote(resource_label, safe='')}/files/{quoted_relpath}",
    )

    if DRY_RUN or str(experiment_id).startswith("DRYRUN_"):
        print(f"[DRY RUN] Upload {local_file} -> {url}")
        return

    mime_type = _guess_mime_type(local_file)
    params = {"overwrite": "true"} if overwrite else None

    with local_file.open("rb") as f:
        files = {"file": (local_file.name, f, mime_type)}
        r = sess.put(url, params=params, files=files, timeout=REQUEST_TIMEOUT)

    if r.status_code >= 400:
        raise RuntimeError(f"Upload failed {r.status_code}: {url}\n{r.text[:500]}")

    if r.text and r.text.strip():
        print(f"XNAT response: {r.text.strip()[:300]}")


def main() -> int:
    if not LOCAL_FILE.exists():
        print(f"ERROR: LOCAL_FILE does not exist: {LOCAL_FILE}")
        return 2

    if not LOCAL_FILE.is_file():
        print(f"ERROR: LOCAL_FILE is not a file: {LOCAL_FILE}")
        return 2

    if not USERNAME or not PASSWORD:
        print("ERROR: XNAT credentials are missing. Set XNAT_USER and XNAT_PASS.")
        return 2

    if not PROJECT_ID or not SUBJECT_ID or not SESSION_LABEL or not SCAN_ID or not RESOURCE_LABEL:
        print("ERROR: PROJECT_ID, SUBJECT_ID, SESSION_LABEL, SCAN_ID, and RESOURCE_LABEL must all be set.")
        return 2

    try:
        dest_relpath = _normalise_resource_relpath(DEST_RESOURCE_PATH, LOCAL_FILE)
    except Exception as e:
        print(f"ERROR: invalid DEST_RESOURCE_PATH: {e}")
        return 2

    base = _norm_base_url(BASE_URL)

    print(f"XNAT base:        {base}")
    print(f"Project:          {PROJECT_ID}")
    print(f"Subject ID:       {SUBJECT_ID}")
    print(f"Session:          {SESSION_LABEL}")
    print(f"Session date:     {SESSION_DATE or 'None'}")
    print(f"Scan ID:          {SCAN_ID}")
    print(f"Scan type:        {SCAN_TYPE or 'None'}")
    print(f"Resource label:   {_safe_resource_label(RESOURCE_LABEL)}")
    print(f"Local file:       {LOCAL_FILE}")
    print(f"Destination path: {dest_relpath}")
    print(f"Overwrite:        {OVERWRITE_EXISTING_FILE}")
    print(f"Dry run:          {DRY_RUN}")
    print()

    xnat = requests.Session()
    xnat.auth = HTTPBasicAuth(USERNAME, PASSWORD)
    xnat.verify = VERIFY_SSL

    # Quick auth sanity check
    ping = _api(BASE_URL, "/data/projects")
    r = xnat.get(ping, params={"format": "json"}, timeout=REQUEST_TIMEOUT)
    if r.status_code >= 400:
        print(f"ERROR: Cannot access {ping} ({r.status_code}). Check URL/credentials/SSL.\n{r.text[:300]}")
        return 3

    try:
        ensure_subject(xnat, PROJECT_ID, SUBJECT_ID, CREATE_SUBJECT_IF_MISSING)
        exp_id = ensure_session(
            xnat,
            PROJECT_ID,
            SUBJECT_ID,
            SESSION_LABEL,
            SESSION_DATE,
            CREATE_SESSION_IF_MISSING,
        )
        ensure_scan(
            xnat,
            exp_id,
            SCAN_ID,
            scan_type=SCAN_TYPE,
            create_if_missing=CREATE_SCAN_IF_MISSING,
        )
        ensure_resource_folder(
            xnat,
            exp_id,
            SCAN_ID,
            RESOURCE_LABEL,
            fmt=RESOURCE_FORMAT,
            content=RESOURCE_CONTENT,
            create_if_missing=CREATE_RESOURCE_IF_MISSING,
        )
    except Exception as e:
        print(f"ERROR: failed to prepare destination: {e}")
        return 4

    print(f"XNAT experiment ID: {exp_id}")

    try:
        exists = False
        if not str(exp_id).startswith("DRYRUN_"):
            exists = xnat_file_exists(
                xnat,
                exp_id,
                SCAN_ID,
                RESOURCE_LABEL,
                dest_relpath,
            )

        if exists and not OVERWRITE_EXISTING_FILE:
            print("Target file already exists and OVERWRITE_EXISTING_FILE=False; skipping upload.")
            print("Done.")
            return 0

        upload_single_file(
            xnat,
            exp_id,
            SCAN_ID,
            RESOURCE_LABEL,
            LOCAL_FILE,
            dest_relpath,
            overwrite=OVERWRITE_EXISTING_FILE,
        )
    except Exception as e:
        print(f"ERROR: upload failed: {e}")
        return 5

    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())