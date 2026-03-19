#!/usr/bin/env python3
"""
xnat_sync_bypass_transfer.py

"XNAT A -> local staging -> XNAT B" transfer.

Preserves scan-level metadata where present, including:
- type
- series_description
- quality
- note

New features
------------
1) Credentials are prompted at runtime via pop-up windows (GUI), so they are
   no longer hard-coded into the script.
2) Direction control:
      DIRECTION = "forwards"   -> A acts as source, B acts as destination
      DIRECTION = "backwards"  -> B acts as source, A acts as destination
"""

from __future__ import annotations

import getpass
import logging
import re
import shutil
import time
import zipfile
from pathlib import Path, PurePosixPath
from typing import Dict, List, Optional, Tuple

import requests


# =========================
# USER CONFIG
# =========================

# -------------------------
# Fixed endpoint definitions
# -------------------------
A_BASE_URL = ""
A_PROJECT = ""

B_BASE_URL = ""
B_PROJECT = ""

# Direction of transfer:
#   "forwards"  = A -> B
#   "backwards" = B -> A
DIRECTION = "forwards"

# Subjects to transfer (labels)
SUBJECT_LABELS = [""]

# Optional: only process sessions whose label matches this regex (None = all)
SESSION_LABEL_REGEX = None  # e.g. r"^\d{3}_MR_\d+$", r"^[A-Za-z0-9]+_MR_\d+$"

STAGING_DIR = Path(r"D:\Downloads\XNAT_transfer_staging")

PHASE = "dicom"   # "dicom" or "resources"
DRY_RUN = False       # True = no PUT/POST, only GET + plan

# In resources phase, skip these resource labels
SKIP_RESOURCE_LABELS = {"DICOM"}

# If True, skip uploading when destination resource already has files (>0)
SKIP_EXISTING = True

# Metadata rebuild behaviour:
#   "none"    = don't call pullDataFromHeaders at all
#   "scan"    = call pullDataFromHeaders per scan (with retries + waits)
#   "session" = call pullDataFromHeaders once per session (recommended)
PULL_HEADERS_MODE = "session"

# Preserve source scan metadata such as type / series_description / quality / note
PRESERVE_SCAN_METADATA = True

# After pullDataFromHeaders, re-apply preserved source scan metadata.
# Recommended True when source has meaningful custom scan type labels.
REAPPLY_SCAN_METADATA_AFTER_PULL = True

# After upload+extract, refresh catalog so extracted files are added to catalog entries.
REFRESH_CATALOG_AFTER_UPLOAD = True
REFRESH_CATALOG_OPTIONS = "append,populateStats"  # safe combo

# Wait/poll after upload until dest resource file_count reaches expected count (helps avoid races)
WAIT_FOR_DICOM_FILES = True
WAIT_TIMEOUT_SEC = 180
WAIT_POLL_SEC = 5

VERIFY_TLS = True
TIMEOUT = 1800  # seconds

# -------------------------
# ZIP PATH NORMALISATION
# -------------------------
# If True, repackage downloaded zips so extracted files land correctly at destination.
NORMALIZE_DOWNLOADED_ZIPS = True
# Skip normalisation for these resources (DICOM often huge and usually fine to keep as-is)
NORMALIZE_SKIP_RESOURCE_LABELS = set()
# Delete the original downloaded zip after normalisation (saves disk)
DELETE_ORIGINAL_ZIP_AFTER_NORMALIZE = True

# Ensure zip entries are WRITTEN in alphabetical order (helps extraction/catalog order)
SORT_ZIP_ENTRIES_ALPHABETICAL = True

# -------------------------
# RESILIENCY / LARGE FILES
# -------------------------

DOWNLOAD_RETRIES = 3
UPLOAD_RETRIES = 3
RETRY_BACKOFF_BASE_SEC = 6  # backoff = base * attempt

FULL_ZIP_INTEGRITY_CHECK = False
ZIP_SAMPLE_READ_FILES = 3
ZIP_SAMPLE_READ_BYTES = 4096

SPLIT_LARGE_ZIPS = True
SPLIT_THRESHOLD_BYTES = int(500 * 1024 * 1024)
SPLIT_PART_TARGET_BYTES = int(250 * 1024 * 1024)

PRESERVE_ZIP_FILENAME_ON_DEST = True
CLEANUP_STAGED_ZIPS = True

RETRYABLE_ERROR_SUBSTRINGS = (
    "unexpected EOF",
    "EOF",
    "Connection aborted",
    "ConnectionResetError",
    "connection reset",
    "Broken pipe",
    "Read timed out",
    "timed out",
    "504",
    "502",
    "503",
)

# -------------------------
# Active runtime mapping
# -------------------------
# These are populated automatically from A/B + DIRECTION in main().
SRC_BASE_URL = ""
DST_BASE_URL = ""
SRC_PROJECT = ""
DST_PROJECT = ""

SOURCE_SIDE_NAME = ""
DEST_SIDE_NAME = ""


# =========================
# END USER CONFIG
# =========================


class CredentialPromptCancelled(Exception):
    """Raised when the user cancels credential entry."""


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def resolve_direction() -> Tuple[str, str]:
    """
    Resolve A/B into active source/destination settings based on DIRECTION.
    Populates global SRC_* / DST_* variables so the rest of the script can
    continue to use them.
    """
    global SRC_BASE_URL, DST_BASE_URL, SRC_PROJECT, DST_PROJECT
    global SOURCE_SIDE_NAME, DEST_SIDE_NAME

    d = str(DIRECTION).strip().lower()

    if d == "forwards":
        SRC_BASE_URL = A_BASE_URL
        SRC_PROJECT = A_PROJECT
        DST_BASE_URL = B_BASE_URL
        DST_PROJECT = B_PROJECT
        SOURCE_SIDE_NAME = "A"
        DEST_SIDE_NAME = "B"
    elif d in {"backwards"}:
        SRC_BASE_URL = B_BASE_URL
        SRC_PROJECT = B_PROJECT
        DST_BASE_URL = A_BASE_URL
        DST_PROJECT = A_PROJECT
        SOURCE_SIDE_NAME = "B"
        DEST_SIDE_NAME = "A"
    else:
        raise ValueError(
            "DIRECTION must be 'forwards' or 'backwards' "
        )

    return SOURCE_SIDE_NAME, DEST_SIDE_NAME


def validate_runtime_config() -> None:
    missing = []

    if not _clean_text(A_BASE_URL):
        missing.append("A_BASE_URL")
    if not _clean_text(A_PROJECT):
        missing.append("A_PROJECT")
    if not _clean_text(B_BASE_URL):
        missing.append("B_BASE_URL")
    if not _clean_text(B_PROJECT):
        missing.append("B_PROJECT")

    if missing:
        raise ValueError("Missing required config values: " + ", ".join(missing))

    if PHASE not in {"dicom", "resources"}:
        raise ValueError("PHASE must be 'dicom' or 'resources'")


def _prompt_credentials_gui(endpoint_label: str, base_url: str) -> Tuple[str, str]:
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
            user = simpledialog.askstring(
                title="XNAT Login",
                prompt=f"Enter username for {endpoint_label}\n{base_url}",
                parent=root,
            )
            if user is None:
                raise CredentialPromptCancelled(f"Credential entry cancelled for {endpoint_label}.")
            user = user.strip()
            if user:
                break
            messagebox.showerror("Missing username", f"Username for {endpoint_label} cannot be empty.", parent=root)

        while True:
            password = simpledialog.askstring(
                title="XNAT Login",
                prompt=f"Enter password for {endpoint_label}\n{base_url}",
                parent=root,
                show="*",
            )
            if password is None:
                raise CredentialPromptCancelled(f"Credential entry cancelled for {endpoint_label}.")
            if password:
                break
            messagebox.showerror("Missing password", f"Password for {endpoint_label} cannot be empty.", parent=root)

        return user, password

    finally:
        try:
            root.destroy()
        except Exception:
            pass


def prompt_credentials(endpoint_label: str, base_url: str) -> Tuple[str, str]:
    """
    Ask for credentials. Uses a GUI popup when available, with a terminal fallback.
    """
    try:
        return _prompt_credentials_gui(endpoint_label, base_url)
    except CredentialPromptCancelled:
        raise
    except Exception as e:
        logging.warning(
            f"[AUTH] GUI credential prompt unavailable for {endpoint_label}: {e}. "
            f"Falling back to terminal input."
        )

        user = input(f"Enter username for {endpoint_label} ({base_url}): ").strip()
        if not user:
            raise CredentialPromptCancelled(f"Username entry cancelled/empty for {endpoint_label}.")

        password = getpass.getpass(f"Enter password for {endpoint_label} ({base_url}): ").strip()
        if not password:
            raise CredentialPromptCancelled(f"Password entry cancelled/empty for {endpoint_label}.")

        return user, password


def _sleep_backoff(attempt: int) -> None:
    time.sleep(RETRY_BACKOFF_BASE_SEC * attempt)


def _is_retryable_error(msg: str) -> bool:
    m = (msg or "").lower()
    return any(s.lower() in m for s in RETRYABLE_ERROR_SUBSTRINGS)


def _clean_text(v: object) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None


def validate_zip(zip_path: Path) -> bool:
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            infos = [i for i in zf.infolist() if not i.is_dir()]
            if not infos:
                return True

            if FULL_ZIP_INTEGRITY_CHECK:
                bad = zf.testzip()
                return bad is None

            n = min(ZIP_SAMPLE_READ_FILES, len(infos))
            for i in range(n):
                with zf.open(infos[i], "r") as f:
                    f.read(ZIP_SAMPLE_READ_BYTES)
            return True
    except Exception:
        return False


def _safe_rel_posix(p: str) -> str:
    """
    Convert to safe relative posix path (no leading '/', no '..').
    """
    pp = PurePosixPath(p.replace("\\", "/"))
    parts = [x for x in pp.parts if x not in ("", "/", ".", "..")]
    return "/".join(parts)


def _strip_prefix_to_resource_files(member_path: str, resource_label: str) -> Optional[str]:
    """
    If member_path contains:
        .../resources/<resource_label>/files/<REL>
    return <REL>. Otherwise None.
    """
    p = _safe_rel_posix(member_path)
    marker = f"resources/{resource_label}/files/"
    idx = p.find(marker)
    if idx < 0:
        return None
    return p[idx + len(marker):]


def normalize_zip_to_resource_files_root(zip_in: Path, resource_label: str) -> Tuple[Path, int]:
    """
    Create a new zip where entries are relative to the resource's files root.

    Also writes entries in alphabetical order (if SORT_ZIP_ENTRIES_ALPHABETICAL=True).
    """
    if DRY_RUN:
        return zip_in, 0

    if not validate_zip(zip_in):
        raise RuntimeError(f"Cannot normalise invalid zip: {zip_in}")

    zip_out = zip_in.with_name(zip_in.stem + "__normalized.zip")

    entries: List[Tuple[str, zipfile.ZipInfo, bool]] = []

    with zipfile.ZipFile(zip_in, "r") as zin:
        for info in zin.infolist():
            if info.is_dir():
                continue

            new_rel = _strip_prefix_to_resource_files(info.filename, resource_label)
            if new_rel is None:
                final_name = _safe_rel_posix(info.filename)
                was_rewritten = False
            else:
                final_name = _safe_rel_posix(new_rel)
                was_rewritten = True

            if final_name:
                entries.append((final_name, info, was_rewritten))

        if SORT_ZIP_ENTRIES_ALPHABETICAL:
            entries.sort(key=lambda t: t[0])

        rewritten = sum(1 for _, _, w in entries if w)

        name_counts: Dict[str, int] = {}

        with zipfile.ZipFile(zip_out, "w", compression=zipfile.ZIP_DEFLATED) as zout:
            for base_name, info, _ in entries:
                c = name_counts.get(base_name, 0) + 1
                name_counts[base_name] = c
                final_name = base_name if c == 1 else f"{base_name}__dup{c}"

                with zin.open(info, "r") as src:
                    zi = zipfile.ZipInfo(filename=final_name, date_time=info.date_time)
                    zi.external_attr = info.external_attr
                    zi.compress_type = zipfile.ZIP_DEFLATED
                    with zout.open(zi, "w") as dst:
                        shutil.copyfileobj(src, dst, length=1024 * 1024)

    if not validate_zip(zip_out):
        try:
            zip_out.unlink()
        except Exception:
            pass
        raise RuntimeError(f"Normalised zip failed validation: {zip_out}")

    if rewritten == 0:
        try:
            zip_out.unlink()
        except Exception:
            pass
        return zip_in, 0

    if DELETE_ORIGINAL_ZIP_AFTER_NORMALIZE:
        try:
            zip_in.unlink()
        except Exception:
            pass

    return zip_out, rewritten


def split_zip_into_parts(zip_path: Path, out_dir: Path, part_target_bytes: int) -> List[Path]:
    """
    Split a zip into multiple smaller zips.
    Ensures entries are processed in alphabetical order first (if enabled),
    so part001/part002 preserve overall alphabetical ordering.
    """
    parts: List[Path] = []
    out_dir.mkdir(parents=True, exist_ok=True)

    base_stem = zip_path.stem
    with zipfile.ZipFile(zip_path, "r") as zin:
        infos = [i for i in zin.infolist() if not i.is_dir()]
        if not infos:
            return [zip_path]

        if SORT_ZIP_ENTRIES_ALPHABETICAL:
            infos.sort(key=lambda i: _safe_rel_posix(i.filename))

        part_idx = 1
        current_part_path = out_dir / f"{base_stem}.part{part_idx:03d}.zip"
        current_part_uncompressed_est = 0

        zout = zipfile.ZipFile(current_part_path, "w", compression=zipfile.ZIP_DEFLATED)
        wrote_any = False

        try:
            for info in infos:
                next_est = current_part_uncompressed_est + int(info.file_size or 0)
                if wrote_any and next_est > part_target_bytes:
                    zout.close()
                    parts.append(current_part_path)

                    part_idx += 1
                    current_part_path = out_dir / f"{base_stem}.part{part_idx:03d}.zip"
                    current_part_uncompressed_est = 0
                    zout = zipfile.ZipFile(current_part_path, "w", compression=zipfile.ZIP_DEFLATED)
                    wrote_any = False

                with zin.open(info, "r") as src:
                    zi = zipfile.ZipInfo(filename=_safe_rel_posix(info.filename), date_time=info.date_time)
                    zi.external_attr = info.external_attr
                    zi.compress_type = zipfile.ZIP_DEFLATED
                    with zout.open(zi, "w") as dst:
                        shutil.copyfileobj(src, dst, length=1024 * 1024)

                wrote_any = True
                current_part_uncompressed_est += int(info.file_size or 0)

            zout.close()
            parts.append(current_part_path)
        finally:
            try:
                zout.close()
            except Exception:
                pass

    for p in parts:
        if not validate_zip(p):
            raise RuntimeError(f"Split produced an invalid zip part: {p}")

    return parts


def prepare_zip_for_upload(zip_path: Path) -> List[Path]:
    if DRY_RUN:
        return [zip_path]

    if not validate_zip(zip_path):
        raise RuntimeError(f"Zip is invalid/corrupt: {zip_path}")

    if SPLIT_LARGE_ZIPS:
        try:
            size_bytes = zip_path.stat().st_size
        except Exception:
            size_bytes = 0

        if size_bytes > SPLIT_THRESHOLD_BYTES:
            logging.info(
                f"[ZIP] large zip {zip_path.name} ({size_bytes/1024**3:.2f} GB) -> splitting into parts (~{SPLIT_PART_TARGET_BYTES/1024**2:.0f} MB)"
            )
            part_dir = zip_path.parent / (zip_path.stem + "_parts")
            return split_zip_into_parts(zip_path, part_dir, SPLIT_PART_TARGET_BYTES)

    return [zip_path]


class XNAT:
    def __init__(self, base_url: str, user: str, password: str, verify_tls: bool = True):
        self.base_url = base_url.rstrip("/")
        self.s = requests.Session()
        self.s.auth = (user, password)
        self.s.verify = verify_tls

    def _url(self, path: str) -> str:
        if not path.startswith("/"):
            path = "/" + path
        return self.base_url + path

    def get(self, path: str, params: Optional[Dict] = None) -> requests.Response:
        return self.s.get(self._url(path), params=params, timeout=TIMEOUT)

    def get_json(self, path: str, params: Optional[Dict] = None) -> Dict:
        r = self.get(path, params=params)
        if r.status_code >= 400:
            raise RuntimeError(f"GET {path} failed: {r.status_code} {r.text[:300]}")
        return r.json()

    def put(self, path: str, params: Optional[Dict] = None) -> str:
        try:
            r = self.s.put(self._url(path), params=params, timeout=TIMEOUT)
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"PUT {path} failed (request exception): {e}") from e

        if r.status_code >= 400:
            raise RuntimeError(f"PUT {path} failed: {r.status_code} {r.text[:300]}")
        return (r.text or "").strip()

    def post(self, path: str, params: Optional[Dict] = None) -> str:
        try:
            r = self.s.post(self._url(path), params=params, timeout=TIMEOUT)
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"POST {path} failed (request exception): {e}") from e

        if r.status_code >= 400:
            raise RuntimeError(f"POST {path} failed: {r.status_code} {r.text[:300]}")
        return (r.text or "").strip()

    def put_file_multipart(self, path: str, file_path: Path, params: Optional[Dict] = None) -> str:
        try:
            with file_path.open("rb") as f:
                files = {"file": (file_path.name, f, "application/zip")}
                r = self.s.put(self._url(path), params=params, files=files, timeout=TIMEOUT)
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"PUT(file) {path} failed (request exception): {e}") from e

        if r.status_code >= 400:
            raise RuntimeError(f"PUT(file) {path} failed: {r.status_code} {r.text[:300]}")
        return (r.text or "").strip()

    def download_to_file(self, path: str, out_path: Path, params: Optional[Dict] = None) -> None:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = out_path.with_suffix(out_path.suffix + ".part")

        try:
            with self.s.get(self._url(path), params=params, stream=True, timeout=TIMEOUT) as r:
                if r.status_code >= 400:
                    raise RuntimeError(f"GET(download) {path} failed: {r.status_code} {r.text[:300]}")
                with tmp_path.open("wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            f.write(chunk)
            if out_path.exists():
                out_path.unlink()
            tmp_path.rename(out_path)
        except Exception:
            try:
                if tmp_path.exists():
                    tmp_path.unlink()
            except Exception:
                pass
            raise


def rs_result_list(d: Dict) -> List[Dict]:
    return d.get("ResultSet", {}).get("Result", []) or []


# -------- Scan metadata preservation helpers --------

def build_scan_metadata_from_row(scan_row: Dict) -> Dict[str, str]:
    """
    Extract source scan metadata that should be preserved on destination.
    """
    meta: Dict[str, str] = {}

    xsi_type = _clean_text(scan_row.get("xsiType")) or "xnat:mrScanData"
    meta["xsiType"] = xsi_type

    for key in ("type", "series_description", "quality", "note"):
        val = _clean_text(scan_row.get(key))
        if val:
            meta[key] = val

    return meta


def build_scan_put_params(scan_meta: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    """
    Build query-string params for creating/updating a scan and its metadata.
    """
    params: Dict[str, str] = {
        "xsiType": (scan_meta or {}).get("xsiType", "xnat:mrScanData"),
        "req_format": "qs",
    }

    if scan_meta:
        for key in ("type", "series_description", "quality", "note"):
            val = _clean_text(scan_meta.get(key))
            if val:
                params[key] = val

    return params


def update_dest_scan_metadata(
    xdst: XNAT,
    dst_expt_id: str,
    scan_id: str,
    scan_meta: Optional[Dict[str, str]],
) -> None:
    """
    Update an existing destination scan with preserved metadata.
    """
    if not PRESERVE_SCAN_METADATA:
        return
    if not scan_meta:
        return
    if DRY_RUN or dst_expt_id.startswith("DRYRUN_"):
        logging.info(f"[DST] would update scan metadata for scan {scan_id}: {scan_meta}")
        return

    params = build_scan_put_params(scan_meta)
    logging.info(
        f"[DST] preserving scan metadata scan={scan_id}"
        f" type={scan_meta.get('type', '')!r}"
        f" series_description={scan_meta.get('series_description', '')!r}"
        f" quality={scan_meta.get('quality', '')!r}"
    )
    xdst.put(f"/data/experiments/{dst_expt_id}/scans/{scan_id}", params=params)


def reapply_scan_metadata_map(
    xdst: XNAT,
    dst_expt_id: str,
    scan_meta_by_id: Dict[str, Dict[str, str]],
) -> None:
    """
    Re-apply preserved scan metadata for all scans in a session.
    Useful after pullDataFromHeaders.
    """
    if not PRESERVE_SCAN_METADATA:
        return
    if not REAPPLY_SCAN_METADATA_AFTER_PULL:
        return

    for scan_id, scan_meta in scan_meta_by_id.items():
        try:
            update_dest_scan_metadata(xdst, dst_expt_id, scan_id, scan_meta)
        except Exception as e:
            logging.warning(f"[DST] re-apply scan metadata failed scan={scan_id}: {e}")


# -------- Source enumeration --------

def list_source_sessions_for_subject(xsrc: XNAT, subject_label: str) -> List[Dict]:
    j = xsrc.get_json(
        f"/data/projects/{SRC_PROJECT}/subjects/{subject_label}/experiments",
        params={"format": "json", "limit": "*"},
    )
    expts = rs_result_list(j)
    out = [e for e in expts if "mrSessionData" in str(e.get("xsiType", ""))]
    if SESSION_LABEL_REGEX:
        rgx = re.compile(SESSION_LABEL_REGEX)
        out = [e for e in out if rgx.match(str(e.get("label", "")))]
    return out


def get_session_date(x: XNAT, experiment_id: str) -> Optional[str]:
    j = x.get_json(f"/data/experiments/{experiment_id}", params={"format": "json"})
    try:
        return j["items"][0]["data_fields"].get("date") or None
    except Exception:
        return None


def list_scans_by_expt_id(x: XNAT, experiment_id: str) -> List[Dict]:
    """
    Ask XNAT for scan rows including key scan metadata that we may want to preserve.
    """
    j = x.get_json(
        f"/data/experiments/{experiment_id}/scans",
        params={
            "format": "json",
            "columns": "ID,xsiType,type,series_description,quality,note",
        },
    )
    return rs_result_list(j)


def list_scan_resources(x: XNAT, experiment_id: str, scan_id: str) -> List[Dict]:
    j = x.get_json(f"/data/experiments/{experiment_id}/scans/{scan_id}/resources", params={"format": "json"})
    return rs_result_list(j)


def resource_exists(x: XNAT, experiment_id: str, scan_id: str, resource_label: str) -> bool:
    for r in list_scan_resources(x, experiment_id, scan_id):
        if str(r.get("label", "")) == resource_label:
            return True
    return False


def get_resource_file_count(x: XNAT, experiment_id: str, scan_id: str, resource_label: str) -> int:
    resources = list_scan_resources(x, experiment_id, scan_id)
    for r in resources:
        if r.get("label") == resource_label:
            try:
                return int(r.get("file_count") or 0)
            except Exception:
                return 0
    return 0


def find_experiment_in_project_by_label(x: XNAT, project: str, label: str) -> Optional[str]:
    j = x.get_json(
        f"/data/projects/{project}/experiments",
        params={
            "format": "json",
            "limit": "*",
            "xsiType": "xnat:mrSessionData",
            "label": label,
            "columns": "ID,label,project,xsiType",
        },
    )
    rows = rs_result_list(j)
    exact = [r for r in rows if str(r.get("label", "")) == label]
    if not exact:
        return None
    for r in exact:
        if str(r.get("project", "")) == project:
            return str(r.get("ID"))
    return str(exact[0].get("ID"))


def ensure_dest_session(xdst: XNAT, subject_label: str, session_label: str, session_date: Optional[str]) -> str:
    existing = find_experiment_in_project_by_label(xdst, DST_PROJECT, session_label)
    if existing:
        return existing

    if DRY_RUN:
        logging.info(f"[DST] would create session {session_label} (subject {subject_label})")
        return f"DRYRUN_{session_label}"

    logging.info(f"[DST] creating session {session_label} (subject {subject_label})")
    params = {"xsiType": "xnat:mrSessionData", "req_format": "qs"}
    if session_date:
        params["xnat:mrSessionData/date"] = session_date

    put_path = f"/data/projects/{DST_PROJECT}/subjects/{subject_label}/experiments/{session_label}"
    new_id = xdst.put(put_path, params=params)
    return new_id or (find_experiment_in_project_by_label(xdst, DST_PROJECT, session_label) or "")


def ensure_dest_scan(
    xdst: XNAT,
    subject_label: str,
    session_label: str,
    dst_expt_id: str,
    scan_id: str,
    scan_meta: Optional[Dict[str, str]] = None,
) -> None:
    dst_scans = []
    if not dst_expt_id.startswith("DRYRUN_"):
        dst_scans = list_scans_by_expt_id(xdst, dst_expt_id)

    already_exists = any(str(s.get("ID")) == str(scan_id) for s in dst_scans)

    if already_exists:
        if PRESERVE_SCAN_METADATA and scan_meta:
            update_dest_scan_metadata(xdst, dst_expt_id, scan_id, scan_meta)
        return

    if DRY_RUN or dst_expt_id.startswith("DRYRUN_"):
        logging.info(f"[DST] would create scan {scan_id} in {session_label}")
        if PRESERVE_SCAN_METADATA and scan_meta:
            logging.info(f"[DST] would set scan metadata for scan {scan_id}: {scan_meta}")
        return

    logging.info(f"[DST] creating scan {scan_id} in {session_label}")
    put_path = f"/data/projects/{DST_PROJECT}/subjects/{subject_label}/experiments/{session_label}/scans/{scan_id}"
    params = build_scan_put_params(scan_meta)
    xdst.put(put_path, params=params)


def ensure_dest_resource_folder(
    xdst: XNAT,
    dst_expt_id: str,
    scan_id: str,
    resource_label: str,
    fmt: Optional[str] = None,
    content: Optional[str] = None,
) -> None:
    """
    Create the resource folder if missing.
    Avoids 409 by checking existence first.
    """
    if DRY_RUN or dst_expt_id.startswith("DRYRUN_"):
        return

    if resource_exists(xdst, dst_expt_id, scan_id, resource_label):
        return

    params: Dict[str, str] = {}
    if fmt:
        params["format"] = str(fmt)
    if content:
        params["content"] = str(content)

    xdst.put(
        f"/data/experiments/{dst_expt_id}/scans/{scan_id}/resources/{resource_label}",
        params=params if params else None,
    )


def refresh_catalog_append(xdst: XNAT, archive_resource_path: str, options: str) -> None:
    if DRY_RUN:
        return
    xdst.post("/data/services/refresh/catalog", params={"resource": archive_resource_path, "options": options})


def wait_for_resource_files(
    xdst: XNAT,
    dst_expt_id: str,
    scan_id: str,
    resource_label: str,
    expected_min: int,
    timeout_sec: int,
    poll_sec: int,
) -> None:
    if DRY_RUN or dst_expt_id.startswith("DRYRUN_"):
        return
    t0 = time.time()
    while True:
        cnt = get_resource_file_count(xdst, dst_expt_id, scan_id, resource_label)
        if cnt >= expected_min:
            return
        if time.time() - t0 > timeout_sec:
            logging.warning(
                f"[WAIT] timeout waiting for {resource_label} scan={scan_id}: have {cnt}, expected >= {expected_min}"
            )
            return
        time.sleep(poll_sec)


def download_resource_zip_once(xsrc: XNAT, src_expt_id: str, scan_id: str, resource_label: str, out_zip: Path) -> None:
    path = f"/data/experiments/{src_expt_id}/scans/{scan_id}/resources/{resource_label}/files"
    logging.info(f"[SRC] download zip res={resource_label} scan={scan_id} -> {out_zip.name}")
    if DRY_RUN:
        return
    xsrc.download_to_file(path, out_zip, params={"format": "zip"})


def download_resource_zip_with_retry(
    xsrc: XNAT,
    src_expt_id: str,
    scan_id: str,
    resource_label: str,
    out_zip: Path,
) -> None:
    if DRY_RUN:
        download_resource_zip_once(xsrc, src_expt_id, scan_id, resource_label, out_zip)
        return

    for attempt in range(1, DOWNLOAD_RETRIES + 1):
        try:
            if out_zip.exists():
                out_zip.unlink()
        except Exception:
            pass

        try:
            download_resource_zip_once(xsrc, src_expt_id, scan_id, resource_label, out_zip)
            if validate_zip(out_zip):
                return
            raise RuntimeError(f"Downloaded zip failed validation: {out_zip.name}")
        except Exception as e:
            logging.warning(f"[SRC] download failed (attempt {attempt}/{DOWNLOAD_RETRIES}): {e}")
            if attempt >= DOWNLOAD_RETRIES:
                raise
            _sleep_backoff(attempt)


def maybe_normalize_zip(zip_path: Path, resource_label: str) -> Path:
    """
    Normalise zip paths so extraction lands at the destination resource root correctly.
    """
    if DRY_RUN:
        return zip_path
    if not NORMALIZE_DOWNLOADED_ZIPS:
        return zip_path
    if resource_label in NORMALIZE_SKIP_RESOURCE_LABELS:
        return zip_path

    new_zip, rewritten = normalize_zip_to_resource_files_root(zip_path, resource_label)
    if rewritten > 0:
        logging.info(f"[ZIP] normalised {zip_path.name} -> {new_zip.name} (rewrote {rewritten} entries)")
        return new_zip
    return zip_path


def upload_resource_zip_extract_once(
    xdst: XNAT,
    dst_expt_id: str,
    scan_id: str,
    resource_label: str,
    zip_path: Path,
) -> None:
    dest_name = zip_path.name if PRESERVE_ZIP_FILENAME_ON_DEST else f"{resource_label}.zip"
    path = f"/data/experiments/{dst_expt_id}/scans/{scan_id}/resources/{resource_label}/files/{dest_name}"
    logging.info(f"[DST] upload+extract res={resource_label} scan={scan_id} <- {zip_path.name}")
    if DRY_RUN or dst_expt_id.startswith("DRYRUN_"):
        return
    xdst.put_file_multipart(path, zip_path, params={"extract": "true"})


def upload_resource_zip_extract_with_retry(
    xdst: XNAT,
    dst_expt_id: str,
    scan_id: str,
    resource_label: str,
    zip_path: Path,
) -> None:
    if DRY_RUN or dst_expt_id.startswith("DRYRUN_"):
        upload_resource_zip_extract_once(xdst, dst_expt_id, scan_id, resource_label, zip_path)
        return

    if not validate_zip(zip_path):
        raise RuntimeError(f"Refusing to upload invalid zip: {zip_path}")

    last_err: Optional[Exception] = None
    for attempt in range(1, UPLOAD_RETRIES + 1):
        try:
            upload_resource_zip_extract_once(xdst, dst_expt_id, scan_id, resource_label, zip_path)
            return
        except Exception as e:
            last_err = e
            msg = str(e)
            if _is_retryable_error(msg) and attempt < UPLOAD_RETRIES:
                logging.warning(f"[DST] upload failed (attempt {attempt}/{UPLOAD_RETRIES}) retryable: {msg}")
                _sleep_backoff(attempt)
                continue
            raise

    if last_err:
        raise last_err


def upload_resource_zip_extract_resilient(
    xdst: XNAT,
    dst_expt_id: str,
    scan_id: str,
    resource_label: str,
    zip_path: Path,
) -> None:
    parts = prepare_zip_for_upload(zip_path)

    if len(parts) == 1:
        upload_resource_zip_extract_with_retry(xdst, dst_expt_id, scan_id, resource_label, parts[0])
        if CLEANUP_STAGED_ZIPS and not DRY_RUN:
            try:
                parts[0].unlink()
            except Exception:
                pass
        return

    for p in parts:
        upload_resource_zip_extract_with_retry(xdst, dst_expt_id, scan_id, resource_label, p)
        if CLEANUP_STAGED_ZIPS and not DRY_RUN:
            try:
                p.unlink()
            except Exception:
                pass


def pull_headers_session(xdst: XNAT, dst_expt_id: str) -> None:
    if DRY_RUN or dst_expt_id.startswith("DRYRUN_"):
        return
    xdst.put(f"/data/experiments/{dst_expt_id}", params={"pullDataFromHeaders": "true"})


def pull_headers_scan(xdst: XNAT, dst_expt_id: str, scan_id: str) -> None:
    if DRY_RUN or dst_expt_id.startswith("DRYRUN_"):
        return
    xdst.put(f"/data/experiments/{dst_expt_id}/scans/{scan_id}", params={"pullDataFromHeaders": "true"})


def run_dicom_phase(xsrc: XNAT, xdst: XNAT) -> None:
    STAGING_DIR.mkdir(parents=True, exist_ok=True)

    for subj in SUBJECT_LABELS:
        logging.info(f"=== SUBJECT {subj} ===")
        sessions = list_source_sessions_for_subject(xsrc, subj)
        if not sessions:
            logging.warning(f"[SRC] no MR sessions found for subject {subj}")
            continue

        for sess in sessions:
            src_expt_id = sess["ID"]
            sess_label = sess["label"]
            sess_date = get_session_date(xsrc, src_expt_id)

            logging.info(f"--- SESSION {sess_label} (SRC ID={src_expt_id}, date={sess_date or 'NA'}) ---")

            dst_expt_id = ensure_dest_session(xdst, subj, sess_label, sess_date)
            if not dst_expt_id:
                logging.error(f"[DST] could not resolve destination experiment id for {sess_label}")
                continue
            if dst_expt_id.startswith("DRYRUN_"):
                logging.info(f"[DST] dry-run placeholder experiment id: {dst_expt_id}")
            else:
                logging.info(f"[DST] experiment id: {dst_expt_id}")

            src_scans = list_scans_by_expt_id(xsrc, src_expt_id)
            src_scan_meta_by_id: Dict[str, Dict[str, str]] = {}
            for s in src_scans:
                sid = _clean_text(s.get("ID"))
                if sid:
                    src_scan_meta_by_id[sid] = build_scan_metadata_from_row(s)

            for s in src_scans:
                scan_id = str(s.get("ID"))
                if not scan_id:
                    continue

                scan_meta = src_scan_meta_by_id.get(scan_id)

                try:
                    ensure_dest_scan(xdst, subj, sess_label, dst_expt_id, scan_id, scan_meta=scan_meta)

                    expected = get_resource_file_count(xsrc, src_expt_id, scan_id, "DICOM")
                    ensure_dest_resource_folder(xdst, dst_expt_id, scan_id, "DICOM", fmt="DICOM", content="RAW")

                    if SKIP_EXISTING and not dst_expt_id.startswith("DRYRUN_"):
                        if get_resource_file_count(xdst, dst_expt_id, scan_id, "DICOM") > 0:
                            logging.info(f"[DST] skip existing DICOM scan={scan_id}")
                            if PULL_HEADERS_MODE == "scan":
                                try:
                                    logging.info(f"[DST] pullDataFromHeaders scan={scan_id}")
                                    pull_headers_scan(xdst, dst_expt_id, scan_id)
                                    if REAPPLY_SCAN_METADATA_AFTER_PULL:
                                        update_dest_scan_metadata(xdst, dst_expt_id, scan_id, scan_meta)
                                except Exception as e:
                                    logging.warning(f"[DST] pullDataFromHeaders scan={scan_id} failed: {e}")
                            continue

                    zip_path = STAGING_DIR / f"SRC_{SRC_PROJECT}_{subj}_{sess_label}_scan{scan_id}_DICOM.zip"

                    download_resource_zip_with_retry(xsrc, src_expt_id, scan_id, "DICOM", zip_path)
                    zip_path_use = maybe_normalize_zip(zip_path, "DICOM")
                    upload_resource_zip_extract_resilient(xdst, dst_expt_id, scan_id, "DICOM", zip_path_use)

                    if REFRESH_CATALOG_AFTER_UPLOAD and not dst_expt_id.startswith("DRYRUN_"):
                        archive_path = f"/archive/experiments/{dst_expt_id}/scans/{scan_id}/resources/DICOM"
                        refresh_catalog_append(xdst, archive_path, REFRESH_CATALOG_OPTIONS)

                    if WAIT_FOR_DICOM_FILES and expected > 0 and not dst_expt_id.startswith("DRYRUN_"):
                        wait_for_resource_files(
                            xdst,
                            dst_expt_id,
                            scan_id,
                            "DICOM",
                            expected_min=expected,
                            timeout_sec=WAIT_TIMEOUT_SEC,
                            poll_sec=WAIT_POLL_SEC,
                        )

                    if PULL_HEADERS_MODE == "scan":
                        for attempt in range(1, 4):
                            try:
                                logging.info(f"[DST] pullDataFromHeaders scan={scan_id} (attempt {attempt})")
                                pull_headers_scan(xdst, dst_expt_id, scan_id)
                                if REAPPLY_SCAN_METADATA_AFTER_PULL:
                                    update_dest_scan_metadata(xdst, dst_expt_id, scan_id, scan_meta)
                                break
                            except Exception as e:
                                logging.warning(f"[DST] pullDataFromHeaders scan={scan_id} failed: {e}")
                                time.sleep(5 * attempt)

                except Exception as e:
                    logging.error(f"[SCAN] failed subj={subj} sess={sess_label} scan={scan_id}: {e}")
                    continue

            if PULL_HEADERS_MODE == "session" and not dst_expt_id.startswith("DRYRUN_"):
                try:
                    logging.info(f"[DST] pullDataFromHeaders session={dst_expt_id}")
                    pull_headers_session(xdst, dst_expt_id)
                    if REAPPLY_SCAN_METADATA_AFTER_PULL:
                        reapply_scan_metadata_map(xdst, dst_expt_id, src_scan_meta_by_id)
                except Exception as e:
                    logging.warning(f"[DST] pullDataFromHeaders session failed: {e}")


def run_resources_phase(xsrc: XNAT, xdst: XNAT) -> None:
    STAGING_DIR.mkdir(parents=True, exist_ok=True)

    for subj in SUBJECT_LABELS:
        logging.info(f"=== SUBJECT {subj} ===")
        sessions = list_source_sessions_for_subject(xsrc, subj)
        if not sessions:
            logging.warning(f"[SRC] no MR sessions found for subject {subj}")
            continue

        for sess in sessions:
            src_expt_id = sess["ID"]
            sess_label = sess["label"]

            dst_expt_id = ensure_dest_session(xdst, subj, sess_label, session_date=None)
            if not dst_expt_id:
                logging.error(f"[DST] could not resolve destination experiment id for {sess_label}")
                continue

            src_scans = list_scans_by_expt_id(xsrc, src_expt_id)
            src_scan_meta_by_id: Dict[str, Dict[str, str]] = {}
            for s in src_scans:
                sid = _clean_text(s.get("ID"))
                if sid:
                    src_scan_meta_by_id[sid] = build_scan_metadata_from_row(s)

            for s in src_scans:
                scan_id = str(s.get("ID"))
                if not scan_id:
                    continue

                scan_meta = src_scan_meta_by_id.get(scan_id)

                try:
                    ensure_dest_scan(xdst, subj, sess_label, dst_expt_id, scan_id, scan_meta=scan_meta)

                    src_resources = list_scan_resources(xsrc, src_expt_id, scan_id)
                    for r in src_resources:
                        label = r.get("label")
                        if not label or label in SKIP_RESOURCE_LABELS:
                            continue

                        try:
                            src_fc = int(r.get("file_count") or 0)
                        except Exception:
                            src_fc = 0
                        if src_fc <= 0:
                            continue

                        if SKIP_EXISTING and not dst_expt_id.startswith("DRYRUN_"):
                            if get_resource_file_count(xdst, dst_expt_id, scan_id, label) > 0:
                                logging.info(f"[DST] skip existing resource scan={scan_id} res={label}")
                                continue

                        fmt = r.get("format") or None
                        content = r.get("content") or None
                        ensure_dest_resource_folder(xdst, dst_expt_id, scan_id, label, fmt=fmt, content=content)

                        zip_path = STAGING_DIR / f"SRC_{SRC_PROJECT}_{subj}_{sess_label}_scan{scan_id}_{label}.zip"

                        download_resource_zip_with_retry(xsrc, src_expt_id, scan_id, label, zip_path)

                        zip_path_use = maybe_normalize_zip(zip_path, label)

                        upload_resource_zip_extract_resilient(xdst, dst_expt_id, scan_id, label, zip_path_use)

                        if REFRESH_CATALOG_AFTER_UPLOAD and not dst_expt_id.startswith("DRYRUN_"):
                            archive_path = f"/archive/experiments/{dst_expt_id}/scans/{scan_id}/resources/{label}"
                            refresh_catalog_append(xdst, archive_path, REFRESH_CATALOG_OPTIONS)

                except Exception as e:
                    logging.error(f"[SCAN] resource phase failed subj={subj} sess={sess_label} scan={scan_id}: {e}")
                    continue


def main() -> int:
    setup_logging()

    try:
        validate_runtime_config()
        source_side, dest_side = resolve_direction()
    except Exception as e:
        logging.error(str(e))
        return 2

    logging.info(
        f"DIRECTION={DIRECTION!r} -> source={source_side} destination={dest_side}"
    )

    logging.info(
        f"PHASE={PHASE} | DRY_RUN={DRY_RUN} | SKIP_EXISTING={SKIP_EXISTING} | "
        f"PULL_HEADERS_MODE={PULL_HEADERS_MODE} | PRESERVE_SCAN_METADATA={PRESERVE_SCAN_METADATA} | "
        f"REAPPLY_AFTER_PULL={REAPPLY_SCAN_METADATA_AFTER_PULL} | "
        f"NORMALIZE_ZIPS={NORMALIZE_DOWNLOADED_ZIPS} | SORT_ZIP_ALPHA={SORT_ZIP_ENTRIES_ALPHABETICAL}"
    )
    logging.info(f"SRC={SRC_BASE_URL} project={SRC_PROJECT} | DST={DST_BASE_URL} project={DST_PROJECT}")

    try:
        # Always prompt in fixed endpoint order: A first, then B.
        # This avoids any confusion when DIRECTION="backwards".
        a_user, a_pass = prompt_credentials(
            endpoint_label="endpoint A",
            base_url=A_BASE_URL,
        )
        b_user, b_pass = prompt_credentials(
            endpoint_label="endpoint B",
            base_url=B_BASE_URL,
        )
    except CredentialPromptCancelled as e:
        logging.error(str(e))
        return 1
    except Exception as e:
        logging.exception(f"[AUTH] failed to obtain credentials: {e}")
        return 1

    # Build fixed endpoint clients first.
    x_a = XNAT(A_BASE_URL, a_user, a_pass, verify_tls=VERIFY_TLS)
    x_b = XNAT(B_BASE_URL, b_user, b_pass, verify_tls=VERIFY_TLS)

    # Then map them to active source/destination according to DIRECTION.
    if SOURCE_SIDE_NAME == "A":
        xsrc = x_a
        xdst = x_b
    elif SOURCE_SIDE_NAME == "B":
        xsrc = x_b
        xdst = x_a
    else:
        logging.error(f"Unexpected SOURCE_SIDE_NAME={SOURCE_SIDE_NAME!r}")
        return 2

    logging.info(
        f"[AUTH] credential mapping applied: "
        f"A->{A_BASE_URL} | B->{B_BASE_URL} | "
        f"active source={SOURCE_SIDE_NAME} active destination={DEST_SIDE_NAME}"
    )

    try:
        if PHASE == "dicom":
            run_dicom_phase(xsrc, xdst)
        else:
            run_resources_phase(xsrc, xdst)
    except Exception as e:
        logging.exception(str(e))
        return 1

    logging.info("DONE")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())