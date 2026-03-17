#!/usr/bin/env python3
"""
xnat_copy_scan_resource.py

Copy a scan-level resource folder to a new resource label *within the same XNAT*,
using REST:

  1) Download existing resource as zip (format=zip)
  2) Create destination resource folder (new label)
  3) Upload zip into destination resource with extract=true
  4) (Optional) refresh catalog and wait for expected file_count
  5) (Optional) delete the source resource (i.e., "rename" behaviour)

Credentials
-----------
- Username and password are prompted at runtime via pop-up windows.
"""

from __future__ import annotations

import getpass
import logging
import shutil
import time
import zipfile
from pathlib import Path, PurePosixPath
from typing import Dict, List, Optional, Tuple

import requests

# =========================
# USER CONFIG
# =========================

BASE_URL = ""
PROJECT = ""

# Which subjects to process (subject labels)
SUBJECT_LABELS = [""]

# Session labels
SESSION_LABELS: Optional[List[str]] = [""]

# Scan IDs
SCAN_IDS: Optional[List[str]] = [""]

# Resource copy rules (source_label -> dest_label)
RESOURCE_COPIES: List[Tuple[str, str]] = [
    ("", ""),
]

STAGING_DIR = Path(r"")

DRY_RUN = False

# If True, skip when destination resource already has files (>0)
SKIP_EXISTING = True

# If True, delete destination resource before uploading (clean overwrite)
CLEAR_DEST_RESOURCE_BEFORE_UPLOAD = False

# If True, delete the source resource after successful copy (rename-like)
DELETE_SOURCE_RESOURCE_AFTER_COPY = False

# After upload+extract, refresh catalog so extracted files are added to catalog entries.
REFRESH_CATALOG_AFTER_UPLOAD = True
REFRESH_CATALOG_OPTIONS = "append,populateStats"

# Wait/poll after upload until dest resource file_count reaches expected count
WAIT_FOR_FILES = True
WAIT_TIMEOUT_SEC = 600
WAIT_POLL_SEC = 10

VERIFY_TLS = True
TIMEOUT = 3600  # seconds

# -------------------------
# ZIP PATH NORMALISATION
# -------------------------

# If True, normalise the downloaded zip so extracted files land at the resource root.
NORMALIZE_DOWNLOADED_ZIPS = True

# Skip normalisation for these resources (usually DICOM)
NORMALIZE_SKIP_RESOURCE_LABELS = {"DICOM"}

# If True, delete original downloaded zip once normalised zip is created
DELETE_ORIGINAL_ZIP_AFTER_NORMALIZE = True

# Ensure zip entries are WRITTEN in alphabetical order (helps extraction/catalog order)
SORT_ZIP_ENTRIES_ALPHABETICAL = True

# -------------------------
# RESILIENCY / LARGE FILES
# -------------------------

DOWNLOAD_RETRIES = 3
UPLOAD_RETRIES = 3
RETRY_BACKOFF_BASE_SEC = 6  # backoff = base * attempt

# Zip validation
FULL_ZIP_INTEGRITY_CHECK = False  # CRC all files (slow)
ZIP_SAMPLE_READ_FILES = 3
ZIP_SAMPLE_READ_BYTES = 4096

# Large zip splitting
SPLIT_LARGE_ZIPS = True
SPLIT_THRESHOLD_BYTES = int(500 * 1024 * 1024)
SPLIT_PART_TARGET_BYTES = int(250 * 1024 * 1024)

PRESERVE_ZIP_FILENAME_ON_DEST = True
CLEANUP_STAGED_ZIPS = True

RETRYABLE_ERROR_SUBSTRINGS = (
    "unexpected eof",
    "eof",
    "connection aborted",
    "connectionreseterror",
    "connection reset",
    "broken pipe",
    "read timed out",
    "timed out",
    "invalidchunklength",
    "chunkedencodingerror",
    "502",
    "503",
    "504",
)

# =========================
# END USER CONFIG
# =========================


class CredentialPromptCancelled(Exception):
    """Raised when the user cancels credential entry."""


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
            user = simpledialog.askstring(
                title="XNAT Login",
                prompt=f"Enter username for:\n{base_url}",
                parent=root,
            )
            if user is None:
                raise CredentialPromptCancelled("Credential entry cancelled.")
            user = user.strip()
            if user:
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

        return user, password

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
        logging.warning(f"[AUTH] GUI credential prompt unavailable: {e}. Falling back to terminal input.")

        user = input(f"Enter username for {base_url}: ").strip()
        if not user:
            raise CredentialPromptCancelled("Username entry cancelled/empty.")

        password = getpass.getpass(f"Enter password for {base_url}: ").strip()
        if not password:
            raise CredentialPromptCancelled("Password entry cancelled/empty.")

        return user, password


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def _sleep_backoff(attempt: int) -> None:
    time.sleep(RETRY_BACKOFF_BASE_SEC * attempt)


def _is_retryable_error(msg: str) -> bool:
    m = (msg or "").lower()
    return any(s in m for s in RETRYABLE_ERROR_SUBSTRINGS)


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
    pp = PurePosixPath(p.replace("\\", "/"))
    parts = [x for x in pp.parts if x not in ("", "/", ".", "..")]
    return "/".join(parts)


def _strip_prefix_to_resource_files(member_path: str, resource_label: str) -> Optional[str]:
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


def maybe_normalize_zip(zip_path: Path, resource_label: str) -> Path:
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


def split_zip_into_parts(zip_path: Path, out_dir: Path, part_target_bytes: int) -> List[Path]:
    """
    Split a zip into multiple smaller zips, keeping entries in alphabetical order
    within each part (and globally if we split a sorted list in sequence).
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
        current_est = 0
        wrote_any = False

        zout = zipfile.ZipFile(current_part_path, "w", compression=zipfile.ZIP_DEFLATED)
        try:
            for info in infos:
                next_est = current_est + int(info.file_size or 0)
                if wrote_any and next_est > part_target_bytes:
                    zout.close()
                    parts.append(current_part_path)

                    part_idx += 1
                    current_part_path = out_dir / f"{base_stem}.part{part_idx:03d}.zip"
                    current_est = 0
                    wrote_any = False
                    zout = zipfile.ZipFile(current_part_path, "w", compression=zipfile.ZIP_DEFLATED)

                with zin.open(info, "r") as src:
                    zi = zipfile.ZipInfo(filename=_safe_rel_posix(info.filename), date_time=info.date_time)
                    zi.external_attr = info.external_attr
                    zi.compress_type = zipfile.ZIP_DEFLATED
                    with zout.open(zi, "w") as dst:
                        shutil.copyfileobj(src, dst, length=1024 * 1024)

                wrote_any = True
                current_est += int(info.file_size or 0)

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
        raise RuntimeError(f"Downloaded zip is invalid/corrupt: {zip_path}")

    if SPLIT_LARGE_ZIPS:
        size_bytes = zip_path.stat().st_size if zip_path.exists() else 0
        if size_bytes > SPLIT_THRESHOLD_BYTES:
            logging.info(f"[ZIP] large zip {zip_path.name} -> splitting into parts")
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

    def delete(self, path: str, params: Optional[Dict] = None) -> str:
        try:
            r = self.s.delete(self._url(path), params=params, timeout=TIMEOUT)
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"DELETE {path} failed (request exception): {e}") from e
        if r.status_code >= 400:
            raise RuntimeError(f"DELETE {path} failed: {r.status_code} {r.text[:300]}")
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


def list_sessions_for_subject(x: XNAT, subject_label: str) -> List[Dict]:
    j = x.get_json(
        f"/data/projects/{PROJECT}/subjects/{subject_label}/experiments",
        params={"format": "json", "limit": "*"},
    )
    expts = rs_result_list(j)
    out = [e for e in expts if "mrSessionData" in str(e.get("xsiType", ""))]

    if SESSION_LABELS:
        wanted = set(SESSION_LABELS)
        out = [e for e in out if str(e.get("label", "")) in wanted]

    return out


def list_scans_by_expt_id(x: XNAT, experiment_id: str) -> List[Dict]:
    j = x.get_json(f"/data/experiments/{experiment_id}/scans", params={"format": "json"})
    scans = rs_result_list(j)
    if SCAN_IDS:
        wanted = set(str(s) for s in SCAN_IDS)
        scans = [s for s in scans if str(s.get("ID")) in wanted]
    return scans


def list_scan_resources(x: XNAT, experiment_id: str, scan_id: str) -> List[Dict]:
    j = x.get_json(f"/data/experiments/{experiment_id}/scans/{scan_id}/resources", params={"format": "json"})
    return rs_result_list(j)


def get_resource_meta(x: XNAT, experiment_id: str, scan_id: str, resource_label: str) -> Optional[Dict]:
    for r in list_scan_resources(x, experiment_id, scan_id):
        if str(r.get("label", "")) == resource_label:
            return r
    return None


def get_resource_file_count(x: XNAT, experiment_id: str, scan_id: str, resource_label: str) -> int:
    r = get_resource_meta(x, experiment_id, scan_id, resource_label)
    if not r:
        return 0
    try:
        return int(r.get("file_count") or 0)
    except Exception:
        return 0


def resource_exists(x: XNAT, experiment_id: str, scan_id: str, resource_label: str) -> bool:
    for r in list_scan_resources(x, experiment_id, scan_id):
        if str(r.get("label", "")) == resource_label:
            return True
    return False


def ensure_resource_folder(
    x: XNAT,
    experiment_id: str,
    scan_id: str,
    resource_label: str,
    fmt: Optional[str] = None,
    content: Optional[str] = None,
) -> None:
    if DRY_RUN:
        return

    if resource_exists(x, experiment_id, scan_id, resource_label):
        return

    params: Dict[str, str] = {}
    if fmt:
        params["format"] = str(fmt)
    if content:
        params["content"] = str(content)

    x.put(
        f"/data/experiments/{experiment_id}/scans/{scan_id}/resources/{resource_label}",
        params=params if params else None,
    )


def refresh_catalog_append(x: XNAT, archive_resource_path: str, options: str) -> None:
    if DRY_RUN:
        return
    x.post("/data/services/refresh/catalog", params={"resource": archive_resource_path, "options": options})


def wait_for_resource_files(
    x: XNAT,
    experiment_id: str,
    scan_id: str,
    resource_label: str,
    expected_min: int,
    timeout_sec: int,
    poll_sec: int,
) -> None:
    if DRY_RUN:
        return
    t0 = time.time()
    while True:
        cnt = get_resource_file_count(x, experiment_id, scan_id, resource_label)
        if cnt >= expected_min:
            return
        if time.time() - t0 > timeout_sec:
            logging.warning(
                f"[WAIT] timeout waiting for {resource_label} scan={scan_id}: have {cnt}, expected >= {expected_min}"
            )
            return
        time.sleep(poll_sec)


def download_resource_zip_with_retry(
    x: XNAT,
    experiment_id: str,
    scan_id: str,
    resource_label: str,
    out_zip: Path,
) -> None:
    path = f"/data/experiments/{experiment_id}/scans/{scan_id}/resources/{resource_label}/files"
    logging.info(f"[XNAT] download zip res={resource_label} scan={scan_id} -> {out_zip.name}")
    if DRY_RUN:
        return

    for attempt in range(1, DOWNLOAD_RETRIES + 1):
        try:
            if out_zip.exists():
                out_zip.unlink()
        except Exception:
            pass

        try:
            x.download_to_file(path, out_zip, params={"format": "zip"})
            if validate_zip(out_zip):
                return
            raise RuntimeError(f"Downloaded zip failed validation: {out_zip.name}")
        except Exception as e:
            logging.warning(f"[XNAT] download failed (attempt {attempt}/{DOWNLOAD_RETRIES}): {e}")
            if attempt >= DOWNLOAD_RETRIES:
                raise
            _sleep_backoff(attempt)


def upload_zip_extract_with_retry(
    x: XNAT,
    experiment_id: str,
    scan_id: str,
    resource_label: str,
    zip_path: Path,
) -> None:
    if DRY_RUN:
        logging.info(f"[XNAT] (dry-run) would upload+extract {zip_path.name} -> res={resource_label} scan={scan_id}")
        return

    if not validate_zip(zip_path):
        raise RuntimeError(f"Refusing to upload invalid zip: {zip_path}")

    dest_name = zip_path.name if PRESERVE_ZIP_FILENAME_ON_DEST else f"{resource_label}.zip"
    put_path = f"/data/experiments/{experiment_id}/scans/{scan_id}/resources/{resource_label}/files/{dest_name}"

    last_err: Optional[Exception] = None
    for attempt in range(1, UPLOAD_RETRIES + 1):
        try:
            logging.info(f"[XNAT] upload+extract res={resource_label} scan={scan_id} <- {zip_path.name}")
            x.put_file_multipart(put_path, zip_path, params={"extract": "true"})
            return
        except Exception as e:
            last_err = e
            msg = str(e)
            if _is_retryable_error(msg) and attempt < UPLOAD_RETRIES:
                logging.warning(f"[XNAT] upload failed (attempt {attempt}/{UPLOAD_RETRIES}) retryable: {msg}")
                _sleep_backoff(attempt)
                continue
            raise

    if last_err:
        raise last_err


def upload_zip_extract_resilient(
    x: XNAT,
    experiment_id: str,
    scan_id: str,
    resource_label: str,
    zip_path: Path,
) -> None:
    parts = prepare_zip_for_upload(zip_path)
    for p in parts:
        upload_zip_extract_with_retry(x, experiment_id, scan_id, resource_label, p)
        if CLEANUP_STAGED_ZIPS and not DRY_RUN:
            try:
                p.unlink()
            except Exception:
                pass


def copy_resource_for_scan(
    x: XNAT,
    experiment_id: str,
    scan_id: str,
    src_res: str,
    dst_res: str,
) -> None:
    src_meta = get_resource_meta(x, experiment_id, scan_id, src_res)
    if not src_meta:
        logging.info(f"[SKIP] src resource not found: scan={scan_id} res={src_res}")
        return

    try:
        src_fc = int(src_meta.get("file_count") or 0)
    except Exception:
        src_fc = 0
    if src_fc <= 0:
        logging.info(f"[SKIP] src resource empty: scan={scan_id} res={src_res}")
        return

    if SKIP_EXISTING and get_resource_file_count(x, experiment_id, scan_id, dst_res) > 0:
        logging.info(f"[SKIP] dst already has files: scan={scan_id} res={dst_res}")
        return

    if CLEAR_DEST_RESOURCE_BEFORE_UPLOAD and not DRY_RUN:
        try:
            logging.info(f"[DST] clearing destination resource: scan={scan_id} res={dst_res}")
            x.delete(f"/data/experiments/{experiment_id}/scans/{scan_id}/resources/{dst_res}")
        except Exception as e:
            logging.warning(f"[DST] clear destination resource warning: {e}")

    fmt = src_meta.get("format") or None
    content = src_meta.get("content") or None
    ensure_resource_folder(x, experiment_id, scan_id, dst_res, fmt=fmt, content=content)

    STAGING_DIR.mkdir(parents=True, exist_ok=True)
    zip_path = STAGING_DIR / f"{PROJECT}_{experiment_id}_scan{scan_id}_{src_res}.zip"

    download_resource_zip_with_retry(x, experiment_id, scan_id, src_res, zip_path)

    zip_path_use = maybe_normalize_zip(zip_path, src_res)

    upload_zip_extract_resilient(x, experiment_id, scan_id, dst_res, zip_path_use)

    if REFRESH_CATALOG_AFTER_UPLOAD and not DRY_RUN:
        archive_path = f"/archive/experiments/{experiment_id}/scans/{scan_id}/resources/{dst_res}"
        refresh_catalog_append(x, archive_path, REFRESH_CATALOG_OPTIONS)

    if WAIT_FOR_FILES and src_fc > 0 and not DRY_RUN:
        wait_for_resource_files(
            x,
            experiment_id,
            scan_id,
            dst_res,
            expected_min=src_fc,
            timeout_sec=WAIT_TIMEOUT_SEC,
            poll_sec=WAIT_POLL_SEC,
        )

    if CLEANUP_STAGED_ZIPS and not DRY_RUN:
        for p in {zip_path, zip_path_use}:
            try:
                if p.exists():
                    p.unlink()
            except Exception:
                pass

    if DELETE_SOURCE_RESOURCE_AFTER_COPY and not DRY_RUN:
        try:
            logging.info(f"[DELETE] deleting source resource after copy: scan={scan_id} res={src_res}")
            x.delete(f"/data/experiments/{experiment_id}/scans/{scan_id}/resources/{src_res}")
        except Exception as e:
            logging.warning(f"[DELETE] failed to delete source resource scan={scan_id} res={src_res}: {e}")


def run(x: XNAT) -> None:
    for subj in SUBJECT_LABELS:
        logging.info(f"=== SUBJECT {subj} ===")

        try:
            sessions = list_sessions_for_subject(x, subj)
        except Exception as e:
            logging.error(f"[SUBJECT] failed to list sessions for {subj}: {e}")
            continue

        if not sessions:
            logging.warning(f"[SUBJECT] no MR sessions found for {subj}")
            continue

        for sess in sessions:
            expt_id = str(sess.get("ID"))
            sess_label = str(sess.get("label", ""))
            logging.info(f"--- SESSION {sess_label} (EXPT ID={expt_id}) ---")

            try:
                scans = list_scans_by_expt_id(x, expt_id)
            except Exception as e:
                logging.error(f"[SESSION] failed to list scans expt={expt_id}: {e}")
                continue

            for s in scans:
                scan_id = str(s.get("ID") or "")
                if not scan_id:
                    continue

                for (src_res, dst_res) in RESOURCE_COPIES:
                    try:
                        logging.info(f"[COPY] scan={scan_id} {src_res} -> {dst_res}")
                        copy_resource_for_scan(x, expt_id, scan_id, src_res, dst_res)
                    except Exception as e:
                        logging.error(f"[COPY] failed expt={expt_id} scan={scan_id} {src_res}->{dst_res}: {e}")
                        continue


def main() -> int:
    setup_logging()

    logging.info(
        f"DRY_RUN={DRY_RUN} | SKIP_EXISTING={SKIP_EXISTING} | CLEAR_DEST={CLEAR_DEST_RESOURCE_BEFORE_UPLOAD} "
        f"| NORMALIZE_ZIPS={NORMALIZE_DOWNLOADED_ZIPS} | SORT_ZIP_ALPHA={SORT_ZIP_ENTRIES_ALPHABETICAL}"
    )
    logging.info(f"XNAT={BASE_URL} project={PROJECT}")
    logging.info(f"SUBJECTS={SUBJECT_LABELS}")
    logging.info(f"SESSION_LABELS={SESSION_LABELS}")
    logging.info(f"SCAN_IDS={SCAN_IDS}")
    logging.info(f"RESOURCE_COPIES={RESOURCE_COPIES}")

    try:
        user, password = prompt_credentials(BASE_URL)
    except CredentialPromptCancelled as e:
        logging.error(str(e))
        return 1
    except Exception as e:
        logging.exception(f"[AUTH] failed to obtain credentials: {e}")
        return 1

    x = XNAT(BASE_URL, user, password, verify_tls=VERIFY_TLS)

    try:
        run(x)
    except Exception as e:
        logging.exception(str(e))
        return 1

    logging.info("DONE")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())