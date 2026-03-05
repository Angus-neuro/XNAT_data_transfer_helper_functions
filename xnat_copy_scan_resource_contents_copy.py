#!/usr/bin/env python3
"""
xnat_copy_between_scans_resource_contents.py

Copy the *contents* of one scan resource into another scan resource
WITHIN THE SAME MR SESSION (experiment), WITHOUT deleting the original.

Example:
  Copy scan 21 / resource NIFTI  ->  scan 15 / resource NIFTI

"""

from __future__ import annotations

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

USER = ""
PASS = ""

# Identify the data scope
SUBJECT_LABEL = ""
SESSION_LABEL = ""   # session label inside the project (MR session)

# Source and destination
SRC_SCAN_ID = "15"
SRC_RESOURCE_LABEL = "NIFTI"

DST_SCAN_ID = "9"
DST_RESOURCE_LABEL = "NIFTI"

STAGING_DIR = Path(r"")

DRY_RUN = False

# If True, skip copy if destination resource already has files (>0)
SKIP_EXISTING_DST = False

# If True, delete destination resource before uploading (overwrite behaviour)
# This does NOT delete the source resource.
CLEAR_DST_RESOURCE_BEFORE_UPLOAD = False

# --- IMPORTANT FIX FOR YOUR ISSUE ---
# If True, repackage the downloaded zip so it extracts into destination resource root.
NORMALIZE_DOWNLOADED_ZIP = True

# Normalisation mode:
#   "strip_to_files" : if zip entries contain ".../files/<name>", keep only "<name>" (and any subfolders under files/)
#   "basename"       : keep only the basename of each file (flattens everything)
ZIP_NORMALIZE_MODE = "strip_to_files"

# After upload+extract, refresh catalog so extracted files are added to catalog entries.
REFRESH_CATALOG_AFTER_UPLOAD = True
REFRESH_CATALOG_OPTIONS = "append,populateStats"

# Wait/poll after upload until dst file_count reaches src file_count (helps avoid races)
WAIT_FOR_FILES = True
WAIT_TIMEOUT_SEC = 600
WAIT_POLL_SEC = 10

VERIFY_TLS = True
TIMEOUT = 3600  # seconds

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
SPLIT_THRESHOLD_BYTES = int(500 * 1024 * 1024)      # split if zip > 500MB
SPLIT_PART_TARGET_BYTES = int(250 * 1024 * 1024)    # aim for ~250MB parts (uncompressed est)

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


def _safe_posix_relpath(p: str) -> str:
    """
    Make a safe relative posix path (strip leading '/', strip '..' segments).
    """
    pp = PurePosixPath(p.replace("\\", "/"))
    parts = [x for x in pp.parts if x not in ("", "/", ".", "..")]
    return "/".join(parts)


def normalize_zip_paths(zip_in: Path, zip_out: Path, mode: str) -> Tuple[Path, int]:
    """
    Repackage zip so internal member paths are appropriate for upload+extract into a resource.

    Returns: (zip_out, num_files_written)

    mode:
      - "strip_to_files": keep only portion after the first occurrence of "/files/"
      - "basename": keep only filename (flatten)
    """
    if DRY_RUN:
        return zip_in, 0

    if not validate_zip(zip_in):
        raise RuntimeError(f"Cannot normalise invalid zip: {zip_in}")

    zip_out.parent.mkdir(parents=True, exist_ok=True)
    if zip_out.exists():
        zip_out.unlink()

    used_names: Dict[str, int] = {}
    written = 0

    with zipfile.ZipFile(zip_in, "r") as zin, zipfile.ZipFile(zip_out, "w", compression=zipfile.ZIP_DEFLATED) as zout:
        for info in zin.infolist():
            if info.is_dir():
                continue

            name = info.filename.replace("\\", "/")

            if mode == "strip_to_files":
                marker = "/files/"
                if marker in name:
                    new_name = name.split(marker, 1)[1]
                else:
                    # fallback: just basename if no obvious /files/ segment
                    new_name = PurePosixPath(name).name
            elif mode == "basename":
                new_name = PurePosixPath(name).name
            else:
                raise ValueError(f"Unknown ZIP_NORMALIZE_MODE: {mode}")

            new_name = _safe_posix_relpath(new_name)

            if not new_name:
                continue

            # handle collisions deterministically
            if new_name in used_names:
                used_names[new_name] += 1
                stem = PurePosixPath(new_name).stem
                suf = PurePosixPath(new_name).suffix
                parent = str(PurePosixPath(new_name).parent)
                parent = "" if parent == "." else parent
                alt = f"{stem}__dup{used_names[new_name]:03d}{suf}"
                new_name = f"{parent}/{alt}" if parent else alt
            else:
                used_names[new_name] = 0

            with zin.open(info, "r") as src:
                # Stream into the new zip
                with zout.open(new_name, "w") as dst:
                    shutil.copyfileobj(src, dst, length=1024 * 1024)

            written += 1

    if not validate_zip(zip_out):
        raise RuntimeError(f"Normalised zip failed validation: {zip_out}")

    return zip_out, written


def split_zip_into_parts(zip_path: Path, out_dir: Path, part_target_bytes: int) -> List[Path]:
    parts: List[Path] = []
    out_dir.mkdir(parents=True, exist_ok=True)

    base_stem = zip_path.stem
    with zipfile.ZipFile(zip_path, "r") as zin:
        infos = [i for i in zin.infolist() if not i.is_dir()]
        if not infos:
            return [zip_path]

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
                    with zout.open(info.filename, "w") as dst:
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
        raise RuntimeError(f"Zip is invalid/corrupt: {zip_path}")

    if SPLIT_LARGE_ZIPS:
        size_bytes = zip_path.stat().st_size if zip_path.exists() else 0
        if size_bytes > SPLIT_THRESHOLD_BYTES:
            logging.info(
                f"[ZIP] large zip {zip_path.name} ({size_bytes/1024**3:.2f} GB) -> splitting into parts"
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


def find_experiment_id_by_label(x: XNAT, project: str, session_label: str) -> Optional[str]:
    j = x.get_json(
        f"/data/projects/{project}/experiments",
        params={
            "format": "json",
            "limit": "*",
            "xsiType": "xnat:mrSessionData",
            "label": session_label,
            "columns": "ID,label,project",
        },
    )
    rows = rs_result_list(j)
    for r in rows:
        if str(r.get("label", "")) == session_label and str(r.get("project", "")) == project:
            return str(r.get("ID"))
    if rows:
        return str(rows[0].get("ID"))
    return None


def ensure_scan_exists(x: XNAT, project: str, subject_label: str, session_label: str, scan_id: str) -> None:
    j = x.get_json(
        f"/data/projects/{project}/subjects/{subject_label}/experiments/{session_label}/scans",
        params={"format": "json"},
    )
    scans = rs_result_list(j)
    if any(str(s.get("ID")) == str(scan_id) for s in scans):
        return

    if DRY_RUN:
        logging.info(f"[XNAT] (dry-run) would create scan {scan_id} under session {session_label}")
        return

    logging.info(f"[XNAT] creating destination scan {scan_id} under session {session_label}")
    x.put(
        f"/data/projects/{project}/subjects/{subject_label}/experiments/{session_label}/scans/{scan_id}",
        params={"xsiType": "xnat:mrScanData", "req_format": "qs"},
    )


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


def ensure_resource_folder(
    x: XNAT,
    experiment_id: str,
    scan_id: str,
    resource_label: str,
    fmt: Optional[str] = None,
    content: Optional[str] = None,
) -> None:
    """
    Create destination resource folder ONLY if it doesn't already exist.
    Avoids the 409 "Specified resource already exists" error.
    """
    if DRY_RUN:
        return

    if get_resource_meta(x, experiment_id, scan_id, resource_label) is not None:
        return

    params: Dict[str, str] = {}
    if fmt:
        params["format"] = str(fmt)
    if content:
        params["content"] = str(content)

    try:
        x.put(
            f"/data/experiments/{experiment_id}/scans/{scan_id}/resources/{resource_label}",
            params=params if params else None,
        )
    except RuntimeError as e:
        # If it raced and now exists, treat 409 as OK
        if " 409 " in str(e) or "Specified resource already exists" in str(e):
            return
        raise


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
    logging.info(f"[XNAT] download zip scan={scan_id} res={resource_label} -> {out_zip.name}")
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
        logging.info(f"[XNAT] (dry-run) would upload+extract {zip_path.name} -> scan={scan_id} res={resource_label}")
        return

    if not validate_zip(zip_path):
        raise RuntimeError(f"Refusing to upload invalid zip: {zip_path}")

    dest_name = zip_path.name if PRESERVE_ZIP_FILENAME_ON_DEST else f"{resource_label}.zip"
    put_path = f"/data/experiments/{experiment_id}/scans/{scan_id}/resources/{resource_label}/files/{dest_name}"

    last_err: Optional[Exception] = None
    for attempt in range(1, UPLOAD_RETRIES + 1):
        try:
            logging.info(f"[XNAT] upload+extract scan={scan_id} res={resource_label} <- {zip_path.name}")
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


def copy_between_scans(
    x: XNAT,
    experiment_id: str,
    src_scan: str,
    src_res: str,
    dst_scan: str,
    dst_res: str,
) -> None:
    src_meta = get_resource_meta(x, experiment_id, src_scan, src_res)
    if not src_meta:
        raise RuntimeError(f"Source resource not found: scan={src_scan} res={src_res}")

    try:
        src_fc = int(src_meta.get("file_count") or 0)
    except Exception:
        src_fc = 0

    if src_fc <= 0:
        raise RuntimeError(f"Source resource is empty: scan={src_scan} res={src_res}")

    if SKIP_EXISTING_DST and get_resource_file_count(x, experiment_id, dst_scan, dst_res) > 0:
        logging.info(f"[SKIP] destination already has files: scan={dst_scan} res={dst_res}")
        return

    if CLEAR_DST_RESOURCE_BEFORE_UPLOAD and not DRY_RUN:
        logging.info(f"[DST] clearing destination resource: scan={dst_scan} res={dst_res}")
        x.delete(f"/data/experiments/{experiment_id}/scans/{dst_scan}/resources/{dst_res}")

    # Create destination resource folder if missing (no 409 failure)
    fmt = src_meta.get("format") or None
    content = src_meta.get("content") or None
    ensure_resource_folder(x, experiment_id, dst_scan, dst_res, fmt=fmt, content=content)

    STAGING_DIR.mkdir(parents=True, exist_ok=True)

    download_zip = STAGING_DIR / f"{PROJECT}_{experiment_id}_scan{src_scan}_{src_res}__download.zip"
    upload_zip = STAGING_DIR / f"{PROJECT}_{experiment_id}_scan{src_scan}_{src_res}__upload.zip"

    download_resource_zip_with_retry(x, experiment_id, src_scan, src_res, download_zip)

    zip_to_upload = download_zip
    if NORMALIZE_DOWNLOADED_ZIP:
        logging.info(f"[ZIP] normalising zip paths mode={ZIP_NORMALIZE_MODE}")
        zip_to_upload, n_written = normalize_zip_paths(download_zip, upload_zip, ZIP_NORMALIZE_MODE)
        logging.info(f"[ZIP] normalised zip ready: {zip_to_upload.name} (files={n_written})")

    upload_zip_extract_resilient(x, experiment_id, dst_scan, dst_res, zip_to_upload)

    if REFRESH_CATALOG_AFTER_UPLOAD and not DRY_RUN:
        archive_path = f"/archive/experiments/{experiment_id}/scans/{dst_scan}/resources/{dst_res}"
        refresh_catalog_append(x, archive_path, REFRESH_CATALOG_OPTIONS)

    if WAIT_FOR_FILES and not DRY_RUN:
        wait_for_resource_files(
            x,
            experiment_id,
            dst_scan,
            dst_res,
            expected_min=src_fc,
            timeout_sec=WAIT_TIMEOUT_SEC,
            poll_sec=WAIT_POLL_SEC,
        )

    if CLEANUP_STAGED_ZIPS and not DRY_RUN:
        for p in (download_zip, upload_zip):
            try:
                if p.exists():
                    p.unlink()
            except Exception:
                pass


def main() -> int:
    setup_logging()

    logging.info(
        f"DRY_RUN={DRY_RUN} | SKIP_EXISTING_DST={SKIP_EXISTING_DST} | CLEAR_DST={CLEAR_DST_RESOURCE_BEFORE_UPLOAD} | "
        f"NORMALIZE_ZIP={NORMALIZE_DOWNLOADED_ZIP} ({ZIP_NORMALIZE_MODE})"
    )
    logging.info(f"XNAT={BASE_URL} project={PROJECT} subject={SUBJECT_LABEL} session={SESSION_LABEL}")
    logging.info(f"SRC scan={SRC_SCAN_ID} res={SRC_RESOURCE_LABEL} -> DST scan={DST_SCAN_ID} res={DST_RESOURCE_LABEL}")

    x = XNAT(BASE_URL, USER, PASS, verify_tls=VERIFY_TLS)

    expt_id = find_experiment_id_by_label(x, PROJECT, SESSION_LABEL)
    if not expt_id:
        logging.error(f"Could not find experiment (session) in project: {PROJECT} label={SESSION_LABEL}")
        return 2

    # Ensure destination scan exists (so we can create dst resource under it)
    try:
        ensure_scan_exists(x, PROJECT, SUBJECT_LABEL, SESSION_LABEL, DST_SCAN_ID)
    except Exception as e:
        logging.error(f"Failed ensuring destination scan exists: {e}")
        return 2

    try:
        copy_between_scans(x, expt_id, SRC_SCAN_ID, SRC_RESOURCE_LABEL, DST_SCAN_ID, DST_RESOURCE_LABEL)
    except Exception as e:
        logging.exception(str(e))
        return 1

    logging.info("DONE")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
