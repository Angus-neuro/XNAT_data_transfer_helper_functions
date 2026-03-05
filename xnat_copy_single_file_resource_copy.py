#!/usr/bin/env python3
"""
xnat_copy_single_file_resource.py

Copy ONE targeted file from:
  scan A / resource X  ->  scan B / resource Y
WITHIN THE SAME MR SESSION (experiment), based on a partial string match.

Optionally delete the source file after successful upload (move behaviour).

Rules:
- Identify candidate source files by substring match against the resource file "Name".
- If 0 matches: do nothing (log and exit 0).
- If >1 matches: do nothing (log and exit 0).  **No changes are made.**
- If exactly 1 match: download that file, upload to destination resource,
  then optionally delete it from source (controlled by DELETE_SOURCE_AFTER_UPLOAD).

"""

from __future__ import annotations

import logging
import time
from pathlib import Path, PurePosixPath
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote

import requests

# Optional progress bar
try:
    from tqdm import tqdm  # type: ignore
except Exception:
    tqdm = None


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
SRC_SCAN_ID = "17"
SRC_RESOURCE_LABEL = ""

DST_SCAN_ID = "17"
DST_RESOURCE_LABEL = ""

# --- Targeting ---
TARGET_PARTIAL_MATCH = ""  # substring match on source file name
MATCH_CASE_INSENSITIVE = True

# --- OPTIONAL: rename on upload ---
# If set to a non-empty string, upload will use this EXACT destination name (can include subfolders).
# Example:
#   DST_RENAME_TO = "my_new_name.nii.gz"
DST_RENAME_TO: Optional[str] = ""

# Destination filename handling (used ONLY if DST_RENAME_TO is None/empty):
#   "basename"       -> upload as basename only (lands in destination resource root)
#   "preserve_path"  -> upload preserving any subfolders (as-is, sanitised)
DST_FILENAME_MODE = "basename"

STAGING_DIR = Path(r"")

DRY_RUN = False

# If True: if destination already contains the destination filename, skip (do nothing).
SKIP_IF_DST_FILE_EXISTS = True

# If True and destination filename exists: delete destination file before uploading.
# Only used when SKIP_IF_DST_FILE_EXISTS is False.
OVERWRITE_DST_FILE = False

# NEW: If True, delete source file after successful upload (move behaviour).
# If False, keep source file (copy-only behaviour).
DELETE_SOURCE_AFTER_UPLOAD = False

# After upload/delete, refresh catalog so changes appear promptly in XNAT UI/API.
REFRESH_CATALOG_AFTER_UPLOAD = True
REFRESH_CATALOG_AFTER_DELETE = True
REFRESH_CATALOG_OPTIONS = "append,populateStats"

VERIFY_TLS = True
TIMEOUT = 3600  # seconds

# -------------------------
# RESILIENCY
# -------------------------

DOWNLOAD_RETRIES = 3
UPLOAD_RETRIES = 3
DELETE_RETRIES = 3
RETRY_BACKOFF_BASE_SEC = 6  # backoff = base * attempt

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


def _safe_posix_relpath(p: str) -> str:
    """
    Make a safe relative posix path (strip leading '/', strip '..' segments).
    """
    pp = PurePosixPath(p.replace("\\", "/"))
    parts = [x for x in pp.parts if x not in ("", "/", ".", "..")]
    return "/".join(parts)


def _quote_path_keep_slashes(p: str) -> str:
    """
    URL-encode a path component that may include '/' subfolders, preserving '/'.
    """
    return quote(p, safe="/")


def _make_pbar(total: Optional[int], desc: str):
    """
    Create a tqdm progress bar if tqdm is available; otherwise return None.
    """
    if tqdm is None:
        return None
    return tqdm(
        total=total,
        desc=desc,
        unit="B",
        unit_scale=True,
        unit_divisor=1024,
        leave=True,
    )


class _ProgressFileWrapper:
    """
    Wrap a binary file object so reads update a tqdm progress bar.
    """
    def __init__(self, fp, pbar):
        self._fp = fp
        self._pbar = pbar

    def read(self, n: int = -1):
        data = self._fp.read(n)
        if data and self._pbar is not None:
            self._pbar.update(len(data))
        return data

    def __getattr__(self, name):
        return getattr(self._fp, name)


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

    def get(self, path: str, params: Optional[Dict] = None, stream: bool = False) -> requests.Response:
        return self.s.get(self._url(path), params=params, timeout=TIMEOUT, stream=stream)

    def get_json(self, path: str, params: Optional[Dict] = None) -> Dict:
        r = self.get(path, params=params, stream=False)
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

    def put_file_multipart(
        self,
        path: str,
        file_path: Path,
        params: Optional[Dict] = None,
        content_type: str = "application/octet-stream",
        field_name: str = "file",
        progress_desc: Optional[str] = None,
    ) -> str:
        """
        Upload a file as multipart/form-data with an optional tqdm progress bar (if tqdm is installed).
        """
        try:
            total = None
            try:
                total = int(file_path.stat().st_size)
            except Exception:
                total = None

            pbar = _make_pbar(total, progress_desc or f"Upload {file_path.name}")

            with file_path.open("rb") as raw_fp:
                fp = _ProgressFileWrapper(raw_fp, pbar)
                files = {field_name: (file_path.name, fp, content_type)}
                r = self.s.put(self._url(path), params=params, files=files, timeout=TIMEOUT)

            if pbar is not None:
                pbar.close()

        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"PUT(file) {path} failed (request exception): {e}") from e
        finally:
            # Ensure the bar isn't left open on unexpected errors
            try:
                if "pbar" in locals() and pbar is not None:
                    pbar.close()
            except Exception:
                pass

        if r.status_code >= 400:
            raise RuntimeError(f"PUT(file) {path} failed: {r.status_code} {r.text[:300]}")
        return (r.text or "").strip()

    def download_to_file(self, path: str, out_path: Path, params: Optional[Dict] = None, progress_desc: Optional[str] = None) -> None:
        """
        Stream-download to a file with an optional tqdm progress bar (if tqdm is installed).
        """
        out_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = out_path.with_suffix(out_path.suffix + ".part")

        pbar = None
        try:
            with self.get(path, params=params, stream=True) as r:
                if r.status_code >= 400:
                    raise RuntimeError(f"GET(download) {path} failed: {r.status_code} {r.text[:300]}")

                total = None
                cl = r.headers.get("Content-Length") or r.headers.get("content-length")
                try:
                    if cl is not None:
                        total = int(cl)
                except Exception:
                    total = None

                pbar = _make_pbar(total, progress_desc or f"Download {out_path.name}")

                with tmp_path.open("wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        if not chunk:
                            continue
                        f.write(chunk)
                        if pbar is not None:
                            pbar.update(len(chunk))

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
        finally:
            try:
                if pbar is not None:
                    pbar.close()
            except Exception:
                pass


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
        if " 409 " in str(e) or "Specified resource already exists" in str(e):
            return
        raise


def refresh_catalog_append(x: XNAT, archive_resource_path: str, options: str) -> None:
    if DRY_RUN:
        return
    x.post("/data/services/refresh/catalog", params={"resource": archive_resource_path, "options": options})


def list_resource_files(x: XNAT, experiment_id: str, scan_id: str, resource_label: str) -> List[Dict]:
    j = x.get_json(
        f"/data/experiments/{experiment_id}/scans/{scan_id}/resources/{resource_label}/files",
        params={"format": "json", "limit": "*"},
    )
    return rs_result_list(j)


def _row_name(r: Dict) -> str:
    return str(r.get("Name") or r.get("name") or r.get("path") or "").strip()


def find_single_match_file(
    files: List[Dict],
    partial: str,
    case_insensitive: bool = True,
) -> Tuple[List[str], List[str]]:
    all_names: List[str] = []
    matched: List[str] = []
    p = (partial or "")
    if case_insensitive:
        p = p.lower()

    for r in files:
        name = _row_name(r)
        if not name or name.endswith("/"):
            continue

        all_names.append(name)
        hay = name.lower() if case_insensitive else name
        if p in hay:
            matched.append(name)

    return all_names, matched


def download_file_with_retry(
    x: XNAT,
    experiment_id: str,
    scan_id: str,
    resource_label: str,
    remote_name: str,
    out_path: Path,
) -> None:
    remote_name = _safe_posix_relpath(remote_name)
    enc = _quote_path_keep_slashes(remote_name)
    path = f"/data/experiments/{experiment_id}/scans/{scan_id}/resources/{resource_label}/files/{enc}"

    logging.info(f"[XNAT] download file scan={scan_id} res={resource_label} name={remote_name}")
    if DRY_RUN:
        return

    desc = f"Download scan {scan_id} {resource_label}: {PurePosixPath(remote_name).name}"

    for attempt in range(1, DOWNLOAD_RETRIES + 1):
        try:
            if out_path.exists():
                out_path.unlink()
        except Exception:
            pass

        try:
            x.download_to_file(path, out_path, params=None, progress_desc=desc)
            if out_path.exists() and out_path.stat().st_size >= 0:
                return
            raise RuntimeError("Downloaded file missing after download.")
        except Exception as e:
            logging.warning(f"[XNAT] download failed (attempt {attempt}/{DOWNLOAD_RETRIES}): {e}")
            if attempt >= DOWNLOAD_RETRIES:
                raise
            _sleep_backoff(attempt)


def upload_file_with_retry(
    x: XNAT,
    experiment_id: str,
    scan_id: str,
    resource_label: str,
    local_file: Path,
    dst_name: str,
) -> None:
    dst_name = _safe_posix_relpath(dst_name)
    enc = _quote_path_keep_slashes(dst_name)
    put_path = f"/data/experiments/{experiment_id}/scans/{scan_id}/resources/{resource_label}/files/{enc}"

    if DRY_RUN:
        logging.info(
            f"[XNAT] (dry-run) would upload scan={scan_id} res={resource_label} <- {local_file.name} as {dst_name}"
        )
        return

    desc = f"Upload scan {scan_id} {resource_label}: {PurePosixPath(dst_name).name}"

    last_err: Optional[Exception] = None
    for attempt in range(1, UPLOAD_RETRIES + 1):
        try:
            logging.info(f"[XNAT] upload scan={scan_id} res={resource_label} <- {local_file.name} as {dst_name}")
            x.put_file_multipart(
                put_path,
                local_file,
                params=None,
                content_type="application/octet-stream",
                progress_desc=desc,
            )
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


def delete_file_with_retry(
    x: XNAT,
    experiment_id: str,
    scan_id: str,
    resource_label: str,
    remote_name: str,
) -> None:
    remote_name = _safe_posix_relpath(remote_name)
    enc = _quote_path_keep_slashes(remote_name)
    path = f"/data/experiments/{experiment_id}/scans/{scan_id}/resources/{resource_label}/files/{enc}"

    if DRY_RUN:
        logging.info(f"[XNAT] (dry-run) would delete source file scan={scan_id} res={resource_label} name={remote_name}")
        return

    for attempt in range(1, DELETE_RETRIES + 1):
        try:
            logging.info(f"[XNAT] delete source file scan={scan_id} res={resource_label} name={remote_name}")
            x.delete(path)
            return
        except Exception as e:
            msg = str(e)
            if _is_retryable_error(msg) and attempt < DELETE_RETRIES:
                logging.warning(f"[XNAT] delete failed (attempt {attempt}/{DELETE_RETRIES}) retryable: {msg}")
                _sleep_backoff(attempt)
                continue
            raise


def file_exists_in_resource(
    x: XNAT,
    experiment_id: str,
    scan_id: str,
    resource_label: str,
    name_to_check: str,
    case_insensitive: bool = False,
) -> bool:
    files = list_resource_files(x, experiment_id, scan_id, resource_label)
    want = _safe_posix_relpath(name_to_check)
    if case_insensitive:
        want = want.lower()
    for r in files:
        n = _safe_posix_relpath(_row_name(r))
        if not n:
            continue
        if case_insensitive:
            if n.lower() == want:
                return True
        else:
            if n == want:
                return True
    return False


def delete_dst_file_if_needed(
    x: XNAT,
    experiment_id: str,
    dst_scan: str,
    dst_res: str,
    dst_name: str,
) -> None:
    if not OVERWRITE_DST_FILE:
        return
    if DRY_RUN:
        logging.info(f"[XNAT] (dry-run) would delete destination existing file {dst_name}")
        return

    if file_exists_in_resource(x, experiment_id, dst_scan, dst_res, dst_name, case_insensitive=False):
        enc = _quote_path_keep_slashes(_safe_posix_relpath(dst_name))
        path = f"/data/experiments/{experiment_id}/scans/{dst_scan}/resources/{dst_res}/files/{enc}"
        logging.info(f"[XNAT] delete destination existing file scan={dst_scan} res={dst_res} name={dst_name}")
        x.delete(path)


def choose_dst_name(src_name: str) -> str:
    """
    Decide destination name/path inside the destination resource.
    Precedence:
      1) DST_RENAME_TO (if non-empty) [exact]
      2) DST_FILENAME_MODE applied to src_name
    """
    if DST_RENAME_TO is not None and str(DST_RENAME_TO).strip() != "":
        return _safe_posix_relpath(str(DST_RENAME_TO).strip())

    src_safe = _safe_posix_relpath(src_name)
    if DST_FILENAME_MODE == "basename":
        return PurePosixPath(src_safe).name
    if DST_FILENAME_MODE == "preserve_path":
        return src_safe
    raise ValueError(f"Unknown DST_FILENAME_MODE: {DST_FILENAME_MODE}")


def move_single_file_between_scans(
    x: XNAT,
    experiment_id: str,
    src_scan: str,
    src_res: str,
    dst_scan: str,
    dst_res: str,
    partial: str,
) -> None:
    src_meta = get_resource_meta(x, experiment_id, src_scan, src_res)
    if not src_meta:
        raise RuntimeError(f"Source resource not found: scan={src_scan} res={src_res}")

    # Ensure destination resource folder exists
    fmt = src_meta.get("format") or None
    content = src_meta.get("content") or None
    ensure_resource_folder(x, experiment_id, dst_scan, dst_res, fmt=fmt, content=content)

    # List and match source files
    src_files = list_resource_files(x, experiment_id, src_scan, src_res)
    all_names, matched = find_single_match_file(src_files, partial, case_insensitive=MATCH_CASE_INSENSITIVE)

    logging.info(f"[SRC] files in scan={src_scan} res={src_res}: {len(all_names)}")
    logging.info(f"[MATCH] partial='{partial}' -> matches={len(matched)}")

    if len(matched) == 0:
        logging.info("[NOOP] no matching file found; nothing to do.")
        return

    if len(matched) > 1:
        logging.warning("[NOOP] more than one file matched; refusing to act.")
        for m in matched:
            logging.warning(f"  match: {m}")
        return

    src_name = matched[0]
    dst_name = choose_dst_name(src_name)

    # Destination existence logic
    if file_exists_in_resource(x, experiment_id, dst_scan, dst_res, dst_name, case_insensitive=False):
        if SKIP_IF_DST_FILE_EXISTS:
            logging.info(f"[NOOP] destination already has '{dst_name}' (SKIP_IF_DST_FILE_EXISTS=True).")
            return
        if not OVERWRITE_DST_FILE:
            raise RuntimeError(
                f"Destination already has '{dst_name}'. "
                f"Set SKIP_IF_DST_FILE_EXISTS=True to noop, or OVERWRITE_DST_FILE=True to overwrite."
            )

    # Stage local path (use basename for local staging)
    STAGING_DIR.mkdir(parents=True, exist_ok=True)
    staged = STAGING_DIR / (
        f"{PROJECT}_{experiment_id}_scan{src_scan}_{src_res}__{PurePosixPath(_safe_posix_relpath(src_name)).name}"
    )

    action_label = "move" if DELETE_SOURCE_AFTER_UPLOAD else "copy"
    logging.info(f"[PLAN] {action_label} '{src_name}' -> dst as '{dst_name}'")

    # Download
    download_file_with_retry(x, experiment_id, src_scan, src_res, src_name, staged)

    # Upload (optionally delete existing destination file first)
    if not SKIP_IF_DST_FILE_EXISTS and OVERWRITE_DST_FILE:
        delete_dst_file_if_needed(x, experiment_id, dst_scan, dst_res, dst_name)

    upload_file_with_retry(x, experiment_id, dst_scan, dst_res, staged, dst_name)

    # Refresh destination catalog
    if REFRESH_CATALOG_AFTER_UPLOAD and not DRY_RUN:
        archive_path = f"/archive/experiments/{experiment_id}/scans/{dst_scan}/resources/{dst_res}"
        refresh_catalog_append(x, archive_path, REFRESH_CATALOG_OPTIONS)

    # Optional source delete (move vs copy behaviour)
    if DELETE_SOURCE_AFTER_UPLOAD:
        delete_file_with_retry(x, experiment_id, src_scan, src_res, src_name)

        # Refresh source catalog (optional)
        if REFRESH_CATALOG_AFTER_DELETE and not DRY_RUN:
            archive_path = f"/archive/experiments/{experiment_id}/scans/{src_scan}/resources/{src_res}"
            refresh_catalog_append(x, archive_path, REFRESH_CATALOG_OPTIONS)
    else:
        logging.info("[INFO] DELETE_SOURCE_AFTER_UPLOAD=False -> source file retained (copy-only mode).")

    # Cleanup staged file
    if not DRY_RUN:
        try:
            if staged.exists():
                staged.unlink()
        except Exception:
            pass

    logging.info(f"[DONE] {action_label} complete.")


def main() -> int:
    setup_logging()

    logging.info(
        f"DRY_RUN={DRY_RUN} | MATCH='{TARGET_PARTIAL_MATCH}' (case_insensitive={MATCH_CASE_INSENSITIVE}) | "
        f"DST_RENAME_TO={DST_RENAME_TO!r} | DST_FILENAME_MODE={DST_FILENAME_MODE} | "
        f"SKIP_IF_DST_EXISTS={SKIP_IF_DST_FILE_EXISTS} | OVERWRITE_DST={OVERWRITE_DST_FILE} | "
        f"DELETE_SOURCE_AFTER_UPLOAD={DELETE_SOURCE_AFTER_UPLOAD} | "
        f"tqdm={'yes' if tqdm is not None else 'no'}"
    )
    logging.info(f"XNAT={BASE_URL} project={PROJECT} subject={SUBJECT_LABEL} session={SESSION_LABEL}")
    logging.info(f"SRC scan={SRC_SCAN_ID} res={SRC_RESOURCE_LABEL} -> DST scan={DST_SCAN_ID} res={DST_RESOURCE_LABEL}")

    x = XNAT(BASE_URL, USER, PASS, verify_tls=VERIFY_TLS)

    expt_id = find_experiment_id_by_label(x, PROJECT, SESSION_LABEL)
    if not expt_id:
        logging.error(f"Could not find experiment (session) in project: {PROJECT} label={SESSION_LABEL}")
        return 2

    # Ensure destination scan exists
    try:
        ensure_scan_exists(x, PROJECT, SUBJECT_LABEL, SESSION_LABEL, DST_SCAN_ID)
    except Exception as e:
        logging.error(f"Failed ensuring destination scan exists: {e}")
        return 2

    try:
        move_single_file_between_scans(
            x,
            expt_id,
            SRC_SCAN_ID,
            SRC_RESOURCE_LABEL,
            DST_SCAN_ID,
            DST_RESOURCE_LABEL,
            TARGET_PARTIAL_MATCH,
        )
    except Exception as e:
        logging.exception(str(e))
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())