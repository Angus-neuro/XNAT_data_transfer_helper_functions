#!/usr/bin/env python3
"""
folderise_dicoms.py

Organise DICOM files (e.g., .IMA / .dcm / extensionless) into subfolders based on
a scan name read from the DICOM header.

"""

import os
import re
import shutil
from collections import defaultdict

import pydicom
from pydicom.errors import InvalidDicomError


# ------------------------------- USER SETTINGS -------------------------------

INPUT_DIR = r""          
OUTPUT_DIR = r""  

MOVE_FILES = False        # True = move files; False = copy files
DRY_RUN = True          # True = show what would happen, but do not move/copy
RECURSIVE = True         # True = include subfolders inside INPUT_DIR
PREFIX_WITH_SERIES_NUMBER = True  # Helps disambiguate folders if names repeat

# If OUTPUT_DIR is inside INPUT_DIR, we will automatically skip walking into it.


# ------------------------------- PROGRESS BAR --------------------------------

try:
    from tqdm import tqdm
except Exception:
    tqdm = None


def progress_iter(iterable, total=None, desc=None):
    """
    Wrap an iterable with a progress bar if tqdm is available, otherwise return as-is.
    """
    if tqdm is None:
        return iterable
    return tqdm(iterable, total=total, desc=desc, unit="file")


# ------------------------------- HELPERS -------------------------------------

def sanitise_folder_name(name: str, max_len: int = 120) -> str:
    """
    Make a safe folder name for Windows/Linux.
    """
    name = name.strip()
    if not name:
        return "UNKNOWN_SCAN"

    # Replace disallowed characters
    name = re.sub(r'[<>:"/\\|?*\x00-\x1F]', "_", name)
    # Collapse whitespace/underscores a bit
    name = re.sub(r"\s+", " ", name).strip()
    name = re.sub(r"_+", "_", name)

    # Avoid trailing dots/spaces (Windows)
    name = name.rstrip(" .")

    if not name:
        return "UNKNOWN_SCAN"

    if len(name) > max_len:
        name = name[:max_len].rstrip(" ._")

    return name or "UNKNOWN_SCAN"


def is_under(path: str, maybe_parent: str) -> bool:
    """
    Return True if path is under maybe_parent.
    """
    path = os.path.abspath(path)
    maybe_parent = os.path.abspath(maybe_parent)
    try:
        common = os.path.commonpath([path, maybe_parent])
    except ValueError:
        return False
    return common == maybe_parent


def iter_files(root: str, recursive: bool = True):
    """
    Yield file paths from root (optionally recursive).
    Skip OUTPUT_DIR only if it is inside INPUT_DIR.
    """
    root = os.path.abspath(root)
    output_abs = os.path.abspath(OUTPUT_DIR)

    if not recursive:
        for fn in os.listdir(root):
            fp = os.path.join(root, fn)
            if os.path.isfile(fp):
                yield fp
        return

    for dirpath, dirnames, filenames in os.walk(root):
        dirpath_abs = os.path.abspath(dirpath)

        # Only skip the output tree if OUTPUT_DIR is inside the input tree
        # and we have actually walked into OUTPUT_DIR or one of its children.
        if is_under(output_abs, root) and is_under(dirpath_abs, output_abs):
            dirnames[:] = []
            continue

        for fn in filenames:
            yield os.path.join(dirpath, fn)

def get_scan_folder_name(ds) -> str:
    """
    Derive a folder name from a DICOM dataset.
    """
    # Preferred tags
    series_desc = getattr(ds, "SeriesDescription", None)
    protocol = getattr(ds, "ProtocolName", None)
    sequence = getattr(ds, "SequenceName", None)
    series_uid = getattr(ds, "SeriesInstanceUID", None)

    base = series_desc or protocol or sequence or series_uid or "UNKNOWN_SCAN"
    base = str(base)

    if PREFIX_WITH_SERIES_NUMBER:
        series_number = getattr(ds, "SeriesNumber", None)
        if series_number is not None:
            try:
                base = f"{int(series_number):03d} - {base}"
            except Exception:
                base = f"{series_number} - {base}"

    return sanitise_folder_name(base)


def ensure_unique_destination(dest_path: str) -> str:
    """
    If dest_path exists, append a numeric suffix to avoid overwriting.
    """
    if not os.path.exists(dest_path):
        return dest_path

    base, ext = os.path.splitext(dest_path)
    i = 1
    while True:
        candidate = f"{base}__{i:03d}{ext}"
        if not os.path.exists(candidate):
            return candidate
        i += 1


# ------------------------------- MAIN LOGIC ----------------------------------

def organise_dicoms(input_dir: str, output_dir: str):
    input_dir = os.path.abspath(input_dir)
    output_dir = os.path.abspath(output_dir)
    os.makedirs(output_dir, exist_ok=True)

    # Build a list first so we know the total for the progress bar
    files = list(iter_files(input_dir, recursive=RECURSIVE))

    total = 0
    dicom_ok = 0
    skipped_not_dicom = 0
    failed_reads = 0

    per_folder_counts = defaultdict(int)

    for fp in progress_iter(files, total=len(files), desc="Organising DICOMs"):
        total += 1

        if not os.path.isfile(fp):
            continue

        # Attempt to read DICOM header
        try:
            ds = pydicom.dcmread(fp, stop_before_pixels=True, force=False)
        except InvalidDicomError:
            # Some Siemens .IMA can still be valid DICOM but fail strict checks.
            # Try again with force=True (still safe for header-only).
            try:
                ds = pydicom.dcmread(fp, stop_before_pixels=True, force=True)
            except Exception:
                skipped_not_dicom += 1
                continue
        except Exception:
            failed_reads += 1
            continue

        # Basic sanity check
        if not hasattr(ds, "SOPInstanceUID") and not hasattr(ds, "SeriesInstanceUID"):
            skipped_not_dicom += 1
            continue

        dicom_ok += 1

        folder_name = get_scan_folder_name(ds)
        dest_folder = os.path.join(output_dir, folder_name)
        os.makedirs(dest_folder, exist_ok=True)

        dest_path = os.path.join(dest_folder, os.path.basename(fp))
        dest_path = ensure_unique_destination(dest_path)

        per_folder_counts[folder_name] += 1

        if DRY_RUN:
            print(f"[DRY RUN] {'MOVE' if MOVE_FILES else 'COPY'}: {fp} -> {dest_path}")
            continue

        try:
            if MOVE_FILES:
                shutil.move(fp, dest_path)
            else:
                shutil.copy2(fp, dest_path)
        except Exception as e:
            print(f"[WARN] Failed to {'move' if MOVE_FILES else 'copy'} '{fp}': {e}")

    # Summary
    print("\n-------------------- SUMMARY --------------------")
    print(f"Input directory:     {input_dir}")
    print(f"Output directory:    {output_dir}")
    print(f"Mode:                {'MOVE' if MOVE_FILES else 'COPY'}")
    print(f"Dry run:             {DRY_RUN}")
    print(f"Recursive:           {RECURSIVE}")
    print(f"Total files seen:    {total}")
    print(f"DICOM organised:     {dicom_ok}")
    print(f"Skipped (not DICOM): {skipped_not_dicom}")
    print(f"Failed reads:        {failed_reads}")
    print("\nFolders created / file counts:")
    for k in sorted(per_folder_counts.keys()):
        print(f"  - {k}: {per_folder_counts[k]}")
    print("------------------------------------------------\n")


if __name__ == "__main__":
    organise_dicoms(INPUT_DIR, OUTPUT_DIR)
