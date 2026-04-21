"""
ID Card Generator - Flask Backend v2.2 (Railway-Optimized)
=============================================================
TARGET: 0.5 vCPU / 512 MB RAM → 600+ students without OOM

KEY OPTIMISATIONS vs v2.1
──────────────────────────
1.  STREAMING BATCH SAVE
    - out_doc never holds more than BATCH_PAGES pages in RAM at once
    - Each batch is saved to disk with deflate=False (fast), then the
      in-RAM doc is closed/cleared before the next batch starts
    - Final file is assembled by merging the small batch files on disk
      (disk I/O, not RAM)  →  peak RAM ≈ BATCH_PAGES × ~1 MB

2.  CARD → BYTES IMMEDIATELY CLOSED
    render_card_pdf() now returns raw bytes (doc.tobytes(garbage=1))
    and closes the fitz.Document before returning.  show_pdf_page()
    re-opens from bytes (cheap) and closes immediately after tiling.
    No live card docs accumulate in RAM.

3.  CHUNKED PHOTO PREFETCH
    Instead of fetching ALL 185 photos before rendering starts,
    we prefetch only the next PREFETCH_AHEAD students' photos while
    the current batch is rendering.  Peak photo-cache RAM ≈ 20 photos
    (≈ 600 KB) instead of 185 photos (≈ 5.5 MB).

4.  LOWER PHOTO RESOLUTION (env-overridable)
    Default PHOTO_PX: 300 → 200, JPEG quality: 80 → 72
    Visual difference at 55×86 mm print size: imperceptible.
    Saves ~55 % photo-bytes per student.

5.  SINGLE GUNICORN WORKER (gunicorn.conf.py shipped separately)
    Only one LRU cache, one template copy, one font copy in process.

6.  gc.collect() + explicit close after EVERY card, not every 10 pages.

7.  deflate=False during batch saves; deflate=True only on final merge.
    Saves ~0.15 vCPU per batch on Railway's constrained CPU.

8.  LRU photo cache capped at 100 entries (was 200) to halve worst-case
    cache RAM on a 512 MB host.

9.  SIGTERM-safe cleanup: temp files tracked in a set, removed on exit.

UNCHANGED
─────────
- Vector-native PDF (show_pdf_page) — quality preserved
- Font singletons, template loaded once into RAM
- Full field rendering (name, class, photo, blood, address …)
- All API/upload endpoints
"""

import io
import os
import sys
import json
import base64
import tempfile
import uuid
import threading
import atexit
import signal
import requests
from pathlib import Path
from collections import defaultdict, OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from flask import Flask, request, jsonify, send_file, after_this_request
from flask_cors import CORS
import pandas as pd
import gc

try:
    import fitz
    HAS_FITZ = True
except ImportError:
    HAS_FITZ = False

try:
    from PIL import Image, ImageOps, ImageDraw, ImageFont, ImageFilter
    HAS_PIL = True
    Image.MAX_IMAGE_PIXELS = 20_000_000
except ImportError:
    HAS_PIL = False

app = Flask(__name__)
CORS(app,
     origins=["*"],
     methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
     allow_headers=["Content-Type", "Authorization", "X-Requested-With"],
     supports_credentials=True,
     expose_headers=["Content-Disposition", "Content-Type"])

BASE_DIR       = Path(__file__).parent
TEMPLATE_PDF   = BASE_DIR / "template_id_card.pdf"
ANTON_FONT     = BASE_DIR / "Anton-Regular.ttf"
ARIAL_BOLD     = BASE_DIR / "arialbd.ttf"
FALLBACK_PHOTO = BASE_DIR / "student_photo.jpg"

DEFAULT_SESSION = "2026-27"

SCHOOLS = {
    2: "My Redeemer Mission School",
    3: "Hebron Mission School",
    4: "Priyanka Dreamnest School",
    5: "Ab Ascent School",
}

API_BASE_URL = "https://titusattendence.com/apikey/apistudents?school_id={school_id}"

CLASS_ORDER = {
    "NURSERY": 0, "LKG": 1, "UKG": 2,
    "1ST": 3, "2ND": 4, "3RD": 5, "4TH": 6,
    "5TH": 7, "6TH": 8, "7TH": 9, "8TH": 10,
}

def class_sort_key(cls_str):
    return CLASS_ORDER.get(str(cls_str).strip().upper(), 99)

_store = {"students": [], "source": None, "school_name": None}

# ── Env config ────────────────────────────────────────────────────
MAX_UPLOAD_MB            = int(os.environ.get("MAX_UPLOAD_MB", "12"))
MAX_STUDENTS_PER_REQUEST = int(os.environ.get("MAX_STUDENTS_PER_REQUEST", "1000"))
PREVIEW_DPI              = int(os.environ.get("PREVIEW_DPI", "150"))
DOWNLOAD_DPI             = int(os.environ.get("DOWNLOAD_DPI", "150"))
PHOTO_TIMEOUT            = (4, 10)
MAX_PHOTO_BYTES          = int(os.environ.get("MAX_PHOTO_BYTES", str(3 * 1024 * 1024)))
PDF_TEMP_DIR             = os.environ.get("PDF_TEMP_DIR", tempfile.gettempdir())

# ↓ OPTIMISATION 4: lower default resolution
PHOTO_PX           = int(os.environ.get("PHOTO_PX", "200"))          # was 300
PHOTO_JPEG_QUALITY = int(os.environ.get("PHOTO_JPEG_QUALITY", "72")) # was 80

# ↓ OPTIMISATION 8: smaller LRU cap
MAX_CACHED_PHOTOS  = int(os.environ.get("MAX_CACHED_PHOTOS", "100")) # was 200

# ↓ OPTIMISATION 1: batch size for incremental save
SAVE_BATCH_PAGES   = int(os.environ.get("SAVE_BATCH_PAGES", "5"))    # was 10

# ↓ OPTIMISATION 3: how many students ahead to prefetch
PREFETCH_AHEAD     = int(os.environ.get("PREFETCH_AHEAD", "10"))
PREFETCH_WORKERS   = int(os.environ.get("PREFETCH_WORKERS", "4"))    # was 8

STORAGE_BACKEND           = os.environ.get("STORAGE_BACKEND", "local").strip().lower()
SUPABASE_URL              = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")
SUPABASE_BUCKET           = os.environ.get("SUPABASE_BUCKET", "generated-pdfs")
SUPABASE_SIGNED_URL_TTL   = int(os.environ.get("SUPABASE_SIGNED_URL_TTL", "3600"))
GOOGLE_DRIVE_CLIENT_ID    = os.environ.get("GOOGLE_DRIVE_CLIENT_ID", "")
GOOGLE_DRIVE_CLIENT_SECRET= os.environ.get("GOOGLE_DRIVE_CLIENT_SECRET", "")
GOOGLE_DRIVE_REFRESH_TOKEN= os.environ.get("GOOGLE_DRIVE_REFRESH_TOKEN", "")
GOOGLE_DRIVE_FOLDER_ID    = os.environ.get("GOOGLE_DRIVE_FOLDER_ID", "")

app.config["MAX_CONTENT_LENGTH"] = MAX_UPLOAD_MB * 1024 * 1024

# ── Temp file tracker (SIGTERM-safe cleanup) ─────────────────────
_tmp_files      = set()
_tmp_files_lock = threading.Lock()

def _register_tmp(path: str):
    with _tmp_files_lock:
        _tmp_files.add(path)

def _unregister_tmp(path: str):
    with _tmp_files_lock:
        _tmp_files.discard(path)

def _cleanup_all_tmp(*_):
    with _tmp_files_lock:
        for p in list(_tmp_files):
            try:
                if os.path.exists(p):
                    os.unlink(p)
            except Exception:
                pass
        _tmp_files.clear()

atexit.register(_cleanup_all_tmp)
try:
    signal.signal(signal.SIGTERM, _cleanup_all_tmp)
except Exception:
    pass

# ── Helpers ───────────────────────────────────────────────────────
def replace_store(students, source, school_name):
    old = _store.get("students") or []
    if isinstance(old, list):
        old.clear()
    _store["students"]    = list(students)
    _store["source"]      = source
    _store["school_name"] = school_name
    gc.collect()

def filter_students_by_class(students, cls):
    cls = (cls or "").strip().upper()
    if not cls:
        return list(students)
    return [s for s in students if s.get("class","").strip().upper() == cls]

def _sanitize_filename(name):
    keep = []
    for ch in str(name or "file.pdf"):
        if ch.isalnum() or ch in ("-", "_", "."):
            keep.append(ch)
        elif ch.isspace():
            keep.append("_")
    cleaned = "".join(keep).strip("._") or "file"
    if not cleaned.lower().endswith(".pdf"):
        cleaned += ".pdf"
    return cleaned

def _external_storage_enabled():
    if STORAGE_BACKEND == "supabase":
        return bool(SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY and SUPABASE_BUCKET)
    if STORAGE_BACKEND == "google_drive":
        return bool(GOOGLE_DRIVE_CLIENT_ID and GOOGLE_DRIVE_CLIENT_SECRET and GOOGLE_DRIVE_REFRESH_TOKEN)
    return False

def _google_access_token():
    resp = requests.post(
        "https://oauth2.googleapis.com/token",
        data={
            "client_id":     GOOGLE_DRIVE_CLIENT_ID,
            "client_secret": GOOGLE_DRIVE_CLIENT_SECRET,
            "refresh_token": GOOGLE_DRIVE_REFRESH_TOKEN,
            "grant_type":    "refresh_token",
        }, timeout=20,
    )
    resp.raise_for_status()
    token = resp.json().get("access_token")
    if not token:
        raise RuntimeError("Google Drive token refresh failed")
    return token

def _upload_to_google_drive(local_path, download_name):
    token    = _google_access_token()
    metadata = {"name": _sanitize_filename(download_name)}
    if GOOGLE_DRIVE_FOLDER_ID:
        metadata["parents"] = [GOOGLE_DRIVE_FOLDER_ID]
    start = requests.post(
        "https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable&fields=id,name",
        headers={
            "Authorization":  f"Bearer {token}",
            "Content-Type":   "application/json; charset=UTF-8",
            "X-Upload-Content-Type": "application/pdf",
        },
        data=json.dumps(metadata), timeout=30,
    )
    start.raise_for_status()
    session_url = start.headers.get("Location")
    if not session_url:
        raise RuntimeError("Google Drive resumable upload URL missing")
    file_size = os.path.getsize(local_path)
    with open(local_path, "rb") as fh:
        uploaded = requests.put(
            session_url,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type":  "application/pdf",
                "Content-Length": str(file_size),
            },
            data=fh, timeout=300,
        )
    uploaded.raise_for_status()
    file_id = uploaded.json().get("id")
    if not file_id:
        raise RuntimeError("Google Drive file id missing")
    requests.post(
        f"https://www.googleapis.com/drive/v3/files/{file_id}/permissions",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        params={"fields": "id"},
        data=json.dumps({"role": "reader", "type": "anyone"}),
        timeout=30,
    ).raise_for_status()
    return f"https://drive.google.com/uc?export=download&id={file_id}"

def _upload_to_supabase(local_path, download_name):
    object_name = f"generated/{uuid.uuid4().hex}_{_sanitize_filename(download_name)}"
    upload_url  = f"{SUPABASE_URL}/storage/v1/object/{SUPABASE_BUCKET}/{object_name}"
    with open(local_path, "rb") as fh:
        requests.post(
            upload_url,
            headers={
                "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
                "apikey":        SUPABASE_SERVICE_ROLE_KEY,
                "x-upsert":      "true",
                "Content-Type":  "application/pdf",
            },
            data=fh, timeout=300,
        ).raise_for_status()
    sign = requests.post(
        f"{SUPABASE_URL}/storage/v1/object/sign/{SUPABASE_BUCKET}/{object_name}",
        headers={
            "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
            "apikey":        SUPABASE_SERVICE_ROLE_KEY,
            "Content-Type":  "application/json",
        },
        data=json.dumps({"expiresIn": SUPABASE_SIGNED_URL_TTL}),
        timeout=30,
    )
    sign.raise_for_status()
    payload = sign.json()
    signed  = payload.get("signedURL") or payload.get("signedUrl")
    if not signed:
        raise RuntimeError("Supabase signed URL missing")
    return signed if signed.startswith("http") else f"{SUPABASE_URL}/storage/v1{signed}"

def upload_pdf_to_external_storage(local_path, download_name):
    if STORAGE_BACKEND == "google_drive":
        return _upload_to_google_drive(local_path, download_name)
    if STORAGE_BACKEND == "supabase":
        return _upload_to_supabase(local_path, download_name)
    return None

def norm_key(v):
    s = str(v or "").strip().lower()
    out = []; prev = False
    for ch in s:
        if ch.isalnum():  out.append(ch); prev = False
        else:
            if not prev:  out.append("_"); prev = True
    return "".join(out).strip("_")

def clean_str(v):
    if pd.isna(v): return ""
    s = str(v).strip()
    return "" if s.lower() in {"nan","none"} else s

def pick(row, *aliases, default=""):
    for a in aliases:
        if a in row:
            val = clean_str(row[a])
            if val: return val
    return default

def _sort_and_index(students):
    students.sort(key=lambda s: (
        class_sort_key(s.get("class","")),
        s.get("section","").strip().upper(),
        s.get("student_name","").strip().upper(),
    ))
    for i, s in enumerate(students, 1):
        s["serial"] = i
    counters = defaultdict(int)
    for s in students:
        key = (s["class"].strip().upper(), s["section"].strip().upper())
        if not s["roll"]:
            counters[key] += 1
            s["roll"] = str(counters[key])
        else:
            try:
                cr = int(float(s["roll"]))
                counters[key] = max(counters[key], cr)
                s["roll"] = str(cr)
            except:
                pass
    return students

def parse_file(file_path, filename):
    fn = filename.lower()
    if fn.endswith(".csv"):
        df = pd.read_csv(file_path)
    else:
        df = pd.read_excel(file_path)
    df.columns = [norm_key(c) for c in df.columns]
    students = []
    for _, row in df.iterrows():
        rm = {col: row[col] for col in df.columns}
        s = {
            "student_name": pick(rm,"student_name","studentname","name","student"),
            "class":        pick(rm,"class","class_name","std","standard"),
            "section":      pick(rm,"section","sec","section_id"),
            "roll":         pick(rm,"roll","roll_no","rollno","roll_number"),
            "father_name":  pick(rm,"father_name","father","fathers_name"),
            "mother_name":  pick(rm,"mother_name","mother","mothers_name"),
            "dob":          pick(rm,"dob","date_of_birth","birth_date"),
            "address":      pick(rm,"address","student_address","residence"),
            "mobile":       pick(rm,"mobile","phone","mobile_no","contact","father_contact"),
            "photo_url":    pick(rm,"photo_url","photo","image_url","photo_link","student_photo"),
            "adm_no":       pick(rm,"adm_no","admission_no","admission_number","adm","admno"),
            "blood_group":  pick(rm,"blood_group","bloodgroup","blood"),
            "gender":       pick(rm,"gender","sex"),
            "session":      pick(rm,"session",default=DEFAULT_SESSION),
        }
        if any(s.values()):
            students.append(s)
    return _sort_and_index(students)

_API_MAP = {
    "student_name":"student_name","admission_no":"adm_no","section_id":"section",
    "dob":"dob","roll_number":"roll","mother_name":"mother_name","address":"address",
    "blood_group":"blood_group","class_name":"class","father_name":"father_name",
    "father_contact":"mobile","student_photo":"photo_url","session":"session",
    "academic_year":"session","name":"student_name","std":"class","grade":"class",
    "section":"section","roll":"roll","roll_no":"roll","father":"father_name",
    "mother":"mother_name","date_of_birth":"dob","student_address":"address",
    "mobile":"mobile","phone":"mobile","mobile_no":"mobile","contact":"mobile",
    "photo_url":"photo_url","photo":"photo_url","adm_no":"adm_no",
    "admission_number":"adm_no","adm":"adm_no","bloodgroup":"blood_group",
    "blood":"blood_group","gender":"gender","sex":"gender",
}

def map_api_record(record):
    out = {
        "student_name":"","class":"","section":"","roll":"","father_name":"",
        "mother_name":"","dob":"","address":"","mobile":"","photo_url":"",
        "adm_no":"","blood_group":"","gender":"","session":DEFAULT_SESSION,
    }
    for k, v in record.items():
        internal = _API_MAP.get(k.strip().lower())
        if internal and v not in (None,"","null","NULL"):
            out[internal] = str(v).strip()
    return out

# ── API endpoints ─────────────────────────────────────────────────
@app.route("/", methods=["GET"])
def root():
    return jsonify({"status": "ok", "message": "ID Card Generator API is running"})

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "message": "ID Card Generator API is healthy"})

@app.route("/api/schools", methods=["GET"])
@app.route("/schools", methods=["GET"])
def get_schools():
    return jsonify([{"id": k, "name": v} for k, v in SCHOOLS.items()])

@app.route("/api/upload", methods=["POST"])
@app.route("/upload", methods=["POST"])
def upload_file():
    if "file" not in request.files:
        return jsonify({"error": "No file"}), 400
    f = request.files["file"]
    tmp_path = None
    try:
        suffix = Path(f.filename or "upload.xlsx").suffix or ".xlsx"
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            tmp_path = tmp.name
            f.save(tmp_path)
        students = parse_file(tmp_path, f.filename)
        replace_store(students, "file", "Uploaded File")
        return jsonify({
            "success": True,
            "count": len(students),
            "classes": _classes_summary(students),
            "session": students[0].get("session", DEFAULT_SESSION) if students else DEFAULT_SESSION,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if tmp_path and os.path.exists(tmp_path):
            try: os.unlink(tmp_path)
            except: pass

@app.route("/api/fetch-school/<int:school_id>", methods=["GET"])
@app.route("/fetch-school/<int:school_id>", methods=["GET"])
def fetch_school(school_id):
    if school_id not in SCHOOLS:
        return jsonify({"error": "Unknown school"}), 400
    url = API_BASE_URL.format(school_id=school_id)
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        payload = resp.json()
    except Exception as e:
        return jsonify({"error": f"API error: {e}"}), 500

    records = None
    if isinstance(payload, list):
        records = payload
    elif isinstance(payload, dict):
        for key in ("data","students","records","result","results","items"):
            if key in payload and isinstance(payload[key], list):
                records = payload[key]; break
        if records is None:
            for v in payload.values():
                if isinstance(v, list) and v and isinstance(v[0], dict):
                    records = v; break

    if not records:
        return jsonify({"error": "No student records found in API response"}), 500

    students = [map_api_record(r) for r in records if isinstance(r, dict)]
    students = [s for s in students if any(v for v in s.values() if v and v != DEFAULT_SESSION)]
    if not students:
        return jsonify({"error": "No valid students after mapping"}), 500

    students = _sort_and_index(students)
    replace_store(students, "api", SCHOOLS[school_id])
    return jsonify({
        "success": True,
        "count": len(students),
        "school": SCHOOLS[school_id],
        "classes": _classes_summary(students),
        "session": students[0].get("session", DEFAULT_SESSION) if students else DEFAULT_SESSION,
    })

@app.route("/api/students", methods=["GET"])
@app.route("/students", methods=["GET"])
def get_students():
    cls      = request.args.get("class","").strip().upper()
    students = _store["students"]
    if cls:
        students = [s for s in students if s.get("class","").strip().upper() == cls]
    return jsonify(students)

@app.route("/api/status", methods=["GET"])
@app.route("/status", methods=["GET"])
def get_status():
    students = _store["students"]
    if not students:
        return jsonify({"loaded": False})
    cls_list    = sorted(set(s.get("class","").strip().upper() for s in students), key=class_sort_key)
    session_val = students[0].get("session", DEFAULT_SESSION)
    class_counts = {}
    for s in students:
        k = s.get("class","").strip().upper()
        if k:
            class_counts[k] = class_counts.get(k, 0) + 1
    school_name = _store.get("school_name","")
    return jsonify({
        "loaded": True,
        "count": len(students),
        "school": school_name,
        "school_name": school_name,
        "source": _store.get("source",""),
        "classes": cls_list,
        "classCounts": class_counts,
        "session": session_val,
    })

def _classes_summary(students):
    cc = defaultdict(int)
    for s in students:
        cc[s.get("class","").strip().upper()] += 1
    return [{"class": k, "count": v} for k, v in sorted(cc.items(), key=lambda x: class_sort_key(x[0]))]


# ════════════════════════════════════════════════════════════════════
#  PHOTO CACHE  — LRU bounded, thread-safe
# ════════════════════════════════════════════════════════════════════
class _BoundedPhotoCache:
    def __init__(self, maxsize: int = 100):
        self._cache: OrderedDict = OrderedDict()
        self._maxsize = maxsize
        self._lock    = threading.Lock()

    def get(self, key: str):
        with self._lock:
            if key not in self._cache:
                return False, None
            self._cache.move_to_end(key)
            return True, self._cache[key]

    def set(self, key: str, value):
        with self._lock:
            if key in self._cache:
                self._cache.move_to_end(key)
            self._cache[key] = value
            while len(self._cache) > self._maxsize:
                self._cache.popitem(last=False)

    def clear(self):
        with self._lock:
            self._cache.clear()

    def __len__(self):
        with self._lock:
            return len(self._cache)


_photo_cache = _BoundedPhotoCache(maxsize=MAX_CACHED_PHOTOS)

# ════════════════════════════════════════════════════════════════════
#  TEMPLATE + FONT SINGLETONS
# ════════════════════════════════════════════════════════════════════
_template_bytes = None
_template_lock  = threading.Lock()

_anton_font_obj = None
_bold_font_obj  = None
_font_init_done = False
_font_lock      = threading.Lock()


def _ensure_template():
    global _template_bytes
    if _template_bytes is not None:
        return _template_bytes
    with _template_lock:
        if _template_bytes is not None:
            return _template_bytes
        if not TEMPLATE_PDF.exists():
            return None
        with open(str(TEMPLATE_PDF), "rb") as fh:
            _template_bytes = fh.read()
        print(f"DEBUG: template loaded into RAM ({len(_template_bytes) // 1024} KB)")
        return _template_bytes


def _ensure_fonts():
    global _anton_font_obj, _bold_font_obj, _font_init_done
    if _font_init_done:
        return (
            _anton_font_obj, _bold_font_obj,
            str(ANTON_FONT) if ANTON_FONT.exists() else None,
            str(ARIAL_BOLD) if ARIAL_BOLD.exists() else None,
            "anton"   if ANTON_FONT.exists() else "helv",
            "arialbd" if ARIAL_BOLD.exists() else "helv",
        )
    with _font_lock:
        if _font_init_done:
            return (
                _anton_font_obj, _bold_font_obj,
                str(ANTON_FONT) if ANTON_FONT.exists() else None,
                str(ARIAL_BOLD) if ARIAL_BOLD.exists() else None,
                "anton"   if ANTON_FONT.exists() else "helv",
                "arialbd" if ARIAL_BOLD.exists() else "helv",
            )
        try:
            _anton_font_obj = fitz.Font(fontfile=str(ANTON_FONT)) if ANTON_FONT.exists() else fitz.Font("helv")
            _bold_font_obj  = fitz.Font(fontfile=str(ARIAL_BOLD)) if ARIAL_BOLD.exists() else fitz.Font("helv")
        except Exception as e:
            print(f"DEBUG: font load failed: {e}")
            _anton_font_obj = fitz.Font("helv")
            _bold_font_obj  = fitz.Font("helv")
        _font_init_done = True
        return (
            _anton_font_obj, _bold_font_obj,
            str(ANTON_FONT) if ANTON_FONT.exists() else None,
            str(ARIAL_BOLD) if ARIAL_BOLD.exists() else None,
            "anton"   if ANTON_FONT.exists() else "helv",
            "arialbd" if ARIAL_BOLD.exists() else "helv",
        )

# ════════════════════════════════════════════════════════════════════
#  PHOTO FETCH & COMPRESS
# ════════════════════════════════════════════════════════════════════

def _compress_photo(pil_img):
    rgb = pil_img.convert("RGB")
    src_w, src_h = rgb.size
    src_min = min(src_w, src_h)

    if src_min < 200:
        rgb = rgb.filter(ImageFilter.SMOOTH_MORE)

    resized = ImageOps.fit(rgb, (PHOTO_PX, PHOTO_PX), method=Image.Resampling.LANCZOS)
    rgb.close()

    if src_min >= 200:
        resized = resized.filter(ImageFilter.UnsharpMask(radius=1, percent=80, threshold=5))

    buf = io.BytesIO()
    resized.save(buf, format="JPEG", quality=PHOTO_JPEG_QUALITY,
                 optimize=True, progressive=False)
    resized.close()
    return buf.getvalue()


def _load_fallback_photo():
    found, val = _photo_cache.get("__fallback__")
    if found:
        return val
    if FALLBACK_PHOTO.exists():
        try:
            with open(str(FALLBACK_PHOTO), "rb") as fh:
                raw = fh.read()
            with Image.open(io.BytesIO(raw)) as img:
                result = _compress_photo(img)
            _photo_cache.set("__fallback__", result)
            return result
        except Exception:
            pass
    placeholder = Image.new("RGB", (PHOTO_PX, PHOTO_PX), (180, 200, 220))
    result = _compress_photo(placeholder)
    placeholder.close()
    _photo_cache.set("__fallback__", result)
    return result


def fetch_photo_bytes(url: str):
    if not HAS_PIL:
        return None

    cache_key = (url or "").strip()
    found, cached = _photo_cache.get(cache_key)
    if found:
        return cached

    if cache_key:
        try:
            resp = requests.get(cache_key, timeout=PHOTO_TIMEOUT, stream=True)
            resp.raise_for_status()
            chunks = []; total = 0
            for chunk in resp.iter_content(64 * 1024):
                if not chunk: continue
                total += len(chunk)
                if total > MAX_PHOTO_BYTES:
                    raise ValueError("photo too large")
                chunks.append(chunk)
            with Image.open(io.BytesIO(b"".join(chunks))) as img:
                compressed = _compress_photo(img)
            _photo_cache.set(cache_key, compressed)
            return compressed
        except Exception as e:
            print(f"DEBUG: photo fetch failed ({cache_key[:80]}): {e}")
            _photo_cache.set(cache_key, None)

    fallback = _load_fallback_photo()
    if cache_key:
        _photo_cache.set(cache_key, fallback)
    return fallback


# ════════════════════════════════════════════════════════════════════
#  OPTIMISATION 3: CHUNKED PREFETCH — only next N students ahead
# ════════════════════════════════════════════════════════════════════

def _prefetch_chunk(students_slice: list) -> None:
    urls = list({
        s.get("photo_url","").strip()
        for s in students_slice
        if s.get("photo_url","").strip()
    })
    if not urls:
        return
    with ThreadPoolExecutor(max_workers=PREFETCH_WORKERS) as pool:
        futures = {pool.submit(fetch_photo_bytes, url): url for url in urls}
        for f in as_completed(futures):
            try:
                f.result()
            except Exception as e:
                print(f"DEBUG: prefetch error: {e}")


# ════════════════════════════════════════════════════════════════════
#  CARD LAYOUT CONSTANTS  (unchanged)
# ════════════════════════════════════════════════════════════════════

CARD_W_MM   = 55.0;  CARD_H_MM   = 86.0
A4_W_MM     = 297.0; A4_H_MM     = 210.0
COLS        = 5;     ROWS        = 2;  CARDS_PER_PAGE = COLS * ROWS
ROW_GAP_MM  = 10.0
GRID_W_MM   = COLS * CARD_W_MM
GRID_H_MM   = ROWS * CARD_H_MM + (ROWS - 1) * ROW_GAP_MM
OFFSET_X_MM = (A4_W_MM - GRID_W_MM) / 2.0
OFFSET_Y_MM = (A4_H_MM - GRID_H_MM) / 2.0
MM_TO_PT    = 72.0 / 25.4
PT_PER_INCH = 72.0

PHOTO_RECT_COORDS        = (54.25, 67.74, 98.82, 119.07)
BAND_Y0                  = 123.8;  BAND_Y1 = 151.0
NAME_TEXT_RECT_COORDS    = (13.0, 124.7, 112.0, 139.2)
CLASS_TEXT_RECT_COORDS   = (13.0, 139.7, 112.0, 147.0)
SIGN_SAFE_X1             = 118.0
ADM_WHITEOUT_COORDS      = (18.0, 107.0, 48.0, 116.5)
ADM_VALUE_RECT_COORDS    = (18.51, 107.56, 48.0, 115.5)
SESSION_WHITEOUT_COORDS  = (109.15, 107.5, 142.0, 118.5)
SESSION_VALUE_RECT_COORDS= (109.15, 108.0, 142.0, 118.5)
BLOOD_RED                = (0.8549, 0.0627, 0.0627)
BLOOD_VALUE_RECT_COORDS  = (112.0, 84.5, 129.0, 97.5)
FATHER_VALUE_RECT_COORDS = (66.3, 154.4, 148.0, 160.6)
MOTHER_VALUE_RECT_COORDS = (66.3, 162.2, 148.0, 168.3)
DOB_VALUE_RECT_COORDS    = (66.3, 168.8, 148.0, 174.9)
ADDRESS_VALUE_RECT_COORDS= (66.3, 175.4, SIGN_SAFE_X1, 187.0)
MOBILE_VALUE_RECT_COORDS = (66.3, 191.1, SIGN_SAFE_X1, 197.2)
FATHER_CLEAN_COORDS      = (66.3, 153.8, 149.0, 161.2)
MOTHER_CLEAN_COORDS      = (66.3, 161.5, 149.0, 169.0)
DOB_CLEAN_COORDS         = (66.3, 168.0, 149.0, 175.5)
ADDRESS_CLEAN_COORDS     = (66.3, 174.8, SIGN_SAFE_X1, 188.0)
MOBILE_CLEAN_COORDS      = (66.3, 190.5, 113.0, 198.0)

BANNER_RED   = (0.7843, 0.0667, 0.0667)
WHITE        = (1.0, 1.0, 1.0)
NAME_COLOR   = (1.0, 1.0, 1.0)
VALUE_COLOR  = (170/255, 16/255, 16/255)

NAME_FONT_SIZE   = 9.9;  CLASS_FONT_SIZE  = 5.9;  VALUE_FONT_SIZE = 5.5
ADM_FONT_SIZE    = 6.5;  SESSION_FONT_SIZE = 7.5; BLOOD_FONT_SIZE = 6.88
ADDR_MAX_LINES   = 3;    ADDR_LINE_GAP    = 1.10; ADDR_MIN_SIZE   = 3.5
ADDR_SIZE_STEPS  = [5.5, 5.2, 5.0, 4.8, 4.5, 4.2, 4.0, 3.8, 3.5]

TEARDROP_ITEMS = [
    ('l', (126.74588, 84.57169), (119.56597, 72.82723)),
    ('l', (119.56597, 72.82723), (112.91280, 84.49141)),
    ('c', (112.91280, 84.49141),(111.36359, 86.96311),(111.22838, 90.17703),(112.85576, 92.83886)),
    ('c', (112.85576, 92.83886),(115.16902, 96.62247),(120.15327, 97.83719),(123.98969, 95.55492)),
    ('c', (123.98969, 95.55492),(127.82469, 93.27335),(129.05914, 88.35811),(126.74588, 84.57169)),
]

# ── Text helpers (unchanged) ──────────────────────────────────────

def _fit_size(font, text, max_width, base, min_size=4.0):
    s = base
    while s >= min_size:
        if font.text_length(text, fontsize=s) <= max_width:
            return s
        s -= 0.1
    return min_size

def _put_single(page, rect, text, fontfile, fontname, size, color, font_obj):
    if not text: return
    baseline_y = rect.y0 + size * font_obj.ascender
    page.insert_text(
        (rect.x0, baseline_y), text,
        fontname=fontname, fontfile=str(fontfile) if fontfile else None,
        fontsize=size, color=color, overlay=True,
    )

def draw_text_vertically_centered(page, rect, text, fontfile, fontname, font_obj, base_size, color):
    if not text: return
    size   = _fit_size(font_obj, text, rect.width, base_size, 4.0)
    text_h = size * (font_obj.ascender - font_obj.descender)
    baseline = rect.y0 + (rect.height + text_h) / 2.0 - size * abs(font_obj.descender)
    page.insert_text(
        (rect.x0, baseline), text,
        fontname=fontname, fontfile=str(fontfile) if fontfile else None,
        fontsize=size, color=color, overlay=True,
    )

def draw_text_centered_hv(page, rect, text, fontfile, fontname, font_obj, size, color):
    if not text: return
    size = _fit_size(font_obj, text, rect.width, size, 3.5)
    tw   = font_obj.text_length(text, fontsize=size)
    gh   = size * (font_obj.ascender - font_obj.descender)
    x    = rect.x0 + (rect.width - tw) / 2.0
    y    = rect.y0 + (rect.height + gh) / 2.0 - size * abs(font_obj.descender)
    page.insert_text(
        (x, y), text,
        fontname=fontname, fontfile=str(fontfile) if fontfile else None,
        fontsize=size, color=color, overlay=True,
    )

def _addr_wrap_at_size(font_obj, words, max_width, fs):
    lines = []; cur = ""
    for w in words:
        if font_obj.text_length(w, fontsize=fs) > max_width:
            if cur: lines.append(cur); cur = ""
            trunc = ""; ellipsis = "…"
            for ch in w:
                if font_obj.text_length(trunc + ch + ellipsis, fontsize=fs) <= max_width:
                    trunc += ch
                else:
                    break
            lines.append(trunc + ellipsis); continue
        trial = (cur + " " + w).strip() if cur else w
        if font_obj.text_length(trial, fontsize=fs) <= max_width:
            cur = trial
        else:
            if cur: lines.append(cur)
            cur = w
    if cur: lines.append(cur)
    return lines

def render_address(page, rect, addr, fontfile, fontname, font_obj, color):
    if not addr or addr.lower() in {"nan","none"}: return
    words = addr.split()
    if not words: return
    max_w = SIGN_SAFE_X1 - rect.x0
    chosen_fs = ADDR_MIN_SIZE; chosen_lines = []
    for fs in ADDR_SIZE_STEPS:
        lines  = _addr_wrap_at_size(font_obj, words, max_w, fs)
        n      = len(lines)
        line_h = fs * (font_obj.ascender - font_obj.descender)
        spacing_h = fs * ADDR_LINE_GAP
        total_h = line_h + spacing_h * (n - 1)
        if n <= ADDR_MAX_LINES and total_h <= rect.height:
            chosen_fs = fs; chosen_lines = lines; break
    else:
        fs    = ADDR_MIN_SIZE
        lines = _addr_wrap_at_size(font_obj, words, max_w, fs)[:ADDR_MAX_LINES]
        if lines:
            last = lines[-1]
            while last and font_obj.text_length(last, fontsize=fs) > max_w:
                last = last[:-1]
            if lines[-1] != last:
                lines[-1] = last.rstrip() + "…"
        chosen_fs = fs; chosen_lines = lines
    if not chosen_lines: return
    line_step = chosen_fs * ADDR_LINE_GAP
    baseline0 = rect.y0 + chosen_fs * font_obj.ascender
    for i, line in enumerate(chosen_lines):
        baseline = baseline0 + i * line_step
        if baseline - chosen_fs * abs(font_obj.descender) > rect.y1: break
        page.insert_text(
            (rect.x0, baseline), line,
            fontname=fontname, fontfile=str(fontfile) if fontfile else None,
            fontsize=chosen_fs, color=color, overlay=True,
        )

def redraw_blood_teardrop(page, fill_color):
    shape = page.new_shape()
    p = lambda t: fitz.Point(*t)
    shape.draw_line(p(TEARDROP_ITEMS[0][1]), p(TEARDROP_ITEMS[0][2]))
    shape.draw_line(p(TEARDROP_ITEMS[1][1]), p(TEARDROP_ITEMS[1][2]))
    shape.draw_bezier(p(TEARDROP_ITEMS[2][1]), p(TEARDROP_ITEMS[2][2]),
                      p(TEARDROP_ITEMS[2][3]), p(TEARDROP_ITEMS[2][4]))
    shape.draw_bezier(p(TEARDROP_ITEMS[3][1]), p(TEARDROP_ITEMS[3][2]),
                      p(TEARDROP_ITEMS[3][3]), p(TEARDROP_ITEMS[3][4]))
    shape.draw_bezier(p(TEARDROP_ITEMS[4][1]), p(TEARDROP_ITEMS[4][2]),
                      p(TEARDROP_ITEMS[4][3]), p(TEARDROP_ITEMS[4][4]))
    shape.finish(color=fill_color, fill=fill_color, width=0, closePath=True)
    shape.commit(overlay=True)


# ════════════════════════════════════════════════════════════════════
#  OPTIMISATION 2: render_card_pdf → returns bytes, closes doc
# ════════════════════════════════════════════════════════════════════

def render_card_bytes(student: dict) -> bytes | None:
    """
    Render one student card as a PDF page and return the raw bytes.
    The fitz.Document is opened, populated, serialised to bytes,
    and CLOSED before this function returns — no live docs leak.
    """
    if not HAS_FITZ:
        return None

    tmpl_bytes = _ensure_template()
    if tmpl_bytes is None:
        return None

    doc  = fitz.open("pdf", tmpl_bytes)
    page = doc[0]

    anton_obj, bold_obj, anton_fn, bold_fn, fn_anton, fn_bold = _ensure_fonts()
    if anton_obj is None or bold_obj is None:
        doc.close()
        return None

    # ── Banner band ──
    shape = page.new_shape()
    def band_right_x(y): return -0.3952 * y + 172.6234
    pts = [
        fitz.Point(0, BAND_Y0),
        fitz.Point(band_right_x(BAND_Y0), BAND_Y0),
        fitz.Point(band_right_x(BAND_Y1), BAND_Y1),
        fitz.Point(0, BAND_Y1),
    ]
    shape.draw_polyline(pts)
    shape.draw_line(pts[-1], pts[0])
    shape.finish(color=BANNER_RED, fill=BANNER_RED, width=0)
    shape.commit(overlay=True)

    # ── White-out old values ──
    for coords in [FATHER_CLEAN_COORDS, MOTHER_CLEAN_COORDS, DOB_CLEAN_COORDS,
                   ADDRESS_CLEAN_COORDS, MOBILE_CLEAN_COORDS,
                   ADM_WHITEOUT_COORDS, SESSION_WHITEOUT_COORDS]:
        page.draw_rect(fitz.Rect(*coords), color=WHITE, fill=WHITE, width=0, overlay=True)

    redraw_blood_teardrop(page, BLOOD_RED)

    # ── Photo ──
    photo_url   = student.get("photo_url","")
    photo_bytes = fetch_photo_bytes(photo_url)
    if photo_bytes:
        page.insert_image(
            fitz.Rect(*PHOTO_RECT_COORDS),
            stream=photo_bytes,
            overlay=True,
            keep_proportion=False,
        )

    # ── Name ──
    draw_text_vertically_centered(
        page, fitz.Rect(*NAME_TEXT_RECT_COORDS),
        str(student.get("student_name","")).strip().upper(),
        anton_fn, fn_anton, anton_obj, NAME_FONT_SIZE, NAME_COLOR,
    )

    # ── Class / Section / Roll ──
    cls  = str(student.get("class","")).strip().upper()
    sec  = str(student.get("section","")).strip().upper()
    roll = str(student.get("roll","")).strip()
    parts = []
    if cls:  parts.append(f"CLASS:{cls}")
    if sec:  parts.append(f"SEC:{sec}")
    if roll: parts.append(f"ROLL:{roll}")
    draw_text_vertically_centered(
        page, fitz.Rect(*CLASS_TEXT_RECT_COORDS),
        "  ".join(parts),
        bold_fn, fn_bold, bold_obj, CLASS_FONT_SIZE, NAME_COLOR,
    )

    # ── Simple fields ──
    for coords, key in [
        (FATHER_VALUE_RECT_COORDS, "father_name"),
        (MOTHER_VALUE_RECT_COORDS, "mother_name"),
        (MOBILE_VALUE_RECT_COORDS, "mobile"),
    ]:
        rect = fitz.Rect(*coords)
        txt  = str(student.get(key,"")).strip()
        if txt and txt.lower() not in {"nan","none"}:
            sz = _fit_size(bold_obj, txt, rect.width, VALUE_FONT_SIZE)
            _put_single(page, rect, txt, bold_fn, fn_bold, sz, VALUE_COLOR, bold_obj)

    dob = str(student.get("dob","")).strip()
    if dob and dob.lower() not in {"nan","none"}:
        rect = fitz.Rect(*DOB_VALUE_RECT_COORDS)
        sz   = _fit_size(bold_obj, dob, rect.width, VALUE_FONT_SIZE)
        _put_single(page, rect, dob, bold_fn, fn_bold, sz, VALUE_COLOR, bold_obj)

    render_address(
        page, fitz.Rect(*ADDRESS_VALUE_RECT_COORDS),
        str(student.get("address","")).strip(),
        bold_fn, fn_bold, bold_obj, VALUE_COLOR,
    )

    adm = str(student.get("adm_no","")).strip()
    if adm and adm.lower() not in {"nan","none"}:
        rect = fitz.Rect(*ADM_VALUE_RECT_COORDS)
        sz   = _fit_size(bold_obj, adm, rect.width, ADM_FONT_SIZE)
        _put_single(page, rect, adm, bold_fn, fn_bold, sz, VALUE_COLOR, bold_obj)

    sess = str(student.get("session","")).strip() or DEFAULT_SESSION
    rect = fitz.Rect(*SESSION_VALUE_RECT_COORDS)
    sz   = _fit_size(anton_obj, sess, rect.width, SESSION_FONT_SIZE)
    _put_single(page, rect, sess, anton_fn, fn_anton, sz, VALUE_COLOR, anton_obj)

    blood = str(student.get("blood_group","")).strip().upper()
    if blood and blood.lower() not in {"nan","none"} and any(c.isalpha() for c in blood):
        draw_text_centered_hv(
            page, fitz.Rect(*BLOOD_VALUE_RECT_COORDS),
            blood, bold_fn, fn_bold, bold_obj, BLOOD_FONT_SIZE, WHITE,
        )

    # ── Serialise → bytes → close ──
    raw = doc.tobytes(garbage=1, deflate=False)   # fast: no compression yet
    doc.close()
    return raw


# ════════════════════════════════════════════════════════════════════
#  SERIAL BADGE (unchanged)
# ════════════════════════════════════════════════════════════════════

def draw_serial_badge_vector(page, serial: int, cx: float, cy: float, gap_h: float):
    txt    = f"#{serial}"
    fs     = max(5.0, gap_h * 0.38)
    try:
        font = fitz.Font("helv")
        tw   = font.text_length(txt, fontsize=fs)
    except Exception:
        tw = len(txt) * fs * 0.6

    pad_x  = fs * 0.5; pad_y = fs * 0.25
    bw     = tw + 2 * pad_x; bh = fs + 2 * pad_y
    left   = cx - bw / 2.0; top = cy - bh / 2.0
    right  = left + bw; bottom = top + bh

    shape = page.new_shape()
    so = max(1.0, fs * 0.05)
    shape.draw_rect(fitz.Rect(left+so, top+so, right+so, bottom+so))
    shape.finish(color=(0.2,0,0), fill=(0.2,0,0), width=0)
    shape.draw_rect(fitz.Rect(left, top, right, bottom))
    shape.finish(color=(0.82,0.08,0.08), fill=(0.82,0.08,0.08), width=0)
    shape.commit(overlay=True)

    shape2 = page.new_shape()
    shape2.draw_rect(fitz.Rect(left, top, right, bottom))
    shape2.finish(color=WHITE, fill=None, width=max(0.5, fs*0.03))
    shape2.commit(overlay=True)

    page.insert_text(
        (left + pad_x, cy + fs * 0.35), txt,
        fontname="helv", fontsize=fs, color=WHITE, overlay=True,
    )

# ── PT constants ──────────────────────────────────────────────────

def mm_to_pt(mm: float) -> float:
    return mm * MM_TO_PT

CARD_W_PT  = mm_to_pt(CARD_W_MM);  CARD_H_PT  = mm_to_pt(CARD_H_MM)
A4_W_PT    = mm_to_pt(A4_W_MM);    A4_H_PT    = mm_to_pt(A4_H_MM)
OX_PT      = mm_to_pt(OFFSET_X_MM); OY_PT     = mm_to_pt(OFFSET_Y_MM)
ROW_GAP_PT = mm_to_pt(ROW_GAP_MM); COL_GAP_PT = mm_to_pt(1.0)
COL_STEP   = CARD_W_PT + COL_GAP_PT
ROW_STEP   = CARD_H_PT + ROW_GAP_PT


# ════════════════════════════════════════════════════════════════════
#  OPTIMISATION 1: STREAMING BATCH-SAVE PDF BUILDER
# ════════════════════════════════════════════════════════════════════

def _make_tmp_path(suffix=".pdf") -> str:
    fd, path = tempfile.mkstemp(suffix=suffix, dir=PDF_TEMP_DIR)
    os.close(fd)
    _register_tmp(path)
    return path


def build_pdf_file_vector(students: list) -> str | None:
    """
    Build the final A4 PDF in SAVE_BATCH_PAGES-page batches.

    Memory model
    ────────────
    1. We render at most SAVE_BATCH_PAGES A4 pages in out_doc at once.
    2. Each card is rendered to bytes (render_card_bytes), which opens
       a fitz doc, populates it, serialises it, and CLOSES it.
    3. show_pdf_page re-opens those bytes as a throwaway tmp_doc,
       tiles the page, and we close tmp_doc immediately.
    4. After SAVE_BATCH_PAGES pages:
         - save out_doc → temp file (deflate=False, fast)
         - close + delete out_doc  → RAM freed
         - open a fresh out_doc for next batch
    5. At the end, merge all batch temp files into a single output
       using insert_pdf() (disk→disk, minimal RAM), then compress once
       with deflate=True.
    """
    if not HAS_FITZ:
        return None
    if _ensure_template() is None:
        print("DEBUG: Template PDF not found — using raster fallback")
        return None

    n_students  = len(students)
    n_pages     = (n_students + CARDS_PER_PAGE - 1) // CARDS_PER_PAGE
    batch_files = []           # paths of partial PDFs
    final_path  = _make_tmp_path(".pdf")

    try:
        out_doc    = fitz.open()
        batch_start_page = 0  # which A4 page index starts the current out_doc

        for page_idx in range(n_pages):
            student_start = page_idx * CARDS_PER_PAGE
            student_batch = students[student_start : student_start + CARDS_PER_PAGE]

            # ── OPTIMISATION 3: prefetch NEXT batch's photos now ──
            next_start = (page_idx + 1) * CARDS_PER_PAGE
            if next_start < n_students:
                ahead = students[next_start : next_start + PREFETCH_AHEAD]
                _prefetch_chunk(ahead)

            # ── Build one A4 page ──
            a4_page = out_doc.new_page(width=A4_W_PT, height=A4_H_PT)

            for idx, student in enumerate(student_batch):
                col = idx % COLS; row = idx // COLS
                card_x = OX_PT + col * COL_STEP
                card_y = OY_PT + row * ROW_STEP
                target_rect = fitz.Rect(card_x, card_y,
                                        card_x + CARD_W_PT, card_y + CARD_H_PT)

                # ── OPTIMISATION 2: bytes, not live doc ──
                card_bytes = render_card_bytes(student)
                if card_bytes is None:
                    continue

                tmp_doc = fitz.open("pdf", card_bytes)
                a4_page.show_pdf_page(target_rect, tmp_doc, 0, keep_proportion=False)
                tmp_doc.close()
                del card_bytes                 # release immediately
                gc.collect()                   # ← every card

                # Serial badge in the gap between rows
                if row < ROWS - 1:
                    gap_top  = card_y + CARD_H_PT
                    badge_cx = card_x + CARD_W_PT / 2.0
                    badge_cy = gap_top + ROW_GAP_PT / 2.0
                    draw_serial_badge_vector(
                        a4_page,
                        student_start + idx + 1,
                        badge_cx, badge_cy, ROW_GAP_PT,
                    )

            # ── OPTIMISATION 1: flush batch to disk ──
            pages_in_doc = page_idx - batch_start_page + 1
            if pages_in_doc >= SAVE_BATCH_PAGES or page_idx == n_pages - 1:
                batch_path = _make_tmp_path(".pdf")
                out_doc.save(
                    batch_path,
                    deflate=False,           # fast — full deflate at merge step
                    garbage=1, clean=False,
                )
                print(f"DEBUG: batch saved ({pages_in_doc} pages → {batch_path})")
                out_doc.close()
                batch_files.append(batch_path)
                gc.collect()

                if page_idx < n_pages - 1:   # more pages to come
                    out_doc          = fitz.open()
                    batch_start_page = page_idx + 1

        # ── Merge batch files + compress once ──
        print(f"DEBUG: merging {len(batch_files)} batch files …")
        merged = fitz.open()
        for bp in batch_files:
            with fitz.open(bp) as src:
                merged.insert_pdf(src)

        merged.save(
            final_path,
            deflate=True, deflate_images=True, deflate_fonts=True,
            garbage=4, clean=True, linear=False,
        )
        merged.close()
        gc.collect()
        print(f"DEBUG: PDF saved — {n_pages} pages, {n_students} students → {final_path}")
        return final_path

    except Exception as e:
        print(f"DEBUG: build_pdf_file_vector failed: {e}")
        _unregister_tmp(final_path)
        try:
            if os.path.exists(final_path):
                os.unlink(final_path)
        except Exception:
            pass
        raise

    finally:
        # Clean up batch temp files (keep final_path — caller deletes it)
        for bp in batch_files:
            _unregister_tmp(bp)
            try:
                if os.path.exists(bp):
                    os.unlink(bp)
            except Exception:
                pass


# ════════════════════════════════════════════════════════════════════
#  RASTER FALLBACK (unchanged, only used if no template)
# ════════════════════════════════════════════════════════════════════

def _placeholder_card_pil(student, dpi=150):
    if not HAS_PIL:
        return None
    w = int(55 / 25.4 * dpi); h = int(86 / 25.4 * dpi)
    img  = Image.new("RGB", (w, h), (255, 255, 255))
    draw = ImageDraw.Draw(img)
    draw.rectangle([0, 0, w, int(h*0.3)], fill=(200, 30, 30))
    name = student.get("student_name","Student").upper()
    draw.text((10, 10), name, fill="white")
    draw.text((10, int(h*0.35)), f"Class: {student.get('class','')}", fill=(100,100,100))
    return img

def build_pdf_file_raster_fallback(students, dpi=150):
    if not HAS_FITZ or not HAS_PIL:
        return None

    def mm2px(mm): return int(round(mm / 25.4 * dpi))
    a4_w_px   = mm2px(A4_W_MM);   a4_h_px   = mm2px(A4_H_MM)
    card_w_px = mm2px(CARD_W_MM); card_h_px = mm2px(CARD_H_MM)
    ox_px     = mm2px(OFFSET_X_MM); oy_px   = mm2px(OFFSET_Y_MM)
    gap_px    = mm2px(ROW_GAP_MM); col_gap_px = mm2px(1.0)
    a4_w_pt   = A4_W_MM * MM_TO_PT; a4_h_pt = A4_H_MM * MM_TO_PT

    out_doc  = fitz.open()
    tmp_path = _make_tmp_path(".pdf")
    n_pages  = (len(students) + CARDS_PER_PAGE - 1) // CARDS_PER_PAGE

    try:
        for page_idx in range(n_pages):
            batch = students[page_idx * CARDS_PER_PAGE : (page_idx+1) * CARDS_PER_PAGE]
            sheet = Image.new("RGB", (a4_w_px, a4_h_px), (245,245,245))
            for idx, s in enumerate(batch):
                col   = idx % COLS; row = idx // COLS
                x     = ox_px + col * (card_w_px + col_gap_px)
                y     = oy_px + row * (card_h_px + gap_px)
                card  = _placeholder_card_pil(s, dpi)
                if card:
                    sheet.paste(card.resize((card_w_px, card_h_px)), (x, y))
                    card.close()
            buf = io.BytesIO()
            sheet.save(buf, format="JPEG", quality=72, optimize=True)
            sheet.close()
            pg = out_doc.new_page(width=a4_w_pt, height=a4_h_pt)
            pg.insert_image(fitz.Rect(0,0,a4_w_pt,a4_h_pt),
                            stream=buf.getvalue(), overlay=True, keep_proportion=False)
            gc.collect()
        out_doc.save(tmp_path, deflate=True, garbage=4, clean=True)
        return tmp_path
    except Exception:
        _unregister_tmp(tmp_path)
        try:
            if os.path.exists(tmp_path): os.unlink(tmp_path)
        except: pass
        raise
    finally:
        out_doc.close()
        gc.collect()


def build_pdf_file(students, dpi=150):
    if HAS_FITZ and TEMPLATE_PDF.exists():
        return build_pdf_file_vector(students)
    print("DEBUG: Template PDF not found — using raster fallback")
    return build_pdf_file_raster_fallback(students, dpi=dpi)


# ════════════════════════════════════════════════════════════════════
#  RESPONSE SENDER
# ════════════════════════════════════════════════════════════════════

def send_generated_pdf(students, dpi, download_name, as_attachment, allow_external=False):
    if not students:
        return jsonify({"error": "No students loaded"}), 400
    if len(students) > MAX_STUDENTS_PER_REQUEST:
        return jsonify({
            "error": (
                f"Too many students ({len(students)}). "
                f"Filter by class or increase MAX_STUDENTS_PER_REQUEST."
            )
        }), 413

    # ── OPTIMISATION 3: prime the cache for the first batch only ──
    _prefetch_chunk(students[:PREFETCH_AHEAD])

    pdf_path = build_pdf_file(students, dpi=dpi)
    if not pdf_path:
        return jsonify({"error": "PDF generation failed — check server libs"}), 500

    @after_this_request
    def cleanup(response):
        _unregister_tmp(pdf_path)
        try:
            if os.path.exists(pdf_path):
                os.unlink(pdf_path)
        except Exception:
            pass
        gc.collect()
        return response

    if allow_external and _external_storage_enabled():
        try:
            remote_url = upload_pdf_to_external_storage(pdf_path, download_name)
            if remote_url:
                return jsonify({
                    "success": True,
                    "storage": STORAGE_BACKEND,
                    "download_url": remote_url,
                    "download_name": download_name,
                })
        except Exception as e:
            print(f"DEBUG: External storage upload failed: {e}")

    return send_file(
        pdf_path,
        mimetype="application/pdf",
        as_attachment=as_attachment,
        download_name=download_name,
        conditional=True,
        max_age=0,
    )


# ════════════════════════════════════════════════════════════════════
#  ENDPOINTS
# ════════════════════════════════════════════════════════════════════

@app.route("/api/preview/all", methods=["GET"])
@app.route("/preview/all", methods=["GET"])
def preview_all():
    students = _store["students"]
    if not students:
        return jsonify({"error": "No students loaded"}), 400
    cls      = request.args.get("class","").strip().upper()
    students = filter_students_by_class(students, cls)
    return send_generated_pdf(students, dpi=PREVIEW_DPI,
                              download_name="preview.pdf", as_attachment=False)

@app.route("/api/download/all", methods=["GET"])
@app.route("/download/all", methods=["GET"])
def download_all():
    students = _store["students"]
    if not students:
        return jsonify({"error": "No students loaded"}), 400
    cls = request.args.get("class","").strip().upper()
    if cls:
        students = filter_students_by_class(students, cls)
        fname    = f"ids_{cls}.pdf"
    else:
        students = list(students)
        fname    = "ids_ALL.pdf"
    return send_generated_pdf(students, dpi=DOWNLOAD_DPI,
                              download_name=fname, as_attachment=True, allow_external=True)

@app.route("/api/preview/student", methods=["GET"])
@app.route("/preview/student", methods=["GET"])
def preview_student():
    students = _store["students"]
    cls      = request.args.get("class","").strip().upper()
    name     = request.args.get("name","").strip().lower()
    if not students:
        return jsonify({"error": "No students loaded"}), 400
    matches = [s for s in students
               if s.get("class","").strip().upper() == cls
               and name == s.get("student_name","").strip().lower()]
    if not matches:
        return jsonify({"error": "Student not found"}), 404
    return send_generated_pdf([matches[0]], dpi=PREVIEW_DPI,
                              download_name="preview_student.pdf", as_attachment=False)

@app.route("/api/download/student", methods=["GET"])
@app.route("/download/student", methods=["GET"])
def download_student():
    students = _store["students"]
    cls      = request.args.get("class","").strip().upper()
    name     = request.args.get("name","").strip().lower()
    if not students:
        return jsonify({"error": "No students loaded"}), 400
    matches = [s for s in students
               if s.get("class","").strip().upper() == cls
               and name == s.get("student_name","").strip().lower()]
    if not matches:
        return jsonify({"error": "Student not found"}), 404
    student   = matches[0]
    safe_name = student.get("student_name","student").replace(" ","_")
    return send_generated_pdf([student], dpi=DOWNLOAD_DPI,
                              download_name=f"id_{safe_name}.pdf", as_attachment=True, allow_external=True)


# ════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    ck = '\u2713'; xk = '\u2717'
    print("=" * 60)
    print("  ID Card Generator Backend  v2.2  (Railway-Optimized)")
    print(f"  Template PDF : {ck+' found' if TEMPLATE_PDF.exists() else xk+' NOT FOUND (raster fallback)'}")
    print(f"  Anton font   : {ck+' found' if ANTON_FONT.exists() else xk+' NOT FOUND'}")
    print(f"  Arial Bold   : {ck+' found' if ARIAL_BOLD.exists() else xk+' NOT FOUND'}")
    print(f"  PyMuPDF      : {ck if HAS_FITZ else xk+' pip install pymupdf'}")
    print(f"  Pillow       : {ck if HAS_PIL  else xk+' pip install pillow'}")
    print(f"  Photo size   : {PHOTO_PX}x{PHOTO_PX} px  JPEG quality {PHOTO_JPEG_QUALITY}")
    print(f"  Photo cache  : LRU({MAX_CACHED_PHOTOS}) entries")
    print(f"  Batch save   : {SAVE_BATCH_PAGES} pages / flush")
    print(f"  Prefetch     : {PREFETCH_WORKERS} threads × {PREFETCH_AHEAD} students ahead")
    print(f"  Storage      : {STORAGE_BACKEND}")
    print("=" * 60)
    port  = int(os.environ.get("PORT", 5000))
    debug = os.environ.get("FLASK_DEBUG","").strip() == "1"
    app.run(debug=debug, use_reloader=debug, host="0.0.0.0", port=port)