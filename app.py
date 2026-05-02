"""
ID Card Generator - Flask Backend v2.3
Fast vector-native PDF assembly tuned for 512 MB / 0.5 CPU production.

Key changes vs v2.1:
  • Empty image rect when photo missing (no fallback / no sample image)
  • DOB normalised to DD-MM-YYYY for ALL 4 templates
  • Addresses containing HTML (<br>, <p>, &nbsp; …) are dropped to empty
  • Aggressive parallel photo prefetch with HTTP connection pooling
  • Per-card render pool (priyanka / ab_ascent) — uses CPU + I/O in parallel
  • PHOTO_PX 360 / JPEG q=85 — higher quality than before
  • All DEBUG prints removed
  • Short timeouts + keep-alive — works on WiFi (corporate proxies no longer
    kill long idle requests) AND mobile data
"""

import io
import os
import re
import sys
import json
import tempfile
import uuid
import threading
import requests
from pathlib import Path
from datetime import datetime
from collections import defaultdict, OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from flask import Flask, request, jsonify, send_file, after_this_request
from flask_cors import CORS
from requests.adapters import HTTPAdapter
import pandas as pd
import gc

try:
    import fitz  # PyMuPDF
    HAS_FITZ = True
except ImportError:
    HAS_FITZ = False

try:
    from PIL import Image, ImageOps, ImageDraw, ImageFont, ImageFilter
    HAS_PIL = True
    Image.MAX_IMAGE_PIXELS = 20_000_000
except ImportError:
    HAS_PIL = False

# ─────────────────────────────────────────────────────────────────
app = Flask(__name__)
CORS(app,
     origins=["*"],
     methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
     allow_headers=["Content-Type", "Authorization", "X-Requested-With"],
     supports_credentials=False,
     expose_headers=["Content-Disposition", "Content-Type"])

@app.after_request
def _add_cors(response):
    response.headers["Access-Control-Allow-Origin"]   = "*"
    response.headers["Access-Control-Allow-Methods"]  = "GET, POST, PUT, DELETE, OPTIONS"
    response.headers["Access-Control-Allow-Headers"]  = "Content-Type, Authorization, X-Requested-With"
    response.headers["Access-Control-Expose-Headers"] = "Content-Disposition, Content-Type"
    # Tell proxies (corporate WiFi) to keep the connection alive while the PDF builds
    response.headers["Connection"] = "keep-alive"
    response.headers["Cache-Control"] = "no-store"
    return response

@app.route("/api/<path:subpath>", methods=["OPTIONS"])
@app.route("/<path:subpath>", methods=["OPTIONS"])
def _options_handler(subpath=""):
    return ("", 204)


BASE_DIR               = Path(__file__).parent
TEMPLATE_PDF_HEBRON    = BASE_DIR / "template_id_card.pdf"
TEMPLATE_PDF_REDEEMER  = BASE_DIR / "template_redeemer.pdf"
TEMPLATE_PDF_PRIYANKA  = BASE_DIR / "template_priyanka.pdf"
TEMPLATE_PDF_AB_ASCENT = BASE_DIR / "template_ab_ascent.pdf"
ANTON_FONT             = BASE_DIR / "Anton-Regular.ttf"
ARIAL_BOLD             = BASE_DIR / "arialbd.ttf"

DEFAULT_SESSION = "2026-27"
DEFAULT_TEMPLATE = "redeemer"

SCHOOLS = {
    2: "My Redeemer Mission School",
    3: "Hebron Mission School",
    4: "Priyanka Dreamnest School",
    5: "Ab Ascent School",
}

TEMPLATE_CONFIGS = {
    "hebron": {
        "key": "hebron",
        "label": "Hebron",
        "display_name": "Hebron Mission School",
        "pdf": TEMPLATE_PDF_HEBRON,
        "description": "Red Hebron layout with section, roll, mother name and blood group.",
        "fields": [
            "student_name", "class", "section", "roll", "father_name",
            "mother_name", "dob", "address", "mobile", "adm_no",
            "blood_group", "session", "photo_url",
        ],
    },
    "redeemer": {
        "key": "redeemer",
        "label": "Redeemer",
        "display_name": "My Redeemer Mission School",
        "pdf": TEMPLATE_PDF_REDEEMER,
        "description": "Blue Redeemer layout with father name, DOB, mobile and address.",
        "fields": [
            "student_name", "class", "father_name", "dob", "address",
            "mobile", "session", "photo_url", "adm_no",
        ],
    },
    "priyanka": {
        "key": "priyanka",
        "label": "Priyanka",
        "display_name": "Priyanka Dreamnest School",
        "pdf": TEMPLATE_PDF_PRIYANKA if TEMPLATE_PDF_PRIYANKA.exists() else TEMPLATE_PDF_REDEEMER,
        "description": "Priyanka Dreamnest School ID layout.",
        "fields": [
            "student_name", "class", "father_name", "dob", "address",
            "mobile", "session", "photo_url", "adm_no",
        ],
    },
    "ab_ascent": {
        "key": "ab_ascent",
        "label": "Ab Ascent",
        "display_name": "Ab Ascent School",
        "pdf": TEMPLATE_PDF_AB_ASCENT if TEMPLATE_PDF_AB_ASCENT.exists() else TEMPLATE_PDF_REDEEMER,
        "description": "Ab Ascent School ID layout.",
        "fields": [
            "student_name", "class", "father_name", "dob", "address",
            "mobile", "session", "photo_url", "adm_no",
        ],
    },
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

MAX_UPLOAD_MB             = int(os.environ.get("MAX_UPLOAD_MB", "12"))
MAX_STUDENTS_PER_REQUEST  = int(os.environ.get("MAX_STUDENTS_PER_REQUEST", "1000"))
PREVIEW_DPI               = int(os.environ.get("PREVIEW_DPI", "150"))
DOWNLOAD_DPI              = int(os.environ.get("DOWNLOAD_DPI", "150"))
# ⏱  Photo timeouts — connect 6s, read 12s. titusattendence.com can be slow.
PHOTO_TIMEOUT             = (6, 12)
MAX_PHOTO_BYTES           = int(os.environ.get("MAX_PHOTO_BYTES", str(4 * 1024 * 1024)))

# 🏭  PRODUCTION MODE: set PRODUCTION=1 env var on Railway (512 MB / 0.5 CPU).
#     Locally leave unset for full performance.
_IS_PRODUCTION = os.environ.get("PRODUCTION", "0").strip() in ("1", "true", "yes")

# Production: use /tmp (always writable, cleaned by OS). Local: tempfile default.
_PROD_TMP = "/tmp"
PDF_TEMP_DIR = os.environ.get(
    "PDF_TEMP_DIR",
    _PROD_TMP if _IS_PRODUCTION else tempfile.gettempdir()
)
# Ensure the temp dir exists and is writable; fall back to /tmp
try:
    os.makedirs(PDF_TEMP_DIR, exist_ok=True)
    _test = tempfile.NamedTemporaryFile(delete=True, dir=PDF_TEMP_DIR)
    _test.close()
except Exception:
    PDF_TEMP_DIR = _PROD_TMP
    os.makedirs(PDF_TEMP_DIR, exist_ok=True)

# 📷  Quality: production uses smaller photos to save RAM; local uses higher quality
PHOTO_PX           = int(os.environ.get("PHOTO_PX", "240" if _IS_PRODUCTION else "360"))
PHOTO_JPEG_QUALITY = int(os.environ.get("PHOTO_JPEG_QUALITY", "75" if _IS_PRODUCTION else "85"))

# 🧠  Memory caps: production must stay under ~350 MB working set
MAX_CACHED_PHOTOS   = int(os.environ.get("MAX_CACHED_PHOTOS",  "80"  if _IS_PRODUCTION else "600"))
# Production: fewer threads — 0.5 CPU means 2 real threads is enough; local: 16
PREFETCH_WORKERS    = int(os.environ.get("PREFETCH_WORKERS",   "4"   if _IS_PRODUCTION else "16"))
# Production: serial per-card render avoids OOM on large batches
CARD_RENDER_WORKERS = int(os.environ.get("CARD_RENDER_WORKERS","1"   if _IS_PRODUCTION else "4"))
PREVIEW_EXTERNAL_THRESHOLD = int(os.environ.get("PREVIEW_EXTERNAL_THRESHOLD", "9999"))
REDEEMER_GRAD_STEPS = int(os.environ.get("REDEEMER_GRAD_STEPS", "30" if _IS_PRODUCTION else "60"))

# 🗑  Max students per request in production — keeps peak RAM predictable
if _IS_PRODUCTION:
    MAX_STUDENTS_PER_REQUEST = min(MAX_STUDENTS_PER_REQUEST,
                                   int(os.environ.get("MAX_STUDENTS_PER_REQUEST", "300")))

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

# ─────────────────────────────────────────────────────────────────
# Global HTTP Session — connection pooling massively cuts photo-fetch latency.
# Without it, requests opens a new TCP+TLS handshake per photo (~500 ms each).
# Pool of 32 keep-alive sockets → near-zero handshake cost on repeat hosts.
# Solves the WiFi-vs-mobile flakiness: many WiFi networks throttle SYN bursts.
# ─────────────────────────────────────────────────────────────────
_HTTP = requests.Session()
_HTTP.headers.update({
    "User-Agent": "Mozilla/5.0 (compatible; IDCardGen/2.4)",
    "Accept": "image/*,*/*;q=0.8",
    "Connection": "keep-alive",
})
# Retry up to 3 times with backoff - helps with flaky school servers
from urllib3.util.retry import Retry
_retry = Retry(total=3, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504],
               allowed_methods=["GET"], raise_on_status=False)
_adapter = HTTPAdapter(pool_connections=32, pool_maxsize=64, max_retries=_retry)
_HTTP.mount("http://",  _adapter)
_HTTP.mount("https://", _adapter)


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
        },
        timeout=20,
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
        data=json.dumps(metadata),
        timeout=30,
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
            data=fh,
            timeout=300,
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
            data=fh,
            timeout=300,
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

# ─────────────────────────────────────────────────────────────────
# DATA-CLEANING HELPERS  (NEW: HTML scrub + DOB normalisation)
# ─────────────────────────────────────────────────────────────────
_HTML_TAG_RE = re.compile(r"<[^>]+>")
_HTML_ENT_RE = re.compile(r"&(?:nbsp|amp|lt|gt|quot|apos|#\d+|#x[0-9a-fA-F]+);")

def has_html(text: str) -> bool:
    if not text:
        return False
    if _HTML_TAG_RE.search(text):
        return True
    if _HTML_ENT_RE.search(text):
        return True
    return False

def clean_address(text) -> str:
    """Drop addresses that contain HTML markup — return empty string instead."""
    if text is None:
        return ""
    s = str(text).strip()
    if not s or s.lower() in {"nan", "none", "null", "nil"}:
        return ""
    if has_html(s):
        return ""   # 🚫  HTML found in DB row → keep blank
    return s

def format_dob(text) -> str:
    """
    Normalise any DOB into DD-MM-YYYY (zero-padded).
    Accepts: 2010-04-23, 23/04/2010, 23-04-2010, 04/23/2010, 2010/04/23,
             '23 April 2010', timestamps, ints, etc.
    Returns "" for invalid / empty / placeholder values.
    """
    if text is None:
        return ""
    s = str(text).strip()
    if not s:
        return ""
    low = s.lower()
    if low in {"nan", "none", "null", "nil", "0000-00-00", "00-00-0000",
               "0000/00/00", "00/00/0000"}:
        return ""

    # Try strict ISO-style + common explicit formats first
    fmt_candidates = [
        "%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d",
        "%d-%m-%Y", "%d/%m/%Y", "%d.%m.%Y",
        "%m-%d-%Y", "%m/%d/%Y",
        "%d-%b-%Y", "%d %b %Y", "%d %B %Y",
        "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S",
    ]
    only_date = s.split(" ")[0] if "T" not in s else s
    for fmt in fmt_candidates:
        try:
            dt = datetime.strptime(s, fmt)
            return f"{dt.day:02d}-{dt.month:02d}-{dt.year:04d}"
        except Exception:
            pass
        try:
            dt = datetime.strptime(only_date, fmt)
            return f"{dt.day:02d}-{dt.month:02d}-{dt.year:04d}"
        except Exception:
            pass

    # Last-chance: pandas parser (handles oddities)
    try:
        dt = pd.to_datetime(s, errors="raise", dayfirst=True)
        if pd.notna(dt):
            return f"{dt.day:02d}-{dt.month:02d}-{dt.year:04d}"
    except Exception:
        pass

    # Bare digits like 20100423
    digits = re.sub(r"\D", "", s)
    if len(digits) == 8:
        try:
            dt = datetime.strptime(digits, "%Y%m%d")
            return f"{dt.day:02d}-{dt.month:02d}-{dt.year:04d}"
        except Exception:
            try:
                dt = datetime.strptime(digits, "%d%m%Y")
                return f"{dt.day:02d}-{dt.month:02d}-{dt.year:04d}"
            except Exception:
                pass

    return ""   # could not parse → keep empty


# ─────────────────────────────────────────────────────────────────
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

def _post_clean_student(s: dict) -> dict:
    """Apply DOB normalisation + address HTML scrub + general value cleanup."""
    s["dob"]     = format_dob(s.get("dob", ""))
    s["address"] = clean_address(s.get("address", ""))
    return s

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
            "adm_no":       pick(rm,"adm_no","admission_no","admission_number","adm","admno","reg_no","registration_no"),
            "blood_group":  pick(rm,"blood_group","bloodgroup","blood"),
            "gender":       pick(rm,"gender","sex"),
            "session":      pick(rm,"session",default=DEFAULT_SESSION),
            "bus_route":    pick(rm,"bus_route","bus","bus_no","bus_number","route"),
        }
        if any(s.values()):
            students.append(_post_clean_student(s))
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
    "admission_number":"adm_no","adm":"adm_no","reg_no":"adm_no","registration_no":"adm_no","bloodgroup":"blood_group",
    "blood":"blood_group","gender":"gender","sex":"gender",
    "bus_route":"bus_route","bus":"bus_route","bus_no":"bus_route",
    "bus_number":"bus_route","route":"bus_route",
}

def map_api_record(record):
    out = {
        "student_name":"","class":"","section":"","roll":"","father_name":"",
        "mother_name":"","dob":"","address":"","mobile":"","photo_url":"",
        "adm_no":"","blood_group":"","gender":"","session":DEFAULT_SESSION,
        "bus_route":"",
    }
    for k, v in record.items():
        internal = _API_MAP.get(k.strip().lower())
        if internal and v not in (None,"","null","NULL"):
            val = str(v).strip()
            # Clean photo URLs — fix escaped slashes from JSON copy-paste
            if internal == "photo_url":
                val = val.replace("\\/", "/")
            out[internal] = val
    return _post_clean_student(out)

# ─────────────────────────────────────────────────────────────────
# ROUTES
# ─────────────────────────────────────────────────────────────────

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
        resp = _HTTP.get(url, timeout=30)
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


def normalize_template_key(value):
    key = str(value or DEFAULT_TEMPLATE).strip().lower()
    return key if key in TEMPLATE_CONFIGS else DEFAULT_TEMPLATE


def get_template_config(template_key=None):
    return TEMPLATE_CONFIGS[normalize_template_key(template_key)]

# ─────────────────────────────────────────────────────────────────
# PHOTO CACHE
# ─────────────────────────────────────────────────────────────────

class _BoundedPhotoCache:
    def __init__(self, maxsize: int = 600):
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

# ─────────────────────────────────────────────────────────────────
# TEMPLATE + FONT SINGLETONS
# ─────────────────────────────────────────────────────────────────

_template_bytes_cache: dict = {}
_template_locks = {key: threading.Lock() for key in TEMPLATE_CONFIGS}
_template_doc_cache: dict = {}
_template_doc_locks = {key: threading.Lock() for key in TEMPLATE_CONFIGS}
_template_preview_cache: dict = {}
_template_preview_locks = {key: threading.Lock() for key in TEMPLATE_CONFIGS}

_anton_font_obj = None
_bold_font_obj  = None
_font_init_done = False
_font_lock      = threading.Lock()


def _ensure_template(template_key: str = DEFAULT_TEMPLATE):
    template = get_template_config(template_key)
    key = template["key"]
    if key in _template_bytes_cache:
        return _template_bytes_cache[key]
    lock = _template_locks[key]
    with lock:
        if key in _template_bytes_cache:
            return _template_bytes_cache[key]
        pdf_path = template["pdf"]
        if not pdf_path.exists():
            return None
        with open(str(pdf_path), "rb") as fh:
            _template_bytes_cache[key] = fh.read()
        return _template_bytes_cache[key]


def _get_template_doc(template_key: str = DEFAULT_TEMPLATE):
    if not HAS_FITZ:
        return None
    key = normalize_template_key(template_key)
    if key in _template_doc_cache:
        return _template_doc_cache[key]
    lock = _template_doc_locks[key]
    with lock:
        if key in _template_doc_cache:
            return _template_doc_cache[key]
        tmpl_bytes = _ensure_template(key)
        if tmpl_bytes is None:
            return None
        _template_doc_cache[key] = fitz.open("pdf", tmpl_bytes)
        return _template_doc_cache[key]


def _get_template_preview_png(template_key: str = DEFAULT_TEMPLATE):
    key = normalize_template_key(template_key)
    if key in _template_preview_cache:
        return _template_preview_cache[key]
    if not HAS_FITZ:
        return None
    lock = _template_preview_locks[key]
    with lock:
        if key in _template_preview_cache:
            return _template_preview_cache[key]
        tmpl_bytes = _ensure_template(key)
        if tmpl_bytes is None:
            return None
        doc = fitz.open("pdf", tmpl_bytes)
        try:
            page = doc[0]
            pix = page.get_pixmap(matrix=fitz.Matrix(2, 2), alpha=False)
            _template_preview_cache[key] = pix.tobytes("png")
        finally:
            doc.close()
        return _template_preview_cache[key]


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
        except Exception:
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

# ─────────────────────────────────────────────────────────────────
# PHOTO COMPRESSION  (higher quality)
# ─────────────────────────────────────────────────────────────────

def _compress_photo(pil_img) -> bytes:
    # ✅ FIX: Apply EXIF orientation FIRST — phones store rotation in metadata.
    # Without this, a portrait photo taken on a phone appears sideways because
    # PIL reads raw pixels (landscape) without honouring the EXIF rotation tag.
    try:
        pil_img = ImageOps.exif_transpose(pil_img)
    except Exception:
        pass  # exif_transpose may fail on images without EXIF — ignore safely

    rgb = pil_img.convert("RGB")
    src_w, src_h = rgb.size
    src_min = min(src_w, src_h)

    if src_min < 280:
        rgb = rgb.filter(ImageFilter.SMOOTH)

    resized = ImageOps.fit(rgb, (PHOTO_PX, PHOTO_PX), method=Image.Resampling.LANCZOS)
    if rgb is not pil_img:
        rgb.close()

    if src_min >= 280:
        resized = resized.filter(ImageFilter.UnsharpMask(radius=1.2, percent=90, threshold=4))

    buf = io.BytesIO()
    resized.save(buf, format="JPEG", quality=PHOTO_JPEG_QUALITY,
                 optimize=True, progressive=False, subsampling=1)
    resized.close()
    return buf.getvalue()


# ─────────────────────────────────────────────────────────────────
# PHOTO FETCH — empty space when missing (NO fallback image)
# ─────────────────────────────────────────────────────────────────
def _clean_photo_url(url: str) -> str:
    """
    Sanitise a photo URL:
      - Strip whitespace / zero-width chars
      - Remove markdown link syntax like [domain](http://actual-url)
      - Remove any trailing punctuation left by copy-paste
    """
    if not url:
        return ""
    url = url.strip()
    # Handle markdown link format: [text](url)  ->  url
    import re as _re
    md = _re.match(r"\[.*?\]\((https?://[^)]+)\)", url)
    if md:
        url = md.group(1)
    # Replace encoded backslashes that appear in JSON copy-paste
    url = url.replace("\\/ ", "/").replace("\\/", "/")
    # Make sure it starts with http
    if not (url.startswith("http://") or url.startswith("https://")):
        return ""
    return url.strip()


def fetch_photo_bytes(url: str):
    """
    Returns compressed JPEG bytes, or None when the photo is missing/invalid.
    None is rendered as empty (transparent) rect on the card — no sample image.
    Tries https with verify=True first; falls back to verify=False for self-signed certs.
    """
    if not HAS_PIL:
        return None

    cache_key = _clean_photo_url((url or "").strip())
    if not cache_key:
        return None

    found, cached = _photo_cache.get(cache_key)
    if found:
        return cached  # may legitimately be None

    def _do_fetch(verify_ssl=True):
        resp = _HTTP.get(cache_key, timeout=PHOTO_TIMEOUT, stream=True,
                         allow_redirects=True, verify=verify_ssl)
        resp.raise_for_status()
        ct = (resp.headers.get("Content-Type") or "").lower()
        if "text/html" in ct:
            return None
        chunks = []
        total  = 0
        for chunk in resp.iter_content(64 * 1024):
            if not chunk:
                continue
            total += len(chunk)
            if total > MAX_PHOTO_BYTES:
                raise ValueError("photo too large")
            chunks.append(chunk)
        resp.close()
        return b"".join(chunks)

    try:
        raw = _do_fetch(verify_ssl=True)
        if raw is None:
            _photo_cache.set(cache_key, None)
            return None
    except Exception:
        # SSL / connection error — retry without verification (handles self-signed certs)
        try:
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            raw = _do_fetch(verify_ssl=False)
            if raw is None:
                _photo_cache.set(cache_key, None)
                return None
        except Exception:
            _photo_cache.set(cache_key, None)
            return None

    try:
        with Image.open(io.BytesIO(raw)) as img:
            compressed = _compress_photo(img)
        _photo_cache.set(cache_key, compressed)
        return compressed
    except Exception:
        _photo_cache.set(cache_key, None)
        return None


def clear_photo_cache():
    _photo_cache.clear()


def prefetch_photos(students: list) -> None:
    urls = list({
        s.get("photo_url","").strip()
        for s in students
        if s.get("photo_url","").strip()
    })
    if not urls:
        return
    workers = min(PREFETCH_WORKERS, max(4, len(urls)))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(fetch_photo_bytes, url) for url in urls]
        for f in as_completed(futures):
            try:
                f.result()
            except Exception:
                pass

# ─────────────────────────────────────────────────────────────────
# CARD LAYOUT CONSTANTS
# ─────────────────────────────────────────────────────────────────
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

REDEEMER_BG_COLOR               = (0.9529, 0.9922, 1.0)
REDEEMER_GRAD_LEFT             = (234/255, 250/255, 255/255)
REDEEMER_GRAD_RIGHT            = (245/255, 253/255, 255/255)
REDEEMER_BLUE                  = (31/255, 72/255, 255/255)
REDEEMER_RED                   = (1.0, 0.1922, 0.1922)
REDEEMER_WHITE                 = (232/255, 246/255, 255/255)
REDEEMER_BLACK                 = (0.0, 0.0, 0.0)
REDEEMER_PHOTO_OUTER_RECT      = (53.55, 75.973, 99.45, 132.989)
REDEEMER_PHOTO_RECT_COORDS     = (54.58, 77.072, 98.594, 131.969)
REDEEMER_PHOTO_BORDER_W        = 1.03
REDEEMER_BANNER_RECT           = (0.0, 140.0, 126.0, 163.94)
REDEEMER_BANNER_TEXT_LEFT      = 4.0
REDEEMER_BANNER_TEXT_RIGHT     = 126.0
REDEEMER_BANNER_CENTER_X       = 63.0
REDEEMER_BANNER_ACCENT_POINTS  = (
    (126.0, 140.0),
    (151.4, 140.0),
    (142.0, 163.94),
    (122.8, 163.94),
)
REDEEMER_NAME_TEXT_RECT        = (4.0, 142.0, 126.0, 153.8)
REDEEMER_CLASS_TEXT_RECT       = (8.0, 154.0, 126.0, 163.2)
REDEEMER_NAME_BASELINE_Y       = 150.032
REDEEMER_CLASS_BASELINE_Y      = 161.681
REDEEMER_SESSION_CLEAN_COORDS  = (103.0, 103.8, 137.6, 114.9)
REDEEMER_SESSION_VALUE_RECT    = (106.8, 104.4, 136.0, 114.2)
REDEEMER_DATA_CLEAN_RECT       = (63.0, 167.5, 153.0, 207.0)
REDEEMER_VALUE_X               = 64.951
REDEEMER_VALUE_MAX_X           = 149.0
REDEEMER_FATHER_BASELINE_Y     = 175.50
REDEEMER_DOB_BASELINE_Y        = 185.85
REDEEMER_MOBILE_BASELINE_Y     = 196.20
REDEEMER_ADDRESS_BASELINE_Y    = 206.55
REDEEMER_NAME_FONT_SIZE        = 10.8775
REDEEMER_NAME_MIN_SIZE         = 6.0
REDEEMER_NAME_TRACKING         = 0.874
REDEEMER_CLASS_FONT_SIZE       = 5.8842
REDEEMER_CLASS_TRACKING        = 0.477
REDEEMER_VALUE_FONT_SIZE       = 6.8
REDEEMER_ADDRESS_MAX_LINES     = 2
REDEEMER_ADDRESS_LINE_GAP      = 1.02
REDEEMER_SESSION_FONT_SIZE     = 7.2

TEARDROP_ITEMS = [
    ('l', (126.74588, 84.57169), (119.56597, 72.82723)),
    ('l', (119.56597, 72.82723), (112.91280, 84.49141)),
    ('c', (112.91280, 84.49141),(111.36359, 86.96311),(111.22838, 90.17703),(112.85576, 92.83886)),
    ('c', (112.85576, 92.83886),(115.16902, 96.62247),(120.15327, 97.83719),(123.98969, 95.55492)),
    ('c', (123.98969, 95.55492),(127.82469, 93.27335),(129.05914, 88.35811),(126.74588, 84.57169)),
]

# ─────────────────────────────────────────────────────────────────
# TEXT RENDERING HELPERS
# ─────────────────────────────────────────────────────────────────

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


def clean_visible_text(text):
    if text is None:
        return ""
    text = str(text)
    text = (text
            .replace("\xa0", " ")
            .replace("\u200b", "")
            .replace("\u200c", "")
            .replace("\u200d", "")
            .replace("\ufeff", "")
            .replace("\ufffd", " "))
    text = "".join(ch for ch in text if ch == "\n" or ord(ch) >= 32)
    return " ".join(text.split()).strip()


def clean_card_value(text):
    text = clean_visible_text(text)
    if not text:
        return ""
    lowered = text.lower()
    if lowered in {"nan", "none", "null", "nil", "0000-00-00", "00-00-0000", "0000/00/00"}:
        return ""
    if has_html(text):           # 🚫 HTML in any field → blank
        return ""
    # Only reject strings that are purely zeros with separators (e.g. "00/00/0000")
    # Do NOT reject valid values like "2026-27" (session), "21-04-2014" (DOB), or adm_no digits.
    digits_only = re.sub(r"[0\-/:\. ]", "", text)
    if not digits_only and re.search(r"[1-9]", text) is None:
        return ""
    return text


def insert_image_safe(page, rect, photo_bytes):
    """Insert image only if bytes available; otherwise leave the rect EMPTY."""
    if not photo_bytes:
        return
    page.insert_image(rect, stream=photo_bytes, overlay=True, keep_proportion=False)


def prepare_photo_for_rect(photo_bytes, rect_coords, scale=6, output_format="JPEG"):
    if not HAS_PIL or not photo_bytes:
        return photo_bytes
    x0, y0, x1, y1 = rect_coords
    target_w = max(1, int(round((x1 - x0) * scale)))
    target_h = max(1, int(round((y1 - y0) * scale)))
    target_ratio = (x1 - x0) / max(1e-6, (y1 - y0))
    try:
        with Image.open(io.BytesIO(photo_bytes)) as img:
            rgb = img.convert("RGB")
            src_w, src_h = rgb.size
            src_ratio = src_w / max(1e-6, src_h)
            if src_ratio > target_ratio:
                new_w = max(1, int(round(src_h * target_ratio)))
                left = max(0, (src_w - new_w) // 2)
                rgb = rgb.crop((left, 0, left + new_w, src_h))
            elif src_ratio < target_ratio:
                new_h = max(1, int(round(src_w / target_ratio)))
                top = max(0, (src_h - new_h) // 2)
                rgb = rgb.crop((0, top, src_w, top + new_h))
            resized = rgb.resize((target_w, target_h), Image.Resampling.LANCZOS)
            buf = io.BytesIO()
            save_fmt = (output_format or "JPEG").upper()
            if save_fmt == "JPEG":
                resized.save(buf, format="JPEG", quality=PHOTO_JPEG_QUALITY,
                             optimize=True, progressive=False, subsampling=1)
            else:
                resized.save(buf, format="PNG")
            resized.close()
            if rgb is not img:
                rgb.close()
            return buf.getvalue()
    except Exception:
        return photo_bytes


def insert_tracked_text(page, x, baseline_y, text, fontfile, fontname, font_obj, size, color, tracking=0.0):
    text = clean_visible_text(text)
    if not text:
        return
    if tracking <= 0 or len(text) <= 1:
        page.insert_text(
            (x, baseline_y), text,
            fontname=fontname, fontfile=str(fontfile) if fontfile else None,
            fontsize=size, color=color, overlay=True,
        )
        return
    cursor = x
    for ch in text:
        page.insert_text(
            (cursor, baseline_y), ch,
            fontname=fontname, fontfile=str(fontfile) if fontfile else None,
            fontsize=size, color=color, overlay=True,
        )
        cursor += font_obj.text_length(ch, fontsize=size) + tracking


def draw_redeemer_banner_text(page, text, center_x, baseline_y, max_width, fontfile, fontname, font_obj, base_size, color, tracking=0.0, min_size=4.0):
    text = clean_visible_text(text).upper()
    if not text:
        return
    size, adjusted_tracking = _fit_tracked_text(font_obj, text, max_width, base_size, tracking, min_size=min_size)
    text = _ellipsize_tracked_to_width(font_obj, text, max_width, size, adjusted_tracking)
    if not text:
        return
    total_width = _tracked_text_width(font_obj, text, size, adjusted_tracking)
    if total_width > max_width:
        adjusted_tracking = 0.0
        text = _ellipsize_tracked_to_width(font_obj, text, max_width, size, adjusted_tracking)
        total_width = _tracked_text_width(font_obj, text, size, adjusted_tracking)
    insert_tracked_text(
        page,
        center_x - total_width / 2.0,
        baseline_y,
        text,
        fontfile, fontname, font_obj, size, color, adjusted_tracking,
    )


def draw_redeemer_value(page, text, x, baseline_y, max_width, fontfile, fontname, font_obj, base_size, color, min_size=5.0):
    value = clean_card_value(text)
    if not value:
        return
    size = _fit_size(font_obj, value, max_width, base_size, min_size)
    value = _ellipsize_to_width(font_obj, value, max_width, size)
    if not value:
        return
    page.insert_text(
        (x, baseline_y), value,
        fontname=fontname, fontfile=str(fontfile) if fontfile else None,
        fontsize=size, color=color, overlay=True,
    )


def render_redeemer_address(page, addr, x, baseline_y, max_width, fontfile, fontname, font_obj, color, base_size=6.8, min_size=4.8, max_lines=2, line_gap=1.03):
    addr = clean_card_value(addr)
    if not addr:
        return
    words = addr.split()
    if not words:
        return
    for fs in [base_size, 6.5, 6.2, 6.0, 5.7, 5.4, 5.1, min_size]:
        lines = _addr_wrap_at_size(font_obj, words, max_width, fs)
        if len(lines) <= max_lines:
            chosen_fs = fs
            chosen_lines = lines
            break
    else:
        chosen_fs = min_size
        chosen_lines = _addr_wrap_at_size(font_obj, words, max_width, min_size)[:max_lines]
        if chosen_lines:
            last = chosen_lines[-1]
            while last and font_obj.text_length(last + "…", fontsize=min_size) > max_width:
                last = last[:-1]
            chosen_lines[-1] = last.rstrip() + ("…" if last.rstrip() != addr else "")
    step = chosen_fs * line_gap
    for idx, line in enumerate(chosen_lines[:max_lines]):
        page.insert_text(
            (x, baseline_y + idx * step), line,
            fontname=fontname, fontfile=str(fontfile) if fontfile else None,
            fontsize=chosen_fs, color=color, overlay=True,
        )


def _tracked_text_width(font_obj, text, fontsize, tracking=0.0):
    if not text:
        return 0.0
    base = font_obj.text_length(text, fontsize=fontsize)
    if tracking <= 0 or len(text) <= 1:
        return base
    return base + tracking * (len(text) - 1)


def _fit_tracked_text(font_obj, text, max_width, base_size, tracking=0.0, min_size=4.0):
    size = base_size
    adjusted_tracking = tracking
    while size >= min_size:
        if _tracked_text_width(font_obj, text, size, adjusted_tracking) <= max_width:
            return size, adjusted_tracking
        size -= 0.1
        adjusted_tracking = tracking * (size / base_size) if base_size else tracking
    return min_size, 0.0


def _ellipsize_to_width(font_obj, text, max_width, fontsize):
    text = clean_visible_text(text)
    if not text:
        return ""
    if font_obj.text_length(text, fontsize=fontsize) <= max_width:
        return text
    ellipsis = "…"
    if font_obj.text_length(ellipsis, fontsize=fontsize) > max_width:
        return ""
    trimmed = text.rstrip()
    while trimmed and font_obj.text_length(trimmed + ellipsis, fontsize=fontsize) > max_width:
        trimmed = trimmed[:-1].rstrip()
    return (trimmed + ellipsis) if trimmed else ellipsis


def _ellipsize_tracked_to_width(font_obj, text, max_width, fontsize, tracking=0.0):
    text = clean_visible_text(text)
    if not text:
        return ""
    if _tracked_text_width(font_obj, text, fontsize, tracking) <= max_width:
        return text
    ellipsis = "…"
    if _tracked_text_width(font_obj, ellipsis, fontsize, 0.0) > max_width:
        return ""
    trimmed = text.rstrip()
    while trimmed and _tracked_text_width(font_obj, trimmed + ellipsis, fontsize, tracking) > max_width:
        trimmed = trimmed[:-1].rstrip()
    return (trimmed + ellipsis) if trimmed else ellipsis


def _draw_horizontal_gradient_mask(page, rect, left_color, right_color, steps):
    steps = max(8, int(steps))
    x0, y0, x1, y1 = rect.x0, rect.y0, rect.x1, rect.y1
    band_w = (x1 - x0) / steps
    for step in range(steps):
        t = 0.0 if steps == 1 else step / (steps - 1)
        color = (
            left_color[0] + t * (right_color[0] - left_color[0]),
            left_color[1] + t * (right_color[1] - left_color[1]),
            left_color[2] + t * (right_color[2] - left_color[2]),
        )
        rx0 = x0 + step * band_w
        rx1 = x0 + (step + 1) * band_w + 0.02
        page.draw_rect(fitz.Rect(rx0, y0, rx1, y1), color=color, fill=color, width=0, overlay=True)


def _draw_redeemer_overlay_core(page, student: dict, map_point, map_rect, scale_x=1.0, scale_y=1.0):
    anton_obj, bold_obj, anton_fn, bold_fn, fn_anton, fn_bold = _ensure_fonts()
    if anton_obj is None or bold_obj is None:
        return

    page.draw_rect(
        map_rect(REDEEMER_BANNER_RECT),
        color=REDEEMER_BLUE, fill=REDEEMER_BLUE, width=0, overlay=True,
    )

    page.draw_rect(
        map_rect(REDEEMER_SESSION_CLEAN_COORDS),
        color=REDEEMER_BG_COLOR, fill=REDEEMER_BG_COLOR, width=0, overlay=True,
    )
    _draw_horizontal_gradient_mask(
        page,
        map_rect(REDEEMER_DATA_CLEAN_RECT),
        REDEEMER_GRAD_LEFT,
        REDEEMER_GRAD_RIGHT,
        max(20, REDEEMER_GRAD_STEPS),
    )

    page.draw_rect(
        map_rect(REDEEMER_PHOTO_OUTER_RECT),
        color=REDEEMER_WHITE, fill=REDEEMER_WHITE, width=0, overlay=True,
    )
    photo_bytes = prepare_photo_for_rect(fetch_photo_bytes(student.get("photo_url", "")), REDEEMER_PHOTO_RECT_COORDS)
    insert_image_safe(page, map_rect(REDEEMER_PHOTO_RECT_COORDS), photo_bytes)
    page.draw_rect(
        map_rect(REDEEMER_PHOTO_OUTER_RECT),
        color=REDEEMER_BLACK, fill=None, width=max(0.1, REDEEMER_PHOTO_BORDER_W * ((scale_x + scale_y) / 2.0)), overlay=True,
    )

    center_x = map_point(REDEEMER_BANNER_CENTER_X, 0).x
    banner_max_width = max(1.0, (REDEEMER_BANNER_TEXT_RIGHT - REDEEMER_BANNER_TEXT_LEFT) * scale_x)
    banner_min_scale = max(0.5, min(scale_x, scale_y))

    draw_redeemer_banner_text(
        page,
        student.get("student_name", ""),
        center_x,
        map_point(0, REDEEMER_NAME_BASELINE_Y).y,
        banner_max_width,
        anton_fn, fn_anton, anton_obj,
        REDEEMER_NAME_FONT_SIZE * banner_min_scale, REDEEMER_WHITE,
        tracking=REDEEMER_NAME_TRACKING * scale_x,
        min_size=REDEEMER_NAME_MIN_SIZE * banner_min_scale,
    )

    class_text = clean_card_value(student.get("class", "")).upper()
    if class_text:
        draw_redeemer_banner_text(
            page,
            f"CLASS:  {class_text}",
            center_x,
            map_point(0, REDEEMER_CLASS_BASELINE_Y).y,
            banner_max_width,
            bold_fn, fn_bold, bold_obj,
            REDEEMER_CLASS_FONT_SIZE * banner_min_scale, REDEEMER_WHITE,
            tracking=REDEEMER_CLASS_TRACKING * scale_x,
            min_size=4.5 * banner_min_scale,
        )

    value_x = map_point(REDEEMER_VALUE_X, 0).x
    value_max_width = max(1.0, (REDEEMER_VALUE_MAX_X - REDEEMER_VALUE_X) * scale_x)
    value_base_size = REDEEMER_VALUE_FONT_SIZE * min(scale_x, scale_y)
    value_min_size = 4.7 * min(scale_x, scale_y)

    draw_redeemer_value(page, student.get("father_name", ""), value_x, map_point(0, REDEEMER_FATHER_BASELINE_Y).y, value_max_width, bold_fn, fn_bold, bold_obj, value_base_size, REDEEMER_BLACK, min_size=value_min_size)
    # DOB already pre-formatted as DD-MM-YYYY
    draw_redeemer_value(page, student.get("dob", ""), value_x, map_point(0, REDEEMER_DOB_BASELINE_Y).y, value_max_width, bold_fn, fn_bold, bold_obj, value_base_size, REDEEMER_BLACK, min_size=value_min_size)
    draw_redeemer_value(page, student.get("mobile", ""), value_x, map_point(0, REDEEMER_MOBILE_BASELINE_Y).y, value_max_width, bold_fn, fn_bold, bold_obj, value_base_size, REDEEMER_BLACK, min_size=value_min_size)
    render_redeemer_address(page, student.get("address", ""), value_x, map_point(0, REDEEMER_ADDRESS_BASELINE_Y).y, value_max_width, bold_fn, fn_bold, bold_obj, REDEEMER_BLACK, base_size=value_base_size, min_size=4.6 * min(scale_x, scale_y), max_lines=REDEEMER_ADDRESS_MAX_LINES, line_gap=REDEEMER_ADDRESS_LINE_GAP)

    session_value = clean_card_value(student.get("session", "")) or DEFAULT_SESSION
    session_rect = map_rect(REDEEMER_SESSION_VALUE_RECT)
    session_size = _fit_size(anton_obj, session_value, session_rect.width, REDEEMER_SESSION_FONT_SIZE * min(scale_x, scale_y), 5.6 * min(scale_x, scale_y))
    session_value = _ellipsize_to_width(anton_obj, session_value, session_rect.width, session_size)
    _put_single(page, session_rect, session_value, anton_fn, fn_anton, session_size, REDEEMER_BLACK, anton_obj)


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

def render_address(page, rect, addr, fontfile, fontname, font_obj, color, max_x=None):
    addr = clean_card_value(addr)   # ⬅ blank if HTML / placeholder
    if not addr: return
    words = addr.split()
    if not words: return
    max_w = (max_x if max_x is not None else rect.x1) - rect.x0
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

# ─────────────────────────────────────────────────────────────────
# CARD TRANSFORM HELPERS
# ─────────────────────────────────────────────────────────────────

def _make_card_transform(source_rect, target_rect):
    sx = target_rect.width / source_rect.width
    sy = target_rect.height / source_rect.height
    return {"src": source_rect, "dst": target_rect, "sx": sx, "sy": sy}


def _tr_point(tr, x, y):
    return fitz.Point(
        tr["dst"].x0 + (x - tr["src"].x0) * tr["sx"],
        tr["dst"].y0 + (y - tr["src"].y0) * tr["sy"],
    )


def _tr_rect(tr, coords):
    x0, y0, x1, y1 = coords
    p0 = _tr_point(tr, x0, y0)
    p1 = _tr_point(tr, x1, y1)
    return fitz.Rect(p0.x, p0.y, p1.x, p1.y)


def _tr_font_size(tr, size):
    return size * min(tr["sx"], tr["sy"])


def redraw_blood_teardrop_transformed(page, tr, fill_color):
    shape = page.new_shape()
    p = lambda t: _tr_point(tr, *t)
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


def draw_card_overlay_hebron(page, student: dict, tr):
    anton_obj, bold_obj, anton_fn, bold_fn, fn_anton, fn_bold = _ensure_fonts()
    if anton_obj is None or bold_obj is None:
        return

    shape = page.new_shape()
    def band_right_x(y):
        return -0.3952 * y + 172.6234
    pts = [
        _tr_point(tr, 0, BAND_Y0),
        _tr_point(tr, band_right_x(BAND_Y0), BAND_Y0),
        _tr_point(tr, band_right_x(BAND_Y1), BAND_Y1),
        _tr_point(tr, 0, BAND_Y1),
    ]
    shape.draw_polyline(pts)
    shape.draw_line(pts[-1], pts[0])
    shape.finish(color=BANNER_RED, fill=BANNER_RED, width=0)
    shape.commit(overlay=True)

    for coords in [FATHER_CLEAN_COORDS, MOTHER_CLEAN_COORDS, DOB_CLEAN_COORDS,
                   ADDRESS_CLEAN_COORDS, MOBILE_CLEAN_COORDS,
                   ADM_WHITEOUT_COORDS, SESSION_WHITEOUT_COORDS]:
        page.draw_rect(_tr_rect(tr, coords), color=WHITE, fill=WHITE, width=0, overlay=True)

    redraw_blood_teardrop_transformed(page, tr, BLOOD_RED)

    photo_bytes = fetch_photo_bytes(student.get("photo_url", ""))
    insert_image_safe(page, _tr_rect(tr, PHOTO_RECT_COORDS), photo_bytes)

    draw_text_vertically_centered(
        page, _tr_rect(tr, NAME_TEXT_RECT_COORDS),
        str(student.get("student_name", "")).strip().upper(),
        anton_fn, fn_anton, anton_obj, _tr_font_size(tr, NAME_FONT_SIZE), NAME_COLOR,
    )

    cls = str(student.get("class", "")).strip().upper()
    sec = str(student.get("section", "")).strip().upper()
    roll = str(student.get("roll", "")).strip()
    parts = []
    if cls:
        parts.append(f"CLASS:{cls}")
    if sec:
        parts.append(f"SEC:{sec}")
    if roll:
        parts.append(f"ROLL:{roll}")
    draw_text_vertically_centered(
        page, _tr_rect(tr, CLASS_TEXT_RECT_COORDS),
        "  ".join(parts),
        bold_fn, fn_bold, bold_obj, _tr_font_size(tr, CLASS_FONT_SIZE), NAME_COLOR,
    )

    for coords, key in [
        (FATHER_VALUE_RECT_COORDS, "father_name"),
        (MOTHER_VALUE_RECT_COORDS, "mother_name"),
        (MOBILE_VALUE_RECT_COORDS, "mobile"),
    ]:
        rect = _tr_rect(tr, coords)
        txt = clean_card_value(student.get(key, ""))
        if txt:
            sz = _fit_size(bold_obj, txt, rect.width, _tr_font_size(tr, VALUE_FONT_SIZE))
            _put_single(page, rect, txt, bold_fn, fn_bold, sz, VALUE_COLOR, bold_obj)

    dob = clean_card_value(student.get("dob", ""))
    if dob:
        rect = _tr_rect(tr, DOB_VALUE_RECT_COORDS)
        sz = _fit_size(bold_obj, dob, rect.width, _tr_font_size(tr, VALUE_FONT_SIZE))
        _put_single(page, rect, dob, bold_fn, fn_bold, sz, VALUE_COLOR, bold_obj)

    render_address(
        page, _tr_rect(tr, ADDRESS_VALUE_RECT_COORDS),
        student.get("address", ""),
        bold_fn, fn_bold, bold_obj, VALUE_COLOR,
    )

    adm = clean_card_value(student.get("adm_no", ""))
    if adm:
        rect = _tr_rect(tr, ADM_VALUE_RECT_COORDS)
        sz = _fit_size(bold_obj, adm, rect.width, _tr_font_size(tr, ADM_FONT_SIZE))
        _put_single(page, rect, adm, bold_fn, fn_bold, sz, VALUE_COLOR, bold_obj)

    sess = clean_card_value(student.get("session", "")) or DEFAULT_SESSION
    rect = _tr_rect(tr, SESSION_VALUE_RECT_COORDS)
    sz = _fit_size(anton_obj, sess, rect.width, _tr_font_size(tr, SESSION_FONT_SIZE))
    _put_single(page, rect, sess, anton_fn, fn_anton, sz, VALUE_COLOR, anton_obj)

    blood = str(student.get("blood_group", "")).strip().upper()
    if blood and blood.lower() not in {"nan", "none"} and any(c.isalpha() for c in blood):
        draw_text_centered_hv(
            page, _tr_rect(tr, BLOOD_VALUE_RECT_COORDS),
            blood, bold_fn, fn_bold, bold_obj, _tr_font_size(tr, BLOOD_FONT_SIZE), WHITE,
        )


def draw_card_overlay_redeemer(page, student: dict, tr):
    _draw_redeemer_overlay_core(
        page, student,
        lambda x, y: _tr_point(tr, x, y),
        lambda coords: _tr_rect(tr, coords),
        tr["sx"], tr["sy"],
    )


# ─────────────────────────────────────────────────────────────────
# PRIYANKA per-card renderer
# Coordinates taken directly from PRIYANKA_DREAMNEST.txt FIELDS map.
# Template page size: 141.75 x 240.75 pt.
# The template already has static label text (e.g. "Class:", "Sec:",
# "Father's Name:", etc.) — we only erase and rewrite the VALUES.
# ─────────────────────────────────────────────────────────────────
def _render_priyanka_card_bytes(student: dict, tmpl_bytes: bytes):
    doc = fitz.open("pdf", tmpl_bytes)
    page = doc[0]

    _, bold_obj, _, bold_fn, _, fn_bold = _ensure_fonts()
    if bold_obj is None:
        doc.close()
        return None

    # Colour from PRIYANKA_DREAMNEST.txt:  COLOR_DARK_BLUE = (15/255, 0/255, 106/255)
    PRIY_BLUE = (15/255, 0/255, 106/255)

    # ── Redact only the sample VALUE rectangles extracted from the template
    #    (from PRIYANKA_DREAMNEST.txt sample_value_rects).
    #    These are tight around the sample data so NO label text is wiped.
    sample_value_rects = [
        # name "AARAV SHARMA"       x0=8.13  y0=130.69 x1=90.08 y1=140.73
        ( 8.13, 130.69,  90.08, 140.73),
        # class "NURSERY"           x0=26.29 y0=141.29 x1=52.74 y1=148.32
        (26.29, 141.29,  52.74, 148.32),
        # sec "A"                   x0=70.81 y0=141.29 x1=74.84 y1=148.32
        (70.81, 141.29,  74.84, 148.32),
        # roll "10"                 x0=96.53 y0=141.29 x1=103.41 y1=148.32
        (96.53, 141.29, 103.41, 148.32),
        # session "2026-27"         x0=109.16 y0=110.50 x1=128.80 y1=117.11
        (109.16, 110.50, 128.80, 117.11),

        # father "SUYASH SHARMA"    x0=56.76 y0=155.18 x1=109.17 y1=161.88
        (56.76, 155.18, 109.17, 161.88),
        # mother "POOJA SHARMA"     x0=56.76 y0=162.73 x1=105.50 y1=169.42
        (56.76, 162.73, 105.50, 169.42),
        # dob "21-04-2014"          x0=56.76 y0=170.27 x1=87.42 y1=176.97
        (56.76, 170.27,  87.42, 176.97),
        # addr1 "BHARKO,"           x0=56.76 y0=178.56 x1=84.73 y1=185.26
        (56.76, 178.56,  84.73, 185.26),
        # addr2 "AMARPUR, BANKA"    x0=56.76 y0=186.56 x1=112.37 y1=193.25
        (56.76, 186.56, 112.37, 193.25),
        # contact "1234567890"      x0=56.76 y0=194.35 x1=90.09 y1=201.05
        (56.76, 194.35,  90.09, 201.05),
    ]
    for x0, y0, x1, y1 in sample_value_rects:
        page.add_redact_annot(fitz.Rect(x0 - 0.3, y0 - 0.3, x1 + 0.3, y1 + 0.3), fill=None)
    page.apply_redactions(images=fitz.PDF_REDACT_IMAGE_NONE)

    # ── Photo — from PRIYANKA_DREAMNEST.txt PHOTO_BOX
    # PHOTO_BOX = {"x": 46.34, "y": 57.75, "w": 49.92, "h": 63.95}
    # PAD = 2.2 pt inner padding so photo stays inside the pink rounded container
    PAD = 2.2
    BOX_X, BOX_Y, BOX_W, BOX_H = 46.34, 57.75, 49.92, 63.95
    # Step 1: Erase the template's embedded placeholder photo with a white fill
    photo_erase_rect = fitz.Rect(BOX_X - 0.5, BOX_Y - 0.5, BOX_X + BOX_W + 0.5, BOX_Y + BOX_H + 0.5)
    page.add_redact_annot(photo_erase_rect, fill=(1.0, 1.0, 1.0))
    page.apply_redactions(images=fitz.PDF_REDACT_IMAGE_PIXELS)

    # Step 2: Fetch and insert student photo with rounded corners (from PRIYANKA_DREAMNEST.txt)
    # The template's own pink rounded rectangle border is preserved — we do NOT draw any border.
    # Code exactly mirrors PRIYANKA_DREAMNEST.txt render_id_card() Step 2:
    #   photo_fit = fit_photo_to_box(photo, int(box_w * 8), int(box_h * 8))
    #   photo_fit = apply_rounded_corners(photo_fit, radius=int(box_w * 8 * 0.08))
    box_inner_w = BOX_W - 2 * PAD
    box_inner_h = BOX_H - 2 * PAD
    photo_inner_rect = fitz.Rect(BOX_X + PAD, BOX_Y + PAD,
                                  BOX_X + PAD + box_inner_w, BOX_Y + PAD + box_inner_h)
    photo_bytes = fetch_photo_bytes(student.get("photo_url", ""))
    if photo_bytes and HAS_PIL:
        try:
            scale = 8
            target_w = max(1, int(box_inner_w * scale))
            target_h = max(1, int(box_inner_h * scale))
            with Image.open(io.BytesIO(photo_bytes)) as _img:
                _rgb = _img.convert("RGB")
                src_w, src_h = _rgb.size
                target_ratio = target_w / target_h
                src_ratio = src_w / src_h
                if src_ratio > target_ratio:
                    new_w = int(src_h * target_ratio)
                    left = (src_w - new_w) // 2
                    _rgb = _rgb.crop((left, 0, left + new_w, src_h))
                elif src_ratio < target_ratio:
                    new_h = int(src_w / target_ratio)
                    top = (src_h - new_h) // 2
                    _rgb = _rgb.crop((0, top, src_w, top + new_h))
                _resized = _rgb.resize((target_w, target_h), Image.Resampling.LANCZOS)
                _rgb.close()
            # apply_rounded_corners: radius = int(box_inner_w * scale * 0.08)
            _radius = int(box_inner_w * scale * 0.08)
            _rgba = _resized.convert("RGBA")
            _resized.close()
            _mask = Image.new("L", (target_w, target_h), 0)
            from PIL import ImageDraw as _IDraw
            _md = _IDraw.Draw(_mask)
            _md.rounded_rectangle((0, 0, target_w, target_h), radius=_radius, fill=255)
            _rgba.putalpha(_mask)
            _buf = io.BytesIO()
            _rgba.save(_buf, format="PNG")
            _rgba.close()
            page.insert_image(photo_inner_rect, stream=_buf.getvalue(),
                              keep_proportion=False, overlay=True)
        except Exception:
            insert_image_safe(page, photo_inner_rect, photo_bytes)
    else:
        insert_image_safe(page, photo_inner_rect, photo_bytes)
    # NOTE: NO border drawn here — the template PDF already has the pink rounded
    #       rectangle as a vector drawing that shows around the padded photo area.

    # ── Text helper — uses PRIYANKA_DREAMNEST.txt field "x" and "y" (baseline)
    def put(text, x, baseline_y, max_width, sz=6.0, min_sz=3.5):
        val = clean_card_value(str(text) if text else "")
        if not val:
            return
        fs = _fit_size(bold_obj, val, max_width, sz, min_sz)
        page.insert_text((x, baseline_y), val,
                         fontname=fn_bold, fontfile=bold_fn,
                         fontsize=fs, color=PRIY_BLUE, overlay=True)

    # ── Write fields using exact FIELDS map from PRIYANKA_DREAMNEST.txt:
    #
    # "name":    x=8.13   y=138.6  max_width=100  size=8.99
    name = clean_card_value(student.get("student_name", "")).upper()
    if name:
        sz = _fit_size(bold_obj, name, 100.0, 8.99, 4.0)
        page.insert_text((8.13, 138.6), name,
                         fontname=fn_bold, fontfile=bold_fn,
                         fontsize=sz, color=PRIY_BLUE, overlay=True)

    # "class":   x=26.29  y=146.8  max_width=30   size=6.0
    put(student.get("class", "").upper(),   26.29, 146.8, 30.0, 6.0)

    # "sec":     x=70.81  y=146.8  max_width=12   size=6.0
    put(student.get("section", "").upper(), 70.81, 146.8, 12.0, 6.0)

    # "roll":    x=96.53  y=146.8  max_width=18   size=6.0
    put(student.get("roll", ""),            96.53, 146.8, 18.0, 6.0)

    # "session": x=109.16 y=115.5  max_width=28   size=6.0  font=anton
    sess = clean_card_value(student.get("session", "")) or DEFAULT_SESSION
    sz = _fit_size(bold_obj, sess, 28.0, 6.0, 3.5)
    page.insert_text((109.16, 115.5), sess,
                     fontname=fn_bold, fontfile=bold_fn,
                     fontsize=sz, color=PRIY_BLUE, overlay=True)

    # "father":  x=56.76  y=160.4  max_width=80   size=6.0
    put(student.get("father_name", ""),  56.76, 160.4, 80.0, 6.0)

    # "mother":  x=56.76  y=168.4  max_width=80   size=6.0
    put(student.get("mother_name", ""),  56.76, 168.4, 80.0, 6.0)

    # "dob":     x=56.76  y=176.4  max_width=80   size=6.0
    put(student.get("dob", ""),          56.76, 176.4, 80.0, 6.0)

    # "address": x=56.76  y=184.4  max_width=80   size=6.0  multiline  line_height=7.5  max_lines=2
    addr = clean_card_value(student.get("address", ""))
    if addr:
        words = addr.split()
        lines = _addr_wrap_at_size(bold_obj, words, 80.0, 6.0)[:2]
        for i, line in enumerate(lines):
            page.insert_text((56.76, 184.4 + i * 7.5), line,
                             fontname=fn_bold, fontfile=bold_fn,
                             fontsize=6.0, color=PRIY_BLUE, overlay=True)

    # "contact": x=56.76  y=200.0  max_width=80   size=6.0
    put(student.get("mobile", ""),       56.76, 200.0, 80.0, 6.0)

    buf = io.BytesIO()
    doc.save(buf, garbage=4, deflate=True, incremental=False)
    doc.close()
    return buf.getvalue()


# ─────────────────────────────────────────────────────────────────
# AB ASCENT per-card renderer
# Coordinates taken directly from AB_ASCENT.txt PLACEHOLDERS map.
# The template PDF already contains all static label text
# (e.g. "Adm No.", "Class:", "Sec:", "Roll:", "FATHER'S NAME :" …).
# We ONLY erase and rewrite the dynamic VALUE portions.
# ─────────────────────────────────────────────────────────────────
def _render_ab_ascent_card_bytes(student: dict, tmpl_bytes: bytes):
    doc = fitz.open("pdf", tmpl_bytes)
    page = doc[0]

    _, bold_obj, _, bold_fn, _, fn_bold = _ensure_fonts()
    if bold_obj is None:
        doc.close()
        return None

    # ── Colours (from AB_ASCENT.txt PLACEHOLDERS color_int values)
    def h(c): return ((c>>16)&0xFF)/255, ((c>>8)&0xFF)/255, (c&0xFF)/255
    NAVY    = h(0x224499)   # all data fields
    RED     = h(0xC83030)   # student name
    WHITE_C = (1.0, 1.0, 1.0)  # blood group text
    BLACK   = (0.0, 0.0, 0.0)  # bus route

    # ── Redact zones — exactly covering the value bbox + max_x from
    #    AB_ASCENT.txt PLACEHOLDERS.  y0 uses the original y0 from the
    #    PLACEHOLDER bbox; y1 uses the original y1 + a small pad.
    #    We do NOT touch label areas (e.g. "Adm No.", "Class:", etc.).
    redact_zones = [
        # session     bbox y0=106.44 y1=117.44  max_x=148
        # ✅ FIX: label "Session:" occupies y=100.2–107.5; start redact at 107.5 to protect it
        (109.15, 107.50, 148.0,  118.50),
        # adm_no      bbox y0=106.44 y1=117.44  max_x=50
        # ✅ FIX: label "Adm No :" occupies y=100.2–107.5; start redact at 107.5 to protect it
        ( 25.07, 107.50,  50.0,  118.50),
        # name        bbox y0=127.58 y1=137.63  max_x=140
        ( 17.73, 126.60, 140.0,  138.50),
        # class_      bbox y0=138.40 y1=145.43  max_x=58
        ( 26.46, 137.50,  58.0,  146.30),
        # section     bbox y0=138.40 y1=145.43  max_x=84
        ( 73.90, 137.50,  84.0,  146.30),
        # roll        bbox y0=138.40 y1=145.43  max_x=115
        (100.07, 137.50, 115.0,  146.30),
        # father_name bbox y0=154.44 y1=161.14  max_x=150
        ( 60.74, 153.50, 150.0,  162.00),
        # mother_name bbox y0=161.99 y1=168.69  max_x=150
        ( 60.74, 161.00, 150.0,  169.50),
        # dob         bbox y0=169.25 y1=175.95  max_x=150
        ( 60.74, 168.30, 150.0,  176.80),
        # addr1       bbox y0=176.52 y1=183.22  max_x=150
        ( 60.74, 175.60, 150.0,  184.10),
        # addr2       bbox y0=184.51 y1=191.21  max_x=150
        ( 60.74, 183.60, 150.0,  192.00),
        # mobile      bbox y0=192.06 y1=198.76  max_x=150
        ( 60.74, 191.10, 150.0,  199.60),
        # bus_route   bbox y0=204.56 y1=210.70  max_x=65
        ( 29.21, 203.60,  65.0,  211.60),
        # blood_group bbox y0=85.52  y1=93.34   max_x=125.56
        (116.03,  84.50, 125.56,  94.00),
    ]
    for x0, y0, x1, y1 in redact_zones:
        page.add_redact_annot(fitz.Rect(x0, y0, x1, y1), fill=None)
    page.apply_redactions(images=fitz.PDF_REDACT_IMAGE_NONE)

    # ── Photo (from AB_ASCENT.txt: PHOTO_RECT and draw_photo_border)
    # PHOTO_RECT = (52.93, 63.01, 100.07, 116.96)  x0,y0,x1,y1  (47.1 × 54.0 pt)
    # PHOTO_BORDER_COLOR = (0.08, 0.31, 0.86)  blue
    # PHOTO_BORDER_WIDTH = 1.5 pt
    PHOTO = (52.93, 63.01, 100.07, 116.96)
    AB_BORDER_COLOR = (0.08, 0.31, 0.86)  # blue — exact value from AB_ASCENT.txt
    AB_BORDER_WIDTH = 1.5                 # pt — exact value from AB_ASCENT.txt

    # Step 1: Erase template placeholder photo (use transparent fill to avoid white frame)
    photo_erase_rect = fitz.Rect(PHOTO[0] - 0.5, PHOTO[1] - 0.5, PHOTO[2] + 0.5, PHOTO[3] + 0.5)
    page.add_redact_annot(photo_erase_rect, fill=None)   # transparent — no white bleed
    page.apply_redactions(images=fitz.PDF_REDACT_IMAGE_PIXELS)

    # Step 2: Insert student photo
    photo_bytes = fetch_photo_bytes(student.get("photo_url", ""))
    insert_image_safe(page, fitz.Rect(*PHOTO), photo_bytes)

    # Step 3: Draw blue border ON TOP of photo exactly as AB_ASCENT.txt draw_photo_border()
    #   shape.draw_rect(rect); shape.finish(color=color, fill=None, width=width, closePath=True)
    _ab_shape = page.new_shape()
    _ab_shape.draw_rect(fitz.Rect(*PHOTO))
    _ab_shape.finish(color=AB_BORDER_COLOR, fill=None, width=AB_BORDER_WIDTH, closePath=True)
    _ab_shape.commit(overlay=True)

    # ── Text insertion helper — mirrors fit_text_to_box() from AB_ASCENT.txt
    #    x0, y1 = original bbox coords from PLACEHOLDERS; max_x = max_x column.
    #    baseline = y1 - 0.22 * size  (same formula as original)
    def put(text, x0, y1_bbox, color, max_x, sz=6.0, align="left"):
        val = clean_card_value(str(text) if text else "")
        if not val:
            return
        available_w = max_x - x0
        fs = _fit_size(bold_obj, val, available_w, sz, 3.0)
        tw = bold_obj.text_length(val, fontsize=fs)
        if align == "center":
            x = x0 + (available_w - tw) / 2.0
        else:
            x = x0
        baseline = y1_bbox - 0.22 * sz
        page.insert_text((x, baseline), val,
                         fontname=fn_bold, fontfile=bold_fn,
                         fontsize=fs, color=color, overlay=True)

    # ── Now write every field using EXACTLY the same bbox coords and
    #    parameters as the PLACEHOLDERS table in AB_ASCENT.txt:
    #
    #  ("session",     "2026-27",        (109.15,106.44,133.71,117.44), "hebo",7.5,0x224499,"left",148.0)
    put(student.get("session", "") or DEFAULT_SESSION,
        109.15, 117.44, NAVY, 148.0, sz=7.5)

    #  ("adm_no",      "1678",           ( 25.07,106.44, 38.66,117.44), "hebo",7.5,0x224499,"left", 50.0)
    #  Label "Adm No." starts at x=18.5 (from static_zones). Align value to same x.
    put(student.get("adm_no", ""),
        18.5, 117.44, NAVY, 50.0, sz=7.5)

    #  ("name",        "AARAV SHARMA",   ( 17.73,127.58, 99.71,137.63), "hebo",9.0,0xC83030,"left",140.0)
    put(student.get("student_name", "").upper(),
        17.73, 137.63, RED, 140.0, sz=9.0)

    #  ("class_",      "VI",             ( 26.46,138.40, 32.14,145.43), "hebo",6.0,0x224499,"left", 58.0)
    put(student.get("class", "").upper(),
        26.46, 145.43, NAVY, 58.0, sz=6.0)

    #  ("section",     "A",              ( 73.90,138.40, 77.93,145.43), "hebo",6.0,0x224499,"left", 84.0)
    put(student.get("section", "").upper(),
        73.90, 145.43, NAVY, 84.0, sz=6.0)

    #  ("roll",        "21",             (100.07,138.40,106.95,145.43), "hebo",6.0,0x224499,"left",115.0)
    put(student.get("roll", ""),
        100.07, 145.43, NAVY, 115.0, sz=6.0)

    #  ("father_name", "SUYASH SHARMA",  ( 60.74,154.44,113.16,161.14), "hebo",6.0,0x224499,"left",150.0)
    put(student.get("father_name", ""),
        60.74, 161.14, NAVY, 150.0, sz=6.0)

    #  ("mother_name", "POOJA SHARMA",   ( 60.74,161.99,109.49,168.69), "hebo",6.0,0x224499,"left",150.0)
    put(student.get("mother_name", ""),
        60.74, 168.69, NAVY, 150.0, sz=6.0)

    #  ("dob",         "21-04-2014",     ( 60.74,169.25, 91.41,175.95), "hebo",6.0,0x224499,"left",150.0)
    put(student.get("dob", ""),
        60.74, 175.95, NAVY, 150.0, sz=6.0)

    #  ("mobile",      "1234567890",     ( 60.74,192.06, 94.08,198.76), "hebo",6.0,0x224499,"left",150.0)
    put(student.get("mobile", ""),
        60.74, 198.76, NAVY, 150.0, sz=6.0)

    #  ("blood_group", "O+",             (116.03,85.52,125.56,93.34),   "hebo",7.0,0xFFFFFF,"center",125.56)
    blood = clean_card_value(student.get("blood_group", "")).upper()
    if blood and any(c.isalpha() for c in blood):
        put(blood, 116.03, 93.34, WHITE_C, 125.56, sz=7.0, align="center")

    #  ("bus_route",   "BUS 1",          ( 29.21,204.56, 45.40,210.70), "hebo",5.5,0x000000,"left", 65.0)
    bus = clean_card_value(student.get("bus_route", ""))
    if bus:
        put(bus, 29.21, 210.70, BLACK, 65.0, sz=5.5)

    # ── ADDRESS: split into addr1 / addr2
    #  ("addr1",       "BHARKO,",        ( 60.74,176.52, 88.72,183.22), "hebo",6.0,0x224499,"left",150.0)
    #  ("addr2",       "AMARPUR, BANKA", ( 60.74,184.51,116.37,191.21), "hebo",6.0,0x224499,"left",150.0)
    addr = clean_card_value(student.get("address", ""))
    if addr:
        if "," in addr:
            addr1, addr2 = addr.split(",", 1)
            addr1 = addr1.strip() + ","
            addr2 = addr2.strip()
        else:
            words = addr.split()
            mid = max(1, len(words) // 2)
            addr1 = " ".join(words[:mid])
            addr2 = " ".join(words[mid:])
        put(addr1, 60.74, 183.22, NAVY, 150.0, sz=6.0)
        if addr2:
            put(addr2, 60.74, 191.21, NAVY, 150.0, sz=6.0)

    buf = io.BytesIO()
    doc.save(buf, garbage=4, deflate=True, incremental=False)
    doc.close()
    return buf.getvalue()


def draw_card_on_page(page, student, target_rect, template_key, template_doc, template_source_rect):
    page.show_pdf_page(target_rect, template_doc, 0, keep_proportion=False, overlay=True)
    tr = _make_card_transform(template_source_rect, target_rect)
    if template_key == "redeemer":
        draw_card_overlay_redeemer(page, student, tr)
    else:
        draw_card_overlay_hebron(page, student, tr)

# ─────────────────────────────────────────────────────────────────
# SERIAL BADGE
# ─────────────────────────────────────────────────────────────────
def draw_serial_badge_vector(page, serial: int, cx: float, cy: float, gap_h: float):
    txt    = f"#{serial}"
    fs     = max(5.0, gap_h * 0.38)
    try:
        font = fitz.Font("helv")
        tw   = font.text_length(txt, fontsize=fs)
    except Exception:
        tw = len(txt) * fs * 0.6

    pad_x  = fs * 0.5
    pad_y  = fs * 0.25
    bw     = tw + 2 * pad_x
    bh     = fs + 2 * pad_y

    left   = cx - bw / 2.0
    top    = cy - bh / 2.0
    right  = left + bw
    bottom = top  + bh

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

    baseline = cy + fs * 0.35
    page.insert_text(
        (left + pad_x, baseline), txt,
        fontname="helv", fontsize=fs, color=WHITE, overlay=True,
    )

# ─────────────────────────────────────────────────────────────────
# PT constants
# ─────────────────────────────────────────────────────────────────

def mm_to_pt(mm: float) -> float:
    return mm * MM_TO_PT

CARD_W_PT  = mm_to_pt(CARD_W_MM)
CARD_H_PT  = mm_to_pt(CARD_H_MM)
A4_W_PT    = mm_to_pt(A4_W_MM)
A4_H_PT    = mm_to_pt(A4_H_MM)
OX_PT      = mm_to_pt(OFFSET_X_MM)
OY_PT      = mm_to_pt(OFFSET_Y_MM)
ROW_GAP_PT = mm_to_pt(ROW_GAP_MM)
COL_GAP_PT = mm_to_pt(1.0)
COL_STEP   = CARD_W_PT + COL_GAP_PT
ROW_STEP   = CARD_H_PT + ROW_GAP_PT

# ─────────────────────────────────────────────────────────────────
# A4 SHEET BUILDER  — parallelised per-card render for priyanka/ab_ascent
# ─────────────────────────────────────────────────────────────────

def _check_tmp_space_mb(path: str, needed_mb: float = 20.0) -> bool:
    """Return True if the directory has at least needed_mb of free space."""
    try:
        import shutil
        free = shutil.disk_usage(path).free / (1024 * 1024)
        return free >= needed_mb
    except Exception:
        return True  # assume ok if we can't check


def _resolve_pdf_tmp_dir() -> str:
    """Pick a writable temp dir with enough space. Falls back through candidates."""
    candidates = [PDF_TEMP_DIR, "/tmp", tempfile.gettempdir()]
    for d in candidates:
        try:
            os.makedirs(d, exist_ok=True)
            if _check_tmp_space_mb(d, needed_mb=10.0):
                # Verify we can actually write there
                t = tempfile.NamedTemporaryFile(delete=True, dir=d, suffix=".pdf")
                t.close()
                return d
        except Exception:
            continue
    return tempfile.gettempdir()  # last resort


def build_pdf_file_vector(students: list, template_key: str = DEFAULT_TEMPLATE):
    if not HAS_FITZ:
        return None
    template_key = normalize_template_key(template_key)
    tmpl_bytes = _ensure_template(template_key)
    if tmpl_bytes is None:
        return None

    template_doc = _get_template_doc(template_key)
    if template_doc is None:
        return None

    source_rect = fitz.Rect(template_doc[0].rect)
    n_pages = (len(students) + CARDS_PER_PAGE - 1) // CARDS_PER_PAGE
    # ✅ FIX: Pick a writable temp dir with enough free space
    tmp_dir = _resolve_pdf_tmp_dir()
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf", dir=tmp_dir)
    tmp.close()
    out_path = tmp.name
    # Aggressive GC: every 5 pages in production, every 25 locally
    GC_EVERY_PAGES = 5 if _IS_PRODUCTION else 25

    use_per_card = template_key in ("priyanka", "ab_ascent")
    render_fn = _render_priyanka_card_bytes if template_key == "priyanka" else _render_ab_ascent_card_bytes

    # 🚀 Per-card pre-render: parallel locally, serial in production to save RAM
    prerendered = None
    if use_per_card:
        prerendered = [None] * len(students)
        workers = min(CARD_RENDER_WORKERS, max(1, len(students)))
        if workers > 1:
            with ThreadPoolExecutor(max_workers=workers) as pool:
                future_to_idx = {
                    pool.submit(render_fn, students[i], tmpl_bytes): i
                    for i in range(len(students))
                }
                for f in as_completed(future_to_idx):
                    idx = future_to_idx[f]
                    try:
                        prerendered[idx] = f.result()
                    except Exception:
                        prerendered[idx] = None
        else:
            # Serial render — minimal peak RAM
            for i, s in enumerate(students):
                try:
                    prerendered[i] = render_fn(s, tmpl_bytes)
                except Exception:
                    prerendered[i] = None

    out_doc = fitz.open()
    try:
        for page_idx in range(n_pages):
            student_start = page_idx * CARDS_PER_PAGE
            student_batch = students[student_start: student_start + CARDS_PER_PAGE]

            a4_page = out_doc.new_page(width=A4_W_PT, height=A4_H_PT)

            for idx, student in enumerate(student_batch):
                col = idx % COLS
                row = idx // COLS
                card_x = OX_PT + col * COL_STEP
                card_y = OY_PT + row * ROW_STEP
                target_rect = fitz.Rect(card_x, card_y, card_x + CARD_W_PT, card_y + CARD_H_PT)

                if use_per_card:
                    card_bytes = prerendered[student_start + idx]
                    if card_bytes:
                        card_doc = fitz.open("pdf", card_bytes)
                        a4_page.show_pdf_page(target_rect, card_doc, 0,
                                              keep_proportion=False, overlay=True)
                        card_doc.close()
                        # ✅ FIX: free per-card bytes immediately after use to save RAM
                        prerendered[student_start + idx] = None
                else:
                    draw_card_on_page(
                        a4_page, student, target_rect, template_key,
                        template_doc=template_doc, template_source_rect=source_rect,
                    )

                # Draw serial badge for EVERY card in the gap between rows.
                # For row 0: badge is in the gap below (between row 0 and row 1).
                # For row 1: badge is in the gap above (between row 0 and row 1) — same y.
                # Both produce the same physical gap_cy so each card column gets one badge.
                if ROWS > 1:
                    if row == 0:
                        # Gap below row 0
                        gap_cy = card_y + CARD_H_PT + ROW_GAP_PT / 2.0
                    else:
                        # Gap above row 1 (= gap below row 0)
                        gap_cy = card_y - ROW_GAP_PT / 2.0
                    badge_cx = card_x + CARD_W_PT / 2.0
                    draw_serial_badge_vector(
                        a4_page, student_start + idx + 1,
                        badge_cx, gap_cy, ROW_GAP_PT,
                    )

            if (page_idx + 1) % GC_EVERY_PAGES == 0:
                gc.collect()

        # ✅ FIX: save with incremental=False so MuPDF doesn't need extra tmp space
        out_doc.save(
            out_path,
            deflate=True, deflate_images=True, deflate_fonts=True,
            garbage=4, clean=True, linear=False, incremental=False,
        )
        return out_path

    except Exception:
        try:
            if os.path.exists(out_path):
                os.unlink(out_path)
        except Exception:
            pass
        raise
    finally:
        out_doc.close()
        # Free per-card byte buffers ASAP
        if prerendered:
            prerendered.clear()
        gc.collect()

# ─────────────────────────────────────────────────────────────────
# RASTER FALLBACK (used only if template PDF is missing)
# ─────────────────────────────────────────────────────────────────

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
    a4_w_px  = mm2px(A4_W_MM); a4_h_px   = mm2px(A4_H_MM)
    card_w_px= mm2px(CARD_W_MM); card_h_px = mm2px(CARD_H_MM)
    ox_px    = mm2px(OFFSET_X_MM); oy_px    = mm2px(OFFSET_Y_MM)
    gap_px   = mm2px(ROW_GAP_MM); col_gap_px= mm2px(1.0)
    a4_w_pt  = A4_W_MM * MM_TO_PT; a4_h_pt  = A4_H_MM * MM_TO_PT

    out_doc  = fitz.open()
    _tmp_dir = _resolve_pdf_tmp_dir()
    tmp      = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf", dir=_tmp_dir)
    tmp.close()
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
            sheet.save(buf, format="JPEG", quality=80, optimize=True)
            sheet.close()
            pg = out_doc.new_page(width=a4_w_pt, height=a4_h_pt)
            pg.insert_image(fitz.Rect(0,0,a4_w_pt,a4_h_pt), stream=buf.getvalue(), overlay=True, keep_proportion=False)
            gc.collect()
        out_doc.save(tmp.name, deflate=True, garbage=4, clean=True)
        return tmp.name
    except Exception:
        try:
            if os.path.exists(tmp.name): os.unlink(tmp.name)
        except: pass
        raise
    finally:
        out_doc.close()
        gc.collect()

# ─────────────────────────────────────────────────────────────────
def build_pdf_file(students, dpi=150, template_key: str = DEFAULT_TEMPLATE):
    template = get_template_config(template_key)
    if HAS_FITZ and template["pdf"].exists():
        return build_pdf_file_vector(students, template_key=template["key"])
    return build_pdf_file_raster_fallback(students, dpi=dpi)

# ─────────────────────────────────────────────────────────────────
def send_generated_pdf(students, dpi, download_name, as_attachment, allow_external=False, template_key: str = DEFAULT_TEMPLATE):
    if not students:
        return jsonify({"error": "No students loaded"}), 400
    if len(students) > MAX_STUDENTS_PER_REQUEST:
        return jsonify({
            "error": (
                f"Too many students in one request ({len(students)}). "
                f"Please filter by class or increase MAX_STUDENTS_PER_REQUEST."
            )
        }), 413

    if (not as_attachment) and len(students) >= PREVIEW_EXTERNAL_THRESHOLD and _external_storage_enabled():
        allow_external = True

    prefetch_photos(students)
    pdf_path = build_pdf_file(students, dpi=dpi, template_key=template_key)
    if not pdf_path:
        return jsonify({"error": "PDF generation failed — check server libs"}), 500

    @after_this_request
    def cleanup(response):
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
        except Exception:
            pass

    return send_file(
        pdf_path,
        mimetype="application/pdf",
        as_attachment=as_attachment,
        download_name=download_name,
        conditional=True,
        max_age=0,
    )

# ─────────────────────────────────────────────────────────────────
# TEMPLATE API
# ─────────────────────────────────────────────────────────────────

TEMPLATE_BRAND_COLORS = {
    "hebron":    "#DC2626",
    "redeemer":  "#4F46E5",
    "priyanka":  "#0F006A",
    "ab_ascent": "#224499",
}

@app.route("/api/templates", methods=["GET"])
@app.route("/templates", methods=["GET"])
def get_templates():
    payload = []
    for key, template in TEMPLATE_CONFIGS.items():
        payload.append({
            "key": key,
            "label": template["label"],
            "display_name": template["display_name"],
            "description": template["description"],
            "fields": template["fields"],
            "color": TEMPLATE_BRAND_COLORS.get(key, "#4F46E5"),
            "preview_url": f"/api/templates/{key}/preview.png",
        })
    return jsonify(payload)


@app.route("/api/templates/<template_key>/preview.png", methods=["GET"])
@app.route("/templates/<template_key>/preview.png", methods=["GET"])
def get_template_preview(template_key):
    png_bytes = _get_template_preview_png(template_key)
    if not png_bytes:
        return jsonify({"error": "Template preview unavailable"}), 404
    return send_file(io.BytesIO(png_bytes), mimetype="image/png", download_name=f"{normalize_template_key(template_key)}_preview.png")


def _request_template_key():
    raw = request.args.get("template", DEFAULT_TEMPLATE)
    key = str(raw or DEFAULT_TEMPLATE).strip().lower()
    if key not in TEMPLATE_CONFIGS:
        return None, jsonify({"error": f"Unknown template: {raw}"}), 400
    return key, None, None

# ─────────────────────────────────────────────────────────────────
# PDF / PREVIEW ENDPOINTS
# ─────────────────────────────────────────────────────────────────

@app.route("/api/preview/all", methods=["GET"])
@app.route("/preview/all", methods=["GET"])
def preview_all():
    template_key, err_resp, err_code = _request_template_key()
    if err_resp:
        return err_resp, err_code
    students = _store["students"]
    if not students:
        return jsonify({"error": "No students loaded"}), 400
    cls      = request.args.get("class","").strip().upper()
    students = filter_students_by_class(students, cls)
    return send_generated_pdf(students, dpi=PREVIEW_DPI,
                              download_name=f"preview_{template_key}.pdf", as_attachment=False,
                              template_key=template_key)

@app.route("/api/download/all", methods=["GET"])
@app.route("/download/all", methods=["GET"])
def download_all():
    template_key, err_resp, err_code = _request_template_key()
    if err_resp:
        return err_resp, err_code
    students = _store["students"]
    if not students:
        return jsonify({"error": "No students loaded"}), 400
    cls = request.args.get("class","").strip().upper()
    if cls:
        students = filter_students_by_class(students, cls)
        fname    = f"ids_{template_key}_{cls}.pdf"
    else:
        students = list(students)
        fname    = f"ids_{template_key}_ALL.pdf"
    return send_generated_pdf(students, dpi=DOWNLOAD_DPI,
                              download_name=fname, as_attachment=True, allow_external=True,
                              template_key=template_key)

@app.route("/api/preview/student", methods=["GET"])
@app.route("/preview/student", methods=["GET"])
def preview_student():
    template_key, err_resp, err_code = _request_template_key()
    if err_resp:
        return err_resp, err_code
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
                              download_name=f"preview_student_{template_key}.pdf", as_attachment=False,
                              template_key=template_key)

@app.route("/api/download/student", methods=["GET"])
@app.route("/download/student", methods=["GET"])
def download_student():
    template_key, err_resp, err_code = _request_template_key()
    if err_resp:
        return err_resp, err_code
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
                              download_name=f"id_{template_key}_{safe_name}.pdf", as_attachment=True, allow_external=True,
                              template_key=template_key)

# ─────────────────────────────────────────────────────────────────
def _startup_log():
    ck = chr(0x2713); xk = chr(0x2717)
    print("=" * 62)
    print("  ID Card Generator  v2.4  (fast / vector-native)")
    print(f"  Mode          : {'PRODUCTION (512MB/0.5CPU)' if _IS_PRODUCTION else 'LOCAL (full performance)'}")
    print(f"  Hebron PDF    : {ck+' found' if TEMPLATE_PDF_HEBRON.exists() else xk+' NOT FOUND'}")
    print(f"  Redeemer PDF  : {ck+' found' if TEMPLATE_PDF_REDEEMER.exists() else xk+' NOT FOUND'}")
    print(f"  Priyanka PDF  : {ck+' found' if TEMPLATE_PDF_PRIYANKA.exists() else xk+' NOT FOUND'}")
    print(f"  Ab Ascent PDF : {ck+' found' if TEMPLATE_PDF_AB_ASCENT.exists() else xk+' NOT FOUND'}")
    print(f"  PyMuPDF       : {ck if HAS_FITZ else xk}")
    print(f"  Pillow        : {ck if HAS_PIL  else xk}")
    print(f"  Temp dir      : {PDF_TEMP_DIR}")
    print(f"  Photo prefetch: {PREFETCH_WORKERS} threads | timeout {PHOTO_TIMEOUT}")
    print(f"  Card render   : {CARD_RENDER_WORKERS} thread(s)")
    print(f"  Photo quality : {PHOTO_PX}px @ JPEG q={PHOTO_JPEG_QUALITY}")
    print(f"  Photo cache   : max {MAX_CACHED_PHOTOS} entries")
    print("=" * 62)

_startup_log()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    # threaded=True → multiple in-flight requests don't block each other
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
