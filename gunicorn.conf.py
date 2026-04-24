# gunicorn.conf.py  — place this next to app.py and run:
#   gunicorn -c gunicorn.conf.py app:app
##
# FIX v2.2: The root cause of "server error" for 70+ students is that
# Gunicorn's default 30-second worker timeout kills the PDF generation
# worker mid-flight.  600 students at 0.5 CPU takes ~60-120 seconds.
# These settings keep the worker alive for the full generation.

import multiprocessing
import os

# ── Worker type ───────────────────────────────────────────────────
# "gthread" shares one process across N threads — much lower RSS than
# "gevent" and plays well with PyMuPDF (which is not async-safe).
worker_class = "gthread"
threads      = int(os.environ.get("GUNICORN_THREADS", "2"))

# 1 worker on free tier (512 MB RAM).  Each gthread worker uses ~120 MB
# at peak for 600 students.  2 workers would need ~240 MB overhead.
workers = int(os.environ.get("GUNICORN_WORKERS", "1"))

# ── Timeouts ──────────────────────────────────────────────────────
# 300 s = 5 minutes.  600 students at 0.5 CPU takes ~90-150 s worst case.
# The keepalive bump stops Nginx/Render from closing the upstream connection.
timeout          = int(os.environ.get("GUNICORN_TIMEOUT",    "300"))
keepalive        = int(os.environ.get("GUNICORN_KEEPALIVE",  "5"))
graceful_timeout = int(os.environ.get("GUNICORN_GRACEFUL",   "30"))

# ── Binding ───────────────────────────────────────────────────────
bind = f"0.0.0.0:{os.environ.get('PORT', '5000')}"

# ── Logging ───────────────────────────────────────────────────────
accesslog  = "-"
errorlog   = "-"
loglevel   = os.environ.get("GUNICORN_LOG_LEVEL", "info")

# ── Max request size (must match Flask MAX_CONTENT_LENGTH) ────────
# This prevents Gunicorn from rejecting large Excel uploads before Flask sees them.
limit_request_line  = 8190
limit_request_field_size = 8190
