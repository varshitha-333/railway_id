# gunicorn.conf.py — Railway-optimised for 0.5 vCPU / 512 MB
#
# CRITICAL: 1 worker only.
#   Multiple workers = multiple copies of LRU photo cache + template bytes.
#   With 512 MB total RAM, even 2 workers will OOM on a 600-student render.
#
# --max-requests 50
#   Worker is recycled after 50 requests to prevent slow memory leaks.
#   A fresh worker starts before the old one exits (--max-requests-jitter).
#
# --timeout 300
#   600 students × ~0.1 s/card ≈ 60 s on 0.5 vCPU.
#   Give 5× headroom for photo fetch latency.

import os

# ── Workers ───────────────────────────────────────────────────────
workers          = 1                    # MUST stay at 1 on 512 MB
worker_class     = "sync"
threads          = 1

# ── Request recycling ─────────────────────────────────────────────
max_requests       = 50
max_requests_jitter = 10               # randomise so no thundering herd

# ── Timeouts ──────────────────────────────────────────────────────
timeout          = 300                 # 5 min — enough for 600 students
graceful_timeout = 30
keepalive        = 5

# ── Network ───────────────────────────────────────────────────────
bind             = f"0.0.0.0:{os.environ.get('PORT', 5000)}"

# ── Logging ───────────────────────────────────────────────────────
accesslog        = "-"
errorlog         = "-"
loglevel         = "info"

# ── Pre-load app (load template + fonts before first request) ─────
preload_app      = True