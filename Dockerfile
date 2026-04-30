# WarMap upload server.
#
# Bundles the FastAPI receiver (app/main.py) and the merger script
# (merger/merge.py) into one image.  The merger runs both on-demand
# (POST /merge) and on a periodic schedule.

FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /srv

COPY requirements.txt /srv/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Bundle the merger module so the server can import it as `merge`.
COPY merger/merge.py /srv/merger/merge.py

COPY app /srv/app

# Default WarMap data root. Overridable via env at runtime; we mount a
# volume here in compose.
ENV WARMAP_ROOT=/data
RUN mkdir -p /data/dumps /data/data/zones /data/sidecar /data/logs

EXPOSE 8000
# WARMAP_WORKERS controls uvicorn worker count.  Default 4 — sized for a
# small VPS; bump for bigger boxes.  The merger schedule is gated to a
# single worker via a file lock (see app/main.py _merge_periodically),
# so scaling workers does not multiply merge runs.
ENV WARMAP_WORKERS=4
CMD ["sh", "-c", "exec uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers ${WARMAP_WORKERS}"]
