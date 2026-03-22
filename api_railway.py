"""
Auto Zoek Bot – Flask API (Railway/cloud versie)
Geen SIGHUP, geen subprocess: bot-control via Supabase.
"""

import os
import json
import threading
import logging
from datetime import datetime, timezone
from flask import Flask, jsonify, request, Response
from flask_cors import CORS
import db

app = Flask(__name__)
CORS(app)

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [API] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger(__name__)

# In-memory log buffer (laatste 500 regels)
_log_buffer: list[str] = []
_log_lock = threading.Lock()
_log_listeners: list = []   # SSE queues


def append_log(line: str):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    entry = f"{ts} {line}"
    with _log_lock:
        _log_buffer.append(entry)
        if len(_log_buffer) > 500:
            _log_buffer.pop(0)
        for q in _log_listeners:
            try: q.put_nowait(entry)
            except Exception: pass


# ── Status ────────────────────────────────────────────────────────────────────
@app.get("/api/status")
def get_status():
    try:
        total, today, last_found = db.get_stats()
        command, _ = db.get_bot_command()
        return jsonify({
            "running":    command == "start",
            "total_ads":  total,
            "today_ads":  today,
            "last_found": last_found,
            "pid":        None,  # geen PID in cloud
        })
    except Exception as e:
        log.error(f"Status fout: {e}")
        return jsonify({"running": False, "total_ads": 0, "today_ads": 0,
                        "last_found": None, "pid": None})


# ── Bot start / stop via Supabase ─────────────────────────────────────────────
@app.post("/api/bot/start")
def start_bot():
    db.set_bot_command("start")
    append_log("[API] Bot start-commando verstuurd")
    return jsonify({"ok": True})


@app.post("/api/bot/stop")
def stop_bot():
    db.set_bot_command("stop")
    append_log("[API] Bot stop-commando verstuurd")
    return jsonify({"ok": True})


# ── Zoekopdrachten ────────────────────────────────────────────────────────────
@app.get("/api/searches")
def get_searches():
    searches, _ = db.get_searches()
    return jsonify(searches)


@app.put("/api/searches")
def save_searches():
    data = request.json
    if not isinstance(data, list):
        return jsonify({"ok": False, "error": "Verwacht een lijst"}), 400
    versie = db.save_searches(data)
    append_log(f"[API] Zoekopdrachten opgeslagen ({len(data)} stuks, versie {versie})")
    return jsonify({"ok": True, "versie": versie})


# ── Config ────────────────────────────────────────────────────────────────────
@app.get("/api/config")
def get_config():
    return jsonify(db.get_config())


@app.put("/api/config")
def save_config():
    db.save_config(request.json)
    append_log("[API] Configuratie opgeslagen")
    return jsonify({"ok": True})


# ── Advertenties ──────────────────────────────────────────────────────────────
@app.get("/api/ads")
def get_ads():
    limit  = int(request.args.get("limit", 50))
    offset = int(request.args.get("offset", 0))
    ads, total = db.get_ads(limit=limit, offset=offset,
        website=request.args.get("website"),
        search_naam=request.args.get("search"))
    return jsonify({"ads": ads, "total": total})


# ── Logs via SSE ──────────────────────────────────────────────────────────────
@app.get("/api/logs/stream")
def stream_logs():
    import queue

    q = queue.Queue(maxsize=200)
    with _log_lock:
        # Stuur laatste 50 regels als backfill
        for line in _log_buffer[-50:]:
            q.put_nowait(line)
        _log_listeners.append(q)

    def generate():
        try:
            while True:
                try:
                    line = q.get(timeout=20)
                    yield f"data: {json.dumps(line)}\n\n"
                except Exception:
                    yield ": keepalive\n\n"
        finally:
            with _log_lock:
                if q in _log_listeners:
                    _log_listeners.remove(q)

    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.get("/api/logs")
def get_logs():
    with _log_lock:
        lines = list(_log_buffer[-100:])
    return jsonify({"lines": lines})


# ── Health check (Railway vereist dit) ───────────────────────────────────────
@app.get("/health")
def health():
    return jsonify({"ok": True}), 200


# ── Bot runner in dezelfde process (één Railway service) ─────────────────────
def start_bot_thread():
    """Start de bot als achtergrond-thread naast Flask."""
    import bot_runner
    t = threading.Thread(target=bot_runner.run, args=(append_log,), daemon=True)
    t.start()
    log.info("Bot runner thread gestart")


if __name__ == "__main__":
    start_bot_thread()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
