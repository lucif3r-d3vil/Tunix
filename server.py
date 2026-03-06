import os
import sqlite3
import hashlib
import threading
import mimetypes
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import mutagen
from fastapi import FastAPI, HTTPException, Depends, Query, Header, BackgroundTasks, Request
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware

from security import decode_token

DB_PATH = os.environ.get("DB_PATH", "/data/music.db")
MUSIC_PATH = os.environ.get("MUSIC_PATH", "/mnt/tank/Music")

AUDIO_EXT = {".mp3", ".flac", ".m4a", ".aac", ".ogg", ".opus", ".wav"}

app = FastAPI(title="HomeTunes")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

scan_status = {
    "running": False,
    "done": 0,
    "total": 0,
    "message": "Idle"
}

scan_lock = threading.Lock()


# ---------------------------------------------------
# Database
# ---------------------------------------------------

def get_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():

    conn = get_db()

    conn.executescript("""
    CREATE TABLE IF NOT EXISTS songs(
        id TEXT PRIMARY KEY,
        path TEXT UNIQUE,
        title TEXT,
        artist TEXT,
        album TEXT,
        duration REAL,
        bitrate INTEGER,
        mtime REAL
    );

    CREATE TABLE IF NOT EXISTS users(
        id TEXT PRIMARY KEY,
        username TEXT UNIQUE,
        password_hash TEXT,
        role TEXT
    );
    """)

    conn.commit()
    conn.close()


# ---------------------------------------------------
# Auth
# ---------------------------------------------------

def require_user(authorization: str = Header(None)):

    if not authorization:
        raise HTTPException(401, "Login required")

    token = authorization.replace("Bearer ", "")

    try:
        return decode_token(token)
    except:
        raise HTTPException(401, "Invalid token")


# ---------------------------------------------------
# Scanner
# ---------------------------------------------------

def read_metadata(path):

    try:

        af = mutagen.File(path)

        title = Path(path).stem
        artist = "Unknown Artist"
        album = "Unknown Album"
        duration = 0
        bitrate = 0

        if af:

            duration = getattr(af.info, "length", 0)

            raw_br = getattr(af.info, "bitrate", 0)
            bitrate = int(raw_br / 1000) if raw_br else 0

            if af.tags:

                tags = af.tags

                if "TIT2" in tags:
                    title = str(tags["TIT2"])

                if "TPE1" in tags:
                    artist = str(tags["TPE1"])

                if "TALB" in tags:
                    album = str(tags["TALB"])

        return {
            "id": hashlib.md5(path.encode()).hexdigest(),
            "path": path,
            "title": title,
            "artist": artist,
            "album": album,
            "duration": duration,
            "bitrate": bitrate,
            "mtime": os.path.getmtime(path)
        }

    except:
        return None


def scan_library():

    if not scan_lock.acquire(blocking=False):
        return

    try:

        scan_status["running"] = True
        scan_status["message"] = "Scanning..."

        files = []

        for root, _, fs in os.walk(MUSIC_PATH):

            for f in fs:

                if Path(f).suffix.lower() in AUDIO_EXT:
                    files.append(os.path.join(root, f))

        scan_status["total"] = len(files)
        scan_status["done"] = 0

        results = []

        with ThreadPoolExecutor(max_workers=6) as executor:

            futures = [executor.submit(read_metadata, f) for f in files]

            for future in as_completed(futures):

                r = future.result()

                if r:
                    results.append(r)

                scan_status["done"] += 1

        conn = get_db()

        conn.executemany("""
        INSERT OR REPLACE INTO songs
        (id,path,title,artist,album,duration,bitrate,mtime)
        VALUES
        (:id,:path,:title,:artist,:album,:duration,:bitrate,:mtime)
        """, results)

        conn.commit()
        conn.close()

        scan_status["running"] = False
        scan_status["message"] = f"Scan complete ({len(results)} songs)"

    finally:
        scan_lock.release()


# ---------------------------------------------------
# API
# ---------------------------------------------------

@app.post("/api/scan")
def start_scan(background_tasks: BackgroundTasks, user=Depends(require_user)):

    if scan_status["running"]:
        return {"status": "already_running"}

    background_tasks.add_task(scan_library)

    return {"status": "started"}


@app.get("/api/scan/status")
def get_scan_status(user=Depends(require_user)):
    return scan_status


@app.get("/api/songs")
def songs(limit: int = 5000, offset: int = 0, user=Depends(require_user)):

    conn = get_db()

    rows = conn.execute(
        "SELECT * FROM songs ORDER BY artist, album, title LIMIT ? OFFSET ?",
        (limit, offset)
    ).fetchall()

    conn.close()

    return [dict(r) for r in rows]


# ---------------------------------------------------
# Streaming
# ---------------------------------------------------

@app.get("/api/songs/{song_id}/stream")
def stream_song(song_id: str, request: Request, token: str = Query(None)):

    if not token:
        raise HTTPException(401)

    try:
        decode_token(token)
    except:
        raise HTTPException(401)

    conn = get_db()

    row = conn.execute(
        "SELECT path FROM songs WHERE id=?",
        (song_id,)
    ).fetchone()

    conn.close()

    if not row:
        raise HTTPException(404)

    path = row["path"]

    if not os.path.exists(path):
        raise HTTPException(404)

    file_size = os.path.getsize(path)

    mime, _ = mimetypes.guess_type(path)
    mime = mime or "audio/mpeg"

    range_header = request.headers.get("range")

    def file_iter(start, end):

        with open(path, "rb") as f:

            f.seek(start)

            remaining = end - start + 1

            while remaining > 0:

                chunk = f.read(min(65536, remaining))

                if not chunk:
                    break

                yield chunk

                remaining -= len(chunk)

    if range_header:

        start, end = range_header.replace("bytes=", "").split("-")

        start = int(start) if start else 0
        end = int(end) if end else file_size - 1

        return StreamingResponse(
            file_iter(start, end),
            status_code=206,
            media_type=mime,
            headers={
                "Content-Range": f"bytes {start}-{end}/{file_size}",
                "Accept-Ranges": "bytes",
                "Content-Length": str(end - start + 1),
            },
        )

    return StreamingResponse(
        file_iter(0, file_size - 1),
        media_type=mime,
        headers={
            "Accept-Ranges": "bytes",
            "Content-Length": str(file_size),
        },
    )


# ---------------------------------------------------
# Startup
# ---------------------------------------------------

@app.on_event("startup")
def startup():

    init_db()

    # automatic scan on server start
    threading.Thread(target=scan_library, daemon=True).start()