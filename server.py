import os, sqlite3, hashlib, uuid, threading, mimetypes, json, asyncio
from pathlib import Path
from typing import Optional, List
from concurrent.futures import ThreadPoolExecutor, as_completed

import mutagen
import httpx
from fastapi import FastAPI, HTTPException, Depends, Query, Header, BackgroundTasks, Request
from fastapi.responses import FileResponse, Response, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from security import hash_password, verify_password, create_token, decode_token

DB_PATH    = os.environ.get("DB_PATH",    "/data/music.db")
STATIC_DIR = "/app/static"
os.makedirs(STATIC_DIR, exist_ok=True)
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

app = FastAPI(title="HomeTunes")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

scan_status = {"running":False,"done":0,"total":0,"message":"Idle","new":0,"skipped":0}
_scan_lock  = threading.Lock()

# ── DB ─────────────────────────────────────────────────────────────────────────
def get_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=10000")
    return conn

def init_db():
    conn = get_db()
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS config (key TEXT PRIMARY KEY, value TEXT);
    CREATE TABLE IF NOT EXISTS songs (
        id TEXT PRIMARY KEY, path TEXT UNIQUE, title TEXT, artist TEXT,
        album TEXT, album_artist TEXT, track_number INTEGER DEFAULT 0,
        duration REAL DEFAULT 0, bitrate INTEGER DEFAULT 0,
        file_size INTEGER DEFAULT 0, format TEXT,
        cover_art INTEGER DEFAULT 0, low_quality INTEGER DEFAULT 0,
        mtime REAL DEFAULT 0, play_count INTEGER DEFAULT 0,
        last_played TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_songs_artist ON songs(artist);
    CREATE INDEX IF NOT EXISTS idx_songs_album  ON songs(album);
    CREATE INDEX IF NOT EXISTS idx_songs_lq     ON songs(low_quality);
    CREATE TABLE IF NOT EXISTS users (
        id TEXT PRIMARY KEY, username TEXT UNIQUE, password_hash TEXT,
        role TEXT DEFAULT 'user', created_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS playlists (
        id TEXT PRIMARY KEY, name TEXT, owner_id TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS playlist_songs (
        playlist_id TEXT, song_id TEXT, position INTEGER DEFAULT 0,
        PRIMARY KEY (playlist_id, song_id)
    );
    CREATE TABLE IF NOT EXISTS play_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        song_id TEXT, user_id TEXT, played_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS music_requests (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id TEXT, username TEXT, query TEXT,
        status TEXT DEFAULT 'pending',
        created_at TEXT DEFAULT (datetime('now'))
    );
    """)
    conn.commit(); conn.close()

def get_config(key, default=None):
    conn = get_db()
    row  = conn.execute("SELECT value FROM config WHERE key=?", (key,)).fetchone()
    conn.close()
    return row["value"] if row else default

def set_config(key, value):
    conn = get_db()
    conn.execute("INSERT OR REPLACE INTO config (key,value) VALUES (?,?)", (key, value))
    conn.commit(); conn.close()

def get_music_path():
    return get_config("music_path", os.environ.get("MUSIC_PATH","/mnt/tank/Music"))

# ── Auth ───────────────────────────────────────────────────────────────────────
def require_user(authorization: str = Header(None)):
    if not authorization: raise HTTPException(401, "Login required")
    token = authorization.replace("Bearer ","")
    try:    return decode_token(token)
    except: raise HTTPException(401, "Invalid token")

def require_admin(user=Depends(require_user)):
    if user.get("role") != "admin": raise HTTPException(403, "Admin required")
    return user

# ── Scanner ────────────────────────────────────────────────────────────────────
AUDIO_EXTS = {".mp3",".flac",".m4a",".aac",".ogg",".opus",".wav"}

def get_tag(tags, *keys, default=""):
    for k in keys:
        v = tags.get(k)
        if v:
            s = str(v[0]) if isinstance(v, list) else str(v)
            if s.strip(): return s.strip()
    return default

def extract_cover_flag(af, path):
    try:
        if hasattr(af,'pictures') and af.pictures: return True
        if af.tags:
            for k in af.tags.keys():
                if 'APIC' in str(k): return True
    except: pass
    folder = os.path.dirname(path)
    for name in ('cover.jpg','cover.png','folder.jpg','folder.png','album.jpg'):
        if os.path.exists(os.path.join(folder, name)): return True
    return False

def process_file(full_path):
    try:
        mtime=os.path.getmtime(full_path); file_size=os.path.getsize(full_path)
        song_id=hashlib.md5(full_path.encode()).hexdigest()
        ext=Path(full_path).suffix.lower().lstrip(".")
        title=Path(full_path).stem; artist="Unknown Artist"; album="Unknown Album"
        album_art=""; track_num=0; duration=0.0; bitrate=0; has_cover=False
        af=mutagen.File(full_path, easy=False)
        if af:
            info=af.info
            duration=float(getattr(info,'length',0) or 0)
            raw_br=getattr(info,'bitrate',0) or 0
            bitrate=int(raw_br/1000) if raw_br>=1000 else int(raw_br)
            has_cover=extract_cover_flag(af, full_path)
            if af.tags:
                t=dict(af.tags)
                title    =get_tag(t,'TIT2','title','\xa9nam', default=title)
                artist   =get_tag(t,'TPE1','artist','\xa9ART',default=artist)
                album    =get_tag(t,'TALB','album','\xa9alb', default=album)
                album_art=get_tag(t,'TPE2','aART',            default="")
                trk=get_tag(t,'TRCK','tracknumber','trkn',default="0")
                try: track_num=int(str(trk).split('/')[0])
                except: track_num=0
        return {"id":song_id,"path":full_path,"title":title,"artist":artist,
                "album":album,"album_artist":album_art,"track_number":track_num,
                "duration":duration,"bitrate":bitrate,"file_size":file_size,
                "format":ext,"cover_art":1 if has_cover else 0,
                "low_quality":1 if 0<bitrate<128 else 0,"mtime":mtime}
    except: return None

def scan_library():
    global scan_status
    if not _scan_lock.acquire(blocking=False): return
    try:
        music_path=get_music_path()
        scan_status={"running":True,"done":0,"total":0,"message":"Finding files…","new":0,"skipped":0}
        if not os.path.isdir(music_path):
            scan_status={"running":False,"done":0,"total":0,"message":f"ERROR: Music path not found: {music_path}","new":0,"skipped":0}
            return
        all_files=[]
        for root,_,files in os.walk(music_path):
            for f in files:
                if Path(f).suffix.lower() in AUDIO_EXTS:
                    all_files.append(os.path.join(root,f))
        total=len(all_files); scan_status["total"]=total
        conn=get_db()
        existing={row["path"]:row["mtime"] for row in conn.execute("SELECT path,mtime FROM songs").fetchall()}
        conn.close()
        to_process=[]; to_skip=[]
        for fp in all_files:
            try: cur_mtime=os.path.getmtime(fp)
            except: continue
            if fp in existing and abs(existing[fp]-cur_mtime)<0.1: to_skip.append(fp)
            else: to_process.append(fp)
        scan_status["skipped"]=len(to_skip)
        scan_status["message"]=f"Scanning {len(to_process)} new/changed ({len(to_skip)} skipped)…"
        done_count=0; new_count=0; buf=[]
        def flush(b):
            if not b: return
            c=get_db()
            c.executemany("""INSERT OR REPLACE INTO songs
                (id,path,title,artist,album,album_artist,track_number,duration,
                 bitrate,file_size,format,cover_art,low_quality,mtime)
                VALUES (:id,:path,:title,:artist,:album,:album_artist,:track_number,
                        :duration,:bitrate,:file_size,:format,:cover_art,:low_quality,:mtime)""",b)
            c.commit(); c.close()
        WORKERS=min(8, os.cpu_count() or 4)
        with ThreadPoolExecutor(max_workers=WORKERS) as ex:
            futs={ex.submit(process_file,fp):fp for fp in to_process}
            for fut in as_completed(futs):
                r=fut.result(); done_count+=1
                if r: buf.append(r); new_count+=1
                scan_status["done"]=done_count+len(to_skip)
                scan_status["new"]=new_count
                scan_status["message"]=f"Scanning {done_count}/{len(to_process)} — {new_count} indexed"
                if len(buf)>=200: flush(buf); buf=[]
        flush(buf)
        all_paths=set(all_files)
        conn=get_db()
        removed=0
        for p in [r["path"] for r in conn.execute("SELECT path FROM songs").fetchall()]:
            if p not in all_paths: conn.execute("DELETE FROM songs WHERE path=?", (p,)); removed+=1
        conn.commit(); conn.close()
        scan_status={"running":False,"done":total,"total":total,
                     "message":f"Done — {new_count} indexed, {len(to_skip)} skipped, {removed} removed",
                     "new":new_count,"skipped":len(to_skip)}
    finally: _scan_lock.release()

# ── Setup ──────────────────────────────────────────────────────────────────────
@app.get("/api/setup/status")
def setup_status():
    conn=get_db(); count=conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]; conn.close()
    return {"needs_setup": count==0}

class SetupData(BaseModel):
    username: str; password: str; music_path: str

@app.post("/api/setup")
def setup(data: SetupData):
    conn=get_db()
    if conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]>0:
        conn.close(); raise HTTPException(400,"Already set up")
    if len(data.username)<2: conn.close(); raise HTTPException(400,"Username too short")
    if len(data.password)<4: conn.close(); raise HTTPException(400,"Password too short")
    uid=str(uuid.uuid4())
    conn.execute("INSERT INTO users VALUES (?,?,?,?,datetime('now'))",
                 (uid,data.username,hash_password(data.password),"admin"))
    conn.commit(); conn.close()
    set_config("music_path", data.music_path)
    return {"token":create_token(uid,"admin"),"role":"admin","username":data.username}

# ── Auth ───────────────────────────────────────────────────────────────────────
class LoginData(BaseModel):
    username: str; password: str

@app.post("/api/auth/login")
def login(data: LoginData):
    conn=get_db()
    user=conn.execute("SELECT * FROM users WHERE username=?", (data.username,)).fetchone()
    conn.close()
    if not user or not verify_password(data.password, user["password_hash"]):
        raise HTTPException(401,"Invalid credentials")
    return {"token":create_token(user["id"],user["role"]),"role":user["role"],"username":user["username"]}

# ── Users ──────────────────────────────────────────────────────────────────────
@app.get("/api/users")
def list_users(admin=Depends(require_admin)):
    conn=get_db(); rows=conn.execute("SELECT id,username,role,created_at FROM users ORDER BY created_at").fetchall(); conn.close()
    return [dict(r) for r in rows]

class UserCreate(BaseModel):
    username: str; password: str; role: str="user"

@app.post("/api/users")
def create_user(data: UserCreate, admin=Depends(require_admin)):
    if len(data.username)<2: raise HTTPException(400,"Username too short")
    if len(data.password)<4: raise HTTPException(400,"Password too short")
    if data.role not in ("admin","user"): raise HTTPException(400,"Invalid role")
    conn=get_db()
    if conn.execute("SELECT id FROM users WHERE username=?", (data.username,)).fetchone():
        conn.close(); raise HTTPException(400,"Username taken")
    uid=str(uuid.uuid4())
    conn.execute("INSERT INTO users VALUES (?,?,?,?,datetime('now'))",(uid,data.username,hash_password(data.password),data.role))
    conn.commit(); conn.close()
    return {"id":uid,"username":data.username,"role":data.role}

@app.delete("/api/users/{uid}")
def delete_user(uid: str, admin=Depends(require_admin)):
    conn=get_db()
    admins=conn.execute("SELECT COUNT(*) FROM users WHERE role='admin'").fetchone()[0]
    target=conn.execute("SELECT role FROM users WHERE id=?", (uid,)).fetchone()
    if not target: conn.close(); raise HTTPException(404)
    if target["role"]=="admin" and admins<=1: conn.close(); raise HTTPException(400,"Cannot delete last admin")
    conn.execute("DELETE FROM users WHERE id=?", (uid,)); conn.commit(); conn.close()
    return {"status":"deleted"}

# ── Config ─────────────────────────────────────────────────────────────────────
@app.get("/api/config")
def get_all_config(user=Depends(require_user)):
    is_admin=user.get("role")=="admin" 
    conn=get_db(); rows=conn.execute("SELECT key,value FROM config").fetchall(); conn.close()
    cfg={r["key"]:r["value"] for r in rows}
    cfg.setdefault("music_path", os.environ.get("MUSIC_PATH","/mnt/tank/Music"))
    if not is_admin:
        # Only expose non-sensitive keys to regular users
        safe_keys={"admin_css","music_path"}
        cfg={k:v for k,v in cfg.items() if k in safe_keys}
    return cfg

class ConfigUpdate(BaseModel):
    music_path:         Optional[str]=None
    telegram_bot_token: Optional[str]=None
    telegram_chat_id:   Optional[str]=None
    lastfm_api_key:     Optional[str]=None
    lastfm_secret:      Optional[str]=None
    listenbrainz_token: Optional[str]=None
    admin_css:          Optional[str]=None

@app.patch("/api/config")
def update_config(data: ConfigUpdate, user=Depends(require_user)):
    d=data.dict(exclude_none=True)
    # Sensitive keys require admin
    admin_keys={"music_path","telegram_bot_token","telegram_chat_id","lastfm_api_key","lastfm_secret","listenbrainz_token"}
    has_admin_key=any(k in admin_keys for k in d)
    if has_admin_key and user.get("role")!="admin":
        raise HTTPException(403,"Admin required for these settings")
    for k,v in d.items(): set_config(k,v)
    return {"status":"ok"}

# ── Songs ──────────────────────────────────────────────────────────────────────
@app.get("/api/songs")
def get_songs(limit:int=5000, offset:int=0, artist:Optional[str]=None, album:Optional[str]=None, user=Depends(require_user)):
    conn=get_db(); where,params=[],[]
    if artist: where.append("artist=?"); params.append(artist)
    if album:  where.append("album=?");  params.append(album)
    w=("WHERE "+" AND ".join(where)) if where else ""
    rows=conn.execute(f"SELECT id,path,title,artist,album,album_artist,track_number,duration,bitrate,file_size,format,cover_art,low_quality,play_count FROM songs {w} ORDER BY artist,album,track_number,title LIMIT ? OFFSET ?",params+[limit,offset]).fetchall()
    conn.close(); return [dict(r) for r in rows]

@app.get("/api/songs/{sid}/stream")
def stream(sid:str, request:Request, token:str=Query(None)):
    if not token: raise HTTPException(401)
    try: decode_token(token)
    except: raise HTTPException(401)
    conn=get_db(); row=conn.execute("SELECT path FROM songs WHERE id=?",(sid,)).fetchone(); conn.close()
    if not row: raise HTTPException(404)
    path=row["path"]
    if not os.path.exists(path): raise HTTPException(404,"File not found on disk")
    # Record play count async
    def record():
        c=get_db()
        c.execute("UPDATE songs SET play_count=play_count+1, last_played=datetime('now') WHERE id=?",(sid,))
        c.commit(); c.close()
    threading.Thread(target=record, daemon=True).start()
    file_size=os.path.getsize(path)
    mime,_=mimetypes.guess_type(path); mime=mime or "audio/mpeg"
    range_header=request.headers.get("range")
    def iter_file(s,e):
        with open(path,"rb") as f:
            f.seek(s); remaining=e-s+1
            while remaining>0:
                data=f.read(min(65536,remaining))
                if not data: break
                yield data; remaining-=len(data)
    if range_header:
        try:
            rv=range_header.strip().replace("bytes=","").split("-")
            start=int(rv[0]) if rv[0] else 0
            end=int(rv[1]) if rv[1] else file_size-1
        except: start,end=0,file_size-1
        end=min(end,file_size-1)
        return StreamingResponse(iter_file(start,end),status_code=206,media_type=mime,
            headers={"Content-Range":f"bytes {start}-{end}/{file_size}","Accept-Ranges":"bytes","Content-Length":str(end-start+1),"Cache-Control":"no-cache"})
    return StreamingResponse(iter_file(0,file_size-1),media_type=mime,
        headers={"Accept-Ranges":"bytes","Content-Length":str(file_size),"Cache-Control":"no-cache"})

@app.get("/api/songs/{sid}/cover")
def cover(sid:str, token:str=Query(None), authorization:str=Header(None)):
    tok=token or (authorization or "").replace("Bearer ","")
    if not tok: raise HTTPException(401)
    try: decode_token(tok)
    except: raise HTTPException(401)
    conn=get_db(); row=conn.execute("SELECT path FROM songs WHERE id=?",(sid,)).fetchone(); conn.close()
    if not row: raise HTTPException(404)
    path=row["path"]
    try:
        af=mutagen.File(path,easy=False)
        if af:
            if hasattr(af,'pictures') and af.pictures:
                p=af.pictures[0]; return Response(content=p.data,media_type=p.mime_type or "image/jpeg")
            if af.tags:
                for k in list(af.tags.keys()):
                    if 'APIC' in str(k):
                        p=af.tags[k]; return Response(content=p.data,media_type=p.mime_type or "image/jpeg")
    except: pass
    folder=os.path.dirname(path)
    for name in ('cover.jpg','cover.png','folder.jpg','folder.png','album.jpg'):
        fp=os.path.join(folder,name)
        if os.path.exists(fp): return FileResponse(fp)
    raise HTTPException(404)

class MetadataUpdate(BaseModel):
    title:Optional[str]=None; artist:Optional[str]=None
    album:Optional[str]=None; track_number:Optional[int]=None

@app.patch("/api/songs/{sid}/metadata")
def update_metadata(sid:str, data:MetadataUpdate, user=Depends(require_admin)):
    conn=get_db(); row=conn.execute("SELECT path FROM songs WHERE id=?",(sid,)).fetchone()
    if not row: conn.close(); raise HTTPException(404)
    updates,params=[],[]
    for field,val in data.dict(exclude_none=True).items():
        updates.append(f"{field}=?"); params.append(val)
    if updates: params.append(sid); conn.execute(f"UPDATE songs SET {', '.join(updates)} WHERE id=?",params); conn.commit()
    conn.close(); return {"status":"ok"}

@app.delete("/api/songs/{sid}")
def delete_song(sid:str, user=Depends(require_admin)):
    conn=get_db(); row=conn.execute("SELECT path FROM songs WHERE id=?",(sid,)).fetchone()
    if not row: conn.close(); raise HTTPException(404)
    try: os.remove(row["path"])
    except: pass
    conn.execute("DELETE FROM songs WHERE id=?",(sid,)); conn.commit(); conn.close()
    return {"status":"deleted"}

class BulkDelete(BaseModel):
    ids: List[str]

@app.post("/api/songs/bulk-delete")
def bulk_delete(data:BulkDelete, user=Depends(require_admin)):
    conn=get_db(); deleted=0
    for sid in data.ids:
        row=conn.execute("SELECT path FROM songs WHERE id=?",(sid,)).fetchone()
        if row:
            try: os.remove(row["path"])
            except: pass
            conn.execute("DELETE FROM songs WHERE id=?",(sid,)); deleted+=1
    conn.commit(); conn.close(); return {"deleted":deleted}

class BulkPlaylist(BaseModel):
    ids: List[str]; playlist_id: str

@app.post("/api/songs/bulk-add-playlist")
def bulk_add_playlist(data:BulkPlaylist, user=Depends(require_user)):
    conn=get_db()
    pos=conn.execute("SELECT COUNT(*) FROM playlist_songs WHERE playlist_id=?",(data.playlist_id,)).fetchone()[0]
    for sid in data.ids:
        conn.execute("INSERT OR IGNORE INTO playlist_songs VALUES (?,?,?)",(data.playlist_id,sid,pos)); pos+=1
    conn.commit(); conn.close(); return {"added":len(data.ids)}

# ── Artists / Albums / Stats ───────────────────────────────────────────────────
@app.get("/api/artists")
def get_artists(user=Depends(require_user)):
    conn=get_db(); rows=conn.execute("SELECT artist,COUNT(*) as song_count,COUNT(DISTINCT album) as album_count FROM songs GROUP BY artist ORDER BY artist").fetchall(); conn.close()
    return [dict(r) for r in rows]

@app.get("/api/albums")
def get_albums(user=Depends(require_user)):
    conn=get_db(); rows=conn.execute("SELECT album,artist,COUNT(*) as song_count FROM songs GROUP BY album,artist ORDER BY artist,album").fetchall(); conn.close()
    return [dict(r) for r in rows]

@app.get("/api/stats")
def get_stats(user=Depends(require_user)):
    conn=get_db(); r=conn.execute("SELECT COUNT(*) as total_songs,COUNT(DISTINCT artist) as total_artists,COUNT(DISTINCT album) as total_albums,SUM(duration) as total_duration,SUM(file_size) as total_size,SUM(low_quality) as low_quality_count,AVG(CASE WHEN bitrate>0 THEN bitrate END) as avg_bitrate FROM songs").fetchone(); conn.close()
    return dict(r)

# ── Play history & suggestions ─────────────────────────────────────────────────
@app.post("/api/songs/{sid}/played")
def record_played(sid:str, user=Depends(require_user)):
    conn=get_db()
    conn.execute("INSERT INTO play_history (song_id,user_id) VALUES (?,?)",(sid,user["sub"]))
    conn.execute("UPDATE songs SET play_count=play_count+1,last_played=datetime('now') WHERE id=?",(sid,))
    conn.commit(); conn.close(); return {"status":"ok"}

@app.get("/api/suggestions/{sid}")
def get_suggestions(sid:str, limit:int=10, user=Depends(require_user)):
    conn=get_db()
    song=conn.execute("SELECT * FROM songs WHERE id=?",(sid,)).fetchone()
    if not song: conn.close(); raise HTTPException(404)
    # Same artist + album first, then same artist, then similar by play count
    rows=conn.execute("""
        SELECT id,title,artist,album,duration,cover_art,play_count,
          CASE WHEN artist=? AND album=? THEN 3
               WHEN artist=? THEN 2
               ELSE 1 END as score
        FROM songs WHERE id!=?
        ORDER BY score DESC, play_count DESC, RANDOM()
        LIMIT ?
    """,(song["artist"],song["album"],song["artist"],sid,limit)).fetchall()
    conn.close(); return [dict(r) for r in rows]

# ── Playlists ──────────────────────────────────────────────────────────────────
class PlaylistCreate(BaseModel):
    name: str

@app.get("/api/playlists")
def get_playlists(user=Depends(require_user)):
    conn=get_db(); rows=conn.execute("SELECT * FROM playlists ORDER BY created_at DESC").fetchall(); conn.close()
    return [dict(r) for r in rows]

@app.post("/api/playlists")
def create_playlist(data:PlaylistCreate, user=Depends(require_user)):
    pid=str(uuid.uuid4()); conn=get_db()
    conn.execute("INSERT INTO playlists (id,name,owner_id) VALUES (?,?,?)",(pid,data.name,user["sub"]))
    conn.commit(); conn.close(); return {"id":pid,"name":data.name}

@app.delete("/api/playlists/{pid}")
def delete_playlist(pid:str, user=Depends(require_user)):
    conn=get_db(); conn.execute("DELETE FROM playlists WHERE id=?",(pid,)); conn.execute("DELETE FROM playlist_songs WHERE playlist_id=?",(pid,)); conn.commit(); conn.close()
    return {"status":"deleted"}

@app.get("/api/playlists/{pid}/songs")
def playlist_songs(pid:str, user=Depends(require_user)):
    conn=get_db(); rows=conn.execute("SELECT s.id,s.title,s.artist,s.album,s.duration,s.bitrate,s.cover_art,s.play_count FROM songs s JOIN playlist_songs ps ON s.id=ps.song_id WHERE ps.playlist_id=? ORDER BY ps.position",(pid,)).fetchall(); conn.close()
    return [dict(r) for r in rows]

class AddToPlaylist(BaseModel):
    song_id: str

@app.post("/api/playlists/{pid}/songs")
def add_to_playlist(pid:str, data:AddToPlaylist, user=Depends(require_user)):
    conn=get_db(); pos=conn.execute("SELECT COUNT(*) FROM playlist_songs WHERE playlist_id=?",(pid,)).fetchone()[0]
    conn.execute("INSERT OR IGNORE INTO playlist_songs VALUES (?,?,?)",(pid,data.song_id,pos)); conn.commit(); conn.close()
    return {"status":"ok"}

# ── Scrobbling (Last.fm + ListenBrainz) ───────────────────────────────────────
import time, hmac as _hmac, hashlib as _hs

@app.post("/api/scrobble")
async def scrobble(request:Request, user=Depends(require_user)):
    body=await request.json()
    sid=body.get("song_id"); song_data=body.get("song")
    if not song_data: raise HTTPException(400,"song data required")
    title=song_data.get("title",""); artist=song_data.get("artist",""); album=song_data.get("album","")
    ts=int(time.time()); results={}
    # Last.fm
    lfm_key=get_config("lastfm_api_key"); lfm_secret=get_config("lastfm_secret"); lfm_session=get_config("lastfm_session")
    if lfm_key and lfm_secret and lfm_session:
        try:
            params={"method":"track.scrobble","api_key":lfm_key,"sk":lfm_session,"artist":artist,"track":title,"album":album,"timestamp":str(ts)}
            sig_str="".join(f"{k}{v}" for k,v in sorted(params.items()))+lfm_secret
            params["api_sig"]=_hs.md5(sig_str.encode()).hexdigest(); params["format"]="json"
            async with httpx.AsyncClient() as client:
                r=await client.post("https://ws.audioscrobbler.com/2.0/",data=params,timeout=5)
                results["lastfm"]=r.status_code==200
        except: results["lastfm"]=False
    # ListenBrainz
    lbz_token=get_config("listenbrainz_token")
    if lbz_token:
        try:
            payload={"listen_type":"single","payload":[{"listened_at":ts,"track_metadata":{"artist_name":artist,"track_name":title,"release_name":album}}]}
            async with httpx.AsyncClient() as client:
                r=await client.post("https://api.listenbrainz.org/1/submit-listens",json=payload,headers={"Authorization":f"Token {lbz_token}"},timeout=5)
                results["listenbrainz"]=r.status_code==200
        except: results["listenbrainz"]=False
    return results

@app.get("/api/lastfm/auth-url")
def lastfm_auth_url(admin=Depends(require_admin)):
    key=get_config("lastfm_api_key")
    if not key: raise HTTPException(400,"No Last.fm API key configured")
    return {"url":f"https://www.last.fm/api/auth/?api_key={key}&cb={get_config('lastfm_callback','http://localhost:8765/api/lastfm/callback')}"}

@app.get("/api/lastfm/callback")
async def lastfm_callback(token:str=Query(None), admin=Depends(require_admin)):
    key=get_config("lastfm_api_key"); secret=get_config("lastfm_secret")
    if not key or not secret or not token: raise HTTPException(400,"Missing params")
    params={"method":"auth.getSession","api_key":key,"token":token}
    sig_str="".join(f"{k}{v}" for k,v in sorted(params.items()))+secret
    params["api_sig"]=_hs.md5(sig_str.encode()).hexdigest(); params["format"]="json"
    async with httpx.AsyncClient() as client:
        r=await client.get("https://ws.audioscrobbler.com/2.0/",params=params)
        d=r.json()
    if "session" in d:
        set_config("lastfm_session",d["session"]["key"]); return {"status":"ok","user":d["session"]["name"]}
    raise HTTPException(400,str(d))

# ── Music Requests ─────────────────────────────────────────────────────────────
class MusicRequest(BaseModel):
    query: str

@app.post("/api/requests")
async def submit_request(data:MusicRequest, user=Depends(require_user)):
    if not data.query.strip(): raise HTTPException(400,"Empty request")
    conn=get_db()
    conn.execute("INSERT INTO music_requests (user_id,username,query) VALUES (?,?,?)",(user["sub"],user.get("username","?"),data.query.strip()))
    conn.commit(); conn.close()
    # Send to Telegram
    bot_token=get_config("telegram_bot_token"); chat_id=get_config("telegram_chat_id")
    if bot_token and chat_id:
        try:
            msg=f"🎵 *Music Request*\nFrom: {user.get('username','user')}\nQuery: `{data.query}`"
            async with httpx.AsyncClient() as client:
                await client.post(f"https://api.telegram.org/bot{bot_token}/sendMessage",
                    json={"chat_id":chat_id,"text":msg,"parse_mode":"Markdown"},timeout=5)
        except: pass
    return {"status":"submitted"}

@app.get("/api/requests")
def get_requests(admin=Depends(require_admin)):
    conn=get_db(); rows=conn.execute("SELECT * FROM music_requests ORDER BY created_at DESC LIMIT 100").fetchall(); conn.close()
    return [dict(r) for r in rows]

@app.patch("/api/requests/{rid}")
def update_request(rid:int, status:str=Query(...), admin=Depends(require_admin)):
    if status not in ("pending","fulfilled","rejected"): raise HTTPException(400)
    conn=get_db(); conn.execute("UPDATE music_requests SET status=? WHERE id=?",(status,rid)); conn.commit(); conn.close()
    return {"status":"ok"}

# ── Scan ───────────────────────────────────────────────────────────────────────
@app.post("/api/scan/start")
def start_scan(background_tasks:BackgroundTasks, user=Depends(require_user)):
    if scan_status.get("running"): return {"status":"already running",**scan_status}
    background_tasks.add_task(scan_library); return {"status":"started"}

@app.get("/api/scan/status")
def get_scan_status(user=Depends(require_user)): return scan_status

# ── Static ─────────────────────────────────────────────────────────────────────
@app.get("/")
def index():
    for p in ("/scripts/hometunes/index.html", f"{STATIC_DIR}/index.html"):
        if os.path.exists(p): return FileResponse(p)
    raise HTTPException(404,"Frontend not found")

@app.on_event("startup")
def startup():
    init_db(); print(f"HomeTunes ready — DB: {DB_PATH}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8765, reload=False)