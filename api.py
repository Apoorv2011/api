import os, re, sqlite3, asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, HTTPException, Query
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pyrogram import Client
from ytmusicapi import YTMusic

# ============ CONFIG ============
API_ID   = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
SESSION  = os.environ["SESSION"]

DEFAULT_DB_URL     = "postgresql://neondb_owner:npg_G7efH9DhxOmg@ep-snowy-king-a1cz8but.ap-southeast-1.aws.neon.tech/neondb?sslmode=require"
DEFAULT_CHANNEL_ID = "-1001677848376"
DEFAULT_TOKEN      = "123"

DATABASE_URL = os.getenv("DATABASE_URL", DEFAULT_DB_URL)
DB_CHANNEL   = int(os.getenv("ARC_DB_CHANNEL_ID", DEFAULT_CHANNEL_ID))
ADMIN_TOKEN  = os.getenv("ADMIN_TOKEN", DEFAULT_TOKEN)

client = None
ytm = YTMusic()

# ============ STORAGE ============
if DATABASE_URL:
    import asyncpg
    IS_PG = True
    _pool = None
    async def db_connect():
        global _pool
        _pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=4)
        async with _pool.acquire() as c:
            await c.execute("""CREATE TABLE IF NOT EXISTS songs (
                video_id TEXT PRIMARY KEY, msg_id BIGINT NOT NULL,
                channel_id BIGINT NOT NULL)""")
            await c.execute("CREATE INDEX IF NOT EXISTS idx_msg ON songs (msg_id)")
    async def db_upsert(rows):
        async with _pool.acquire() as c:
            await c.executemany("""INSERT INTO songs (video_id,msg_id,channel_id)
                VALUES ($1,$2,$3)
                ON CONFLICT (video_id) DO UPDATE
                SET msg_id=EXCLUDED.msg_id, channel_id=EXCLUDED.channel_id""", rows)
    async def db_get_by_video(vid):
        async with _pool.acquire() as c:
            return await c.fetchrow("SELECT * FROM songs WHERE video_id=$1", vid)
    async def db_get_by_msg(mid):
        async with _pool.acquire() as c:
            return await c.fetchrow("SELECT * FROM songs WHERE msg_id=$1", mid)
    async def db_has_many(vids):
        async with _pool.acquire() as c:
            rows = await c.fetch("SELECT video_id FROM songs WHERE video_id = ANY($1::text[])", vids)
            return {r["video_id"] for r in rows}
    def row_video(r): return r["video_id"]
    def row_msg(r): return r["msg_id"]
    def row_channel(r): return r["channel_id"]
else:
    IS_PG = False
    _conn = None
    async def db_connect():
        global _conn
        _conn = sqlite3.connect("songs.db", check_same_thread=False)
        _conn.execute("""CREATE TABLE IF NOT EXISTS songs (
            video_id TEXT PRIMARY KEY, msg_id INTEGER NOT NULL,
            channel_id INTEGER NOT NULL)""")
        _conn.execute("CREATE INDEX IF NOT EXISTS idx_msg ON songs (msg_id)")
        _conn.commit()
    async def db_upsert(rows):
        _conn.executemany("INSERT OR REPLACE INTO songs VALUES (?,?,?)", rows)
        _conn.commit()
    async def db_get_by_video(vid):
        cur = _conn.cursor(); cur.execute("SELECT * FROM songs WHERE video_id=?", (vid,))
        return cur.fetchone()
    async def db_get_by_msg(mid):
        cur = _conn.cursor(); cur.execute("SELECT * FROM songs WHERE msg_id=?", (mid,))
        return cur.fetchone()
    async def db_has_many(vids):
        q = ",".join("?" * len(vids))
        cur = _conn.cursor()
        cur.execute(f"SELECT video_id FROM songs WHERE video_id IN ({q})", vids)
        return {r[0] for r in cur.fetchall()}
    def row_video(r): return r[0]
    def row_msg(r): return r[1]
    def row_channel(r): return r[2]

# ============ INDEXER ============
_rebuild_state = {"running": False, "indexed": 0, "done": False, "error": None}

def _extract_video_id(msg):
    txt = (msg.caption or "").strip()
    m = re.search(r"([A-Za-z0-9_-]{11})(?:\.[a-zA-Z0-9]{2,4})?$", txt)
    if not m:
        m = re.search(r"(?<![A-Za-z0-9_-])([A-Za-z0-9_-]{11})(?![A-Za-z0-9_-])", txt)
    if m: return m.group(1)

    file_name = None
    if msg.audio: file_name = msg.audio.file_name
    elif msg.video: file_name = msg.video.file_name
    elif msg.document: file_name = msg.document.file_name
    
    if file_name:
        m = re.search(r"([A-Za-z0-9_-]{11})(?:\.[a-zA-Z0-9]{2,4})?$", file_name)
        if m: return m.group(1)
    return None

async def index_channel():
    global _rebuild_state
    _rebuild_state.update({"running": True, "indexed": 0, "done": False, "error": None})
    batch, count = [], 0
    try:
        async for msg in client.get_chat_history(DB_CHANNEL):
            if not (msg.audio or msg.video or msg.document):
                continue
            vid = _extract_video_id(msg)
            if not vid: 
                continue
            batch.append((vid, msg.id, DB_CHANNEL))
            count += 1
            _rebuild_state["indexed"] = count
            if len(batch) >= 200:
                await db_upsert(batch); batch = []
        if batch:
            await db_upsert(batch)
        _rebuild_state["done"] = True
    except Exception as e:
        _rebuild_state["error"] = str(e)
    finally:
        _rebuild_state["running"] = False

# ============ APP ============
@asynccontextmanager
async def lifespan(app: FastAPI):
    global client
    client = Client(":memory:", api_id=API_ID, api_hash=API_HASH, session_string=SESSION)
    await client.start()
    await db_connect()
    yield
    await client.stop()

app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["GET","POST"], allow_headers=["*"], expose_headers=["Content-Range","Accept-Ranges","Content-Length"])

@app.post("/rebuild")
async def rebuild(token: str = Query("")):
    if ADMIN_TOKEN and token != ADMIN_TOKEN: raise HTTPException(403)
    if _rebuild_state["running"]: return {"status": "already running"}
    asyncio.create_task(index_channel())
    return {"status": "started"}

@app.get("/status")
async def status():
    return _rebuild_state

@app.get("/get/{video_id}")
async def get_by_id(video_id: str):
    row = await db_get_by_video(video_id)
    if not row: raise HTTPException(404, "Song ID not found in channel index")
    mid = row_msg(row)
    try:
        info = await asyncio.to_thread(lambda: ytm.get_song(video_id).get("videoDetails", {}))
        title = info.get("title", "Unknown")
        artist = info.get("author", "Unknown")
        thumbnails = info.get("thumbnail", {}).get("thumbnails", [])
        thumb = thumbnails[-1].get("url", "") if thumbnails else ""
    except Exception:
        title, artist, thumb = video_id, "Unknown", ""
    return {"id": mid, "video_id": video_id, "title": title, "artist": artist, "thumbnail": thumb, "stream": f"/stream/{mid}", "download": f"/download/{mid}", "similar": f"/similar/{video_id}"}

@app.get("/search")
async def search(q: str, limit: int = 10):
    def _yt(): return ytm.search(q, filter="songs", limit=limit * 3)
    try: results = await asyncio.to_thread(_yt)
    except Exception: results = []
    
    candidates, meta = [], {}
    for item in results:
        vid = item.get("videoId")
        if not vid or vid in meta: continue
        artists = item.get("artists") or []
        thumbs  = item.get("thumbnails") or []
        meta[vid] = {"video_id": vid, "title": item.get("title", "Unknown"), "artist": artists[0].get("name", "Unknown") if artists else "Unknown", "duration": item.get("duration_seconds", 0) or 0, "thumbnail": thumbs[-1].get("url", "") if thumbs else ""}
        candidates.append(vid)
    if not candidates: return []
    
    have = await db_has_many(candidates)
    out = []
    for vid in candidates:
        if vid not in have: continue
        row = await db_get_by_video(vid)
        mid = row_msg(row)
        out.append({**meta[vid], "id": mid, "stream": f"/stream/{mid}", "download": f"/download/{mid}", "similar": f"/similar/{vid}"})
        if len(out) >= limit: break
    return out

@app.get("/similar/{video_id}")
async def similar(video_id: str, limit: int = 10):
    try:
        data = await asyncio.to_thread(lambda: ytm.get_watch_playlist(videoId=video_id, limit=limit + 5))
        tracks = data.get("tracks", [])
        vids, meta = [], {}
        for t in tracks:
            tid = t.get("videoId")
            if not tid or tid == video_id: continue
            artists = t.get("artists") or []
            thumbs = t.get("thumbnail") or []
            if isinstance(thumbs, dict): thumbs = [thumbs]
            meta[tid] = {"video_id": tid, "title": t.get("title", "Unknown"), "artist": artists[0].get("name", "Unknown") if artists else "Unknown", "duration": t.get("length", "0:00"), "thumbnail": thumbs[-1].get("url", "") if thumbs else ""}
            vids.append(tid)
        if not vids: return []
        have = await db_has_many(vids)
        out = []
        for vid in vids:
            if vid not in have: continue
            row = await db_get_by_video(vid)
            mid = row_msg(row)
            out.append({**meta[vid], "id": mid, "stream": f"/stream/{mid}", "download": f"/download/{mid}", "similar": f"/similar/{vid}"})
            if len(out) >= limit: break
        return out
    except Exception: return []

async def _get_msg(msg_id: int):
    row = await db_get_by_msg(msg_id)
    if not row: raise HTTPException(404, "not indexed")
    msg = await client.get_messages(row_channel(row), msg_id)
    if not msg: raise HTTPException(404, "file gone")
    return msg

def _parse_range(h, size):
    if not h: return 0, size-1, 200
    m = re.match(r"bytes=(\d+)-(\d*)", h)
    s = int(m.group(1)); e = int(m.group(2)) if m.group(2) else size-1
    return s, e, 206

def _get_media_info(msg):
    if msg.audio:
        return msg.audio, msg.audio.file_size, msg.audio.mime_type or "audio/mpeg", "m4a"
    elif msg.video:
        return msg.video, msg.video.file_size, msg.video.mime_type or "video/mp4", "mp4"
    elif msg.document:
        mime = msg.document.mime_type or "application/octet-stream"
        ext = "mp4" if "video" in mime else ("m4a" if "audio" in mime else "bin")
        return msg.document, msg.document.file_size, mime, ext
    return None, 0, "", ""

@app.get("/stream/{msg_id}")
async def stream(msg_id: int, request: Request):
    msg = await _get_msg(msg_id)
    media, size, mime, _ = _get_media_info(msg)
    if not media: raise HTTPException(404, "No media found")
    
    s, e, status = _parse_range(request.headers.get("range"), size)
    headers = {"Accept-Ranges":"bytes", "Content-Type": mime, "Content-Length": str(e-s+1)}
    if status == 206: headers["Content-Range"] = f"bytes {s}-{e}/{size}"
    
    chunk_size = 1024 * 1024
    start_chunk = s // chunk_size
    end_byte = e
    
    async def gen():
        pos = s
        # Pyrogram's iter_download uses byte offset directly
        async for chunk in client.stream_media(msg, offset=start_chunk):
            if pos == s and s % chunk_size != 0:
                skip = s % chunk_size
                chunk = chunk[skip:]
            if pos + len(chunk) > end_byte:
                yield chunk[: end_byte - pos + 1]; break
            yield chunk
            pos += len(chunk)
            
    return StreamingResponse(gen(), status_code=status, headers=headers)

@app.get("/download/{msg_id}")
async def download(msg_id: int):
    msg = await _get_msg(msg_id)
    media, size, mime, ext = _get_media_info(msg)
    if not media: raise HTTPException(404, "No media found")
    
    async def gen():
        async for chunk in client.stream_media(msg):
            yield chunk
            
    return StreamingResponse(gen(), media_type=mime, headers={"Content-Disposition": f'attachment; filename="{msg_id}.{ext}"', "Content-Length": str(size)})
