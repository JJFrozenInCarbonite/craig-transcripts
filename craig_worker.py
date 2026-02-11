import io
import json
import os
import re
import sqlite3
import zipfile
from datetime import datetime, timezone
from typing import Optional, Tuple

import requests
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# =======================
# CONFIG
# =======================
SERVICE_ACCOUNT_JSON = "/opt/craig-transcripts/service-account.json"

# Secrets: set via environment (e.g. export in shell, systemd Environment=, or .env)
def _get_required_env(name: str) -> str:
    val = os.environ.get(name)
    if not val or not val.strip():
        raise SystemExit(
            f"Missing required environment variable: {name}. "
            f"Set it before running (e.g. export {name}='...')."
        )
    return val.strip()


CRAIG_PARENT_FOLDER_ID = _get_required_env("CRAIG_PARENT_FOLDER_ID")
DISCORD_WEBHOOK_URL = _get_required_env("DISCORD_WEBHOOK_URL")

WORK_DIR = "/opt/craig-transcripts/work"
DB_PATH = "/opt/craig-transcripts/state.db"

# Run via cron every 5 min; flock prevents overlap if a run exceeds 5 min (next run skips):
#   */5 * * * * flock -n /opt/craig-transcripts/craig_worker.lock bash -c 'cd /opt/craig-transcripts && . .env && ./venv/bin/python craig_worker.py'

ZIP_NAME_RE = re.compile(r"_(\d{4})-(\d{1,2})-(\d{1,2})_(\d{1,2})-(\d{1,2})-(\d{1,2})")

# =======================
# Setup
# =======================
def ensure_dirs() -> None:
    os.makedirs(WORK_DIR, exist_ok=True)

def init_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS processed (
            file_id TEXT PRIMARY KEY,
            processed_at_utc TEXT NOT NULL
        )
        """
    )
    conn.commit()
    return conn

def already_processed(conn: sqlite3.Connection, file_id: str) -> bool:
    cur = conn.execute("SELECT 1 FROM processed WHERE file_id = ?", (file_id,))
    return cur.fetchone() is not None

def mark_processed(conn: sqlite3.Connection, file_id: str) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO processed (file_id, processed_at_utc) VALUES (?, ?)",
        (file_id, datetime.now(timezone.utc).isoformat()),
    )
    conn.commit()

# =======================
# Parsing and formatting
# =======================
def parse_utc_from_zip_name(zip_name: str) -> Tuple[str, str]:
    """
    Assumes the timestamp embedded in the Craig filename is UTC.
    Returns:
      iso_utc: 2026-02-09T22:57:11Z
      file_ts: 2026-02-09_22-57-11
    """
    m = ZIP_NAME_RE.search(zip_name)
    if not m:
        return "Unknown", "unknown_time"

    year, mon, day, hh, mm, ss = map(int, m.groups())
    dt = datetime(year, mon, day, hh, mm, ss, tzinfo=timezone.utc)
    iso_utc = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    file_ts = dt.strftime("%Y-%m-%d_%H-%M-%S")
    return iso_utc, file_ts

def build_drive_download_link(file_id: str) -> str:
    return f"https://drive.google.com/uc?export=download&id={file_id}"

def vtt_to_markdown(vtt_text: str) -> str:
    """
    Output format per cue:
    [00:00:00.000] JJ - text...
    No asterisks.
    """
    lines = vtt_text.replace("\r", "").split("\n")

    out_lines = []
    current_ts = None
    current_speaker = None
    buffer = []

    def flush():
        nonlocal buffer
        if not current_ts or not current_speaker or not buffer:
            buffer = []
            return
        text = " ".join(buffer)
        text = re.sub(r"\s+", " ", text).strip()
        if text:
            out_lines.append(f"[{current_ts}] {current_speaker} - {text}")
        buffer = []

    for raw in lines:
        line = raw.strip()
        if not line:
            continue
        if line == "WEBVTT" or line.startswith("NOTE") or line.startswith("STYLE") or line.startswith("REGION"):
            continue
        if line.isdigit():
            continue

        if "-->" in line:
            flush()
            current_ts = line.split("-->")[0].strip()
            current_speaker = None
            continue

        if line.startswith("<v"):
            flush()
            m = re.search(r"<v\s+([^>]+)>", line)
            current_speaker = m.group(1).strip() if m else "Unknown"

            cleaned = line
            cleaned = re.sub(r"<v[^>]*>", "", cleaned)
            cleaned = cleaned.replace("</v>", "")
            cleaned = re.sub(r"<\d\d:\d\d:\d\d\.\d\d\d>", "", cleaned)
            cleaned = re.sub(r"</?c>", "", cleaned)
            cleaned = cleaned.strip()

            if cleaned:
                buffer.append(cleaned)
            continue

        cleaned = re.sub(r"<\d\d:\d\d:\d\d\.\d\d\d>", "", line)
        cleaned = re.sub(r"</?c>", "", cleaned).strip()
        if cleaned:
            buffer.append(cleaned)

    flush()
    return "\n".join(out_lines).strip() + "\n"

# =======================
# Google Drive API
# =======================
def drive_service():
    scopes = ["https://www.googleapis.com/auth/drive"]
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_JSON, scopes=scopes
    )
    return build("drive", "v3", credentials=creds, cache_discovery=False)

def list_craig_zips(svc, folder_id: str):
    q = (
        f"'{folder_id}' in parents and trashed = false "
        f"and name contains 'craig_' and name contains '.zip'"
    )
    page_token = None
    files = []
    while True:
        resp = (
            svc.files()
            .list(
                q=q,
                fields="nextPageToken, files(id, name, size, createdTime, modifiedTime)",
                pageToken=page_token,
                pageSize=100,
            )
            .execute()
        )
        files.extend(resp.get("files", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return files

def download_drive_file(svc, file_id: str, dest_path: str) -> None:
    request = svc.files().get_media(fileId=file_id)
    with io.FileIO(dest_path, "wb") as fh:
        downloader = MediaIoBaseDownload(fh, request, chunksize=1024 * 1024 * 8)
        done = False
        while not done:
            _, done = downloader.next_chunk()

def extract_transcription_vtt(zip_path: str) -> Optional[str]:
    with zipfile.ZipFile(zip_path, "r") as z:
        target = None
        for name in z.namelist():
            if name == "transcription.vtt" or name.endswith("/transcription.vtt"):
                target = name
                break
        if not target:
            return None
        with z.open(target) as f:
            return f.read().decode("utf-8", errors="replace")

def ensure_anyone_with_link(svc, file_id: str) -> None:
    perm = {"type": "anyone", "role": "reader"}
    try:
        svc.permissions().create(fileId=file_id, body=perm, fields="id").execute()
    except Exception:
        pass

# =======================
# Discord webhook
# =======================
DISCORD_CONTENT_LIMIT = 2000  # Discord message limit

def post_discord_message(webhook_url: str, content: str) -> None:
    r = requests.post(webhook_url, json={"content": content}, timeout=30)
    if r.status_code < 200 or r.status_code >= 300:
        raise RuntimeError(f"Discord webhook failed ({r.status_code}): {r.text}")


def post_discord_with_file(
    webhook_url: str,
    content: str,
    file_path: str,
    filename: str,
) -> None:
    """Post a message and attach a file (multipart/form-data)."""
    with open(file_path, "rb") as f:
        payload = {"content": content[:DISCORD_CONTENT_LIMIT]}
        r = requests.post(
            webhook_url,
            data={"payload_json": json.dumps(payload)},
            files={"file": (filename, f, "text/markdown")},
            timeout=60,
        )
    if r.status_code < 200 or r.status_code >= 300:
        raise RuntimeError(f"Discord webhook failed ({r.status_code}): {r.text}")

# =======================
# Worker loop
# =======================
def run_once(svc, conn: sqlite3.Connection) -> None:
    files = list_craig_zips(svc, CRAIG_PARENT_FOLDER_ID)
    if not files:
        return

    files.sort(key=lambda x: x.get("createdTime", ""))

    for f in files:
        file_id = f["id"]
        name = f["name"]

        if already_processed(conn, file_id):
            continue

        iso_utc, file_ts = parse_utc_from_zip_name(name)
        md_name = f"{file_ts}.md"
        local_zip_path = os.path.join(WORK_DIR, name)
        local_md_path = os.path.join(WORK_DIR, md_name)

        print(f"Processing: {name}")

        ensure_anyone_with_link(svc, file_id)
        zip_link = build_drive_download_link(file_id)

        download_drive_file(svc, file_id, local_zip_path)

        vtt = extract_transcription_vtt(local_zip_path)
        if not vtt:
            print(f"No transcription.vtt found inside {name}")
            continue

        md = vtt_to_markdown(vtt)
        with open(local_md_path, "w", encoding="utf-8") as out:
            out.write(md)

        # Build Discord message: info + transcript (truncated if needed); attach .md file
        info_lines = [
            "**Craig recording processed**",
            f"Recording time (UTC): {iso_utc}",
            f"Recording ZIP (download): {zip_link}",
            "",
        ]
        info_block = "\n".join(info_lines)
        full_transcript_block = f"**Transcript:**\n```markdown\n{md}\n```"
        content = info_block + full_transcript_block
        if len(content) > DISCORD_CONTENT_LIMIT:
            trailer = "\n```\n*(Full transcript in attached file)*"
            prefix = info_block + "**Transcript (preview):**\n```markdown\n"
            max_md = DISCORD_CONTENT_LIMIT - len(prefix) - len(trailer) - 4
            content = prefix + md[:max_md] + "\n..." + trailer
        post_discord_with_file(DISCORD_WEBHOOK_URL, content, local_md_path, md_name)

        mark_processed(conn, file_id)

        # Cleanup local
        try:
            os.remove(local_zip_path)
        except OSError:
            pass
        try:
            ARCHIVE_DIR = "/opt/craig-transcripts/archive"
            os.makedirs(ARCHIVE_DIR, exist_ok=True)
            archived_md_path = os.path.join(ARCHIVE_DIR, md_name)
            os.replace(local_md_path, archived_md_path)
        except OSError:
            pass

def main() -> None:
    ensure_dirs()
    conn = init_db()
    svc = drive_service()
    try:
        run_once(svc, conn)
    except Exception as e:
        print(f"Error: {e}")
        raise

if __name__ == "__main__":
    main()
