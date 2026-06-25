"""
mediafire_dl.py — Async Mediafire resolver + downloader
Tested working approach (June 2026):
- User-Agent: MediaFire/5.1 (Android) bypasses Cloudflare TLS block
- file/get_info GET → file metadata
- file/get_links GET with link_type=normal_download → page URL
- Download URL = normal_download URL (aiohttp redirects follow karke actual file deta hai)
- direct_download needs session token (Insufficient Permissions error 45 bina token ke)
"""

import re
import asyncio
import aiohttp
import aiofiles
from typing import Callable, Optional

# ── Key insight: MediaFire/5.1 (Android) User-Agent bypass karta hai Cloudflare ko ──
MF_HEADERS = {
    "User-Agent": "MediaFire/5.1 (Android)",
}

# Browser UA sirf download stream ke liye (CDN pe Cloudflare nahi hota)
DL_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Linux; Android 10; K) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Mobile Safari/537.36"
    ),
}

BASE_API       = "https://www.mediafire.com/api/1.5"
FILE_GET_INFO  = f"{BASE_API}/file/get_info.php"
FILE_GET_LINKS = f"{BASE_API}/file/get_links.php"
FOLDER_GET_CONTENT = f"{BASE_API}/folder/get_content.php"

MAX_RETRIES      = 4
RETRY_DELAY      = 2
CHUNK_SIZE       = 524288   # 512 KB
MAX_FOLDER_DEPTH = 10

TIMEOUT_SHORT = aiohttp.ClientTimeout(total=60,   connect=15)
TIMEOUT_DL    = aiohttp.ClientTimeout(total=None, connect=15)


def is_folder_link(url: str) -> bool:
    return bool(re.search(r"mediafire\.com/folder/", url, re.I))


def extract_folder_key(url: str) -> str:
    m = re.search(r"mediafire\.com/folder/([a-zA-Z0-9]+)", url, re.I)
    if m:
        return m.group(1)
    h = re.search(r"#([a-zA-Z0-9]+)", url)
    return h.group(1) if h else ""


def extract_file_key(url: str) -> str:
    m = re.search(r"mediafire\.com/file/([a-zA-Z0-9]+)", url, re.I)
    return m.group(1) if m else ""


async def _get_json(session: aiohttp.ClientSession, url: str, timeout=None) -> dict:
    """GET request with MF Android UA — bypasses Cloudflare block. Retries on failure."""
    last_exc = Exception("Unknown error")
    t = timeout or TIMEOUT_SHORT
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with session.get(
                url, timeout=t, allow_redirects=True,
                headers=MF_HEADERS
            ) as resp:
                resp.raise_for_status()
                return await resp.json(content_type=None)
        except Exception as e:
            last_exc = e
            if attempt < MAX_RETRIES:
                await asyncio.sleep(RETRY_DELAY * attempt)
    raise last_exc


async def get_info(url: str) -> Optional[dict]:
    """
    Resolve a Mediafire file URL.
    Returns: {'name': str, 'size': int (bytes), 'url': str, 'key': str} or None
    """
    async with aiohttp.ClientSession() as session:
        key = extract_file_key(url)
        if not key:
            raise Exception("Could not extract file key from URL.")
        return await get_file_info_by_key(session, key)


async def get_file_info_by_key(session: aiohttp.ClientSession, key: str) -> Optional[dict]:
    """
    Get file info + download URL using MediaFire API.
    Uses MediaFire/5.1 (Android) UA to bypass Cloudflare TLS fingerprinting.
    Returns: {'name': str, 'size': int (bytes), 'url': str, 'key': str} or None
    """
    try:
        # Step 1: File metadata
        info_url = f"{FILE_GET_INFO}?quick_key={key}&response_format=json"
        data = await _get_json(session, info_url)
        if data.get("response", {}).get("result") != "Success":
            return None
        fi = data["response"].get("file_info", {})

        # Step 2: Get normal_download link
        # direct_download needs session token (error 45 bina token ke)
        # normal_download URL pe aiohttp redirect follow karke actual file download karta hai
        links_url = f"{FILE_GET_LINKS}?quick_key={key}&link_type=normal_download&response_format=json"
        ldata = await _get_json(session, links_url)

        links = ldata.get("response", {}).get("links", [])
        dl_url = ""
        if isinstance(links, list) and links:
            dl_url = links[0].get("normal_download", "")

        if not dl_url:
            return None

        return {
            "name": fi.get("filename", "file"),
            "size": _parse_size(fi.get("size", "0")),
            "url":  dl_url,
            "key":  key,
        }
    except Exception:
        return None


async def get_folder_files(folder_key: str) -> list:
    files = []
    async with aiohttp.ClientSession() as session:
        await _collect_files(session, folder_key, files, depth=0)
    return files


async def _collect_files(session, folder_key: str, result: list, depth: int = 0):
    if depth > MAX_FOLDER_DEPTH:
        return

    chunk = 1
    while True:
        url = f"{FOLDER_GET_CONTENT}?folder_key={folder_key}&content_type=files&chunk_size=100&chunk={chunk}&response_format=json"
        try:
            data = await _get_json(session, url)
        except Exception:
            break

        fc = data.get("response", {}).get("folder_content", {})
        for f in fc.get("files") or []:
            file_key = f.get("quickkey", "")
            if not file_key:
                continue
            result.append({
                "name": f.get("filename", "file"),
                "size": _parse_size(f.get("size", "0")),
                "key":  file_key,
            })

        if fc.get("more_chunks") == "yes":
            chunk += 1
        else:
            break

    sub_chunk = 1
    while True:
        url = f"{FOLDER_GET_CONTENT}?folder_key={folder_key}&content_type=folders&chunk_size=100&chunk={sub_chunk}&response_format=json"
        try:
            data = await _get_json(session, url)
        except Exception:
            break

        fc = data.get("response", {}).get("folder_content", {})
        for sf in fc.get("folders") or []:
            sub_key = sf.get("folderkey", "")
            if sub_key:
                await _collect_files(session, sub_key, result, depth=depth + 1)

        if fc.get("more_chunks") == "yes":
            sub_chunk += 1
        else:
            break


async def _resolve_direct_url(session: aiohttp.ClientSession, url: str) -> str:
    """
    MediaFire normal_download URL se actual CDN download URL nikalta hai.
    Strategy:
    1. MF Android UA se GET karo — agar redirect mile toh woh CDN URL hai
    2. Agar HTML mile toh usme download URL dhundo (download\d+.mediafire.com)
    3. Warna url as-is return karo (aiohttp redirect handle kar lega)
    """
    try:
        async with session.get(
            url,
            timeout=TIMEOUT_SHORT,
            allow_redirects=False,
            headers=MF_HEADERS,
        ) as resp:
            loc = resp.headers.get("Location", "")
            if loc and "download" in loc:
                return loc

        # Try following redirects with MF UA
        async with session.get(
            url,
            timeout=TIMEOUT_SHORT,
            allow_redirects=True,
            headers=MF_HEADERS,
        ) as resp:
            final_url = str(resp.url)
            if "download" in final_url and "mediafire.com" in final_url:
                return final_url
            # Check if it's HTML with embedded download URL
            ct = resp.headers.get("Content-Type", "")
            if "text/html" in ct:
                html = await resp.text()
                m = re.search(r'https://download\d*\.mediafire\.com/[^\s"\'<>]+', html)
                if m:
                    return m.group(0)
    except Exception:
        pass

    return url  # fallback — aiohttp will handle redirects at download time


async def download(
    url: str,
    dest: str,
    progress_cb: Optional[Callable] = None,
    cancel_check: Optional[Callable] = None,
    chunk_size: int = CHUNK_SIZE,
):
    """
    Stream full file from url to dest.
    Pehle actual CDN URL resolve karta hai, phir download karta hai.
    """
    last_exc = Exception("Download failed")
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with aiohttp.ClientSession() as session:
                # Resolve actual download URL
                direct_url = await _resolve_direct_url(session, url)

                async with session.get(
                    direct_url,
                    timeout=TIMEOUT_DL,
                    allow_redirects=True,
                    headers=DL_HEADERS,
                ) as resp:
                    resp.raise_for_status()
                    total = int(resp.headers.get("Content-Length", 0))
                    done  = 0
                    async with aiofiles.open(dest, "wb") as f:
                        async for data in resp.content.iter_chunked(chunk_size):
                            if cancel_check and cancel_check():
                                raise asyncio.CancelledError("User cancelled")
                            await f.write(data)
                            done += len(data)
                            if progress_cb:
                                await progress_cb(done, total)
            return  # success
        except asyncio.CancelledError:
            raise
        except Exception as e:
            last_exc = e
            if attempt < MAX_RETRIES:
                await asyncio.sleep(RETRY_DELAY * attempt)
                try:
                    import os
                    if os.path.exists(dest):
                        os.remove(dest)
                except Exception:
                    pass
    raise last_exc


def _parse_size(val) -> int:
    try:
        return int(float(str(val).replace(",", "").strip()))
    except (ValueError, TypeError):
        return 0


def _human_to_bytes(n: float, unit: str) -> int:
    mul = {"B": 1, "KB": 1024, "MB": 1024**2, "GB": 1024**3, "TB": 1024**4}
    return int(n * mul.get(unit, 1))
