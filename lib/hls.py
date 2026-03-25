import math
import re
from base64 import b64decode
from urllib.parse import parse_qs
from urllib.parse import urljoin

import requests

DEFAULT_TIMEOUT = 30


def get_media_kind(url):
    lower = str(url or "").lower()
    if ".m3u8" in lower or re.search(r"/(?:playlist/)?m3u8(?:$|[/?#&])", lower):
        return "m3u8"

    if any(ext in lower for ext in (".ass", ".srt", ".vtt")):
        return "subtitle"

    return None


def decode_possibly_encoded(value):
    current = str(value or "")

    for _ in range(3):
        try:
            decoded = requests.utils.unquote(current)
        except Exception:
            break

        if decoded == current:
            break
        current = decoded

    return current


def normalize_text_for_extraction(value):
    return decode_possibly_encoded(
        str(value or "")
        .replace("\\u002F", "/")
        .replace("\\u002f", "/")
        .replace("\\u003A", ":")
        .replace("\\u003a", ":")
        .replace("\\u0026", "&")
        .replace("\\u003D", "=")
        .replace("\\u003d", "=")
        .replace("\\/", "/")
    )


def clean_candidate_url(url):
    return str(url or "").strip().lstrip("'\"").rstrip(")'\",")


def resolve_candidate_url(raw_url, base_url=None):
    candidate = clean_candidate_url(raw_url)
    if not candidate:
        return None

    try:
        if re.match(r"^https?://", candidate, re.I):
            return candidate

        if candidate.startswith("//"):
            if base_url:
                protocol = requests.utils.urlparse(base_url).scheme or "https"
            else:
                protocol = "https"
            return f"{protocol}:{candidate}"

        if base_url and re.match(r"^(?:/|\./|\.\./)", candidate):
            return urljoin(base_url, candidate)
    except Exception:
        return None

    return None


def extract_interesting_urls_from_text(value, base_url=None):
    normalized = normalize_text_for_extraction(value)
    found = []
    seen = set()

    def push_candidate(candidate):
        resolved = resolve_candidate_url(candidate, base_url)
        kind = get_media_kind(resolved)
        if not resolved or not kind or resolved in seen:
            return

        seen.add(resolved)
        found.append(resolved)

    direct_patterns = [
        r'https?://[^\s"\'<>\\]+?\.m3u8[^\s"\'<>\\]*',
        r'https?://[^\s"\'<>\\]+?/playlist/m3u8[^\s"\'<>\\]*',
        r'https?://[^\s"\'<>\\]+?\.(?:ass|srt|vtt)[^\s"\'<>\\]*',
        r'//[^\s"\'<>\\]+?\.m3u8[^\s"\'<>\\]*',
        r'//[^\s"\'<>\\]+?/playlist/m3u8[^\s"\'<>\\]*',
        r'//[^\s"\'<>\\]+?\.(?:ass|srt|vtt)[^\s"\'<>\\]*',
    ]

    for pattern in direct_patterns:
        for match in re.findall(pattern, normalized, re.I):
            push_candidate(match)

    query_patterns = [
        r'(?:^|[?&])(?:m3u8URL|playurl|playUrl|masterUrl|master_url|subtitle|subtitleUrl|subtitle_url)=([^&#]+)',
        r'"(?:m3u8URL|playurl|playUrl|masterUrl|master_url|subtitle|subtitleUrl|subtitle_url)"\s*:\s*"([^"]+)"',
    ]

    for pattern in query_patterns:
        for match in re.finditer(pattern, normalized, re.I):
            push_candidate(match.group(1))

    return found


def should_inspect_response(url, mime_type="", status=200):
    if not url or status >= 400 or get_media_kind(url):
        return False

    lower_url = str(url).lower()
    lower_type = str(mime_type or "").lower()
    return (
        any(token in lower_type for token in ("json", "javascript", "text", "xml"))
        or re.search(r"acs\.youku|ups\.youku|mtop|playlist|playlog|strategy|subtitle|media|stream", lower_url)
    )


def decode_response_body(body, base64_encoded=False):
    if not body:
        return ""

    if not base64_encoded:
        return body

    try:
        return b64decode(body).decode("utf-8", errors="ignore")
    except Exception:
        return ""


def format_duration(seconds):
    if not isinstance(seconds, (int, float)) or seconds <= 0:
        return "未知时长"

    total_seconds = int(round(seconds))
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    remaining_seconds = total_seconds % 60

    if hours > 0:
        return f"{hours}:{minutes:02d}:{remaining_seconds:02d}"

    return f"{minutes}:{remaining_seconds:02d}"


def parse_attribute(line, attribute):
    match = re.search(rf'{attribute}="([^"]+)"', line)
    return match.group(1) if match else ""


def get_query_number(url, key):
    try:
        value = requests.utils.urlparse(url).query
    except Exception:
        return None

    values = parse_qs(value).get(key)
    if not values:
        return None
    raw_value = values[0]

    try:
        number = float(raw_value)
    except Exception:
        return None

    return int(number) if number.is_integer() else number


def parse_segment_number(url):
    return get_query_number(url, "ts_seg_no")


def build_segment_key(url):
    segment_no = parse_segment_number(url)
    if segment_no is not None:
        return f"seg:{segment_no}"

    ts_start = get_query_number(url, "ts_start")
    ts_end = get_query_number(url, "ts_end")
    if ts_start is not None or ts_end is not None:
        return f"range:{ts_start if ts_start is not None else 'x'}-{ts_end if ts_end is not None else 'x'}"

    return url


def resolve_url(url, base_url):
    if re.match(r"^https?://", url, re.I):
        return url

    if url.startswith("//"):
        protocol = requests.utils.urlparse(base_url).scheme or "https"
        return f"{protocol}:{url}"

    return urljoin(base_url, url)


def choose_best_variant(lines, playlist_url):
    variants = []

    for index, line in enumerate(lines):
        if not line.startswith("#EXT-X-STREAM-INF:"):
            continue

        next_line = None
        for candidate in lines[index + 1:]:
            if candidate and not candidate.startswith("#"):
                next_line = candidate
                break

        if not next_line:
            continue

        bandwidth_match = re.search(r"BANDWIDTH=(\d+)", line, re.I)
        resolution_match = re.search(r"RESOLUTION=(\d+)x(\d+)", line, re.I)
        width = int(resolution_match.group(1)) if resolution_match else 0
        height = int(resolution_match.group(2)) if resolution_match else 0

        variants.append({
            "url": resolve_url(next_line, playlist_url),
            "bandwidth": int(bandwidth_match.group(1)) if bandwidth_match else 0,
            "width": width,
            "height": height,
            "area": width * height,
        })

    if not variants:
        raise RuntimeError("主清单里没有可用的码率分支。")

    variants.sort(key=lambda item: (item["bandwidth"], item["area"]), reverse=True)
    return variants[0]


def parse_media_playlist(text, playlist_url):
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    segment_entries = []
    init_segment = None
    encrypted = False
    pending_duration = None

    for line in lines:
        if line.startswith("#EXTINF:"):
            duration_match = re.match(r"^#EXTINF:([0-9.]+)", line, re.I)
            pending_duration = float(duration_match.group(1)) if duration_match else None
            continue

        if line.startswith("#EXT-X-KEY"):
            encrypted = True
            continue

        if line.startswith("#EXT-X-MAP:"):
            uri = parse_attribute(line, "URI")
            if uri:
                init_segment = resolve_url(uri, playlist_url)
            continue

        if line.startswith("#"):
            continue

        url = resolve_url(line, playlist_url)
        segment_entries.append({
            "url": url,
            "key": build_segment_key(url),
            "segment_no": parse_segment_number(url),
            "ts_start": get_query_number(url, "ts_start"),
            "ts_end": get_query_number(url, "ts_end"),
            "duration": pending_duration,
        })
        pending_duration = None

    return {
        "encrypted": encrypted,
        "init_segment": init_segment,
        "segment_entries": segment_entries,
        "segments": [entry["url"] for entry in segment_entries],
        "total_duration": sum((entry["duration"] or 0) for entry in segment_entries),
    }


def fetch_text(url, session=None, timeout=DEFAULT_TIMEOUT):
    client = session or requests
    response = client.get(url, timeout=timeout)
    response.raise_for_status()
    response.encoding = response.encoding or "utf-8"
    return response.text


def load_playlist(url, session=None, timeout=DEFAULT_TIMEOUT, max_depth=5):
    current_url = url
    selected_variant = None

    for _ in range(max_depth):
        text = fetch_text(current_url, session=session, timeout=timeout)
        lines = [line.strip() for line in text.splitlines()]

        if any(line.startswith("#EXT-X-STREAM-INF:") for line in lines):
            variant = choose_best_variant(lines, current_url)
            selected_variant = variant
            current_url = variant["url"]
            continue

        playlist = parse_media_playlist(text, current_url)
        playlist["request_url"] = url
        playlist["playlist_url"] = current_url
        playlist["selected_variant"] = selected_variant
        return playlist

    raise RuntimeError("主清单层级过深，停止继续解析。")


def compare_segment_entries(left, right):
    if left.get("segment_no") is not None and right.get("segment_no") is not None:
        if left["segment_no"] != right["segment_no"]:
            return -1 if left["segment_no"] < right["segment_no"] else 1

    if left.get("ts_start") is not None and right.get("ts_start") is not None:
        if left["ts_start"] != right["ts_start"]:
            return -1 if left["ts_start"] < right["ts_start"] else 1

    if left.get("ts_end") is not None and right.get("ts_end") is not None:
        if left["ts_end"] != right["ts_end"]:
            return -1 if left["ts_end"] < right["ts_end"] else 1

    return -1 if left.get("_order", 0) < right.get("_order", 0) else 1


def summarize_coverage(playlist):
    count = len(playlist.get("segment_entries") or playlist.get("segments") or [])
    segment_numbers = [
        entry["segment_no"]
        for entry in playlist.get("segment_entries", [])
        if entry.get("segment_no") is not None
    ]
    if segment_numbers:
        return (
            f"{count} 段 / {format_duration(playlist.get('total_duration', 0))} / "
            f"ts_seg_no {min(segment_numbers)}-{max(segment_numbers)}"
        )

    return f"{count} 段 / {format_duration(playlist.get('total_duration', 0))}"


def summarize_quality(playlist):
    variant = playlist.get("selected_variant") or {}
    width = variant.get("width") or 0
    height = variant.get("height") or 0
    bandwidth = variant.get("bandwidth") or 0
    parts = []

    if width and height:
        parts.append(f"{width}x{height}")
    if bandwidth:
        parts.append(f"{bandwidth / 1000:.0f} kbps")

    return " / ".join(parts) if parts else "清晰度未知"


def summarize_playlist_diagnostic(playlist):
    return f"{summarize_coverage(playlist)} / {summarize_quality(playlist)}"


def pick_best_playlist(playlists):
    def sort_key(item):
        first_segment = next(
            (entry.get("segment_no") for entry in item.get("segment_entries", []) if entry.get("segment_no") is not None),
            float("inf"),
        )
        return (
            item.get("total_duration", 0),
            len(item.get("segment_entries", [])),
            -first_segment,
        )

    return sorted(playlists, key=sort_key, reverse=True)[0]


def merge_playlists(playlists):
    seed = pick_best_playlist(playlists)
    init_segment = next((playlist.get("init_segment") for playlist in playlists if playlist.get("init_segment")), None)
    by_key = {}

    for playlist_index, playlist in enumerate(playlists):
        for entry_index, entry in enumerate(playlist.get("segment_entries", [])):
            enriched = dict(entry)
            enriched["_order"] = playlist_index * 10000 + entry_index
            current = by_key.get(enriched["key"])
            if current is None:
                by_key[enriched["key"]] = enriched
                continue

            if current.get("duration") is None and enriched.get("duration") is not None:
                current["duration"] = enriched["duration"]
            if current.get("ts_start") is None and enriched.get("ts_start") is not None:
                current["ts_start"] = enriched["ts_start"]
            if current.get("ts_end") is None and enriched.get("ts_end") is not None:
                current["ts_end"] = enriched["ts_end"]

    segment_entries = sorted(
        by_key.values(),
        key=lambda item: (
            item.get("segment_no") if item.get("segment_no") is not None else math.inf,
            item.get("ts_start") if item.get("ts_start") is not None else math.inf,
            item.get("ts_end") if item.get("ts_end") is not None else math.inf,
            item.get("_order", 0),
        ),
    )

    return {
        **seed,
        "init_segment": init_segment,
        "segment_entries": segment_entries,
        "segments": [entry["url"] for entry in segment_entries],
        "total_duration": sum((entry.get("duration") or 0) for entry in segment_entries),
    }


def load_best_playlist(candidate_urls, session=None, timeout=DEFAULT_TIMEOUT, limit=12):
    unique_urls = []
    seen = set()
    for url in candidate_urls:
        if not url or url in seen:
            continue
        seen.add(url)
        unique_urls.append(url)
        if len(unique_urls) >= limit:
            break

    playlists = []
    failures = []
    inspections = []

    for url in unique_urls:
        try:
            playlist = load_playlist(url, session=session, timeout=timeout)
            playlists.append(playlist)
            inspections.append({
                "url": url,
                "ok": True,
                "summary": summarize_playlist_diagnostic(playlist),
                "playlist_url": playlist.get("playlist_url"),
                "selected_variant": playlist.get("selected_variant"),
            })
        except Exception as exc:
            failures.append(str(exc))
            inspections.append({
                "url": url,
                "ok": False,
                "summary": str(exc),
                "playlist_url": "",
                "selected_variant": None,
            })

    if not playlists:
        raise RuntimeError(failures[0] if failures else "没有拿到可用的 m3u8 清单。")

    playable = [playlist for playlist in playlists if not playlist.get("encrypted")]
    if not playable:
        encrypted = playlists[0]
        return {
            "playlist": encrypted,
            "detail": f"检测到加密流: {summarize_coverage(encrypted)}",
            "candidate_count": len(unique_urls),
            "merged": False,
            "failures": failures,
            "inspections": inspections,
        }

    best_single = pick_best_playlist(playable)
    merged_playlist = merge_playlists(playable) if len(playable) > 1 else best_single
    use_merged = len(merged_playlist.get("segments", [])) > len(best_single.get("segments", []))
    playlist = merged_playlist if use_merged else best_single

    if use_merged:
        detail = f"已合并 {len(playable)} 条 m3u8，当前覆盖 {summarize_coverage(playlist)}"
    elif len(playable) > 1:
        detail = f"检测到 {len(playable)} 条 m3u8，已自动选择覆盖最完整的一条: {summarize_coverage(playlist)}"
    else:
        detail = summarize_coverage(playlist)

    return {
        "playlist": playlist,
        "detail": detail,
        "candidate_count": len(unique_urls),
        "merged": use_merged,
        "failures": failures,
        "inspections": inspections,
    }


def build_local_playlist_text(playlist):
    segment_entries = playlist.get("segment_entries", [])
    if not segment_entries:
        raise RuntimeError("清单里没有可用分片。")

    max_duration = max(
        max((entry.get("duration") or 0) for entry in segment_entries),
        1,
    )

    lines = [
        "#EXTM3U",
        "#EXT-X-VERSION:3",
        f"#EXT-X-TARGETDURATION:{int(math.ceil(max_duration))}",
        "#EXT-X-MEDIA-SEQUENCE:0",
    ]

    if playlist.get("init_segment"):
        lines.append(f'#EXT-X-MAP:URI="{playlist["init_segment"]}"')

    for entry in segment_entries:
        duration = entry.get("duration")
        if duration is None:
            duration = 0
        lines.append(f"#EXTINF:{float(duration):.3f},")
        lines.append(entry["url"])

    lines.append("#EXT-X-ENDLIST")
    return "\n".join(lines) + "\n"
