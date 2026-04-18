import math
import os
import re
from base64 import b64decode
from urllib.parse import parse_qs
from urllib.parse import urljoin

import requests

DEFAULT_TIMEOUT = 30
ALLOW_PLAYLIST_MERGE = str(os.getenv("YOUKU_ALLOW_PLAYLIST_MERGE", "0")).strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
QUALITY_TYPE_MAP = {
    "hd3": {"width": 1920, "height": 1080, "bandwidth": 3000000, "label": "1080p"},
    "mp4hd3": {"width": 1920, "height": 1080, "bandwidth": 3000000, "label": "1080p"},
    "1080p": {"width": 1920, "height": 1080, "bandwidth": 3000000, "label": "1080p"},
    "hd2": {"width": 1280, "height": 720, "bandwidth": 1500000, "label": "720p"},
    "mp4hd2": {"width": 1280, "height": 720, "bandwidth": 1500000, "label": "720p"},
    "720p": {"width": 1280, "height": 720, "bandwidth": 1500000, "label": "720p"},
    "hd": {"width": 854, "height": 480, "bandwidth": 800000, "label": "480p"},
    "mp4hd": {"width": 854, "height": 480, "bandwidth": 800000, "label": "480p"},
    "flvhd": {"width": 854, "height": 480, "bandwidth": 800000, "label": "480p"},
    "480p": {"width": 854, "height": 480, "bandwidth": 800000, "label": "480p"},
    "mp4sd": {"width": 640, "height": 360, "bandwidth": 450000, "label": "360p"},
    "3gphd": {"width": 640, "height": 360, "bandwidth": 450000, "label": "360p"},
    "360p": {"width": 640, "height": 360, "bandwidth": 450000, "label": "360p"},
}


def get_media_kind(url):
    lower = str(url or "").lower()
    try:
        parsed = requests.utils.urlparse(str(url or ""))
        path = str(parsed.path or "").lower()
    except Exception:
        path = lower

    if ".m3u8" in path or re.search(r"/(?:playlist/)?m3u8(?:$|/)", path):
        return "m3u8"

    if any(ext in path for ext in (".ass", ".srt", ".vtt")):
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
        r'(?:^|[?&])(?:m3u8URL|playurl|playUrl|masterUrl|master_url|subtitle|subtitleUrl|subtitle_url)=((?:https?:)?//[^\s"\'<>\\]+)',
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


def infer_variant_from_url(url):
    query = {}
    try:
        query = parse_qs(requests.utils.urlparse(str(url or "")).query)
    except Exception:
        query = {}

    quality_type = ""
    for key in ("type", "quality", "stream_type", "streamType"):
        values = query.get(key)
        if values and values[0]:
            quality_type = str(values[0]).strip().lower()
            break

    if not quality_type:
        return None

    base = QUALITY_TYPE_MAP.get(quality_type)
    if not base:
        return None

    return {
        "url": str(url or ""),
        "bandwidth": base["bandwidth"],
        "width": base["width"],
        "height": base["height"],
        "area": base["width"] * base["height"],
        "label": base["label"],
        "source": "query-type",
    }


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
    raw_segment_entries = []
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
        raw_segment_entries.append({
            "url": url,
            "key": build_segment_key(url),
            "segment_no": parse_segment_number(url),
            "ts_start": get_query_number(url, "ts_start"),
            "ts_end": get_query_number(url, "ts_end"),
            "duration": pending_duration,
        })
        pending_duration = None

    segment_entries, normalization = normalize_segment_entries(raw_segment_entries)

    playlist = {
        "encrypted": encrypted,
        "init_segment": init_segment,
        "segment_entries": segment_entries,
        "segments": [entry["url"] for entry in segment_entries],
        "total_duration": sum((entry["duration"] or 0) for entry in segment_entries),
        "normalization": normalization,
    }
    playlist["integrity"] = analyze_playlist_integrity(playlist)
    return playlist


def is_cycle_restart(previous_entry, current_entry):
    previous_start = previous_entry.get("ts_start")
    previous_end = previous_entry.get("ts_end")
    current_start = current_entry.get("ts_start")

    if not isinstance(current_start, (int, float)):
        return False

    if (
        isinstance(previous_start, (int, float))
        and current_start + 0.5 < previous_start
    ):
        return True

    if not isinstance(previous_end, (int, float)):
        return False

    if current_start <= 1.0 and previous_end >= 30:
        return True

    return current_start + 8 < previous_end and current_start <= 5


def split_segment_cycles(segment_entries):
    if not segment_entries:
        return []

    cycles = []
    current_cycle = [dict(segment_entries[0])]
    previous_entry = segment_entries[0]

    for entry in segment_entries[1:]:
        if is_cycle_restart(previous_entry, entry):
            cycles.append(current_cycle)
            current_cycle = [dict(entry)]
        else:
            current_cycle.append(dict(entry))
        previous_entry = entry

    if current_cycle:
        cycles.append(current_cycle)

    return cycles


def get_cycle_sort_key(entries):
    max_ts_end = max(
        (entry.get("ts_end") or 0 for entry in entries if isinstance(entry.get("ts_end"), (int, float))),
        default=0,
    )
    total_duration = sum((entry.get("duration") or 0) for entry in entries)
    max_segment_no = max(
        (entry.get("segment_no") for entry in entries if entry.get("segment_no") is not None),
        default=-1,
    )
    return (
        max_ts_end,
        total_duration,
        len(entries),
        max_segment_no,
    )


def dedupe_cycle_entries(entries):
    normalized = []
    seen_keys = set()
    dropped = 0

    for entry in entries:
        key = entry.get("key")
        if key and key in seen_keys:
            dropped += 1
            continue

        if normalized:
            previous_entry = normalized[-1]
            previous_segment_no = previous_entry.get("segment_no")
            current_segment_no = entry.get("segment_no")

            if (
                previous_segment_no is not None
                and current_segment_no is not None
                and current_segment_no < previous_segment_no
            ):
                dropped += 1
                continue

            previous_start = previous_entry.get("ts_start")
            current_start = entry.get("ts_start")
            if (
                isinstance(previous_start, (int, float))
                and isinstance(current_start, (int, float))
                and current_start + 0.5 < previous_start
            ):
                dropped += 1
                continue

            previous_end = previous_entry.get("ts_end")
            current_end = entry.get("ts_end")
            if (
                isinstance(previous_end, (int, float))
                and isinstance(current_end, (int, float))
                and current_end <= previous_end + 0.2
            ):
                dropped += 1
                continue

        normalized.append(dict(entry))
        if key:
            seen_keys.add(key)

    return normalized, dropped


def normalize_segment_entries(segment_entries):
    raw_entries = [dict(entry) for entry in segment_entries]
    if not raw_entries:
        return [], {
            "raw_segment_count": 0,
            "normalized_segment_count": 0,
            "cycle_count": 0,
            "selected_cycle_index": 0,
            "discarded_segment_count": 0,
            "raw_total_duration": 0,
            "normalized_total_duration": 0,
            "note": "",
        }

    cycles = split_segment_cycles(raw_entries)
    selected_cycle_index = 0
    if cycles:
        selected_cycle_index = max(range(len(cycles)), key=lambda index: get_cycle_sort_key(cycles[index]))
        selected_cycle = cycles[selected_cycle_index]
    else:
        selected_cycle = raw_entries
        cycles = [selected_cycle]

    normalized_entries, dropped_in_cycle = dedupe_cycle_entries(selected_cycle)
    normalized_total_duration = sum((entry.get("duration") or 0) for entry in normalized_entries)
    raw_total_duration = sum((entry.get("duration") or 0) for entry in raw_entries)
    discarded_segment_count = len(raw_entries) - len(normalized_entries)

    notes = []
    if len(cycles) > 1:
        notes.append(
            "检测到 {0} 轮重复时间轴，已保留覆盖最长的一轮".format(len(cycles))
        )
    if dropped_in_cycle:
        notes.append("已额外剔除 {0} 个重复或倒退分片".format(dropped_in_cycle))

    return normalized_entries, {
        "raw_segment_count": len(raw_entries),
        "normalized_segment_count": len(normalized_entries),
        "cycle_count": len(cycles),
        "selected_cycle_index": selected_cycle_index + 1,
        "discarded_segment_count": discarded_segment_count,
        "raw_total_duration": raw_total_duration,
        "normalized_total_duration": normalized_total_duration,
        "note": "；".join(notes),
    }


def fetch_text(url, session=None, timeout=DEFAULT_TIMEOUT):
    client = session or requests
    response = client.get(url, timeout=timeout)
    response.raise_for_status()
    response.encoding = response.encoding or "utf-8"
    return response.text


def load_playlist(url, session=None, timeout=DEFAULT_TIMEOUT, max_depth=5):
    current_url = url
    selected_variant = infer_variant_from_url(url)

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
        playlist["selected_variant"] = selected_variant or infer_variant_from_url(current_url)
        playlist["integrity"] = analyze_playlist_integrity(playlist)
        return playlist

    raise RuntimeError("主清单层级过深，停止继续解析。")


def analyze_playlist_integrity(playlist):
    entries = list(playlist.get("segment_entries") or [])
    report = {
        "segment_count": len(entries),
        "duplicate_keys": 0,
        "duplicate_segment_numbers": 0,
        "backward_segment_numbers": 0,
        "backward_timestamps": 0,
        "overlaps": 0,
        "gaps": 0,
    }

    seen_keys = set()
    seen_segment_numbers = set()
    previous_entry = None

    for entry in entries:
        key = entry.get("key")
        if key:
            if key in seen_keys:
                report["duplicate_keys"] += 1
            seen_keys.add(key)

        segment_no = entry.get("segment_no")
        if segment_no is not None:
            if segment_no in seen_segment_numbers:
                report["duplicate_segment_numbers"] += 1
            seen_segment_numbers.add(segment_no)

        if previous_entry is not None:
            previous_segment_no = previous_entry.get("segment_no")
            if (
                previous_segment_no is not None
                and segment_no is not None
                and segment_no < previous_segment_no
            ):
                report["backward_segment_numbers"] += 1

            previous_end = previous_entry.get("ts_end")
            current_start = entry.get("ts_start")
            if isinstance(previous_end, (int, float)) and isinstance(current_start, (int, float)):
                if current_start + 0.2 < previous_end:
                    report["overlaps"] += 1
                elif current_start > previous_end + 1.5:
                    report["gaps"] += 1

            previous_start = previous_entry.get("ts_start")
            if isinstance(previous_start, (int, float)) and isinstance(current_start, (int, float)):
                if current_start + 0.2 < previous_start:
                    report["backward_timestamps"] += 1

        previous_entry = entry

    report["critical_issue_count"] = (
        report["duplicate_keys"]
        + report["duplicate_segment_numbers"]
        + report["backward_segment_numbers"]
        + report["backward_timestamps"]
        + report["overlaps"]
    )
    report["warning_issue_count"] = report["gaps"]
    report["safe_for_binary_concat"] = report["critical_issue_count"] == 0
    report["safe_for_merge"] = report["critical_issue_count"] == 0
    return report


def summarize_integrity(playlist):
    report = playlist.get("integrity") or analyze_playlist_integrity(playlist)
    critical = report.get("critical_issue_count", 0)
    warnings = report.get("warning_issue_count", 0)
    if not critical and not warnings:
        return "时间轴稳定"

    issues = []
    if report.get("overlaps"):
        issues.append(f'{report["overlaps"]} 处重叠')
    if report.get("duplicate_segment_numbers"):
        issues.append(f'{report["duplicate_segment_numbers"]} 处重复分片号')
    if report.get("duplicate_keys"):
        issues.append(f'{report["duplicate_keys"]} 处重复分片')
    if report.get("backward_timestamps"):
        issues.append(f'{report["backward_timestamps"]} 处时间回退')
    if report.get("backward_segment_numbers"):
        issues.append(f'{report["backward_segment_numbers"]} 处分片号回退')
    if report.get("gaps"):
        issues.append(f'{report["gaps"]} 处缺口')
    return "；".join(issues)


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
    label = variant.get("label") or ""
    parts = []

    if width and height:
        parts.append(f"{width}x{height}")
    elif label:
        parts.append(label)
    if bandwidth:
        parts.append(f"{bandwidth / 1000:.0f} kbps")

    return " / ".join(parts) if parts else "清晰度未知"


def summarize_playlist_diagnostic(playlist):
    summary = (
        f"{summarize_coverage(playlist)} / "
        f"{summarize_quality(playlist)} / "
        f"{summarize_integrity(playlist)}"
    )
    note = (playlist.get("normalization") or {}).get("note")
    if note:
        return f"{summary} / {note}"
    return summary


def get_playlist_sort_key(item):
    integrity = item.get("integrity") or analyze_playlist_integrity(item)
    first_segment = next(
        (entry.get("segment_no") for entry in item.get("segment_entries", []) if entry.get("segment_no") is not None),
        float("inf"),
    )
    variant = item.get("selected_variant") or {}
    bandwidth = variant.get("bandwidth") or 0
    area = variant.get("area") or 0
    height = variant.get("height") or 0
    width = variant.get("width") or 0
    return (
        -integrity.get("critical_issue_count", 0),
        -integrity.get("warning_issue_count", 0),
        item.get("total_duration", 0),
        len(item.get("segment_entries", [])),
        bandwidth,
        area,
        height,
        width,
        -first_segment,
    )


def pick_best_playlist(playlists):
    return sorted(playlists, key=get_playlist_sort_key, reverse=True)[0]


def merge_playlists(playlists):
    ranked_playlists = sorted(playlists, key=get_playlist_sort_key, reverse=True)
    seed = ranked_playlists[0]
    init_segment = next((playlist.get("init_segment") for playlist in playlists if playlist.get("init_segment")), None)
    by_key = {}

    for playlist_index, playlist in enumerate(ranked_playlists):
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

    merged = {
        **seed,
        "init_segment": init_segment,
        "segment_entries": segment_entries,
        "segments": [entry["url"] for entry in segment_entries],
        "total_duration": sum((entry.get("duration") or 0) for entry in segment_entries),
    }
    merged["integrity"] = analyze_playlist_integrity(merged)
    return merged


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
            if not playlist.get("encrypted") and not playlist.get("segment_entries"):
                raise RuntimeError("清单为空或缺少有效分片。")
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
    playlist = best_single
    use_merged = False
    merge_reason = ""

    if len(playable) > 1:
        if not ALLOW_PLAYLIST_MERGE:
            merge_reason = "为避免跨候选拼重，已默认禁用多清单合并"
        else:
            merged_playlist = merge_playlists(playable)
            merged_integrity = merged_playlist.get("integrity") or analyze_playlist_integrity(merged_playlist)
            if not merged_integrity.get("safe_for_merge"):
                merge_reason = "检测到合并后存在时间轴重叠/回退风险，已放弃合并"
            elif len(merged_playlist.get("segments", [])) > len(best_single.get("segments", [])):
                playlist = merged_playlist
                use_merged = True
            else:
                merge_reason = "最佳单清单已经覆盖完整，未启用跨候选合并"

    if use_merged:
        detail = f"已严格合并 {len(playable)} 条 m3u8，当前覆盖 {summarize_playlist_diagnostic(playlist)}"
    elif len(playable) > 1:
        detail = (
            f"检测到 {len(playable)} 条 m3u8，已优先选择时间轴最稳定且画质最高的一条: "
            f"{summarize_playlist_diagnostic(playlist)}"
        )
        if merge_reason:
            detail = f"{detail}；{merge_reason}"
    else:
        detail = summarize_playlist_diagnostic(playlist)

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

    previous_entry = None
    for entry in segment_entries:
        if previous_entry and should_insert_discontinuity(previous_entry, entry):
            lines.append("#EXT-X-DISCONTINUITY")
        duration = entry.get("duration")
        if duration is None:
            duration = 0
        lines.append(f"#EXTINF:{float(duration):.3f},")
        lines.append(entry["url"])
        previous_entry = entry

    lines.append("#EXT-X-ENDLIST")
    return "\n".join(lines) + "\n"


def should_insert_discontinuity(previous_entry, current_entry):
    previous_start = previous_entry.get("ts_start")
    current_start = current_entry.get("ts_start")
    previous_end = previous_entry.get("ts_end")

    if (
        isinstance(previous_start, (int, float))
        and isinstance(current_start, (int, float))
        and current_start + 0.5 < previous_start
    ):
        return True

    if (
        isinstance(previous_end, (int, float))
        and isinstance(current_start, (int, float))
        and current_start + 0.5 < previous_end
    ):
        return True

    return False
