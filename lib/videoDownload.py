import csv
import re
import shutil
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

import lib.hls as hls

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DOWNLOAD_ROOT = PROJECT_ROOT / 'download'
DEFAULT_USER_AGENT = (
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
    'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36'
)
CSV_FIELDS = [
    'index',
    'title',
    'url',
    'capture_status',
    'convert_status',
    'mp4_path',
    'last_error',
    'updated_at',
]
STATUS_PENDING = 'pending'
STATUS_RUNNING = 'running'
STATUS_DONE = 'done'


@dataclass
class VideoRecord:
    index: int
    title: str
    url: str
    capture_status: str = STATUS_PENDING
    convert_status: str = STATUS_PENDING
    mp4_path: str = ''
    last_error: str = ''
    updated_at: str = ''

    def to_row(self):
        return {
            'index': str(self.index),
            'title': self.title,
            'url': self.url,
            'capture_status': self.capture_status,
            'convert_status': self.convert_status,
            'mp4_path': self.mp4_path,
            'last_error': self.last_error,
            'updated_at': self.updated_at,
        }

    @classmethod
    def from_row(cls, row):
        return cls(
            index=int(row.get('index') or 0),
            title=row.get('title', ''),
            url=row.get('url', ''),
            capture_status=row.get('capture_status') or STATUS_PENDING,
            convert_status=row.get('convert_status') or STATUS_PENDING,
            mp4_path=row.get('mp4_path', ''),
            last_error=row.get('last_error', ''),
            updated_at=row.get('updated_at', ''),
        )


class VideoDownload:
    def __init__(self, videoGroupName, videoUrl):
        self.__videoGroupName = self.__safeFileName(videoGroupName)
        self.__videoUrl = videoUrl

    def getCsvFile(self):
        return str(self.__groupDir() / 'video.csv')

    def syncVideoCsv(self):
        episodes = self.__fetchEpisodes()
        if not episodes:
            raise RuntimeError('未能从页面中解析到可下载的视频条目。')

        existingByUrl = {record.url: record for record in self.__loadRecords()}
        nextRecords = []

        for index, episode in enumerate(episodes, start=1):
            title = self.__safeFileName(episode['title'])
            url = episode['url']
            current = VideoRecord(index=index, title=title, url=url)
            previous = existingByUrl.get(url)

            if previous:
                current.capture_status = previous.capture_status
                current.convert_status = previous.convert_status
                current.mp4_path = previous.mp4_path
                current.last_error = previous.last_error
                current.updated_at = previous.updated_at

                if str(current.capture_status).startswith('fail:'):
                    current.capture_status = STATUS_PENDING
                if str(current.convert_status).startswith('fail:'):
                    current.convert_status = STATUS_PENDING

            self.__repairRecordArtifacts(current)
            nextRecords.append(current)

        self.__saveRecords(nextRecords)
        return nextRecords

    def getPendingCaptureRecord(self):
        for record in self.__loadRecords():
            if self.__repairRecordArtifacts(record):
                self.__updateRecord(record)
            if record.convert_status == STATUS_DONE:
                continue
            if record.capture_status in (STATUS_PENDING, STATUS_RUNNING):
                return record
        return None

    def getPendingConvertRecord(self):
        for record in self.__loadRecords():
            if self.__repairRecordArtifacts(record):
                self.__updateRecord(record)
            if record.capture_status == STATUS_DONE and record.convert_status in (STATUS_PENDING, STATUS_RUNNING):
                return record
        return None

    def markCaptureStarted(self, record):
        record.capture_status = STATUS_RUNNING
        record.convert_status = STATUS_PENDING
        record.last_error = ''
        record.updated_at = self.__now()
        self.__updateRecord(record)

    def markCaptureSuccess(self, record):
        record.capture_status = STATUS_DONE
        record.convert_status = STATUS_PENDING
        record.last_error = ''
        record.updated_at = self.__now()
        self.__updateRecord(record)

    def markCaptureFailed(self, record, error):
        record.capture_status = self.__statusError(error)
        record.convert_status = STATUS_PENDING
        record.last_error = str(error)
        record.updated_at = self.__now()
        self.__updateRecord(record)

    def markConvertStarted(self, record):
        record.convert_status = STATUS_RUNNING
        record.last_error = ''
        record.updated_at = self.__now()
        self.__updateRecord(record)

    def markConvertSuccess(self, record, mp4Path):
        record.convert_status = STATUS_DONE
        record.mp4_path = mp4Path
        record.last_error = ''
        record.updated_at = self.__now()
        self.__updateRecord(record)

    def markConvertFailed(self, record, error):
        record.convert_status = self.__statusError(error)
        record.last_error = str(error)
        record.updated_at = self.__now()
        self.__updateRecord(record)

    def getSummary(self):
        records = self.__loadRecords()
        changed = False
        for record in records:
            changed = self.__repairRecordArtifacts(record) or changed
        if changed:
            self.__saveRecords(records)
        return {
            'total': len(records),
            'captured': sum(record.capture_status == STATUS_DONE for record in records),
            'converted': sum(record.convert_status == STATUS_DONE for record in records),
            'failed': sum(
                record.capture_status.startswith('fail:')
                or record.convert_status.startswith('fail:')
                for record in records
            ),
        }

    def convertRecordToMp4(self, record):
        m3u8FilePath = Path(self.getFileM3u8Path(record.index, record.title))
        assFilePath = Path(self.getFileAssPath(record.index, record.title))
        mp4FilePath = Path(self.getFileMp4Path(record.index, record.title))

        if not m3u8FilePath.exists():
            raise RuntimeError('本地 m3u8 清单不存在，需要重新抓取。')

        expectedDuration = self.__readPlaylistDuration(m3u8FilePath)
        ffmpegPath = self.__getFfmpegPath()
        inputVariants = [
            {
                'name': '仅带 UA',
                'options': [
                    '-user_agent', DEFAULT_USER_AGENT,
                ],
            },
            {
                'name': '纯本地清单输入',
                'options': [],
            },
        ]

        failures = []
        subtitleEmbedded = False
        for inputVariant in inputVariants:
            commands = self.__buildFfmpegCommands(
                ffmpegPath=ffmpegPath,
                inputOptions=inputVariant['options'],
                m3u8FilePath=m3u8FilePath,
                assFilePath=assFilePath,
                mp4FilePath=mp4FilePath,
            )

            for item in commands:
                result = subprocess.run(
                    item['cmd'],
                    check=False,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
                if result.returncode != 0:
                    failures.append(
                        '{0} / {1}: {2}'.format(
                            inputVariant['name'],
                            item['name'],
                            self.__tailText(result.stderr),
                        )
                    )
                    continue

                if expectedDuration and not self.__durationLooksReasonable(mp4FilePath, expectedDuration):
                    actualDuration = self.__probeDuration(mp4FilePath)
                    failures.append(
                        '{0} / {1}: 输出时长异常，预期约 {2:.1f} 秒，实际 {3:.1f} 秒'.format(
                            inputVariant['name'],
                            item['name'],
                            expectedDuration,
                            actualDuration or -1,
                        )
                    )
                    self.__safeUnlink(mp4FilePath)
                    continue

                subtitleEmbedded = item['burns_subtitle']
                break

            if mp4FilePath.exists():
                break

        if not mp4FilePath.exists():
            raise RuntimeError(
                'ffmpeg 输出 mp4 失败：{0}'.format(
                    ' | '.join(failures[-3:]) if failures else '未知错误'
                )
            )

        self.__safeUnlink(m3u8FilePath)
        if subtitleEmbedded:
            self.__safeUnlink(assFilePath)

        return str(mp4FilePath)

    def __buildFfmpegCommands(self, ffmpegPath, inputOptions, m3u8FilePath, assFilePath, mp4FilePath):
        baseCmd = [
            ffmpegPath,
            '-protocol_whitelist', 'file,http,https,tcp,tls,crypto,data',
            '-allowed_extensions', 'ALL',
            '-fflags', '+genpts+discardcorrupt+igndts',
            '-analyzeduration', '100M',
            '-probesize', '100M',
            *inputOptions,
            '-i', str(m3u8FilePath),
            '-map', '0:v:0',
            '-map', '0:a?',
            '-movflags', '+faststart',
            '-avoid_negative_ts', 'make_zero',
            '-y',
        ]

        commands = []
        if assFilePath.exists():
            subtitlePath = assFilePath.resolve().as_posix().replace("'", r"\'")
            commands.append({
                'name': '烧录字幕并重新编码',
                'burns_subtitle': True,
                'cmd': baseCmd + [
                    '-vf', "subtitles='{0}'".format(subtitlePath),
                    '-c:v', 'libx264',
                    '-preset', 'veryfast',
                    '-crf', '20',
                    '-c:a', 'aac',
                    str(mp4FilePath),
                ],
            })

        commands.extend([
            {
                'name': '直接拷贝音视频流',
                'burns_subtitle': False,
                'cmd': baseCmd + [
                    '-c', 'copy',
                    str(mp4FilePath),
                ],
            },
            {
                'name': '直接拷贝音视频流并修正 AAC',
                'burns_subtitle': False,
                'cmd': baseCmd + [
                    '-c', 'copy',
                    '-bsf:a', 'aac_adtstoasc',
                    str(mp4FilePath),
                ],
            },
            {
                'name': '重新编码视频与音频',
                'burns_subtitle': False,
                'cmd': baseCmd + [
                    '-c:v', 'libx264',
                    '-preset', 'veryfast',
                    '-crf', '20',
                    '-c:a', 'aac',
                    str(mp4FilePath),
                ],
            },
        ])

        return commands

    def getFileM3u8Path(self, videoIndex, videoName):
        return str(self.__groupDir() / '{0}_{1}.m3u8'.format(videoIndex, videoName))

    def getFileAssPath(self, videoIndex, videoName):
        return str(self.__groupDir() / '{0}_{1}.ass'.format(videoIndex, videoName))

    def getFileMp4Path(self, videoIndex, videoName):
        return str(self.__groupDir() / '{0}_{1}.mp4'.format(videoIndex, videoName))

    def __fetchEpisodes(self):
        session = self.__buildPublicSession()
        response = session.get(self.__videoUrl, timeout=30)
        response.raise_for_status()
        pageContent = response.text
        soup = BeautifulSoup(pageContent, 'html.parser')

        showId = self.__readShowId(pageContent)
        episodes = self.__collectEpisodesFromDom(soup)

        if showId:
            playlistEpisodes = self.__fetchPlaylistEpisodes(session, showId)
            if playlistEpisodes:
                episodes = playlistEpisodes

        if episodes:
            return episodes

        title = soup.title.get_text(strip=True) if soup.title else self.__videoGroupName
        return [{
            'title': title,
            'url': self.__videoUrl,
        }]

    def __loadRecords(self):
        csvFile = Path(self.getCsvFile())
        if not csvFile.exists():
            return []

        with csvFile.open('r', newline='', encoding='utf-8') as videoCsv:
            rows = list(csv.reader(videoCsv))

        if not rows:
            return []

        if rows[0] == CSV_FIELDS:
            with csvFile.open('r', newline='', encoding='utf-8') as videoCsv:
                return [VideoRecord.from_row(row) for row in csv.DictReader(videoCsv)]

        records = []
        for row in rows:
            if len(row) < 3:
                continue
            record = VideoRecord(
                index=int(row[0]),
                title=row[1],
                url=row[2],
            )

            if len(row) >= 4:
                record.capture_status = STATUS_DONE if row[3] == 'm3u8' else (row[3] or STATUS_PENDING)
            if len(row) >= 5:
                record.convert_status = STATUS_DONE if row[4] == 'mp4' else (row[4] or STATUS_PENDING)
            if record.convert_status == STATUS_DONE:
                record.mp4_path = self.getFileMp4Path(record.index, record.title)

            records.append(record)

        return records

    def __saveRecords(self, records):
        csvFile = Path(self.getCsvFile())
        csvFile.parent.mkdir(parents=True, exist_ok=True)

        with csvFile.open('w', newline='', encoding='utf-8') as videoCsv:
            writer = csv.DictWriter(videoCsv, fieldnames=CSV_FIELDS)
            writer.writeheader()
            for record in records:
                writer.writerow(record.to_row())

    def __updateRecord(self, nextRecord):
        records = self.__loadRecords()
        updated = False

        for index, record in enumerate(records):
            if record.index != nextRecord.index:
                continue

            records[index] = nextRecord
            updated = True
            break

        if not updated:
            records.append(nextRecord)

        records.sort(key=lambda item: item.index)
        self.__saveRecords(records)

    def __repairRecordArtifacts(self, record):
        changed = False
        mp4FilePath = Path(self.getFileMp4Path(record.index, record.title))
        m3u8FilePath = Path(self.getFileM3u8Path(record.index, record.title))

        if record.convert_status == STATUS_DONE and not mp4FilePath.exists():
            record.convert_status = STATUS_PENDING
            record.mp4_path = ''
            changed = True

        if record.capture_status == STATUS_RUNNING:
            record.capture_status = STATUS_PENDING
            changed = True

        if record.convert_status == STATUS_RUNNING:
            record.convert_status = STATUS_PENDING
            changed = True

        if record.capture_status == STATUS_DONE and record.convert_status != STATUS_DONE and not m3u8FilePath.exists():
            record.capture_status = STATUS_PENDING
            changed = True

        expectedMp4Path = str(mp4FilePath) if record.convert_status == STATUS_DONE else ''
        if record.mp4_path != expectedMp4Path:
            record.mp4_path = expectedMp4Path
            changed = True

        return changed

    def __buildPublicSession(self):
        session = requests.Session()
        session.headers.update({
            'User-Agent': DEFAULT_USER_AGENT,
            'Referer': self.__videoUrl,
        })
        return session

    def __readShowId(self, pageContent):
        patterns = [
            r'showid:\s*[\'"]([^\'"]+)[\'"]',
            r'"showid"\s*:\s*"([^"]+)"',
            r'"showId"\s*:\s*"([^"]+)"',
        ]

        for pattern in patterns:
            match = re.search(pattern, pageContent, re.I)
            if match:
                return match.group(1)

        return ''

    def __collectEpisodesFromDom(self, soup):
        selectors = [
            '.anthology-wrap .anthology-content a.box-item',
            '.anthology-content a.box-item',
            'a[data-spm^="dplaypage.episode"]',
            'a[href*="/v_show/"]',
        ]

        for selector in selectors:
            anchors = soup.select(selector)
            seen = set()
            episodes = []

            for anchor in anchors:
                title = (anchor.get('title') or anchor.get_text(strip=True) or '').strip()
                href = anchor.get('href') or ''
                if not title or '/v_show/' not in href:
                    continue

                absoluteUrl = urljoin(self.__videoUrl, href)
                key = (title, absoluteUrl)
                if key in seen:
                    continue

                seen.add(key)
                episodes.append({
                    'title': title,
                    'url': absoluteUrl,
                })

            if episodes:
                return episodes[:80]

        return []

    def __fetchPlaylistEpisodes(self, session, showId):
        episodes = []
        seen = set()

        for pageIndex in range(1, 21):
            playlistUrl = (
                'https://v.youku.com/page/playlist?showid={0}&isSimple=false&page={1}'
                .format(showId, pageIndex)
            )
            response = session.get(playlistUrl, timeout=30)
            if not response.ok:
                break

            try:
                data = response.json()
            except ValueError:
                break

            html = data.get('html')
            if not html:
                break

            soup = BeautifulSoup(html, 'html.parser')
            anchors = soup.select('div.item.item-cover a.sn')
            if not anchors:
                break

            for anchor in anchors:
                title = (anchor.get('title') or anchor.get_text(strip=True) or '').strip()
                href = anchor.get('href') or ''
                if not title or not href:
                    continue

                absoluteUrl = urljoin('https://v.youku.com', href)
                key = (title, absoluteUrl)
                if key in seen:
                    continue

                seen.add(key)
                episodes.append({
                    'title': title,
                    'url': absoluteUrl,
                })

        return episodes[:80]

    def __readPlaylistDuration(self, playlistPath):
        try:
            playlistText = Path(playlistPath).read_text(encoding='utf-8')
        except Exception:
            return None

        try:
            playlist = hls.parse_media_playlist(playlistText, str(playlistPath))
        except Exception:
            return None

        duration = playlist.get('total_duration') or 0
        return duration if duration > 0 else None

    def __durationLooksReasonable(self, mp4Path, expectedDuration):
        actualDuration = self.__probeDuration(mp4Path)
        if not actualDuration or expectedDuration <= 0:
            return True

        ratio = actualDuration / expectedDuration
        tolerance = max(4.0, expectedDuration * 0.08)
        if abs(actualDuration - expectedDuration) <= tolerance:
            return True

        return 0.7 <= ratio <= 1.3

    def __probeDuration(self, videoPath):
        ffprobePath = self.__getFfprobePath()
        if not ffprobePath:
            return None

        result = subprocess.run(
            [
                ffprobePath,
                '-v', 'error',
                '-show_entries', 'format=duration',
                '-of', 'default=noprint_wrappers=1:nokey=1',
                str(videoPath),
            ],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        if result.returncode != 0:
            return None

        try:
            return float(result.stdout.strip())
        except Exception:
            return None

    def __getFfmpegPath(self):
        candidates = [
            PROJECT_ROOT / 'tools' / 'ffmpeg',
            PROJECT_ROOT / 'tools' / 'ffmpeg.exe',
        ]

        for item in candidates:
            if item.exists():
                return str(item)

        ffmpegPath = shutil.which('ffmpeg')
        if ffmpegPath:
            return ffmpegPath

        raise FileNotFoundError('未找到 ffmpeg，请先安装 ffmpeg 或将其放到 tools 目录中。')

    def __getFfprobePath(self):
        candidates = [
            PROJECT_ROOT / 'tools' / 'ffprobe',
            PROJECT_ROOT / 'tools' / 'ffprobe.exe',
        ]

        for item in candidates:
            if item.exists():
                return str(item)

        return shutil.which('ffprobe')

    def __groupDir(self):
        path = DOWNLOAD_ROOT / self.__videoGroupName
        path.mkdir(parents=True, exist_ok=True)
        return path

    def __safeFileName(self, fileName):
        value = re.sub(r'[\/:*?"<>|]', '-', str(fileName or '').strip())
        value = re.sub(r'\s+', ' ', value).strip()
        return value or 'youku-video'

    def __statusError(self, error):
        return 'fail:{0}'.format(str(error).strip().replace('\n', ' ')[:240])

    def __tailText(self, value, limit=8):
        lines = [line.strip() for line in str(value or '').splitlines() if line.strip()]
        if not lines:
            return '无详细输出'
        return ' / '.join(lines[-limit:])

    def __safeUnlink(self, path):
        try:
            Path(path).unlink()
        except FileNotFoundError:
            return

    def __now(self):
        return time.strftime('%Y-%m-%d %H:%M:%S')
