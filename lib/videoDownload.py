import csv
import os
import re
import shutil
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from threading import RLock
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

import lib.chromeCatch as chromeCatch
import lib.hls as hls

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DOWNLOAD_ROOT = PROJECT_ROOT / 'download'
DOWNLOAD_ROOT = Path(
    os.getenv('YOUKU_DOWNLOAD_ROOT', str(DEFAULT_DOWNLOAD_ROOT))
).expanduser()
DEFAULT_USER_AGENT = (
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
    'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36'
)
FFMPEG_PROCESS_TIMEOUT = int(os.getenv('YOUKU_FFMPEG_PROCESS_TIMEOUT', '420'))
FFMPEG_DURATION_FIX_PADDING = float(os.getenv('YOUKU_FFMPEG_DURATION_FIX_PADDING', '1.2'))
DEFAULT_OUTPUT_FPS = float(os.getenv('YOUKU_OUTPUT_FPS', '30'))
KEEP_MEDIA_CACHE = str(os.getenv('YOUKU_KEEP_MEDIA_CACHE', '0')).strip().lower() in {
    '1',
    'true',
    'yes',
    'on',
}
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
FAILED_CSV_FIELDS = [
    'index',
    'title',
    'url',
    'stage',
    'status',
    'error',
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
        self.__recordLock = RLock()

    def getCsvFile(self):
        return str(self.__groupDir() / 'video.csv')

    def getFailedCsvFile(self):
        return str(self.__groupDir() / 'failed.csv')

    def getFailedUrlsFile(self):
        return str(self.__groupDir() / 'failed_urls.txt')

    def syncVideoCsv(self):
        with self.__recordLock:
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
        with self.__recordLock:
            for record in self.__loadRecords():
                if self.__repairRecordArtifacts(record):
                    self.__updateRecord(record)
                if record.convert_status == STATUS_DONE:
                    continue
                if record.capture_status in (STATUS_PENDING, STATUS_RUNNING):
                    return record
            return None

    def getPendingConvertRecord(self):
        with self.__recordLock:
            for record in self.__loadRecords():
                if self.__repairRecordArtifacts(record):
                    self.__updateRecord(record)
                if record.capture_status == STATUS_DONE and record.convert_status in (STATUS_PENDING, STATUS_RUNNING):
                    return record
            return None

    def getReadyForPrepareRecords(self):
        ready = []
        with self.__recordLock:
            records = self.__loadRecords()
            changed = False
            for record in records:
                changed = self.__repairRecordArtifacts(record) or changed
                if record.capture_status == STATUS_DONE and record.convert_status != STATUS_DONE:
                    ready.append(record)
            if changed:
                self.__saveRecords(records)
        return ready

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
        with self.__recordLock:
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

    def exportFailedRecords(self):
        with self.__recordLock:
            records = self.__loadRecords()
            failedRows = []

            for record in records:
                if self.__repairRecordArtifacts(record):
                    self.__updateRecord(record)

                stage = ''
                status = ''
                if str(record.capture_status).startswith('fail:'):
                    stage = 'capture'
                    status = record.capture_status
                elif str(record.convert_status).startswith('fail:'):
                    stage = 'convert'
                    status = record.convert_status

                if not stage:
                    continue

                failedRows.append({
                    'index': str(record.index),
                    'title': record.title,
                    'url': record.url,
                    'stage': stage,
                    'status': status,
                    'error': record.last_error or status,
                    'updated_at': record.updated_at,
                })

        failedCsvPath = Path(self.getFailedCsvFile())
        failedUrlsPath = Path(self.getFailedUrlsFile())

        if not failedRows:
            self.__safeUnlink(failedCsvPath)
            self.__safeUnlink(failedUrlsPath)
            return {
                'count': 0,
                'csv_path': str(failedCsvPath),
                'urls_path': str(failedUrlsPath),
            }

        failedCsvPath.parent.mkdir(parents=True, exist_ok=True)
        with failedCsvPath.open('w', newline='', encoding='utf-8') as failedCsv:
            writer = csv.DictWriter(failedCsv, fieldnames=FAILED_CSV_FIELDS)
            writer.writeheader()
            for row in failedRows:
                writer.writerow(row)

        with failedUrlsPath.open('w', encoding='utf-8') as failedUrls:
            for row in failedRows:
                failedUrls.write('{0}\n'.format(row['url']))

        return {
            'count': len(failedRows),
            'csv_path': str(failedCsvPath),
            'urls_path': str(failedUrlsPath),
        }

    def convertRecordToMp4(self, record):
        m3u8FilePath = Path(self.getFileM3u8Path(record.index, record.title))
        assFilePath = Path(self.getFileAssPath(record.index, record.title))
        mp4FilePath = Path(self.getFileMp4Path(record.index, record.title))

        if not m3u8FilePath.exists():
            raise RuntimeError('本地 m3u8 清单不存在，需要重新抓取。')

        playlist = self.__loadLocalPlaylist(m3u8FilePath)
        expectedDuration = playlist.get('total_duration') or None
        ffmpegPath = self.__getFfmpegPath()
        preparedSource = self.__ensurePreparedOfflineMediaSource(record, playlist)
        targetFrameRate = (
            self.__probeFrameRate(preparedSource['path'])
            or self.__probeFrameRate(m3u8FilePath)
            or DEFAULT_OUTPUT_FPS
        )

        failures = []
        subtitleEmbedded = False
        commands = self.__buildOfflineFfmpegCommands(
            ffmpegPath=ffmpegPath,
            sourceInfo=preparedSource,
            assFilePath=assFilePath,
            mp4FilePath=mp4FilePath,
            targetFrameRate=targetFrameRate,
        )

        for item in commands:
            print(
                'FFmpeg 尝试：第 {0} 条视频 {1} / {2}'.format(
                    record.index,
                    preparedSource['label'],
                    item['name'],
                )
            )
            self.__safeUnlink(mp4FilePath)

            try:
                result = subprocess.run(
                    item['cmd'],
                    check=False,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    timeout=FFMPEG_PROCESS_TIMEOUT,
                )
            except subprocess.TimeoutExpired as error:
                self.__safeUnlink(mp4FilePath)
                raise TimeoutError(
                    '{0} / {1}: ffmpeg 执行超时（>{2} 秒）{3}'.format(
                        preparedSource['label'],
                        item['name'],
                        FFMPEG_PROCESS_TIMEOUT,
                        ' / {0}'.format(self.__tailText(error.stderr)) if error.stderr else '',
                    )
                )

            if result.returncode != 0:
                failures.append(
                    '{0} / {1}: {2}'.format(
                        preparedSource['label'],
                        item['name'],
                        self.__tailText(result.stderr),
                    )
                )
                continue

            if expectedDuration and not self.__durationLooksReasonable(mp4FilePath, expectedDuration):
                actualDuration = self.__probeDuration(mp4FilePath)
                if (
                    actualDuration
                    and actualDuration > expectedDuration
                    and self.__repairLongOutputDuration(
                        ffmpegPath=ffmpegPath,
                        mp4FilePath=mp4FilePath,
                        expectedDuration=expectedDuration,
                        actualDuration=actualDuration,
                        targetFrameRate=targetFrameRate,
                    )
                    and self.__durationLooksReasonable(mp4FilePath, expectedDuration)
                ):
                    subtitleEmbedded = item['burns_subtitle']
                    break

                fixedDuration = self.__probeDuration(mp4FilePath)
                failures.append(
                    '{0} / {1}: 输出时长异常，预期约 {2:.1f} 秒，实际 {3:.1f} 秒'.format(
                        preparedSource['label'],
                        item['name'],
                        expectedDuration,
                        fixedDuration or actualDuration or -1,
                    )
                )
                self.__safeUnlink(mp4FilePath)
                continue

            subtitleEmbedded = item['burns_subtitle']
            break

        if not mp4FilePath.exists():
            raise RuntimeError(
                'ffmpeg 输出 mp4 失败：{0}'.format(
                    ' | '.join(failures[-3:]) if failures else '未知错误'
                )
            )

        if not KEEP_MEDIA_CACHE:
            self.__safeRmtree(preparedSource['work_dir'])

        return str(mp4FilePath)

    def prepareRecordMedia(self, record):
        m3u8FilePath = Path(self.getFileM3u8Path(record.index, record.title))
        if not m3u8FilePath.exists():
            raise RuntimeError('本地 m3u8 清单不存在，需要重新抓取。')

        playlist = self.__loadLocalPlaylist(m3u8FilePath)
        return self.__ensurePreparedOfflineMediaSource(record, playlist)

    def __buildOfflineFfmpegCommands(
        self,
        ffmpegPath,
        sourceInfo,
        assFilePath,
        mp4FilePath,
        targetFrameRate=DEFAULT_OUTPUT_FPS,
    ):
        baseCmd = [ffmpegPath]
        if sourceInfo['type'] == 'playlist':
            baseCmd.extend([
                '-protocol_whitelist', 'file,crypto,data',
                '-allowed_extensions', 'ALL',
            ])
        elif sourceInfo['type'] == 'concat':
            baseCmd.extend([
                '-f', 'concat',
                '-safe', '0',
            ])

        baseCmd.extend([
            '-fflags', '+genpts+discardcorrupt+igndts',
            '-analyzeduration', '100M',
            '-probesize', '100M',
            '-i', str(sourceInfo['path']),
            '-map', '0:v:0',
            '-map', '0:a?',
            '-movflags', '+faststart',
            '-avoid_negative_ts', 'make_zero',
            '-y',
        ])
        reencodeAvOptions = [
            '-fps_mode', 'cfr',
            '-r', self.__formatFrameRate(targetFrameRate),
            '-af', 'aresample=async=1:first_pts=0',
        ]
        videoFilters = [
            'setpts=PTS-STARTPTS',
            'fps={0}'.format(self.__formatFrameRate(targetFrameRate)),
        ]

        commands = []
        if assFilePath.exists():
            subtitlePath = assFilePath.resolve().as_posix().replace("'", r"\'")
            subtitleFilters = list(videoFilters)
            subtitleFilters.append("subtitles='{0}'".format(subtitlePath))
            commands.append({
                'name': '硬件重编码并稳定帧率',
                'burns_subtitle': True,
                'cmd': baseCmd + [
                    '-vf', ','.join(subtitleFilters),
                    '-c:v', 'h264_videotoolbox',
                    '-b:v', '6M',
                    '-maxrate', '8M',
                    '-bufsize', '12M',
                    '-c:a', 'aac',
                    *reencodeAvOptions,
                    str(mp4FilePath),
                ],
            })
            commands.append({
                'name': '软件重编码并稳定帧率',
                'burns_subtitle': True,
                'cmd': baseCmd + [
                    '-vf', ','.join(subtitleFilters),
                    '-c:v', 'libx264',
                    '-preset', 'veryfast',
                    '-crf', '20',
                    '-c:a', 'aac',
                    *reencodeAvOptions,
                    str(mp4FilePath),
                ],
            })
            return commands

        commands = [
            {
                'name': '硬件重编码并稳定帧率',
                'burns_subtitle': False,
                'cmd': baseCmd + [
                    '-vf', ','.join(videoFilters),
                    '-c:v', 'h264_videotoolbox',
                    '-b:v', '6M',
                    '-maxrate', '8M',
                    '-bufsize', '12M',
                    '-c:a', 'aac',
                    *reencodeAvOptions,
                    str(mp4FilePath),
                ],
            },
            {
                'name': '软件重编码并稳定帧率',
                'burns_subtitle': False,
                'cmd': baseCmd + [
                    '-vf', ','.join(videoFilters),
                    '-c:v', 'libx264',
                    '-preset', 'veryfast',
                    '-crf', '20',
                    '-c:a', 'aac',
                    *reencodeAvOptions,
                    str(mp4FilePath),
                ],
            },
        ]

        return commands

    def __loadLocalPlaylist(self, m3u8FilePath):
        playlistText = Path(m3u8FilePath).read_text(encoding='utf-8')
        playlist = hls.parse_media_playlist(playlistText, str(m3u8FilePath))
        normalizedText = hls.build_local_playlist_text(playlist)
        if normalizedText != playlistText:
            Path(m3u8FilePath).write_text(normalizedText, encoding='utf-8')
        return playlist

    def __ensurePreparedOfflineMediaSource(self, record, playlist):
        cached = self.__locatePreparedOfflineMediaSource(record, playlist)
        if cached:
            return cached
        return self.__prepareOfflineMediaSource(record, playlist)

    def __locatePreparedOfflineMediaSource(self, record, playlist):
        workDir = self.__getMediaWorkDir(record.index, record.title)
        if not workDir.exists():
            return None

        concatPath = workDir / 'source.ffconcat'
        if concatPath.exists():
            return {
                'type': 'concat',
                'path': concatPath,
                'work_dir': workDir,
                'label': '本地分片 concat',
            }

        localPlaylistPath = workDir / 'source.m3u8'
        if localPlaylistPath.exists():
            return {
                'type': 'playlist',
                'path': localPlaylistPath,
                'work_dir': workDir,
                'label': '本地分片清单',
            }

        return None

    def __prepareOfflineMediaSource(self, record, playlist):
        workDir = self.__getMediaWorkDir(record.index, record.title)
        workDir.mkdir(parents=True, exist_ok=True)
        session = self.__buildSegmentSession(record.url)
        integrity = playlist.get('integrity') or hls.analyze_playlist_integrity(playlist)

        localPlaylist = {
            **playlist,
            'segment_entries': [],
            'segments': [],
            'integrity': integrity,
        }

        if playlist.get('init_segment'):
            initPath = workDir / self.__buildLocalMediaFileName('init', 0, playlist['init_segment'], defaultExt='.mp4')
            self.__downloadUrlToFile(session, playlist['init_segment'], initPath)
            localPlaylist['init_segment'] = initPath.name
        else:
            localPlaylist['init_segment'] = None

        downloadedSegmentFiles = []
        for index, entry in enumerate(playlist.get('segment_entries', []), start=1):
            localPath = workDir / self.__buildLocalMediaFileName('seg', index, entry['url'])
            self.__downloadUrlToFile(session, entry['url'], localPath)
            downloadedSegmentFiles.append(localPath)

            localEntry = dict(entry)
            localEntry['url'] = localPath.name
            localPlaylist['segment_entries'].append(localEntry)
            localPlaylist['segments'].append(localPath.name)

        if self.__canUseConcatDemuxer(localPlaylist, downloadedSegmentFiles):
            concatPath = workDir / 'source.ffconcat'
            self.__writeConcatManifest(localPlaylist, downloadedSegmentFiles, concatPath)
            return {
                'type': 'concat',
                'path': concatPath,
                'work_dir': workDir,
                'label': '本地分片 concat',
            }

        localPlaylistPath = workDir / 'source.m3u8'
        localPlaylistPath.write_text(
            hls.build_local_playlist_text(localPlaylist),
            encoding='utf-8',
        )
        return {
            'type': 'playlist',
            'path': localPlaylistPath,
            'work_dir': workDir,
            'label': '本地分片清单',
        }

    def __buildSegmentSession(self, refererUrl):
        session = requests.Session()
        session.headers.update({
            'User-Agent': DEFAULT_USER_AGENT,
            'Referer': refererUrl,
        })
        return session

    def __buildLocalMediaFileName(self, prefix, index, url, defaultExt='.ts'):
        try:
            suffixes = Path(requests.utils.urlparse(str(url or '')).path).suffixes
        except Exception:
            suffixes = []

        suffix = suffixes[-1] if suffixes else defaultExt
        if suffix.lower() not in {'.ts', '.m2ts', '.mts', '.m4s', '.mp4', '.aac'}:
            suffix = defaultExt
        return '{0}_{1:05d}{2}'.format(prefix, index, suffix)

    def __downloadUrlToFile(self, session, url, targetPath, timeout=30, retries=3):
        if targetPath.exists() and targetPath.stat().st_size > 0:
            return

        partPath = Path(str(targetPath) + '.part')
        self.__safeUnlink(partPath)

        lastError = None
        for _ in range(retries):
            try:
                with session.get(url, timeout=timeout, stream=True) as response:
                    response.raise_for_status()
                    with partPath.open('wb') as outputFile:
                        for chunk in response.iter_content(chunk_size=1024 * 256):
                            if not chunk:
                                continue
                            outputFile.write(chunk)

                if not partPath.exists() or partPath.stat().st_size <= 0:
                    raise RuntimeError('下载后的分片文件为空。')

                partPath.replace(targetPath)
                return
            except Exception as error:
                lastError = error
                self.__safeUnlink(partPath)
                time.sleep(1)

        raise RuntimeError('分片下载失败：{0}'.format(lastError))

    def __canUseConcatDemuxer(self, playlist, segmentFiles):
        if playlist.get('init_segment'):
            return False

        if not segmentFiles:
            return False

        allowed = {'.ts', '.m2ts', '.mts'}
        return all(item.suffix.lower() in allowed for item in segmentFiles)

    def __writeConcatManifest(self, playlist, segmentFiles, concatPath):
        lines = ['ffconcat version 1.0']
        entries = list(playlist.get('segment_entries') or [])

        for index, segmentPath in enumerate(segmentFiles):
            absolutePath = segmentPath.resolve().as_posix()
            lines.append("file '{0}'".format(self.__escapeConcatPath(absolutePath)))

            if index >= len(entries) - 1:
                continue

            duration = entries[index].get('duration')
            if isinstance(duration, (int, float)) and duration > 0:
                lines.append('duration {0:.6f}'.format(float(duration)))

        concatPath.write_text('\n'.join(lines) + '\n', encoding='utf-8')

    def __escapeConcatPath(self, value):
        return str(value or '').replace("'", r"'\''")

    def getFileM3u8Path(self, videoIndex, videoName):
        return str(self.__groupDir() / '{0}_{1}.m3u8'.format(videoIndex, videoName))

    def getFileAssPath(self, videoIndex, videoName):
        return str(self.__groupDir() / '{0}_{1}.ass'.format(videoIndex, videoName))

    def getFileMp4Path(self, videoIndex, videoName):
        return str(self.__groupDir() / '{0}_{1}.mp4'.format(videoIndex, videoName))

    def __getMediaWorkDir(self, videoIndex, videoName):
        return self.__groupDir() / '.media-cache' / '{0}_{1}'.format(videoIndex, videoName)

    def __fetchEpisodes(self):
        if self.__isEpisodeListFile(self.__videoUrl):
            episodes = self.__loadEpisodesFromFile(self.__videoUrl)
            if episodes:
                return episodes

            raise RuntimeError('输入文件里没有可下载的视频链接。')

        if self.__looksLikeLocalFilePath(self.__videoUrl):
            raise FileNotFoundError('输入文件不存在：{0}'.format(Path(str(self.__videoUrl)).expanduser()))

        if self.__isSearchUrl(self.__videoUrl):
            episodes = chromeCatch.ChromeCatch.collect_search_results(self.__videoUrl)
            if episodes:
                return episodes

            raise RuntimeError('搜索页未识别到任何可下载的视频。')

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

    def __isSearchUrl(self, url):
        return '/search' in str(url or '') and 'so.youku.com' in str(url or '')

    def __isEpisodeListFile(self, value):
        try:
            path = Path(str(value or '')).expanduser()
        except Exception:
            return False

        return path.is_file()

    def __looksLikeLocalFilePath(self, value):
        raw = str(value or '').strip()
        if not raw:
            return False

        if raw.startswith(('/', './', '../', '~')):
            return True

        suffix = Path(raw).suffix.lower()
        return suffix in {'.csv', '.txt'}

    def __loadEpisodesFromFile(self, filePath):
        path = Path(str(filePath)).expanduser()
        suffix = path.suffix.lower()

        if suffix == '.csv':
            return self.__loadEpisodesFromCsv(path)

        return self.__loadEpisodesFromText(path)

    def __loadEpisodesFromCsv(self, path):
        with path.open('r', newline='', encoding='utf-8') as sourceFile:
            reader = csv.DictReader(sourceFile)
            if not reader.fieldnames or 'url' not in reader.fieldnames:
                return []

            episodes = []
            seen = set()
            for index, row in enumerate(reader, start=1):
                url = str(row.get('url') or '').strip()
                if not url or url in seen:
                    continue

                seen.add(url)
                title = str(row.get('title') or '').strip() or 'retry-{0}'.format(index)
                episodes.append({
                    'title': title,
                    'url': url,
                })

        return episodes

    def __loadEpisodesFromText(self, path):
        episodes = []
        seen = set()
        for index, rawLine in enumerate(path.read_text(encoding='utf-8').splitlines(), start=1):
            url = rawLine.strip()
            if not url or url.startswith('#') or url in seen:
                continue

            seen.add(url)
            episodes.append({
                'title': 'retry-{0}'.format(index),
                'url': url,
            })

        return episodes

    def __loadRecords(self):
        with self.__recordLock:
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
        with self.__recordLock:
            csvFile = Path(self.getCsvFile())
            csvFile.parent.mkdir(parents=True, exist_ok=True)

            with csvFile.open('w', newline='', encoding='utf-8') as videoCsv:
                writer = csv.DictWriter(videoCsv, fieldnames=CSV_FIELDS)
                writer.writeheader()
                for record in records:
                    writer.writerow(record.to_row())

    def __updateRecord(self, nextRecord):
        with self.__recordLock:
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

        if m3u8FilePath.exists() and record.capture_status != STATUS_DONE:
            record.capture_status = STATUS_DONE
            changed = True

        if record.convert_status == STATUS_DONE and not mp4FilePath.exists():
            record.convert_status = STATUS_PENDING
            record.mp4_path = ''
            changed = True

        if record.capture_status == STATUS_RUNNING and not m3u8FilePath.exists():
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

    def __durationLooksReasonable(self, mp4Path, expectedDuration):
        actualDuration = self.__probeDuration(mp4Path)
        if not actualDuration or expectedDuration <= 0:
            return True

        ratio = actualDuration / expectedDuration
        tolerance = max(4.0, expectedDuration * 0.08)
        if abs(actualDuration - expectedDuration) <= tolerance:
            return True

        return 0.7 <= ratio <= 1.3

    def __repairLongOutputDuration(self, ffmpegPath, mp4FilePath, expectedDuration, actualDuration, targetFrameRate):
        tolerance = max(4.0, expectedDuration * 0.08)
        if actualDuration <= expectedDuration + tolerance:
            return False

        trimTarget = min(actualDuration, expectedDuration + FFMPEG_DURATION_FIX_PADDING)
        tempMp4Path = Path(str(mp4FilePath) + '.duration-fix.mp4')
        self.__safeUnlink(tempMp4Path)

        commands = [
            {
                'name': '直接拷贝裁切',
                'cmd': [
                    ffmpegPath,
                    '-i', str(mp4FilePath),
                    '-map', '0:v:0',
                    '-map', '0:a?',
                    '-t', f'{trimTarget:.3f}',
                    '-c', 'copy',
                    '-movflags', '+faststart',
                    '-avoid_negative_ts', 'make_zero',
                    '-y',
                    str(tempMp4Path),
                ],
            },
            {
                'name': '重新编码裁切',
                'cmd': [
                    ffmpegPath,
                    '-i', str(mp4FilePath),
                    '-map', '0:v:0',
                    '-map', '0:a?',
                    '-t', f'{trimTarget:.3f}',
                    '-vf', 'fps={0}'.format(self.__formatFrameRate(targetFrameRate)),
                    '-c:v', 'libx264',
                    '-preset', 'veryfast',
                    '-crf', '20',
                    '-c:a', 'aac',
                    '-af', 'aresample=async=1:first_pts=0',
                    '-fps_mode', 'cfr',
                    '-r', self.__formatFrameRate(targetFrameRate),
                    '-movflags', '+faststart',
                    '-y',
                    str(tempMp4Path),
                ],
            },
        ]

        for item in commands:
            print(
                'FFmpeg 时长修正：目标 {0:.1f} 秒 / 预期 {1:.1f} 秒 / {2}'.format(
                    trimTarget,
                    expectedDuration,
                    item['name'],
                )
            )
            self.__safeUnlink(tempMp4Path)

            try:
                result = subprocess.run(
                    item['cmd'],
                    check=False,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    timeout=FFMPEG_PROCESS_TIMEOUT,
                )
            except subprocess.TimeoutExpired as error:
                self.__safeUnlink(tempMp4Path)
                raise TimeoutError(
                    '时长修正 / {0}: ffmpeg 执行超时（>{1} 秒）{2}'.format(
                        item['name'],
                        FFMPEG_PROCESS_TIMEOUT,
                        ' / {0}'.format(self.__tailText(error.stderr)) if error.stderr else '',
                    )
                )

            if result.returncode != 0 or not tempMp4Path.exists():
                self.__safeUnlink(tempMp4Path)
                continue

            if not self.__durationLooksReasonable(tempMp4Path, expectedDuration):
                self.__safeUnlink(tempMp4Path)
                continue

            self.__safeUnlink(mp4FilePath)
            tempMp4Path.replace(mp4FilePath)
            return True

        self.__safeUnlink(tempMp4Path)
        return False

    def __probeFrameRate(self, videoPath):
        ffprobePath = self.__getFfprobePath()
        if not ffprobePath:
            return None

        result = subprocess.run(
            [
                ffprobePath,
                '-v', 'error',
                '-select_streams', 'v:0',
                '-show_entries', 'stream=avg_frame_rate,r_frame_rate',
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

        for line in [item.strip() for item in result.stdout.splitlines() if item.strip()]:
            value = self.__parseFrameRate(line)
            if value:
                return value

        return None

    def __parseFrameRate(self, value):
        raw = str(value or '').strip()
        if not raw or raw in ('0/0', 'N/A'):
            return None

        try:
            if '/' in raw:
                numerator, denominator = raw.split('/', 1)
                fps = float(numerator) / float(denominator)
            else:
                fps = float(raw)
        except Exception:
            return None

        if not 10 <= fps <= 120:
            return None

        return fps

    def __formatFrameRate(self, fps):
        value = fps or DEFAULT_OUTPUT_FPS
        return ('{0:.3f}'.format(float(value))).rstrip('0').rstrip('.')

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
        raw = value or ''
        if isinstance(raw, bytes):
            raw = raw.decode('utf-8', errors='ignore')
        else:
            raw = str(raw)

        lines = [line.strip() for line in raw.splitlines() if line.strip()]
        if not lines:
            return '无详细输出'

        compact = []
        for line in lines[-limit:]:
            compact.append(line if len(line) <= 220 else '{0}...'.format(line[:220]))
        return ' / '.join(compact)

    def __safeUnlink(self, path):
        try:
            Path(path).unlink()
        except FileNotFoundError:
            return

    def __safeRmtree(self, path):
        try:
            shutil.rmtree(path)
        except FileNotFoundError:
            return

    def __now(self):
        return time.strftime('%Y-%m-%d %H:%M:%S')
