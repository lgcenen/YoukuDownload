import argparse
import os
from queue import Queue
from threading import Thread

import lib.chromeCatch as chromeCatch
import lib.videoDownload as videoDownload

DEFAULT_VIDEO_GROUP_NAME = 'youku-download'
PREPARE_WORKER_COUNT = max(1, int(os.getenv('YOUKU_PREPARE_WORKERS', '3')))
CONVERT_WORKER_COUNT = max(1, int(os.getenv('YOUKU_CONVERT_WORKERS', '1')))


def parse_args():
    parser = argparse.ArgumentParser(description='优酷视频下载工具')
    parser.add_argument(
        'arg1',
        nargs='?',
        help='视频链接、搜索页链接、失败清单文件，或旧版调用方式里的下载目录名称',
    )
    parser.add_argument(
        'arg2',
        nargs='?',
        help='旧版调用方式里的优酷视频页或合集页地址',
    )
    parser.add_argument(
        '--group',
        dest='video_group_name',
        default='',
        help='下载目录名称，默认使用 youku-download',
    )
    return parser.parse_args()


def resolve_cli_args(args):
    if args.arg2:
        videoGroupName = args.video_group_name or args.arg1 or DEFAULT_VIDEO_GROUP_NAME
        videoHomeUrl = args.arg2
        return videoGroupName, videoHomeUrl

    if args.arg1:
        videoGroupName = args.video_group_name or DEFAULT_VIDEO_GROUP_NAME
        videoHomeUrl = args.arg1
        return videoGroupName, videoHomeUrl

    raise SystemExit('请提供优酷视频页、搜索页或失败清单文件。示例：python3 main.py --group 验证 "https://v.youku.com/v_show/..."')


def prepare_worker(videoHelper, prepareQueue, convertQueue):
    while True:
        record = prepareQueue.get()
        if record is None:
            prepareQueue.task_done()
            return

        try:
            print('开始下载分片：第 {0} 条视频 {1}'.format(record.index, record.title))
            videoHelper.markConvertStarted(record)
            preparedSource = videoHelper.prepareRecordMedia(record)
            print(
                '第 {0} 条视频分片准备完成：{1}'.format(
                    record.index,
                    preparedSource.get('label', '本地分片缓存'),
                )
            )
            convertQueue.put(record)
        except Exception as error:
            videoHelper.markConvertFailed(record, error)
            print('第 {0} 条视频分片准备失败：{1}'.format(record.index, error))
        finally:
            prepareQueue.task_done()


def convert_worker(videoHelper, convertQueue):
    while True:
        record = convertQueue.get()
        if record is None:
            convertQueue.task_done()
            return

        try:
            print('开始输出 MP4：第 {0} 条视频 {1}'.format(record.index, record.title))
            videoHelper.markConvertStarted(record)
            mp4Path = videoHelper.convertRecordToMp4(record)
            videoHelper.markConvertSuccess(record, mp4Path)
            print('第 {0} 条视频 MP4 输出完成：{1}'.format(record.index, mp4Path))
        except Exception as error:
            videoHelper.markConvertFailed(record, error)
            print('第 {0} 条视频 MP4 输出失败：{1}'.format(record.index, error))
        finally:
            convertQueue.task_done()


def start(videoGroupName, videoHomeUrl):
    videoHelper = videoDownload.VideoDownload(videoGroupName, videoHomeUrl)
    try:
        records = videoHelper.syncVideoCsv()
    except Exception as error:
        chromeCatch.ChromeCatch.close()
        raise SystemExit('任务初始化失败：{0}'.format(error))
    print('任务列表已准备，共 {0} 条视频。'.format(len(records)))
    print(
        '已启用流水线：页面抓取串行，后台分片下载 {0} 线程，MP4 导出 {1} 线程。'.format(
            PREPARE_WORKER_COUNT,
            CONVERT_WORKER_COUNT,
        )
    )

    prepareQueue = Queue()
    convertQueue = Queue()
    queuedPrepareIndexes = set()

    def enqueue_prepare(record):
        if record.index in queuedPrepareIndexes:
            return
        queuedPrepareIndexes.add(record.index)
        prepareQueue.put(record)

    prepareThreads = [
        Thread(
            target=prepare_worker,
            args=(videoHelper, prepareQueue, convertQueue),
            name='prepare-worker-{0}'.format(index + 1),
            daemon=True,
        )
        for index in range(PREPARE_WORKER_COUNT)
    ]
    convertThreads = [
        Thread(
            target=convert_worker,
            args=(videoHelper, convertQueue),
            name='convert-worker-{0}'.format(index + 1),
            daemon=True,
        )
        for index in range(CONVERT_WORKER_COUNT)
    ]

    for thread in prepareThreads + convertThreads:
        thread.start()

    try:
        for record in videoHelper.getReadyForPrepareRecords():
            enqueue_prepare(record)

        while True:
            record = videoHelper.getPendingCaptureRecord()
            if record is None:
                break

            print('开始处理第 {0} 条视频：{1}'.format(record.index, record.title))
            chromeHandler = chromeCatch.ChromeCatch(
                record.index,
                record.title,
                record.url,
                videoGroupName,
            )
            videoHelper.markCaptureStarted(record)

            try:
                chromeHandler.login()
                chromeHandler.downloadVideoMidFile()
            except Exception as error:
                videoHelper.markCaptureFailed(record, error)
                print('第 {0} 条视频清单抓取失败：{1}'.format(record.index, error))
                continue

            videoHelper.markCaptureSuccess(record)
            print('第 {0} 条视频清单抓取完成。'.format(record.index))
            enqueue_prepare(record)

        prepareQueue.join()
        for _ in prepareThreads:
            prepareQueue.put(None)
        for thread in prepareThreads:
            thread.join()

        convertQueue.join()
        for _ in convertThreads:
            convertQueue.put(None)
        for thread in convertThreads:
            thread.join()
    finally:
        chromeCatch.ChromeCatch.close()

    summary = videoHelper.getSummary()
    failedManifest = videoHelper.exportFailedRecords()
    print('======================finish=======================')
    print(
        '共 {0} 条视频，清单完成 {1} 条，MP4 完成 {2} 条，失败 {3} 条。'.format(
            summary['total'],
            summary['captured'],
            summary['converted'],
            summary['failed'],
        )
    )
    if failedManifest['count'] > 0:
        print(
            '失败清单已导出：{0} 条。CSV: {1} | URL 列表: {2}'.format(
                failedManifest['count'],
                failedManifest['csv_path'],
                failedManifest['urls_path'],
            )
        )
        print(
            '重跑失败项示例：python3 main.py --group {0} "{1}"'.format(
                '{0}-retry'.format(videoGroupName),
                failedManifest['csv_path'],
            )
        )


if __name__ == "__main__":
    args = parse_args()
    videoGroupName, videoHomeUrl = resolve_cli_args(args)
    start(videoGroupName, videoHomeUrl)
