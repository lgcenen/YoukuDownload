import argparse

import lib.chromeCatch as chromeCatch
import lib.videoDownload as videoDownload

DEFAULT_VIDEO_GROUP_NAME = 'youku-download'


def parse_args():
    parser = argparse.ArgumentParser(description='优酷视频下载工具')
    parser.add_argument(
        'arg1',
        nargs='?',
        help='视频链接，或旧版调用方式里的下载目录名称',
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

    raise SystemExit('请提供优酷视频页或合集页地址。示例：python3 main.py --group 验证 "https://v.youku.com/v_show/..."')


def start(videoGroupName, videoHomeUrl):
    videoHelper = videoDownload.VideoDownload(videoGroupName, videoHomeUrl)
    records = videoHelper.syncVideoCsv()
    print('任务列表已准备，共 {0} 条视频。'.format(len(records)))

    try:
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

        while True:
            record = videoHelper.getPendingConvertRecord()
            if record is None:
                break

            print('开始输出 MP4：第 {0} 条视频 {1}'.format(record.index, record.title))
            videoHelper.markConvertStarted(record)

            try:
                mp4Path = videoHelper.convertRecordToMp4(record)
            except Exception as error:
                videoHelper.markConvertFailed(record, error)
                print('第 {0} 条视频 MP4 输出失败：{1}'.format(record.index, error))
                continue

            videoHelper.markConvertSuccess(record, mp4Path)
            print('第 {0} 条视频 MP4 输出完成：{1}'.format(record.index, mp4Path))
    finally:
        chromeCatch.ChromeCatch.close()

    summary = videoHelper.getSummary()
    print('======================finish=======================')
    print(
        '共 {0} 条视频，清单完成 {1} 条，MP4 完成 {2} 条，失败 {3} 条。'.format(
            summary['total'],
            summary['captured'],
            summary['converted'],
            summary['failed'],
        )
    )


if __name__ == "__main__":
    args = parse_args()
    videoGroupName, videoHomeUrl = resolve_cli_args(args)
    start(videoGroupName, videoHomeUrl)
