# YoukuDownload

一个面向本地环境的优酷视频下载工具。

它会拉起你自己的 Chrome，复用你的登录态，自动从视频页面抓取可用的 `m3u8` 清单和字幕链接。

抓到清单后，程序会先把视频分片下载到本地，再用本地 `ffconcat` / 本地 HLS 清单稳定输出 `mp4`，不再直接把远程 HLS 清单交给 `ffmpeg` 处理。

对于优酷经常出现的“同一条清单里重复附带多轮时间轴”问题，程序会自动识别时间轴重启，只保留覆盖最长的一轮有效分片，避免后半段重复、卡顿或时长异常。

默认运行模式是流水线：

- 浏览器页面抓取始终串行，避免多个 Chrome 实例互相抢登录态
- 抓到某条视频的清单后，会立刻进入后台分片下载队列
- 主流程会继续读取下一个视频
- 分片下载完成后，再进入 MP4 导出队列


## 环境要求

- Python 3.10+
- Google Chrome
- `ffmpeg`
- `ffprobe`

`ffprobe` 通常会随 `ffmpeg` 一起安装。

## 部署

### 1. 克隆项目

```bash
git clone <your-repo-url>
cd YoukuDownload
```

### 2. 创建虚拟环境并安装依赖

macOS / Linux:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Windows PowerShell:

```powershell
py -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 3. 安装 ffmpeg

macOS:

```bash
brew install ffmpeg
```

Ubuntu / Debian:

```bash
sudo apt update
sudo apt install ffmpeg
```

Windows:

- 安装可用版本的 `ffmpeg`
- 确保 `ffmpeg` 和 `ffprobe` 在系统 `PATH` 中

也可以把可执行文件直接放到：

- `tools/ffmpeg`
- `tools/ffprobe`
- `tools/ffmpeg.exe`
- `tools/ffprobe.exe`

### 4. 运行

最简单的方式是直接传视频链接：

```bash
python3 main.py "https://v.youku.com/v_show/..."
```

如果你想指定输出目录名：

```bash
python3 main.py --group "我的下载" "https://v.youku.com/v_show/..."
```

兼容旧版调用顺序：

```bash
python3 main.py "我的下载" "https://v.youku.com/v_show/..."
```

也可以直接传失败清单重新跑：

```bash
python3 main.py --group "我的下载-retry" "/绝对路径/failed.csv"
```

## 首次登录

首次运行时程序会打开：

- `https://account.youku.com/`

如果浏览器自动跳转到：

- `https://www.youku.com/ku/webhome...`

就会被视为登录成功。

登录态默认会保存在：

- `/Volumes/Lexar E300 Media/dance/.chrome-profile/`

如果你通过 `YOUKU_DOWNLOAD_ROOT` 改过下载目录，登录态也会跟着放到对应根目录下的 `.chrome-profile/`。  
如果登录态异常，删除这个目录后重新运行即可。

## 输出目录

运行后会生成：

```text
/Volumes/Lexar E300 Media/dance/
  <分组名>/
    video.csv
    failed.csv
    failed_urls.txt
    1_<标题>.m3u8
    1_<标题>.ass
    1_<标题>.mp4
    .media-cache/
```

说明：

- `video.csv`：任务状态表，可恢复中断任务
- `failed.csv`：失败项清单，包含失败阶段和错误信息
- `failed_urls.txt`：失败项 URL 列表
- `.m3u8`：本地生成且已清洗过时间轴的播放清单
- `.ass`：抓到字幕时才会生成
- `.mp4`：最终输出文件
- `.media-cache/`：分片下载缓存，默认成功后会自动清理；设置 `YOUKU_KEEP_MEDIA_CACHE=1` 可强制保留

为了便于断点续跑和排查问题，`.m3u8` 与字幕文件默认不会在成功后自动删除。

## 常用环境变量

```bash
export YOUKU_SCAN_TIMEOUT=180
export YOUKU_PAGE_LOAD_TIMEOUT=25
export YOUKU_CAPTURE_TIMEOUT=60
export YOUKU_MANUAL_PLAY_HINT_AFTER=8
export YOUKU_OPEN_URL_RETRY_COUNT=3
export YOUKU_OPEN_URL_RETRY_DELAY=2
export YOUKU_PREPARE_WORKERS=3
export YOUKU_CONVERT_WORKERS=1
export YOUKU_KEEP_MEDIA_CACHE=0
```

含义：

- `YOUKU_SCAN_TIMEOUT`：扫码登录最长等待秒数
- `YOUKU_PAGE_LOAD_TIMEOUT`：单次页面打开最长等待秒数
- `YOUKU_CAPTURE_TIMEOUT`：视频页抓取候选资源最长等待秒数
- `YOUKU_MANUAL_PLAY_HINT_AFTER`：多久后提示你手动点击一次播放
- `YOUKU_OPEN_URL_RETRY_COUNT`：视频页打开失败时的自动重试次数
- `YOUKU_OPEN_URL_RETRY_DELAY`：每次重试之间的等待秒数
- `YOUKU_PREPARE_WORKERS`：后台分片下载线程数，默认 `3`
- `YOUKU_CONVERT_WORKERS`：后台 MP4 导出线程数，默认 `1`
- `YOUKU_KEEP_MEDIA_CACHE`：是否保留本地分片缓存，默认 `0`

## 常见问题

### 1. 打开视频页时报 `ERR_CONNECTION_CLOSED`

项目已经内置自动重试，并会禁用 Chrome 的 `QUIC`。  
如果仍然出现，通常是网络环境、代理、站点风控或本地浏览器状态导致。可以尝试：

- 关闭程序自动拉起的 Chrome 后重新运行
- 删除 `download/.chrome-profile/` 后重新扫码
- 检查本地代理、抓包软件、VPN 或防火墙

### 2. 已经登录，但一直抓不到 `m3u8`

有些页面必须真正开始播放后才会吐出清单。  
程序会自动尝试起播；如果还不行，请在打开的浏览器里手动点一次播放。

### 3. 某条视频转换超时或失败

程序会把失败项导出到：

- `/Volumes/Lexar E300 Media/dance/<分组>/failed.csv`
- `/Volumes/Lexar E300 Media/dance/<分组>/failed_urls.txt`

重新跑时直接把 `failed.csv` 当输入即可。

如果本地已经存在该视频的 `.m3u8` 或 `.media-cache` 分片缓存，程序会优先复用本地内容继续转换。  
如果同一条 `.m3u8` 里混进了多轮重复时间轴，程序会在重开时自动重新清洗，不需要你手工改清单。


## 已知限制

- 遇到加密 HLS（`#EXT-X-KEY`）时，当前不会自动处理
- 如果优酷改版，页面选择器或抓取规则可能需要更新
- 这个项目依赖你自己的浏览器环境和登录态，不适合作为免登录服务端下载器

## 免责声明

本项目仅用于学习浏览器自动化、HLS 抓取与本地转封装流程。  
请自行确保你的使用行为符合当地法律法规、优酷服务条款以及内容版权要求。
