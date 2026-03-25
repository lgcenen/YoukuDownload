# YoukuDownload

一个面向本地环境的优酷视频下载工具。

它会拉起你自己的 Chrome，复用你的登录态，自动从视频页面抓取可用的 `m3u8` 清单和字幕链接，再用本机 `ffmpeg` 输出 `mp4`。


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

## 首次登录

首次运行时程序会打开：

- `https://account.youku.com/`

如果浏览器自动跳转到：

- `https://www.youku.com/ku/webhome...`

就会被视为登录成功。

登录态会保存在：

- `download/.chrome-profile/`

如果登录态异常，删除 `download/.chrome-profile/` 后重新运行即可。

## 输出目录

运行后会生成：

```text
download/
  <分组名>/
    video.csv
    1_<标题>.m3u8
    1_<标题>.ass
    1_<标题>.mp4
```

说明：

- `video.csv`：任务状态表，可恢复中断任务
- `.m3u8`：本地生成的完整播放清单
- `.ass`：抓到字幕时才会生成
- `.mp4`：最终输出文件

如果字幕烧录成功，`.ass` 会自动清理；如果字幕烧录失败，字幕文件会保留，便于你手动处理。

## 常用环境变量

```bash
export YOUKU_SCAN_TIMEOUT=180
export YOUKU_PAGE_LOAD_TIMEOUT=25
export YOUKU_CAPTURE_TIMEOUT=60
export YOUKU_MANUAL_PLAY_HINT_AFTER=8
export YOUKU_OPEN_URL_RETRY_COUNT=3
export YOUKU_OPEN_URL_RETRY_DELAY=2
```

含义：

- `YOUKU_SCAN_TIMEOUT`：扫码登录最长等待秒数
- `YOUKU_PAGE_LOAD_TIMEOUT`：单次页面打开最长等待秒数
- `YOUKU_CAPTURE_TIMEOUT`：视频页抓取候选资源最长等待秒数
- `YOUKU_MANUAL_PLAY_HINT_AFTER`：多久后提示你手动点击一次播放
- `YOUKU_OPEN_URL_RETRY_COUNT`：视频页打开失败时的自动重试次数
- `YOUKU_OPEN_URL_RETRY_DELAY`：每次重试之间的等待秒数

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


## 已知限制

- 遇到加密 HLS（`#EXT-X-KEY`）时，当前不会自动处理
- 如果优酷改版，页面选择器或抓取规则可能需要更新
- 这个项目依赖你自己的浏览器环境和登录态，不适合作为免登录服务端下载器

## 免责声明

本项目仅用于学习浏览器自动化、HLS 抓取与本地转封装流程。  
请自行确保你的使用行为符合当地法律法规、优酷服务条款以及内容版权要求。
