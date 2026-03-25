import json
import os
import re
import time
from pathlib import Path
from urllib.parse import urlparse

import requests
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException

import lib.hls as hls

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_USER_AGENT = (
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
    'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36'
)
CHROME_PROFILE_DIR = PROJECT_ROOT / 'download' / '.chrome-profile'
LOGIN_URL = 'https://account.youku.com/'
LOGIN_SUCCESS_URL_PREFIX = 'https://www.youku.com/ku/webhome'
SCAN_LOGIN_TIMEOUT = int(os.getenv('YOUKU_SCAN_TIMEOUT', '180'))
PAGE_LOAD_TIMEOUT = int(os.getenv('YOUKU_PAGE_LOAD_TIMEOUT', '25'))
CAPTURE_TIMEOUT = int(os.getenv('YOUKU_CAPTURE_TIMEOUT', '60'))
MANUAL_PLAY_HINT_AFTER = int(os.getenv('YOUKU_MANUAL_PLAY_HINT_AFTER', '8'))
CAPTURE_POLL_INTERVAL = float(os.getenv('YOUKU_CAPTURE_POLL_INTERVAL', '2'))
RESPONSE_BODY_LIMIT = int(os.getenv('YOUKU_RESPONSE_BODY_LIMIT', '1500000'))
MAX_RESPONSE_BODIES_PER_PASS = int(os.getenv('YOUKU_MAX_RESPONSE_BODIES_PER_PASS', '10'))
MAX_PERFORMANCE_LOGS_PER_PASS = int(os.getenv('YOUKU_MAX_PERFORMANCE_LOGS_PER_PASS', '800'))
PAGE_TEXT_LIMIT = int(os.getenv('YOUKU_PAGE_TEXT_LIMIT', '1200000'))
OPEN_URL_RETRY_COUNT = int(os.getenv('YOUKU_OPEN_URL_RETRY_COUNT', '3'))
OPEN_URL_RETRY_DELAY = float(os.getenv('YOUKU_OPEN_URL_RETRY_DELAY', '2'))
YOUKU_HOME_URL = 'https://www.youku.com/'

singleBrowser = None
isLogin = False


def _build_browser():
    CHROME_PROFILE_DIR.mkdir(parents=True, exist_ok=True)

    option = webdriver.ChromeOptions()
    option.add_argument('--log-level=3')
    option.add_argument('--start-maximized')
    option.add_argument('--disable-blink-features=AutomationControlled')
    option.add_argument('--disable-infobars')
    option.add_argument('--disable-quic')
    option.add_argument(f'--user-data-dir={CHROME_PROFILE_DIR}')
    option.add_experimental_option('excludeSwitches', ['enable-automation'])
    option.add_experimental_option('useAutomationExtension', False)
    option.page_load_strategy = 'eager'
    option.set_capability('goog:loggingPrefs', {'performance': 'ALL'})

    chrome_binary = os.getenv('GOOGLE_CHROME_BIN')
    if chrome_binary:
        option.binary_location = chrome_binary

    browser = webdriver.Chrome(options=option)
    browser.set_page_load_timeout(PAGE_LOAD_TIMEOUT)

    try:
        browser.execute_cdp_cmd(
            'Page.addScriptToEvaluateOnNewDocument',
            {
                'source': """
                    Object.defineProperty(navigator, 'webdriver', {
                      get: () => undefined
                    });
                """,
            },
        )
    except Exception:
        pass

    return browser


def _is_browser_alive(browser):
    if browser is None:
        return False

    try:
        browser.current_url
    except Exception:
        return False

    return True


def _ensure_browser():
    global singleBrowser
    global isLogin

    if _is_browser_alive(singleBrowser):
        return singleBrowser

    if singleBrowser is not None:
        try:
            singleBrowser.quit()
        except Exception:
            pass

    singleBrowser = _build_browser()
    isLogin = False
    return singleBrowser


class ChromeCatch:
    def __init__(self, videoIndex, videoName, videoUrl, videoGroupName):
        self.__videoIndex = videoIndex
        self.__videoName = self.__safe_file_name(videoName)
        self.__videoUrl = videoUrl
        self.__videoGroupName = self.__safe_file_name(videoGroupName)

    def login(self):
        global isLogin

        browser = _ensure_browser()
        if isLogin:
            return

        print('正在打开优酷登录页。')
        self.__open_url(browser, LOGIN_URL, '登录页')

        if self.__wait_for_login_redirect(browser, timeout=10):
            print('已复用本地 Chrome 登录态。')
            isLogin = True
            return

        print('请在打开的浏览器中扫码登录优酷，登录成功后程序会自动继续。')
        print('Chrome 登录态会保存在 {0}'.format(CHROME_PROFILE_DIR))

        if self.__wait_for_login_redirect(browser, timeout=SCAN_LOGIN_TIMEOUT):
            print('扫码登录成功。')
            isLogin = True
            return

        raise RuntimeError(
            '等待扫码登录超时（{0} 秒）。请在浏览器完成扫码后重新运行，'
            '登录态会保存在 {1}。'.format(SCAN_LOGIN_TIMEOUT, CHROME_PROFILE_DIR)
        )

    def downloadVideoMidFile(self):
        browser = _ensure_browser()
        browser.execute_cdp_cmd('Network.enable', {})
        try:
            browser.get_log('performance')
        except Exception:
            pass

        print('正在打开视频页面。')
        self.__open_url(browser, self.__videoUrl, '视频页')
        print('视频页面已打开，开始抓取候选资源。')

        mediaState = self.__collect_media_candidates(browser)
        m3u8Candidates = list(reversed(mediaState['m3u8']))
        subtitleCandidates = list(reversed(mediaState['subtitle']))

        if not m3u8Candidates:
            raise RuntimeError(
                '等待 {0} 秒后仍未抓到 m3u8。请确认视频页已开始播放，再重新运行。'
                .format(CAPTURE_TIMEOUT)
            )

        print('共捕获到 {0} 条候选 m3u8，开始自动比对。'.format(len(m3u8Candidates)))

        assFilePath = self.__getFileAssPath()
        m3u8FilePath = self.__getFileM3u8Path()
        Path(m3u8FilePath).parent.mkdir(parents=True, exist_ok=True)

        session = self.__build_authenticated_session(browser)
        playlistResult = hls.load_best_playlist(m3u8Candidates, session=session, timeout=15, limit=12)
        playlist = playlistResult['playlist']

        inspections = playlistResult.get('inspections') or []
        if inspections:
            print('候选 m3u8 诊断：')
            for index, item in enumerate(inspections, start=1):
                prefix = '候选 {0}'.format(index)
                if item.get('ok'):
                    print('{0}: {1}'.format(prefix, item.get('summary')))
                else:
                    print('{0}: 解析失败，{1}'.format(prefix, item.get('summary')))

        if playlist.get('encrypted'):
            raise RuntimeError('检测到 #EXT-X-KEY，加密 HLS 暂不支持自动下载。')

        Path(m3u8FilePath).write_text(
            hls.build_local_playlist_text(playlist),
            encoding='utf-8',
        )

        subtitleUrl = self.__choose_subtitle(subtitleCandidates)
        if subtitleUrl:
            response = session.get(subtitleUrl, timeout=30)
            response.raise_for_status()
            Path(assFilePath).write_bytes(response.content)

        print(
            '已为 {0} 生成本地清单，候选 m3u8 {1} 条，{2}'.format(
                self.__videoName,
                playlistResult['candidate_count'],
                playlistResult['detail'],
            )
        )

    @staticmethod
    def close():
        global singleBrowser
        global isLogin

        if singleBrowser is not None:
            try:
                singleBrowser.quit()
            except Exception:
                pass

        singleBrowser = None
        isLogin = False

    def __wait_for_login_redirect(self, browser, timeout):
        startedAt = time.time()
        while time.time() - startedAt < timeout:
            if self.__is_logged_in(browser):
                return True
            time.sleep(1)
        return False

    def __is_logged_in(self, browser):
        try:
            currentUrl = (browser.current_url or '').strip().lower()
        except Exception:
            return False

        return currentUrl.startswith(LOGIN_SUCCESS_URL_PREFIX)

    def __collect_media_candidates(self, browser):
        mediaState = {
            'm3u8': [],
            'subtitle': [],
        }
        trackedResponses = {}
        seenUrls = set()
        previousCount = 0
        stableRounds = 0
        manualPlayHintPrinted = False
        startedAt = time.time()
        roundIndex = 0

        while time.time() - startedAt < CAPTURE_TIMEOUT:
            roundIndex += 1
            self.__try_start_playback(browser)
            time.sleep(1.2)

            self.__consume_performance_entries(browser, mediaState, trackedResponses, seenUrls)
            self.__consume_resource_entries(browser, mediaState, seenUrls)
            self.__consume_page_text(browser, mediaState, seenUrls)

            elapsed = int(time.time() - startedAt)
            currentCount = len(mediaState['m3u8'])
            playerState = self.__read_player_state(browser)
            playerLabel = self.__format_player_state(playerState)

            if currentCount != previousCount:
                print(
                    '抓取轮次 {0}: 已发现 {1} 条候选 m3u8，{2}'.format(
                        roundIndex,
                        currentCount,
                        playerLabel,
                    )
                )
            elif roundIndex == 1 or roundIndex % 3 == 0:
                print(
                    '抓取轮次 {0}: 暂无新增 m3u8，已等待 {1} 秒，{2}'.format(
                        roundIndex,
                        elapsed,
                        playerLabel,
                    )
                )

            if currentCount and currentCount == previousCount:
                stableRounds += 1
            else:
                stableRounds = 0

            previousCount = currentCount

            if currentCount and stableRounds >= 2:
                break

            if not currentCount and not manualPlayHintPrinted and elapsed >= MANUAL_PLAY_HINT_AFTER:
                print('浏览器如果没有自动开始播放，请手动点一次播放，程序会继续等待并自动抓取。')
                manualPlayHintPrinted = True

            time.sleep(CAPTURE_POLL_INTERVAL)

        return mediaState

    def __open_url(self, browser, url, label):
        errors = []

        for attempt in range(1, OPEN_URL_RETRY_COUNT + 1):
            try:
                self.__prepare_navigation_attempt(browser, attempt)
                self.__navigate_once(browser, url, label, attempt)
                self.__ensure_page_available(browser, url, label)
                return
            except RuntimeError as error:
                errors.append(str(error))
                if attempt >= OPEN_URL_RETRY_COUNT:
                    break

                print(
                    '{0}打开异常，准备第 {1} 次重试：{2}'.format(
                        label,
                        attempt + 1,
                        self.__compact_error(error),
                    )
                )
                time.sleep(OPEN_URL_RETRY_DELAY)

        raise RuntimeError('{0}打开失败：{1}'.format(label, ' | '.join(errors[-OPEN_URL_RETRY_COUNT:])))

    def __prepare_navigation_attempt(self, browser, attempt):
        if attempt == 1:
            return

        if attempt == 2:
            self.__navigate_with_get(browser, YOUKU_HOME_URL, '优酷首页预热')
            return

        self.__open_new_tab(browser)

    def __navigate_once(self, browser, url, label, attempt):
        if attempt < OPEN_URL_RETRY_COUNT:
            self.__navigate_with_get(browser, url, label)
            return

        self.__navigate_with_script(browser, url, label)

    def __navigate_with_get(self, browser, url, label):
        try:
            browser.get(url)
        except TimeoutException:
            try:
                browser.execute_script('window.stop();')
            except Exception:
                pass

            print('{0}加载超过 {1} 秒，已停止继续等待，转入后续步骤。'.format(label, PAGE_LOAD_TIMEOUT))
        except WebDriverException as error:
            raise RuntimeError(self.__normalize_webdriver_error(error)) from error

    def __navigate_with_script(self, browser, url, label):
        try:
            browser.execute_script('window.location.href = arguments[0];', url)
        except WebDriverException as error:
            raise RuntimeError(self.__normalize_webdriver_error(error)) from error

        deadline = time.time() + PAGE_LOAD_TIMEOUT
        while time.time() < deadline:
            currentUrl = self.__get_current_url(browser)
            if currentUrl and currentUrl != 'about:blank':
                return
            time.sleep(0.5)

        print('{0}脚本跳转超过 {1} 秒，继续检查当前页面。'.format(label, PAGE_LOAD_TIMEOUT))

    def __ensure_page_available(self, browser, url, label):
        currentUrl = self.__get_current_url(browser)
        pageTitle = self.__get_page_title(browser)

        if self.__looks_like_browser_error_page(currentUrl, pageTitle):
            raise RuntimeError('{0}仍停留在浏览器错误页：{1}'.format(label, currentUrl or pageTitle or '未知错误'))

        if self.__is_same_target_domain(currentUrl, url):
            return

        if currentUrl and currentUrl.startswith('data:'):
            raise RuntimeError('{0}打开后落在 data 错误页。'.format(label))

    def __open_new_tab(self, browser):
        try:
            browser.switch_to.new_window('tab')
            browser.get('about:blank')
            return
        except Exception:
            pass

        try:
            browser.execute_script('window.open("about:blank", "_blank");')
            browser.switch_to.window(browser.window_handles[-1])
        except Exception:
            return

    def __normalize_webdriver_error(self, error):
        message = str(error).strip().replace('\n', ' ')
        if 'ERR_CONNECTION_CLOSED' in message:
            return '浏览器连接被关闭（ERR_CONNECTION_CLOSED）'
        if 'ERR_HTTP2_PROTOCOL_ERROR' in message:
            return '浏览器网络协议错误（ERR_HTTP2_PROTOCOL_ERROR）'
        return message

    def __compact_error(self, error):
        text = str(error).strip().replace('\n', ' ')
        return text[:180]

    def __get_current_url(self, browser):
        try:
            return (browser.current_url or '').strip()
        except Exception:
            return ''

    def __get_page_title(self, browser):
        try:
            return (browser.title or '').strip()
        except Exception:
            return ''

    def __looks_like_browser_error_page(self, currentUrl, pageTitle):
        urlValue = str(currentUrl or '').lower()
        titleValue = str(pageTitle or '').lower()
        return (
            urlValue.startswith('chrome-error://')
            or '无法访问此网站' in pageTitle
            or 'site can’t be reached' in titleValue
            or 'this site can’t be reached' in titleValue
            or 'err_connection_closed' in titleValue
        )

    def __is_same_target_domain(self, currentUrl, targetUrl):
        try:
            current = urlparse(currentUrl)
            target = urlparse(targetUrl)
        except Exception:
            return False

        return bool(current.scheme and current.netloc and current.netloc == target.netloc)

    def __consume_performance_entries(self, browser, mediaState, trackedResponses, seenUrls):
        try:
            entries = browser.get_log('performance')
        except Exception:
            return

        if len(entries) > MAX_PERFORMANCE_LOGS_PER_PASS:
            entries = entries[-MAX_PERFORMANCE_LOGS_PER_PASS:]

        bodyRequestsProcessed = 0
        for entry in entries:
            try:
                messageObj = json.loads(entry['message']).get('message', {})
            except Exception:
                continue

            method = messageObj.get('method')
            params = messageObj.get('params', {})

            if method == 'Network.requestWillBeSent':
                requestUrl = params.get('request', {}).get('url')
                self.__capture_candidate(requestUrl, mediaState, seenUrls)
                self.__capture_embedded_text(requestUrl, requestUrl, mediaState, seenUrls)
                continue

            if method == 'Network.responseReceived':
                response = params.get('response', {})
                requestId = params.get('requestId')
                responseUrl = response.get('url')
                self.__capture_candidate(responseUrl, mediaState, seenUrls)

                if not requestId or not hls.should_inspect_response(
                    responseUrl,
                    response.get('mimeType'),
                    response.get('status', 200),
                ):
                    continue

                if self.__response_too_large(response.get('headers') or {}):
                    continue

                trackedResponses[requestId] = responseUrl
                continue

            if method == 'Network.loadingFinished':
                requestId = params.get('requestId')
                responseUrl = trackedResponses.pop(requestId, None)
                if not requestId or not responseUrl:
                    continue

                if bodyRequestsProcessed >= MAX_RESPONSE_BODIES_PER_PASS:
                    continue

                bodyRequestsProcessed += 1
                try:
                    bodyResult = browser.execute_cdp_cmd('Network.getResponseBody', {'requestId': requestId})
                except Exception:
                    continue

                decoded = hls.decode_response_body(
                    bodyResult.get('body', ''),
                    bodyResult.get('base64Encoded', False),
                )
                self.__capture_embedded_text(decoded, responseUrl, mediaState, seenUrls)
                continue

            if method == 'Network.loadingFailed':
                requestId = params.get('requestId')
                if requestId:
                    trackedResponses.pop(requestId, None)

    def __consume_resource_entries(self, browser, mediaState, seenUrls):
        try:
            resourceUrls = browser.execute_script(
                'return (window.performance && window.performance.getEntriesByType'
                ' ? window.performance.getEntriesByType("resource").map(function(entry) { return entry.name; })'
                ' : []);'
            )
        except Exception:
            resourceUrls = []

        for item in resourceUrls[-160:]:
            self.__capture_candidate(item, mediaState, seenUrls)
            self.__capture_embedded_text(item, item, mediaState, seenUrls)

    def __consume_page_text(self, browser, mediaState, seenUrls):
        try:
            pageSource = browser.execute_script(
                '''
                const html = document.documentElement?.outerHTML || "";
                const limit = arguments[0];
                return html.length > limit ? html.slice(-limit) : html;
                ''',
                PAGE_TEXT_LIMIT,
            )
        except Exception:
            return

        self.__capture_embedded_text(pageSource, self.__videoUrl, mediaState, seenUrls)

    def __capture_embedded_text(self, text, baseUrl, mediaState, seenUrls):
        for url in hls.extract_interesting_urls_from_text(text, baseUrl):
            self.__capture_candidate(url, mediaState, seenUrls)

    def __capture_candidate(self, url, mediaState, seenUrls):
        if not url or url in seenUrls:
            return

        kind = hls.get_media_kind(url)
        if not kind:
            return

        seenUrls.add(url)
        mediaState[kind].append(url)

    def __build_authenticated_session(self, browser, referer=None):
        session = requests.Session()

        try:
            userAgent = browser.execute_script('return navigator.userAgent') or DEFAULT_USER_AGENT
        except Exception:
            userAgent = DEFAULT_USER_AGENT

        session.headers.update({
            'User-Agent': userAgent,
            'Referer': referer or self.__videoUrl,
            'Origin': 'https://v.youku.com',
        })

        for cookie in browser.get_cookies():
            session.cookies.set(
                cookie.get('name'),
                cookie.get('value'),
                domain=cookie.get('domain'),
                path=cookie.get('path', '/'),
            )

        return session

    def __choose_subtitle(self, subtitleCandidates):
        def sort_key(item):
            lower = str(item).lower()
            return (
                0 if '.ass' in lower else 1,
                item,
            )

        ranked = sorted(set(subtitleCandidates), key=sort_key)
        return ranked[0] if ranked else None

    def __try_start_playback(self, browser):
        script = '''
            const video = document.querySelector("video");
            if (video) {
              video.muted = true;
              const playPromise = video.play();
              if (playPromise && typeof playPromise.catch === "function") {
                playPromise.catch(() => {});
              }
            }

            const selectors = [
              ".xplayer-play-btn",
              ".xplayer-play-icon",
              ".xplayer-start-btn",
              ".kui-control-icon.play",
              ".spv_play",
              "button[aria-label*='播放']",
              ".xplayer-poster",
              ".poster"
            ];

            for (const selector of selectors) {
              const target = document.querySelector(selector);
              if (!target) {
                continue;
              }

              try {
                target.click();
                break;
              } catch (_error) {}
            }
        '''

        try:
            browser.execute_script(script)
        except Exception:
            pass

    def __read_player_state(self, browser):
        try:
            return browser.execute_script(
                '''
                const video = document.querySelector("video");
                if (!video) {
                  return {
                    hasVideo: false,
                    paused: true,
                    ended: false,
                    currentTime: 0,
                    duration: 0
                  };
                }

                return {
                  hasVideo: true,
                  paused: Boolean(video.paused),
                  ended: Boolean(video.ended),
                  currentTime: Number(video.currentTime || 0),
                  duration: Number(video.duration || 0)
                };
                '''
            )
        except Exception:
            return {
                'hasVideo': False,
                'paused': True,
                'ended': False,
                'currentTime': 0,
                'duration': 0,
            }

    def __format_player_state(self, playerState):
        if not playerState.get('hasVideo'):
            return '页面里还没拿到 video 元素'

        if playerState.get('ended'):
            return '播放器已播放结束'

        if not playerState.get('paused'):
            return '播放器已起播，当前 {0:.1f} 秒'.format(playerState.get('currentTime') or 0)

        return '播放器已加载但仍处于暂停状态'

    def __response_too_large(self, headers):
        contentLength = None
        for key, value in headers.items():
            if str(key).lower() != 'content-length':
                continue

            try:
                contentLength = int(float(value))
            except Exception:
                contentLength = None
            break

        return bool(contentLength and contentLength > RESPONSE_BODY_LIMIT)

    def __safe_file_name(self, value):
        cleaned = re.sub(r'[\/:*?"<>|]', '-', str(value or '').strip())
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        return cleaned or 'youku-video'

    def __getFileM3u8Path(self):
        return str(PROJECT_ROOT / 'download' / self.__videoGroupName / '{0}_{1}.m3u8'.format(self.__videoIndex, self.__videoName))

    def __getFileAssPath(self):
        return str(PROJECT_ROOT / 'download' / self.__videoGroupName / '{0}_{1}.ass'.format(self.__videoIndex, self.__videoName))
