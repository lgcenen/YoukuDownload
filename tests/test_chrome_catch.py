import unittest

from lib.chromeCatch import ChromeCatch, VerificationRequiredError


PUNISH_URL = (
    'https://v.youku.com/v_show/id_example.html/'
    '_____tmd_____/punish?x5step=1'
)


class VerificationPageBrowser:
    current_url = PUNISH_URL
    title = '请验证后继续'

    def __init__(self):
        self.get_calls = 0
        self.execute_script_calls = 0

    def get(self, _url):
        self.get_calls += 1

    def execute_script(self, *_args):
        self.execute_script_calls += 1
        return ''


class VerificationTextBrowser(VerificationPageBrowser):
    current_url = 'https://v.youku.com/v_show/id_example.html'
    title = '优酷视频'

    def execute_script(self, *_args):
        self.execute_script_calls += 1
        return '为确认您是真人，请按住滑块，拖动到最右边'


class ChromeCatchVerificationTests(unittest.TestCase):
    def setUp(self):
        self.handler = ChromeCatch(
            1,
            'test',
            'https://v.youku.com/v_show/id_example.html',
            'test-group',
        )

    def test_verification_page_is_not_accepted_as_target_page(self):
        browser = VerificationPageBrowser()

        with self.assertRaises(VerificationRequiredError):
            self.handler._ChromeCatch__ensure_page_available(
                browser,
                'https://v.youku.com/v_show/id_example.html',
                '视频页',
            )

    def test_verification_text_is_detected_without_punish_url(self):
        browser = VerificationTextBrowser()

        with self.assertRaises(VerificationRequiredError):
            self.handler._ChromeCatch__ensure_page_available(
                browser,
                'https://v.youku.com/v_show/id_example.html',
                '视频页',
            )

    def test_verification_page_does_not_retry_navigation(self):
        browser = VerificationPageBrowser()

        with self.assertRaises(VerificationRequiredError):
            self.handler._ChromeCatch__open_url(
                browser,
                'https://v.youku.com/v_show/id_example.html',
                '视频页',
            )

        self.assertEqual(browser.get_calls, 1)

    def test_capture_stops_before_playback_on_verification_page(self):
        browser = VerificationPageBrowser()

        with self.assertRaises(VerificationRequiredError):
            self.handler._ChromeCatch__collect_media_candidates(browser)

        self.assertEqual(browser.execute_script_calls, 0)


if __name__ == '__main__':
    unittest.main()
