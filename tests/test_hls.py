import unittest

from lib import hls


class HlsNormalizationTests(unittest.TestCase):
    def test_keeps_contiguous_segment_numbers_across_timestamp_reset(self):
        entries = [
            {
                "url": "https://example.test/seg-0.ts?ts_seg_no=0",
                "key": "seg:0",
                "segment_no": 0,
                "ts_start": 0.0,
                "ts_end": 10.0,
                "duration": 10.0,
            },
            {
                "url": "https://example.test/seg-1.ts?ts_seg_no=1",
                "key": "seg:1",
                "segment_no": 1,
                "ts_start": 10.0,
                "ts_end": 20.0,
                "duration": 10.0,
            },
            {
                "url": "https://example.test/seg-2.ts?ts_seg_no=2",
                "key": "seg:2",
                "segment_no": 2,
                "ts_start": 0.0,
                "ts_end": 10.0,
                "duration": 10.0,
            },
            {
                "url": "https://example.test/seg-3.ts?ts_seg_no=3",
                "key": "seg:3",
                "segment_no": 3,
                "ts_start": 10.0,
                "ts_end": 20.0,
                "duration": 10.0,
            },
        ]

        normalized, metadata = hls.normalize_segment_entries(entries)
        playlist = {"segment_entries": normalized}

        self.assertEqual([entry["segment_no"] for entry in normalized], [0, 1, 2, 3])
        self.assertEqual(metadata["cycle_count"], 1)
        self.assertEqual(hls.analyze_playlist_integrity(playlist)["critical_issue_count"], 0)
        self.assertTrue(hls.should_insert_discontinuity(normalized[1], normalized[2]))


if __name__ == "__main__":
    unittest.main()
