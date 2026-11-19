"""Context ownership and failure semantics without loading a GPU library."""
from pathlib import Path
import sys
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'adapter'))
import compute_node


class NativeEngineTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / 'library'
        self.path.touch()
        self.events = []
        self.bridge = SimpleNamespace(
            compute_open=Mock(side_effect=lambda *args: self.events.append('open') or 123),
            compute_execute=Mock(side_effect=lambda *args: self.events.append('execute') or 0),
            compute_close=Mock(side_effect=lambda *args: self.events.append('close')))
        self.reader = SimpleNamespace(close=Mock(side_effect=lambda: self.events.append('release')))
        for patcher in (
            patch.object(compute_node.ctypes, 'CDLL', return_value=self.bridge),
            patch.object(compute_node, 'encode_plan', return_value=(b'plan', [])),
            patch.object(compute_node, 'pa', SimpleNamespace(RecordBatchReader=SimpleNamespace(
                _import_from_c=Mock(return_value=self.reader))))):
            patcher.start()
            self.addCleanup(patcher.stop)
        self.engine = compute_node.NativeEngine(self.path, self.path)
        self.addCleanup(self.engine.close)

    def test_reuses_context_but_releases_each_result_before_next_execution(self):
        for reused in (False, True):
            with self.engine.result('{}', {}) as (_, info):
                self.assertEqual(info['context_reused'], reused)
        self.assertEqual(self.events, ['open', 'execute', 'release', 'execute', 'release'])
        self.engine.close()
        self.engine.close()
        self.assertEqual(self.events[-1], 'close')
        self.bridge.compute_close.assert_called_once_with(123)

    def test_one_shot_wrapper_still_closes_context(self):
        with compute_node.open_native_result('{}', {}, self.path, self.path) as (_, info):
            self.assertNotIn('close', self.events)
        self.assertEqual(self.events, ['open', 'execute', 'release', 'close'])
        self.assertIn('close_seconds', info)

    def test_engine_failure_is_not_reused(self):
        self.bridge.compute_execute.return_value = -1
        self.bridge.compute_execute.side_effect = None
        with self.assertRaises(RuntimeError):
            with self.engine.result('{}', {}):
                self.fail('Failed execution yielded a result')
        with self.assertRaisesRegex(RuntimeError, 'failed'):
            with self.engine.result('{}', {}):
                pass
        self.bridge.compute_execute.assert_called_once()

    def test_consumer_failure_releases_result_and_poisons_context(self):
        with self.assertRaisesRegex(ValueError, 'consumer'):
            with self.engine.result('{}', {}):
                raise ValueError('consumer')
        self.assertTrue(self.engine.poisoned)
        self.assertEqual(self.events[-1], 'release')

    def test_invalid_plan_never_opens_engine(self):
        with patch.object(compute_node, 'encode_plan', side_effect=ValueError('plan')):
            with self.assertRaises(ValueError):
                with self.engine.result('{}', {}):
                    pass
        self.assertEqual(self.events, [])

    def test_active_result_blocks_close_and_nested_execution(self):
        with self.engine.result('{}', {}):
            with self.assertRaisesRegex(RuntimeError, 'Release'):
                self.engine.close()
            with self.assertRaisesRegex(RuntimeError, 'busy'):
                with self.engine.result('{}', {}):
                    pass
        self.assertFalse(self.engine.poisoned)

    def test_context_cannot_move_to_another_thread(self):
        errors = []
        def wrong_thread():
            try:
                self.engine.open()
            except RuntimeError as error:
                errors.append(str(error))
        thread = threading.Thread(target=wrong_thread)
        thread.start()
        thread.join()
        self.assertEqual(len(errors), 1)
        self.assertEqual(self.events, [])


if __name__ == '__main__':
    unittest.main()
