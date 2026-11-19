"""Exercise real worker processes/FD transfer and cancellation without CUDA."""
import os
from pathlib import Path
import signal
import socket
import sys
import tempfile
import threading
import time
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'adapter'))
from worker_process import WorkerProcess


CHILD = '''
import os, socket, sys, time
sys.path.insert(0, ADAPTER)
from worker_process import send_control, receive_control
control = socket.socket(fileno=int(sys.argv[-1]))
while True:
    try:
        message, fd = receive_control(control)
    except EOFError:
        break
    if message.get('event') == 'ping':
        send_control(control, {'event': 'ready'})
        continue
    with socket.socket(fileno=fd) as conn:
        mode = conn.recv(1)
        if mode == b'c': os._exit(3)
        if mode == b'd':
            send_control(control, {'event': 'deadline', 'deadline': time.monotonic() + .1})
            time.sleep(30)
        if mode == b'h': time.sleep(30)
        send_control(control, {'event': 'finishing'})
        conn.sendall(b'z')
    send_control(control, {'event': 'done', 'success': mode != b'e'})
    if mode == b'e': break
'''


@unittest.skipUnless(sys.platform.startswith('linux'), 'Linux POLLRDHUP supervision')
class WorkerProcessTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name)
        child = self.path / 'child.py'
        child.write_text(CHILD.replace('ADAPTER', repr(str(Path(__file__).resolve().parents[2] / 'adapter'))))
        self.slot = WorkerProcess([sys.executable, str(child)], startup_timeout=3)
        self.addCleanup(self.slot.close)
        self.stopping = threading.Event()

    def run_query(self, mode=b'o', disconnect=False):
        parent, client = socket.socketpair()
        with parent, client:
            client.sendall(mode)
            if disconnect:
                client.close()
            return self.slot.run(parent, self.path, self.stopping)

    def test_successes_reuse_pid(self):
        self.assertTrue(self.run_query())
        pid = self.slot.process.pid
        self.assertTrue(self.run_query())
        self.assertEqual(self.slot.process.pid, pid)

    def test_immediate_peer_close_after_success_keeps_worker(self):
        parent, client = socket.socketpair()
        with parent:
            def peer():
                with client:
                    client.sendall(b'o')
                    self.assertEqual(client.recv(1), b'z')
            thread = threading.Thread(target=peer)
            thread.start()
            self.assertTrue(self.slot.run(parent, self.path, self.stopping))
            thread.join()
        self.assertIsNone(self.slot.process.poll())

    def test_error_retires_then_next_query_recovers(self):
        self.assertFalse(self.run_query(b'e'))
        self.assertIsNone(self.slot.process)
        self.assertTrue(self.run_query())

    def test_idle_crash_is_replaced(self):
        self.assertTrue(self.run_query())
        pid = self.slot.process.pid
        os.kill(pid, signal.SIGKILL)
        self.slot.process.wait()
        self.assertTrue(self.run_query())
        self.assertNotEqual(self.slot.process.pid, pid)

    def test_active_crash_retires_then_recovers(self):
        try:
            self.assertFalse(self.run_query(b'c'))
        except EOFError:
            pass
        self.assertIsNone(self.slot.process)
        self.assertTrue(self.run_query())

    def test_disconnect_reaps_active_worker(self):
        self.assertFalse(self.run_query(b'h', disconnect=True))
        self.assertIsNone(self.slot.process)
        self.assertTrue(self.run_query())

    def test_deadline_interrupts_blocked_native_work(self):
        with self.assertRaises(TimeoutError):
            self.run_query(b'd')
        self.assertIsNone(self.slot.process)
        self.assertTrue(self.run_query())

    def test_shutdown_interrupts_active_worker(self):
        timer = threading.Timer(.15, self.stopping.set)
        timer.start()
        try:
            self.assertFalse(self.run_query(b'h'))
        finally:
            timer.join()
        self.assertIsNone(self.slot.process)

    def test_no_reuse_mode_reaps_successful_worker(self):
        self.slot.reuse = False
        self.assertTrue(self.run_query())
        self.assertIsNone(self.slot.process)


if __name__ == '__main__':
    unittest.main()
