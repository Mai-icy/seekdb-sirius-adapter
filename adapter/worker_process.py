"""Supervise one reusable compute process, without importing CUDA in the parent.

A private Unix packet socket carries accepted connection FDs and lifecycle
messages. Only one request may borrow a slot at a time. Cancellation, deadline,
protocol failure or engine failure retires the process rather than reusing it.
"""
import array
import json
import os
import select
import socket
import subprocess
import time


CONTROL_BYTES = 4096


def send_control(control, message, fd=None):
    payload = json.dumps(message).encode()
    if len(payload) > CONTROL_BYTES:
        raise ValueError("Oversized worker control message")
    ancillary = [] if fd is None else [(socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array('i', [fd]))]
    if control.sendmsg([payload], ancillary) != len(payload):
        raise ConnectionError("Incomplete worker control message")


def receive_control(control):
    """Receive a private control packet, closing any unexpected attached FDs."""
    size = array.array('i').itemsize
    payload, ancillary, flags, _ = control.recvmsg(CONTROL_BYTES, socket.CMSG_SPACE(size * 4))
    descriptors = []
    try:
        for level, kind, data in ancillary:
            if level == socket.SOL_SOCKET and kind == socket.SCM_RIGHTS:
                values = array.array('i')
                values.frombytes(data[:len(data) - len(data) % size])
                descriptors.extend(values)
        if not payload:
            raise EOFError("Compute worker disconnected")
        if flags & (socket.MSG_TRUNC | socket.MSG_CTRUNC) or len(descriptors) > 1:
            raise ValueError("Invalid worker control packet")
        message = json.loads(payload)
        if not isinstance(message, dict):
            raise ValueError("Invalid worker control message")
        fd = descriptors[0] if descriptors else None
        if fd is not None:
            os.set_inheritable(fd, False)
            descriptors.pop()
        return message, fd
    finally:
        for fd in descriptors:
            os.close(fd)


class WorkerProcess:
    """A slot leased exclusively by a handler thread; native work stays in its child."""

    def __init__(self, command, reuse=True, startup_timeout=30):
        self.command = command
        self.reuse = reuse
        self.startup_timeout = startup_timeout
        self.process = None
        self.control = None

    def spawn(self):
        parent, child = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
        try:
            self.process = subprocess.Popen(
                [*self.command, '--control-fd', str(child.fileno())],
                pass_fds=(child.fileno(),), start_new_session=True)
            self.control = parent
        except BaseException:
            parent.close()
            raise
        finally:
            child.close()

    def start(self, stopping):
        # A killed process can still appear alive while the driver tears it down.
        # Probe the idle control loop before passing a client FD. Only this
        # pre-dispatch probe may be retried; dispatched queries are never replayed.
        for attempt in range(2):
            try:
                if self.process is None or self.process.poll() is not None:
                    self.close()
                    self.spawn()
                send_control(self.control, {'event': 'ping'})
                deadline = time.monotonic() + self.startup_timeout
                while not stopping.is_set():
                    if time.monotonic() >= deadline:
                        raise TimeoutError("Compute worker did not become ready")
                    if select.select([self.control], [], [], 0.05)[0]:
                        message, fd = receive_control(self.control)
                        if fd is not None:
                            os.close(fd)
                            raise ValueError("Unexpected worker descriptor")
                        if message != {'event': 'ready'}:
                            raise ValueError("Expected idle worker readiness")
                        return
                raise InterruptedError("Service is stopping")
            except (OSError, EOFError, ValueError):
                self.close()
                if attempt or stopping.is_set():
                    raise

    def close(self):
        """Let an idle child close its Context; forcibly reap a stuck/active child."""
        if self.control is not None:
            self.control.close()
            self.control = None
        if self.process is not None:
            try:
                self.process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
            self.process = None

    def run(self, connection, work, stopping):
        """Return only after per-query cleanup; keep the process solely on success."""
        healthy = False
        try:
            self.start(stopping)
            send_control(self.control, {'event': 'query', 'work': str(work)}, connection.fileno())
            deadline = time.monotonic() + self.startup_timeout
            poller = select.poll()
            poller.register(self.control, select.POLLIN | select.POLLHUP | select.POLLERR)
            # POLLRDHUP observes TCP/Unix disconnects without consuming TLS bytes.
            disconnected = select.POLLHUP | select.POLLERR | select.POLLRDHUP
            poller.register(connection, disconnected)
            finishing = False
            while not stopping.is_set() and self.process.poll() is None:
                if time.monotonic() >= deadline:
                    raise TimeoutError("Compute worker exceeded query deadline")
                events = dict(poller.poll(50))
                if self.control.fileno() in events:
                    message, fd = receive_control(self.control)
                    if fd is not None:
                        os.close(fd)
                        raise ValueError("Unexpected worker descriptor")
                    kind = message.get('event')
                    if kind == 'deadline':
                        deadline = message['deadline']
                    elif kind == 'finishing':
                        # Result resources are gone; Z and socket close may race
                        # the peer's disconnect. Wait for the explicit done ack.
                        finishing = True
                        poller.unregister(connection)
                    elif kind == 'done':
                        healthy = message.get('success') is True
                        return healthy
                    else:
                        raise ValueError("Unexpected worker lifecycle message")
                if not finishing and events.get(connection.fileno(), 0) & disconnected:
                    # A finishing packet may have been queued just after poll.
                    # One brief grace period avoids retiring a successful worker.
                    if select.select([self.control], [], [], 0.05)[0]:
                        continue
                    return False
            return False
        finally:
            if not healthy or not self.reuse:
                self.close()
