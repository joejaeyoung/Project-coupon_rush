import contextlib
import importlib.util
import io
import json
from pathlib import Path
import signal
import socket
import subprocess
import tempfile
import unittest
from unittest.mock import Mock, patch


SPEC = importlib.util.spec_from_file_location("coupon_lab_app", Path(__file__).resolve().parents[1] / "app.py")
app = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(app)


class AppLifecycleTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.root = self.directory / "current-checkout"
        self.runtime = self.root / "runtime"
        self.runtime.mkdir(parents=True)
        self.state_file = self.runtime / "app-process.json"
        self.jar = self.root / "spring/build/libs/coupon-lab.jar"
        self.jar.parent.mkdir(parents=True)
        self.jar.touch()
        for name, value in (("ROOT", self.root), ("RUNTIME", self.runtime), ("STATE", self.state_file)):
            patcher = patch.object(app, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)

    def write_state(self, jar):
        state = {"pid": 12345, "jar": str(jar), "startedAt": 1}
        self.state_file.write_text(json.dumps(state))
        return state

    def test_copied_state_cannot_stop_or_crash_another_checkout(self):
        other_jar = self.directory / "original-checkout/spring/build/libs/coupon-lab.jar"
        state = self.write_state(other_jar)
        process = subprocess.CompletedProcess([], 0, stdout=f"S java -jar {other_jar}")
        with patch.object(app.subprocess, "run", return_value=process), \
                patch.object(app.os, "kill", side_effect=AssertionError("Foreign process must never be signalled")) as kill:
            for crash in (False, True):
                with self.subTest(crash=crash):
                    with self.assertRaisesRegex(RuntimeError, "another checkout"):
                        app.stop(crash=crash)
                    kill.assert_not_called()
                    self.assertEqual(json.loads(self.state_file.read_text()), state)
            self.assertIsNone(app.owned_process(strict=False))

    def test_owned_process_can_still_be_stopped(self):
        state = self.write_state(self.jar)
        running = subprocess.CompletedProcess([], 0, stdout=f"S java -jar {self.jar}")
        exited = subprocess.CompletedProcess([], 1, stdout="")
        with patch.object(app.subprocess, "run", side_effect=[running, exited, exited]), \
                patch.object(app.os, "kill") as kill, contextlib.redirect_stdout(io.StringIO()):
            app.stop()
        kill.assert_called_once_with(state["pid"], signal.SIGTERM)
        self.assertFalse(self.state_file.exists())

    def test_occupied_http_port_fails_before_launching_jvm(self):
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            listener.listen(1)
            port = listener.getsockname()[1]
            response = io.BytesIO(b'{"status":"UP"}')
            response.status = 200
            process = Mock(pid=12345)
            process.poll.return_value = None
            with patch.object(app, "PORT", port, create=True), \
                    patch.object(app.subprocess, "Popen", return_value=process) as launch, \
                    patch.object(app.urllib.request, "urlopen", return_value=response) as health, \
                    contextlib.redirect_stdout(io.StringIO()):
                with self.assertRaisesRegex(RuntimeError, f"Port {port} is occupied"):
                    app.start()
                launch.assert_not_called()
                health.assert_not_called()
                self.assertFalse(self.state_file.exists())


if __name__ == "__main__":
    unittest.main()
