import os
import subprocess
import tempfile
from pathlib import Path

import pytest


@pytest.mark.asyncio
async def test_tui_hang_on_quit():
    """Verify that gfal-tui exits within a reasonable time even with hung workers."""
    script = """
import time
import sys
import os
from gfal_cli.tui import GfalTui
from gfal_cli.base import interactive

class HungTui(GfalTui):
    def on_mount(self):
        print("HungTui mounted", file=sys.stderr)
        def hung_worker():
            print("Hung worker started", file=sys.stderr)
            time.sleep(100)
        self.run_worker(hung_worker, thread=True)
        print("Timer set", file=sys.stderr)
        self.set_timer(0.5, self.action_quit)

    def action_quit(self):
        print("action_quit called", file=sys.stderr)
        super().action_quit()

if __name__ == "__main__":
    print("Starting HungTui", file=sys.stderr)
    app = HungTui()
    app.run()
    print("EXIT_SUCCESS", file=sys.stderr)
    sys.exit(0)
"""
    test_script_path = Path(tempfile.gettempdir()) / "test_tui_hang.py"
    test_script_path.write_text(script)

    import sys

    env = os.environ.copy()
    env["PYTHONPATH"] = "src"
    # Ensure no-verify and other gfal-specific env vars don't interfere
    env["TEXTUAL_CLIPBOARD"] = "none"

    process = subprocess.Popen(
        [sys.executable, test_script_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
        text=True,
    )

    try:
        stdout, stderr = process.communicate(timeout=10)
        print(f"DEBUG STDOUT: {stdout}")
        print(f"DEBUG STDERR: {stderr}")
        if "EXIT_SUCCESS" in stderr:
            return
    except subprocess.TimeoutExpired:
        process.kill()
        stdout, stderr = process.communicate()
        pytest.fail(f"TUI hung for more than 10 seconds. STDERR: {stderr}")
    finally:
        if test_script_path.exists():
            test_script_path.unlink()

    assert process.returncode == 0
