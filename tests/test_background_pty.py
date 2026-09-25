#!/usr/bin/env python3
"""PTY-based test for background introspection envelope integrity.

Unlike test_background.sh (which uses subprocess pipes), this test uses a
real PTY — the same transport Emacs comint uses.  It also simulates the
Emacs preoutput filter logic in Python so we can detect envelope leaking
under realistic conditions.

Usage:
    source ~/.venv/bin/activate
    python tests/test_background_pty.py
"""

import os
import pty
import re
import select
import signal
import sys
import time
import threading

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SERVER = os.environ.get("DATUM_MSSQL_SERVER", "127.0.0.1")
PORT = os.environ.get("DATUM_MSSQL_PORT", "1434")
USER = "sa"
PASS = "DatumTest1!"
DB = "datum_test"

def find_driver():
    import pyodbc
    drivers = [d for d in pyodbc.drivers() if "SQL Server" in d]
    return drivers[-1] if drivers else None

DRIVER = find_driver()
if not DRIVER:
    print("SKIP: No MSSQL ODBC driver found")
    sys.exit(0)

CONN_STRING = (
    f"Driver={{{DRIVER}}};Server=tcp:{SERVER},{PORT};"
    f"Database={DB};Uid={USER};Pwd={PASS};TrustServerCertificate=yes"
)

# Verify connectivity
try:
    import pyodbc
    conn = pyodbc.connect(CONN_STRING)
    conn.close()
except Exception as e:
    print(f"SKIP: Cannot connect to MSSQL: {e}")
    sys.exit(0)

# Check datum_big exists
try:
    conn = pyodbc.connect(CONN_STRING)
    cur = conn.cursor()
    cur.execute("SELECT DB_ID('datum_big')")
    if cur.fetchone()[0] is None:
        print("SKIP: datum_big database not found")
        sys.exit(0)
    conn.close()
except Exception as e:
    print(f"SKIP: Cannot check datum_big: {e}")
    sys.exit(0)


# ---------------------------------------------------------------------------
# PTY process management
# ---------------------------------------------------------------------------
class PtyProcess:
    """Run a subprocess with a real PTY (like Emacs comint does)."""

    def __init__(self, argv):
        self.master_fd, slave_fd = pty.openpty()
        self.pid = os.fork()
        if self.pid == 0:
            # Child: become session leader, set controlling terminal
            os.setsid()
            os.dup2(slave_fd, 0)
            os.dup2(slave_fd, 1)
            os.dup2(slave_fd, 2)
            if slave_fd > 2:
                os.close(slave_fd)
            os.close(self.master_fd)
            os.execvp(argv[0], argv)
            os._exit(1)
        # Parent
        os.close(slave_fd)
        self.output_chunks = []
        self.output_lock = threading.Lock()
        self._reader_thread = threading.Thread(
            target=self._reader, daemon=True, name="pty-reader"
        )
        self._reader_thread.start()

    def _reader(self):
        """Read raw bytes from the PTY master as they arrive."""
        while True:
            try:
                r, _, _ = select.select([self.master_fd], [], [], 1.0)
                if r:
                    data = os.read(self.master_fd, 65536)
                    if not data:
                        break
                    text = data.decode("utf-8", errors="replace")
                    with self.output_lock:
                        self.output_chunks.append(text)
            except OSError:
                break

    def send(self, text):
        """Send text to the PTY (like Emacs comint-send-string)."""
        os.write(self.master_fd, (text + "\n").encode("utf-8"))

    def get_chunks(self):
        """Return a copy of all output chunks received so far."""
        with self.output_lock:
            return list(self.output_chunks)

    def wait(self, timeout=10):
        """Wait for the child process to exit."""
        import signal as sig
        try:
            os.kill(self.pid, sig.SIGTERM)
        except OSError:
            pass
        deadline = time.time() + timeout
        while time.time() < deadline:
            pid, status = os.waitpid(self.pid, os.WNOHANG)
            if pid != 0:
                return status
            time.sleep(0.1)
        try:
            os.kill(self.pid, sig.SIGKILL)
            os.waitpid(self.pid, 0)
        except OSError:
            pass
        return -1

    def close(self):
        try:
            os.close(self.master_fd)
        except OSError:
            pass


# ---------------------------------------------------------------------------
# Emacs preoutput filter simulation
# ---------------------------------------------------------------------------
ENVELOPE_RE = re.compile(r"##DATUM:([^:]+):(.*?)##")
PARTIAL_SUFFIX_RE = re.compile(r"##D(A(T(U(M:?)?)?)?)?$")


class PreoutputFilter:
    """Python simulation of sql-datum--preoutput-filter.

    Processes chunks of raw PTY output exactly as Emacs would:
    - Splits on \\n
    - Detects and buffers partial envelope lines
    - Strips complete envelope lines
    - Returns 'clean' text that would appear in the comint buffer
    """

    def __init__(self, strip_cr=True):
        self.strip_cr = strip_cr
        self.partial_line = None
        self.envelopes_received = []  # list of (type, payload_len)
        self.clean_output = []        # text that would appear in buffer
        self.leaked_envelopes = []    # envelope text that leaked through
        self.chunks_processed = 0
        self.log = []                 # detailed log entries

    def process_chunk(self, output):
        """Process one chunk of PTY output (one comint filter call)."""
        self.chunks_processed += 1
        chunk_id = self.chunks_processed

        # Optionally remove \r from \r\n (pty ONLCR adds \r before \n).
        # Emacs process coding system may or may not do this.
        if self.strip_cr:
            output = output.replace("\r\n", "\n").replace("\r", "")

        self.log.append(
            f"[CHUNK {chunk_id}] len={len(output)} "
            f"has_DATUM={'##DATUM:' in output} "
            f"ends_nl={output.endswith(chr(10))}"
        )
        if "##DATUM:" in output:
            preview = output[:200].replace("\n", "\\n")
            self.log.append(f"  raw: {preview!r}")

        lines = output.split("\n")
        clean_lines = []

        # Prepend buffered partial line
        if self.partial_line is not None and lines:
            self.log.append(
                f"  prepending partial ({len(self.partial_line)} chars)"
            )
            lines[0] = self.partial_line + lines[0]
            self.partial_line = None

        # Check if last line is a partial envelope
        if lines:
            last = lines[-1]
            if last:
                is_partial = False
                # Has ##DATUM: but no complete envelope
                if "##DATUM:" in last:
                    if not re.search(r"##DATUM:[^:]+:.*?##", last):
                        is_partial = True
                        self.log.append(
                            f"  BUFFERING partial (no closing ##): "
                            f"{last[:80]!r}"
                        )
                # Ends with partial ##DATUM: prefix
                if not is_partial and PARTIAL_SUFFIX_RE.search(last):
                    is_partial = True
                    self.log.append(
                        f"  BUFFERING partial (mid-marker): {last[-20:]!r}"
                    )
                # Ends with # or ## but no ##DATUM: — pty split at very
                # start of envelope marker (between the two "#" chars)
                if not is_partial and "##DATUM:" not in last:
                    if re.search(r"#{1,2}$", last):
                        is_partial = True
                        self.log.append(
                            f"  BUFFERING partial (trailing #): {last[-20:]!r}"
                        )
                if is_partial:
                    self.partial_line = last
                    lines = lines[:-1]

        # Process each line
        for line in lines:
            if "##DATUM:" in line:
                # Try to match all envelopes on this line
                start = 0
                handled = False
                while True:
                    m = ENVELOPE_RE.search(line, start)
                    if not m:
                        break
                    etype = m.group(1)
                    epayload = m.group(2)
                    self.envelopes_received.append((etype, len(epayload)))
                    start = m.end()
                    handled = True
                if not handled:
                    self.log.append(
                        f"  SUPPRESSED unmatched fragment: {line[:120]!r}"
                    )
            else:
                clean_lines.append(line)

        result = "\n".join(clean_lines)

        # Check: does the clean result still contain envelope markers?
        if "##DATUM:" in result:
            self.leaked_envelopes.append(result)
            self.log.append(f"  *** LEAKED ENVELOPE in clean result! ***")
            self.log.append(f"  leaked: {result[:200]!r}")

        if result.strip():
            self.clean_output.append(result)

        return result


# ---------------------------------------------------------------------------
# Test execution
# ---------------------------------------------------------------------------
def run_filter_on_chunks(chunks, strip_cr, label):
    """Run the preoutput filter simulation on captured PTY chunks.

    Returns (filter, failed) where failed is True if leaks detected.
    """
    filt = PreoutputFilter(strip_cr=strip_cr)
    for chunk in chunks:
        filt.process_chunk(chunk)

    print(f"\n--- {label} ---")
    print(f"Chunks processed: {filt.chunks_processed}")

    # Count envelope types
    type_counts = {}
    for etype, _ in filt.envelopes_received:
        type_counts[etype] = type_counts.get(etype, 0) + 1
    for etype, count in sorted(type_counts.items()):
        print(f"  {etype}: {count}")
    print(f"  Total: {len(filt.envelopes_received)}")

    failed = False

    # Check for leaked envelopes
    if filt.leaked_envelopes:
        print(f"FAIL: {len(filt.leaked_envelopes)} leaked envelope(s)!")
        for i, leaked in enumerate(filt.leaked_envelopes[:5]):
            print(f"  [{i+1}] {leaked[:200]!r}")
        failed = True
    else:
        print("PASS: No leaked envelopes")

    # Check clean output for stray markers
    all_clean = "\n".join(filt.clean_output)
    stray_count = all_clean.count("##DATUM:")
    if stray_count > 0:
        print(f"FAIL: {stray_count} stray ##DATUM: in clean output!")
        for line in all_clean.split("\n"):
            if "##DATUM:" in line:
                print(f"  {line[:200]!r}")
        failed = True
    else:
        print("PASS: No stray markers in clean output")

    # Envelope stats
    table_envelopes = type_counts.get("introspect", 0) + type_counts.get("introspect+", 0)
    bg_ready = type_counts.get("bg-ready", 0)
    if table_envelopes > 0:
        print(f"PASS: {table_envelopes} introspect envelope(s)")
    else:
        print("WARN: No introspect envelopes")
    if bg_ready > 0:
        print(f"PASS: {bg_ready} bg task(s) completed")

    partials = sum(1 for e in filt.log if "BUFFERING" in e)
    print(f"Partial lines buffered: {partials}")

    return filt, failed


def main():
    print("=== PTY-Based Background Introspection Test ===")
    print(f"Driver: {DRIVER}")
    print(f"Server: tcp:{SERVER},{PORT}")
    print()

    cmd = [
        "datum",
        "--conn-string", CONN_STRING,
        "--sql-type", "mssql",
    ]

    proc = PtyProcess(cmd)

    # Wait for connection
    print("Waiting for connection...")
    time.sleep(4)

    # Switch to datum_big
    print("Switching to datum_big...")
    proc.send(":use datum_big")
    time.sleep(1)

    # Trigger refresh (like Emacs)
    print("Triggering refresh...")
    for cmd_str in [
        ":refresh-databases",
        ":refresh-schemas",
        ":refresh-tables",
        ":refresh-routines",
    ]:
        proc.send(cmd_str)
        time.sleep(0.1)

    # Query while introspection loads
    print("Running query...")
    proc.send("SELECT COUNT(*) AS table_count FROM INFORMATION_SCHEMA.TABLES;;")

    # Wait for background introspection to finish
    print("Waiting for background introspection...")
    idle = 0
    for _ in range(600):
        time.sleep(0.1)
        chunks = proc.get_chunks()
        if len(chunks) == getattr(main, '_prev_len', 0):
            idle += 0.1
            if idle >= 3:
                break
        else:
            idle = 0
        main._prev_len = len(chunks)

    # Exit
    proc.send(":exit")
    time.sleep(1)

    # Capture all chunks
    all_chunks = proc.get_chunks()
    proc.wait(timeout=5)
    proc.close()

    # PTY stats
    sizes = [len(c) for c in all_chunks]
    print(f"\nPTY: {len(sizes)} chunks, "
          f"min={min(sizes)}, max={max(sizes)}, avg={sum(sizes)//len(sizes)}")

    # Run filter in BOTH modes
    filt1, fail1 = run_filter_on_chunks(all_chunks, strip_cr=True,
                                         label="Mode 1: strip \\r (unix coding)")
    filt2, fail2 = run_filter_on_chunks(all_chunks, strip_cr=False,
                                         label="Mode 2: keep \\r (raw PTY)")

    # Write detailed logs
    log_path = "/tmp/datum-pty-test.log"
    with open(log_path, "w") as f:
        f.write("=== PTY Test Detailed Log ===\n\n")
        for label, filt in [("strip_cr=True", filt1), ("strip_cr=False", filt2)]:
            f.write(f"\n{'='*60}\n{label}\n{'='*60}\n")
            for entry in filt.log:
                f.write(entry + "\n")
        f.write(f"\n{'='*60}\nRaw PTY Chunks\n{'='*60}\n")
        for i, chunk in enumerate(all_chunks):
            f.write(f"\n[RAW CHUNK {i+1}] len={len(chunk)}\n")
            preview = chunk[:500].replace("\r", "\\r").replace("\n", "\\n")
            f.write(f"  {preview!r}\n")
    print(f"\nDetailed log: {log_path}")

    failed = fail1 or fail2
    print()
    if failed:
        print("=== FAILED ===")
        sys.exit(1)
    else:
        print("=== PASSED ===")


if __name__ == "__main__":
    main()
