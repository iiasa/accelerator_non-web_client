"""
Parallel read test for a file on the hf-mount (/mnt/wdrv).

Starts N_CHILDREN WKube jobs at once; each job starts PROCS_PER_CHILD processes
that read the same file in 4 KB chunks (like the MSVC runtime under Wine) and
compare the bytes read and the checksum against the file size on disk.

A child exits with code 2 if any read was short, failed, or returned different
content, so failures show up as failed jobs.
"""
from accli import WKubeTask

# ---- settings -------------------------------------------------------------
TEST_FILE = "/mnt/wdrv/Working/6_TestRun03a_ResilientLandHAfr_a_GLOini_20260908/inputPath_constant/output_glo4myk_SupplyWood_myk_price_06072023.csv"  # <-- set this
N_CHILDREN = 8          # parallel WKube jobs
PROCS_PER_CHILD = 4     # parallel reader processes inside each job
ROUNDS = 3              # only the first round reads cold; later rounds hit caches

# ---- inline test script (Python 3.7 compatible) ---------------------------
READ_TEST = r'''
import hashlib, os, socket, sys, time
from multiprocessing import Pool

PATH = "__FILE__"
PROCS = __PROCS__
ROUNDS = __ROUNDS__

def read_once(_):
    h, n, t0 = hashlib.sha256(), 0, time.time()
    try:
        with open(PATH, "rb", buffering=0) as f:
            while True:
                chunk = f.read(4096)
                if not chunk:
                    break
                h.update(chunk)
                n += len(chunk)
        return (n, h.hexdigest()[:16], None, time.time() - t0)
    except OSError as e:
        return (n, None, "%s: %s" % (type(e).__name__, e), time.time() - t0)

if __name__ == "__main__":
    host = socket.gethostname()
    expected = os.stat(PATH).st_size
    print("[%s] file=%s expected_size=%d" % (host, PATH, expected))
    bad = 0
    for r in range(ROUNDS):
        with Pool(PROCS) as p:
            results = p.map(read_once, range(PROCS))
        digests = sorted(set(d for _, d, _, _ in results if d))
        for n, d, err, secs in results:
            status = "OK" if (err is None and n == expected) else "BAD"
            if status == "BAD":
                bad += 1
            print("[%s] round %d: %s got %d/%d bytes in %.1fs sha=%s err=%s"
                  % (host, r, status, n, expected, secs, d, err))
        if len(digests) > 1:
            bad += 1
            print("[%s] round %d: BAD different contents seen: %s" % (host, r, digests))
    print("[%s] RESULT: %s" % (host, "all reads complete" if bad == 0 else "%d bad reads" % bad))
    sys.exit(2 if bad else 0)
'''

script = (READ_TEST
          .replace("__FILE__", TEST_FILE)
          .replace("__PROCS__", str(PROCS_PER_CHILD))
          .replace("__ROUNDS__", str(ROUNDS)))

# The agent runs the command with `sh -c`, so a quoted heredoc passes the script verbatim.
command = "python - <<'PYEOF'\n" + script + "\nPYEOF\n"

# ---- workflow: holder task with parallel children -------------------------
routine = WKubeTask(name="hf-mount parallel read test")

for i in range(N_CHILDREN):
    routine.add_child(WKubeTask(
        name=f"read test {i}",
        job_folder='./',
        base_stack='PYTHON3_7',
        command=command,
        required_cores=0.5,
        required_ram=256 * 1024 ** 2,
        required_storage_local=1 * 1024 ** 3,
        required_storage_workflow=0,
        timeout=60 * 60,
        conf={
            "input_mappings": "",
            "output_mappings": ""
        }
    ))
