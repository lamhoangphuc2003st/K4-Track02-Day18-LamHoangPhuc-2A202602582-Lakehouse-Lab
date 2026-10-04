# ---
# jupyter:
#   jupytext:
#     formats: py:percent
# ---

# %% [markdown]
# # PoC — PII tokenization at Bronze landing + purge from time-travel history
#
# Spike for Topic A (`../ARCHITECTURE.md`). It tests the hardest mechanism in the design:
#
# 1. Redact/tokenize PII **before** the first lakehouse write (deterministic HMAC tokens keep joins working).
# 2. Detect a redactor regression with **canary** values seeded into the stream.
# 3. Show that `DELETE` of the leaked rows is **not** enough: time travel still returns the PII.
# 4. Purge it physically with `VACUUM` and verify by scanning every Parquet file on disk.
#
# Runs offline with the lab's lightweight dependencies (`deltalake`, `polars`, `pyarrow`).

# %%
import hashlib
import hmac
import os
import random
import re
import shutil
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq
from deltalake import DeltaTable, write_deltalake

ROOT = next(p for p in [Path.cwd(), *Path.cwd().parents] if (p / "requirements.txt").exists())
TABLE = ROOT / "_lakehouse" / "bonus" / "bronze_llm_payload"
shutil.rmtree(TABLE, ignore_errors=True)

# Demo-only key. Production: per-environment key from a KMS, rotated, never in the repo.
KEY = os.environ.get("PII_HMAC_KEY", "demo-key-not-a-secret").encode()


def tok(kind: str, value: str) -> str:
    digest = hmac.new(KEY, f"{kind}:{value.lower()}".encode(), hashlib.sha256).hexdigest()[:16]
    return f"<{kind}:{digest}>"


EMAIL = re.compile(r"[\w.+-]+@[\w-]+\.[\w.]+")
PHONE_V1 = re.compile(r"\b0\d{9}\b")                       # v1 only knows 0xxxxxxxxx
PHONE_V2 = re.compile(r"(?:\+84|\b0)\d{9}\b")              # v2 also handles +84xxxxxxxxx
CCCD = re.compile(r"\b\d{12}\b")                           # 12-digit citizen ID


def redact(text: str, version: str) -> str:
    phone = PHONE_V1 if version == "v1" else PHONE_V2
    text = EMAIL.sub(lambda m: tok("EMAIL", m.group()), text)
    text = CCCD.sub(lambda m: tok("ID", m.group()), text)
    return phone.sub(lambda m: tok("PHONE", m.group()), text)


# %% [markdown]
# ## 1. Synthetic request stream with seeded PII and canaries
#
# Canaries are fake PII values the platform injects itself, so a leak can be detected
# without anyone reading real prompts.

# %%
random.seed(18)
CANARIES = ["canary.ops@example.com", "0900000018", "+84900000018", "001200000018"]
TEMPLATES = [
    "Summarise the contract for {email}",
    "Call me back at {phone} about invoice 42",
    "My ID is {cccd}, reset my account",
    "Translate this paragraph into Vietnamese",
    "Customer {email} reported latency, phone {phone}",
]


def make_batch(n: int, start: int) -> list[dict]:
    rows = []
    for i in range(start, start + n):
        user = random.randint(1, 500)
        phone = f"+849{user:08d}" if i % 3 == 0 else f"09{user:08d}"
        prompt = random.choice(TEMPLATES).format(email=f"user{user}@mail.vn", phone=phone, cccd=f"0012{user:08d}")
        if i % 1000 == 0:                                   # one canary row per 1,000 requests
            prompt = f"canary {CANARIES[0]} {CANARIES[1]} {CANARIES[2]} {CANARIES[3]}"
        rows.append({"request_id": i, "tenant_id": f"t{user % 7}", "prompt": prompt})
    return rows


def land(rows: list[dict], version: str) -> None:
    df = pl.DataFrame([{**r, "prompt": redact(r["prompt"], version), "redaction_version": version} for r in rows])
    write_deltalake(TABLE, df.to_arrow(), mode="append")


batch_a, batch_b = make_batch(5_000, 0), make_batch(5_000, 5_000)
land(batch_a, "v1")          # buggy redactor in production for the first window
for r in pl.from_arrow(DeltaTable(TABLE).to_pyarrow_table()).sort("request_id").head(4).iter_rows(named=True):
    print(r["request_id"], r["prompt"])

# %% [markdown]
# ## 2. Canary scanner detects the leak

# %%
def leaks(df: pl.DataFrame) -> pl.DataFrame:
    pattern = "|".join(re.escape(c) for c in CANARIES) + r"|\+84\d{9}|\b0\d{9}\b|@[\w-]+\.\w+"
    return df.filter(pl.col("prompt").str.contains(pattern))


bronze = pl.from_arrow(DeltaTable(TABLE).to_pyarrow_table())
leaked = leaks(bronze)
print(f"Bronze v{DeltaTable(TABLE).version()}: {bronze.height:,} rows, rows with raw PII: {leaked.height:,}")
print("canary +84 phone visible:", bronze["prompt"].str.contains(re.escape(CANARIES[2])).any())
print("example leak:", leaked["prompt"][0])

# %% [markdown]
# ## 3. Fix: deploy v2, re-redact the leaked window from the replay buffer (Kafka in production)

# %%
dt = DeltaTable(TABLE)
dt.delete("redaction_version = 'v1'")
land(batch_a, "v2")          # replay the same source window through the fixed redactor
land(batch_b, "v2")
current = pl.from_arrow(DeltaTable(TABLE).to_pyarrow_table())
print(f"current v{DeltaTable(TABLE).version()}: {current.height:,} rows, rows with raw PII: {leaks(current).height}")

# Deterministic tokens keep analytics working: same user -> same token across batches.
tokens = current["prompt"].str.extract_all(r"<EMAIL:[0-9a-f]+>").explode(empty_as_null=True).drop_nulls()
print(f"distinct email tokens: {tokens.n_unique()}  (synthetic users: 500 + 1 canary)")

# %% [markdown]
# ## 4. DELETE is not erasure: time travel still serves the PII

# %%
old = pl.from_arrow(DeltaTable(TABLE, version=0).to_pyarrow_table())
print(f"time travel to v0 → rows with raw PII: {leaks(old).height:,}")


def pii_files_on_disk() -> int:
    hits = 0
    for f in TABLE.rglob("*.parquet"):
        if "_delta_log" in f.parts:
            continue
        col = pq.read_table(f, columns=["prompt"]).column("prompt").to_pylist()
        hits += any(CANARIES[2] in p for p in col)
    return hits


print("parquet files on disk still holding the +84 canary:", pii_files_on_disk())

# %% [markdown]
# ## 5. Purge: VACUUM the tombstoned files, then verify on disk and via time travel
#
# Retention 0 is an incident-only override (normal Bronze retention: 48 h). It must be paired with
# stopping long-running readers, because it deletes files they may still be reading.

# %%
removed = DeltaTable(TABLE).vacuum(retention_hours=0, enforce_retention_duration=False, dry_run=False)
print(f"vacuum removed {len(removed)} file(s)")
print("parquet files on disk still holding the +84 canary:", pii_files_on_disk())
try:
    DeltaTable(TABLE, version=0).to_pyarrow_table()
    print("time travel to v0: still readable  ← NOT purged")
except Exception as e:  # files referenced by v0 are gone
    print(f"time travel to v0: fails as intended ({type(e).__name__})")

checks = {
    "v1 leak detected by canary scan": leaked.height > 0,
    "current version has 0 raw-PII rows": leaks(current).height == 0,
    "DELETE alone left PII reachable via time travel": leaks(old).height > 0,
    "after VACUUM no file on disk holds the canary": pii_files_on_disk() == 0,
}
for k, v in checks.items():
    print(f"  [{'PASS' if v else 'FAIL'}] {k}")
assert all(checks.values())
