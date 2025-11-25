# debug_csv.py
import sys
from pathlib import Path
import pandas as pd

p = Path(sys.argv[1])
print("FILE:", p)
print("Exists:", p.exists())
print("\n--- RAW BYTES (first 64 bytes) ---")
b = p.open("rb").read(64)
print(b)
try:
    preview = b.decode("utf-8")
except Exception:
    try:
        preview = b.decode("utf-8-sig")
    except Exception:
        preview = b.decode("latin-1", errors="replace")
print("\n--- RAW TEXT PREVIEW (first 4 lines) ---")
print("\n".join(preview.splitlines()[:4]))

encodings = ["utf-8-sig", "utf-8", "cp1252", "latin-1"]
delims = [",", ";", "\t", "|"]

for enc in encodings:
    for d in delims:
        try:
            print(f"\n--- Trying encoding={enc!r}, delimiter={d!r} ---")
            df = pd.read_csv(p, encoding=enc, sep=d, engine="python", nrows=5)
            print("Columns:", list(df.columns))
            print("Head:")
            print(df.head(3).to_string(index=False))
        except Exception as e:
            print("ERROR:", type(e).__name__, str(e))
