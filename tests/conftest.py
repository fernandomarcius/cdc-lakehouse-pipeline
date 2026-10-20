import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
for sub in ("sink", "worker"):
    sys.path.insert(0, os.path.join(ROOT, sub))
