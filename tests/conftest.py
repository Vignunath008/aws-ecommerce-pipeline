import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
# "lambda" is a Python keyword, so add the folders to sys.path instead of importing as packages
for sub in ("lambda", "glue", "dags"):
    sys.path.insert(0, os.path.join(ROOT, sub))

os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
