#!/Users/charlottefelius/Documents/Repositories/temp/duckdb-quack/.venv/bin/python3
import sys
from clang_format import clang_format_diff
if __name__ == '__main__':
    sys.argv[0] = sys.argv[0].removesuffix('.exe')
    sys.exit(clang_format_diff())
