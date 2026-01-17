# src/utils/numbers.py
from typing import Any, Optional


def safe_int(v: Any) -> Optional[int]:
    """値を安全に整数に変換する。変換できない場合は None を返す"""
    if v is None:
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        return int(v)
    if isinstance(v, str):
        s = v.strip()
        if s == "":
            return None
        try:
            return int(float(s))
        except ValueError:
            return None
    return None

