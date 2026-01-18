# src/utils/ddb_keys.py
from typing import Dict, Union, List, Optional
# DynamoDB テーブルのPK/SK生成処理を管理するユーティリティクラス


def make_daily_pk(facility: str) -> str:
    """Daily テーブルの PK を生成"""
    return f"FACILITY#{facility}"


def make_daily_sk(date: str) -> str:
    """Daily テーブルの SK を生成"""
    return f"DATE#{date}"


def make_catch_pk(facility: str, fish: str) -> str:
    """Catch テーブルの PK を生成"""
    return f"FACILITY#{facility}#FISH#{fish}"


def make_catch_sk(date: str, slot: int, place: Optional[Union[str, List[str]]]) -> str:
    """Catch テーブルの SK を生成"""
    # placeがリストの場合は文字列に変換
    if isinstance(place, list):
        place = " ".join(str(p) for p in place if p) if place else None
    place_clean = (place or "UNKNOWN").replace("\n", " ").strip()
    return f"DATE#{date}#SLOT#{slot:02d}#PLACE#{place_clean}"

