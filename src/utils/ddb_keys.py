# src/utils/ddb_keys.py
from typing import Dict
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


def make_catch_sk(date: str, slot: int, place: str) -> str:
    """Catch テーブルの SK を生成"""
    place_clean = (place or "UNKNOWN").replace("\n", " ").strip()
    return f"DATE#{date}#SLOT#{slot:02d}#PLACE#{place_clean}"

