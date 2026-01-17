import os
import json
import datetime as dt
from typing import Any, Dict, Optional, List

import boto3
from boto3.dynamodb.conditions import Key

DDB_CATCH_TABLE = os.environ["DDB_CATCH_TABLE"]
DDB_DAILY_TABLE = os.environ["DDB_DAILY_TABLE"]
FACILITY_DEFAULT = os.environ.get("FACILITY_DEFAULT", "honmoku")

ddb = boto3.resource("dynamodb")


def _resp(status: int, body: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "statusCode": status,
        "headers": {
            "content-type": "application/json; charset=utf-8",
        },
        "body": json.dumps(body, ensure_ascii=False),
    }


def _get_qs(event: Dict[str, Any]) -> Dict[str, str]:
    return event.get("queryStringParameters") or {}


def _parse_date(s: str) -> dt.date:
    return dt.datetime.strptime(s, "%Y-%m-%d").date()


def handle_series(event: Dict[str, Any]) -> Dict[str, Any]:
    qs = _get_qs(event)
    facility = qs.get("facility", FACILITY_DEFAULT).strip()
    fish = qs.get("fish")
    if not fish:
        return _resp(400, {"error": "missing query param: fish"})

    from_s = qs.get("from")
    to_s = qs.get("to")
    # 省略時は全期間（まずは緩く）
    date_from: Optional[dt.date] = _parse_date(from_s) if from_s else None
    date_to: Optional[dt.date] = _parse_date(to_s) if to_s else None

    pk = f"FACILITY#{facility}#FISH#{fish}"

    table = ddb.Table(DDB_CATCH_TABLE)

    # DynamoDBのSKは "DATE#YYYY-MM-DD#..." なので、begins_with/Betweenが使える
    # from/toがある場合は、DATE#... をキー条件に入れる
    if date_from and date_to:
        sk_from = f"DATE#{date_from.isoformat()}#"
        sk_to = f"DATE#{date_to.isoformat()}#\uffff"  # 末尾まで含めるため
        key_cond = Key("PK").eq(pk) & Key("SK").between(sk_from, sk_to)
    elif date_from:
        sk_from = f"DATE#{date_from.isoformat()}#"
        key_cond = Key("PK").eq(pk) & Key("SK").gte(sk_from)
    else:
        key_cond = Key("PK").eq(pk)

    items: List[Dict[str, Any]] = []
    last_evaluated_key = None

    while True:
        kwargs = {"KeyConditionExpression": key_cond}
        if last_evaluated_key:
            kwargs["ExclusiveStartKey"] = last_evaluated_key
        r = table.query(**kwargs)
        items.extend(r.get("Items", []))
        last_evaluated_key = r.get("LastEvaluatedKey")
        if not last_evaluated_key:
            break

    # 念のため日付順にソート（SKに日付が入ってるので並ぶことが多いが保証しない）
    def extract_date(it: Dict[str, Any]) -> str:
        sk = it.get("SK", "")
        # "DATE#2026-01-15#..." -> "2026-01-15"
        try:
            return sk.split("#")[1]
        except Exception:
            return "0000-00-00"

    items.sort(key=extract_date)

    # フロント向けに整形
    out = []
    for it in items:
        out.append({
            "date": extract_date(it),
            "count": it.get("count"),
            "minSize": it.get("minSize"),
            "maxSize": it.get("maxSize"),
            "unit": it.get("unit"),
            "place": it.get("place"),
        })

    return _resp(200, {"facility": facility, "fish": fish, "items": out})


def handle_day(event: Dict[str, Any]) -> Dict[str, Any]:
    qs = _get_qs(event)
    facility = qs.get("facility", FACILITY_DEFAULT).strip()
    date = qs.get("date")
    if not date:
        return _resp(400, {"error": "missing query param: date (YYYY-MM-DD)"})

    table = ddb.Table(DDB_DAILY_TABLE)
    pk = f"FACILITY#{facility}"
    sk = f"DATE#{date}"
    r = table.get_item(Key={"PK": pk, "SK": sk})
    item = r.get("Item")
    if not item:
        return _resp(404, {"error": "not found"})
    # PK/SKなど内部キーは落として返す
    item.pop("PK", None)
    item.pop("SK", None)
    return _resp(200, item)


def lambda_handler(event, context):
    path = (event.get("rawPath") or event.get("path") or "").lower()
    if path.endswith("/v1/series"):
        return handle_series(event)
    if path.endswith("/v1/day"):
        return handle_day(event)
    return _resp(404, {"error": "not found"})
