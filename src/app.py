"""
API Gateway用のLambda関数
DynamoDBから釣果データを取得してJSON形式で返すREST APIを提供
"""

import os
import json
import datetime as dt
from decimal import Decimal
from typing import Any, Dict, Optional, List

import boto3
from boto3.dynamodb.conditions import Key

# 環境変数からDynamoDBテーブル名とデフォルト施設名を取得
DDB_CATCH_TABLE = os.environ["DDB_CATCH_TABLE"]  # 釣果データテーブル
DDB_DAILY_TABLE = os.environ["DDB_DAILY_TABLE"]  # 日次統計データテーブル
FACILITY_DEFAULT = os.environ.get("FACILITY_DEFAULT", "honmoku")  # デフォルトの施設名

# DynamoDBリソースの初期化
ddb = boto3.resource("dynamodb")


def _resp(status: int, body: Dict[str, Any]) -> Dict[str, Any]:
    """
    API Gateway用のHTTPレスポンスを生成する
    
    Args:
        status: HTTPステータスコード（例: 200, 400, 404）
        body: レスポンスボディに含めるデータ（辞書形式）
    
    Returns:
        API Gateway形式のレスポンス辞書
    """
    return {
        "statusCode": status,
        "headers": {
            "content-type": "application/json; charset=utf-8",
        },
        "body": json.dumps(body, ensure_ascii=False),  # ensure_ascii=Falseで日本語を正しく出力
    }


def _get_qs(event: Dict[str, Any]) -> Dict[str, str]:
    """
    API Gatewayイベントからクエリパラメータを取得する
    
    Args:
        event: API Gatewayから渡されるイベントオブジェクト
    
    Returns:
        クエリパラメータの辞書（存在しない場合は空辞書）
    """
    return event.get("queryStringParameters") or {}


def _parse_date(s: str) -> dt.date:
    """
    文字列を日付オブジェクトに変換する
    
    Args:
        s: "YYYY-MM-DD"形式の日付文字列
    
    Returns:
        日付オブジェクト
    
    Raises:
        ValueError: 日付形式が不正な場合
    """
    return dt.datetime.strptime(s, "%Y-%m-%d").date()


def _convert_decimal(obj: Any) -> Any:
    """
    DynamoDBのDecimal型をJSONシリアライズ可能なintまたはfloatに変換する
    DynamoDBは数値をDecimal型で返すが、標準のjson.dumps()ではシリアライズできないため変換が必要
    
    Args:
        obj: 変換対象のオブジェクト（Decimal、dict、list、その他）
    
    Returns:
        Decimal型がintまたはfloatに変換されたオブジェクト
    """
    if isinstance(obj, Decimal):
        # 小数点以下が0の場合はint、そうでなければfloatに変換
        if obj % 1 == 0:
            return int(obj)
        return float(obj)
    elif isinstance(obj, dict):
        # 辞書の場合は各値を再帰的に変換
        return {k: _convert_decimal(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        # リストの場合は各要素を再帰的に変換
        return [_convert_decimal(item) for item in obj]
    return obj


def handle_series(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    時系列データ取得APIのハンドラ
    指定した魚種の釣果データを期間指定で取得する
    
    Args:
        event: API Gatewayイベント
    
    Returns:
        API Gateway形式のレスポンス
        - 成功時(200): 施設名、魚種、時系列データのリスト
        - エラー時(400): エラーメッセージ（必須パラメータ不足など）
    
    クエリパラメータ:
        - fish (必須): 魚種名
        - facility (任意): 施設名（デフォルト: honmoku）
        - from (任意): 開始日（YYYY-MM-DD形式）
        - to (任意): 終了日（YYYY-MM-DD形式）
    """
    # クエリパラメータを取得
    qs = _get_qs(event)
    facility = qs.get("facility", FACILITY_DEFAULT).strip()
    fish = qs.get("fish")
    
    # 必須パラメータのチェック
    if not fish:
        return _resp(400, {"error": "missing query param: fish"})

    # 日付範囲の取得とパース（省略時は全期間）
    from_s = qs.get("from")
    to_s = qs.get("to")
    date_from: Optional[dt.date] = _parse_date(from_s) if from_s else None
    date_to: Optional[dt.date] = _parse_date(to_s) if to_s else None

    # DynamoDBのパーティションキーを構築（形式: FACILITY#{facility}#FISH#{fish}）
    pk = f"FACILITY#{facility}#FISH#{fish}"

    # 釣果データテーブルを取得
    table = ddb.Table(DDB_CATCH_TABLE)

    # ソートキー（SK）は "DATE#YYYY-MM-DD" 形式なので、日付範囲でクエリ可能
    # クエリ条件を構築（from/toの有無に応じて条件を変更）
    if date_from and date_to:
        # 開始日と終了日の両方が指定されている場合：範囲指定
        sk_from = f"DATE#{date_from.isoformat()}"
        sk_to = f"DATE#{date_to.isoformat()}"
        key_cond = Key("PK").eq(pk) & Key("SK").between(sk_from, sk_to)
    elif date_from:
        # 開始日のみ指定されている場合：開始日以降
        sk_from = f"DATE#{date_from.isoformat()}"
        key_cond = Key("PK").eq(pk) & Key("SK").gte(sk_from)
    else:
        # 日付指定がない場合：全期間
        key_cond = Key("PK").eq(pk)

    # DynamoDBからデータを取得（ページネーション対応）
    items: List[Dict[str, Any]] = []
    last_evaluated_key = None

    while True:
        kwargs = {"KeyConditionExpression": key_cond}
        if last_evaluated_key:
            # 前回のクエリで取得しきれなかった場合は続きから取得
            kwargs["ExclusiveStartKey"] = last_evaluated_key
        r = table.query(**kwargs)
        items.extend(r.get("Items", []))
        last_evaluated_key = r.get("LastEvaluatedKey")
        if not last_evaluated_key:
            # すべてのデータを取得した場合はループを終了
            break

    # 日付順にソート（SKに日付が入っているので概ね並んでいるが、保証されないため明示的にソート）
    def extract_date(it: Dict[str, Any]) -> str:
        """SKから日付部分を抽出する（"DATE#2026-01-15" -> "2026-01-15"）"""
        sk = it.get("SK", "")
        try:
            return sk.split("#")[1]
        except Exception:
            # パースに失敗した場合は最小値として扱う
            return "0000-00-00"

    items.sort(key=extract_date)

    # フロントエンド向けにデータを整形（PK/SKなどの内部キーは除外）
    out = []
    for it in items:
        out.append({
            "date": extract_date(it),
            "count": it.get("count"),        # 釣れた数
            "minSize": it.get("minSize"),    # 最小サイズ
            "maxSize": it.get("maxSize"),    # 最大サイズ
            "unit": it.get("unit"),          # サイズの単位（cmなど）
            "place": it.get("place"),        # 釣れた場所
        })

    # DynamoDBのDecimal型をJSONシリアライズ可能な型に変換
    result = {"facility": facility, "fish": fish, "items": out}
    result = _convert_decimal(result)

    return _resp(200, result)


def handle_day(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    日次データ取得APIのハンドラ
    指定した日付の日次統計データを取得する
    
    Args:
        event: API Gatewayイベント
    
    Returns:
        API Gateway形式のレスポンス
        - 成功時(200): 日次統計データ
        - エラー時(400): エラーメッセージ（必須パラメータ不足）
        - エラー時(404): エラーメッセージ（データが見つからない）
    
    クエリパラメータ:
        - date (必須): 日付（YYYY-MM-DD形式）
        - facility (任意): 施設名（デフォルト: honmoku）
    """
    # クエリパラメータを取得
    qs = _get_qs(event)
    facility = qs.get("facility", FACILITY_DEFAULT).strip()
    date = qs.get("date")
    
    # 必須パラメータのチェック
    if not date:
        return _resp(400, {"error": "missing query param: date (YYYY-MM-DD)"})

    # 日次統計データテーブルを取得
    table = ddb.Table(DDB_DAILY_TABLE)
    
    # DynamoDBのキーを構築
    pk = f"FACILITY#{facility}"  # パーティションキー
    sk = f"DATE#{date}"          # ソートキー
    
    # DynamoDBから指定したキーのデータを取得
    r = table.get_item(Key={"PK": pk, "SK": sk})
    item = r.get("Item")
    
    # データが見つからない場合
    if not item:
        return _resp(404, {"error": "not found"})
    
    # PK/SKなどの内部キーは削除して返す（フロントエンドには不要）
    item.pop("PK", None)
    item.pop("SK", None)
    
    # DynamoDBのDecimal型をJSONシリアライズ可能な型に変換
    item = _convert_decimal(item)
    
    return _resp(200, item)


def lambda_handler(event, context):
    """
    Lambda関数のエントリーポイント
    API Gatewayからのリクエストを受け取り、パスに応じて適切なハンドラにルーティングする
    
    Args:
        event: API Gatewayから渡されるイベントオブジェクト
            - rawPath: HTTP APIのパス（例: "/v1/series"）
            - path: REST APIのパス（HTTP APIではrawPathを使用）
        context: Lambda実行コンテキスト（未使用）
    
    Returns:
        API Gateway形式のレスポンス
    
    対応するエンドポイント:
        - GET /v1/series: 時系列データ取得API
        - GET /v1/day: 日次データ取得API
        - その他: 404エラー
    """
    # リクエストパスを取得（HTTP APIの場合はrawPath、REST APIの場合はpath）
    path = (event.get("rawPath") or event.get("path") or "").lower()
    
    # パスに応じて適切なハンドラを呼び出す
    if path.endswith("/v1/series"):
        return handle_series(event)
    if path.endswith("/v1/day"):
        return handle_day(event)
    
    # 対応していないパスの場合は404エラーを返す
    return _resp(404, {"error": "not found"})
