# src/fetcher.py
"""
釣果データをGraphQL APIから取得し、S3とDynamoDBに保存するLambda関数
"""
import os
import json
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple

import boto3

from utils.dates import ymd_dash, ymd_slash, yesterday_jst
from utils.graphql import appsync_post
from utils.numbers import safe_int
from utils.ddb_keys import make_catch_pk, make_catch_sk, make_daily_pk, make_daily_sk

# 環境変数から設定値を取得
S3_BUCKET = os.environ["S3_BUCKET"]  # 生データを保存するS3バケット名
DDB_DAILY_TABLE = os.environ["DDB_DAILY_TABLE"]  # 日次データを保存するDynamoDBテーブル名
DDB_CATCH_TABLE = os.environ["DDB_CATCH_TABLE"]  # 釣果データを保存するDynamoDBテーブル名
SES_FROM = os.environ["SES_FROM"]  # メール送信元アドレス
SES_TO = os.environ["SES_TO"]  # メール送信先アドレス

# デフォルトの施設名（環境変数で指定されない場合）
FACILITY_DEFAULT = os.environ.get("FACILITY_DEFAULT", "honmoku")

# AWSサービスのクライアントを初期化
s3 = boto3.client("s3")
ddb = boto3.resource("dynamodb")
ses = boto3.client("ses")


# ---- GraphQL queries (your captured payloads) ----
# その日の気象条件などを取得する
QUERY_FIELD_CONDITION = r"""
query FirstPostsByFacilityAndDate($facility: String!, $date: ModelStringKeyConditionInput, $sortDirection: ModelSortDirection, $filter: ModelFirstPostFilterInput, $limit: Int, $nextToken: String) {
  firstPostsByFacilityAndDate(
    facility: $facility
    date: $date
    sortDirection: $sortDirection
    filter: $filter
    limit: $limit
    nextToken: $nextToken
  ) {
    items {
      id
      date
      facility
      sentence
      weather
      temp
      waterTemp
      windDirection
      windSpeed
      tide
      highTide
      lowTide
      warning
      advisory
      images
      createdAt
      updatedAt
      __typename
    }
    nextToken
    __typename
  }
}
"""

# 釣果途中経過のデータを取得する　sentenceなど。
QUERY_FISHING_REPORT = r"""
query MiddlePostsByFacilityAndDate($facility: String!, $date: ModelStringKeyConditionInput, $sortDirection: ModelSortDirection, $filter: ModelMiddlePostFilterInput, $limit: Int, $nextToken: String) {
  middlePostsByFacilityAndDate(
    facility: $facility
    date: $date
    sortDirection: $sortDirection
    filter: $filter
    limit: $limit
    nextToken: $nextToken
  ) {
    items {
      id
      date
      time
      facility
      sentence
      weather
      images
      createdAt
      updatedAt
      __typename
    }
    nextToken
    __typename
  }
}
"""

# 魚種ごとの釣果状況を取得する
QUERY_CATCH_COUNT = r"""
query LastPostsByFacilityAndDate($facility: String!, $date: ModelStringKeyConditionInput, $sortDirection: ModelSortDirection, $filter: ModelLastPostFilterInput, $limit: Int, $nextToken: String) {
  lastPostsByFacilityAndDate(
    facility: $facility
    date: $date
    sortDirection: $sortDirection
    filter: $filter
    limit: $limit
    nextToken: $nextToken
  ) {
    items {
      id
      date
      month
      facility
      sentence
      weather
      waterTemp
      tide
      visitors
      fish1Name fish1MinSize fish1MaxSize fish1Unit fish1Count fish1Place
      fish2Name fish2MinSize fish2MaxSize fish2Unit fish2Count fish2Place
      fish3Name fish3MinSize fish3MaxSize fish3Unit fish3Count fish3Place
      fish4Name fish4MinSize fish4MaxSize fish4Unit fish4Count fish4Place
      fish5Name fish5MinSize fish5MaxSize fish5Unit fish5Count fish5Place
      fish6Name fish6MinSize fish6MaxSize fish6Unit fish6Count fish6Place
      fish7Name fish7MinSize fish7MaxSize fish7Unit fish7Count fish7Place
      fish8Name fish8MinSize fish8MaxSize fish8Unit fish8Count fish8Place
      fish9Name fish9MinSize fish9MaxSize fish9Unit fish9Count fish9Place
      fish10Name fish10MinSize fish10MaxSize fish10Unit fish10Count fish10Place
      fish11Name fish11MinSize fish11MaxSize fish11Unit fish11Count fish11Place
      fish12Name fish12MinSize fish12MaxSize fish12Unit fish12Count fish12Place
      fish13Name fish13MinSize fish13MaxSize fish13Unit fish13Count fish13Place
      fish14Name fish14MinSize fish14MaxSize fish14Unit fish14Count fish14Place
      fish15Name fish15MinSize fish15MaxSize fish15Unit fish15Count fish15Place
      fish16Name fish16MinSize fish16MaxSize fish16Unit fish16Count fish16Place
      fish17Name fish17MinSize fish17MaxSize fish17Unit fish17Count fish17Place
      fish18Name fish18MinSize fish18MaxSize fish18Unit fish18Count fish18Place
      fish19Name fish19MinSize fish19MaxSize fish19Unit fish19Count fish19Place
      fish20Name fish20MinSize fish20MaxSize fish20Unit fish20Count fish20Place
      fish21Name fish21MinSize fish21MaxSize fish21Unit fish21Count fish21Place
      fish22Name fish22MinSize fish22MaxSize fish22Unit fish22Count fish22Place
      fish23Name fish23MinSize fish23MaxSize fish23Unit fish23Count fish23Place
      fish24Name fish24MinSize fish24MaxSize fish24Unit fish24Count fish24Place
      fish25Name fish25MinSize fish25MaxSize fish25Unit fish25Count fish25Place
      fish26Name fish26MinSize fish26MaxSize fish26Unit fish26Count fish26Place
      fish27Name fish27MinSize fish27MaxSize fish27Unit fish27Count fish27Place
      fish28Name fish28MinSize fish28MaxSize fish28Unit fish28Count fish28Place
      fish29Name fish29MinSize fish29MaxSize fish29Unit fish29Count fish29Place
      fish30Name fish30MinSize fish30MaxSize fish30Unit fish30Count fish30Place
      images
      createdAt
      updatedAt
      __typename
    }
    nextToken
    __typename
  }
}
"""

# SESを使ってメールを送信する
def send_mail(subject: str, body: str) -> None:
    """
    AWS SESを使用してメールを送信する
    
    Args:
        subject: メールの件名
        body: メールの本文
    """
    ses.send_email(
        Source=SES_FROM,
        Destination={"ToAddresses": [SES_TO]},
        Message={
            "Subject": {"Data": subject, "Charset": "UTF-8"},
            "Body": {"Text": {"Data": body, "Charset": "UTF-8"}},
        },
    )


def pick_latest(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    アイテムリストから最新のアイテム（updatedAtが最も新しいもの）を取得する
    
    Args:
        items: アイテムのリスト
        
    Returns:
        最新のアイテム（updatedAtでソートした最初の要素）
        
    Raises:
        RuntimeError: アイテムが空の場合
    """
    if not items:
        raise RuntimeError("No items returned")
    return sorted(items, key=lambda x: x.get("updatedAt") or "", reverse=True)[0]


def put_raw_to_s3(facility: str, kind: str, date_dash: str, raw: Dict[str, Any]) -> str:
    """
    GraphQL APIから取得した生データをS3に保存する
    
    Args:
        facility: 施設名
        kind: データの種類（"field_condition", "fishing_report", "catch_count"のいずれか）
        date_dash: 日付（YYYY-MM-DD形式）
        raw: 保存する生データ（JSON形式）
        
    Returns:
        S3に保存されたオブジェクトのキー（パス）
    """
    key = f"raw/{facility}/{date_dash}/{kind}.json"
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(raw, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json; charset=utf-8",
    )
    return key


def normalize_catch_count(item: Dict[str, Any], facility: str, date_dash: str) -> Dict[str, Any]:
    """
    釣果数（catch count）のデータを正規化する
    釣果数には釣果の集計情報が含まれる
    
    Args:
        item: GraphQL APIから取得したcatch countのアイテム
        facility: 施設名
        date_dash: 日付（YYYY-MM-DD形式）
        
    Returns:
        正規化された日次データの辞書
        
    Raises:
        ValueError: 必須フィールド（weather, waterTemp, tide, visitors）が欠けている場合
    """
    required = ["weather", "waterTemp", "tide", "visitors"]
    missing = [k for k in required if item.get(k) in (None, "", [])]
    if missing:
        raise ValueError(f"Missing required fields in catch count: {missing}")

    return {
        "facility": facility,
        "date": date_dash,
        "weather": item.get("weather"),
        "waterTemp": item.get("waterTemp"),
        "tide": item.get("tide"),
        "visitors": safe_int(item.get("visitors")),
        "sentence": item.get("sentence"),
        "sourceId": item.get("id"),
        "updatedAt": item.get("updatedAt"),
    }


def normalize_field_condition(item: Dict[str, Any], facility: str, date_dash: str) -> Dict[str, Any]:
    """
    フィールド条件（field condition）のデータを正規化する
    フィールド条件にはその日の気象条件などの初期情報が含まれる
    
    Args:
        item: GraphQL APIから取得したfield conditionのアイテム
        facility: 施設名
        date_dash: 日付（YYYY-MM-DD形式）
        
    Returns:
        正規化されたfield conditionデータの辞書
    """
    return {
        "facility": facility,
        "date": date_dash,
        "firstSentence": item.get("sentence"),
        "firstWeather": item.get("weather"),
        "temp": item.get("temp"),
        "waterTempFirst": item.get("waterTemp"),
        "windDirection": item.get("windDirection"),
        "windSpeed": item.get("windSpeed"),
        "tideFirst": item.get("tide"),
        "highTide": item.get("highTide"),
        "lowTide": item.get("lowTide"),
        "warning": item.get("warning"),
        "advisory": item.get("advisory"),
        "firstSourceId": item.get("id"),
        "firstUpdatedAt": item.get("updatedAt"),
    }


def normalize_fishing_reports(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    釣果レポート（fishing reports）のデータを正規化する
    その日の途中経過情報を時系列でソートして保持する
    
    Args:
        items: GraphQL APIから取得したfishing reportのアイテムリスト
        
    Returns:
        正規化されたfishing reportデータのリスト（時間順にソート済み）
    """
    # その日のコンパクトなログリストを保持
    out = []
    for it in sorted(items, key=lambda x: (x.get("time") or "", x.get("updatedAt") or "")):
        out.append({
            "time": it.get("time"),
            "sentence": it.get("sentence"),
            "weather": it.get("weather"),
            "sourceId": it.get("id"),
            "updatedAt": it.get("updatedAt"),
        })
    return out


def normalize_fishes(item: Dict[str, Any], facility: str, date_dash: str) -> List[Dict[str, Any]]:
    """
    釣果数から魚種ごとの釣果データを抽出して正規化する
    catch countには最大30種類の魚の情報が含まれる（fish1Name～fish30Name）
    
    Args:
        item: GraphQL APIから取得したcatch countのアイテム
        facility: 施設名
        date_dash: 日付（YYYY-MM-DD形式）
        
    Returns:
        正規化された釣果データのリスト（魚種ごとに1エントリ）
    """
    fishes: List[Dict[str, Any]] = []
    for i in range(1, 31):
        name = item.get(f"fish{i}Name")
        if not name:
            continue

        fishes.append({
            "facility": facility,
            "date": date_dash,
            "fish": name,
            "count": safe_int(item.get(f"fish{i}Count")),
            "minSize": safe_int(item.get(f"fish{i}MinSize")),
            "maxSize": safe_int(item.get(f"fish{i}MaxSize")),
            "unit": item.get(f"fish{i}Unit"),
            "place": item.get(f"fish{i}Place"),
            "slot": i,
        })
    return fishes


def put_ddb_daily(daily_item: Dict[str, Any], raw_keys: Dict[str, str], fishing_report_log: List[Dict[str, Any]]) -> None:
    """
    日次データをDynamoDBのdailyテーブルに保存する
    
    Args:
        daily_item: 正規化された日次データ（天気、水温、潮、来場者数など）
        raw_keys: S3に保存された生データのキー（{"catch_count": "...", "field_condition": "...", "fishing_report": "..."}）
        fishing_report_log: その日の釣果レポートログのリスト（{time, sentence, ...}のリスト）
    """
    table = ddb.Table(DDB_DAILY_TABLE)
    pk = make_daily_pk(daily_item['facility'])
    sk = make_daily_sk(daily_item['date'])
    item = {
        "PK": pk,
        "SK": sk,
        **daily_item,
        "rawKeys": raw_keys,     # {"catch_count": "...", "field_condition": "...", "fishing_report": "..."}
        "fishingReportLog": fishing_report_log, # list of {time, sentence, ...}
    }
    table.put_item(Item=item)


def put_ddb_catches(catches: List[Dict[str, Any]]) -> None:
    """
    釣果データをDynamoDBのcatchテーブルに一括保存する
    バッチライターを使用して効率的に書き込む
    
    Args:
        catches: 正規化された釣果データのリスト（魚種ごとに1エントリ）
    """
    table = ddb.Table(DDB_CATCH_TABLE)
    with table.batch_writer() as bw:
        for c in catches:
            pk = make_catch_pk(c['facility'], c['fish'])
            sk = make_catch_sk(c['date'], c['slot'], c.get("place"))
            bw.put_item(Item={"PK": pk, "SK": sk, **c})


def fetch_kind(kind: str, facility: str, target: dt.date) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """
    指定された種類のデータをGraphQL APIから取得する
    
    Args:
        kind: データの種類（"catch_count", "field_condition", "fishing_report"のいずれか）
        facility: 施設名
        target: 取得対象の日付
        
    Returns:
        (raw, items) のタプル
        - raw: GraphQL APIからの生レスポンス全体
        - items: 抽出されたアイテムのリスト
        
    Raises:
        ValueError: 未知のkindが指定された場合
    """
    variables = {"facility": facility, "date": {"eq": ymd_slash(target)}}

    if kind == "catch_count":
        raw = appsync_post(QUERY_CATCH_COUNT, variables)
        items = raw["data"]["lastPostsByFacilityAndDate"]["items"]
        return raw, items

    if kind == "field_condition":
        raw = appsync_post(QUERY_FIELD_CONDITION, variables)
        items = raw["data"]["firstPostsByFacilityAndDate"]["items"]
        return raw, items

    if kind == "fishing_report":
        raw = appsync_post(QUERY_FISHING_REPORT, variables)
        items = raw["data"]["middlePostsByFacilityAndDate"]["items"]
        return raw, items

    raise ValueError(f"unknown kind: {kind}")


def get_target_date() -> dt.date:
    """
    対象日付を取得する
    環境変数TARGET_DATEが設定されている場合はその日付を使用、
    設定されていない場合は日本時間基準の前日を返す
    
    環境変数TARGET_DATEの形式: YYYY-MM-DD（例: "2024-01-15"）
    
    Returns:
        対象日付
    
    Raises:
        ValueError: 環境変数TARGET_DATEの形式が不正な場合
    """
    target_date_str = os.environ.get("TARGET_DATE")
    if target_date_str:
        try:
            return dt.datetime.strptime(target_date_str, "%Y-%m-%d").date()
        except ValueError as e:
            raise ValueError(f"Invalid TARGET_DATE format: {target_date_str}. Expected YYYY-MM-DD format. {e}")
    return yesterday_jst()


def get_date_range() -> Optional[Tuple[dt.date, dt.date]]:
    """
    環境変数から日付レンジを取得する
    
    環境変数START_DATEとEND_DATEが設定されている場合はそのレンジを返す
    どちらか一方のみが設定されている場合はエラーを発生させる
    
    環境変数の形式: YYYY-MM-DD（例: "2024-01-15"）
    
    Returns:
        (start_date, end_date) のタプル、または None（レンジが指定されていない場合）
    
    Raises:
        ValueError: 環境変数の形式が不正な場合、または一方のみが設定されている場合
    """
    start_date_str = os.environ.get("START_DATE")
    end_date_str = os.environ.get("END_DATE")
    
    # どちらも設定されていない場合はNoneを返す
    if not start_date_str and not end_date_str:
        return None
    
    # 一方のみが設定されている場合はエラー
    if not start_date_str or not end_date_str:
        raise ValueError(
            f"Both START_DATE and END_DATE must be set together. "
            f"START_DATE: {start_date_str}, END_DATE: {end_date_str}"
        )
    
    try:
        start_date = dt.datetime.strptime(start_date_str, "%Y-%m-%d").date()
        end_date = dt.datetime.strptime(end_date_str, "%Y-%m-%d").date()
    except ValueError as e:
        raise ValueError(
            f"Invalid date format. Expected YYYY-MM-DD format. "
            f"START_DATE: {start_date_str}, END_DATE: {end_date_str}. {e}"
        )
    
    # 開始日が終了日より後である場合はエラー
    if start_date > end_date:
        raise ValueError(
            f"START_DATE ({start_date_str}) must be before or equal to END_DATE ({end_date_str})"
        )
    
    return (start_date, end_date)


def generate_date_list(start_date: dt.date, end_date: dt.date) -> List[dt.date]:
    """
    開始日から終了日までの日付リストを生成する（両端を含む）
    
    Args:
        start_date: 開始日
        end_date: 終了日
    
    Returns:
        日付のリスト（開始日から終了日まで、時系列順）
    """
    date_list = []
    current_date = start_date
    while current_date <= end_date:
        date_list.append(current_date)
        current_date += dt.timedelta(days=1)
    return date_list


def process_single_date(facility: str, target_date: dt.date) -> Dict[str, Any]:
    """
    単一日付のデータを取得してS3とDynamoDBに保存する
    
    処理フロー:
    1. 指定日のcatch countを取得（必須）- 釣果の集計情報を含む
    2. 同じ日のfield conditionを取得（オプション）- 初期の気象条件など
    3. 同じ日のfishing reportを取得（オプション）- 途中経過情報
    4. データを正規化してS3とDynamoDBに保存
    
    Args:
        facility: 施設名
        target_date: 処理対象の日付
        
    Returns:
        処理結果の辞書（status, date, catches数, raw keys）
        
    Raises:
        Exception: 処理中にエラーが発生した場合
    """
    date_dash = ymd_dash(target_date)
    raw_keys: Dict[str, str] = {}

    # --- catch_count (required) ---
    # 釣果数を取得（必須）- 釣果の集計情報が含まれる
    raw_catch_count, items_catch_count = fetch_kind("catch_count", facility, target_date)
    item_catch_count = pick_latest(items_catch_count)
    raw_keys["catch_count"] = put_raw_to_s3(facility, "catch_count", date_dash, raw_catch_count)

    # 釣果数から日次データと釣果データを正規化
    daily = normalize_catch_count(item_catch_count, facility, date_dash)
    catches = normalize_fishes(item_catch_count, facility, date_dash)
    if not catches:
        # 致命的ではないが、警告に値する
        pass

    # --- field_condition (optional) ---
    # フィールド条件を取得（オプション）- 初期の気象条件などが含まれる
    # 最初のリリースでは無効化（後から追加する可能性があるためコメントアウト）
    # field_condition_payload = {}
    # try:
    #     raw_field_condition, items_field_condition = fetch_kind("field_condition", facility, target_date)
    #     if items_field_condition:
    #         item_field_condition = pick_latest(items_field_condition)
    #         raw_keys["field_condition"] = put_raw_to_s3(facility, "field_condition", date_dash, raw_field_condition)
    #         daily.update(normalize_field_condition(item_field_condition, facility, date_dash))
    #     else:
    #         field_condition_payload = {"note": "no field condition posts"}
    # except Exception as e:
    #     field_condition_payload = {"error": f"{e}"}

    # --- fishing_report (optional) ---
    # 釣果レポートを取得（オプション）- その日の途中経過情報が含まれる
    # 最初のリリースでは無効化（後から追加する可能性があるためコメントアウト）
    fishing_report_log: List[Dict[str, Any]] = []
    # fishing_report_payload = {}
    # try:
    #     raw_fishing_report, items_fishing_report = fetch_kind("fishing_report", facility, target_date)
    #     raw_keys["fishing_report"] = put_raw_to_s3(facility, "fishing_report", date_dash, raw_fishing_report)
    #     fishing_report_log = normalize_fishing_reports(items_fishing_report)
    # except Exception as e:
    #     fishing_report_payload = {"error": f"{e}"}

    # --- persist ---
    # DynamoDBにデータを保存
    put_ddb_daily(daily, raw_keys, fishing_report_log)
    put_ddb_catches(catches)

    return {"status": "ok", "date": date_dash, "catches": len(catches), "raw": raw_keys}


def lambda_handler(event, context):
    """
    Lambda関数のエントリーポイント
    指定された日付（または日付レンジ）の釣果データを取得し、S3とDynamoDBに保存する
    
    環境変数の設定:
    - START_DATE, END_DATE: 日付レンジを指定（両方設定が必要、形式: YYYY-MM-DD）
    - TARGET_DATE: 単一日付を指定（形式: YYYY-MM-DD）
    - どちらも設定されていない場合: 前日を処理
    
    処理フロー:
    1. 環境変数から日付レンジまたは単一日付を取得
    2. 各日付に対してシリアルにデータ取得処理を実行
    3. 成功/失敗のメール通知を送信
    
    Args:
        event: Lambdaイベント（facilityを指定可能）
        context: Lambdaコンテキスト
        
    Returns:
        処理結果の辞書（status, processed_dates, results）
        
    Raises:
        Exception: 処理中にエラーが発生した場合（メール通知後に再スロー）
    """
    # イベントから施設名を取得、なければデフォルト値を使用
    facility = (event.get("facility") if isinstance(event, dict) else None) or FACILITY_DEFAULT

    try:
        # 日付レンジまたは単一日付を取得
        date_range = get_date_range()
        
        if date_range:
            # 日付レンジが指定されている場合
            start_date, end_date = date_range
            target_dates = generate_date_list(start_date, end_date)
        else:
            # 単一日付または前日を処理
            target_date = get_target_date()
            target_dates = [target_date]

        # 各日付に対してシリアルに処理を実行
        results: List[Dict[str, Any]] = []
        errors: List[Dict[str, Any]] = []
        
        for target_date in target_dates:
            date_dash = ymd_dash(target_date)
            try:
                result = process_single_date(facility, target_date)
                results.append(result)
            except Exception as e:
                error_info = {
                    "date": date_dash,
                    "error": str(e),
                }
                errors.append(error_info)
                # エラーが発生しても次の日付の処理を続行

        # メール通知を送信
        processed_dates = [r["date"] for r in results]
        total_catches = sum(r["catches"] for r in results)
        
        if errors:
            # エラーが発生した場合
            error_summary = "\n".join([f"- {e['date']}: {e['error']}" for e in errors])
            body = (
                f"バッチ処理完了（一部エラーあり）\n"
                f"facility: {facility}\n"
                f"処理日数: {len(results)}/{len(target_dates)}\n"
                f"成功日付: {', '.join(processed_dates)}\n"
                f"総釣果数: {total_catches}\n\n"
                f"エラー:\n{error_summary}\n"
            )
            send_mail(
                subject=f"[WARN] fishing batch {facility} ({len(results)}/{len(target_dates)} success)",
                body=body,
            )
        else:
            # 全て成功した場合
            body = (
                f"バッチ処理成功\n"
                f"facility: {facility}\n"
                f"処理日数: {len(results)}\n"
                f"処理日付: {', '.join(processed_dates)}\n"
                f"総釣果数: {total_catches}\n"
            )
            send_mail(
                subject=f"[OK] fishing batch {facility} ({len(results)} dates)",
                body=body,
            )

        return {
            "status": "ok" if not errors else "partial",
            "processed_dates": processed_dates,
            "total_dates": len(target_dates),
            "total_catches": total_catches,
            "results": results,
            "errors": errors if errors else None,
        }

    except Exception as e:
        # エラー発生時はメール通知を送信してから再スロー
        send_mail(
            subject=f"[NG] fishing batch {facility}",
            body=f"Error: {e}\n\nEvent:\n{json.dumps(event, ensure_ascii=False)}",
        )
        raise
