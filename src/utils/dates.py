# src/utils/dates.py
import datetime as dt


def ymd_slash(d: dt.date) -> str:
    """日付を YYYY/MM/DD 形式の文字列に変換"""
    return d.strftime("%Y/%m/%d")


def ymd_dash(d: dt.date) -> str:
    """日付を YYYY-MM-DD 形式の文字列に変換"""
    return d.strftime("%Y-%m-%d")


def yesterday_jst() -> dt.date:
    """
    日本時間（JST）基準で前日の日付を取得する
    LambdaはUTC環境で実行されるため、UTCの現在時刻をJSTに変換してから日付を計算する
    
    Returns:
        日本時間基準の前日の日付
    """
    # UTCの現在時刻を取得（timezone.utcを使用）
    utc_now = dt.datetime.now(dt.timezone.utc)
    # JST（UTC+9）に変換
    jst_tz = dt.timezone(dt.timedelta(hours=9))
    jst_now = utc_now.astimezone(jst_tz)
    # 日付を取得して前日を計算
    return jst_now.date() - dt.timedelta(days=1)

