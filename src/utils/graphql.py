# src/utils/graphql.py
import json
import os
from typing import Any, Dict, Optional

import boto3
import httpx

APPSYNC_URL = os.environ["APPSYNC_URL"]
APPSYNC_API_KEY_SECRET_ID = os.environ["APPSYNC_API_KEY_SECRET_ID"]

# Secrets ManagerからAPIキーを取得
# SecretIdにはARNまたはシークレット名のどちらでも指定可能
secrets_client = boto3.client("secretsmanager")
_secret_value = secrets_client.get_secret_value(SecretId=APPSYNC_API_KEY_SECRET_ID)
_secret_dict = json.loads(_secret_value["SecretString"])
APPSYNC_API_KEY = _secret_dict["apiKey"]


def appsync_post(query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
    """AppSync に GraphQL クエリを POST する共通処理"""
    headers = {
        "content-type": "application/json; charset=UTF-8",
        "x-api-key": APPSYNC_API_KEY,
        "accept": "application/json",
    }
    payload = {"query": query, "variables": variables}

    last_err: Optional[Exception] = None
    for _ in range(3):
        try:
            with httpx.Client(timeout=20.0) as client:
                r = client.post(APPSYNC_URL, headers=headers, json=payload)
                r.raise_for_status()
                data = r.json()
                if "errors" in data:
                    raise RuntimeError(f"GraphQL errors: {data['errors']}")
                return data
        except Exception as e:
            last_err = e

    raise RuntimeError(f"AppSync request failed after retries: {last_err}")

