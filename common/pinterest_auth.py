"""Pinterest OAuth token handling, backed by AWS Secrets Manager.

Pinterest access tokens expire (~60 days), so every job run exchanges the
stored refresh token for a fresh access token rather than caching one. If
Pinterest rotates the refresh token in the response (it can), the new value
is written back to Secrets Manager so the next scheduled run still works
unattended.
"""

import base64
import json
import logging

import boto3
import requests

from pinterest_config import PINTEREST_API_BASE

logger = logging.getLogger(__name__)


def get_secret(secret_name: str, region: str) -> dict:
    """Fetch and JSON-decode a secret: {"client_id", "client_secret", "refresh_token"}."""
    client = boto3.client("secretsmanager", region_name=region)
    resp = client.get_secret_value(SecretId=secret_name)
    return json.loads(resp["SecretString"])


def put_secret(secret_name: str, region: str, payload: dict) -> None:
    client = boto3.client("secretsmanager", region_name=region)
    client.put_secret_value(SecretId=secret_name, SecretString=json.dumps(payload))


def refresh_access_token(secret_name: str, region: str, creds: dict) -> str:
    """Exchange the stored refresh token for a fresh access token."""
    basic_auth = base64.b64encode(
        f"{creds['client_id']}:{creds['client_secret']}".encode("utf-8")
    ).decode("utf-8")

    resp = requests.post(
        f"{PINTEREST_API_BASE}/oauth/token",
        headers={
            "Authorization": f"Basic {basic_auth}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        data={
            "grant_type": "refresh_token",
            "refresh_token": creds["refresh_token"],
        },
        timeout=30,
    )
    resp.raise_for_status()
    body = resp.json()

    new_refresh_token = body.get("refresh_token")
    if new_refresh_token and new_refresh_token != creds["refresh_token"]:
        logger.info("Pinterest rotated the refresh token; updating Secrets Manager")
        updated = dict(creds)
        updated["refresh_token"] = new_refresh_token
        put_secret(secret_name, region, updated)

    return body["access_token"]
