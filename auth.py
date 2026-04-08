import requests
import pyotp
import json
import os
from dhanhq import dhanhq

BASE_AUTH   = "https://auth.dhan.co"
BASE_API    = "https://api.dhan.co/v2"
CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath("__file__")), "Data", "config.json")
session     = requests.Session()

def login():

    # ── Load config ──────────────────────────────────────────────────
    with open(CONFIG_PATH, encoding="utf-8") as f:
        config = json.load(f)

    CLIENT_ID   = config["client_id"]
    PIN         = config["pin"]
    TOTP_SECRET = config["totp_secret"]
    access_token   = config.get("access_token", "")
    dhan_client_id = CLIENT_ID

    def save_token(token):
        config["access_token"] = token
        with open(CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(config, f, indent=2)

    def auth_headers():
        return {"access-token": access_token, "dhanClientId": dhan_client_id}

    def is_token_valid():
        try:
            resp = session.get(f"{BASE_API}/profile", headers=auth_headers(), timeout=5)
            return resp.status_code == 200
        except Exception:
            return False

    def renew_token():
        global access_token
        try:
            resp = session.get(f"{BASE_API}/RenewToken", headers=auth_headers(), timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                new_token = data.get("accessToken") or data.get("access_token")
                if new_token:
                    access_token = new_token
                    save_token(access_token)
                    return True
            return False
        except Exception:
            return False

    def fresh_login():
        global access_token
        totp_code = pyotp.TOTP(TOTP_SECRET).now()
        resp = session.post(
            f"{BASE_AUTH}/app/generateAccessToken",
            params={"dhanClientId": CLIENT_ID, "pin": PIN, "totp": totp_code},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("status") == "error":
            raise RuntimeError(f"Login failed: {data.get('message')}")
        token = data.get("accessToken") or data.get("access_token")
        if not token:
            raise RuntimeError(f"No token in response: {data}")
        access_token = token
        save_token(access_token)

    # ── Smart login flow ─────────────────────────────────────────────
    if access_token and is_token_valid():
        print("Token from config is valid. Skipping login.")
    elif access_token and renew_token():
        print("Token renewed successfully.")
    else:
        print("Doing fresh TOTP login...")
        fresh_login()
        print("Login successful. Token saved to config.")

    print(f"Ready. Token: {access_token[:40]}...")

    dhan = dhanhq(dhan_client_id, access_token)

    return dhan
