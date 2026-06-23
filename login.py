from kiteconnect import KiteConnect
from flask import Flask, request
import webbrowser
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ---------- Read cred.inf ----------
def load_credentials(path):
    creds = {}
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if "=" in line:
                key, value = line.split("=", 1)
                creds[key.strip()] = value.strip()
    return creds

cred_path = os.path.join(BASE_DIR, "cred.inf")
creds = load_credentials(cred_path)

API_KEY = creds["API_KEY"]
API_SECRET = creds["API_SECRET"]

# ---------- Kite setup ----------
kite = KiteConnect(api_key=API_KEY)

app = Flask(__name__)

@app.route("/")
def callback():
    request_token = request.args.get("request_token")

    data = kite.generate_session(
        request_token,
        api_secret=API_SECRET
    )

    access_token = data["access_token"]

    token_path = os.path.join(BASE_DIR, "access_token.txt")
    with open(token_path, "w") as f:
        f.write(access_token)

    kite.set_access_token(access_token)

    return "Login successful. Token saved."

if __name__ == "__main__":
    webbrowser.open(kite.login_url())
    app.run(port=5000, debug=False)