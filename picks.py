import base64
import time
import datetime
import os
import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

API_KEY_ID = os.environ["KALSHI_API_KEY_ID"]
PRIVATE_KEY_PEM = os.environ["KALSHI_PRIVATE_KEY"]

BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
API_PREFIX = "/trade-api/v2"


def get_private_key():
    return serialization.load_pem_private_key(PRIVATE_KEY_PEM.encode(), password=None)


def get_headers(method: str, path: str) -> dict:
    private_key = get_private_key()
    timestamp = str(int(time.time() * 1000))
    full_path = API_PREFIX + path
    message = timestamp + method.upper() + full_path
    signature = private_key.sign(
        message.encode("utf-8"),
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=32),
        hashes.SHA256(),
    )
    return {
        "KALSHI-ACCESS-KEY": API_KEY_ID,
        "KALSHI-ACCESS-TIMESTAMP": timestamp,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(signature).decode(),
        "Content-Type": "application/json",
    }


def fetch_market(ticker: str) -> dict | None:
    path = f"/markets/{ticker}"
    resp = requests.get(BASE_URL + path, headers=get_headers("GET", path))
    if resp.status_code == 200:
        return resp.json().get("market", resp.json())
    return None


def get_today_str():
    # Today in format 26MAR29
    now = datetime.datetime.utcnow()
    months = ["JAN","FEB","MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC"]
    return f"26{months[now.month-1]}{now.day:02d}"


def scan_todays_game_tickers():
    """
    Scan open markets and extract unique single-game tickers for today
    from the parlay leg data.
    """
    path = "/markets"
    all_markets = []
    cursor = None
    while len(all_markets) < 500:
        headers = get_headers("GET", path)
        params = {"limit": 100, "status": "open"}
        if cursor:
            params["cursor"] = cursor
        resp = requests.get(BASE_URL + path, headers=headers, params=params)
        data = resp.json()
        batch = data.get("markets", [])
        all_markets.extend(batch)
        cursor = data.get("cursor")
        if not cursor or not batch:
            break

    today = get_today_str()
    unique_tickers = set()
    for m in all_markets:
        cs = m.get("custom_strike", {})
        assoc = cs.get("Associated Markets", "")
        for t in assoc.split(","):
            t = t.strip()
            if today in t and any(s in t for s in [
                "KXNBAGAME", "KXMLBGAME", "KXNHLGAME",
                "KXNCAAMBGAME", "KXNCAAMBSPREAD"
            ]):
                unique_tickers.add(t)
        for leg in m.get("mve_selected_legs", []):
            t = leg.get("market_ticker", "")
            if today in t and any(s in t for s in [
                "KXNBAGAME", "KXMLBGAME", "KXNHLGAME",
                "KXNCAAMBGAME", "KXNCAAMBSPREAD"
            ]):
                unique_tickers.add(t)
    return unique_tickers


def score_pick(market: dict) -> float:
    """
    Score a market for pick-worthiness.
    Prefer: high yes_ask (favorite), high volume (sharp money), game winners over spreads.
    """
    yes = float(market.get("yes_ask_dollars", "0") or 0)
    vol = float(market.get("volume_fp", "0") or 0)
    ticker = market.get("ticker", "")

    # Only consider clear favorites 55c-93c (not near-locks, not coinflips)
    if yes < 0.55 or yes > 0.93:
        return 0

    # Game winners preferred over spreads
    is_game = "GAME" in ticker and "SPREAD" not in ticker
    type_bonus = 1.2 if is_game else 1.0

    # Score: blend of confidence and market volume (sharps agree)
    score = yes * 0.6 + min(vol / 500000, 1.0) * 0.4
    return score * type_bonus


def get_top_picks(n=5):
    print("Scanning today's markets...")
    tickers = scan_todays_game_tickers()
    print(f"Found {len(tickers)} candidate tickers")

    candidates = []
    seen_events = set()

    for ticker in tickers:
        market = fetch_market(ticker)
        if not market:
            continue

        score = score_pick(market)
        if score == 0:
            continue

        # Deduplicate by event (don't pick both sides of same game)
        event = market.get("event_ticker", ticker)
        if event in seen_events:
            continue
        seen_events.add(event)

        yes = float(market.get("yes_ask_dollars", "0") or 0)
        vol = float(market.get("volume_fp", "0") or 0)
        title = market.get("title", ticker)

        candidates.append({
            "ticker": ticker,
            "title": title,
            "yes": yes,
            "volume": vol,
            "score": score,
        })

    # Sort by score descending
    candidates.sort(key=lambda x: x["score"], reverse=True)
    return candidates[:n]


def sport_emoji(ticker: str) -> str:
    if "NBA" in ticker: return "🏀"
    if "MLB" in ticker: return "⚾"
    if "NHL" in ticker: return "🏒"
    if "NCAAMB" in ticker: return "🏀"
    if "NCAAWB" in ticker: return "🏀"
    return "🎯"


def format_picks(picks: list) -> str:
    today = datetime.datetime.utcnow().strftime("%B %d, %Y")
    lines = [f"## 🎯 Kalshi Top {len(picks)} Picks — {today}\n"]
    lines.append("| # | Pick | Confidence | Volume |")
    lines.append("|---|------|-----------|--------|")

    for i, p in enumerate(picks, 1):
        emoji = sport_emoji(p["ticker"])
        conf = int(p["yes"] * 100)
        vol = int(p["volume"])
        title = p["title"][:60]
        lines.append(f"| {i} | {emoji} {title} | {conf}¢ | {vol:,} trades |")

    lines.append("\n> Auto-generated by kalchi-picks. Not financial advice.")
    return "\n".join(lines)


def create_github_issue(title: str, body: str):
    repo = "kalchikee/kalchi-picks"
    gh_token = os.environ["GITHUB_TOKEN"]
    resp = requests.post(
        f"https://api.github.com/repos/{repo}/issues",
        headers={
            "Authorization": f"token {gh_token}",
            "Accept": "application/vnd.github.v3+json",
        },
        json={"title": title, "body": body, "labels": ["picks"]},
    )
    resp.raise_for_status()
    print(f"Issue created: {resp.json()['html_url']}")


if __name__ == "__main__":
    picks = get_top_picks(5)
    if not picks:
        print("No picks found for today.")
        body = "No qualifying markets found for today. Check back tomorrow!"
    else:
        body = format_picks(picks)
        print(body)

    today = datetime.datetime.utcnow().strftime("%B %d, %Y")
    create_github_issue(f"📊 Daily Picks — {today}", body)
