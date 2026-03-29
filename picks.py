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


# Teams that are almost always heavy favorites — if they're priced UNDER 45c,
# something is wrong (injury, rest game, etc). Skip those markets entirely.
STRONG_TEAMS = {
    # NBA elite
    "BOS", "OKC", "DEN", "LAC", "HOU", "CLE", "NYK", "MIN",
    # MLB perennial contenders
    "LAD", "NYY", "ATL", "HOU", "PHI", "NYM",
    # NHL powerhouses
    "FLA", "DAL", "COL", "CAR", "NYR", "WPG",
    # NCAA bluebloods
    "DUKE", "KU", "GONZ", "CONN", "UK", "UCLA",
}


def is_suspicious_market(ticker: str, yes: float) -> bool:
    """
    Returns True if a strong team is priced suspiciously low (likely injured/resting).
    We should skip these — the market knows something we don't.
    """
    parts = ticker.split("-")
    if len(parts) < 3:
        return False
    team_code = parts[-1].upper()
    import re
    team_code = re.sub(r'\d+$', '', team_code)
    # If a known strong team is the pick but priced under 45c, skip it
    if team_code in STRONG_TEAMS and yes < 0.45:
        print(f"  Skipping suspicious market: {ticker} (strong team at only {int(yes*100)}c)")
        return True
    # Also skip near-coinflips (50-55c) for strong teams — not worth the risk
    if team_code in STRONG_TEAMS and yes < 0.58:
        print(f"  Skipping low-confidence strong team: {ticker} ({int(yes*100)}c)")
        return True
    return False


def score_pick(market: dict) -> float:
    """
    Score a market for pick-worthiness.
    Prefer: high yes_ask (favorite), high volume (sharp money), game winners over spreads.
    """
    yes = float(market.get("yes_ask_dollars", "0") or 0)
    vol = float(market.get("volume_fp", "0") or 0)
    ticker = market.get("ticker", "")

    # Only consider clear favorites 58c-93c (not near-locks, not coinflips)
    if yes < 0.58 or yes > 0.93:
        return 0

    # Skip suspicious markets where something seems off
    if is_suspicious_market(ticker, yes):
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


TEAM_NAMES = {
    # NBA
    "ATL": "Atlanta Hawks", "BOS": "Boston Celtics", "BKN": "Brooklyn Nets",
    "CHA": "Charlotte Hornets", "CHI": "Chicago Bulls", "CLE": "Cleveland Cavaliers",
    "DAL": "Dallas Mavericks", "DEN": "Denver Nuggets", "DET": "Detroit Pistons",
    "GSW": "Golden State Warriors", "HOU": "Houston Rockets", "IND": "Indiana Pacers",
    "LAC": "LA Clippers", "LAL": "LA Lakers", "MEM": "Memphis Grizzlies",
    "MIA": "Miami Heat", "MIL": "Milwaukee Bucks", "MIN": "Minnesota Timberwolves",
    "NOP": "New Orleans Pelicans", "NYK": "New York Knicks", "OKC": "Oklahoma City Thunder",
    "ORL": "Orlando Magic", "PHI": "Philadelphia 76ers", "PHX": "Phoenix Suns",
    "POR": "Portland Trail Blazers", "SAC": "Sacramento Kings", "SAS": "San Antonio Spurs",
    "TOR": "Toronto Raptors", "UTA": "Utah Jazz", "WAS": "Washington Wizards",
    # MLB
    "ATH": "Oakland Athletics", "BAL": "Baltimore Orioles", "BOS": "Boston Red Sox",
    "CHC": "Chicago Cubs", "CWS": "Chicago White Sox", "CIN": "Cincinnati Reds",
    "CLE": "Cleveland Guardians", "COL": "Colorado Rockies", "DET": "Detroit Tigers",
    "HOU": "Houston Astros", "KC": "Kansas City Royals", "LAA": "LA Angels",
    "LAD": "LA Dodgers", "MIA": "Miami Marlins", "MIL": "Milwaukee Brewers",
    "MIN": "Minnesota Twins", "NYM": "New York Mets", "NYY": "New York Yankees",
    "OAK": "Oakland A's", "PHI": "Philadelphia Phillies", "PIT": "Pittsburgh Pirates",
    "SD": "San Diego Padres", "SEA": "Seattle Mariners", "SF": "San Francisco Giants",
    "STL": "St. Louis Cardinals", "TB": "Tampa Bay Rays", "TEX": "Texas Rangers",
    "TOR": "Toronto Blue Jays", "WSH": "Washington Nationals",
    # NHL
    "ANA": "Anaheim Ducks", "ARI": "Arizona Coyotes", "BOS": "Boston Bruins",
    "BUF": "Buffalo Sabres", "CAR": "Carolina Hurricanes", "CBJ": "Columbus Blue Jackets",
    "CGY": "Calgary Flames", "CHI": "Chicago Blackhawks", "COL": "Colorado Avalanche",
    "DAL": "Dallas Stars", "DET": "Detroit Red Wings", "EDM": "Edmonton Oilers",
    "FLA": "Florida Panthers", "LAK": "LA Kings", "MIN": "Minnesota Wild",
    "MTL": "Montreal Canadiens", "NJ": "New Jersey Devils", "NSH": "Nashville Predators",
    "NYI": "NY Islanders", "NYR": "NY Rangers", "OTT": "Ottawa Senators",
    "PHI": "Philadelphia Flyers", "PIT": "Pittsburgh Penguins", "SEA": "Seattle Kraken",
    "SJ": "San Jose Sharks", "STL": "St. Louis Blues", "TB": "Tampa Bay Lightning",
    "TOR": "Toronto Maple Leafs", "VAN": "Vancouver Canucks", "VGK": "Vegas Golden Knights",
    "WPG": "Winnipeg Jets", "WSH": "Washington Capitals",
    # NCAA
    "DUKE": "Duke", "MICH": "Michigan", "TENN": "Tennessee", "CONN": "UConn",
    "KU": "Kansas", "HOU": "Houston", "ILL": "Illinois", "ARK": "Arkansas",
    "FLA": "Florida", "ARIZ": "Arizona", "MSU": "Michigan State", "ISU": "Iowa State",
    "UK": "Kentucky", "PUR": "Purdue", "UCLA": "UCLA", "ALA": "Alabama",
    "GONZ": "Gonzaga", "TEX": "Texas", "BYU": "BYU", "ISU": "Iowa State",
}


def extract_pick(ticker: str) -> str:
    """Extract the team/side being picked from the ticker."""
    # Ticker format: KXNBAGAME-26MAR29NYKOKC-OKC  → pick is OKC
    parts = ticker.split("-")
    if len(parts) >= 3:
        team_code = parts[-1].upper()
        # Strip any numeric suffix (e.g. DUKE4 -> DUKE)
        import re
        team_code = re.sub(r'\d+$', '', team_code)
        return TEAM_NAMES.get(team_code, team_code)
    return ticker


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
    lines.append("| # | Bet | Game | Confidence | Volume |")
    lines.append("|---|-----|------|-----------|--------|")

    for i, p in enumerate(picks, 1):
        emoji = sport_emoji(p["ticker"])
        conf = int(p["yes"] * 100)
        vol = int(p["volume"])
        pick_name = extract_pick(p["ticker"])
        game_title = p["title"][:50]
        lines.append(f"| {i} | {emoji} **{pick_name} to WIN** | {game_title} | {conf}¢ | {vol:,} |")

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


def send_email(subject: str, picks: list):
    resend_key = os.environ["RESEND_API_KEY"]
    today = datetime.datetime.utcnow().strftime("%B %d, %Y")

    rows = ""
    for i, p in enumerate(picks, 1):
        emoji = sport_emoji(p["ticker"])
        conf = int(p["yes"] * 100)
        vol = int(p["volume"])
        pick_name = extract_pick(p["ticker"])
        game_title = p["title"][:55]
        color = "#2ecc71" if conf >= 75 else "#f39c12"
        rows += f"""
        <tr style="border-bottom:1px solid #eee;">
          <td style="padding:12px;text-align:center;font-weight:bold;font-size:18px;">{i}</td>
          <td style="padding:12px;">
            <div style="font-size:16px;font-weight:bold;">{emoji} BET: {pick_name} to WIN</div>
            <div style="font-size:12px;color:#888;margin-top:3px;">{game_title}</div>
          </td>
          <td style="padding:12px;text-align:center;">
            <span style="background:{color};color:#fff;padding:4px 10px;border-radius:12px;font-weight:bold;">{conf}¢</span>
          </td>
          <td style="padding:12px;text-align:center;color:#888;font-size:12px;">{vol:,} trades</td>
        </tr>"""

    html = f"""
    <div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;">
      <div style="background:#1a1a2e;padding:20px;border-radius:8px 8px 0 0;">
        <h1 style="color:#fff;margin:0;font-size:22px;">🎯 Kalshi Daily Picks</h1>
        <p style="color:#aaa;margin:5px 0 0;">{today}</p>
      </div>
      <table style="width:100%;border-collapse:collapse;background:#fff;border:1px solid #eee;">
        <thead>
          <tr style="background:#f8f9fa;">
            <th style="padding:10px;">#</th>
            <th style="padding:10px;text-align:left;">Pick</th>
            <th style="padding:10px;">Confidence</th>
            <th style="padding:10px;">Volume</th>
          </tr>
        </thead>
        <tbody>{rows}</tbody>
      </table>
      <div style="background:#f8f9fa;padding:12px;border-radius:0 0 8px 8px;font-size:12px;color:#888;">
        Auto-generated by kalchi-picks. Not financial advice.
      </div>
    </div>"""

    resp = requests.post(
        "https://api.resend.com/emails",
        headers={"Authorization": f"Bearer {resend_key}", "Content-Type": "application/json"},
        json={
            "from": "Kalshi Picks <onboarding@resend.dev>",
            "to": ["kalchikethan@gmail.com"],
            "subject": subject,
            "html": html,
        },
    )
    resp.raise_for_status()
    print(f"Email sent! ID: {resp.json().get('id')}")


if __name__ == "__main__":
    picks = get_top_picks(5)
    today = datetime.datetime.utcnow().strftime("%B %d, %Y")

    if not picks:
        print("No picks found for today.")
        body = "No qualifying markets found for today. Check back tomorrow!"
    else:
        body = format_picks(picks)
        print(body)

    create_github_issue(f"📊 Daily Picks — {today}", body)
    if picks:
        send_email(f"🎯 Kalshi Top 5 Picks — {today}", picks)
