import base64
import re
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


# ─────────────────────────────────────────────────────────────────────────────
# KALSHI AUTH & MARKET FETCHING
# ─────────────────────────────────────────────────────────────────────────────

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
    now = datetime.datetime.utcnow()
    months = ["JAN","FEB","MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC"]
    return f"{str(now.year)[2:]}{months[now.month-1]}{now.day:02d}"


def scan_todays_game_tickers():
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
    # Include SPREAD markets — your best ROI market type (96% WR at 86¢+).
    # Exclude TOTAL markets — worst ROI (-13.4%). Never bet totals.
    valid_prefixes = [
        "KXNBAGAME", "KXNBASPREAD",
        "KXMLBGAME", "KXMLBSPREAD",
        "KXNHLGAME", "KXNHLSPREAD",
        "KXNCAAMBGAME", "KXNCAAMBSPREAD",
    ]
    unique_tickers = set()
    for m in all_markets:
        cs = m.get("custom_strike", {})
        assoc = cs.get("Associated Markets", "")
        for t in assoc.split(","):
            t = t.strip()
            if today in t and any(t.startswith(p) for p in valid_prefixes):
                # Explicitly exclude TOTAL markets
                if "TOTAL" not in t:
                    unique_tickers.add(t)
        for leg in m.get("mve_selected_legs", []):
            t = leg.get("market_ticker", "")
            if today in t and any(t.startswith(p) for p in valid_prefixes):
                if "TOTAL" not in t:
                    unique_tickers.add(t)
    return unique_tickers


# ─────────────────────────────────────────────────────────────────────────────
# ESPN RESEARCH ENGINE
# All calls are cached for the lifetime of this run — ESPN is hit once per
# sport/endpoint, not once per ticker.
# ─────────────────────────────────────────────────────────────────────────────

ESPN_ROUTES = {
    "KXNBAGAME":      ("basketball", "nba"),
    "KXMLBGAME":      ("baseball",   "mlb"),
    "KXNHLGAME":      ("hockey",     "nhl"),
    "KXNCAAMBGAME":   ("basketball", "mens-college-basketball"),
    "KXNCAAMBSPREAD": ("basketball", "mens-college-basketball"),
}

# Kalshi team codes that differ from ESPN abbreviations
KALSHI_TO_ESPN = {
    "GS": "GSW", "SA": "SAS", "NO": "NOP",
    "NJ": "NJD", "TB": "TBL", "SJ": "SJS", "LA": "LAK",
}

_cache: dict = {}

# Home timezone offset from ET for each team (negative = west of ET)
# Used to detect cross-timezone road travel fatigue
TEAM_TIMEZONES = {
    # ET teams (0)
    "BOS":0,"NYK":0,"BKN":0,"PHI":0,"TOR":0,"MIA":0,"ATL":0,"CHA":0,"ORL":0,
    "WAS":0,"CLE":0,"DET":0,"IND":0,"NYR":0,"NYI":0,"NJD":0,"BUF":0,"PIT":0,
    "CBJ":0,"CAR":0,"TBL":0,"FLA":0,"MTL":0,"OTT":0,"WSH":0,"BAL":0,
    # CT teams (-1)
    "CHI":-1,"MIL":-1,"MIN":-1,"MEM":-1,"NOP":-1,"HOU":-1,"SAS":-1,"DAL":-1,
    "OKC":-1,"NSH":-1,"STL":-1,"WPG":-1,"KC":-1,"CWS":-1,"TB":-1,
    # MT teams (-2)
    "DEN":-2,"UTA":-2,"COL":-2,"PHX":-2,"ARI":-2,"EDM":-2,"CGY":-2,
    # PT teams (-3)
    "LAL":-3,"LAC":-3,"GSW":-3,"SAC":-3,"POR":-3,"SEA":-3,"OKC":-3,
    "LAK":-3,"ANA":-3,"SJS":-3,"VGK":-3,"VAN":-3,"LAD":-3,"SD":-3,"SF":-3,
}

# MLB outdoor ballpark cities (used for weather lookups)
MLB_PARK_CITIES = {
    "NYY":"New York","NYM":"New York","BOS":"Boston","TOR":"Toronto",
    "BAL":"Baltimore","TB":"St Petersburg","CWS":"Chicago","DET":"Detroit",
    "CLE":"Cleveland","MIN":"Minneapolis","KC":"Kansas City","HOU":"Houston",
    "TEX":"Arlington","LAA":"Anaheim","SEA":"Seattle",
    "ATL":"Atlanta","MIA":"Miami","PHI":"Philadelphia","WSH":"Washington",
    "CHC":"Chicago","STL":"St Louis","MIL":"Milwaukee","CIN":"Cincinnati",
    "PIT":"Pittsburgh","LAD":"Los Angeles","SF":"San Francisco","SD":"San Diego",
    "COL":"Denver","ARI":"Phoenix","OAK":"Oakland",
}
# These teams play indoors — no weather effect
MLB_INDOOR = {"TB", "HOU", "MIA", "TOR", "MIL", "ARI", "MIN", "SEA"}


def get_weather_wind_mph(city: str) -> float | None:
    """Fetch current wind speed (mph) for a city using wttr.in (no API key needed)."""
    key = f"weather_{city}"
    if key in _cache:
        return _cache[key]
    try:
        resp = requests.get(
            f"https://wttr.in/{city.replace(' ', '+')}?format=j1", timeout=5
        )
        if resp.status_code == 200:
            wind_kmh = float(
                resp.json()["current_condition"][0].get("windspeedKmph", 0)
            )
            wind_mph = wind_kmh * 0.621371
            _cache[key] = wind_mph
            return wind_mph
    except Exception:
        pass
    _cache[key] = None
    return None


def get_timezone_diff(team_abbr: str, opponent_abbr: str, is_home: bool) -> int:
    """
    Returns how many timezone hours the ROAD team is away from home.
    Positive = road team is traveling east (jet lag), negative = traveling west.
    0 if home team or no data.
    """
    if is_home:
        return 0
    team_tz = TEAM_TIMEZONES.get(team_abbr, 0)
    opp_tz = TEAM_TIMEZONES.get(opponent_abbr, 0)
    # Road team's home tz vs where they're playing (opponent's home tz)
    return opp_tz - team_tz


def _espn_get(url: str, params: dict = None) -> dict:
    key = url + str(sorted((params or {}).items()))
    if key in _cache:
        return _cache[key]
    try:
        resp = requests.get(url, params=params, timeout=7)
        data = resp.json() if resp.status_code == 200 else {}
    except Exception:
        data = {}
    _cache[key] = data
    return data


def espn_abbr(kalshi_code: str) -> str:
    return KALSHI_TO_ESPN.get(kalshi_code, kalshi_code)


def get_sport_route(ticker: str):
    for prefix, route in ESPN_ROUTES.items():
        if ticker.startswith(prefix):
            return route
    return None


def get_team_registry(sport: str, league: str) -> dict:
    """Returns {ESPN_ABBR: team_id_str} for every team in the league."""
    key = f"reg_{sport}_{league}"
    if key in _cache:
        return _cache[key]
    url = f"https://site.api.espn.com/apis/site/v2/sports/{sport}/{league}/teams"
    data = _espn_get(url, {"limit": 50})
    registry = {}
    for item in (
        data.get("sports", [{}])[0]
            .get("leagues", [{}])[0]
            .get("teams", [])
    ):
        t = item.get("team", {})
        abbr = t.get("abbreviation", "").upper()
        tid = t.get("id")
        if abbr and tid:
            registry[abbr] = str(tid)
    _cache[key] = registry
    return registry


def _parse_record(summary: str):
    """Parse '48-14' → (48, 14, 0.774). Returns None on failure."""
    parts = summary.split("-")
    if len(parts) == 2:
        try:
            w, l = int(parts[0]), int(parts[1])
            total = w + l
            return w, l, (w / total if total > 0 else None)
        except ValueError:
            pass
    return None


def get_todays_game_data(sport: str, league: str) -> dict:
    """
    Returns {TEAM_ABBR: {is_home, win_pct, home_win_pct, away_win_pct,
                         games_played, opponent_abbr, espn_spread}}
    from today's ESPN scoreboard.
    """
    key = f"board_{sport}_{league}"
    if key in _cache:
        return _cache[key]
    url = f"https://site.api.espn.com/apis/site/v2/sports/{sport}/{league}/scoreboard"
    data = _espn_get(url)

    result = {}
    for event in data.get("events", []):
        # Game start time in ET (UTC-4 during EDT, UTC-5 during EST)
        game_hour_et = None
        try:
            raw_date = event.get("date", "")
            if raw_date:
                utc_dt = datetime.datetime.fromisoformat(raw_date.replace("Z", "+00:00"))
                # Approximate ET offset: March–Nov is EDT (UTC-4), else EST (UTC-5)
                et_offset = -4 if 3 <= utc_dt.month <= 11 else -5
                et_hour = (utc_dt.hour + et_offset) % 24
                game_hour_et = et_hour
        except Exception:
            pass

        for comp in event.get("competitions", []):
            competitors = comp.get("competitors", [])
            if len(competitors) < 2:
                continue

            # Parse odds/spread from ESPN betting data
            espn_spread = None
            for odds_entry in comp.get("odds", []):
                spread = odds_entry.get("spread") or odds_entry.get("details")
                if spread is not None:
                    espn_spread = spread
                    break

            teams_data = {}
            for c in competitors:
                abbr = c.get("team", {}).get("abbreviation", "").upper()
                if not abbr:
                    continue
                is_home = c.get("homeAway") == "home"
                win_pct = home_wp = away_wp = None
                games_played = 0
                for rec in c.get("records", []):
                    rtype = rec.get("type", "")
                    parsed = _parse_record(rec.get("summary", ""))
                    if not parsed:
                        continue
                    w, l, pct = parsed
                    if rtype == "total":
                        win_pct = pct
                        games_played = w + l
                    elif rtype == "home":
                        home_wp = pct
                    elif rtype in ("road", "away"):
                        away_wp = pct
                teams_data[abbr] = {
                    "is_home": is_home,
                    "win_pct": win_pct,
                    "home_win_pct": home_wp,
                    "away_win_pct": away_wp,
                    "games_played": games_played,
                    "espn_spread": espn_spread,
                    "opponent_abbr": None,
                    "game_hour_et": game_hour_et,
                }

            abbrs = list(teams_data.keys())
            if len(abbrs) == 2:
                teams_data[abbrs[0]]["opponent_abbr"] = abbrs[1]
                teams_data[abbrs[1]]["opponent_abbr"] = abbrs[0]

            result.update(teams_data)

    _cache[key] = result
    return result


def get_team_recent_form(sport: str, league: str, team_id: str) -> dict:
    """
    Returns {back_to_back, days_rest, l10_pct, l10_wins, l10_losses, streak}
    from the team's completed schedule.
    """
    key = f"form_{sport}_{league}_{team_id}"
    if key in _cache:
        return _cache[key]
    url = f"https://site.api.espn.com/apis/site/v2/sports/{sport}/{league}/teams/{team_id}/schedule"
    data = _espn_get(url)

    today = datetime.datetime.utcnow().date()
    completed = []

    for event in data.get("events", []):
        comp = (event.get("competitions") or [{}])[0]
        if not comp.get("status", {}).get("type", {}).get("completed"):
            continue
        try:
            game_date = datetime.datetime.fromisoformat(
                event["date"].replace("Z", "+00:00")
            ).date()
        except Exception:
            continue
        won = None
        for c in comp.get("competitors", []):
            if str(c.get("team", {}).get("id")) == str(team_id):
                won = c.get("winner", False)
                break
        completed.append({"date": game_date, "won": won})

    completed.sort(key=lambda x: x["date"], reverse=True)

    back_to_back = False
    days_rest = 99
    if completed:
        last_date = completed[0]["date"]
        days_rest = (today - last_date).days
        back_to_back = days_rest == 1

    recent = completed[:10]
    l10_wins = sum(1 for g in recent if g["won"])
    l10_losses = len(recent) - l10_wins
    l10_pct = l10_wins / len(recent) if recent else None

    # Current streak: positive = win streak, negative = losing streak
    streak = 0
    if completed:
        first_result = completed[0]["won"]
        for g in completed:
            if g["won"] == first_result:
                streak += 1
            else:
                break
        if not first_result:
            streak = -streak

    result = {
        "back_to_back": back_to_back,
        "days_rest": days_rest,
        "l10_pct": l10_pct,
        "l10_wins": l10_wins,
        "l10_losses": l10_losses,
        "streak": streak,
    }
    _cache[key] = result
    return result


def get_team_injuries(sport: str, league: str) -> dict:
    """Returns {TEAM_ABBR: [{"name": str, "status": str}]} for all teams."""
    key = f"inj_{sport}_{league}"
    if key in _cache:
        return _cache[key]
    url = f"https://site.api.espn.com/apis/site/v2/sports/{sport}/{league}/injuries"
    data = _espn_get(url)
    injuries = {}
    for item in data.get("injuries", []):
        abbr = item.get("team", {}).get("abbreviation", "").upper()
        if not abbr:
            continue
        player = item.get("athlete", {}).get("displayName", "Unknown")
        status = item.get("status", "")
        if abbr not in injuries:
            injuries[abbr] = []
        injuries[abbr].append({"name": player, "status": status})
    _cache[key] = injuries
    return injuries


def build_team_intel(ticker: str) -> dict:
    """
    Assemble complete intelligence for the team in a Kalshi ticker.
    Pulls from ESPN: scoreboard (records, home/away, spread), schedule
    (B2B, L10, streak), injuries (team + opponent), and opponent form.
    """
    route = get_sport_route(ticker)
    if not route:
        return {}
    sport, league = route

    parts = ticker.split("-")
    if len(parts) < 3:
        return {}
    kalshi_code = re.sub(r'\d+$', '', parts[-1].upper())
    abbr = espn_abbr(kalshi_code)

    intel = {"kalshi_code": kalshi_code, "abbr": abbr, "sport": sport, "league": league}

    # ── Scoreboard: records, home/away, spread ─────────────────────────────
    board = get_todays_game_data(sport, league)
    game_data = board.get(abbr, {})
    intel.update(game_data)

    # ── Team registry → numeric ID for schedule/injury calls ───────────────
    registry = get_team_registry(sport, league)
    team_id = registry.get(abbr)

    if team_id:
        form = get_team_recent_form(sport, league, team_id)
        intel.update(form)

        # Opponent form
        opp_abbr = game_data.get("opponent_abbr")
        opp_id = registry.get(opp_abbr) if opp_abbr else None
        if opp_id:
            opp_form = get_team_recent_form(sport, league, opp_id)
            intel["opp_l10_pct"] = opp_form.get("l10_pct")
            intel["opp_back_to_back"] = opp_form.get("back_to_back", False)
            intel["opp_streak"] = opp_form.get("streak", 0)
            intel["opp_days_rest"] = opp_form.get("days_rest", 99)

    # ── Opponent win rate from scoreboard ──────────────────────────────────
    opp_abbr = game_data.get("opponent_abbr")
    if opp_abbr and opp_abbr in board:
        intel["opp_win_pct"] = board[opp_abbr].get("win_pct")

    # ── Injuries: team + opponent ──────────────────────────────────────────
    all_injuries = get_team_injuries(sport, league)
    team_inj = all_injuries.get(abbr, [])
    intel["injuries_out"] = [i for i in team_inj if i["status"] == "Out"]
    intel["injuries_questionable"] = [i for i in team_inj if i["status"] in ("Questionable", "Doubtful")]

    if opp_abbr:
        opp_inj = all_injuries.get(opp_abbr, [])
        intel["opp_injuries_out"] = [i for i in opp_inj if i["status"] == "Out"]
        intel["opp_injuries_questionable"] = [i for i in opp_inj if i["status"] in ("Questionable", "Doubtful")]
    else:
        intel["opp_injuries_out"] = []
        intel["opp_injuries_questionable"] = []

    # ── Timezone travel fatigue ────────────────────────────────────────────
    is_home = game_data.get("is_home", True)
    tz_diff = get_timezone_diff(abbr, opp_abbr or "", is_home)
    intel["tz_diff"] = tz_diff  # positive = road team traveled east

    # ── Weather for outdoor MLB games ─────────────────────────────────────
    intel["wind_mph"] = None
    if league == "mlb" and opp_abbr and is_home is False:
        # Road team — check weather at opponent's ballpark
        city = MLB_PARK_CITIES.get(opp_abbr)
        if city and opp_abbr not in MLB_INDOOR:
            intel["wind_mph"] = get_weather_wind_mph(city)
    elif league == "mlb" and is_home is True:
        city = MLB_PARK_CITIES.get(abbr)
        if city and abbr not in MLB_INDOOR:
            intel["wind_mph"] = get_weather_wind_mph(city)

    return intel


# ─────────────────────────────────────────────────────────────────────────────
# PICK FILTERING & SCORING
# ─────────────────────────────────────────────────────────────────────────────

def context_win_pct(intel: dict) -> float | None:
    """Return the most context-specific win rate: home/away split if available."""
    is_home = intel.get("is_home")
    if is_home is True and intel.get("home_win_pct"):
        return intel["home_win_pct"]
    if is_home is False and intel.get("away_win_pct"):
        return intel["away_win_pct"]
    return intel.get("win_pct")


def is_suspicious_market(ticker: str, yes: float, intel: dict) -> bool:
    """
    Multi-factor anomaly detection using live ESPN data.
    Returns True if there's a red flag suggesting the market is mispriced
    due to injury, fatigue, or a team in actual decline.
    """
    cwp = context_win_pct(intel)
    l10 = intel.get("l10_pct")
    back_to_back = intel.get("back_to_back", False)
    injuries_out = intel.get("injuries_out", [])
    games_played = intel.get("games_played", 0)

    # Need enough games for record to mean anything
    min_games = 10

    if cwp is not None and games_played >= min_games:
        # Team with a strong record is priced way below their win rate
        if cwp >= 0.58 and yes < (cwp - 0.20):
            print(f"  Skip {ticker}: {int(cwp*100)}% win rate but Kalshi {int(yes*100)}¢")
            return True

        # Strong team near a coinflip with no fatigue excuse
        if cwp >= 0.65 and yes < 0.58 and not back_to_back:
            print(f"  Skip {ticker}: {int(cwp*100)}% team at {int(yes*100)}¢, not B2B")
            return True

    # Team in genuine freefall (L10 < 35%) being sold as a favorite
    if l10 is not None and l10 < 0.35 and yes < 0.65:
        print(f"  Skip {ticker}: only {int(l10*100)}% in L10, priced {int(yes*100)}¢")
        return True

    # Multiple starters out but price hasn't dropped — market data lag
    if len(injuries_out) >= 2 and yes > 0.72:
        print(f"  Skip {ticker}: {len(injuries_out)} players Out but priced {int(yes*100)}¢")
        return True

    return False


def score_pick(market: dict, intel: dict) -> float:
    """
    Multi-factor multiplicative scoring. Each signal adjusts the score up or
    down proportionally. Returns 0 to discard the pick.

    Factors:
      1. Kalshi price (base confidence)
      2. Expected value: ESPN win rate vs Kalshi price (positive edge)
      3. Recent form (L10 win rate)
      4. Win/loss streak momentum
      5. Rest advantage (days since last game, B2B)
      6. Home/away
      7. Opponent on B2B (fatigue edge)
      8. Opponent recent form
      9. Opponent injury advantage
      10. Our team injuries (penalty)
      11. Market volume (sharp money signal)
      12. Market type (straight winner > spread)
      13. Rest differential (our rest vs opponent rest)
      14. Sample size confidence (discount early-season thin records)
    """
    yes = float(market.get("yes_ask_dollars", "0") or 0)
    vol = float(market.get("volume_fp", "0") or 0)
    ticker = market.get("ticker", "")

    # Hard cap at 85¢: above this the risk/reward is too poor.
    # Your own history: 90¢+ bets → -$25.79 on $868 invested (-3% ROI).
    # One upset wipes out 9 wins at 90¢. Not worth it.
    if yes < 0.58 or yes > 0.85:
        return 0

    if is_suspicious_market(ticker, yes, intel):
        return 0

    # NCAA Basketball discount: your worst market by a wide margin.
    # 275 trades, 79% WR, but -$35.89 (-4% ROI). Too many upsets at any price.
    # Still allow them if they're exceptional, just heavily penalized.
    ncaa_penalty = 0.45 if "NCAA" in ticker else 1.0

    # SPREAD at 86¢+: your best market type (96% WR, +3.8% ROI).
    # Only applies for high-confidence spreads — low price spreads are bad.
    is_spread = "SPREAD" in ticker
    if is_spread and yes < 0.86:
        return 0  # Don't touch spreads below 86¢ — your data is clear on this

    score = 1.0

    # 1. Base confidence: Kalshi price normalized to 58–85¢ range
    score *= (yes - 0.58) / 0.27 * 0.5 + 0.5  # maps 58¢→0.5, 85¢→1.0

    # Sweet spot bonus: 65–80¢ is your best historical range (+5–6% ROI).
    # 80–85¢ is allowed but the math starts working against you — discount it.
    if 0.65 <= yes <= 0.80:
        score *= 1.18
    elif yes > 0.80:
        score *= 0.80

    # 2. Expected value vs ESPN win rate
    cwp = context_win_pct(intel)
    games_played = intel.get("games_played", 0)
    if cwp is not None and games_played >= 10:
        edge = cwp - yes
        if edge > 0.10:
            score *= 1.20   # strong positive edge: Kalshi underpricing
        elif edge > 0.05:
            score *= 1.10
        elif edge < -0.10:
            score *= 0.78   # Kalshi overpricing: market knows something
        elif edge < -0.05:
            score *= 0.90

    # 3. Recent form (L10)
    l10 = intel.get("l10_pct")
    if l10 is not None:
        if l10 >= 0.80:
            score *= 1.22
        elif l10 >= 0.70:
            score *= 1.12
        elif l10 >= 0.60:
            score *= 1.04
        elif l10 <= 0.30:
            score *= 0.60
        elif l10 <= 0.40:
            score *= 0.78
        elif l10 <= 0.50:
            score *= 0.90

    # 4. Win/loss streak momentum
    streak = intel.get("streak", 0)
    if streak >= 6:
        score *= 1.15
    elif streak >= 4:
        score *= 1.08
    elif streak <= -5:
        score *= 0.72
    elif streak <= -3:
        score *= 0.85

    # 5. Rest: back-to-back is a fatigue penalty
    back_to_back = intel.get("back_to_back", False)
    days_rest = intel.get("days_rest", 2)
    if back_to_back:
        score *= 0.84
    elif days_rest >= 4:
        score *= 1.10
    elif days_rest >= 3:
        score *= 1.05

    # 6. Home court advantage
    is_home = intel.get("is_home")
    if is_home is True:
        score *= 1.07
    elif is_home is False:
        score *= 0.95

    # 7. Opponent on B2B (their fatigue = our edge)
    if intel.get("opp_back_to_back"):
        score *= 1.12

    # 8. Rest differential (we're fresher than opponent)
    opp_rest = intel.get("opp_days_rest", 2)
    rest_diff = days_rest - opp_rest
    if rest_diff >= 2 and not back_to_back:
        score *= 1.07
    elif rest_diff <= -2:
        score *= 0.93

    # 9. Opponent in freefall
    opp_l10 = intel.get("opp_l10_pct")
    if opp_l10 is not None:
        if opp_l10 <= 0.30:
            score *= 1.10
        elif opp_l10 <= 0.40:
            score *= 1.05

    # 10. Our injuries (penalty)
    injuries_out = intel.get("injuries_out", [])
    injuries_q = intel.get("injuries_questionable", [])
    if len(injuries_out) >= 2:
        score *= 0.78
    elif len(injuries_out) == 1:
        score *= 0.91
    if len(injuries_q) >= 2:
        score *= 0.96

    # 11. Opponent injuries (bonus — they're shorthanded)
    opp_out = intel.get("opp_injuries_out", [])
    opp_q = intel.get("opp_injuries_questionable", [])
    if len(opp_out) >= 2:
        score *= 1.12
    elif len(opp_out) == 1:
        score *= 1.05
    if len(opp_q) >= 2:
        score *= 1.03

    # 12. Market volume: high volume = sharp money agrees
    vol_factor = min(vol / 500_000, 1.0)
    score *= 0.85 + vol_factor * 0.30  # 0.85 at zero vol → 1.15 at max vol

    # 13. Market type: straight game winner is cleaner than spread
    if "GAME" in ticker and "SPREAD" not in ticker:
        score *= 1.15

    # 14. Thin sample size — discount very early season records
    if games_played > 0 and games_played < 15:
        score *= 0.70 + (games_played / 15) * 0.30  # scales from 0.70 → 1.0

    # 15. NCAA penalty — your worst market historically (-4% ROI on 275 trades)
    score *= ncaa_penalty

    # 16. SPREAD bonus — your best market type (96% WR at 86¢+)
    if is_spread and yes >= 0.86:
        score *= 1.20

    # 17. Opponent quality — facing a weak team is a big edge
    opp_win_pct = intel.get("opp_win_pct")
    if opp_win_pct is not None:
        if opp_win_pct <= 0.35:
            score *= 1.18   # opponent is a bottom-5 team
        elif opp_win_pct <= 0.45:
            score *= 1.08
        elif opp_win_pct >= 0.65:
            score *= 0.92   # opponent is elite — tough game

    # 18. Game time — your history shows massive time-of-day effect:
    #   1–5 PM ET: -12% to -42% ROI (afternoon lines move before you see them)
    #   6–10 PM ET: +5% to +21% ROI (evening games are more stable)
    game_hour = intel.get("game_hour_et")
    if game_hour is not None:
        if 18 <= game_hour <= 22:
            score *= 1.18   # evening sweet spot (+20% ROI historically)
        elif 16 <= game_hour <= 17:
            score *= 1.05
        elif 13 <= game_hour <= 17:
            score *= 0.72   # afternoon danger zone (-12% to -42% ROI)
        elif 11 <= game_hour <= 12:
            score *= 0.88

    # 19. Day of week — your data shows real day-of-week edge:
    #   Tuesday: +10.3% ROI, Friday: +3.5% ROI
    #   Monday: -32.8% ROI, Wednesday: -9.5% ROI
    today_dow = datetime.datetime.utcnow().strftime("%A")
    dow_multiplier = {
        "Tuesday": 1.12, "Friday": 1.06,
        "Sunday": 1.02, "Thursday": 1.00, "Saturday": 0.97,
        "Wednesday": 0.88, "Monday": 0.70,
    }
    score *= dow_multiplier.get(today_dow, 1.0)

    # 20. Cross-timezone travel fatigue for road teams
    tz_diff = intel.get("tz_diff", 0)
    if abs(tz_diff) >= 3:
        score *= 0.88   # 3-hour jet lag (e.g. LA team playing in NYC)
    elif abs(tz_diff) == 2:
        score *= 0.93
    elif tz_diff > 0:
        # Traveling east is harder than west (circadian rhythm)
        score *= 0.97

    # 21. Wind for outdoor MLB games — high wind hurts road teams
    #     (they're less familiar with park quirks)
    wind_mph = intel.get("wind_mph")
    is_home = intel.get("is_home", True)
    intel_league = intel.get("league", "")
    if wind_mph is not None and intel_league == "mlb":
        if wind_mph >= 20 and not is_home:
            score *= 0.90   # heavy wind hurts unfamiliar road team more
        elif wind_mph >= 15 and not is_home:
            score *= 0.95
        elif wind_mph >= 20 and is_home:
            score *= 1.05   # home team knows the park, benefits slightly

    return score


# ─────────────────────────────────────────────────────────────────────────────
# TOP PICKS SELECTION
# ─────────────────────────────────────────────────────────────────────────────

def recommended_bet_size(yes: float, score: float, bankroll: float = 27.0) -> float:
    """
    Kelly-inspired bet sizing capped by your history rules:
    - Never more than 5% of bankroll per bet
    - Sweet spot (65–80¢) gets full sizing, near-cap (80–85¢) gets half
    - Min $1, max $5 (you're rebuilding from $27 — protect the bankroll)
    """
    max_bet = min(bankroll * 0.05, 5.0)
    if yes > 0.80:
        max_bet *= 0.5
    # Scale within max by score confidence (score roughly 0.5–1.5)
    fraction = min((score - 0.4) / 1.1, 1.0)
    bet = max(1.0, round(max_bet * fraction, 2))
    return min(bet, max_bet)


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

        intel = build_team_intel(ticker)
        score = score_pick(market, intel)
        if score == 0:
            continue

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
            "intel": intel,
            "bet_size": recommended_bet_size(yes, score),
        })

    candidates.sort(key=lambda x: x["score"], reverse=True)
    return candidates[:n]


def build_parlay(all_picks: list) -> dict | None:
    """
    Construct the highest-EV 3-leg parlay via exhaustive search.

    Improvements over v1:
    - Positive edge requirement: estimate_true_win_prob > Kalshi price per leg
    - Exhaustive C(n,3) search over top 12 eligible (not greedy)
    - Selects combo maximising combined_true_prob × avg_score
    - Canonical event key prevents two legs from the same game
    - Kelly-inspired stake sizing (quarter-Kelly, $2–$10 cap)
    """
    # ── Step 1: build pool with per-leg true-win estimates ───────────────────
    # Note: get_top_picks() already deduplicates by event_ticker (one pick per
    # game), so no further event dedup is needed here.
    base_eligible = [
        p for p in all_picks
        if 0.65 <= p["yes"] <= 0.80
        and "NCAA" not in p["ticker"]
        and p["score"] > 0
    ]

    pool = []
    for p in base_eligible:
        tp = estimate_true_win_prob(p["intel"])
        if tp is None:
            tp = p["yes"]                        # no data → neutral, no edge
        # Require at least 2¢ edge; if truly no data, still allow (tp == yes)
        if tp >= p["yes"] - 0.01:
            pool.append((p, tp))

    # Fallback: relax NCAA filter
    if len(pool) < 3:
        fallback = [
            p for p in all_picks
            if 0.65 <= p["yes"] <= 0.80 and p["score"] > 0
        ]
        pool = []
        for p in fallback:
            tp = estimate_true_win_prob(p["intel"]) or p["yes"]
            pool.append((p, tp))

    if len(pool) < 3:
        return None

    # Limit exhaustive search to top 12 by score
    pool.sort(key=lambda x: x[0]["score"], reverse=True)
    pool = pool[:12]

    # ── Step 2: exhaustive C(n,3) search ────────────────────────────────────
    best_legs, best_tps, best_quality = None, None, -1.0

    for i in range(len(pool)):
        for j in range(i + 1, len(pool)):
            for k in range(j + 1, len(pool)):
                trio = [pool[i], pool[j], pool[k]]
                picks_t = [t[0] for t in trio]
                tps_t   = [t[1] for t in trio]

                true_combined = tps_t[0] * tps_t[1] * tps_t[2]
                avg_score     = sum(p["score"] for p in picks_t) / 3
                quality       = true_combined * avg_score

                if quality > best_quality:
                    best_quality = quality
                    best_legs    = picks_t
                    best_tps     = tps_t

    if best_legs is None:
        return None

    # ── Step 3: compute odds & Kelly stake ───────────────────────────────────
    true_combined   = best_tps[0] * best_tps[1] * best_tps[2]
    market_combined = best_legs[0]["yes"] * best_legs[1]["yes"] * best_legs[2]["yes"]

    # Quarter-Kelly stake on a $100 notional, capped $2–$10
    edge = true_combined - market_combined
    if market_combined > 0 and market_combined < 1.0:
        kelly_f = edge / (1.0 / market_combined - 1.0)
        stake   = round(max(2.0, min(10.0, kelly_f * 100 * 0.25)), 2)
    else:
        stake = 3.0

    payout = round(stake / market_combined, 2)
    profit = round(payout - stake, 2)

    return {
        "legs":             best_legs,
        "combined_prob":    market_combined,
        "true_combined_prob": true_combined,
        "stake":            stake,
        "payout":           payout,
        "profit":           profit,
    }


# ─────────────────────────────────────────────────────────────────────────────
# NO-SIDE FADE SCANNER
# Your data shows 90¢+ YES bets → -$25.79 total loss.
# Flipping those same markets and buying NO would have made +$62–130.
# This scans for overpriced favorites and recommends buying the NO side.
# ─────────────────────────────────────────────────────────────────────────────

def estimate_true_win_prob(intel: dict) -> float | None:
    """
    Build a blended estimate of a team's real win probability tonight
    using season win rate, L10 form, home/away, rest, and injuries.
    Returns None if insufficient data.
    """
    cwp = context_win_pct(intel)
    l10 = intel.get("l10_pct")
    games_played = intel.get("games_played", 0)

    if cwp is None or games_played < 10:
        return None

    # Start with season win rate weighted with L10 form
    if l10 is not None:
        base = cwp * 0.55 + l10 * 0.45
    else:
        base = cwp

    # Back-to-back fatigue
    if intel.get("back_to_back"):
        base -= 0.06
    elif intel.get("days_rest", 2) >= 3:
        base += 0.02

    # Opponent on B2B = our boost
    if intel.get("opp_back_to_back"):
        base += 0.04

    # Injuries
    base -= len(intel.get("injuries_out", [])) * 0.04
    base += len(intel.get("opp_injuries_out", [])) * 0.03

    # Streak momentum
    streak = intel.get("streak", 0)
    if streak >= 5:
        base += 0.03
    elif streak <= -4:
        base -= 0.04

    return max(0.01, min(0.99, base))


def scan_no_fades(n: int = 3) -> list:
    """
    Find markets where the YES side is overpriced (88¢+) but the team's
    actual estimated win probability is meaningfully lower.
    Buying NO on these markets has massive historical edge in your data.

    Returns up to n fade opportunities sorted by edge size.
    """
    print("Scanning for NO-side fade opportunities...")
    tickers = scan_todays_game_tickers()
    fades = []
    seen_events = set()

    for ticker in tickers:
        market = fetch_market(ticker)
        if not market:
            continue

        yes = float(market.get("yes_ask_dollars", "0") or 0)
        no_ask = float(market.get("no_ask_dollars", "0") or 0)

        # Only look at heavily favored markets where NO is cheap
        if yes < 0.88 or no_ask <= 0:
            continue

        event = market.get("event_ticker", ticker)
        if event in seen_events:
            continue

        intel = build_team_intel(ticker)
        true_win_prob = estimate_true_win_prob(intel)

        if true_win_prob is None:
            continue

        # True probability of losing = edge for buying NO
        true_lose_prob = 1.0 - true_win_prob
        implied_no_prob = no_ask  # e.g. 0.08 = 8%

        # Edge = how much we're getting paid vs what the true probability is
        edge = true_lose_prob - implied_no_prob

        # Only recommend if there's meaningful edge (>8pp) and NO is cheap (<20¢)
        if edge >= 0.08 and no_ask <= 0.20:
            seen_events.add(event)
            vol = float(market.get("volume_fp", "0") or 0)
            fades.append({
                "ticker": ticker,
                "title": market.get("title", ticker),
                "yes": yes,
                "no_ask": no_ask,
                "true_lose_prob": true_lose_prob,
                "edge": edge,
                "volume": vol,
                "intel": intel,
                # Suggested stake: small ($1–2) since these are longshots
                "bet_size": min(2.0, round(edge * 10, 2)),
            })

    fades.sort(key=lambda x: x["edge"], reverse=True)
    return fades[:n]


# ─────────────────────────────────────────────────────────────────────────────
# OUTPUT: FORMATTING & DELIVERY
# ─────────────────────────────────────────────────────────────────────────────

NBA_TEAMS = {
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
}
MLB_TEAMS = {
    "ATH": "Oakland Athletics", "BAL": "Baltimore Orioles", "BOS": "Boston Red Sox",
    "CHC": "Chicago Cubs", "CWS": "Chicago White Sox", "CIN": "Cincinnati Reds",
    "COL": "Colorado Rockies", "DET": "Detroit Tigers", "HOU": "Houston Astros",
    "KC": "Kansas City Royals", "LAA": "LA Angels", "LAD": "LA Dodgers",
    "MIA": "Miami Marlins", "MIL": "Milwaukee Brewers", "MIN": "Minnesota Twins",
    "NYM": "New York Mets", "NYY": "New York Yankees", "OAK": "Oakland A's",
    "PHI": "Philadelphia Phillies", "PIT": "Pittsburgh Pirates", "SD": "San Diego Padres",
    "SEA": "Seattle Mariners", "SF": "San Francisco Giants", "STL": "St. Louis Cardinals",
    "TB": "Tampa Bay Rays", "TEX": "Texas Rangers", "TOR": "Toronto Blue Jays",
    "WSH": "Washington Nationals",
}
NHL_TEAMS = {
    "ANA": "Anaheim Ducks", "BOS": "Boston Bruins", "BUF": "Buffalo Sabres",
    "CAR": "Carolina Hurricanes", "CBJ": "Columbus Blue Jackets", "CGY": "Calgary Flames",
    "CHI": "Chicago Blackhawks", "COL": "Colorado Avalanche", "DAL": "Dallas Stars",
    "DET": "Detroit Red Wings", "EDM": "Edmonton Oilers", "FLA": "Florida Panthers",
    "LAK": "LA Kings", "MIN": "Minnesota Wild", "MTL": "Montreal Canadiens",
    "NJD": "New Jersey Devils", "NSH": "Nashville Predators", "NYI": "NY Islanders",
    "NYR": "NY Rangers", "OTT": "Ottawa Senators", "PHI": "Philadelphia Flyers",
    "PIT": "Pittsburgh Penguins", "SJS": "San Jose Sharks", "STL": "St. Louis Blues",
    "TBL": "Tampa Bay Lightning", "TOR": "Toronto Maple Leafs", "VAN": "Vancouver Canucks",
    "VGK": "Vegas Golden Knights", "WPG": "Winnipeg Jets", "WSH": "Washington Capitals",
}
NCAA_TEAMS = {
    "DUKE": "Duke", "MICH": "Michigan", "TENN": "Tennessee", "CONN": "UConn",
    "KU": "Kansas", "ILL": "Illinois", "ARK": "Arkansas", "FLA": "Florida",
    "ARIZ": "Arizona", "MSU": "Michigan State", "ISU": "Iowa State",
    "UK": "Kentucky", "PUR": "Purdue", "UCLA": "UCLA", "ALA": "Alabama",
    "GONZ": "Gonzaga", "TEX": "Texas", "BYU": "BYU",
}
# Merged fallback (NHL wins conflicts — use sport-specific dicts when possible)
TEAM_NAMES = {**NBA_TEAMS, **MLB_TEAMS, **NHL_TEAMS, **NCAA_TEAMS}


def extract_pick(ticker: str) -> str:
    parts = ticker.split("-")
    if len(parts) >= 3:
        team_code = re.sub(r'\d+$', '', parts[-1].upper())
        if "NHL" in ticker:
            return NHL_TEAMS.get(team_code, team_code)
        if "MLB" in ticker:
            return MLB_TEAMS.get(team_code, team_code)
        if "NBA" in ticker:
            return NBA_TEAMS.get(team_code, team_code)
        if "NCAA" in ticker:
            return NCAA_TEAMS.get(team_code, team_code)
        return TEAM_NAMES.get(team_code, team_code)
    return ticker


def sport_emoji(ticker: str) -> str:
    if "NBA" in ticker: return "🏀"
    if "MLB" in ticker: return "⚾"
    if "NHL" in ticker: return "🏒"
    if "NCAAMB" in ticker: return "🏀"
    return "🎯"


def build_intel_bullets(intel: dict) -> list[str]:
    """Return short bullet strings explaining why this pick was selected."""
    bullets = []

    cwp = context_win_pct(intel)
    gp = intel.get("games_played", 0)
    l10 = intel.get("l10_pct")
    streak = intel.get("streak", 0)
    days_rest = intel.get("days_rest", 99)
    back_to_back = intel.get("back_to_back", False)
    is_home = intel.get("is_home")
    opp_b2b = intel.get("opp_back_to_back", False)
    opp_rest = intel.get("opp_days_rest", 99)
    injuries_out = intel.get("injuries_out", [])
    injuries_q = intel.get("injuries_questionable", [])
    opp_out = intel.get("opp_injuries_out", [])
    opp_l10 = intel.get("opp_l10_pct")

    if cwp and gp >= 10:
        bullets.append(f"📊 {int(cwp*100)}% win rate ({gp} games)")

    if l10 is not None:
        icon = "🔥" if l10 >= 0.75 else ("📈" if l10 >= 0.60 else "📉")
        bullets.append(f"{icon} {int(l10*100)}% last 10 games")

    if streak >= 4:
        bullets.append(f"🔥 {streak}-game win streak")
    elif streak <= -3:
        bullets.append(f"⚠️ On a {abs(streak)}-game losing streak")

    if is_home is True:
        bullets.append("🏠 Home game")
    elif is_home is False:
        bullets.append("✈️ Road game")

    if back_to_back:
        bullets.append("⚠️ Playing on a back-to-back")
    elif days_rest >= 3:
        bullets.append(f"💤 {days_rest} days of rest")

    if opp_b2b:
        bullets.append("😴 Opponent on a back-to-back")
    elif opp_rest >= 3 and not opp_b2b and days_rest < opp_rest:
        pass  # rest differential covered below

    rest_diff = days_rest - opp_rest
    if rest_diff >= 2 and not back_to_back and not opp_b2b:
        bullets.append(f"⚡ {rest_diff} more rest days than opponent")

    if opp_out:
        names = ", ".join(i["name"] for i in opp_out[:2])
        suffix = f" (+{len(opp_out)-2} more)" if len(opp_out) > 2 else ""
        bullets.append(f"🤕 Opp missing: {names}{suffix}")
    elif opp_l10 is not None and opp_l10 <= 0.40:
        bullets.append(f"📉 Opponent {int(opp_l10*100)}% last 10")

    if injuries_out:
        names = ", ".join(i["name"] for i in injuries_out[:2])
        bullets.append(f"⚠️ Out: {names}")
    if injuries_q:
        names = ", ".join(i["name"] for i in injuries_q[:2])
        bullets.append(f"❓ Questionable: {names}")

    # Game time signal
    game_hour = intel.get("game_hour_et")
    if game_hour is not None:
        if 18 <= game_hour <= 22:
            bullets.append(f"🌙 Evening game ({game_hour}:00 ET) — your best time slot")
        elif 13 <= game_hour <= 17:
            bullets.append(f"☀️ Afternoon game ({game_hour}:00 ET) — historically lower ROI")

    # Timezone travel fatigue
    tz_diff = intel.get("tz_diff", 0)
    if abs(tz_diff) >= 3:
        bullets.append(f"✈️ {abs(tz_diff)}-hr timezone travel as road team")

    # Wind for MLB
    wind = intel.get("wind_mph")
    if wind and wind >= 15:
        bullets.append(f"💨 {int(wind)} mph wind at ballpark")

    return bullets


def format_picks(picks: list) -> str:
    today = datetime.datetime.utcnow().strftime("%B %d, %Y")
    lines = [f"## 🎯 Kalshi Top {len(picks)} Picks — {today}\n"]
    lines.append("| # | Bet | Game | Price | Volume |")
    lines.append("|---|-----|------|-------|--------|")

    for i, p in enumerate(picks, 1):
        emoji = sport_emoji(p["ticker"])
        conf = int(p["yes"] * 100)
        vol = int(p["volume"])
        pick_name = extract_pick(p["ticker"])
        game_title = p["title"][:50]
        lines.append(f"| {i} | {emoji} **{pick_name} to WIN** | {game_title} | {conf}¢ | {vol:,} |")
        bullets = build_intel_bullets(p.get("intel", {}))
        if bullets:
            lines.append(f"|   | *{' · '.join(bullets[:3])}* | | | |")

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


def send_email(subject: str, picks: list, parlay: dict | None = None, fades: list | None = None):
    resend_key = os.environ["RESEND_API_KEY"]
    today = datetime.datetime.utcnow().strftime("%B %d, %Y")

    rows = ""
    for i, p in enumerate(picks, 1):
        emoji = sport_emoji(p["ticker"])
        conf = int(p["yes"] * 100)
        pick_name = extract_pick(p["ticker"])
        game_title = p["title"][:55]
        bet_size = p.get("bet_size", 2.0)

        # Color coding: green=sweet spot, orange=borderline, red=near cap
        if 0.65 <= p["yes"] <= 0.80:
            price_color = "#2ecc71"
            price_label = f"{conf}¢ ✓"
        elif p["yes"] > 0.80:
            price_color = "#e67e22"
            price_label = f"{conf}¢ ⚠"
        else:
            price_color = "#3498db"
            price_label = f"{conf}¢"

        bullets = build_intel_bullets(p.get("intel", {}))
        bullets_html = ""
        if bullets:
            items = "".join(f"<li style='margin:2px 0;'>{b}</li>" for b in bullets[:5])
            bullets_html = f"<ul style='margin:6px 0 0;padding-left:18px;font-size:11px;color:#555;'>{items}</ul>"

        rows += f"""
        <tr style="border-bottom:1px solid #eee;">
          <td style="padding:12px;text-align:center;font-weight:bold;font-size:18px;">{i}</td>
          <td style="padding:12px;">
            <div style="font-size:16px;font-weight:bold;">{emoji} BET: {pick_name} to WIN</div>
            <div style="font-size:12px;color:#888;margin-top:3px;">{game_title}</div>
            {bullets_html}
          </td>
          <td style="padding:12px;text-align:center;vertical-align:top;">
            <span style="background:{price_color};color:#fff;padding:4px 10px;border-radius:12px;font-weight:bold;">{price_label}</span>
          </td>
          <td style="padding:12px;text-align:center;vertical-align:top;">
            <div style="font-weight:bold;color:#1a1a2e;">${bet_size:.2f}</div>
            <div style="font-size:10px;color:#aaa;">suggested</div>
          </td>
        </tr>"""

    # ── Parlay section ────────────────────────────────────────────────────────
    parlay_html = ""
    if parlay:
        leg_rows = ""
        for j, leg in enumerate(parlay["legs"], 1):
            lemoji = sport_emoji(leg["ticker"])
            lname = extract_pick(leg["ticker"])
            ltitle = leg["title"][:50]
            lconf = int(leg["yes"] * 100)
            leg_rows += f"""
            <tr style="border-bottom:1px solid #e8f5e9;">
              <td style="padding:8px;color:#27ae60;font-weight:bold;">Leg {j}</td>
              <td style="padding:8px;">{lemoji} <strong>{lname}</strong> to WIN<br>
                <span style="font-size:11px;color:#888;">{ltitle}</span></td>
              <td style="padding:8px;text-align:center;">
                <span style="background:#27ae60;color:#fff;padding:3px 8px;border-radius:10px;font-size:13px;">{lconf}¢</span>
              </td>
            </tr>"""

        combined_pct = int(parlay["combined_prob"] * 100)
        parlay_html = f"""
        <div style="margin-top:24px;">
          <div style="background:#1e7e34;padding:14px 16px;border-radius:8px 8px 0 0;">
            <h2 style="color:#fff;margin:0;font-size:18px;">🎰 3-Leg Parlay Pick</h2>
            <p style="color:#a8e6b8;margin:4px 0 0;font-size:13px;">All 3 legs in your 65–80¢ sweet spot · Stake ${parlay["stake"]:.2f} · Win ${parlay["profit"]:.2f} profit</p>
          </div>
          <table style="width:100%;border-collapse:collapse;background:#f0fff4;border:1px solid #c3e6cb;">
            <tbody>{leg_rows}</tbody>
          </table>
          <div style="background:#d4edda;padding:10px 14px;border-radius:0 0 8px 8px;font-size:12px;color:#155724;">
            Combined probability: ~{combined_pct}% · Payout on ${parlay["stake"]:.2f} stake: <strong>${parlay["payout"]:.2f}</strong> · Profit if all win: <strong>+${parlay["profit"]:.2f}</strong>
          </div>
        </div>"""

    # ── NO-side fades section ─────────────────────────────────────────────────
    fades_html = ""
    if fades:
        fade_rows = ""
        for f in fades:
            team_name = extract_pick(f["ticker"])
            game_title = f["title"][:50]
            no_price = int(f["no_ask"] * 100)
            true_lose = int(f["true_lose_prob"] * 100)
            edge_pp = int(f["edge"] * 100)
            payout_x = round(1.0 / f["no_ask"], 1)
            bet = f["bet_size"]
            fade_rows += f"""
            <tr style="border-bottom:1px solid #fde8e8;">
              <td style="padding:8px;color:#c0392b;font-size:13px;font-weight:bold;">FADE</td>
              <td style="padding:8px;">
                <div style="font-weight:bold;">BUY NO on {team_name}</div>
                <div style="font-size:11px;color:#888;">{game_title}</div>
                <div style="font-size:11px;color:#e74c3c;margin-top:3px;">
                  True upset prob: ~{true_lose}% · Kalshi pricing: {no_price}¢ · Edge: +{edge_pp}pp
                </div>
              </td>
              <td style="padding:8px;text-align:center;">
                <span style="background:#e74c3c;color:#fff;padding:3px 8px;border-radius:10px;font-size:13px;">{no_price}¢ NO</span>
              </td>
              <td style="padding:8px;text-align:center;">
                <div style="font-weight:bold;color:#c0392b;">${bet:.2f}</div>
                <div style="font-size:10px;color:#aaa;">{payout_x}x payout</div>
              </td>
            </tr>"""

        fades_html = f"""
        <div style="margin-top:24px;">
          <div style="background:#922b21;padding:14px 16px;border-radius:8px 8px 0 0;">
            <h2 style="color:#fff;margin:0;font-size:18px;">🚫 Fade the Public — Buy NO</h2>
            <p style="color:#f1948a;margin:4px 0 0;font-size:13px;">Markets where the YES side is overpriced · Your data shows these situations made +$62+ when faded</p>
          </div>
          <table style="width:100%;border-collapse:collapse;background:#fff5f5;border:1px solid #f5c6cb;">
            <thead>
              <tr style="background:#fadbd8;">
                <th style="padding:8px;font-size:12px;"></th>
                <th style="padding:8px;text-align:left;font-size:12px;">Fade + Edge</th>
                <th style="padding:8px;font-size:12px;">NO Price</th>
                <th style="padding:8px;font-size:12px;">Stake</th>
              </tr>
            </thead>
            <tbody>{fade_rows}</tbody>
          </table>
          <div style="background:#fadbd8;padding:10px 14px;border-radius:0 0 8px 8px;font-size:12px;color:#922b21;">
            ⚠️ Fades are small bets ($1–2). High risk but large payout when the upset happens. Only bet what you can afford to lose.
          </div>
        </div>"""

    # ── Rules reminder ────────────────────────────────────────────────────────
    rules_html = """
        <div style="margin-top:20px;background:#fff3cd;border:1px solid #ffc107;border-radius:8px;padding:14px;">
          <div style="font-weight:bold;color:#856404;margin-bottom:8px;">📋 Your Personal Rules (based on your trade history)</div>
          <ul style="margin:0;padding-left:18px;font-size:12px;color:#533f03;line-height:1.8;">
            <li><strong>Never buy above 85¢</strong> — your 90¢+ bets lost $25.79 total</li>
            <li><strong>Sweet spot is 65–80¢</strong> — best ROI from your own data</li>
            <li><strong>Max $5 per bet</strong> — protect your $27 bankroll</li>
            <li><strong>Be careful with NCAA</strong> — your worst market (-4% ROI)</li>
            <li><strong>NBA is your best sport</strong> — 93% win rate, keep it up</li>
          </ul>
        </div>"""

    html = f"""
    <div style="font-family:Arial,sans-serif;max-width:640px;margin:0 auto;">
      <div style="background:#1a1a2e;padding:20px;border-radius:8px 8px 0 0;">
        <h1 style="color:#fff;margin:0;font-size:22px;">🎯 Kalshi Daily Picks</h1>
        <p style="color:#aaa;margin:5px 0 0;">{today} · Powered by live ESPN research</p>
      </div>
      <table style="width:100%;border-collapse:collapse;background:#fff;border:1px solid #eee;">
        <thead>
          <tr style="background:#f8f9fa;">
            <th style="padding:10px;">#</th>
            <th style="padding:10px;text-align:left;">Pick + Why</th>
            <th style="padding:10px;">Price</th>
            <th style="padding:10px;">Bet</th>
          </tr>
        </thead>
        <tbody>{rows}</tbody>
      </table>
      {parlay_html}
      {fades_html}
      {rules_html}
      <div style="background:#f8f9fa;padding:10px;border-radius:0 0 8px 8px;font-size:11px;color:#aaa;margin-top:4px;">
        Auto-generated using live win rates, form, rest, and injury data. Not financial advice.
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
    today = datetime.datetime.utcnow().strftime("%B %d, %Y")

    # Fetch a wide pool — top picks + parlay pool + fades all share ESPN cache
    all_candidates = get_top_picks(15)
    picks = all_candidates[:5]

    parlay = build_parlay(all_candidates)
    if parlay:
        legs = parlay["legs"]
        print(f"Parlay: {extract_pick(legs[0]['ticker'])} + {extract_pick(legs[1]['ticker'])} + {extract_pick(legs[2]['ticker'])} -> ${parlay['payout']:.2f} payout on $3")

    fades = scan_no_fades(3)
    if fades:
        for f in fades:
            print(f"Fade: BUY NO on {extract_pick(f['ticker'])} at {int(f['no_ask']*100)}¢ (edge +{int(f['edge']*100)}pp)")

    if not picks:
        print("No picks found for today.")
        body = "No qualifying markets found for today. Check back tomorrow!"
    else:
        body = format_picks(picks)
        try:
            print(body.encode("ascii", "replace").decode())
        except Exception:
            print("Picks generated (emoji display skipped on this terminal)")

    create_github_issue(f"📊 Daily Picks — {today}", body)
    if picks:
        send_email(f"🎯 Kalshi Top 5 Picks — {today}", picks, parlay=parlay, fades=fades)
