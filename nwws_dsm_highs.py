"""
nwws_cli_highs.py — NWWS-OI CLI (Daily Climate Report) HIGH temp bot. PAPER ONLY.

WHY CLI AND NOT DSM
-------------------
DSMs do not reach this feed. Confirmed over weeks of listening: CLIs arrive
constantly for every watched city, DSMs never do. The previous version fired
only on DSM, which made its fire path unreachable in practice. Everything here
is built on the CLI stream, which is the product that actually shows up.

The provisional/confirmed structure below is the same one designed for DSMs.
That design was correct — it was aimed at a product that never arrives.

HOW THE HIGH GETS LOCKED
------------------------
A daily CLI issued mid-afternoon is a RUNNING max ("VALID TODAY AS OF 0400 PM
LOCAL TIME"), not a settled one. Firing on a running max is the low-side
timing failure in reverse: read the number before the day is done and you
trade a high that hasn't finished forming.

The CLI hands us the tool to solve this. The observed value comes with its
observation time in the LST column:

    MAXIMUM        103R   129 PM 100    1910  90     13       97
                   ^^^^   ^^^^^^
                   value  when it happened (LST)

    issued  22:36Z
    obs     129 PM LST = 13:29 = 20:29Z   (Denver LST = UTC-7)
    staleness = 127 minutes

The peak happened over two hours before the report was issued. That is the
product telling us the high is behind us — not a guess from the wall clock.

    STALENESS  = issue_utc - observation_utc
    CONFIRMED  = staleness >= CLI_CONFIRM_STALE_MIN  (floor, default 90)
                 AND issue >= the city's post-peak cutoff
    FIRE       = CONFIRMED and (corroborated by a second CLI reporting the
                 same max, OR staleness >= CLI_SOLO_STALE_MIN, default 150)

Corroboration matters because a single parse has nothing checking it. Two
consecutive independent CLIs agreeing on the same max IS a cross-check. If the
max rises between reports, the peak is still forming — confirmation resets.

LST IS TRUE STANDARD TIME
-------------------------
Verified against real products on 2026-07-26: the CLI reported 129 PM and
Wethr showed 2:29 PM for the same observation. Exactly one hour apart, which
is the DST offset. So the LST column is genuine standard time and the
'lst_offset' values below are STANDARD offsets, never daylight. Getting this
wrong shifts every staleness calculation by an hour in summer.

WHAT THIS FILE DOES NOT DO
--------------------------
It does NOT import the order layer. No Kalshi key, no signing, no order path.
It physically cannot trade. It catches CLIs, parses the observed max, gates
it, and LOGS. Prove the edge on paper first.

=============================================================================
THE PARSER BUG THIS FILE EXISTS TO NOT REPEAT (2026-07-26)
=============================================================================
Denver's CLI reported an observed max of 103F, a record. The bot reported 90F.

The old regex looked for 'MAXIMUM TEMPERATURE (F)'. In the TODAY block the
words run the other way — section header 'TEMPERATURE (F)', row label
'MAXIMUM' underneath — so it never matched today's observation at all. The
only place that literal phrase appears in a daily CLI is the bottom:

    THE DENVER CO CLIMATE NORMALS FOR TOMORROW
                             NORMAL    RECORD    YEAR
     MAXIMUM TEMPERATURE (F)   90        98      1964

90 was TOMORROW'S 30-YEAR CLIMATE NORMAL. Not an observation of anything.

Replay showed this hit on every day, not just record days — wherever the
tomorrow-normals block exists, the old parser returned the normal. It survived
for weeks because normals sit close to actuals, so the number always looked
plausible. Denver running +13 is the only reason it surfaced.

A second landmine sat in the same row: the value prints as '103R' when a
record is set or tied, which breaks any pattern expecting bare digits followed
by whitespace — on exactly the days with the biggest mispricing.

Guards now in place:
  1. parse_cli_max() cuts the tomorrow-normals block, scopes to the TODAY
     TEMPERATURE section, anchors on the OBSERVED column (value + LST time),
     tolerates R/T flags, and returns None on MM. It REFUSES rather than
     guessing. A skipped trade costs less than a 13F error.
  2. Corroboration: no fire on a single unverified parse unless the
     observation is very stale.
  3. Sanity band on the parsed value, and a hard reset if the max ever
     decreases (a running max cannot fall — that means a parse fault).

Connection guts (TLS, SASL, room, ping-answering) carried over verbatim from
the working bot. Do not touch them — they were hard-won:
  - room is 'nwws@conference...', NOT 'nwws-oi@conference...'
  - server KICKS clients that don't answer <iq><ping/></iq>
  - product text lives in the <x xmlns='nwws-oi'> stanza, not <body>
"""

import os
import re
import ssl
import csv
import json
import socket
import base64
import time
import logging
import urllib.request
from datetime import datetime, timezone, timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s [CLI] %(message)s')
log = logging.getLogger('cli')

# ---------------------------------------------------------------- connection
NWWS_USERNAME = os.environ.get('NWWS_OI_USERNAME', 'noah.wolfe')
NWWS_PASSWORD = os.environ.get('NWWS_OI_PASSWORD', '')
NWWS_SERVER = 'nwws-oi-cprk.weather.gov'
NWWS_PORT = 5222
NWWS_ROOM = 'nwws@conference.nwws-oi.weather.gov'
NWWS_NICK = os.environ.get('NWWS_OI_NICK', 'clihigh1')

TG_TOKEN = os.environ.get('CLI_TELEGRAM_TOKEN',
                          os.environ.get('TELEGRAM_BOT_TOKEN', ''))
TG_CHAT = os.environ.get('CLI_TELEGRAM_CHAT_ID',
                         os.environ.get('TELEGRAM_CHAT_ID', ''))
LOG_PATH = os.environ.get('CLI_LOG', '/tmp/cli_highs.csv')

KALSHI = 'https://api.elections.kalshi.com/trade-api/v2'
UA = {'User-Agent': 'cli-highs/2.0'}

# ---------------------------------------------------------------- gate knobs
# Price ceiling — above this the market has already repriced.
MAX_YES_PCT = int(os.environ.get('CLI_PRICE_CEILING_C', '55'))
# Minutes the observation must predate issuance before the high counts as
# confirmed. Floor for everything.
CONFIRM_STALE_MIN = int(os.environ.get('CLI_CONFIRM_STALE_MIN', '90'))
# Above this staleness a single CLI may fire without a second one agreeing.
SOLO_STALE_MIN = int(os.environ.get('CLI_SOLO_STALE_MIN', '150'))

# ---------------------------------------------------------------- aim table
# Per city: verified Kalshi HIGH series, the LST (STANDARD, never daylight)
# UTC offset used to place the observation time, and the UTC cutoff at/after
# which a CLI is considered post-peak for that city.
#
# Kalshi series tickers VERIFIED live 2026-07-19 (see kalshi_temp_map.py).
# NOTE the T/no-T inconsistency is real: KXHIGHDEN but KXHIGHTPHX.
#
# confirmed_after_z is a backstop, not the primary test. Staleness is the
# primary test. This just stops a freak early-morning report from qualifying.
TARGETS = {
    'CLIDEN': {'name': 'Denver',        'series': 'KXHIGHDEN',
               'station': 'DEN', 'lst_offset': -7, 'confirmed_after_z': (21, 0)},
    'CLIPHX': {'name': 'Phoenix',       'series': 'KXHIGHTPHX',
               'station': 'PHX', 'lst_offset': -7, 'confirmed_after_z': (21, 0)},
    'CLISEA': {'name': 'Seattle',       'series': 'KXHIGHTSEA',
               'station': 'SEA', 'lst_offset': -8, 'confirmed_after_z': (22, 0)},
    'CLILAS': {'name': 'Las Vegas',     'series': 'KXHIGHTLV',
               'station': 'LAS', 'lst_offset': -8, 'confirmed_after_z': (22, 0)},
    'CLIVEF': {'name': 'Las Vegas',     'series': 'KXHIGHTLV',
               'station': 'LAS', 'lst_offset': -8, 'confirmed_after_z': (22, 0)},
    'CLIAUS': {'name': 'Austin',        'series': 'KXHIGHAUS',
               'station': 'AUS', 'lst_offset': -6, 'confirmed_after_z': (20, 0)},
    'CLIHOU': {'name': 'Houston',       'series': 'KXHIGHTHOU',
               'station': 'HOU', 'lst_offset': -6, 'confirmed_after_z': (20, 0)},
    'CLIDFW': {'name': 'Dallas',        'series': 'KXHIGHTDAL',
               'station': 'DFW', 'lst_offset': -6, 'confirmed_after_z': (20, 0)},
    'CLIOKC': {'name': 'OKC',           'series': 'KXHIGHTOKC',
               'station': 'OKC', 'lst_offset': -6, 'confirmed_after_z': (20, 0)},
    'CLIMSP': {'name': 'Minneapolis',   'series': 'KXHIGHTMIN',
               'station': 'MSP', 'lst_offset': -6, 'confirmed_after_z': (20, 0)},
    'CLIMIA': {'name': 'Miami',         'series': 'KXHIGHMIA',
               'station': 'MIA', 'lst_offset': -5, 'confirmed_after_z': (19, 0)},
    'CLINYC': {'name': 'NYC',           'series': 'KXHIGHNY',
               'station': 'NYC', 'lst_offset': -5, 'confirmed_after_z': (19, 0)},
    'CLILAX': {'name': 'LA',            'series': 'KXHIGHLAX',
               'station': 'LAX', 'lst_offset': -8, 'confirmed_after_z': (22, 0)},
    'CLIMDW': {'name': 'Chicago',       'series': 'KXHIGHCHI',
               'station': 'MDW', 'lst_offset': -6, 'confirmed_after_z': (20, 0)},
    'CLIPHL': {'name': 'Philadelphia',  'series': 'KXHIGHPHIL',
               'station': 'PHL', 'lst_offset': -5, 'confirmed_after_z': (19, 0)},
    'CLIATL': {'name': 'Atlanta',       'series': 'KXHIGHTATL',
               'station': 'ATL', 'lst_offset': -5, 'confirmed_after_z': (19, 0)},
    'CLIBOS': {'name': 'Boston',        'series': 'KXHIGHTBOS',
               'station': 'BOS', 'lst_offset': -5, 'confirmed_after_z': (19, 0)},
    'CLIDCA': {'name': 'Washington DC', 'series': 'KXHIGHTDC',
               'station': 'DCA', 'lst_offset': -5, 'confirmed_after_z': (19, 0)},
}

# (station, climate_date) -> running state for the day
#   best        highest observed max seen so far
#   count       how many CLIs have reported that same best
#   confirmed   has it met the staleness + cutoff test
#   fired       have we already sent a paper signal
#   announced   last max we sent any telegram about (noise control)
_day = {}

_all_ids_seen = {}       # CLI ids landing on the wire
_all_products_seen = {}  # every product id, proves the wire is flowing
_processed = set()       # (awipsid, issue) dedup for the re-scan buffer
_vis_logged = set()      # (awipsid, issue) dedup for the visibility log


# ---------------------------------------------------------------- telegram
def telegram(msg):
    if not TG_TOKEN or not TG_CHAT:
        print(f"TELEGRAM: {msg}")
        return
    try:
        data = json.dumps({'chat_id': TG_CHAT, 'text': msg,
                           'parse_mode': 'HTML'}).encode()
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            data=data, headers={'Content-Type': 'application/json'})
        urllib.request.urlopen(req, timeout=8)
    except Exception as e:
        log.error(f"Telegram: {e}")


# ---------------------------------------------------------------- CLI parse
def _cli_today_temp_section(text):
    """Narrow a CLI to the TODAY temperature rows.

    Two cuts, both of which have burned us:
      1. Everything from 'CLIMATE NORMALS FOR TOMORROW' onward. That block
         holds 'MAXIMUM TEMPERATURE (F)   90' — a 30-year normal that looks
         exactly like an observation to a loose regex. This was the bug.
      2. Everything outside TEMPERATURE (F), so PRECIPITATION, DEGREE DAYS
         and WIND rows can never be mistaken for a temperature.
    """
    body = re.split(r'CLIMATE\s+NORMALS\s+FOR\s+TOMORROW', text, flags=re.I)[0]
    m = re.search(r'TEMPERATURE\s*\(F\)(.*?)'
                  r'(?:PRECIPITATION\s*\(IN\)|DEGREE\s+DAYS|WIND\s*\(MPH\)|\Z)',
                  body, re.I | re.S)
    return m.group(1) if m else body


# Observed column, strict: label, value, then the LST time that always follows
# a real observation.  '103R   129 PM'
_OBS_STRICT = re.compile(
    r'^[ \t]*MAXIMUM[ \t]+(-?\d{1,3}|MM)([RT])?[ \t]+(\d{1,4})[ \t]*(AM|PM)',
    re.I | re.M)
# Fallback for offices that omit the time. Still line-anchored and still
# scoped to the TODAY temperature section, so it cannot reach a normal.
_OBS_LOOSE = re.compile(
    r'^[ \t]*MAXIMUM[ \t]+(-?\d{1,3}|MM)([RT])?(?=[ \t]|$)', re.I | re.M)


def parse_cli_max(text):
    """Observed daily MAXIMUM from a CLI. Returns a dict, or None.

    None means WE COULD NOT READ IT. It never means 'here is the closest
    number I found'.

    Keys: max_f, flag ('R' record / 'T' tie / ''), time_lst, lst_minutes
          (minutes past LST midnight, or None), preliminary (bool).
    """
    sec = _cli_today_temp_section(text)
    m = _OBS_STRICT.search(sec)
    loose = False
    if not m:
        m = _OBS_LOOSE.search(sec)
        loose = True
    if not m:
        return None

    raw = m.group(1).upper()
    if raw == 'MM':          # office reported the value missing
        return None
    try:
        v = int(raw)
    except ValueError:
        return None
    if not (-60 <= v <= 140):
        return None

    time_lst, lst_min = '', None
    if not loose:
        time_lst = f'{m.group(3)} {m.group(4)}'.strip()
        lst_min = _hhmm_to_minutes(m.group(3), m.group(4))

    return {
        'max_f': v,
        'flag': (m.group(2) or '').upper(),
        'time_lst': time_lst,
        'lst_minutes': lst_min,
        # 'VALID TODAY AS OF 0400 PM LOCAL TIME' = running max, not settled.
        'preliminary': bool(re.search(r'VALID\s+TODAY\s+AS\s+OF', text, re.I)),
    }


def _hhmm_to_minutes(hhmm, ampm):
    """'129','PM' -> minutes past LST midnight. '1229','AM' -> 149."""
    try:
        s = hhmm.zfill(3)
        hh, mm = int(s[:-2]), int(s[-2:])
    except (ValueError, IndexError):
        return None
    if not (0 <= mm < 60):
        return None
    ampm = (ampm or '').upper()
    if ampm == 'PM' and hh != 12:
        hh += 12
    elif ampm == 'AM' and hh == 12:
        hh = 0
    if not (0 <= hh < 24):
        return None
    return hh * 60 + mm


def observation_staleness_min(cday, lst_minutes, lst_offset, issue_dt):
    """Minutes between when the max was observed and when the CLI was issued.

    This is the whole confirmation mechanism. The LST column is TRUE STANDARD
    TIME (verified 2026-07-26: CLI said 129 PM, Wethr showed 2:29 PM for the
    same observation — exactly the DST hour apart). So the offset applied here
    is the city's STANDARD offset, never its daylight one.

    Returns minutes, or None if the observation time was unreadable.
    """
    if lst_minutes is None or cday is None or issue_dt is None:
        return None
    obs_utc = (datetime(cday.year, cday.month, cday.day,
                        tzinfo=timezone.utc)
               + timedelta(minutes=lst_minutes)
               - timedelta(hours=lst_offset))
    return round((issue_dt - obs_utc).total_seconds() / 60)


MONTHS = {m: i + 1 for i, m in enumerate(
    ['JANUARY', 'FEBRUARY', 'MARCH', 'APRIL', 'MAY', 'JUNE', 'JULY',
     'AUGUST', 'SEPTEMBER', 'OCTOBER', 'NOVEMBER', 'DECEMBER'])}


def parse_climate_date(text):
    """The date the product DESCRIBES, which is NOT the issue date.

    A CLI issued at 12:17Z on Jul 22 is the summary for Jul 21. Using the
    issue date matched yesterday's high against TODAY's market and the gate
    called it cheap. Caught on paper; would have swept the wrong day live.

    Returns a date, or None. None means SKIP — we do not guess.
    """
    from datetime import date as _date
    # Cut the tomorrow-normals block first: it also contains 'FOR TOMORROW'
    # text that a loose date pattern could latch onto.
    head = re.split(r'CLIMATE\s+NORMALS\s+FOR\s+TOMORROW', text, flags=re.I)[0]
    pats = [
        r'CLIMATE\s+SUMMARY\s+FOR\s+([A-Z]+)\s+(\d{1,2})\s+(\d{4})',
        r'CLIMATE\s+REPORT\s+FOR\s+([A-Z]+)\s+(\d{1,2})\s+(\d{4})',
        r'SUMMARY\s+FOR\s+([A-Z]+)\s+(\d{1,2})\s+(\d{4})',
        r'\bFOR\s+([A-Z]{3,9})\s+(\d{1,2})\s+(\d{4})\b',
    ]
    for pat in pats:
        m = re.search(pat, head, re.IGNORECASE)
        if not m:
            continue
        mon = MONTHS.get(m.group(1).upper())
        if not mon:
            continue
        try:
            return _date(int(m.group(3)), mon, int(m.group(2)))
        except ValueError:
            continue
    return None


def issue_to_dt(issue):
    """'2026-07-26T22:36:00Z' -> datetime, or None."""
    try:
        return datetime.fromisoformat(issue.replace('Z', '+00:00'))
    except Exception:
        return None


# ---------------------------------------------------------------- kalshi
_last_call = [0.0]

def kalshi_json(url, timeout=8):
    for attempt in range(4):
        gap = time.time() - _last_call[0]
        if gap < 0.25:
            time.sleep(0.25 - gap)
        try:
            _last_call[0] = time.time()
            req = urllib.request.Request(url, headers=UA)
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code == 429 or 500 <= e.code < 600:
                time.sleep(0.5 * (2 ** attempt))
                continue
            raise
    raise RuntimeError(f"kalshi gave up: {url}")


def dollars(v):
    try:
        return int(round(float(v) * 100))
    except (TypeError, ValueError):
        return None


def event_ticker(series, day):
    return f"{series}-{day.strftime('%y%b%d').upper()}"


def get_brackets(event):
    d = kalshi_json(f"{KALSHI}/markets?event_ticker={event}&limit=100")
    return [m for m in d.get('markets', []) if m.get('status') == 'active']


def match_bracket(brackets, temp_f):
    for m in brackets:
        st = m.get('strike_type')
        fl, cp = m.get('floor_strike'), m.get('cap_strike')
        if st == 'between' and fl is not None and cp is not None:
            if fl <= temp_f <= cp:
                return m
        elif st == 'less' and cp is not None and temp_f < cp:
            return m
        elif st == 'greater' and fl is not None and temp_f > fl:
            return m
    return None


# ---------------------------------------------------------------- log
FIELDS = ['caught_utc', 'awipsid', 'city', 'station', 'issue', 'climate_day',
          'max_f', 'best_f', 'record_flag', 'obs_time_lst', 'stale_min',
          'reports_agreeing', 'preliminary', 'state', 'event', 'bracket',
          'sub_title', 'yes_ask_c', 'depth', 'decision', 'reason',
          'sec_after_issue']

def write_row(d):
    exists = os.path.exists(LOG_PATH)
    try:
        p = os.path.dirname(LOG_PATH)
        if p:
            os.makedirs(p, exist_ok=True)
        with open(LOG_PATH, 'a', newline='') as f:
            w = csv.DictWriter(f, fieldnames=FIELDS)
            if not exists:
                w.writeheader()
            w.writerow(d)
    except Exception as e:
        log.error(f"csv: {e}")


# ---------------------------------------------------------------- handler
def _market_note(st, cfg, msg):
    """Report a market-side problem, once per city per day per distinct msg.

    The weather half of this bot can be perfectly healthy while the Kalshi
    half fails — bad event ticker, market not open yet, temp outside every
    listed bracket. Those used to be silent returns, which made a working
    feed indistinguishable from a dead one. Now they speak, but only once,
    so a persistent problem doesn't turn into a notification flood.
    """
    seen = st.setdefault('market_notes', set())
    if msg in seen:
        return
    seen.add(msg)
    log.warning("MARKET %s — %s", cfg['name'], msg)
    telegram(f"⚠️ {cfg['name']} — weather data fine, market side: {msg}")


def handle_product(awipsid, cccc, issue, text):
    cfg = TARGETS.get(awipsid.upper())
    if not cfg:
        return

    # dedup: the buffer re-scans, so guard against processing the same
    # product twice (same id + same issue time).
    dedup_key = (awipsid.upper(), issue)
    if dedup_key in _processed:
        return
    _processed.add(dedup_key)

    now = datetime.now(timezone.utc)

    parsed = parse_cli_max(text)
    if parsed is None:
        log.warning("CLI %s — could not parse observed max, SKIPPING. "
                    "First 400 chars:\n%s", cfg['name'], text[:400])
        telegram(f"⚠️ {cfg['name']} CLI — observed max unreadable, skipped. "
                 f"Raw in logs.")
        return

    max_f = parsed['max_f']
    idt = issue_to_dt(issue) or now
    lag = round((now - idt).total_seconds())

    cday = parse_climate_date(text)
    if cday is None:
        log.warning("CLI %s — could not parse climate date, SKIPPING. "
                    "First 300 chars:\n%s", cfg['name'], text[:300])
        telegram(f"⚠️ {cfg['name']} CLI — no climate date parsed, skipped.")
        return

    stale_days = (now.date() - cday).days
    stale_min = observation_staleness_min(
        cday, parsed['lst_minutes'], cfg['lst_offset'], idt)

    # ---- running state for this station/day
    key = (cfg['station'], cday)
    st = _day.setdefault(key, {'best': None, 'count': 0, 'confirmed': False,
                               'fired': False, 'announced': None})

    if st['best'] is None or max_f > st['best']:
        # new high — the peak is still forming, confirmation resets
        st['best'] = max_f
        st['count'] = 1
        st['confirmed'] = False
    elif max_f == st['best']:
        # another independent report agreeing. This is the cross-check.
        st['count'] += 1
    else:
        # A running max cannot fall. If it does, either the parser faulted or
        # the office corrected downward. Either way do not trade through it.
        log.error("CLI %s %s — max DECREASED %sF -> %sF. Parse fault or "
                  "office correction. Blocking day.",
                  cfg['name'], cday, st['best'], max_f)
        telegram(f"🛑 <b>{cfg['name']} {cday}</b> — reported max fell "
                 f"{st['best']}°F → {max_f}°F.\nA running max cannot drop. "
                 f"Day blocked, check the log.")
        st['blocked'] = True

    best = st['best']

    # ---- confirmation: staleness is the primary test, clock is the backstop
    hh, mm = cfg['confirmed_after_z']
    past_cutoff = (idt.hour, idt.minute) >= (hh, mm)
    stale_ok = stale_min is not None and stale_min >= CONFIRM_STALE_MIN
    if stale_ok and past_cutoff:
        st['confirmed'] = True

    corroborated = st['count'] >= 2
    solo_ok = stale_min is not None and stale_min >= SOLO_STALE_MIN

    if not parsed['preliminary']:
        state = 'final'
    elif st['confirmed']:
        state = 'confirmed' if (corroborated or solo_ok) else 'confirmed_solo'
    else:
        state = 'provisional'

    row = dict.fromkeys(FIELDS, '')
    row.update(caught_utc=now.strftime('%H:%M:%S'), awipsid=awipsid,
               city=cfg['name'], station=cfg['station'], issue=issue,
               climate_day=str(cday), max_f=max_f, best_f=best,
               record_flag=parsed['flag'], obs_time_lst=parsed['time_lst'],
               stale_min=('' if stale_min is None else stale_min),
               reports_agreeing=st['count'],
               preliminary=('yes' if parsed['preliminary'] else ''),
               state=state, sec_after_issue=lag)

    log.info("CLI %s %s max=%sF%s obs=%s stale=%smin agree=%d state=%s +%ds",
             cfg['name'], cday, best,
             f" {parsed['flag']}" if parsed['flag'] else '',
             parsed['time_lst'] or '?', stale_min, st['count'], state, lag)

    # ---- ANNOUNCE FIRST, before anything market-side can fail.
    #
    # This ordering is deliberate and was learned the hard way. The previous
    # version did Kalshi first and announced last, with three silent returns
    # in between (lookup error / no brackets / no matching bracket). Any of
    # them made a perfectly healthy feed look stone dead, so 'no messages'
    # meant either 'nothing happened' or 'everything is broken' with no way
    # to tell them apart from the outside.
    #
    # Now the weather side always speaks. Market problems get their own
    # message rather than swallowing the whole report.
    if st['announced'] != best:
        telegram(f"📋 {cfg['name']} {cday} max <b>{best}°F</b>"
                 f"{' RECORD' if parsed['flag'] == 'R' else ''} "
                 f"({state}, obs {parsed['time_lst'] or '?'} LST, "
                 f"{stale_min}min stale, {st['count']} report(s))")
        st['announced'] = best

    # ---- Kalshi side
    event = event_ticker(cfg['series'], cday)
    row['event'] = event
    try:
        brackets = get_brackets(event)
    except Exception as e:
        row.update(decision='ERROR', reason=str(e)[:60])
        write_row(row)
        _market_note(st, cfg, f"Kalshi lookup failed: {str(e)[:60]}")
        return
    if not brackets:
        row.update(decision='SKIP', reason='no brackets')
        write_row(row)
        _market_note(st, cfg, f"no active brackets for {event}")
        return

    m = match_bracket(brackets, best)
    if not m:
        row.update(decision='SKIP', reason=f'no bracket for {best}F')
        write_row(row)
        _market_note(st, cfg, f"{best}°F matches no open bracket "
                              f"({len(brackets)} listed)")
        return

    yes_c = dollars(m.get('yes_ask_dollars'))
    depth = int(float(m.get('yes_ask_size_fp') or 0))
    row.update(bracket=m['ticker'], sub_title=m.get('yes_sub_title', ''),
               yes_ask_c=yes_c, depth=depth)

    # ---- gate
    if st.get('blocked'):
        fire, why = False, 'day blocked — max decreased'
    elif st['fired']:
        fire, why = False, 'already signalled this day'
    elif stale_days >= 1:
        fire, why = False, f'product is for {cday}, {stale_days}d old'
    elif not parsed['preliminary']:
        # The settled CLI lands after midnight, long after the market closed.
        # It is the scoreboard, not a signal.
        fire, why = False, 'final CLI — settlement scoreboard, not tradeable'
    elif stale_min is None:
        fire, why = False, 'no observation time — cannot confirm peak passed'
    elif not st['confirmed']:
        fire, why = False, (f'running max, obs only {stale_min}min old '
                            f'(need {CONFIRM_STALE_MIN}) — peak may still form')
    elif not (corroborated or solo_ok):
        fire, why = False, (f'unconfirmed by a second report and only '
                            f'{stale_min}min stale (solo needs {SOLO_STALE_MIN})')
    elif yes_c is None:
        fire, why = False, 'no price'
    elif yes_c >= MAX_YES_PCT:
        fire, why = False, f'repriced {yes_c}c >= {MAX_YES_PCT}c'
    elif depth <= 0:
        fire, why = False, 'no depth'
    else:
        fire, why = True, (f'peak passed {stale_min}min ago, '
                           f'{st["count"]} report(s) agree, cheap')

    row['reason'] = why
    row['decision'] = 'PAPER_BUY' if fire else 'SKIP'
    write_row(row)

    # ---- telegram, state-change only.
    # Every city reports several times a day. Pinging on all of them buries
    # the one message that matters.
    if fire:
        st['fired'] = True
        telegram(
            f"📝 <b>PAPER BUY</b> — {cfg['name']}\n"
            f"max <b>{best}°F</b>"
            f"{' RECORD' if parsed['flag'] == 'R' else ''} "
            f"at {parsed['time_lst']} LST\n"
            f"peak passed {stale_min}min before issue, "
            f"{st['count']} report(s) agree\n"
            f"{m.get('yes_sub_title')} @ {yes_c}¢  depth {depth}\n"
            f"{m['ticker']}\n"
            f"+{lag}s after issue\n"
            f"— paper only, no order —")
        st['announced'] = best


# ---------------------------------------------------------------- xmpp
def answer_pings(sock, text, send):
    """Server KICKS clients that don't answer ping IQs. Whitespace won't do."""
    if 'urn:xmpp:ping' not in text:
        return
    for iq in re.finditer(
            r'<iq[^>]*>\s*<ping[^>]*urn:xmpp:ping[^>]*/>\s*</iq>',
            text, re.DOTALL):
        block = iq.group(0)
        frm = re.search(r'\bfrom=["\']([^"\']+)["\']', block)
        idm = re.search(r'\bid=["\']([^"\']+)["\']', block)
        if not idm:
            continue
        to_attr = f' to="{frm.group(1)}"' if frm else ''
        try:
            send(sock, f'<iq type="result"{to_attr} id="{idm.group(1)}"/>')
        except Exception as e:
            log.warning(f"Ping reply failed: {e}")


def parse_nwws_message(data_bytes):
    """Product text lives in <x xmlns='nwws-oi'>, NOT <body>.

    THIS IS THE BUG THAT KILLED THE OLD BOT: the room JID itself contains the
    string 'nwws-oi', so a naive `if b'nwws-oi' in buf: truncate` fires on
    every stanza — including partial ones — and chops the opening
    <x xmlns='nwws-oi' awipsid=...> tag off multi-KB products before the rest
    arrives. Nothing ever parses. Only discard COMPLETE stanzas.
    """
    text = data_bytes.decode('utf-8', errors='ignore')
    if 'nwws-oi' not in text:
        return 0
    pattern = r'<x[^>]+xmlns=["\']nwws-oi["\'][^>]*>(.*?)</x>'
    for match in re.finditer(pattern, text, re.DOTALL):
        full_x = match.group(0)
        product_text = match.group(1).strip()
        aid = re.search(r'awipsid=["\']([A-Za-z0-9 ]+)["\']', full_x)
        ccc = re.search(r'cccc=["\']([A-Za-z0-9]+)["\']', full_x)
        iss = re.search(r'issue=["\']([^"\']+)["\']', full_x)
        if not aid:
            continue
        awipsid = aid.group(1).replace(' ', '').upper()
        office = ccc.group(1) if ccc else ''

        _all_products_seen[awipsid] = _all_products_seen.get(awipsid, 0) + 1
        vis_key = (awipsid, iss.group(1) if iss else '')

        # Log every CLI id we see, tracked or not, so missing city mappings
        # surface from the tape instead of being guessed.
        if awipsid.startswith('CLI') and vis_key not in _vis_logged:
            _vis_logged.add(vis_key)
            _all_ids_seen[awipsid] = _all_ids_seen.get(awipsid, 0) + 1
            if awipsid not in TARGETS:
                log.info("*** untracked CLI %s office=%s ***", awipsid, office)

        if awipsid in TARGETS:
            handle_product(awipsid, office,
                           iss.group(1) if iss else '', product_text)
    return 0


def xmpp_connect():
    while True:
        sock = None
        try:
            log.info(f"Connecting to {NWWS_SERVER}:{NWWS_PORT}...")
            raw = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            raw.settimeout(30)
            raw.connect((NWWS_SERVER, NWWS_PORT))

            def send(s, data):
                if isinstance(data, str):
                    data = data.encode('utf-8')
                s.sendall(data)

            send(raw, f'<?xml version="1.0"?>'
                      f'<stream:stream to="{NWWS_SERVER}" '
                      f'xmlns="jabber:client" '
                      f'xmlns:stream="http://etherx.jabber.org/streams" '
                      f'version="1.0">')
            buf = b''
            for _ in range(20):
                buf += raw.recv(4096)
                if b'starttls' in buf.lower() or b'features' in buf:
                    break
                time.sleep(0.2)

            send(raw, '<starttls xmlns="urn:ietf:params:xml:ns:xmpp-tls"/>')
            time.sleep(0.5)
            raw.recv(4096)

            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            sock = ctx.wrap_socket(raw, server_hostname=NWWS_SERVER)
            log.info("TLS ready")

            send(sock, f'<?xml version="1.0"?>'
                       f'<stream:stream to="{NWWS_SERVER}" '
                       f'xmlns="jabber:client" '
                       f'xmlns:stream="http://etherx.jabber.org/streams" '
                       f'version="1.0">')
            buf = b''
            for _ in range(20):
                buf += sock.recv(4096)
                if b'mechanisms' in buf or b'features' in buf:
                    break
                time.sleep(0.2)

            auth_str = f'\x00{NWWS_USERNAME}\x00{NWWS_PASSWORD}'
            auth_b64 = base64.b64encode(auth_str.encode()).decode()
            send(sock, f'<auth xmlns="urn:ietf:params:xml:ns:xmpp-sasl" '
                       f'mechanism="PLAIN">{auth_b64}</auth>')
            buf = b''
            for _ in range(20):
                buf += sock.recv(4096)
                if b'success' in buf or b'failure' in buf:
                    break
                time.sleep(0.2)
            if b'failure' in buf:
                log.error("Auth failed")
                telegram("❌ NWWS-OI auth failed")
                time.sleep(60)
                continue
            log.info("Authenticated")

            send(sock, f'<?xml version="1.0"?>'
                       f'<stream:stream to="{NWWS_SERVER}" '
                       f'xmlns="jabber:client" '
                       f'xmlns:stream="http://etherx.jabber.org/streams" '
                       f'version="1.0">')
            time.sleep(1)
            sock.recv(4096)

            send(sock, '<iq type="set" id="bind1">'
                       '<bind xmlns="urn:ietf:params:xml:ns:xmpp-bind">'
                       f'<resource>{NWWS_NICK}</resource>'
                       '</bind></iq>')
            time.sleep(0.5)
            sock.recv(4096)

            send(sock, '<iq type="set" id="sess1">'
                       '<session xmlns="urn:ietf:params:xml:ns:xmpp-session"/>'
                       '</iq>')
            time.sleep(0.5)
            send(sock, '<presence/>')
            send(sock, f'<presence to="{NWWS_ROOM}/{NWWS_NICK}">'
                       f'<x xmlns="http://jabber.org/protocol/muc">'
                       f'<history maxchars="0"/>'
                       f'</x></presence>')

            log.info(f"Joined {NWWS_ROOM} as {NWWS_NICK} — listening...")
            telegram("✅ <b>CLI HIGHS BOT — PAPER</b>\n"
                     f"{len(TARGETS)} cities on the climate-report stream.\n"
                     f"Locks the high off observation staleness: peak must be "
                     f"{CONFIRM_STALE_MIN}min behind issuance, and either a "
                     f"second report agrees or it is {SOLO_STALE_MIN}min stale.\n"
                     "Telegram on state change only.\n"
                     "No order layer — cannot trade.")

            sock.settimeout(15)
            buf = b''
            chunks = 0
            last_ping = time.time()
            last_product = time.time()
            last_report = time.time()

            while True:
                if time.time() - last_ping >= 15:
                    try:
                        send(sock, ' ')
                        last_ping = time.time()
                    except Exception:
                        log.warning("Keepalive failed — reconnecting")
                        break

                if time.time() - last_product > 300:
                    log.warning("No products 5min — stream dead, reconnect")
                    break

                # hourly: which CLI ids are actually landing. Missing cities
                # show up here rather than being guessed at.
                if time.time() - last_report > 3600 and _all_ids_seen:
                    top = sorted(_all_ids_seen.items(),
                                 key=lambda kv: -kv[1])[:40]
                    log.info("CLI ids seen: %s", top)
                    last_report = time.time()

                try:
                    chunk = sock.recv(65536)
                    if not chunk:
                        log.warning("Connection closed")
                        break
                    buf += chunk
                    chunks += 1
                    text = buf.decode('utf-8', errors='ignore')
                    answer_pings(sock, text, send)

                    # Drain COMPLETE stanzas from the FRONT of the buffer as
                    # they arrive. Products come in dense bursts; the old
                    # 256KB rolling re-scan overflowed and the trim chopped
                    # the front of the batch, losing all but a few.
                    if b'nwws-oi' in buf:
                        last_product = time.time()
                    while True:
                        end = buf.find(b'</message>')
                        if end == -1:
                            break
                        end += len(b'</message>')
                        stanza, buf = buf[:end], buf[end:]
                        if b'nwws-oi' in stanza:
                            parse_nwws_message(stanza)
                    # safety: if no </message> yet but the buffer is huge, the
                    # stream may use bare stanzas — drain by </x> so we never
                    # accumulate unbounded.
                    if len(buf) > 524288:
                        while True:
                            ex = buf.find(b'</x>')
                            if ex == -1:
                                break
                            ex += len(b'</x>')
                            chunk_s, buf = buf[:ex], buf[ex:]
                            if b'nwws-oi' in chunk_s:
                                parse_nwws_message(chunk_s)
                        if len(buf) > 524288:
                            buf = buf[-131072:]

                    if chunks % 500 == 0:
                        log.info("%d chunks | %d products | %d distinct CLI ids",
                                 chunks, len(_all_products_seen),
                                 len(_all_ids_seen))
                except socket.timeout:
                    try:
                        send(sock, ' ')
                        last_ping = time.time()
                    except Exception:
                        break

        except Exception as e:
            log.error(f"Connection error: {e}")
        finally:
            if sock:
                try:
                    sock.close()
                except Exception:
                    pass
        log.warning("Reconnecting in 30s...")
        time.sleep(30)


if __name__ == '__main__':
    import sys
    sys.stdout.reconfigure(line_buffering=True)
    if not NWWS_PASSWORD:
        log.error("NWWS_OI_PASSWORD not set")
        exit(1)
    log.info("CLI HIGHS BOT — PAPER ONLY, no order layer")
    log.info(f"{len(TARGETS)} cities, log -> {LOG_PATH}")
    xmpp_connect()
