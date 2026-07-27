"""
nwws_dsm_highs.py — NWWS-OI DSM/CLI HIGH temperature bot. PAPER ONLY.

WHY HIGHS AND NOT LOWS
----------------------
The earlier low version failed for a structural reason: DSMs fire on a fixed
schedule, and the ones carrying a "low so far" fired BEFORE the true overnight
minimum had formed. You were reading a number too early to be the real low.

That same timing works IN OUR FAVOR for highs. The daily max forms mid-
afternoon (~3-4pm local). The afternoon DSMs fire AFTER that. So a post-peak
DSM carries a high that has already happened.

  Denver: peak 3-4pm local = 21-22Z.  DSMs at 22:17Z and 23:17Z.
          -> 22:17Z = PROVISIONAL (right at peak's tail)
          -> 23:17Z = CONFIRMED   (an hour past latest typical peak)

CONFIRMED HIGH = max over every product seen for that station/day. If the
provisional and confirmed readings agree, the high held across the hour —
highest confidence.

WHAT THIS FILE DOES NOT DO
--------------------------
It does NOT import the order layer. No Kalshi key, no signing, no order path.
It physically cannot trade. It catches DSMs, parses the max, matches the
bracket, runs the gate, and LOGS. Prove the edge on paper first — the same
discipline that kept the low side clean.

AWIPS IDS ARE LEARNED, NOT GUESSED
----------------------------------
We don't know every city's exact DSM AWIPS id for certain (the old file had
'DSMLSV' for Vegas, which may or may not be right). So this bot logs EVERY
DSM/CLI product id it sees on the wire, whether or not we track it. After a
day you read the log and you know the real ids instead of guessing.

Connection guts (TLS, SASL, room, ping-answering) are carried over verbatim
from the working low bot. Do not touch them — they were hard-won:
  - room is 'nwws@conference...', NOT 'nwws-oi@conference...'
  - server KICKS clients that don't answer <iq><ping/></iq>
  - product text lives in the <x xmlns='nwws-oi'> stanza, not <body>

=============================================================================
FIX 2026-07-26 — THE CLI COLUMN BUG
=============================================================================
On 2026-07-26 Denver's CLI reported an observed max of 103F (a record). The
bot reported 90F. Root cause, confirmed by replaying the real product:

The old first regex was
    MAXIMUM\\s+TEMPERATURE\\s*\\(F\\)[^\\d]{0,80}?(\\d{1,3})

In the TODAY block the words appear in the OPPOSITE order — the section
header is 'TEMPERATURE (F)' and the row label underneath is 'MAXIMUM'. So
that pattern never matched today's observation at all. The only place the
literal phrase 'MAXIMUM TEMPERATURE (F)' appears in a daily CLI is the
tomorrow-normals block at the bottom:

    THE DENVER CO CLIMATE NORMALS FOR TOMORROW
                             NORMAL    RECORD    YEAR
     MAXIMUM TEMPERATURE (F)   90        98      1964

90 is TOMORROW'S CLIMATE NORMAL. The bot was reading a 30-year average and
calling it a settled observation.

A second, independent landmine sat in the same row. The observed value prints
as '103R' when a record is set or tied, so any pattern expecting bare digits
followed by whitespace also fails on exactly the days with the biggest
mispricing. Both are fixed below.

THREE GUARDS ADDED
  1. parse_cli_max() cuts the tomorrow-normals block, scopes to the TODAY
     TEMPERATURE section, anchors on the OBSERVED column (value + LST time),
     tolerates the R/T flags, and returns None on MM rather than substituting
     whatever number it can find. It refuses instead of guessing.
  2. Preliminary CLIs are now flagged. This CLI said 'VALID TODAY AS OF 0400
     PM LOCAL TIME' — a running max, not a settled one. The old code called
     every CLI 'settled', which is what made the wrong number look official.
  3. DSM/CLI cross-check. If both products for the same station and day report
     a max and they disagree, that is a parser fault by definition — the two
     products describe the same observation. It alerts loudly.

The DSM decoder was NOT at fault: decode_dsm_max() returns 103 from the real
2026-07-26 KDEN DSM. Verified by replay. Left unchanged.
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
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, format='%(asctime)s [DSM] %(message)s')
log = logging.getLogger('dsm')

# ---------------------------------------------------------------- connection
NWWS_USERNAME = os.environ.get('NWWS_OI_USERNAME', 'noah.wolfe')
NWWS_PASSWORD = os.environ.get('NWWS_OI_PASSWORD', '')
NWWS_SERVER = 'nwws-oi-cprk.weather.gov'
NWWS_PORT = 5222
NWWS_ROOM = 'nwws@conference.nwws-oi.weather.gov'
NWWS_NICK = os.environ.get('NWWS_OI_NICK', 'dsmhigh1')

TG_TOKEN = os.environ.get('DSM_TELEGRAM_TOKEN',
                          os.environ.get('TELEGRAM_BOT_TOKEN', ''))
TG_CHAT = os.environ.get('DSM_TELEGRAM_CHAT_ID',
                         os.environ.get('TELEGRAM_CHAT_ID', ''))
LOG_PATH = os.environ.get('DSM_LOG', '/tmp/dsm_highs.csv')

KALSHI = 'https://api.elections.kalshi.com/trade-api/v2'
UA = {'User-Agent': 'dsm-highs/1.1'}

# gate
MAX_YES_PCT = int(os.environ.get('DSM_PRICE_CEILING_C', '55'))

# ---------------------------------------------------------------- aim table
# Per city: the verified Kalshi HIGH series, the local afternoon peak window,
# and which DSM issue times are provisional vs confirmed.
#
# Peak windows are ~3-4pm local. DSM times from the published schedule.
# 'confirmed_after_z' = the UTC hour:min at/after which a DSM is considered
# post-peak for that city. A DSM before that is PROVISIONAL.
#
# Kalshi series tickers VERIFIED live 2026-07-19 (see kalshi_temp_map.py).
# NOTE the T/no-T inconsistency is real: KXHIGHDEN but KXHIGHTPHX.
TARGETS = {
    # awips id -> config
    'DSMDEN': {'name': 'Denver',       'series': 'KXHIGHDEN',
               'station': 'DEN', 'confirmed_after_z': (23, 0)},
    'DSMPHX': {'name': 'Phoenix',      'series': 'KXHIGHTPHX',
               'station': 'PHX', 'confirmed_after_z': (23, 0)},
    'DSMSEA': {'name': 'Seattle',      'series': 'KXHIGHTSEA',
               'station': 'SEA', 'confirmed_after_z': (0, 30)},
    'DSMLAS': {'name': 'Las Vegas',    'series': 'KXHIGHTLV',
               'station': 'LAS', 'confirmed_after_z': (0, 0)},
    'DSMLSV': {'name': 'Las Vegas',    'series': 'KXHIGHTLV',
               'station': 'LAS', 'confirmed_after_z': (0, 0)},
    'DSMAUS': {'name': 'Austin',       'series': 'KXHIGHAUS',
               'station': 'AUS', 'confirmed_after_z': (23, 0)},
    'DSMHOU': {'name': 'Houston',      'series': 'KXHIGHTHOU',
               'station': 'HOU', 'confirmed_after_z': (23, 0)},
    'DSMDFW': {'name': 'Dallas',       'series': 'KXHIGHTDAL',
               'station': 'DFW', 'confirmed_after_z': (23, 0)},
    'DSMOKC': {'name': 'OKC',          'series': 'KXHIGHTOKC',
               'station': 'OKC', 'confirmed_after_z': (23, 0)},
    'DSMMSP': {'name': 'Minneapolis',  'series': 'KXHIGHTMIN',
               'station': 'MSP', 'confirmed_after_z': (23, 0)},
    'DSMMIA': {'name': 'Miami',        'series': 'KXHIGHMIA',
               'station': 'MIA', 'confirmed_after_z': (21, 0)},
    'DSMNYC': {'name': 'NYC',          'series': 'KXHIGHNY',
               'station': 'NYC', 'confirmed_after_z': (21, 0)},
    'DSMLAX': {'name': 'LA',           'series': 'KXHIGHLAX',
               'station': 'LAX', 'confirmed_after_z': (1, 0)},
    'DSMMDW': {'name': 'Chicago',      'series': 'KXHIGHCHI',
               'station': 'MDW', 'confirmed_after_z': (22, 0)},
    'DSMPHL': {'name': 'Philadelphia', 'series': 'KXHIGHPHIL',
               'station': 'PHL', 'confirmed_after_z': (21, 0)},
    'DSMATL': {'name': 'Atlanta',      'series': 'KXHIGHTATL',
               'station': 'ATL', 'confirmed_after_z': (21, 0)},
    'DSMBOS': {'name': 'Boston',       'series': 'KXHIGHTBOS',
               'station': 'BOS', 'confirmed_after_z': (21, 0)},
    'DSMDCA': {'name': 'Washington DC', 'series': 'KXHIGHTDC',
               'station': 'DCA', 'confirmed_after_z': (21, 0)},
}
# CLI products carry the same daily max and are the settlement source.
# We track them too — a CLI max is the strongest confirmation there is.
CLI_TARGETS = {('CLI' + v['station']): dict(v, is_cli=True)
               for v in TARGETS.values()}
for k in CLI_TARGETS:
    CLI_TARGETS[k].setdefault('is_cli', True)

ALL_TARGETS = {}
ALL_TARGETS.update(TARGETS)
ALL_TARGETS.update(CLI_TARGETS)

# remembers today's readings so we can take the max across every product.
# (station, date) -> {'best': f, 'dsm': f, 'cli': f, 'cli_prelim': bool}
_seen_today = {}
_all_ids_seen = {}       # DSM/CLI only
_all_products_seen = {}  # every product id, proves the wire is flowing
_processed = set()       # (awipsid, issue) dedup for the re-scan buffer
_vis_logged = set()      # (awipsid, issue) dedup for the visibility log
_dsm_samples_sent = 0    # push first few raw DSMs to telegram for calibration


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
    """Narrow a CLI down to the TODAY temperature rows.

    Two things get cut, and both of them have burned us:

      1. Everything from 'CLIMATE NORMALS FOR TOMORROW' onward. That block
         contains 'MAXIMUM TEMPERATURE (F)   90' — a 30-year normal that
         looks exactly like an observation to a loose regex. This is the
         2026-07-26 bug.
      2. Everything outside the TEMPERATURE (F) section, so PRECIPITATION,
         DEGREE DAYS and WIND rows can never be mistaken for a temperature.
    """
    body = re.split(r'CLIMATE\s+NORMALS\s+FOR\s+TOMORROW', text, flags=re.I)[0]
    m = re.search(r'TEMPERATURE\s*\(F\)(.*?)'
                  r'(?:PRECIPITATION\s*\(IN\)|DEGREE\s+DAYS|WIND\s*\(MPH\)|\Z)',
                  body, re.I | re.S)
    return m.group(1) if m else body


# Observed column, strict: label, value, then the LST time that always
# follows a real observation. '103R   129 PM'
_CLI_OBS_STRICT = re.compile(
    r'^[ \t]*MAXIMUM[ \t]+(-?\d{1,3}|MM)([RT])?[ \t]+(\d{1,4})[ \t]*(AM|PM)',
    re.I | re.M)
# Fallback for offices that omit the time. Still line-anchored and still
# scoped to the TODAY temperature section, so it cannot reach a normal.
_CLI_OBS_LOOSE = re.compile(
    r'^[ \t]*MAXIMUM[ \t]+(-?\d{1,3}|MM)([RT])?(?=[ \t]|$)', re.I | re.M)


def parse_cli_max(text):
    """Observed daily MAXIMUM from a CLI. Returns a dict, or None.

    None means WE COULD NOT READ IT. It never means 'here is the closest
    number I found'. Returning None costs a skipped trade; guessing cost us
    a 13F error on a record-setting day.

    Keys: max_f, flag ('R' record / 'T' trace-tie / ''), time_lst,
          preliminary (bool).
    """
    sec = _cli_today_temp_section(text)
    m = _CLI_OBS_STRICT.search(sec)
    loose = False
    if not m:
        m = _CLI_OBS_LOOSE.search(sec)
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

    time_lst = '' if loose else f'{m.group(3)} {m.group(4)}'.strip()
    return {
        'max_f': v,
        'flag': (m.group(2) or '').upper(),
        'time_lst': time_lst,
        # 'VALID TODAY AS OF 0400 PM LOCAL TIME' = running max, not settled.
        'preliminary': bool(re.search(r'VALID\s+TODAY\s+AS\s+OF', text, re.I)),
    }


MONTHS = {m: i + 1 for i, m in enumerate(
    ['JANUARY', 'FEBRUARY', 'MARCH', 'APRIL', 'MAY', 'JUNE', 'JULY',
     'AUGUST', 'SEPTEMBER', 'OCTOBER', 'NOVEMBER', 'DECEMBER'])}


def parse_climate_date(text):
    """The date the product DESCRIBES, which is NOT the issue date.

    A CLI issued at 12:17Z on Jul 22 is the summary for Jul 21. Using the
    issue date matched yesterday's 90F high against TODAY's market and the
    gate called it 'confirmed + cheap' at 3c. Caught on paper; would have
    swept 23 contracts on the wrong day live.

    Returns a date, or None. None means SKIP — we do not guess.
    """
    pats = [
        r'CLIMATE\s+SUMMARY\s+FOR\s+([A-Z]+)\s+(\d{1,2})\s+(\d{4})',
        r'CLIMATE\s+REPORT\s+FOR\s+([A-Z]+)\s+(\d{1,2})\s+(\d{4})',
        r'SUMMARY\s+FOR\s+([A-Z]+)\s+(\d{1,2})\s+(\d{4})',
        r'\bFOR\s+([A-Z]{3,9})\s+(\d{1,2})\s+(\d{4})\b',
    ]
    from datetime import date as _date
    # The tomorrow-normals block also contains 'FOR TOMORROW' text; cut it so
    # a loose date pattern can never pick up the wrong day.
    head = re.split(r'CLIMATE\s+NORMALS\s+FOR\s+TOMORROW', text, flags=re.I)[0]
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


def decode_dsm_max(text):
    """Decode a coded DSM (NOT plain text — that was an earlier bug).

    A DSM looks like:
        KDEN DS 1600 26/07 1031329/ 680243// 103/ 68//9771431/00
                           ^^^ ^^^^ max=103F at 1329 LST
    The first field after <dd/mm> is <MAX><HHMM>/, second is <MIN><HHMM>/.
    Temps are 2-3 digits. Returns max_f or None.

    VERIFIED against the real 2026-07-26 KDEN DSM: returns 103. This decoder
    was not the source of the 90F error.
    """
    m = re.search(
        r'\bDS\s+\d{3,4}\s+\d{2}/\d{2}\s+(\d{2,3})(\d{4})/',
        text)
    if m:
        v = int(m.group(1))
        if -60 <= v <= 140:
            return v
    return None


def decode_dsm_date(text, issue_dt):
    """A DSM body carries 'DD/MM' after the DS marker:
        KDEN DS 1600 26/07 1031329/ ...   -> day 26, month 07
    Returns a date. Uses issue year (DSMs don't carry the year). If the
    DD/MM can't be found, falls back to the issue date's date.
    """
    from datetime import date as _date
    m = re.search(r'\bDS\s+\d{3,4}\s+(\d{2})/(\d{2})\b', text)
    if m:
        dd, mm = int(m.group(1)), int(m.group(2))
        yr = issue_dt.year if issue_dt else _date.today().year
        try:
            return _date(yr, mm, dd)
        except ValueError:
            pass
    return issue_dt.date() if issue_dt else None


def issue_to_dt(issue):
    """'2026-07-21T23:17:00Z' -> datetime, or None."""
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
FIELDS = ['caught_utc', 'awipsid', 'product', 'city', 'station', 'issue',
          'max_f', 'best_f', 'record_flag', 'obs_time_lst', 'preliminary',
          'confirmed', 'event', 'bracket', 'sub_title', 'yes_ask_c', 'depth',
          'decision', 'reason', 'sec_after_issue']

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
# station SID (from DSM body 'Kxxx DS') -> config. This is how we ACTUALLY
# match DSMs: by the site in the body, not the awipsid or issuing office,
# because one office issues DSMs for many sites under SID-based ids.
SID_TO_CFG = {}
for _tk, _cfg in list(TARGETS.items()):
    SID_TO_CFG[_cfg['station']] = _cfg          # e.g. 'DEN' -> Denver cfg


def handle_product(awipsid, cccc, issue, text):
    cfg = ALL_TARGETS.get(awipsid.upper())
    # If the awipsid isn't a known target, try matching a DSM by the station
    # in its BODY ('KDEN DS ...' -> SID 'DEN'). This is the real matcher —
    # DSMs arrive under SID-based ids issued by various offices.
    if not cfg and awipsid.upper().startswith('DSM'):
        mbody = re.search(r'\bK?([A-Z]{3})\s+DS\s+\d', text)
        if mbody:
            sid = mbody.group(1)
            sid = sid[1:] if len(sid) == 4 and sid[0] == 'K' else sid
            cfg = SID_TO_CFG.get(sid)
    if not cfg:
        return
    # dedup: the buffer re-scans, so guard against processing the same
    # product twice (same id + same issue time).
    _dedup_key = (awipsid.upper(), issue)
    if _dedup_key in _processed:
        return
    _processed.add(_dedup_key)

    now = datetime.now(timezone.utc)
    is_cli = awipsid.upper().startswith('CLI')
    ptype = 'CLI' if is_cli else 'DSM'

    # DSMs are CODED numeric strings; CLIs are plain text. Different decoders.
    rec_flag, obs_time, prelim = '', '', False
    if is_cli:
        parsed = parse_cli_max(text)
        if parsed is None:
            max_f = None
        else:
            max_f = parsed['max_f']
            rec_flag = parsed['flag']
            obs_time = parsed['time_lst']
            prelim = parsed['preliminary']
    else:
        max_f = decode_dsm_max(text)

    if max_f is None:
        log.warning("%s %s — could not parse max. First 400 chars:\n%s",
                    ptype, cfg['name'], text[:400])
        telegram(f"⚠️ {cfg['name']} {ptype} — no max parsed, SKIPPED. "
                 f"Raw in logs.")
        return

    idt = issue_to_dt(issue) or now
    lag = round((now - idt).total_seconds())

    # The climate day is what the product DESCRIBES, not when it was issued.
    # CLIs carry it as text ('SUMMARY FOR JULY 26 2026'); DSMs carry it as
    # coded 'DD/MM'. Use the right decoder for each.
    if is_cli:
        cday = parse_climate_date(text)
    else:
        cday = decode_dsm_date(text, idt)
    if cday is None:
        log.warning("%s %s — could not parse climate date, SKIPPING. "
                    "First 300 chars:\n%s", ptype, cfg['name'], text[:300])
        telegram(f"⚠️ {cfg['name']} {ptype} — no climate date parsed, "
                 f"skipped. Raw text in logs.")
        return

    stale = (now.date() - cday).days
    if stale >= 1:
        log.info("%s %s is for %s (%d day(s) back) — not today's market",
                 ptype, cfg['name'], cday, stale)

    # provisional vs confirmed, from the aim table
    hh, mm = cfg['confirmed_after_z']
    cutoff_ok = (idt.hour, idt.minute) >= (hh, mm)
    key = (cfg['station'], cday)
    rec = _seen_today.setdefault(key, {})

    # ---- cross-check: DSM and CLI describe the SAME observation. If they
    # disagree, one of the two parsers is wrong. That is not a market signal,
    # it is a bug report, and it should never be traded through.
    rec[ptype.lower()] = max_f
    mismatch = False
    if rec.get('dsm') is not None and rec.get('cli') is not None \
            and rec['dsm'] != rec['cli']:
        mismatch = True
        log.error("PARSER MISMATCH %s %s: DSM=%sF CLI=%sF",
                  cfg['name'], cday, rec['dsm'], rec['cli'])
        telegram(f"🛑 <b>PARSER MISMATCH — {cfg['name']} {cday}</b>\n"
                 f"DSM says <b>{rec['dsm']}°F</b>, CLI says "
                 f"<b>{rec['cli']}°F</b>.\n"
                 f"Same observation, two answers. One decoder is wrong.\n"
                 f"All fires for this station/day are blocked.")
    rec['mismatch'] = rec.get('mismatch', False) or mismatch

    # running max across every product seen for this station/day
    best = rec.get('best')
    final_f = max_f if best is None else max(best, max_f)
    rec['best'] = final_f

    if is_cli:
        confirmed = 'cli_prelim' if prelim else 'cli_final'
    elif cutoff_ok:
        confirmed = 'yes'
    else:
        confirmed = 'no'

    row = dict.fromkeys(FIELDS, '')
    row.update(caught_utc=now.strftime('%H:%M:%S'), awipsid=awipsid,
               product=ptype, city=cfg['name'], station=cfg['station'],
               issue=issue, max_f=max_f, best_f=final_f,
               record_flag=rec_flag, obs_time_lst=obs_time,
               preliminary=('yes' if prelim else ''),
               confirmed=confirmed, sec_after_issue=lag)

    log.info("%s %s MAX=%dF (this product %dF%s, confirmed=%s) +%ds",
             ptype, cfg['name'], final_f, max_f,
             f' {rec_flag}' if rec_flag else '', confirmed, lag)

    # ---- Kalshi side
    event = event_ticker(cfg['series'], cday)
    row['event'] = event
    try:
        brackets = get_brackets(event)
    except Exception as e:
        row.update(decision='ERROR', reason=str(e)[:60])
        write_row(row)
        return
    if not brackets:
        row.update(decision='SKIP', reason='no brackets')
        write_row(row)
        telegram(f"🔆 <b>{cfg['name']} {ptype}</b> max {final_f}°F "
                 f"(+{lag}s)\nno open brackets")
        return

    m = match_bracket(brackets, final_f)
    if not m:
        row.update(decision='SKIP', reason=f'no bracket for {final_f}F')
        write_row(row)
        return

    yes_c = dollars(m.get('yes_ask_dollars'))
    depth = int(float(m.get('yes_ask_size_fp') or 0))
    row.update(bracket=m['ticker'], sub_title=m.get('yes_sub_title', ''),
               yes_ask_c=yes_c, depth=depth)

    # ---- gate
    if rec.get('mismatch'):
        fire, why = False, 'DSM/CLI parser mismatch — blocked'
    elif is_cli:
        # CLI is the SETTLEMENT source, but by the time today's final CLI
        # lands the market is typically done. Log it so we can measure whether
        # CLIs ever arrive early enough to be tradeable — but never fire on
        # one. DSM is the signal; CLI is the scoreboard.
        fire, why = False, ('CLI preliminary — running max, log only'
                            if prelim else 'CLI — log only, DSM fires')
    elif stale >= 1:
        fire, why = False, f'product is for {cday}, {stale}d old — not today'
    elif confirmed == 'no':
        fire, why = False, 'provisional (pre-peak DSM) — watch only'
    elif yes_c is None:
        fire, why = False, 'no price'
    elif yes_c >= MAX_YES_PCT:
        fire, why = False, f'repriced {yes_c}c >= {MAX_YES_PCT}c'
    elif depth <= 0:
        fire, why = False, 'no depth'
    else:
        fire, why = True, 'confirmed + cheap'

    row['reason'] = why
    row['decision'] = 'PAPER_BUY' if fire else 'SKIP'
    write_row(row)

    if is_cli:
        # scoreboard, not a signal — and say plainly which kind it is.
        if prelim:
            telegram(f"📋 {cfg['name']} CLI <i>preliminary</i> max "
                     f"<b>{final_f}°F</b> ({cday})"
                     f"{' RECORD' if rec_flag == 'R' else ''} — running "
                     f"value as of issuance, not settled")
        else:
            telegram(f"📋 {cfg['name']} CLI settled max <b>{final_f}°F</b> "
                     f"({cday}){' RECORD' if rec_flag == 'R' else ''} "
                     f"— reference only")
        return

    tag = ('📝 <b>PAPER BUY</b>' if fire else '⏭️ SKIP')
    telegram(
        f"{tag} — {cfg['name']} {ptype}\n"
        f"max <b>{final_f}°F</b> ({confirmed})\n"
        f"{m.get('yes_sub_title')} @ {yes_c}¢  depth {depth}\n"
        f"{m['ticker']}\n"
        f"+{lag}s after issue\n"
        f"{why}\n"
        f"— paper only, no order —"
    )


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

    Returns the byte offset of the end of the LAST COMPLETE </x> stanza, so
    the caller can discard only what it has fully consumed.

    THIS IS THE BUG THAT KILLED THE OLD BOT: the room JID itself contains the
    string 'nwws-oi', so a naive `if b'nwws-oi' in buf: truncate` fires on
    every stanza — including partial ones — and chops the opening
    <x xmlns='nwws-oi' awipsid=...> tag off multi-KB products before the rest
    arrives. Nothing ever parses. Only discard COMPLETE stanzas.
    """
    text = data_bytes.decode('utf-8', errors='ignore')
    if 'nwws-oi' not in text:
        return 0
    last_end = 0
    pattern = r'<x[^>]+xmlns=["\']nwws-oi["\'][^>]*>(.*?)</x>'
    for match in re.finditer(pattern, text, re.DOTALL):
        last_end = match.end()
        full_x = match.group(0)
        product_text = match.group(1).strip()
        # awipsid can be missing, spaced, or lowercase on some products
        # (notably DSMs, which killed the old assumption). Allow spaces and
        # any case, then normalise. If still absent, DON'T skip — try to
        # recover the id from the product body's first line (e.g. 'KDEN DS').
        aid = re.search(r'awipsid=["\']([A-Za-z0-9 ]+)["\']', full_x)
        ccc = re.search(r'cccc=["\']([A-Za-z0-9]+)["\']', full_x)
        iss = re.search(r'issue=["\']([^"\']+)["\']', full_x)
        if aid:
            awipsid = aid.group(1).replace(' ', '').upper()
        else:
            # recover: a DSM body starts '<SID> DS <time>'. Build DSM<SID>.
            mds = re.search(r'\b(K?[A-Z]{3})\s+DS\s+\d', product_text)
            if mds:
                sid = mds.group(1)
                sid = sid[1:] if len(sid) == 4 and sid[0] == 'K' else sid
                awipsid = 'DSM' + sid
            else:
                continue

        # Visibility with dedup on (id, issue) so the re-scan buffer doesn't
        # spam the same product every recv.
        _all_products_seen[awipsid] = _all_products_seen.get(awipsid, 0) + 1
        _vis_key = (awipsid, iss.group(1) if iss else '')
        office = ccc.group(1) if ccc else ''
        # ONLY DSM/CLI for the normal log.
        interesting = awipsid.startswith(('DSM', 'CLI'))

        if interesting and _vis_key not in _vis_logged:
            _vis_logged.add(_vis_key)
            _all_ids_seen[awipsid] = _all_ids_seen.get(awipsid, 0) + 1
            if awipsid.startswith('DSM'):
                # DSMs are coded — capture the raw string so the decoder can
                # be calibrated against real products, not guesses.
                body = product_text.strip().replace('\n', ' ')[:200]
                decoded = decode_dsm_max(product_text)
                log.info("*** DSM %s office=%s decoded_max=%s ***\n    RAW: %s",
                         awipsid, office, decoded, body)
                global _dsm_samples_sent
                if _dsm_samples_sent < 5:
                    _dsm_samples_sent += 1
                    telegram(f"🔬 <b>DSM SAMPLE {_dsm_samples_sent}/5</b>\n"
                             f"{awipsid} ({office})\n"
                             f"decoded max: <b>{decoded}</b>\n"
                             f"<code>{body[:180]}</code>")
            else:
                parsed = parse_cli_max(product_text)
                log.info("*** %s office=%s parsed_max=%s ***",
                         awipsid, office,
                         parsed['max_f'] if parsed else None)

        # Route to handler if it's a known target OR any DSM (handler does
        # body-based station matching for DSMs under SID-based ids).
        if awipsid.upper() in ALL_TARGETS or awipsid.upper().startswith('DSM'):
            handle_product(awipsid, ccc.group(1) if ccc else '',
                           iss.group(1) if iss else '', product_text)
    return last_end


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
            telegram("✅ <b>DSM HIGHS BOT — PAPER</b>\n"
                     f"{len(TARGETS)} DSM targets + CLI confirmations\n"
                     "CLI parser fixed: reads the OBSERVED column, not the\n"
                     "tomorrow-normals block. Preliminary CLIs now labelled.\n"
                     "DSM/CLI mismatch blocks all fires for that station/day.\n"
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

                # hourly: report which DSM/CLI ids we've actually seen, so
                # we LEARN the real awips ids instead of guessing them
                if time.time() - last_report > 3600 and _all_ids_seen:
                    top = sorted(_all_ids_seen.items(),
                                 key=lambda kv: -kv[1])[:40]
                    log.info("DSM/CLI ids seen: %s", top)
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
                    # they arrive. DSMs come in dense bursts (30+ at 12:16Z);
                    # the old 256KB rolling re-scan overflowed and the trim
                    # chopped the front of the batch, losing all but a few.
                    # Now: repeatedly pull the earliest complete
                    # </message> (or </x></message>) off the front, process it,
                    # and remove it — so no burst can overflow anything.
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
                    # safety: if no </message> yet but buffer is huge, the
                    # stream may use bare stanzas — fall back to draining by
                    # </x> so we still never accumulate unbounded.
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
                        log.info("%d chunks | %d products seen | %d distinct DSM/CLI ids",
                                 chunks, len(_all_products_seen), len(_all_ids_seen))
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
    log.info("DSM HIGHS BOT — PAPER ONLY, no order layer")
    log.info(f"{len(TARGETS)} DSM targets, log -> {LOG_PATH}")
    xmpp_connect()
