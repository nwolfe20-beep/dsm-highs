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

CONFIRMED HIGH = max(provisional_dsm, confirmed_dsm). If both agree, the high
held across the hour — highest confidence.

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
UA = {'User-Agent': 'dsm-highs/1.0'}

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

# remembers today's readings so we can do max(provisional, confirmed)
_seen_today = {}     # (station, date) -> {'prov': f, 'conf': f}
_all_ids_seen = {}       # DSM/CLI only
_all_products_seen = {}  # every product id, proves the wire is flowing
_processed = set()       # (awipsid, issue) dedup for the re-scan buffer


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


# ---------------------------------------------------------------- parsing
def parse_maximum(text):
    """Pull the daily MAXIMUM temperature (F) out of a DSM or CLI.

    DSM/CLI layouts vary. We try the common forms, most specific first, and
    sanity-bound the value. If none match we return None and LOG THE TEXT so
    we can fix the pattern against a real product instead of guessing.
    """
    pats = [
        # CLI tabular: "MAXIMUM TEMPERATURE (F)" then a row like
        #   "  TODAY      94   339 PM  ..."  -> first number after the label
        r'MAXIMUM\s+TEMPERATURE\s*\(F\)[^\d]{0,80}?(\d{1,3})',
        r'MAXIMUM\s+TEMPERATURE\s+\(F\)\s*[.\s]*(\d{1,3})',
        r'MAXIMUM\s+TEMPERATURE\s+(\d{1,3})',
        r'\bMAXIMUM\s+(\d{1,3})\s',
        r'\bMAX\s+TEMP\s+(\d{1,3})',
        r'HIGHEST\s+TEMPERATURE\s+(\d{1,3})',
    ]
    for p in pats:
        m = re.search(p, text, re.IGNORECASE)
        if m:
            v = int(m.group(1))
            if -60 <= v <= 140:
                return v
    return None


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
    for pat in pats:
        m = re.search(pat, text, re.IGNORECASE)
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
          'max_f', 'prov_f', 'confirmed', 'event', 'bracket', 'sub_title',
          'yes_ask_c', 'depth', 'decision', 'reason', 'sec_after_issue']

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
def handle_product(awipsid, cccc, issue, text):
    cfg = ALL_TARGETS.get(awipsid.upper())
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
    max_f = parse_maximum(text)

    if max_f is None:
        # Don't guess — surface the raw text so we can fix the pattern.
        log.warning("%s %s — no MAXIMUM found. First 300 chars:\n%s",
                    ptype, cfg['name'], text[:300])
        telegram(f"⚠️ {cfg['name']} {ptype} — no MAXIMUM parsed. "
                 f"Check logs for the raw text so we can fix the pattern.")
        return

    idt = issue_to_dt(issue) or now
    lag = round((now - idt).total_seconds())

    # The climate day is what the product DESCRIBES, not when it was issued.
    # Morning CLIs summarize YESTERDAY. Refuse to guess.
    cday = parse_climate_date(text)
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
    cutoff_ok = (idt.hour, idt.minute) >= (hh, mm) or is_cli
    key = (cfg['station'], cday)
    rec = _seen_today.setdefault(key, {})

    if is_cli:
        confirmed = 'cli'
    elif cutoff_ok:
        confirmed = 'yes'
    else:
        confirmed = 'no'

    prov = rec.get('prov')
    if confirmed == 'no':
        rec['prov'] = max(prov, max_f) if prov is not None else max_f
        final_f = rec['prov']
    else:
        # confirmed high = max(provisional, this one)
        final_f = max(max_f, prov) if prov is not None else max_f
        rec['conf'] = final_f

    row = dict.fromkeys(FIELDS, '')
    row.update(caught_utc=now.strftime('%H:%M:%S'), awipsid=awipsid,
               product=ptype, city=cfg['name'], station=cfg['station'],
               issue=issue, max_f=max_f, prov_f=(prov if prov else ''),
               confirmed=confirmed, sec_after_issue=lag)

    log.info("%s %s MAX=%dF (confirmed=%s) +%ds",
             ptype, cfg['name'], final_f, confirmed, lag)

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
    if is_cli:
        # CLI is the SETTLEMENT source, but by the time today's CLI lands the
        # market is typically done. Log it so we can measure whether CLIs ever
        # arrive early enough to be tradeable — but never fire on one. DSM is
        # the signal; CLI is the scoreboard.
        fire, why = False, 'CLI — log only, DSM fires'
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
        # scoreboard, not a signal — one quiet line
        telegram(f"📋 {cfg['name']} CLI settled max <b>{final_f}°F</b> "
                 f"({cday}) — reference only")
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
        aid = re.search(r'awipsid=["\']([A-Z0-9]+)["\']', full_x)
        ccc = re.search(r'cccc=["\']([A-Z0-9]+)["\']', full_x)
        iss = re.search(r'issue=["\']([^"\']+)["\']', full_x)
        if not aid:
            continue
        awipsid = aid.group(1)

        # Visibility: count EVERY product id.
        _all_products_seen[awipsid] = _all_products_seen.get(awipsid, 0) + 1
        # DIAGNOSTIC: DSMs are confirmed to exist (Austin dropped one) but
        # aren't appearing as 'DSM...'. So log ANY id containing 'DSM'
        # anywhere, plus anything for our target stations under any prefix.
        our_stations = {'DEN','PHX','SEA','LAS','AUS','HOU','DFW','OKC',
                        'MSP','MIA','NYC','LAX','MDW','PHL','ATL','BOS','DCA'}
        tail = awipsid[3:] if len(awipsid) > 3 else ''
        if 'DSM' in awipsid or awipsid.startswith(('DSM', 'CLI')) or \
           tail in our_stations:
            _all_ids_seen[awipsid] = _all_ids_seen.get(awipsid, 0) + 1
            log.info("*** product: %s (%s) ***", awipsid,
                     ccc.group(1) if ccc else '?')

        if awipsid.upper() in ALL_TARGETS:
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
                     "Catches the post-peak DSM max, gates it, logs it.\n"
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

                    # Re-scan a rolling window (like the proven old bot) so we
                    # NEVER trim past a stanza we haven't parsed. Dedup in
                    # handle_product prevents double-firing. Keeping a generous
                    # 256KB tail means a large product still fully assembles.
                    if b'nwws-oi' in buf:
                        last_product = time.time()
                        parse_nwws_message(buf)
                        buf = buf[-262144:] if len(buf) > 262144 else buf
                    elif len(buf) > 262144:
                        buf = buf[-262144:]

                    # DIAGNOSTIC: show what is actually arriving. Stop
                    # guessing at the parser — look at the wire.
                    if chunks in (5, 50, 200) or chunks % 1000 == 0:
                        sample = buf[-1500:].decode('utf-8', errors='ignore')
                        log.info("RAW@%d (last 1500B):\n%s\n--- end raw ---",
                                 chunks, sample)

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
