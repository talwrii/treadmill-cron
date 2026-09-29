#!/usr/bin/env python3
"""
Treadmill interval daemon. Polls treadmill state, fires intervals on schedule.

Schedule entry format (in ~/.config/treadmill-cron/schedule):
  [priority=N]  TIME_RANGE  speed  incline  [, now-now+MM:SS  speed  incline]...

Time ranges:
  :MM-:MM                hourly minute window
  :MM:SS-:MM:SS          hourly with seconds
  H:MM-H:MM              absolute time of day
  day+MM:SS-day+MM:SS    MM:SS of cumulative belt time today (once/day)

Keeping an axis: write the bare word `speed` or `incline` in place of a number
  to leave that axis alone.
  :00-:05  speed  6.0     set incline to 6%, don't touch the walking speed
  A kept axis is never written and never restored when the entry ends, so a
  manual adjustment mid-interval survives.

Variables: declare one with `var`, then use it as `$name` in any speed or
  incline field. Convention is to declare it directly above the entries using it.
    var challenge default=speed consume
    :00-:05  $challenge  6.0
  default=  what to use while nothing is set. May be a number, a ramp, or the
            bare keyword speed/incline meaning 'leave that axis alone'.
  consume   clear the value once a session has actually used it, so it applies
            to the next session only. A window that passes with the belt
            stopped doesn't burn it -- the value stays armed until it is used.
  Set with `treadmill-cron set NAME VALUE`; values are read fresh every tick,
  so arming one takes effect without restarting the daemon.

Ramping (any number can take +delta/day or +delta/week):
  3.0+0.05/day           +0.05 each day since start_date
  3.5+0.1/week           +0.1/7 per day
  day+120:00-day+123:00+30s/day    end-time grows by 30s/day

Priority:
  No priority -> won't preempt anything; if it would overlap something running, skip.
  priority=N (integer) -> higher wins; preempts a running lower-priority entry.

Flags (arbitrary per-entry metadata):
  flag=blah,other,hello=world   comma-separated; a bare word is presence-only
                                 (True), name=value carries an actual value.
  Each item splits on its *first* '=' only, so a value may itself contain '='
  without quoting: hello=world=extra -> {'hello': 'world=extra'}.
  treadmill-cron itself does nothing with these beyond attaching them to
  session_start/session_end events -- they exist for whatever's consuming
  `treadmill-cron events` to key off of (e.g. counting sessions with a
  particular flag), not for treadmill-cron's own logic.

Sequences (continuations after comma):
  Each continuation runs immediately after the previous chunk for an explicit duration.
  The whole sequence shares the priority and runs uninterrupted (modulo preemption).

Creep:
  TIME_RANGE  creep  interval=10m  step=0.1  max=2.5
  Gentle upward pressure during free walking. Every `interval` of belt time,
  nudge speed up by `step` until `max`. Climbs from the *measured* speed, so a
  manual slow-down just lowers where the next nudge starts. TIME_RANGE limits it
  to a clock window (use `*` for always). Lowest priority: only acts when no
  other entry is running, so every interval above outranks it for free.
  interval accepts s/m/h (e.g. 10m, 600s).

Modes (per-entry @mode tag):
  @boost :00-:10  5.0  12.0    entry belongs to the 'boost' mode
  :00-:05  3.0  10.0           no @ -> the default mode
  Only entries whose mode matches the active mode run. The active mode lives in
  ~/.config/treadmill-cron/mode (missing/empty = default) and is re-read every
  tick, so `treadmill-cron mode ...` (e.g. from a MIDI remote) swaps the live
  schedule without restarting the daemon. Switching mid-session restores the
  walking speed and lets the new mode take over.

Subcommands:
  treadmill-cron status        show effective values for today
  treadmill-cron bar           one compact line for a status bar: active mode
                               plus the running entry's time left, or the next
                               entry and how long until it fires
  treadmill-cron wait [SECS]   block until that line would change (or SECS pass,
                               default 60) so a bar can redraw on events rather
                               than on a fixed tick:
                                 while true; do render; treadmill-cron wait 60; done
  treadmill-cron hold          skip the next daily increment
  treadmill-cron reset         zero the day counter
  treadmill-cron vars          show every declared variable and its value
  treadmill-cron set NAME      print what NAME is set to
  treadmill-cron set NAME VAL  set NAME (VAL may be `off` to clear it)
  treadmill-cron mode          print the active mode
  treadmill-cron mode NAME     switch to mode NAME (e.g. boost, default)
  treadmill-cron mode cycle    rotate to the next mode in the schedule
  treadmill-cron now "SPEC"    run an ad-hoc sequence now, outranking all modes
                               and entries. SPEC = 'speed incline [dur]' chunks,
                               comma-separated; last chunk may omit dur to hold.
                               speed/incline may be absolute (2.5), relative to
                               press-time (+0.5, -0.2), or the keyword speed /
                               incline (= unchanged). now also auto-restores the
                               press-time speed & incline when the last chunk ends.
                               e.g. now "2 2 10m, 3 2"
                                    now "+0.5 incline 5m, -0.2 incline 2m"
  treadmill-cron snooze DUR    suppress all scheduled firing for DUR (e.g. 30m)
                               so manual `now` sessions aren't interrupted; `now`
                               still works. `snooze off` cancels; bare `snooze`
                               shows time left.
  treadmill-cron cancel        abort the routine currently running and restore
                               the speed/incline from before it started.
  treadmill-cron events        connect to the running daemon and print each
                               event (session_start, session_end, mode_changed)
                               as a JSON line, as it happens. Also durably
                               logged to ~/.config/treadmill-cron/events.jsonl
                               regardless of whether anything's subscribed.
                               treadmill-cron itself keeps no derived state
                               (session counts, time-in-mode, etc.) -- that's
                               for whatever's on the other end of this stream.
"""
import re
import socket
import subprocess
import json
import sys
import threading
import time
from datetime import datetime, date, timedelta
from pathlib import Path

CONFIG_DIR = Path.home() / '.config' / 'treadmill-cron'
SCHEDULE_FILE = CONFIG_DIR / 'schedule'
STATE_FILE = CONFIG_DIR / 'state.json'
CONFIG_FILE = CONFIG_DIR / 'config.json'
MODE_FILE = CONFIG_DIR / 'mode'
MODE_REVERT_FILE = CONFIG_DIR / 'mode_revert.json'
NOW_FILE = CONFIG_DIR / 'now'
SNOOZE_FILE = CONFIG_DIR / 'snooze'
CANCEL_FILE = CONFIG_DIR / 'cancel'
VARS_FILE = CONFIG_DIR / 'vars.json'
EVENTS_FILE = CONFIG_DIR / 'events.jsonl'
EVENTS_SOCKET = CONFIG_DIR / 'events.sock'
TICK_SECS = 2.0

# Ad-hoc `now` overrides outrank every scheduled entry and every mode.
NOW_PRIORITY = 10 ** 9

DEFAULT_CONFIG = {
    'messager': [],
    'notify_kinds': ['day'],
}


def load_config():
    cfg = dict(DEFAULT_CONFIG)
    if CONFIG_FILE.exists():
        cfg.update(json.loads(CONFIG_FILE.read_text()))
    return cfg


# ---- variables ----
#
# The schedule declares a variable (name, fallback, whether using it clears it);
# the *value* lives here, because the schedule is hand-authored config the
# daemon has no business rewriting.

def read_vars():
    try:
        return json.loads(VARS_FILE.read_text())
    except (FileNotFoundError, json.JSONDecodeError, ValueError):
        return {}


def get_var(name):
    v = read_vars().get(name)
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def set_var(name, value):
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    vars_ = read_vars()
    vars_[name] = value
    VARS_FILE.write_text(json.dumps(vars_, indent=2, sort_keys=True) + '\n')


def unset_var(name):
    vars_ = read_vars()
    if vars_.pop(name, None) is None:
        return False
    VARS_FILE.write_text(json.dumps(vars_, indent=2, sort_keys=True) + '\n')
    return True


# ---- mode ----

def read_mode():
    """Active mode name; missing/empty file means the default mode."""
    try:
        return MODE_FILE.read_text().strip() or 'default'
    except FileNotFoundError:
        return 'default'


def set_mode(m):
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    MODE_FILE.write_text((m or 'default') + '\n')


# ---- mode --for (temporary switch with auto-revert) ----
#
# One "return mode" slot, not a stack: however many `--for` calls happen
# while an override is already pending -- whatever mode they target -- the
# original return_mode is left alone. It's only captured fresh when there
# is no pending override at all. A plain (non--for) mode switch cancels
# whatever's pending, since an explicit switch is its own new baseline.

def read_mode_revert():
    try:
        return json.loads(MODE_REVERT_FILE.read_text())
    except (FileNotFoundError, json.JSONDecodeError, ValueError):
        return None


def write_mode_revert(d):
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    MODE_REVERT_FILE.write_text(json.dumps(d))


def clear_mode_revert():
    try:
        MODE_REVERT_FILE.unlink()
    except FileNotFoundError:
        pass


def schedule_modes():
    """Mode names that appear in the schedule, default first."""
    try:
        entries = parse_schedule(SCHEDULE_FILE)
    except (FileNotFoundError, ValueError):
        return ['default']
    seen = ['default']
    for e in entries:
        m = e.get('mode', 'default')
        if m not in seen:
            seen.append(m)
    return seen


# ---- snooze ----

def read_snooze_until():
    """datetime until which scheduled firing is suppressed, or None."""
    try:
        return datetime.fromisoformat(SNOOZE_FILE.read_text().strip())
    except (FileNotFoundError, ValueError):
        return None


def is_snoozed(now):
    until = read_snooze_until()
    return until is not None and now < until


# ---- events ----
#
# A record of what happened, not state: `emit_event` appends a line to
# events.jsonl (durable history) and pushes the same line to every process
# currently connected to events.sock (live push). Nothing here reads state
# back -- derived stats (sessions today, time in a mode, whatever) belong to
# whatever's on the other end of `treadmill-cron events`, not to this file.

_event_subscribers: list = []
_event_lock = threading.Lock()


def _event_server_loop():
    try:
        EVENTS_SOCKET.unlink()
    except FileNotFoundError:
        pass
    srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    srv.bind(str(EVENTS_SOCKET))
    srv.listen()
    while True:
        conn, _ = srv.accept()
        with _event_lock:
            _event_subscribers.append(conn)


def start_event_server():
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    t = threading.Thread(target=_event_server_loop, daemon=True)
    t.start()


def emit_event(event, **fields):
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    rec = {'ts': datetime.now().isoformat(timespec='seconds'), 'event': event, **fields}
    line = json.dumps(rec) + '\n'
    with open(EVENTS_FILE, 'a') as f:
        f.write(line)
    with _event_lock:
        dead = []
        for conn in _event_subscribers:
            try:
                conn.sendall(line.encode())
            except OSError:
                dead.append(conn)
        for conn in dead:
            _event_subscribers.remove(conn)
            try:
                conn.close()
            except OSError:
                pass


def cmd_events():
    """Connect to the running daemon's event socket and print each line as it
    arrives. Just a subscriber -- all it does is relay, no interpretation."""
    try:
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.connect(str(EVENTS_SOCKET))
    except OSError as e:
        print(f"treadmill-cron: can't reach event socket ({e}); is the daemon running?",
              file=sys.stderr)
        return 1
    with s, s.makefile('r') as f:
        for line in f:
            print(line, end='', flush=True)


def notify(cfg, title, body):
    cmd = cfg.get('messager') or []
    if cmd:
        subprocess.Popen([*cmd, title, body])
    else:
        print(f"MESSAGE: {title}: {body} (set messager)")


def ctl(*args) -> str:
    result = subprocess.run(['nord-ich-track', 'ctl', *args], capture_output=True, text=True)
    return result.stdout.strip()


def get_treadmill_state():
    try:
        return json.loads(ctl('get_state'))
    except (json.JSONDecodeError, ValueError):
        return {}


def is_running(treadmill):
    return treadmill.get('type') != 'no_state' and treadmill.get('speed_kph', 0) > 0


# ---- state file ----

def load_state():
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {}


def save_state(s):
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(s, indent=2))


def days_elapsed(s):
    """Global day counter (currently used for held_count only)."""
    start = s.get('start_date')
    if not start:
        return 0
    elapsed = (date.today() - date.fromisoformat(start)).days
    return max(0, elapsed - s.get('held_count', 0))


def entry_day(entry, state):
    """Per-entry day count: days since the entry's own start_date, minus global held_count."""
    sd = entry.get('start_date')
    if sd is None:
        return 0
    elapsed = (date.today() - sd).days
    return max(0, elapsed - state.get('held_count', 0))


# ---- ramp parsing ----

def parse_ramp_number(s):
    m = re.fullmatch(r'(-?\d+(?:\.\d+)?)(?:\+(-?\d+(?:\.\d+)?)/(day|week))?', s)
    if not m:
        raise ValueError(f"bad number: {s}")
    delta = float(m.group(2) or 0)
    if m.group(3) == 'week':
        delta /= 7
    return float(m.group(1)), delta


def parse_time_delta(num, unit):
    return float(num) * (60 if unit == 'min' else 1)


def parse_mm_ss(s):
    m = re.fullmatch(r'(\d+):(\d{2})', s)
    if not m:
        raise ValueError(f"bad MM:SS: {s}")
    return int(m.group(1)) * 60 + int(m.group(2))


def parse_duration(s):
    """'10m' / '600s' / '1h' / bare seconds -> int seconds."""
    m = re.fullmatch(r'(\d+(?:\.\d+)?)(s|m|min|h)?', s)
    if not m:
        raise ValueError(f"bad duration: {s}")
    unit = {'s': 1, 'm': 60, 'min': 60, 'h': 3600}[m.group(2) or 's']
    return int(float(m.group(1)) * unit)


def creep_window_open(window, now):
    """Is `now` inside the creep entry's clock window? None window = always."""
    if window is None:
        return True
    if window['kind'] == 'hourly':
        cur = now.minute * 60 + now.second
        return window['start_secs'] <= cur < window['end_secs']
    if window['kind'] == 'absolute':
        cur = now.hour * 3600 + now.minute * 60 + now.second
        start = window['start_h'] * 3600 + window['start_m'] * 60
        end = window['end_h'] * 3600 + window['end_m'] * 60
        return start <= cur < end
    return False


# ---- schedule parsing ----

def _parse_time_range(time_range):
    m = re.fullmatch(r':(\d{1,2})(?::(\d{2}))?-:(\d{1,2})(?::(\d{2}))?', time_range)
    if m:
        return {
            'kind': 'hourly',
            'start_secs': int(m.group(1)) * 60 + int(m.group(2) or 0),
            'end_secs':   int(m.group(3)) * 60 + int(m.group(4) or 0),
        }
    m = re.fullmatch(r'(\d{1,2}):(\d{2})-(\d{1,2}):(\d{2})', time_range)
    if m:
        return {
            'kind': 'absolute',
            'start_h': int(m.group(1)), 'start_m': int(m.group(2)),
            'end_h':   int(m.group(3)), 'end_m':   int(m.group(4)),
        }
    m = re.fullmatch(
        r'day\+(\d+):(\d{2})-day\+(\d+):(\d{2})'
        r'(?:\+(\d+(?:\.\d+)?)(min|s|sec)/day)?',
        time_range,
    )
    if m:
        end_delta = parse_time_delta(m.group(5), m.group(6)) if m.group(5) else 0
        return {
            'kind': 'day',
            'start_offset': int(m.group(1)) * 60 + int(m.group(2)),
            'end_offset':   int(m.group(3)) * 60 + int(m.group(4)),
            'end_delta_per_day': end_delta,
        }
    return None


def _field_has_ramp(base, delta):
    # A variable's fallback can itself ramp, and that still needs a start date.
    if isinstance(base, dict):
        return base['default_delta'] != 0
    return delta != 0


def _entry_has_ramp(entry):
    fields = [(entry['speed_base'], entry['speed_delta']),
              (entry['incline_base'], entry['incline_delta'])]
    for c in entry.get('continuations', []):
        fields.append((c['speed_base'], c['speed_delta']))
        fields.append((c['incline_base'], c['incline_delta']))
    if any(_field_has_ramp(b, d) for b, d in fields):
        return True
    return entry.get('end_delta_per_day', 0) != 0


def _parse_creep(first_parts):
    """Parse `TIME_RANGE creep interval=.. step=.. max=..` (TIME_RANGE may be `*`)."""
    time_tok = first_parts[0]
    if time_tok in ('*', 'always'):
        window = None
    else:
        window = _parse_time_range(time_tok)
        if not window or window['kind'] == 'day':
            raise ValueError(f"creep time must be a clock window or *, got: {time_tok!r}")

    entry = {'kind': 'creep', 'window': window,
             'interval_secs': 600, 'step': 0.1, 'max': 3.0,
             'priority': None, 'start_date': None}
    for tok in first_parts[2:]:
        if '=' not in tok:
            raise ValueError(f"creep param needs key=value: {tok!r}")
        k, v = tok.split('=', 1)
        if k == 'interval':
            entry['interval_secs'] = parse_duration(v)
        elif k == 'step':
            entry['step'] = float(v)
        elif k == 'max':
            entry['max'] = float(v)
        else:
            raise ValueError(f"unknown creep param: {k!r}")
    return entry


def parse_var_decl(line):
    """`var NAME default=VALUE [consume]` -> (name, decl).

    `default=` is what the variable falls back to while nothing is set, and may
    be the bare keyword `speed`/`incline` to mean 'leave that axis alone'.
    `consume` clears the value once a session has actually used it, so a value
    applies to the next session only and can never ambush a later one.
    """
    parts = line.split()
    if len(parts) < 2:
        raise ValueError("var needs a name, e.g. var challenge default=speed")
    name = parts[1]
    decl = {'name': name, 'default': None, 'consume': False}
    for tok in parts[2:]:
        if tok.startswith('default='):
            decl['default'] = tok.split('=', 1)[1]
        elif tok == 'consume':
            decl['consume'] = True
        else:
            raise ValueError(f"unknown var option {tok!r}")
    return name, decl


def parse_ramp_or_keep(s, keyword, var_decls=None):
    """A schedule speed/incline field: a number (optionally ramped), the bare
    keyword ('speed'/'incline') meaning leave that axis where the walker put it,
    or `$name` for a declared variable.

    'Keep' is carried as a None base all the way to the daemon, which then
    simply doesn't send that axis -- and doesn't restore it afterwards either.
    A variable is carried as a dict base, resolved afresh on every read so that
    setting one takes effect without restarting the daemon.
    """
    if s == keyword:
        return None, 0.0
    if s.startswith('$'):
        name = s[1:]
        decl = (var_decls or {}).get(name)
        if decl is None:
            raise ValueError(f"${name} is not declared "
                             f"(add: var {name} default={keyword})")
        default = decl['default']
        if default is None or default == keyword:
            d_base, d_delta = None, 0.0
        else:
            d_base, d_delta = parse_ramp_number(default)
        return {'var': name, 'consume': decl['consume'],
                'default_base': d_base, 'default_delta': d_delta}, 0.0
    return parse_ramp_number(s)


def parse_flags(spec):
    """Parse 'blah,other,hello=world' into {'blah': True, 'other': True,
    'hello': 'world'}. Each comma-separated item splits on the *first* '='
    only, so a value may itself contain '=' without needing to be quoted."""
    flags = {}
    for item in spec.split(','):
        item = item.strip()
        if not item:
            continue
        if '=' in item:
            k, v = item.split('=', 1)
            flags[k] = v
        else:
            flags[item] = True
    return flags


def parse_entry(line, var_decls=None):
    parts = line.split()
    if not parts:
        raise ValueError("empty entry")

    priority = None
    start_date = None
    mode = 'default'
    flags = {}

    while parts and (parts[0].startswith('priority=')
                     or parts[0].startswith('start=')
                     or parts[0].startswith('flag=')
                     or parts[0].startswith('@')):
        tok = parts.pop(0)
        if tok.startswith('priority='):
            priority = int(tok.split('=', 1)[1])
        elif tok.startswith('start='):
            start_date = date.fromisoformat(tok.split('=', 1)[1])
        elif tok.startswith('flag='):
            flags = parse_flags(tok.split('=', 1)[1])
        elif tok.startswith('@'):
            mode = tok[1:]
            if not mode:
                raise ValueError("@ needs a mode name, e.g. @boost")

    rejoined = ' '.join(parts)
    chunk_strs = [c.strip() for c in rejoined.split(',')]

    first_parts = chunk_strs[0].split()

    if len(first_parts) >= 2 and first_parts[1] == 'creep':
        if len(chunk_strs) > 1:
            raise ValueError("creep entry takes no continuations")
        creep = _parse_creep(first_parts)
        creep['mode'] = mode
        creep['flags'] = flags
        return creep

    if len(first_parts) != 3:
        raise ValueError(f"first chunk needs 3 fields (time speed incline), got: {first_parts}")
    time_range, speed_s, incline_s = first_parts

    tr = _parse_time_range(time_range)
    if not tr:
        raise ValueError(f"unrecognized time range: {time_range!r}")

    speed_base, speed_delta = parse_ramp_or_keep(speed_s, 'speed', var_decls)
    incline_base, incline_delta = parse_ramp_or_keep(incline_s, 'incline', var_decls)

    entry = {
        **tr,
        'priority': priority,
        'start_date': start_date,
        'mode': mode,
        'flags': flags,
        'speed_base': speed_base, 'speed_delta': speed_delta,
        'incline_base': incline_base, 'incline_delta': incline_delta,
        'continuations': [],
    }

    for chunk_str in chunk_strs[1:]:
        cparts = chunk_str.split()
        if len(cparts) != 3:
            raise ValueError(f"continuation needs 3 fields: {chunk_str!r}")
        ctr, csp, cinc = cparts
        m = re.fullmatch(r'now-now\+(\d+:\d{2})', ctr)
        if not m:
            raise ValueError(f"continuation time must be now-now+MM:SS, got: {ctr!r}")
        duration = parse_mm_ss(m.group(1))
        csp_b, csp_d = parse_ramp_or_keep(csp, 'speed', var_decls)
        cinc_b, cinc_d = parse_ramp_or_keep(cinc, 'incline', var_decls)
        entry['continuations'].append({
            'duration_secs': duration,
            'speed_base': csp_b, 'speed_delta': csp_d,
            'incline_base': cinc_b, 'incline_delta': cinc_d,
        })

    if _entry_has_ramp(entry) and entry['start_date'] is None:
        raise ValueError("entry has ramp but no start=YYYY-MM-DD")

    return entry


def parse_schedule(path):
    lines = []
    for lineno, line in enumerate(path.read_text().splitlines(), start=1):
        line = line.split('#')[0].strip()
        if line:
            lines.append((lineno, line))

    # Declarations first, in a pass of their own, so a `var` line may sit either
    # side of the entries that use it -- convention is directly above them.
    var_decls = {}
    for lineno, line in lines:
        if line.split()[0] == 'var':
            try:
                name, decl = parse_var_decl(line)
            except ValueError as err:
                raise ValueError(f"{path}:{lineno}: {err}") from err
            var_decls[name] = decl

    entries = []
    for lineno, line in lines:
        if line.split()[0] == 'var':
            continue
        try:
            entries.append(parse_entry(line, var_decls))
        except ValueError as err:
            raise ValueError(f"{path}:{lineno}: {err}") from err
    return entries


# ---- effective values ----

def resolve_value(base, delta, day):
    """Effective number for a speed/incline field, or None for 'keep'.

    Variables are looked up on every call rather than at parse time, so setting
    one takes effect on the next tick without restarting the daemon. An unset
    variable falls back to its declared default, which may itself be 'keep'.
    """
    if base is None:
        return None
    if isinstance(base, dict):
        value = get_var(base['var'])
        if value is not None:
            return value
        if base['default_base'] is None:
            return None
        return base['default_base'] + base['default_delta'] * day
    return base + delta * day


def eff_speed(e, day):
    return resolve_value(e['speed_base'], e['speed_delta'], day)


def eff_incline(e, day):
    return resolve_value(e['incline_base'], e['incline_delta'], day)


def eff_end_offset(e, day):
    return int(e['end_offset'] + e.get('end_delta_per_day', 0) * day)


def first_chunk_duration(entry, day):
    """Full duration of the first chunk (absent partial-window adjustment)."""
    if entry['kind'] == 'hourly':
        return (entry['end_secs'] - entry['start_secs']) % 3600 or 3600
    if entry['kind'] == 'absolute':
        s = entry['start_h'] * 3600 + entry['start_m'] * 60
        e = entry['end_h']   * 3600 + entry['end_m']   * 60
        return (e - s) % 86400 or 86400
    if entry['kind'] == 'day':
        return eff_end_offset(entry, day) - entry['start_offset']
    return 0


def chunks_for(entry, state, first_duration_override=None):
    """List of {duration_secs, speed, incline} for the entry's full sequence."""
    day = entry_day(entry, state)
    chunks = []
    dur = first_duration_override if first_duration_override is not None else first_chunk_duration(entry, day)
    chunks.append({
        'duration_secs': dur,
        'speed': eff_speed(entry, day),
        'incline': eff_incline(entry, day),
    })
    for c in entry.get('continuations', []):
        chunks.append({
            'duration_secs': c['duration_secs'],
            'speed': resolve_value(c['speed_base'], c['speed_delta'], day),
            'incline': resolve_value(c['incline_base'], c['incline_delta'], day),
        })
    return chunks


# ---- cumulative belt-time tracking ----

def update_cumulative(state, treadmill, increment_secs):
    today = date.today().isoformat()
    if state.get('cum_run_date') != today:
        state['cum_run_date'] = today
        state['cum_run_secs'] = 0
        save_state(state)
    if is_running(treadmill):
        state['cum_run_secs'] = state.get('cum_run_secs', 0) + increment_secs
        save_state(state)


def fired_today(state, e):
    return state.get('last_fired', {}).get(_entry_key(e)) == date.today().isoformat()


def mark_fired(state, e):
    state.setdefault('last_fired', {})[_entry_key(e)] = date.today().isoformat()
    save_state(state)


def _entry_key(e):
    if e['kind'] == 'day':
        return f"day:{e['start_offset']}"
    if e['kind'] == 'hourly':
        return f"hourly:{e['start_secs']}-{e['end_secs']}"
    if e['kind'] == 'absolute':
        return f"abs:{e['start_h']}:{e['start_m']}"
    return repr(e)


# ---- readiness check ----

def is_ready_now(entry, now, state):
    """If the first chunk's window is open now, return remaining seconds. Else None."""
    if entry['kind'] == 'hourly':
        cur = now.minute * 60 + now.second
        start, end = entry['start_secs'], entry['end_secs']
        if start <= cur < end:
            return end - cur
        return None
    if entry['kind'] == 'absolute':
        cur = now.hour * 3600 + now.minute * 60 + now.second
        start = entry['start_h'] * 3600 + entry['start_m'] * 60
        end = entry['end_h']   * 3600 + entry['end_m']   * 60
        if start <= cur < end:
            return end - cur
        return None
    if entry['kind'] == 'day':
        if fired_today(state, entry):
            return None
        cum = state.get('cum_run_secs', 0)
        if cum < entry['start_offset']:
            return None
        day = entry_day(entry, state)
        dur = eff_end_offset(entry, day) - entry['start_offset']
        if dur <= 0:
            return None
        return dur
    return None


def priority_lt(a, b):
    """Is priority a strictly lower than priority b? None < any int."""
    if a is None and b is None:
        return False
    if a is None:
        return True
    if b is None:
        return False
    return a < b


def _note(prev, msg):
    """Print `msg` only when it differs from the last note; return it."""
    if msg != prev:
        print(f"treadmill-cron: {msg}")
    return msg


# ---- daemon ----

def consumable_vars(entry):
    """Names of `consume` variables this entry reads, for clearing once it runs."""
    bases = [entry.get('speed_base'), entry.get('incline_base')]
    for c in entry.get('continuations', []):
        bases += [c.get('speed_base'), c.get('incline_base')]
    return [b['var'] for b in bases if isinstance(b, dict) and b['consume']]


def apply_chunk(chunk):
    """Send a chunk's axes to the treadmill. A None axis means 'keep' -- it is
    left exactly where the walker has it."""
    if chunk['speed'] is not None:
        ctl('speed', str(chunk['speed']))
    if chunk['incline'] is not None:
        ctl('incline', str(chunk['incline']))


def fmt_axis(v, unit):
    return 'keep' if v is None else f"{v:.1f}{unit}"


def daemon():
    schedule_path = Path(sys.argv[1]) if len(sys.argv) > 1 else SCHEDULE_FILE
    print(f"treadmill-cron: watching {schedule_path}")
    start_event_server()

    cfg = load_config()
    state = load_state()
    if 'start_date' not in state:
        state['start_date'] = date.today().isoformat()
        save_state(state)

    last_tick = time.monotonic()
    prev_mode = read_mode()

    running_entry = None
    remaining_chunks: list = []
    chunk_end_mono = None
    prev_speed = None
    prev_incline = None
    last_announced_evt = None
    creep_accum = 0.0       # belt-time accrued toward the next creep nudge
    creep_note = None       # dedupes the "at max" log line
    # Which axes the running entry drives; only these get restored when it ends.
    touched_speed = True
    touched_incline = True

    def stop_running(restore: bool, reason: str = ''):
        # Restore only the axes this entry actually drove: an axis it left on
        # 'keep' may have been adjusted by hand mid-interval, and putting it
        # back to the pre-entry value would undo that.
        nonlocal running_entry, remaining_chunks, chunk_end_mono
        if running_entry is None:
            return
        if restore and prev_speed is not None:
            tm = get_treadmill_state()
            if is_running(tm):
                if touched_speed:
                    ctl('speed', str(prev_speed))
                if touched_incline:
                    ctl('incline', str(prev_incline))
        emit_event('session_end', kind=running_entry.get('kind'),
                   mode=running_entry.get('mode'), reason=reason,
                   flags=sorted((running_entry.get('flags') or {}).keys()))
        running_entry = None
        remaining_chunks = []
        chunk_end_mono = None

    while True:
        try:
            entries = parse_schedule(schedule_path)
        except FileNotFoundError:
            print(f"treadmill-cron: schedule {schedule_path} not found")
            return

        treadmill = get_treadmill_state()
        now_mono = time.monotonic()
        dt = now_mono - last_tick
        update_cumulative(state, treadmill, dt)
        last_tick = now_mono
        now = datetime.now()

        pending_revert = read_mode_revert()
        if pending_revert and now >= datetime.fromisoformat(pending_revert['revert_at']):
            return_mode = pending_revert['return_mode']
            print(f"treadmill-cron: mode --for expired, reverting to {return_mode}")
            clear_mode_revert()
            set_mode(return_mode)

        active_mode = read_mode()
        if active_mode != prev_mode:
            emit_event('mode_changed', mode=active_mode, previous=prev_mode)
            prev_mode = active_mode
        snoozed = is_snoozed(now)

        # Cancel: abort the current routine, restoring the pre-routine speed/incline.
        if CANCEL_FILE.exists():
            try:
                CANCEL_FILE.unlink()
            except FileNotFoundError:
                pass
            if running_entry is not None:
                print(f"treadmill-cron: cancelled {running_entry.get('kind')}")
                stop_running(restore=True, reason='cancelled')

        # Treadmill stopped while running an entry -> abort, no restore
        if running_entry and not is_running(treadmill):
            print("treadmill-cron: treadmill stopped, aborting current entry")
            emit_event('session_end', kind=running_entry.get('kind'),
                       mode=running_entry.get('mode'), reason='treadmill_stopped',
                       flags=sorted((running_entry.get('flags') or {}).keys()))
            running_entry = None
            remaining_chunks = []
            chunk_end_mono = None

        # Active mode switched away from the running entry's mode -> hand off:
        # restore the walking speed; the new mode's entries take over below.
        # A `now` override is mode-independent and is never aborted here.
        if (running_entry and running_entry['kind'] != 'now'
                and running_entry.get('mode', 'default') != active_mode):
            print(f"treadmill-cron: mode -> {active_mode}, aborting "
                  f"{_entry_key(running_entry)}")
            stop_running(restore=True, reason='mode_changed')

        # Snooze suppresses all scheduled firing for a window; `now` still rules.
        # If a scheduled entry is running when snooze begins, hand it back.
        if snoozed and running_entry is not None and running_entry['kind'] != 'now':
            print("treadmill-cron: snoozed, aborting scheduled entry")
            stop_running(restore=True, reason='snoozed')

        # Ad-hoc `now` override: consume the file and run it immediately at top
        # priority, preempting whatever is scheduled. Waits for the belt to be
        # moving; holds until it stops, the sequence ends, or another `now` lands.
        if NOW_FILE.exists() and is_running(treadmill):
            try:
                now_chunks = json.loads(NOW_FILE.read_text()).get('chunks', [])
            except (json.JSONDecodeError, ValueError):
                now_chunks = []
            try:
                NOW_FILE.unlink()
            except FileNotFoundError:
                pass
            if now_chunks:
                if running_entry is not None:
                    stop_running(restore=False, reason='preempted_by_now')
                prev_speed = treadmill.get('speed_kph', 0)
                prev_incline = treadmill.get('incline_pct', 0)
                # Resolve tokens (absolute / +/-delta / 'speed'|'incline' keyword)
                # against the speed & incline at this moment.
                remaining_chunks = [{
                    'speed': max(0.0, round(resolve_now_value(c['speed'], prev_speed), 1)),
                    'incline': max(0.0, round(resolve_now_value(c['incline'], prev_incline), 1)),
                    'duration_secs': c['duration_secs'],
                } for c in now_chunks]
                running_entry = {'kind': 'now', 'priority': NOW_PRIORITY, 'mode': None}
                touched_speed = touched_incline = True
                chunk = remaining_chunks[0]
                apply_chunk(chunk)
                dur = chunk['duration_secs']
                chunk_end_mono = (now_mono + dur) if dur is not None else None
                emit_event('session_start', kind='now', mode=None, flags=[])
                print(f"treadmill-cron: now {fmt_axis(chunk['speed'], ' kph')} "
                      f"{fmt_axis(chunk['incline'], '%')} "
                      f"({'hold' if dur is None else str(dur) + 's'})")

        # Advance current chunk if its time is up
        if running_entry and chunk_end_mono is not None and now_mono >= chunk_end_mono:
            remaining_chunks.pop(0)
            if remaining_chunks:
                chunk = remaining_chunks[0]
                apply_chunk(chunk)
                cdur = chunk['duration_secs']
                chunk_end_mono = (now_mono + cdur) if cdur is not None else None
                print(f"treadmill-cron: chunk -> {fmt_axis(chunk['speed'], ' kph')} "
                      f"{fmt_axis(chunk['incline'], '%')} for "
                      f"{'hold' if cdur is None else str(cdur) + 's'}")
            else:
                if running_entry['kind'] == 'day':
                    mark_fired(state, running_entry)
                stop_running(restore=True, reason='completed')

        # Find best candidate to fire now (suppressed entirely while snoozed)
        best = None  # (priority_sort_key, entry, remaining_secs)
        if not snoozed:
            for e in entries:
                if e.get('mode', 'default') != active_mode:
                    continue
                rem = is_ready_now(e, now, state)
                if rem is None:
                    continue
                ep = e.get('priority')
                key = ep if ep is not None else float('-inf')
                if best is None or key > best[0]:
                    best = (key, e, rem)

        if best:
            _, candidate, rem = best
            cp = candidate.get('priority')
            should_start = False
            if running_entry is None:
                should_start = True
            elif candidate is not running_entry:
                rp = running_entry.get('priority')
                if priority_lt(rp, cp):
                    should_start = True

            if should_start and is_running(treadmill):
                if running_entry is not None:
                    print(f"treadmill-cron: preempting {_entry_key(running_entry)}")
                    stop_running(restore=False, reason='preempted')

                chunks = chunks_for(candidate, state, first_duration_override=rem)
                running_entry = candidate
                remaining_chunks = chunks
                prev_speed = treadmill.get('speed_kph', 0)
                prev_incline = treadmill.get('incline_pct', 0)
                touched_speed = any(c['speed'] is not None for c in chunks)
                touched_incline = any(c['incline'] is not None for c in chunks)
                chunk = chunks[0]
                apply_chunk(chunk)
                chunk_end_mono = now_mono + chunk['duration_secs']
                tag = candidate['kind']
                emit_event('session_start', kind=tag, mode=candidate.get('mode'),
                           priority=cp,
                           flags=sorted((candidate.get('flags') or {}).keys()))
                print(f"treadmill-cron: start {tag} (p={cp}) "
                      f"{fmt_axis(chunk['speed'], ' kph')} "
                      f"{fmt_axis(chunk['incline'], '%')} "
                      f"for {chunk['duration_secs']}s "
                      f"(was {prev_speed:.1f} kph {prev_incline:.1f}%)")
                # Clear one-shot variables only now the session has really
                # begun -- a window that passed with the belt stopped leaves
                # the value armed for next time.
                for name in consumable_vars(candidate):
                    if unset_var(name):
                        print(f"treadmill-cron: consumed ${name}")
                if tag in cfg.get('notify_kinds', []):
                    notify(cfg, f"treadmill: {tag}",
                           f"{fmt_axis(chunk['speed'], ' kph')} "
                           f"{fmt_axis(chunk['incline'], '%')} "
                           f"for {chunk['duration_secs']}s")
                last_announced_evt = None

        # Light status when idle
        if not running_entry:
            if snoozed:
                until = read_snooze_until()
                evt = f"snoozed until {until.strftime('%H:%M:%S')}" if until else None
            else:
                nxt = _next_announce(
                    [e for e in entries if e.get('mode', 'default') == active_mode],
                    now, state)
                evt = f"next {nxt}" if nxt else None
            if evt and evt != last_announced_evt:
                print(f"treadmill-cron: {evt}")
                last_announced_evt = evt

        # Creep: gentle upward pressure during free walking. Lowest priority --
        # only acts when no entry is running, while moving, inside a creep
        # window. Accumulator only advances while creeping, so stepping off or
        # leaving a window pauses (doesn't restart) the climb. Climbs from the
        # measured speed, so a manual slow-down lowers where the next nudge starts.
        active_creep = next(
            (c for c in entries
             if c['kind'] == 'creep'
             and c.get('mode', 'default') == active_mode
             and creep_window_open(c['window'], now)),
            None)
        if (running_entry is None and is_running(treadmill)
                and active_creep is not None and not snoozed):
            creep_accum += dt
            if creep_accum >= active_creep['interval_secs']:
                creep_accum = 0.0
                cur = round(treadmill.get('speed_kph', 0), 1)
                ceil = active_creep['max']
                new = round(min(ceil, cur + active_creep['step']), 1)
                if new > cur:
                    ctl('speed', str(new))
                    print(f"treadmill-cron: creep {cur:.1f} -> {new:.1f} kph")
                    creep_note = None
                else:
                    creep_note = _note(creep_note, f"creep at max {ceil:.1f} kph")

        time.sleep(TICK_SECS)


def _next_announce(entries, now, state):
    """Loose preview of the next scheduled event, for logging only."""
    best = None
    for e in entries:
        day = entry_day(e, state)
        if e['kind'] == 'hourly':
            cur = now.minute * 60 + now.second
            wait = (e['start_secs'] - cur) % 3600
            cand = now + timedelta(seconds=wait)
        elif e['kind'] == 'absolute':
            cand = now.replace(hour=e['start_h'], minute=e['start_m'],
                               second=0, microsecond=0)
            if cand < now:
                cand += timedelta(days=1)
        elif e['kind'] == 'day':
            if fired_today(state, e):
                continue
            need = e['start_offset'] - state.get('cum_run_secs', 0)
            if need <= 0:
                continue
            cand = None  # cumulative-driven; no wall-clock prediction
        else:
            continue
        if cand is None:
            label = f"day(p={e.get('priority')}) need {need:.0f}s more belt-time"
        else:
            label = (f"{e['kind']}(p={e.get('priority')}) at "
                     f"{cand.strftime('%H:%M:%S')} "
                     f"{_fmt_pair(eff_speed(e, day), eff_incline(e, day))}")
        if best is None or (cand is not None and (best[0] is None or cand < best[0])):
            best = (cand, label)
    return best[1] if best else None


# ---- subcommands ----

def fmt_secs(s):
    return f"{s//60:02d}:{s%60:02d}"


def _fmt_pair(sp, inc):
    """speed/incline for display; a 'keep' axis shows as `keep` not a number."""
    sp_s = 'keep' if sp is None else f"{sp:.2f} kph"
    inc_s = 'keep' if inc is None else f"{inc:.2f}%"
    return f"{sp_s}  {inc_s}"


def cmd_status():
    s = load_state()
    entries = parse_schedule(SCHEDULE_FILE)
    print(f"active mode:       {read_mode()}  (modes: {', '.join(schedule_modes())})")
    _su = read_snooze_until()
    if _su and datetime.now() < _su:
        print(f"snoozed until:     {_su.strftime('%H:%M:%S')}")
    print(f"daemon start_date: {s.get('start_date', '?')}")
    if s.get('held_count'):
        print(f"held:              {s['held_count']} day(s) skipped")
    cum = s.get('cum_run_secs', 0)
    if s.get('cum_run_date') == date.today().isoformat():
        print(f"belt-time today:   {cum:.0f}s ({cum/60:.1f} min)")
    if s.get('last_fired'):
        print(f"last_fired:        {s['last_fired']}")
    print()
    for e in entries:
        if e['kind'] == 'creep':
            win = e['window']
            if win is None:
                wstr = '*'
            elif win['kind'] == 'hourly':
                wstr = f":{fmt_secs(win['start_secs'])}-:{fmt_secs(win['end_secs'])}"
            else:
                wstr = (f"{win['start_h']:02d}:{win['start_m']:02d}-"
                        f"{win['end_h']:02d}:{win['end_m']:02d}")
            mstr = f"  @{e.get('mode')}" if e.get('mode', 'default') != 'default' else ''
            print(f"  creep   {wstr:<13s}    +{e['step']} kph / "
                  f"{e['interval_secs']}s  ->  max {e['max']:.2f} kph{mstr}")
            continue
        day = entry_day(e, s)
        vals = _fmt_pair(eff_speed(e, day), eff_incline(e, day))
        prio = e.get('priority')
        prio_str = f"p={prio}" if prio is not None else "p=-"
        sd = e.get('start_date')
        sd_str = f" since {sd} (day {day})" if sd else ""
        if e['kind'] == 'hourly':
            base = (f"  hourly  :{fmt_secs(e['start_secs'])}-:{fmt_secs(e['end_secs'])}    "
                    f"{vals}")
        elif e['kind'] == 'absolute':
            base = (f"  abs     {e['start_h']:02d}:{e['start_m']:02d}-"
                    f"{e['end_h']:02d}:{e['end_m']:02d}    {vals}")
        elif e['kind'] == 'day':
            end_off = eff_end_offset(e, day)
            dur = end_off - e['start_offset']
            fired = ' [fired today]' if fired_today(s, e) else ''
            base = (f"  day     day+{fmt_secs(e['start_offset'])}-day+{fmt_secs(end_off)}    "
                    f"({dur}s)    {vals}{fired}")
        else:
            continue
        cont_str = ''
        for c in e.get('continuations', []):
            cs = (None if c['speed_base'] is None
                  else c['speed_base'] + c['speed_delta'] * day)
            ci = (None if c['incline_base'] is None
                  else c['incline_base'] + c['incline_delta'] * day)
            cont_str += f", +{c['duration_secs']}s @ {_fmt_pair(cs, ci)}"
        mode_str = f"@{e.get('mode')} " if e.get('mode', 'default') != 'default' else ''
        print(f"{base}    [{mode_str}{prio_str}{sd_str}]{cont_str}")


def cmd_hold():
    s = load_state()
    s['held_count'] = s.get('held_count', 0) + 1
    save_state(s)
    print(f"treadmill-cron: held. day = {days_elapsed(s)}")


def cmd_reset():
    s = load_state()
    s['start_date'] = date.today().isoformat()
    s['held_count'] = 0
    s.pop('last_fired', None)
    s.pop('cum_run_date', None)
    s.pop('cum_run_secs', None)
    save_state(s)
    print("treadmill-cron: reset. day = 0, cumulative cleared")


def cmd_mode(arg=None, for_arg=None):
    if arg is None and for_arg is None:
        print(read_mode())
        return

    known = schedule_modes()

    if for_arg is None:
        # Plain switch: permanent (until the next explicit switch), and it
        # cancels whatever temporary override might be pending, since an
        # explicit switch is its own new baseline.
        if arg == 'cycle':
            cur = read_mode()
            i = known.index(cur) if cur in known else 0
            arg = known[(i + 1) % len(known)]
        clear_mode_revert()
        set_mode(arg)
        if arg not in known:
            print(f"treadmill-cron: warning: nothing tagged '@{arg}' in schedule "
                  f"(known: {', '.join(known)})", file=sys.stderr)
        notify(load_config(), "treadmill mode", arg)
        print(arg)
        return

    # --for: temporary switch, auto-reverting to return_mode once it elapses.
    now = datetime.now()
    is_relative = for_arg.startswith('+')
    secs = parse_duration(for_arg[1:] if is_relative else for_arg)
    pending = read_mode_revert()

    # One return-mode slot, not a stack: keep it as-is if anything is already
    # pending (whatever mode it targets), only capture a fresh one otherwise.
    return_mode = pending['return_mode'] if pending else read_mode()

    if is_relative and pending:
        base = datetime.fromisoformat(pending['revert_at'])
        if base < now:
            base = now
    else:
        base = now
    revert_at = base + timedelta(seconds=secs)

    write_mode_revert({'return_mode': return_mode, 'revert_at': revert_at.isoformat()})
    set_mode(arg)
    rem = int((revert_at - now).total_seconds())
    notify(load_config(), "treadmill mode", f"{arg} for {rem // 60}m, then {return_mode}")
    print(f"treadmill-cron: mode -> {arg} for {rem // 60}m {rem % 60}s, "
          f"then back to {return_mode}")


def _fmt_short(secs):
    """Compact duration for the status bar: 45s, 7m, 1h04m."""
    secs = int(secs)
    if secs < 60:
        return f"{secs}s"
    if secs < 3600:
        return f"{secs // 60}m"
    return f"{secs // 3600}h{(secs % 3600) // 60:02d}m"


def _fmt_pair_short(e, state):
    """Entry's speed/incline, narrow enough for a status bar. A 'keep' axis is
    simply omitted, so an incline-only entry reads as just `6%`."""
    day = entry_day(e, state)
    sp, inc = eff_speed(e, day), eff_incline(e, day)
    bits = []
    if sp is not None:
        bits.append(f"{sp:.1f}k")
    if inc is not None:
        bits.append(f"{inc:.0f}%")
    return '/'.join(bits) if bits else '-'


def _secs_until(e, now):
    """Seconds until this entry's window next opens, or None if not clock-based."""
    if e['kind'] == 'hourly':
        cur = now.minute * 60 + now.second
        return (e['start_secs'] - cur) % 3600
    if e['kind'] == 'absolute':
        cur = now.hour * 3600 + now.minute * 60 + now.second
        start = e['start_h'] * 3600 + e['start_m'] * 60
        return (start - cur) % 86400
    return None


def bar_line():
    """One compact status-bar line: active mode + what happens next.

    Schedule-derived only -- it does not talk to the daemon, so it reports what
    *should* be happening. Returns None if the schedule is unreadable.
    """
    try:
        entries = parse_schedule(SCHEDULE_FILE)
    except (FileNotFoundError, ValueError):
        return None
    mode = read_mode()
    now = datetime.now()
    state = load_state()

    # A pending `mode --for` override appends "(return_mode in Xm)" to
    # whatever the line would otherwise say, so it reads e.g.
    # "tread default idle (hardcore in 25m)".
    revert_suffix = ''
    pending_revert = read_mode_revert()
    if pending_revert:
        revert_at = datetime.fromisoformat(pending_revert['revert_at'])
        if revert_at > now:
            revert_suffix = (f" ({pending_revert['return_mode']} in "
                              f"{_fmt_short((revert_at - now).total_seconds())})")

    su = read_snooze_until()
    if su and now < su:
        return f"tread {mode} snooze {_fmt_short((su - now).total_seconds())}{revert_suffix}"

    mine = [e for e in entries
            if e.get('mode', 'default') == mode and e['kind'] != 'creep']

    # Something already inside its window wins -- show how long it has left.
    open_now = [(rem, e) for e in mine
                for rem in [is_ready_now(e, now, state)] if rem is not None]
    if open_now:
        rem, e = min(open_now, key=lambda t: t[0])
        return f"tread {mode} {_fmt_pair_short(e, state)} {_fmt_short(rem)} left{revert_suffix}"

    upcoming = [(w, e) for e in mine
                for w in [_secs_until(e, now)] if w is not None]
    if upcoming:
        wait, e = min(upcoming, key=lambda t: t[0])
        return f"tread {mode} {_fmt_pair_short(e, state)} in {_fmt_short(wait)}{revert_suffix}"

    return f"tread {mode} idle{revert_suffix}"


def cmd_bar():
    line = bar_line()
    if line is not None:
        print(line)


def cmd_wait(timeout_s=60.0):
    """Block until the `bar` line would read differently, or until timeout.

    Lets a status bar redraw on treadmill events (a window opening or closing,
    a mode switch, a snooze) instead of on a fixed tick, while the timeout keeps
    the bar's other fields -- clock, wifi, music -- refreshing as before.
    Cheap to poll: this reads files only, it never queries the treadmill.
    """
    start = bar_line()
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        time.sleep(1.0)
        if bar_line() != start:
            return


def cmd_set(name=None, value=None):
    """Arm a variable, or show what is armed."""
    if name is None:
        return cmd_vars()
    if value is None:
        v = get_var(name)
        print(f"{v:g}" if v is not None else "unset")
        return
    if value in ('off', 'clear', 'unset', 'none'):
        print(f"{name} cleared" if unset_var(name) else f"{name} was not set")
        return
    try:
        v = float(value)
    except ValueError:
        print(f"treadmill-cron: {value!r} is not a number", file=sys.stderr)
        return
    set_var(name, v)
    notify(load_config(), "treadmill", f"{name} = {v:g}")
    print(f"{name} = {v:g}")


def cmd_vars():
    """Every variable the schedule declares, with what it would use right now."""
    try:
        parse_schedule(SCHEDULE_FILE)  # surfaces declaration errors early
    except (FileNotFoundError, ValueError) as err:
        print(f"treadmill-cron: {err}", file=sys.stderr)
        return
    decls = {}
    for line in SCHEDULE_FILE.read_text().splitlines():
        line = line.split('#')[0].strip()
        if line and line.split()[0] == 'var':
            name, decl = parse_var_decl(line)
            decls[name] = decl
    if not decls:
        print("no variables declared")
        return
    for name, decl in sorted(decls.items()):
        v = get_var(name)
        armed = f"{v:g}" if v is not None else f"unset -> {decl['default'] or 'keep'}"
        print(f"{name:<12s} {armed}{'  (consume)' if decl['consume'] else ''}")


def _check_now_token(tok, keyword):
    """A now speed/incline token: a number (absolute), +/-N (relative to the
    value at press-time), or the keyword itself (= unchanged)."""
    if tok == keyword:
        return
    if re.fullmatch(r'[+-]?\d+(?:\.\d+)?', tok):
        return
    raise ValueError(f"bad {keyword} value {tok!r}: use a number, +/-delta, or '{keyword}'")


def resolve_now_value(token, base):
    """Resolve a now token against the press-time base value.
    number -> absolute; '+0.5'/'-0.2' -> base+delta; 'speed'/'incline' -> base."""
    if isinstance(token, (int, float)):
        return float(token)
    t = str(token)
    if t in ('speed', 'incline'):
        return float(base)
    if t[0] in '+-':
        return float(base) + float(t)
    return float(t)


def parse_now_spec(spec):
    """'2 2 10m, +0.5 incline 3m, speed incline' -> chunk dicts. Tokens are kept
    raw; relative '+/-' and the 'speed'/'incline' keywords resolve at press-time
    in the daemon. Only the last chunk may omit duration."""
    parts = [c.strip() for c in spec.split(',') if c.strip()]
    if not parts:
        raise ValueError("empty now spec")
    chunks = []
    for i, p in enumerate(parts):
        toks = p.split()
        if len(toks) not in (2, 3):
            raise ValueError(f"chunk needs 'speed incline [duration]': {p!r}")
        _check_now_token(toks[0], 'speed')
        _check_now_token(toks[1], 'incline')
        if len(toks) == 3:
            dur = parse_duration(toks[2])
        elif i != len(parts) - 1:
            raise ValueError(f"only the last chunk may omit duration: {p!r}")
        else:
            dur = None
        chunks.append({'speed': toks[0], 'incline': toks[1], 'duration_secs': dur})
    return chunks


def _fmt_now(chunks):
    return ' -> '.join(
        f"{c['speed']}/{c['incline']}"
        + (' hold' if c['duration_secs'] is None else f" {c['duration_secs']}s")
        for c in chunks)


def cmd_now(spec):
    chunks = parse_now_spec(spec)
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    NOW_FILE.write_text(json.dumps({'chunks': chunks}))
    desc = _fmt_now(chunks)
    notify(load_config(), "treadmill now", desc)
    print(f"treadmill-cron: queued now: {desc}")


def cmd_snooze(arg=None):
    if arg is None:
        until = read_snooze_until()
        now = datetime.now()
        if until and now < until:
            rem = int((until - now).total_seconds())
            print(f"snoozed {rem // 60}m {rem % 60}s left "
                  f"(until {until.strftime('%H:%M:%S')})")
        else:
            print("not snoozed")
        return
    if arg in ('off', '0', 'none', 'cancel'):
        try:
            SNOOZE_FILE.unlink()
        except FileNotFoundError:
            pass
        notify(load_config(), "treadmill snooze", "off")
        print("snooze cleared")
        return
    now = datetime.now()
    if arg.startswith('+'):
        # Extend from whatever's currently remaining, not from now -- so
        # pressing a "+5m" button repeatedly actually accumulates instead of
        # just resetting to 5 minutes out each time.
        secs = parse_duration(arg[1:])
        current = read_snooze_until()
        base = current if current and current > now else now
        until = base + timedelta(seconds=secs)
    else:
        secs = parse_duration(arg)
        until = now + timedelta(seconds=secs)
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    SNOOZE_FILE.write_text(until.isoformat())
    rem = int((until - now).total_seconds())
    notify(load_config(), "treadmill snooze", f"{rem // 60}m left")
    print(f"treadmill-cron: snoozed, {rem // 60}m {rem % 60}s left "
          f"(schedule suppressed until {until.strftime('%H:%M:%S')})")


def cmd_cancel():
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    CANCEL_FILE.write_text('1')
    notify(load_config(), "treadmill", "cancel")
    print("treadmill-cron: cancel requested")


def main():
    if len(sys.argv) >= 2 and sys.argv[1] in ('status', 'hold', 'reset', 'bar', 'events'):
        return {'status': cmd_status, 'hold': cmd_hold, 'reset': cmd_reset,
                'bar': cmd_bar, 'events': cmd_events}[sys.argv[1]]()
    if len(sys.argv) >= 2 and sys.argv[1] == 'wait':
        return cmd_wait(float(sys.argv[2]) if len(sys.argv) >= 3 else 60.0)
    if len(sys.argv) >= 2 and sys.argv[1] == 'vars':
        return cmd_vars()
    if len(sys.argv) >= 2 and sys.argv[1] == 'set':
        return cmd_set(sys.argv[2] if len(sys.argv) >= 3 else None,
                       sys.argv[3] if len(sys.argv) >= 4 else None)
    if len(sys.argv) >= 2 and sys.argv[1] == 'mode':
        mode_args = sys.argv[2:]
        for_arg = None
        if '--for' in mode_args:
            i = mode_args.index('--for')
            for_arg = mode_args[i + 1] if i + 1 < len(mode_args) else None
            mode_args = mode_args[:i] + mode_args[i + 2:]
        return cmd_mode(mode_args[0] if mode_args else None, for_arg)
    if len(sys.argv) >= 2 and sys.argv[1] == 'now':
        return cmd_now(' '.join(sys.argv[2:]))
    if len(sys.argv) >= 2 and sys.argv[1] == 'snooze':
        return cmd_snooze(sys.argv[2] if len(sys.argv) >= 3 else None)
    if len(sys.argv) >= 2 and sys.argv[1] == 'cancel':
        return cmd_cancel()
    daemon()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\ntreadmill-cron: stopped")
