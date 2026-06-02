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

Ramping (any number can take +delta/day or +delta/week):
  3.0+0.05/day           +0.05 each day since start_date
  3.5+0.1/week           +0.1/7 per day
  day+120:00-day+123:00+30s/day    end-time grows by 30s/day

Priority:
  No priority -> won't preempt anything; if it would overlap something running, skip.
  priority=N (integer) -> higher wins; preempts a running lower-priority entry.

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

Subcommands:
  treadmill-cron status   show effective values for today
  treadmill-cron hold     skip the next daily increment
  treadmill-cron reset    zero the day counter
"""
import re
import subprocess
import json
import sys
import time
from datetime import datetime, date, timedelta
from pathlib import Path

CONFIG_DIR = Path.home() / '.config' / 'treadmill-cron'
SCHEDULE_FILE = CONFIG_DIR / 'schedule'
STATE_FILE = CONFIG_DIR / 'state.json'
CONFIG_FILE = CONFIG_DIR / 'config.json'
TICK_SECS = 2.0

DEFAULT_CONFIG = {
    'messager': [],
    'notify_kinds': ['day'],
}


def load_config():
    cfg = dict(DEFAULT_CONFIG)
    if CONFIG_FILE.exists():
        cfg.update(json.loads(CONFIG_FILE.read_text()))
    return cfg


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


def _entry_has_ramp(entry):
    if entry['speed_delta'] != 0 or entry['incline_delta'] != 0:
        return True
    if entry.get('end_delta_per_day', 0) != 0:
        return True
    for c in entry.get('continuations', []):
        if c['speed_delta'] != 0 or c['incline_delta'] != 0:
            return True
    return False


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


def parse_entry(line):
    parts = line.split()
    if not parts:
        raise ValueError("empty entry")

    priority = None
    start_date = None

    while parts and (parts[0].startswith('priority=') or parts[0].startswith('start=')):
        tok = parts.pop(0)
        if tok.startswith('priority='):
            priority = int(tok.split('=', 1)[1])
        elif tok.startswith('start='):
            start_date = date.fromisoformat(tok.split('=', 1)[1])

    rejoined = ' '.join(parts)
    chunk_strs = [c.strip() for c in rejoined.split(',')]

    first_parts = chunk_strs[0].split()

    if len(first_parts) >= 2 and first_parts[1] == 'creep':
        if len(chunk_strs) > 1:
            raise ValueError("creep entry takes no continuations")
        return _parse_creep(first_parts)

    if len(first_parts) != 3:
        raise ValueError(f"first chunk needs 3 fields (time speed incline), got: {first_parts}")
    time_range, speed_s, incline_s = first_parts

    tr = _parse_time_range(time_range)
    if not tr:
        raise ValueError(f"unrecognized time range: {time_range!r}")

    speed_base, speed_delta = parse_ramp_number(speed_s)
    incline_base, incline_delta = parse_ramp_number(incline_s)

    entry = {
        **tr,
        'priority': priority,
        'start_date': start_date,
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
        csp_b, csp_d = parse_ramp_number(csp)
        cinc_b, cinc_d = parse_ramp_number(cinc)
        entry['continuations'].append({
            'duration_secs': duration,
            'speed_base': csp_b, 'speed_delta': csp_d,
            'incline_base': cinc_b, 'incline_delta': cinc_d,
        })

    if _entry_has_ramp(entry) and entry['start_date'] is None:
        raise ValueError("entry has ramp but no start=YYYY-MM-DD")

    return entry


def parse_schedule(path):
    entries = []
    for lineno, line in enumerate(path.read_text().splitlines(), start=1):
        line = line.split('#')[0].strip()
        if not line:
            continue
        try:
            entries.append(parse_entry(line))
        except ValueError as err:
            raise ValueError(f"{path}:{lineno}: {err}") from err
    return entries


# ---- effective values ----

def eff_speed(e, day):
    return e['speed_base'] + e['speed_delta'] * day


def eff_incline(e, day):
    return e['incline_base'] + e['incline_delta'] * day


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
            'speed': c['speed_base'] + c['speed_delta'] * day,
            'incline': c['incline_base'] + c['incline_delta'] * day,
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

def daemon():
    schedule_path = Path(sys.argv[1]) if len(sys.argv) > 1 else SCHEDULE_FILE
    print(f"treadmill-cron: watching {schedule_path}")

    cfg = load_config()
    state = load_state()
    if 'start_date' not in state:
        state['start_date'] = date.today().isoformat()
        save_state(state)

    last_tick = time.monotonic()

    running_entry = None
    remaining_chunks: list = []
    chunk_end_mono = None
    prev_speed = None
    prev_incline = None
    last_announced_evt = None
    creep_accum = 0.0       # belt-time accrued toward the next creep nudge
    creep_note = None       # dedupes the "at max" log line

    def stop_running(restore: bool):
        nonlocal running_entry, remaining_chunks, chunk_end_mono
        if running_entry is None:
            return
        if restore and prev_speed is not None:
            tm = get_treadmill_state()
            if is_running(tm):
                ctl('speed', str(prev_speed))
                ctl('incline', str(prev_incline))
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

        # Treadmill stopped while running an entry -> abort, no restore
        if running_entry and not is_running(treadmill):
            print("treadmill-cron: treadmill stopped, aborting current entry")
            running_entry = None
            remaining_chunks = []
            chunk_end_mono = None

        # Advance current chunk if its time is up
        if running_entry and chunk_end_mono is not None and now_mono >= chunk_end_mono:
            remaining_chunks.pop(0)
            if remaining_chunks:
                chunk = remaining_chunks[0]
                ctl('speed', str(chunk['speed']))
                ctl('incline', str(chunk['incline']))
                chunk_end_mono = now_mono + chunk['duration_secs']
                print(f"treadmill-cron: chunk -> {chunk['speed']:.1f} kph "
                      f"{chunk['incline']:.1f}% for {chunk['duration_secs']}s")
            else:
                if running_entry['kind'] == 'day':
                    mark_fired(state, running_entry)
                stop_running(restore=True)

        # Find best candidate to fire now
        best = None  # (priority_sort_key, entry, remaining_secs)
        for e in entries:
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
                    stop_running(restore=False)

                chunks = chunks_for(candidate, state, first_duration_override=rem)
                running_entry = candidate
                remaining_chunks = chunks
                prev_speed = treadmill.get('speed_kph', 0)
                prev_incline = treadmill.get('incline_pct', 0)
                chunk = chunks[0]
                ctl('speed', str(chunk['speed']))
                ctl('incline', str(chunk['incline']))
                chunk_end_mono = now_mono + chunk['duration_secs']
                tag = candidate['kind']
                print(f"treadmill-cron: start {tag} (p={cp}) "
                      f"{chunk['speed']:.1f} kph {chunk['incline']:.1f}% "
                      f"for {chunk['duration_secs']}s "
                      f"(was {prev_speed:.1f} kph {prev_incline:.1f}%)")
                if tag in cfg.get('notify_kinds', []):
                    notify(cfg, f"treadmill: {tag}",
                           f"{chunk['speed']:.1f} kph {chunk['incline']:.1f}% "
                           f"for {chunk['duration_secs']}s")
                last_announced_evt = None

        # Light status when idle
        if not running_entry:
            evt = _next_announce(entries, now, state)
            if evt and evt != last_announced_evt:
                print(f"treadmill-cron: next {evt}")
                last_announced_evt = evt

        # Creep: gentle upward pressure during free walking. Lowest priority --
        # only acts when no entry is running, while moving, inside a creep
        # window. Accumulator only advances while creeping, so stepping off or
        # leaving a window pauses (doesn't restart) the climb. Climbs from the
        # measured speed, so a manual slow-down lowers where the next nudge starts.
        active_creep = next(
            (c for c in entries
             if c['kind'] == 'creep' and creep_window_open(c['window'], now)),
            None)
        if running_entry is None and is_running(treadmill) and active_creep is not None:
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
                     f"{eff_speed(e, day):.2f} kph {eff_incline(e, day):.2f}%")
        if best is None or (cand is not None and (best[0] is None or cand < best[0])):
            best = (cand, label)
    return best[1] if best else None


# ---- subcommands ----

def fmt_secs(s):
    return f"{s//60:02d}:{s%60:02d}"


def cmd_status():
    s = load_state()
    entries = parse_schedule(SCHEDULE_FILE)
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
            print(f"  creep   {wstr:<13s}    +{e['step']} kph / "
                  f"{e['interval_secs']}s  ->  max {e['max']:.2f} kph")
            continue
        day = entry_day(e, s)
        sp, inc = eff_speed(e, day), eff_incline(e, day)
        prio = e.get('priority')
        prio_str = f"p={prio}" if prio is not None else "p=-"
        sd = e.get('start_date')
        sd_str = f" since {sd} (day {day})" if sd else ""
        if e['kind'] == 'hourly':
            base = (f"  hourly  :{fmt_secs(e['start_secs'])}-:{fmt_secs(e['end_secs'])}    "
                    f"{sp:.2f} kph  {inc:.2f}%")
        elif e['kind'] == 'absolute':
            base = (f"  abs     {e['start_h']:02d}:{e['start_m']:02d}-"
                    f"{e['end_h']:02d}:{e['end_m']:02d}    {sp:.2f} kph  {inc:.2f}%")
        elif e['kind'] == 'day':
            end_off = eff_end_offset(e, day)
            dur = end_off - e['start_offset']
            fired = ' [fired today]' if fired_today(s, e) else ''
            base = (f"  day     day+{fmt_secs(e['start_offset'])}-day+{fmt_secs(end_off)}    "
                    f"({dur}s)    {sp:.2f} kph  {inc:.2f}%{fired}")
        else:
            continue
        cont_str = ''
        for c in e.get('continuations', []):
            cs = c['speed_base'] + c['speed_delta'] * day
            ci = c['incline_base'] + c['incline_delta'] * day
            cont_str += f", +{c['duration_secs']}s @ {cs:.2f} kph {ci:.2f}%"
        print(f"{base}    [{prio_str}{sd_str}]{cont_str}")


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


def main():
    if len(sys.argv) >= 2 and sys.argv[1] in ('status', 'hold', 'reset'):
        return {'status': cmd_status, 'hold': cmd_hold, 'reset': cmd_reset}[sys.argv[1]]()
    daemon()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\ntreadmill-cron: stopped")
