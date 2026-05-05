import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import requests

import os, json
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

def get_calendar_service():
    token = json.loads(os.environ["GOOGLE_TOKEN"])
    creds = Credentials.from_authorized_user_info(token)
    return build("calendar", "v3", credentials=creds)

from datetime import datetime, timedelta

def parse_event(line, date_str):
    if " | " not in line:
        return None

    try:
        time_part, title = line.split(" | ", 1)
        start, end = time_part.split("-")

        start_dt = datetime.fromisoformat(f"{date_str}T{start}:00")
        end_dt = datetime.fromisoformat(f"{date_str}T{end}:00")

        # Handle midnight crossover
        if end_dt <= start_dt:
            end_dt += timedelta(days=1)

        return {
            "summary": title.strip(),
            "start": {
                "dateTime": start_dt.isoformat(),
                "timeZone": "Asia/Dubai",
            },
            "end": {
                "dateTime": end_dt.isoformat(),
                "timeZone": "Asia/Dubai",
            },
            "reminders": {
                "useDefault": False,
                "overrides": [
                    {"method": "popup", "minutes": 0},
                ],
            },
        }
    except Exception:
        return None

def push_to_calendar(plan_text):
    service = get_calendar_service()

    today = local_date_str()  # ✅ FIXED timezone

    # 🔥 Prevent duplicates
    clear_today_events(service)

    for line in plan_text.split("\n"):
        event = parse_event(line, today)

        if not event:
            continue

        service.events().insert(
            calendarId="primary",
            body=event
        ).execute()

from datetime import timezone

def clear_today_events(service):
    now = now_local()

    start_day = now.replace(hour=0, minute=0, second=0, microsecond=0).isoformat() + "+04:00"
    end_day = now.replace(hour=23, minute=59, second=59, microsecond=0).isoformat() + "+04:00"

    events = service.events().list(
        calendarId="primary",
        timeMin=start_day,
        timeMax=end_day,
        singleEvents=True,
        orderBy="startTime"
    ).execute()

    for event in events.get("items", []):
        service.events().delete(
            calendarId="primary",
            eventId=event["id"]
        ).execute()

NOTION_API_KEY = os.environ["NOTION_API_KEY"]
GROQ_API_KEY = os.environ["GROQ_API_KEY"]

TASKS_DB_ID = os.environ["TASKS_DB_ID"]
SCHEDULE_DB_ID = os.environ["SCHEDULE_DB_ID"]
DAILY_PLAN_DB_ID = os.environ["DAILY_PLAN_DB_ID"]

CITY = os.environ.get("CITY", "Al Ain")
COUNTRY = os.environ.get("COUNTRY", "UAE")
LOCAL_UTC_OFFSET_HOURS = int(os.environ.get("LOCAL_UTC_OFFSET_HOURS", "4"))

NOTION_VERSION = "2022-06-28"
GROQ_MODEL = os.environ.get("GROQ_MODEL", "llama-3.3-70b-versatile")

NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_API_KEY}",
    "Notion-Version": NOTION_VERSION,
    "Content-Type": "application/json",
}

GROQ_HEADERS = {
    "Authorization": f"Bearer {GROQ_API_KEY}",
    "Content-Type": "application/json",
}


@dataclass
class Block:
    start: int
    end: int
    label: str
    kind: str  # fixed, task


def now_local() -> datetime:
    return datetime.utcnow() + timedelta(hours=LOCAL_UTC_OFFSET_HOURS)


def local_date_str() -> str:
    return now_local().strftime("%Y-%m-%d")


def local_weekday_names() -> List[str]:
    dt = now_local()
    short = dt.strftime("%a").lower()   # mon
    long = dt.strftime("%A").lower()    # monday
    return [short, long]


def to_minutes(hhmm: str) -> int:
    if not hhmm or ":" not in hhmm:
        return 0
    h, m = hhmm.strip().split(":")
    return int(h) * 60 + int(m)


def fmt_minutes(minutes: int) -> str:
    minutes = max(0, min(24 * 60, int(minutes)))
    h = minutes // 60
    m = minutes % 60
    return f"{h:02d}:{m:02d}"


def notion_query_database(database_id: str) -> List[Dict[str, Any]]:
    url = f"https://api.notion.com/v1/databases/{database_id}/query"
    payload = {"page_size": 100}
    r = requests.post(url, headers=NOTION_HEADERS, json=payload, timeout=30)
    r.raise_for_status()
    return r.json().get("results", [])


def notion_create_page(database_id: str, name: str, date_value: str, plan_text: str) -> Dict[str, Any]:
    url = "https://api.notion.com/v1/pages"
    payload = {
        "parent": {"database_id": database_id},
        "properties": {
            "Name": {
                "title": [
                    {
                        "text": {
                            "content": name
                        }
                    }
                ]
            },
            "Date": {
                "date": {
                    "start": date_value
                }
            },
            "Plan": {
                "rich_text": [
                    {
                        "text": {
                            "content": plan_text[:1900]
                        }
                    }
                ]
            },
        },
    }
    r = requests.post(url, headers=NOTION_HEADERS, json=payload, timeout=30)
    r.raise_for_status()
    return r.json()


def get_prop_title(page: Dict[str, Any], prop: str) -> str:
    return (
        page.get("properties", {})
        .get(prop, {})
        .get("title", [{}])[0]
        .get("plain_text", "")
    )


def get_prop_select(page: Dict[str, Any], prop: str) -> str:
    return (
        page.get("properties", {})
        .get(prop, {})
        .get("select", {})
        .get("name", "")
    )


def get_prop_rich_text(page: Dict[str, Any], prop: str) -> str:
    return (
        page.get("properties", {})
        .get(prop, {})
        .get("rich_text", [{}])[0]
        .get("plain_text", "")
    )


def get_prop_number(page: Dict[str, Any], prop: str, default: int = 0) -> int:
    value = (
        page.get("properties", {})
        .get(prop, {})
        .get("number", None)
    )
    return int(value) if value is not None else default


def get_prop_date_start(page: Dict[str, Any], prop: str) -> str:
    return (
        page.get("properties", {})
        .get(prop, {})
        .get("date", {})
        .get("start", "")
    )


def parse_day_value(page: Dict[str, Any]) -> str:
    props = page.get("properties", {})
    day_prop = props.get("Day", {})
    return (
        day_prop.get("select", {}).get("name", "")
        or day_prop.get("rich_text", [{}])[0].get("plain_text", "")
        or day_prop.get("title", [{}])[0].get("plain_text", "")
        or ""
    ).strip().lower()


def is_today_day_value(day_value: str) -> bool:
    if not day_value:
        return False
    short, long = local_weekday_names()
    d = day_value.strip().lower()
    return d == short or d == long or d.startswith(short) or d.startswith(long)


def merge_blocks(blocks: List[Block]) -> List[Block]:
    if not blocks:
        return []

    sorted_blocks = sorted(blocks, key=lambda b: (b.start, b.end))
    merged: List[Block] = [sorted_blocks[0]]

    for block in sorted_blocks[1:]:
        last = merged[-1]
        if block.start <= last.end:
            last.end = max(last.end, block.end)
            if block.label != last.label:
                last.label = f"{last.label} + {block.label}"
        else:
            merged.append(block)

    return merged


def fetch_prayer_times() -> Dict[str, str]:
    url = "http://api.aladhan.com/v1/timingsByCity"
    params = {"city": CITY, "country": COUNTRY}
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    return data.get("data", {}).get("timings", {})


def build_busy_blocks(schedule_pages: List[Dict[str, Any]], prayer_times: Dict[str, str]) -> List[Block]:
    blocks: List[Block] = []

    # Sleep block: 22:00 -> 05:00 next day
    sleep_start = to_minutes("22:00")
    sleep_end = to_minutes("05:00")
    if sleep_start > sleep_end:
        blocks.append(Block(start=sleep_start, end=24 * 60, label="Sleep", kind="fixed"))
        blocks.append(Block(start=0, end=sleep_end, label="Sleep", kind="fixed"))
    else:
        blocks.append(Block(start=sleep_start, end=sleep_end, label="Sleep", kind="fixed"))

    # Prayer blocks
    prayer_durations = {
        "Fajr": 30,
        "Dhuhr": 30,
        "Asr": 30,
        "Maghrib": 20,
        "Isha": 30,
    }

    for prayer_name, duration in prayer_durations.items():
        t = to_minutes(prayer_times.get(prayer_name, ""))
        if t > 0:
            blocks.append(
                Block(
                    start=t,
                    end=t + duration,
                    label=f"{prayer_name} Prayer",
                    kind="fixed",
                )
            )

    # School / Quran / other fixed schedule
    for page in schedule_pages:
        day_value = parse_day_value(page)
        if not is_today_day_value(day_value):
            continue

        start = (
            page.get("properties", {})
            .get("Start Time", {})
            .get("rich_text", [{}])[0]
            .get("plain_text", "")
        )
        end = (
            page.get("properties", {})
            .get("End Time", {})
            .get("rich_text", [{}])[0]
            .get("plain_text", "")
        )

        if start and end:
            blocks.append(
                Block(
                    start=to_minutes(start),
                    end=to_minutes(end),
                    label=get_prop_title(page, "Name") or "Event",
                    kind="fixed",
                )
            )

    return merge_blocks(blocks)


def build_free_slots(busy_blocks: List[Block]) -> List[Dict[str, int]]:
    free: List[Dict[str, int]] = []
    current = 0

    for block in busy_blocks:
        if block.start > current:
            free.append({"start": current, "end": block.start})
        current = max(current, block.end)

    if current < 24 * 60:
        free.append({"start": current, "end": 24 * 60})

    return free


def extract_tasks(task_pages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    tasks: List[Dict[str, Any]] = []
    today = local_date_str()

    for page in task_pages:
        props = page.get("properties", {})
        status = props.get("Status", {}).get("select", {}).get("name", "")
        if status == "Done":
            continue

        due_date = get_prop_date_start(page, "Due Date")
        if due_date and due_date[:10] > today:
            continue

        tasks.append(
            {
                "name": get_prop_title(page, "Name") or "Unnamed Task",
                "duration": get_prop_number(page, "Duration", 30),
                "priority": get_prop_select(page, "Priority") or "Medium",
            }
        )

    def score(task: Dict[str, Any]) -> int:
        if task["priority"] == "High":
            return 3
        if task["priority"] == "Medium":
            return 2
        return 1

    tasks.sort(key=score, reverse=True)
    return tasks


def ask_groq_for_order(tasks: List[Dict[str, Any]], free_slots: List[Dict[str, int]], fixed_blocks: List[Block]) -> List[str]:
    if not tasks:
        return []

    tasks_text = "\n".join(
        f"- {t['name']} ({t['duration']} min, Priority: {t['priority']})"
        for t in tasks
    )

    free_text = "\n".join(
        f"- {fmt_minutes(s['start'])}-{fmt_minutes(s['end'])}"
        for s in free_slots
    ) or "- No free slots"

    fixed_text = "\n".join(
        f"- {fmt_minutes(b.start)}-{fmt_minutes(b.end)} | {b.label}"
        for b in fixed_blocks
    ) or "- No fixed events"

    prompt = f"""You are a strict task ranker.

Rank the tasks from most important to least important for today.
Do not add times, breaks, or explanations.
Return ONLY a JSON array of task names in order.

Fixed events:
{fixed_text}

Free slots:
{free_text}

Tasks:
{tasks_text}
"""

    body = {
        "model": GROQ_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.1,
    }

    try:
        r = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers=GROQ_HEADERS,
            json=body,
            timeout=45,
        )
        r.raise_for_status()
        content = r.json()["choices"][0]["message"]["content"].strip()
        content = content.replace("```json", "").replace("```", "").strip()
        parsed = json.loads(content)
        if isinstance(parsed, list):
            return [str(x).strip() for x in parsed if str(x).strip()]
    except Exception:
        pass

    # Fallback: keep the already sorted task order
    return [t["name"] for t in tasks]


def build_final_plan(
    tasks: List[Dict[str, Any]],
    free_slots: List[Dict[str, int]],
    fixed_blocks: List[Block],
    ordered_task_names: List[str],
) -> List[Block]:
    task_by_name = {t["name"]: dict(t) for t in tasks}

    ordered_tasks: List[Dict[str, Any]] = []
    seen = set()

    for name in ordered_task_names:
        if name in task_by_name and name not in seen:
            ordered_tasks.append(task_by_name[name])
            seen.add(name)

    for task in tasks:
        if task["name"] not in seen:
            ordered_tasks.append(task)

    final_blocks: List[Block] = [Block(b.start, b.end, b.label, b.kind) for b in fixed_blocks]

    task_index = 0
    remaining = ordered_tasks[0]["duration"] if ordered_tasks else 0

    for slot in free_slots:
        cursor = slot["start"]

        while cursor < slot["end"] and task_index < len(ordered_tasks):
            task = ordered_tasks[task_index]
            remaining = int(remaining or task["duration"])
            room = slot["end"] - cursor

            if room < 10:
                break

            chunk = min(remaining, room)

            final_blocks.append(
                Block(
                    start=cursor,
                    end=cursor + chunk,
                    label=task["name"],
                    kind="task",
                )
            )

            cursor += chunk
            remaining -= chunk

            if remaining <= 0:
                task_index += 1
                if task_index < len(ordered_tasks):
                    remaining = ordered_tasks[task_index]["duration"]
            else:
                # Move to the next free slot for the remaining part of this task
                break

    final_blocks.sort(key=lambda b: (b.start, b.end, b.kind))
    return final_blocks


def render_plan(blocks: List[Block]) -> str:
    lines = []
    for block in blocks:
        lines.append(f"{fmt_minutes(block.start)}-{fmt_minutes(block.end)} | {block.label}")
    return "\n".join(lines)


def main() -> None:
    task_pages = notion_query_database(TASKS_DB_ID)
    schedule_pages = notion_query_database(SCHEDULE_DB_ID)
    prayer_times = fetch_prayer_times()

    fixed_blocks = build_busy_blocks(schedule_pages, prayer_times)
    free_slots = build_free_slots(fixed_blocks)
    tasks = extract_tasks(task_pages)

    ordered_task_names = ask_groq_for_order(tasks, free_slots, fixed_blocks)
    final_blocks = build_final_plan(tasks, free_slots, fixed_blocks, ordered_task_names)

    plan_text = render_plan(final_blocks)
    push_to_calendar(plan_text)
    date_value = local_date_str()
    title = f"Plan for {now_local().strftime('%a %b %d %Y')}"

    notion_create_page(
        DAILY_PLAN_DB_ID,
        name=title,
        date_value=date_value,
        plan_text=plan_text,
    )

    print(plan_text)


if __name__ == "__main__":
    main()
