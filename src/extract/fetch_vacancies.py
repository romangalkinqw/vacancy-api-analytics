from __future__ import annotations

import json
import os
from datetime import datetime, UTC
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from src.api_clients.hh_client import HHClient

ROOT_DIR = Path(__file__).resolve().parents[2]
RAW_DIR = ROOT_DIR / "data" / "raw"
EXPORT_DIR = ROOT_DIR / "data" / "exports"

SEARCH_QUERIES = [
    "аналитик данных",
    "data analyst",
    "bi analyst",
    "product analyst",
]


def ensure_dirs() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)


def build_file_suffix(query: str, fetched_at: str) -> str:
    safe_query = query.replace(" ", "_").replace("/", "_")
    return f"{safe_query}_{fetched_at}"


def extract_key_fields(item: dict) -> dict:
    salary = item.get("salary") or {}
    employer = item.get("employer") or {}
    area = item.get("area") or {}
    schedule = item.get("schedule") or {}
    employment = item.get("employment") or {}
    experience = item.get("experience") or {}
    snippet = item.get("snippet") or {}

    return {
        "source": "hh",
        "vacancy_id": item.get("id"),
        "vacancy_name": item.get("name"),
        "published_at": item.get("published_at"),
        "alternate_url": item.get("alternate_url"),
        "employer_id": employer.get("id"),
        "employer_name": employer.get("name"),
        "area_id": area.get("id"),
        "area_name": area.get("name"),
        "salary_from": salary.get("from"),
        "salary_to": salary.get("to"),
        "salary_currency": salary.get("currency"),
        "salary_gross": salary.get("gross"),
        "employment_id": employment.get("id"),
        "employment_name": employment.get("name"),
        "schedule_id": schedule.get("id"),
        "schedule_name": schedule.get("name"),
        "experience_id": experience.get("id"),
        "experience_name": experience.get("name"),
        "snippet_requirement": snippet.get("requirement"),
        "snippet_responsibility": snippet.get("responsibility"),
        "has_test": item.get("has_test"),
        "archived": item.get("archived"),
    }


def save_raw_json(records: list[dict], query: str, fetched_at: str) -> Path:
    file_suffix = build_file_suffix(query, fetched_at)
    file_path = RAW_DIR / f"hh_raw_{file_suffix}.json"

    with open(file_path, "w", encoding="utf-8") as file:
        json.dump(records, file, ensure_ascii=False, indent=2)

    return file_path


def save_flat_csv(records: list[dict], query: str, fetched_at: str) -> Path:
    flat_rows = [extract_key_fields(item) for item in records]
    dataframe = pd.DataFrame(flat_rows)

    file_suffix = build_file_suffix(query, fetched_at)
    file_path = EXPORT_DIR / f"hh_flat_{file_suffix}.csv"

    dataframe.to_csv(file_path, index=False, encoding="utf-8-sig")

    return file_path


def main() -> None:
    load_dotenv()
    ensure_dirs()

    user_agent = os.getenv("HH_USER_AGENT")
    if not user_agent:
        raise ValueError("HH_USER_AGENT is not set in .env")

    client = HHClient(
        user_agent=user_agent,
        timeout=30,
        sleep_seconds=0.2,
        max_retries=3,
    )

    fetched_at_dt = datetime.now(UTC)
    fetched_at_for_file = fetched_at_dt.strftime("%Y%m%dT%H%M%SZ")

    total_records = 0

    for query in SEARCH_QUERIES:
        print(f"[INFO] Start loading query: {query}")

        records = client.fetch_vacancies(
            text=query,
            area=113,
            per_page=100,
            max_pages=5,
            only_with_salary=False,
            period=30,
        )

        raw_path = save_raw_json(
            records=records,
            query=query,
            fetched_at=fetched_at_for_file,
        )

        csv_path = save_flat_csv(
            records=records,
            query=query,
            fetched_at=fetched_at_for_file,
        )

        print(f"[INFO] Query loaded: {query}")
        print(f"[INFO] Records count: {len(records)}")
        print(f"[INFO] Raw JSON saved to: {raw_path}")
        print(f"[INFO] Flat CSV saved to: {csv_path}")

        total_records += len(records)

    print(f"[INFO] Done. Total records loaded: {total_records}")


if __name__ == "__main__":
    main()