from __future__ import annotations

import time
from typing import Any

import requests


class HHClient:
    BASE_URL = "https://api.hh.ru"

    def __init__(self, user_agent: str, timeout: int = 30, sleep_seconds: float = 0.2, max_retries: int = 3) -> None:
        self.timeout = timeout
        self.sleep_seconds = sleep_seconds
        self.max_retries = max_retries

        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": user_agent,
                "Accept": "application/json",
            }
        )

    def _get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{self.BASE_URL}{path}"

        for attempt in range(1, self.max_retries + 1):
            try:
                response = self.session.get(
                    url,
                    params=params,
                    timeout=self.timeout,
                )

                response.raise_for_status()
                data = response.json()

                if self.sleep_seconds > 0:
                    time.sleep(self.sleep_seconds)

                return data

            except requests.exceptions.Timeout:
                print(f"[WARNING] Timeout: attempt {attempt}/{self.max_retries}")

            except requests.exceptions.ConnectionError:
                print(f"[WARNING] Connection error: attempt {attempt}/{self.max_retries}")

            except requests.exceptions.HTTPError as exc:
                status_code = exc.response.status_code if exc.response is not None else None
                print(f"[ERROR] HTTP error: status_code={status_code}, url={url}")

                if status_code is not None and 400 <= status_code < 500 and status_code != 429:
                    raise

                if attempt == self.max_retries:
                    raise

            except requests.exceptions.RequestException as exc:
                print(f"[ERROR] Request failed: {exc}")
                raise

            if attempt < self.max_retries:
                backoff_seconds = 2 ** (attempt - 1)
                print(f"[INFO] Retry after {backoff_seconds} sec")
                time.sleep(backoff_seconds)

        raise RuntimeError(f"Failed to fetch data from HH API: {url}")

    def search_vacancies(self, text: str, area: int = 113, per_page: int = 100, page: int = 0, only_with_salary: bool = False, period: int | None = None) -> dict[str, Any]:
        if per_page < 1 or per_page > 100:
            raise ValueError("per_page must be between 1 and 100")

        if page < 0:
            raise ValueError("page must be >= 0")

        params: dict[str, Any] = {
            "text": text,
            "area": area,
            "per_page": per_page,
            "page": page,
        }

        if only_with_salary:
            params["only_with_salary"] = True

        if period is not None:
            if period < 1:
                raise ValueError("period must be >= 1")
            params["period"] = period

        return self._get("/vacancies", params=params)

    def get_vacancy(self, vacancy_id: str | int) -> dict[str, Any]:
        return self._get(f"/vacancies/{vacancy_id}")

    def fetch_vacancies(
        self,
        text: str,
        area: int = 113,
        per_page: int = 100,
        max_pages: int = 5,
        only_with_salary: bool = False,
        period: int | None = None,
    ) -> list[dict[str, Any]]:
        if max_pages < 1:
            raise ValueError("max_pages must be >= 1")

        all_items: list[dict[str, Any]] = []

        first_page = self.search_vacancies(
            text=text,
            area=area,
            per_page=per_page,
            page=0,
            only_with_salary=only_with_salary,
            period=period,
        )

        total_pages = first_page.get("pages", 0)
        pages_to_fetch = min(total_pages, max_pages)

        first_page_items = first_page.get("items", [])
        all_items.extend(first_page_items)

        print(
            f"[INFO] Query='{text}' | first_page_items={len(first_page_items)} "
            f"| total_pages={total_pages} | pages_to_fetch={pages_to_fetch}"
        )

        for page in range(1, pages_to_fetch):
            page_data = self.search_vacancies(
                text=text,
                area=area,
                per_page=per_page,
                page=page,
                only_with_salary=only_with_salary,
                period=period,
            )

            page_items = page_data.get("items", [])
            all_items.extend(page_items)

            print(
                f"[INFO] Query='{text}' | page={page} | page_items={len(page_items)} "
                f"| total_accumulated={len(all_items)}"
            )

        return all_items