from __future__ import annotations

import json
import time
import sys
import urllib.error
import urllib.parse
import urllib.request
from typing import Any


def _request(
    method: str,
    url: str,
    *,
    expected_statuses: set[int],
) -> tuple[int, Any]:
    last_error: Exception | None = None
    for attempt in range(3):
        request = urllib.request.Request(url, method=method)
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                payload = response.read().decode("utf-8")
                if payload:
                    try:
                        body = json.loads(payload)
                    except json.JSONDecodeError:
                        body = payload
                else:
                    body = None
                if response.status not in expected_statuses:
                    raise RuntimeError(
                        f"{method} {url} returned unexpected status {response.status}."
                    )
                return response.status, body
        except urllib.error.HTTPError as exc:
            payload = exc.read().decode("utf-8")
            if payload:
                try:
                    body = json.loads(payload)
                except json.JSONDecodeError:
                    body = payload
            else:
                body = None
            if exc.code not in expected_statuses:
                raise RuntimeError(f"{method} {url} returned unexpected status {exc.code}.") from exc
            return exc.code, body
        except (urllib.error.URLError, ConnectionResetError, OSError) as exc:
            last_error = exc
            if attempt == 2:
                break
            time.sleep(0.5)
    if last_error is not None:
        raise last_error
    raise RuntimeError(f"{method} {url} failed without a captured error.")


def main() -> int:
    base_url = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8000"
    base_url = base_url.rstrip("/")

    checks: list[dict[str, Any]] = []

    health_status, _ = _request("GET", f"{base_url}/health", expected_statuses={200})
    checks.append({"name": "health", "status": health_status})

    _, dataset_list = _request("GET", f"{base_url}/api/datasets?page=1&per_page=1", expected_statuses={200})
    dataset_items = (dataset_list or {}).get("items") or []
    if not dataset_items:
        raise RuntimeError("Smoke check requires at least one dataset from /api/datasets.")
    dataset_id = str(dataset_items[0]["dataset_id"])
    checks.append({"name": "datasets_list", "status": 200, "dataset_id": dataset_id})

    options_status, options = _request(
        "GET",
        f"{base_url}/api/datasets/request-options",
        expected_statuses={200},
    )
    if "constraints" not in (options or {}):
        raise RuntimeError("Dataset request options response is missing constraints.")
    checks.append({"name": "dataset_request_options", "status": options_status})

    training_status, training = _request(
        "GET",
        f"{base_url}/api/datasets/training",
        expected_statuses={200},
    )
    if "items" not in (training or {}):
        raise RuntimeError("Training datasets response is missing items.")
    checks.append(
        {
            "name": "training_datasets",
            "status": training_status,
            "total": (training or {}).get("total"),
        }
    )

    dependencies_status, dependencies = _request(
        "GET",
        f"{base_url}/api/datasets/{urllib.parse.quote(dataset_id)}/dependencies",
        expected_statuses={200},
    )
    if "dataset_id" not in (dependencies or {}):
        raise RuntimeError("Dataset dependencies response is missing dataset_id.")
    checks.append({"name": "dataset_dependencies", "status": dependencies_status, "dataset_id": dataset_id})

    delete_probe_id = "__control_smoke_missing_dataset__"
    delete_status, delete_body = _request(
        "DELETE",
        f"{base_url}/api/datasets/{urllib.parse.quote(delete_probe_id)}",
        expected_statuses={404},
    )
    checks.append(
        {
            "name": "dataset_delete_missing_probe",
            "status": delete_status,
            "detail": (delete_body or {}).get("detail"),
        }
    )

    print(json.dumps({"base_url": base_url, "checks": checks}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
