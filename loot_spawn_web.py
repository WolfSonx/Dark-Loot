from __future__ import annotations

import argparse
import csv
import gzip
import io
import json
import math
import pickle
import re
import threading
import time
import traceback
import urllib.error
import urllib.request
import webbrowser
from collections import defaultdict
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, quote, unquote, urlparse

from loot_spawn_analyzer import (
    APP_TITLE as SCANNER_TITLE,
    ScanResult,
    build_database,
    difficulty_sort_key,
    grade_probabilities,
    map_sort_key,
    percent,
)


WEB_APP_TITLE = "DungeonCrawler Loot Browser"
APP_VERSION = "1.0.0"
CACHE_VERSION = 1
DEFAULT_LIMIT = 500
MAX_LIMIT = 5000
DEFAULT_CACHE_FILE = Path(__file__).with_name("loot_spawn_cache.pkl.gz")
HIDDEN_MAPS = {"Global/Default"}
HIDDEN_DIFFS = {"Global"}
HIDDEN_SOURCE_KINDS = {"Spawner"}
HIDDEN_MAP_CODES = {"0"}
RARITY_ORDER = ["Junk", "Common", "Uncommon", "Rare", "Epic", "Legendary", "Unique", "Artifact", "None"]


def clean_terms(value: str) -> list[str]:
    return [term for term in re.split(r"\s+", value.strip().lower()) if term]


def contains_terms(value: str, fields: list[str]) -> bool:
    terms = clean_terms(value)
    if not terms:
        return True
    haystack = " ".join(str(field) for field in fields if field).lower()
    return all(term in haystack for term in terms)


def terms_match_text(terms: list[str], text: str) -> bool:
    return all(term in text for term in terms)


def param(params: dict[str, list[str]], name: str, default: str = "") -> str:
    values = params.get(name)
    return values[0] if values else default


def int_param(params: dict[str, list[str]], name: str, default: int, minimum: int = 0, maximum: int | None = None) -> int:
    try:
        value = int(param(params, name, str(default)))
    except ValueError:
        value = default
    value = max(minimum, value)
    if maximum is not None:
        value = min(value, maximum)
    return value


def scenario_key(row: dict) -> tuple:
    return (
        row["map"],
        row["diff"],
        row["group"],
        row["loot_table"],
        row["rate_table"],
        row["rolls"],
    )


def summarize_values(values, limit: int = 4) -> str:
    ordered = sorted(str(value) for value in values if value is not None and str(value))
    if not ordered:
        return ""
    if len(ordered) <= limit:
        return ", ".join(ordered)
    return ", ".join(ordered[:limit]) + f" +{len(ordered) - limit}"


def visible_map_values(values) -> list[str]:
    return sorted(
        (str(value) for value in values if value is not None and str(value) and str(value) not in HIDDEN_MAPS),
        key=map_sort_key,
    )


def summarize_maps(values, limit: int = 4) -> str:
    visible = visible_map_values(values)
    if not visible:
        return "All Maps"
    return summarize_values(visible, limit)


def visible_map_code_values(values) -> list[str]:
    return sorted(
        (str(value) for value in values if value is not None and str(value) and str(value) not in HIDDEN_MAP_CODES),
        key=lambda code: (len(code), code),
    )


def summarize_map_codes(values, limit: int = 8) -> str:
    visible = visible_map_code_values(values)
    if not visible:
        return ""
    return summarize_values(visible, limit)


def visible_diff_values(values) -> list[str]:
    return sorted(
        (str(value) for value in values if value is not None and str(value) and str(value) not in HIDDEN_DIFFS),
        key=difficulty_sort_key,
    )


def summarize_diffs(values, limit: int = 4) -> str:
    visible = visible_diff_values(values)
    if not visible:
        return "All Difficulties"
    return summarize_values(visible, limit)


def visible_source_row(row: dict) -> bool:
    if str(row.get("source_kind", "")) in HIDDEN_SOURCE_KINDS:
        return False
    if not any(str(value) not in HIDDEN_MAPS for value in row.get("maps", ()) if value is not None and str(value)):
        return False
    if not any(str(value) not in HIDDEN_DIFFS for value in row.get("diffs", ()) if value is not None and str(value)):
        return False
    return True


def scan_luck(result: ScanResult | None) -> int:
    if not result:
        return 500
    try:
        return int(result.stats.get("luck", 500) or 500)
    except (TypeError, ValueError):
        return 500


def row_with_luck(row: dict, result: ScanResult, luck: int, dyn_prob_cache: dict[str, list[float]]) -> dict:
    if luck == scan_luck(result):
        return row
    rate_key = row.get("rate_key")
    grade = int(row.get("grade", 0) or 0)
    weights = result.rate_weights.get(rate_key)
    if not weights or grade < 0 or grade >= len(weights):
        return row
    dyn_probs = dyn_prob_cache.get(rate_key)
    if dyn_probs is None:
        dyn_probs = grade_probabilities(weights, luck)[1]
        dyn_prob_cache[rate_key] = dyn_probs
    choice_fraction = float(row.get("choice_fraction", 0.0) or 0.0)
    rolls = max(1, int(row.get("rolls", 1) or 1))
    dyn_per_roll = dyn_probs[grade] * choice_fraction
    dyn_at_least_one = 1.0 - math.pow(max(0.0, 1.0 - dyn_per_roll), rolls)
    updated = dict(row)
    updated["dyn_per_roll"] = dyn_per_roll
    updated["dyn_at_least_one"] = dyn_at_least_one
    updated["dyn_expected"] = dyn_per_roll * rolls
    return updated


def rows_with_luck(rows: list[dict], result: ScanResult | None, luck: int) -> list[dict]:
    if not result or luck == scan_luck(result):
        return rows
    dyn_prob_cache: dict[str, list[float]] = {}
    return [row_with_luck(row, result, luck, dyn_prob_cache) for row in rows]


def compact_row(row: dict) -> dict:
    return {
        "item": row["item"],
        "itemAsset": row["item_asset"],
        "rarity": row["rarity"],
        "category": row["cat"],
        "source": row["source"],
        "sourceKind": row["source_kind"],
        "sources": sorted(row.get("source_values", [row["source"]])),
        "sourceKinds": sorted(row.get("source_kind_values", [row["source_kind"]])),
        "sourceCount": len(row.get("source_values", [row["source"]])),
        "map": summarize_maps(row["maps"]),
        "maps": visible_map_values(row["maps"]),
        "diff": summarize_diffs(row["diffs"]),
        "diffs": visible_diff_values(row["diffs"]),
        "mapCode": row["map_code"],
        "mapCodes": visible_map_code_values(row["map_codes"]),
        "grade": row["grade"],
        "itemCount": row["item_count"],
        "rolls": row["rolls"],
        "choiceCount": row["choice_count"],
        "gradeChoices": row["grade_choices"],
        "emptyChoices": row["empty_choices"],
        "basePerRoll": percent(row["base_per_roll"]),
        "dynPerRoll": percent(row["dyn_per_roll"]),
        "baseAtLeastOne": percent(row["base_at_least_one"]),
        "dynAtLeastOne": percent(row["dyn_at_least_one"]),
        "baseExpected": f"{row['base_expected']:.6f}",
        "dynExpected": f"{row['dyn_expected']:.6f}",
        "dynAtLeastOneValue": row["dyn_at_least_one"],
        "spawnRate": row["spawn_rate"],
        "mergedRows": row.get("merged_rows", 1),
        "group": row["group"],
        "lootTable": row["loot_table"],
        "rateTable": row["rate_table"],
        "spawner": row["spawner"],
    }


def csv_rows(rows: list[dict]) -> bytes:
    columns = [
        "item",
        "rarity",
        "cat",
        "source",
        "source_kind",
        "map",
        "diff",
        "map_code",
        "grade",
        "item_count",
        "rolls",
        "choice_count",
        "grade_choices",
        "base_per_roll",
        "dyn_per_roll",
        "base_at_least_one",
        "dyn_at_least_one",
        "base_expected",
        "dyn_expected",
        "spawn_rate",
        "merged_rows",
        "group",
        "loot_table",
        "rate_table",
        "spawner",
    ]
    handle = io.StringIO()
    writer = csv.DictWriter(handle, fieldnames=columns)
    writer.writeheader()
    for row in rows:
        csv_row = {column: row.get(column, "") for column in columns}
        csv_row["map"] = summarize_maps(row.get("maps", [row.get("map", "")]), limit=8)
        csv_row["diff"] = summarize_diffs(row.get("diffs", [row.get("diff", "")]), limit=8)
        csv_row["map_code"] = summarize_map_codes(row.get("map_codes", [row.get("map_code", "")]), limit=12)
        writer.writerow(csv_row)
    return handle.getvalue().encode("utf-8-sig")


class WebIndex:
    def __init__(self, rows: list[dict]) -> None:
        self.rows = [row for row in rows if visible_source_row(row)]
        self.hidden_source_rows = len(rows) - len(self.rows)
        self.item_rows: dict[str, list[dict]] = defaultdict(list)
        self.item_search: dict[str, str] = {}
        self.source_rows: dict[tuple[str, str], list[dict]] = defaultdict(list)
        self.source_search: dict[tuple[str, str], set[str]] = defaultdict(set)

        for row in self.rows:
            item_asset = row["item_asset"]
            self.item_rows[item_asset].append(row)
            self.item_search[item_asset] = f"{row['item']} {item_asset}".lower()

            source_key = (row["source"], row["source_kind"])
            self.source_rows[source_key].append(row)
            self.source_search[source_key].update([row["source"], row["source_kind"], row.get("spawner", "")])

        maps = {map_name for row in self.rows for map_name in row["maps"]}
        diffs = {diff for row in self.rows for diff in row["diffs"]}
        categories = {row["cat"] for row in self.rows}
        rarities = {row["rarity"] for row in self.rows}
        self.maps = visible_map_values(maps)
        self.diffs = visible_diff_values(diffs)
        self.categories = sorted(categories)
        self.rarities = [rarity for rarity in RARITY_ORDER if rarity in rarities]
        self.rarities.extend(sorted(rarities - set(self.rarities)))
        self.source_search_text = {
            key: " ".join(sorted(value)).lower()
            for key, value in self.source_search.items()
        }
        self.source_summaries = source_summary(self.rows)
        self.item_summaries = item_summary(self.rows)
        self.item_summaries_by_dyn = sorted(self.item_summaries, key=lambda row: row["dyn_at_least_one"], reverse=True)

    def matching_item_assets(self, query: str) -> set[str] | None:
        terms = clean_terms(query)
        if not terms:
            return None
        return {asset for asset, text in self.item_search.items() if terms_match_text(terms, text)}

    def matching_source_keys(self, query: str) -> set[tuple[str, str]] | None:
        terms = clean_terms(query)
        if not terms:
            return None
        return {key for key, text in self.source_search_text.items() if terms_match_text(terms, text)}

    def candidate_rows(self, item_query: str = "", source_query: str = "") -> tuple[list[dict], set[str] | None, set[tuple[str, str]] | None]:
        item_assets = self.matching_item_assets(item_query)
        source_keys = self.matching_source_keys(source_query)
        item_candidates = None
        source_candidates = None

        if item_assets is not None:
            item_candidates = []
            for asset in item_assets:
                item_candidates.extend(self.item_rows.get(asset, ()))

        if source_keys is not None:
            source_candidates = []
            for key in source_keys:
                source_candidates.extend(self.source_rows.get(key, ()))

        if item_candidates is None and source_candidates is None:
            return self.rows, item_assets, source_keys
        if item_candidates is None:
            return source_candidates or [], item_assets, source_keys
        if source_candidates is None:
            return item_candidates, item_assets, source_keys
        return (item_candidates if len(item_candidates) <= len(source_candidates) else source_candidates), item_assets, source_keys


class AppState:
    def __init__(self, root: Path, luck: int, cache_path: Path = DEFAULT_CACHE_FILE) -> None:
        self.lock = threading.RLock()
        self.root = Path(root)
        self.cache_path = Path(cache_path)
        self.luck = luck
        self.result = None
        self.index: WebIndex | None = None
        self.scanning = False
        self.recalculating = False
        self.saving_cache = False
        self.error = ""
        self.cache_error = ""
        self.started_at = 0.0
        self.finished_at = 0.0
        self.recalc_started_at = 0.0
        self.recalc_finished_at = 0.0
        self.cache_started_at = 0.0
        self.cache_finished_at = 0.0

    def start_scan(self, root: Path | None = None, luck: int | None = None) -> bool:
        with self.lock:
            if self.scanning or self.recalculating:
                return False
            if root is not None:
                self.root = Path(root)
            if luck is not None:
                self.luck = int(luck)
            self.scanning = True
            self.error = ""
            self.started_at = time.time()
            self.finished_at = 0.0
        thread = threading.Thread(target=self._scan_worker, daemon=True)
        thread.start()
        return True

    def _scan_worker(self) -> None:
        try:
            result = build_database(self.root, self.luck)
            index = WebIndex(result.rows)
            error = ""
        except Exception:
            result = None
            index = None
            error = traceback.format_exc()
        with self.lock:
            if not error:
                self.result = result
                self.index = index
            self.error = error
            self.scanning = False
            self.finished_at = time.time()

    def start_recalculate_luck(self, luck: int) -> bool:
        with self.lock:
            if self.scanning or self.recalculating or not self.result:
                return False
            self.luck = int(luck)
            self.error = ""
            self.recalc_started_at = time.time()
            self.recalc_finished_at = self.recalc_started_at
            return True

    def load_cache(self, path: Path | None = None) -> bool:
        cache_path = Path(path or self.cache_path)
        if not cache_path.exists():
            return False
        try:
            with gzip.open(cache_path, "rb") as handle:
                payload = pickle.load(handle)
            if payload.get("cache_version") != CACHE_VERSION:
                raise ValueError(f"Unsupported cache version: {payload.get('cache_version')}")
            result = payload["result"]
            index = WebIndex(result.rows)
            root = Path(payload.get("root") or self.root)
            luck = int(payload.get("luck", scan_luck(result)) or scan_luck(result))
        except Exception:
            with self.lock:
                self.cache_error = traceback.format_exc()
            return False
        with self.lock:
            self.root = root
            self.luck = luck
            self.result = result
            self.index = index
            self.error = ""
            self.cache_error = ""
            self.finished_at = time.time()
        return True

    def start_save_cache(self, path: Path | None = None) -> bool:
        with self.lock:
            if self.saving_cache or not self.result:
                return False
            if path is not None:
                self.cache_path = Path(path)
            self.saving_cache = True
            self.cache_error = ""
            self.cache_started_at = time.time()
            self.cache_finished_at = 0.0
            result = self.result
            root = self.root
            luck = self.luck
            cache_path = self.cache_path
        thread = threading.Thread(target=self._cache_worker, args=(result, root, luck, cache_path), daemon=True)
        thread.start()
        return True

    def _cache_worker(self, result: ScanResult, root: Path, luck: int, cache_path: Path) -> None:
        try:
            payload = {
                "cache_version": CACHE_VERSION,
                "app_version": APP_VERSION,
                "created_at": time.time(),
                "root": str(root),
                "luck": int(luck),
                "result": result,
            }
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            temp_path = cache_path.with_suffix(cache_path.suffix + ".tmp")
            with gzip.open(temp_path, "wb", compresslevel=5) as handle:
                pickle.dump(payload, handle, protocol=pickle.HIGHEST_PROTOCOL)
            temp_path.replace(cache_path)
            error = ""
        except Exception:
            error = traceback.format_exc()
        with self.lock:
            self.cache_error = error
            self.saving_cache = False
            self.cache_finished_at = time.time()

    def snapshot(self) -> dict:
        with self.lock:
            result = self.result
            stats = dict(result.stats) if result else {}
            index = self.index
            filters = {
                "maps": index.maps if index else [],
                "diffs": index.diffs if index else [],
                "categories": index.categories if index else [],
                "rarities": index.rarities if index else [],
            }
            if index:
                stats["rows"] = len(index.rows)
                stats["sources"] = len(index.source_rows)
                stats["items"] = len(index.item_rows)
                stats["hidden_spawner_rows"] = index.hidden_source_rows
            warnings = result.warnings[:80] if result else []
            elapsed = (time.time() - self.started_at) if self.scanning and self.started_at else 0.0
            last_scan = (self.finished_at - self.started_at) if self.finished_at and self.started_at else 0.0
            recalc_elapsed = (time.time() - self.recalc_started_at) if self.recalculating and self.recalc_started_at else 0.0
            last_recalc = (self.recalc_finished_at - self.recalc_started_at) if self.recalc_finished_at and self.recalc_started_at else 0.0
            cache_elapsed = (time.time() - self.cache_started_at) if self.saving_cache and self.cache_started_at else 0.0
            last_cache = (self.cache_finished_at - self.cache_started_at) if self.cache_finished_at and self.cache_started_at else 0.0
            cache_size = self.cache_path.stat().st_size if self.cache_path.exists() else 0
            return {
                "title": WEB_APP_TITLE,
                "scannerTitle": SCANNER_TITLE,
                "version": APP_VERSION,
                "root": str(self.root),
                "luck": self.luck,
                "ready": result is not None,
                "scanning": self.scanning,
                "recalculating": self.recalculating,
                "error": self.error,
                "stats": stats,
                "filters": filters,
                "warnings": warnings,
                "elapsed": elapsed,
                "lastScanSeconds": last_scan,
                "recalcElapsed": recalc_elapsed,
                "lastRecalcSeconds": last_recalc,
                "cache": {
                    "path": str(self.cache_path),
                    "exists": self.cache_path.exists(),
                    "saving": self.saving_cache,
                    "error": self.cache_error,
                    "elapsed": cache_elapsed,
                    "lastSaveSeconds": last_cache,
                    "sizeBytes": cache_size,
                },
            }

    def current_index(self) -> WebIndex | None:
        with self.lock:
            return self.index

    def current_data(self) -> tuple[WebIndex | None, ScanResult | None, int]:
        with self.lock:
            return self.index, self.result, self.luck


def filter_item_rows(index: WebIndex, params: dict[str, list[str]]) -> list[dict]:
    search = param(params, "search")
    source = param(params, "source")
    selected_map = param(params, "map", "All")
    selected_diff = param(params, "diff", "All")
    category = param(params, "category", "All")
    rarity = param(params, "rarity", "All")
    candidates, item_assets, source_keys = index.candidate_rows(search, source)
    filtered = []
    for row in candidates:
        if item_assets is not None and row["item_asset"] not in item_assets:
            continue
        if source_keys is not None and (row["source"], row["source_kind"]) not in source_keys:
            continue
        if selected_map != "All" and selected_map not in row["maps"]:
            continue
        if selected_diff != "All" and selected_diff not in row["diffs"]:
            continue
        if category != "All" and row["cat"] != category:
            continue
        if rarity != "All" and row["rarity"] != rarity:
            continue
        filtered.append(row)
    return filtered


def filter_source_base_rows(index: WebIndex, params: dict[str, list[str]]) -> list[dict]:
    source = param(params, "source")
    item = param(params, "item")
    selected_map = param(params, "map", "All")
    selected_diff = param(params, "diff", "All")
    rarity = param(params, "rarity", "All")
    candidates, item_assets, source_keys = index.candidate_rows(item, source)
    filtered = []
    for row in candidates:
        if source_keys is not None and (row["source"], row["source_kind"]) not in source_keys:
            continue
        if item_assets is not None and row["item_asset"] not in item_assets and not contains_terms(item, [row["loot_table"], row["rate_table"]]):
            continue
        if selected_map != "All" and selected_map not in row["maps"]:
            continue
        if selected_diff != "All" and selected_diff not in row["diffs"]:
            continue
        if rarity != "All" and row["rarity"] != rarity:
            continue
        filtered.append(row)
    return filtered


def source_summary(rows: list[dict]) -> list[dict]:
    grouped: dict[tuple[str, str], dict] = {}
    for row in rows:
        key = (row["source"], row["source_kind"])
        summary = grouped.get(key)
        if not summary:
            summary = {
                "source": row["source"],
                "sourceKind": row["source_kind"],
                "items": set(),
                "maps": set(),
                "diffs": set(),
                "scenarios": set(),
                "best": row,
            }
            grouped[key] = summary
        summary["items"].add(row["item_asset"])
        summary["maps"].update(row["maps"])
        summary["diffs"].update(row["diffs"])
        summary["scenarios"].add(scenario_key(row))
        if row["dyn_at_least_one"] > summary["best"]["dyn_at_least_one"]:
            summary["best"] = row

    summaries = []
    for item in grouped.values():
        best = item["best"]
        summaries.append(
            {
                "source": item["source"],
                "sourceKind": item["sourceKind"],
                "itemCount": len(item["items"]),
                "scenarioCount": len(item["scenarios"]),
                "maps": summarize_maps(item["maps"]),
                "mapValues": visible_map_values(item["maps"]),
                "diffs": summarize_diffs(item["diffs"]),
                "diffValues": visible_diff_values(item["diffs"]),
                "bestDyn": percent(best["dyn_at_least_one"]),
                "bestDynValue": best["dyn_at_least_one"],
                "topItem": best["item"],
            }
        )
    return sorted(summaries, key=lambda value: (value["source"].lower(), value["sourceKind"]))


def item_group_key(row: dict) -> tuple:
    return (
        row["item_asset"],
        row["item"],
        row["rarity"],
        row["cat"],
    )


def item_summary(rows: list[dict]) -> list[dict]:
    grouped: dict[tuple, dict] = {}
    for row in rows:
        key = item_group_key(row)
        summary = grouped.get(key)
        if not summary:
            summary = dict(row)
            summary["maps"] = set(row["maps"])
            summary["diffs"] = set(row["diffs"])
            summary["map_codes"] = set(row["map_codes"])
            summary["spawners"] = set(row.get("spawners", []))
            summary["source_values"] = {row["source"]}
            summary["source_kind_values"] = {row["source_kind"]}
            summary["_groups"] = {row["group"]}
            summary["_loot_tables"] = {row["loot_table"]}
            summary["_rate_tables"] = {row["rate_table"]}
            summary["merged_rows"] = int(row.get("merged_rows", 1) or 1)
            grouped[key] = summary
            continue
        summary["maps"].update(row["maps"])
        summary["diffs"].update(row["diffs"])
        summary["map_codes"].update(row["map_codes"])
        summary["spawners"].update(row.get("spawners", []))
        summary["source_values"].add(row["source"])
        summary["source_kind_values"].add(row["source_kind"])
        summary["_groups"].add(row["group"])
        summary["_loot_tables"].add(row["loot_table"])
        summary["_rate_tables"].add(row["rate_table"])
        summary["spawn_rate"] += row.get("spawn_rate", 0.0)
        summary["merged_rows"] += int(row.get("merged_rows", 1) or 1)
        if row["dyn_at_least_one"] > summary["dyn_at_least_one"]:
            for field in (
                "choice_count",
                "grade_choices",
                "empty_choices",
                "base_per_roll",
                "dyn_per_roll",
                "base_at_least_one",
                "dyn_at_least_one",
                "base_expected",
                "dyn_expected",
            ):
                summary[field] = row[field]

    summaries = list(grouped.values())
    for row in summaries:
        row["map"] = summarize_maps(row["maps"], limit=4)
        row["diff"] = summarize_diffs(row["diffs"], limit=4)
        row["map_code"] = summarize_map_codes(row["map_codes"], limit=8)
        row["source"] = summarize_values(row["source_values"], limit=4)
        row["source_kind"] = summarize_values(row["source_kind_values"], limit=3)
        row["group"] = summarize_values(row.pop("_groups"), limit=2)
        row["loot_table"] = summarize_values(row.pop("_loot_tables"), limit=2)
        row["rate_table"] = summarize_values(row.pop("_rate_tables"), limit=2)
        if row.get("spawners"):
            row["spawner"] = summarize_values(row["spawners"], limit=2)
    return summaries


def detail_group_key(row: dict) -> tuple:
    return (
        row["item_asset"],
        row["item"],
        row["rarity"],
        row["cat"],
        row["grade"],
        row["item_count"],
        row["rolls"],
        row["choice_count"],
        row["grade_choices"],
        row["empty_choices"],
        row.get("loot_asset", row["loot_table"]),
        row.get("rate_key", row["rate_table"]),
        round(row["base_per_roll"], 14),
        round(row["dyn_per_roll"], 14),
        round(row["base_at_least_one"], 14),
        round(row["dyn_at_least_one"], 14),
    )


def detail_summary(rows: list[dict]) -> list[dict]:
    grouped: dict[tuple, dict] = {}
    for row in rows:
        key = detail_group_key(row)
        summary = grouped.get(key)
        if not summary:
            summary = dict(row)
            summary["maps"] = set(row["maps"])
            summary["diffs"] = set(row["diffs"])
            summary["map_codes"] = set(row["map_codes"])
            summary["spawners"] = set(row.get("spawners", []))
            summary["_groups"] = {row["group"]}
            summary["_loot_tables"] = {row["loot_table"]}
            summary["_rate_tables"] = {row["rate_table"]}
            summary["merged_rows"] = int(row.get("merged_rows", 1) or 1)
            grouped[key] = summary
            continue
        summary["maps"].update(row["maps"])
        summary["diffs"].update(row["diffs"])
        summary["map_codes"].update(row["map_codes"])
        summary["spawners"].update(row.get("spawners", []))
        summary["_groups"].add(row["group"])
        summary["_loot_tables"].add(row["loot_table"])
        summary["_rate_tables"].add(row["rate_table"])
        summary["spawn_rate"] += row.get("spawn_rate", 0.0)
        summary["merged_rows"] += int(row.get("merged_rows", 1) or 1)

    summaries = list(grouped.values())
    for row in summaries:
        row["map"] = summarize_maps(row["maps"], limit=4)
        row["diff"] = summarize_diffs(row["diffs"], limit=4)
        row["map_code"] = summarize_map_codes(row["map_codes"], limit=8)
        row["group"] = summarize_values(row.pop("_groups"), limit=4)
        row["loot_table"] = summarize_values(row.pop("_loot_tables"), limit=3)
        row["rate_table"] = summarize_values(row.pop("_rate_tables"), limit=3)
        if row.get("spawners"):
            row["spawner"] = summarize_values(row["spawners"], limit=3)
    return summaries


def is_default_item_query(params: dict[str, list[str]]) -> bool:
    return (
        not param(params, "search")
        and not param(params, "source")
        and param(params, "map", "All") == "All"
        and param(params, "diff", "All") == "All"
        and param(params, "category", "All") == "All"
        and param(params, "rarity", "All") == "All"
    )


def source_summaries_for(index: WebIndex, result: ScanResult | None, luck: int, params: dict[str, list[str]]) -> list[dict]:
    source = param(params, "source")
    item = param(params, "item")
    selected_map = param(params, "map", "All")
    selected_diff = param(params, "diff", "All")
    rarity = param(params, "rarity", "All")
    source_keys = index.matching_source_keys(source)

    if luck == scan_luck(result) and not item and selected_map == "All" and selected_diff == "All" and rarity == "All":
        if source_keys is None:
            return list(index.source_summaries)
        return [
            summary
            for summary in index.source_summaries
            if (summary["source"], summary["sourceKind"]) in source_keys
        ]

    rows = rows_with_luck(filter_source_base_rows(index, params), result, luck)
    return source_summary(rows)


def filter_exact_source_rows(index: WebIndex, params: dict[str, list[str]]) -> list[dict]:
    source = unquote(param(params, "source"))
    kind = unquote(param(params, "kind"))
    selected_map = param(params, "map", "All")
    selected_diff = param(params, "diff", "All")
    item = param(params, "item")
    rarity = param(params, "rarity", "All")
    item_terms = clean_terms(item)
    filtered = []
    for row in index.source_rows.get((source, kind), []):
        if selected_map != "All" and selected_map not in row["maps"]:
            continue
        if selected_diff != "All" and selected_diff not in row["diffs"]:
            continue
        if rarity != "All" and row["rarity"] != rarity:
            continue
        if item_terms and not terms_match_text(item_terms, f"{row['item']} {row['item_asset']} {row['group']} {row['loot_table']} {row['rate_table']}".lower()):
            continue
        filtered.append(row)
    return filtered


def sort_rows(rows: list[dict], sort_key: str, descending: bool) -> list[dict]:
    allowed = {
        "item": lambda row: row["item"].lower(),
        "rarity": lambda row: row["rarity"].lower(),
        "category": lambda row: row["cat"].lower(),
        "source": lambda row: row["source"].lower(),
        "sourceKind": lambda row: row["source_kind"].lower(),
        "entries": lambda row: row.get("merged_rows", 1),
        "map": lambda row: row["map"].lower(),
        "diff": lambda row: row["diff"].lower(),
        "grade": lambda row: row["grade"],
        "count": lambda row: row["item_count"],
        "dyn": lambda row: row["dyn_at_least_one"],
        "dynPerRoll": lambda row: row["dyn_per_roll"],
        "base": lambda row: row["base_at_least_one"],
        "rolls": lambda row: row["rolls"],
        "loot": lambda row: row["loot_table"].lower(),
        "rate": lambda row: row["rate_table"].lower(),
    }
    getter = allowed.get(sort_key, allowed["dyn"])
    return sorted(rows, key=getter, reverse=descending)


def sort_source_rows(rows: list[dict], sort_key: str, descending: bool) -> list[dict]:
    allowed = {
        "source": lambda row: row["source"].lower(),
        "kind": lambda row: row["sourceKind"].lower(),
        "items": lambda row: row["itemCount"],
        "scenarios": lambda row: row["scenarioCount"],
        "maps": lambda row: row["maps"].lower(),
        "diff": lambda row: row["diffs"].lower(),
        "bestDyn": lambda row: row["bestDynValue"],
        "topItem": lambda row: row["topItem"].lower(),
    }
    getter = allowed.get(sort_key, allowed["bestDyn"])
    return sorted(rows, key=getter, reverse=descending)


def sort_detail_rows(rows: list[dict], sort_key: str, descending: bool) -> list[dict]:
    allowed = {
        "scenario": lambda row: (row["map_code"], row["map"].lower(), row["diff"].lower(), row["group"].lower()),
        "map": lambda row: row["map"].lower(),
        "diff": lambda row: row["diff"].lower(),
        "item": lambda row: row["item"].lower(),
        "rarity": lambda row: row["rarity"].lower(),
        "category": lambda row: row["cat"].lower(),
        "grade": lambda row: row["grade"],
        "count": lambda row: row["item_count"],
        "rolls": lambda row: row["rolls"],
        "dyn": lambda row: row["dyn_at_least_one"],
        "dynPerRoll": lambda row: row["dyn_per_roll"],
        "base": lambda row: row["base_at_least_one"],
        "loot": lambda row: row["loot_table"].lower(),
        "rate": lambda row: row["rate_table"].lower(),
    }
    getter = allowed.get(sort_key, allowed["dyn"])
    return sorted(rows, key=getter, reverse=descending)


def item_results_for(index: WebIndex, result: ScanResult | None, luck: int, params: dict[str, list[str]]) -> list[dict]:
    if luck == scan_luck(result) and is_default_item_query(params):
        rows = index.item_summaries
    else:
        rows = item_summary(rows_with_luck(filter_item_rows(index, params), result, luck))
    sort_key = param(params, "sort", "dyn")
    descending = param(params, "dir", "desc") != "asc"
    if rows is index.item_summaries and sort_key == "dyn" and descending:
        return index.item_summaries_by_dyn
    return sort_rows(rows, sort_key, descending)


def page(rows: list, params: dict[str, list[str]]) -> tuple[list, int, int, int]:
    limit = int_param(params, "limit", DEFAULT_LIMIT, 1, MAX_LIMIT)
    offset = int_param(params, "offset", 0, 0)
    total = len(rows)
    return rows[offset : offset + limit], total, offset, limit


INDEX_HTML = r"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>DungeonCrawler Loot Browser</title>
  <style>
    :root {
      --bg: #101312;
      --panel: #1b201d;
      --panel-2: #252c28;
      --line: #344039;
      --text: #f0eee7;
      --muted: #a7b0a9;
      --accent: #78d6b1;
      --accent-2: #e0b85f;
      --danger: #ee7c69;
      --input: #111614;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: var(--bg);
      color: var(--text);
      font-family: "Segoe UI", Arial, sans-serif;
      font-size: 14px;
      overflow-x: hidden;
      min-height: 100vh;
      display: flex;
      flex-direction: column;
    }
    button, input, select {
      font: inherit;
    }
    button {
      border: 1px solid var(--line);
      background: var(--panel-2);
      color: var(--text);
      min-height: 32px;
      border-radius: 6px;
      padding: 0 12px;
      cursor: pointer;
    }
    button.primary {
      background: #256654;
      border-color: #347e69;
      color: #ffffff;
      font-weight: 600;
    }
    button:disabled {
      opacity: .55;
      cursor: default;
    }
    input, select {
      width: 100%;
      min-height: 32px;
      border-radius: 6px;
      border: 1px solid var(--line);
      background: var(--input);
      color: var(--text);
      padding: 0 9px;
      outline: none;
    }
    input:focus, select:focus {
      border-color: var(--accent);
    }
    header {
      padding: 16px 20px 12px;
      border-bottom: 1px solid var(--line);
      background: #171c19;
      box-shadow: 0 12px 40px rgba(0, 0, 0, .18);
      position: sticky;
      top: 0;
      z-index: 5;
    }
    .title-row {
      display: flex;
      gap: 14px;
      align-items: center;
      justify-content: space-between;
      margin-bottom: 12px;
    }
    h1 {
      margin: 0;
      font-size: 18px;
      font-weight: 650;
      letter-spacing: 0;
    }
    .status {
      color: var(--accent);
      white-space: nowrap;
      font-size: 13px;
    }
    .status.error { color: var(--danger); }
    .scan-grid {
      display: grid;
      grid-template-columns: minmax(280px, 1fr) 90px auto auto auto;
      gap: 8px;
      align-items: end;
    }
    label {
      display: block;
      color: var(--muted);
      font-size: 12px;
      margin-bottom: 4px;
    }
    .tabs {
      display: flex;
      gap: 6px;
      padding: 10px 20px 0;
      background: var(--bg);
    }
    .tab {
      border-bottom-left-radius: 0;
      border-bottom-right-radius: 0;
      background: #1d1f1e;
    }
    .tab.active {
      color: #fff;
      border-color: var(--accent);
      background: var(--panel);
    }
    main {
      padding: 14px 20px 18px;
      min-width: 0;
      max-width: 100vw;
      flex: 1;
    }
    .section-head {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: end;
      margin-bottom: 10px;
    }
    .section-head h2 {
      margin: 0;
      font-size: 15px;
      font-weight: 650;
    }
    section {
      display: none;
      min-width: 0;
    }
    section.active { display: block; }
    .stats {
      display: grid;
      grid-template-columns: repeat(8, minmax(92px, 1fr));
      gap: 8px;
    }
    .stat {
      border: 1px solid var(--line);
      background: var(--panel);
      border-radius: 8px;
      padding: 6px 8px;
      min-height: 46px;
      color: var(--muted);
      font-size: 12px;
    }
    .stat b {
      display: block;
      color: var(--accent-2);
      font-size: 15px;
      margin-top: 3px;
    }
    .filters {
      display: grid;
      grid-template-columns: repeat(6, minmax(120px, 1fr));
      gap: 8px;
      margin-bottom: 10px;
      align-items: end;
    }
    .filters .wide { grid-column: span 2; }
    .table-wrap {
      border: 1px solid var(--line);
      border-radius: 8px;
      width: 100%;
      max-width: 100%;
      overflow-x: auto;
      overflow-y: auto;
      background: var(--panel);
      max-height: calc(100vh - 285px);
    }
    table {
      border-collapse: collapse;
      table-layout: fixed;
      width: max(100%, 1450px);
      min-width: 1450px;
    }
    .source-table {
      width: max(100%, 980px);
      min-width: 980px;
    }
    .item-table {
      width: max(100%, 940px);
      min-width: 940px;
    }
    .detail .table-wrap table {
      width: max(100%, 1420px);
      min-width: 1420px;
    }
    th, td {
      border-bottom: 1px solid #333734;
      padding: 8px 9px;
      text-align: left;
      vertical-align: middle;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    th {
      position: sticky;
      top: 0;
      background: #272a28;
      color: #f3f0e9;
      z-index: 1;
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0;
    }
    th.sortable {
      cursor: pointer;
      user-select: none;
    }
    th.sortable:hover {
      color: #ffffff;
      background: #303531;
    }
    th.sortable:focus {
      outline: 1px solid var(--accent);
      outline-offset: -3px;
    }
    th.sortable::after {
      content: "";
      display: inline-block;
      width: 18px;
      color: var(--accent);
      font-size: 11px;
      text-align: right;
    }
    th.sortable.sort-asc::after { content: "^"; }
    th.sortable.sort-desc::after { content: "v"; }
    tr:hover td { background: #252a27; }
    tr.clickable { cursor: pointer; }
    .num { text-align: right; font-variant-numeric: tabular-nums; }
    .muted { color: var(--muted); }
    .toolbar {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 10px;
      margin: 10px 0;
      color: var(--muted);
      flex-wrap: wrap;
    }
    .toolbar-actions {
      display: flex;
      align-items: center;
      justify-content: flex-end;
      gap: 8px;
      flex-wrap: wrap;
    }
    .inline-control {
      display: flex;
      align-items: center;
      gap: 6px;
      color: var(--muted);
      font-size: 12px;
      white-space: nowrap;
    }
    .inline-control select {
      width: 86px;
      min-height: 30px;
    }
    .detail {
      margin-top: 14px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--panel);
      overflow: hidden;
      display: none;
    }
    .detail.active { display: block; }
    .detail-head {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: center;
      padding: 10px 12px;
      border-bottom: 1px solid var(--line);
    }
    .detail-title {
      font-weight: 650;
    }
    .detail-body { padding: 10px 12px 12px; }
    .modal-backdrop {
      display: none;
      position: fixed;
      inset: 0;
      background: rgba(0, 0, 0, .55);
      align-items: center;
      justify-content: center;
      padding: 18px;
      z-index: 10;
    }
    .modal-backdrop.active { display: flex; }
    .modal {
      width: min(520px, 100%);
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #202321;
      box-shadow: 0 20px 70px rgba(0, 0, 0, .45);
      padding: 16px;
    }
    .modal h2 {
      margin: 0 0 12px;
      font-size: 17px;
      letter-spacing: 0;
    }
    .modal-grid {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 10px;
    }
    .modal-actions {
      display: flex;
      justify-content: flex-end;
      gap: 8px;
      margin-top: 14px;
    }
    .notice {
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--panel);
      padding: 12px;
      color: var(--muted);
      margin-bottom: 12px;
    }
    .app-footer {
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      gap: 12px;
      align-items: stretch;
      padding: 12px 20px 16px;
      border-top: 1px solid var(--line);
      background: #151917;
      position: sticky;
      bottom: 0;
      z-index: 4;
    }
    .cache-panel {
      display: flex;
      align-items: center;
      justify-content: flex-end;
      gap: 8px;
      min-width: 320px;
      color: var(--muted);
      font-size: 12px;
    }
    .cache-status {
      max-width: 360px;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .cache-panel button {
      white-space: nowrap;
    }
    .loading {
      opacity: .7;
    }
    @media (max-width: 900px) {
      .scan-grid, .filters, .stats, .modal-grid {
        grid-template-columns: 1fr;
      }
      .app-footer {
        grid-template-columns: 1fr;
      }
      .cache-panel {
        min-width: 0;
        justify-content: flex-start;
        flex-wrap: wrap;
      }
      .filters .wide { grid-column: span 1; }
      .title-row { align-items: flex-start; flex-direction: column; }
      .status { white-space: normal; }
    }
  </style>
</head>
<body>
  <header>
    <div class="title-row">
      <h1>DungeonCrawler Loot Browser</h1>
      <div id="status" class="status">Starting...</div>
    </div>
    <div class="scan-grid">
      <div>
        <label for="rootInput">Export root</label>
        <input id="rootInput" spellcheck="false">
      </div>
      <div>
        <label for="luckInput">Luck</label>
        <input id="luckInput" type="number" min="0" max="500" value="500">
      </div>
      <button id="scanButton" class="primary">Scan</button>
      <button id="luckButton">Apply Luck</button>
      <button id="refreshButton">Refresh</button>
    </div>
  </header>

  <nav class="tabs">
    <button class="tab active" data-tab="sources">Mob/Chest Search</button>
    <button class="tab" data-tab="items">Item Search</button>
    <button class="tab" data-tab="scan">Scan Info</button>
  </nav>

  <main>
    <div id="notReady" class="notice">Scan the export folder to load results. Large exports can take around half a minute.</div>

    <section id="sources" class="active">
      <div class="section-head">
        <h2>Mob/Chest Search</h2>
      </div>
      <div class="filters">
        <div class="wide">
          <label>Search source</label>
          <input id="sourceSearch" placeholder="Abomination, chest, barrel...">
        </div>
        <div class="wide">
          <label>Item filter</label>
          <input id="sourceItem" placeholder="Key, ore, unique item...">
        </div>
        <div>
          <label>Map</label>
          <select id="sourceMap"></select>
        </div>
        <div>
          <label>Difficulty</label>
          <select id="sourceDiff"></select>
        </div>
        <div>
          <label>Rarity</label>
          <select id="sourceRarity"></select>
        </div>
      </div>
      <div class="toolbar">
        <span id="sourceCount">0 sources</span>
        <div class="toolbar-actions">
          <label class="inline-control">Rows
            <select id="sourceLimit"><option>100</option><option>250</option><option selected>600</option><option>1000</option></select>
          </label>
          <button id="sourcePrev">Prev</button>
          <button id="sourceNext">Next</button>
          <button id="clearSourceFilters">Clear Filters</button>
        </div>
      </div>
      <div class="table-wrap">
        <table class="source-table">
          <thead>
            <tr>
              <th class="sortable" data-table="source" data-sort="source" title="Sort by source">Source</th>
              <th class="sortable" data-table="source" data-sort="kind" title="Sort by kind">Kind</th>
              <th class="num sortable" data-table="source" data-sort="items" title="Sort by item count">Items</th>
              <th class="num sortable" data-table="source" data-sort="scenarios" title="Sort by scenario count">Scenarios</th>
              <th class="sortable" data-table="source" data-sort="maps" title="Sort by maps">Maps</th>
              <th class="sortable" data-table="source" data-sort="diff" title="Sort by difficulty">Difficulties</th>
              <th>Open</th>
            </tr>
          </thead>
          <tbody id="sourceRows"></tbody>
        </table>
      </div>
      <div id="sourceDetail" class="detail">
        <div class="detail-head">
          <div>
            <div id="detailTitle" class="detail-title"></div>
            <div id="detailMeta" class="muted"></div>
          </div>
          <div>
            <button id="detailPrev">Prev</button>
            <button id="detailNext">Next</button>
            <button id="exportDetail">Export CSV</button>
            <button id="closeDetail">Close</button>
          </div>
        </div>
        <div class="detail-body">
          <div class="filters">
            <div class="wide">
              <label>Search inside drops</label>
              <input id="detailSearch" placeholder="Filter items, loot table, rate table...">
            </div>
            <div>
              <label>Rarity</label>
              <select id="detailRarity"></select>
            </div>
            <div>
              <label>Rows</label>
              <select id="detailLimit"><option>250</option><option selected>500</option><option>1000</option><option>2500</option></select>
            </div>
          </div>
          <div class="table-wrap" style="max-height: 460px">
            <table>
              <thead>
                <tr>
                  <th class="sortable" data-table="detail" data-sort="scenario" title="Sort by scenario">Scenario</th>
                  <th class="sortable" data-table="detail" data-sort="item" title="Sort by item">Item</th>
                  <th class="sortable" data-table="detail" data-sort="rarity" title="Sort by rarity">Rarity</th>
                  <th class="sortable" data-table="detail" data-sort="category" title="Sort by category">Category</th>
                  <th class="num sortable" data-table="detail" data-sort="grade" title="Sort by grade">Grade</th>
                  <th class="num sortable" data-table="detail" data-sort="count" title="Sort by count">Count</th>
                  <th class="num sortable" data-table="detail" data-sort="rolls" title="Sort by rolls">Rolls</th>
                  <th class="num sortable" data-table="detail" data-sort="base" title="Sort by base chance">Base Chance</th>
                  <th class="num sortable" data-table="detail" data-sort="dyn" title="Sort by chance with luck">Chance With Luck</th>
                  <th class="sortable" data-table="detail" data-sort="loot" title="Sort by loot table">Loot Table</th>
                  <th class="sortable" data-table="detail" data-sort="rate" title="Sort by rate table">Rate Table</th>
                </tr>
              </thead>
              <tbody id="detailRows"></tbody>
            </table>
          </div>
        </div>
      </div>
    </section>

    <section id="items">
      <div class="section-head">
        <h2>Item Search</h2>
      </div>
      <div class="filters">
        <div class="wide">
          <label>Search item</label>
          <input id="itemSearch" placeholder="Gold key, wolf pelt, falchion...">
        </div>
        <div class="wide">
          <label>Source contains</label>
          <input id="itemSource" placeholder="Monster or chest name">
        </div>
        <div>
          <label>Map</label>
          <select id="itemMap"></select>
        </div>
        <div>
          <label>Difficulty</label>
          <select id="itemDiff"></select>
        </div>
        <div>
          <label>Category</label>
          <select id="itemCategory"></select>
        </div>
        <div>
          <label>Rarity</label>
          <select id="itemRarity"></select>
        </div>
      </div>
      <div class="toolbar">
        <span id="itemCount">0 rows</span>
        <div class="toolbar-actions">
          <label class="inline-control">Rows
            <select id="itemLimit"><option>100</option><option>250</option><option selected>700</option><option>1500</option><option>3000</option></select>
          </label>
          <button id="itemPrev">Prev</button>
          <button id="itemNext">Next</button>
          <button id="exportItems">Export CSV</button>
          <button id="clearItemFilters">Clear Filters</button>
        </div>
      </div>
      <div class="table-wrap">
        <table class="item-table">
          <thead>
            <tr>
              <th class="sortable" data-table="item" data-sort="item" title="Sort by item">Item</th>
              <th class="sortable" data-table="item" data-sort="rarity" title="Sort by rarity">Rarity</th>
              <th class="sortable" data-table="item" data-sort="category" title="Sort by category">Category</th>
              <th class="sortable" data-table="item" data-sort="map" title="Sort by map">Map</th>
              <th class="sortable" data-table="item" data-sort="diff" title="Sort by difficulty">Difficulty</th>
              <th>Open</th>
            </tr>
          </thead>
          <tbody id="itemRows"></tbody>
        </table>
      </div>
    </section>

    <section id="scan">
      <div class="section-head">
        <h2>Scan Info</h2>
      </div>
      <div class="notice" id="warnings"></div>
    </section>
  </main>

  <footer class="app-footer">
    <div class="stats" id="stats"></div>
    <div class="cache-panel">
      <button id="saveCacheButton">Save Scan Cache</button>
      <span id="cacheStatus" class="cache-status">No cache saved</span>
    </div>
  </footer>

  <div id="scenarioModal" class="modal-backdrop">
    <div class="modal">
      <h2 id="modalTitle">Choose Scenario</h2>
      <div class="modal-grid">
        <div>
          <label>Map</label>
          <select id="modalMap"></select>
        </div>
        <div>
          <label>Difficulty</label>
          <select id="modalDiff"></select>
        </div>
        <div style="grid-column: 1 / -1">
          <label>Optional item search</label>
          <input id="modalItem" placeholder="Leave empty to show everything">
        </div>
      </div>
      <div id="modalCount" class="muted" style="margin-top: 10px"></div>
      <div class="modal-actions">
        <button id="modalCancel">Cancel</button>
        <button id="modalOpen" class="primary">Open Drops</button>
      </div>
    </div>
  </div>

  <script>
    const state = {
      ready: false,
      activeTab: "sources",
      filters: { maps: [], diffs: [], categories: [], rarities: [] },
      selectedSource: null,
      selectedScenario: null,
      sourceOffset: 0,
      itemOffset: 0,
      detailOffset: 0,
      sourceSort: { key: "source", dir: "asc" },
      itemSort: { key: "item", dir: "asc" },
      detailSort: { key: "dyn", dir: "desc" },
      sourceRequest: 0,
      itemRequest: 0,
      detailRequest: 0,
      lastStatus: null,
    };

    const $ = (id) => document.getElementById(id);

    function debounce(fn, wait = 220) {
      let timer = null;
      return (...args) => {
        clearTimeout(timer);
        timer = setTimeout(() => fn(...args), wait);
      };
    }

    async function api(path, options) {
      const response = await fetch(path, options);
      if (!response.ok) {
        const text = await response.text();
        throw new Error(text || response.statusText);
      }
      return response.json();
    }

    function setStatus(text, isError = false) {
      $("status").textContent = text;
      $("status").classList.toggle("error", isError);
    }

    function fillSelect(id, values, allLabel = "All") {
      const select = $(id);
      const current = select.value || allLabel;
      select.innerHTML = "";
      [allLabel, ...values].forEach((value) => {
        const option = document.createElement("option");
        option.value = value;
        option.textContent = value;
        select.appendChild(option);
      });
      select.value = [...select.options].some((option) => option.value === current) ? current : allLabel;
    }

    function number(value) {
      return Number(value || 0).toLocaleString();
    }

    function rangeLabel(total, offset, shown, noun) {
      if (!total) return `0 ${noun}`;
      if (!shown) return `${number(total)} ${noun}, no rows on this page`;
      const first = offset + 1;
      const last = offset + shown;
      return `${number(total)} ${noun}, showing ${number(first)}-${number(last)}`;
    }

    function setPager(prefix, offset, shown, total) {
      $(`${prefix}Prev`).disabled = offset <= 0;
      $(`${prefix}Next`).disabled = offset + shown >= total;
    }

    function defaultSortDir(key) {
      return new Set(["dyn", "dynPerRoll", "base", "items", "scenarios", "entries", "grade", "count", "rolls"]).has(key) ? "desc" : "asc";
    }

    function sortState(table) {
      return state[`${table}Sort`];
    }

    function updateSortHeaders() {
      document.querySelectorAll("th.sortable").forEach((th) => {
        const current = sortState(th.dataset.table);
        const active = current && current.key === th.dataset.sort;
        th.classList.toggle("sort-asc", active && current.dir === "asc");
        th.classList.toggle("sort-desc", active && current.dir === "desc");
        th.setAttribute("aria-sort", active ? (current.dir === "asc" ? "ascending" : "descending") : "none");
      });
    }

    function setSort(table, key) {
      const current = sortState(table);
      if (!current) return;
      if (current.key === key) {
        current.dir = current.dir === "asc" ? "desc" : "asc";
      } else {
        current.key = key;
        current.dir = defaultSortDir(key);
      }
      if (table === "source") {
        state.sourceOffset = 0;
        loadSources();
      } else if (table === "item") {
        state.itemOffset = 0;
        loadItems();
      } else if (table === "detail") {
        state.detailOffset = 0;
        loadDetail();
      }
      updateSortHeaders();
    }

    function tableMessage(tbodyId, colspan, text) {
      $(tbodyId).innerHTML = `<tr><td colspan="${colspan}" class="muted">${escapeHtml(text)}</td></tr>`;
    }

    function statCards(stats) {
      const pairs = [
        ["Loot Tables", stats.loot_tables],
        ["Rate Tables", stats.rate_tables],
        ["Groups", stats.groups],
        ["Dungeon Codes", stats.dungeon_codes],
        ["Sources", stats.sources],
        ["Items", stats.items],
        ["Rows", stats.rows],
        ["Warnings", stats.warnings],
      ];
      return pairs.map(([label, value]) => `<div class="stat"><span>${label}</span><b>${number(value)}</b></div>`).join("");
    }

    function formatBytes(bytes) {
      const value = Number(bytes || 0);
      if (!value) return "0 B";
      const units = ["B", "KB", "MB", "GB"];
      let size = value;
      let unit = 0;
      while (size >= 1024 && unit < units.length - 1) {
        size /= 1024;
        unit += 1;
      }
      return `${size >= 10 || unit === 0 ? size.toFixed(0) : size.toFixed(1)} ${units[unit]}`;
    }

    function cacheFileName(path) {
      return String(path || "loot_spawn_cache.pkl.gz").split(/[\\/]/).pop();
    }

    function updateCacheStatus(cache = {}) {
      const button = $("saveCacheButton");
      const label = $("cacheStatus");
      button.disabled = !state.ready || Boolean(cache.saving);
      button.textContent = cache.saving ? "Saving..." : "Save Scan Cache";
      label.style.color = "";
      if (cache.saving) {
        label.textContent = `Saving cache... ${Math.round(cache.elapsed || 0)}s`;
      } else if (cache.error) {
        label.textContent = "Cache save failed";
        label.style.color = "var(--danger)";
      } else if (cache.exists) {
        label.textContent = `${cacheFileName(cache.path)} saved (${formatBytes(cache.sizeBytes)})`;
      } else {
        label.textContent = "No cache saved yet";
      }
      label.title = cache.path || "";
    }

    async function loadStatus(refreshData = true) {
      const data = await api("/api/status");
      state.lastStatus = data;
      state.ready = data.ready;
      state.filters = data.filters;
      $("rootInput").value = data.root;
      $("luckInput").value = data.luck;
      $("notReady").style.display = data.ready ? "none" : "block";
      $("scanButton").disabled = data.scanning || data.recalculating;
      $("luckButton").disabled = data.scanning || data.recalculating || !data.ready;
      updateCacheStatus(data.cache || {});
      if (data.scanning) {
        setStatus(`Scanning... ${Math.round(data.elapsed)}s`);
      } else if (data.recalculating) {
        setStatus(`Updating luck... ${Math.round(data.recalcElapsed)}s`);
      } else if (data.error) {
        setStatus("Scan failed. See Scan Info.", true);
      } else if (data.ready) {
        const luckBit = data.lastRecalcSeconds ? ` Luck update ${Math.round(data.lastRecalcSeconds)}s.` : "";
        setStatus(`Ready. Last scan ${Math.round(data.lastScanSeconds)}s.${luckBit}`);
      } else {
        setStatus("Ready to scan.");
      }
      $("stats").innerHTML = statCards(data.stats || {});
      $("warnings").textContent = data.error || (data.warnings && data.warnings.length ? data.warnings.join("\n") : "No warnings.");
      fillSelect("sourceMap", data.filters.maps || []);
      fillSelect("itemMap", data.filters.maps || []);
      fillSelect("sourceDiff", data.filters.diffs || []);
      fillSelect("itemDiff", data.filters.diffs || []);
      fillSelect("itemCategory", data.filters.categories || []);
      fillSelect("sourceRarity", data.filters.rarities || []);
      fillSelect("itemRarity", data.filters.rarities || []);
      fillSelect("detailRarity", data.filters.rarities || []);
      if (data.ready) {
        if (refreshData) {
          if (state.activeTab === "items") {
            loadItems();
          } else if (state.activeTab === "sources") {
            loadSources();
          }
          if (state.selectedScenario && $("sourceDetail").classList.contains("active")) {
            loadDetail();
          }
        }
      } else {
        setPager("source", 0, 0, 0);
        setPager("item", 0, 0, 0);
        setPager("detail", 0, 0, 0);
      }
      if (data.scanning) {
        setTimeout(() => loadStatus(refreshData), 1200);
      } else if (data.recalculating) {
        setTimeout(() => loadStatus(refreshData), 700);
      }
    }

    function sourceQuery() {
      const params = new URLSearchParams();
      params.set("source", $("sourceSearch").value);
      params.set("item", $("sourceItem").value);
      params.set("map", $("sourceMap").value || "All");
      params.set("diff", $("sourceDiff").value || "All");
      params.set("rarity", $("sourceRarity").value || "All");
      params.set("limit", $("sourceLimit").value || "600");
      params.set("offset", state.sourceOffset);
      params.set("sort", state.sourceSort.key);
      params.set("dir", state.sourceSort.dir);
      return params;
    }

    async function loadSources() {
      if (!state.ready) return;
      const requestId = ++state.sourceRequest;
      $("sourceCount").textContent = "Loading sources...";
      $("sourceRows").classList.add("loading");
      try {
        const data = await api(`/api/sources?${sourceQuery().toString()}`);
        if (requestId !== state.sourceRequest) return;
        $("sourceRows").classList.remove("loading");
        $("sourceCount").textContent = rangeLabel(data.total, data.offset, data.rows.length, "sources");
        setPager("source", data.offset, data.rows.length, data.total);
        if (!data.rows.length) {
          tableMessage("sourceRows", 7, "No mob/chest sources match the current filters.");
          return;
        }
        $("sourceRows").innerHTML = data.rows.map((row) => `
          <tr class="clickable" data-source="${encodeURIComponent(row.source)}" data-kind="${encodeURIComponent(row.sourceKind)}">
            <td title="${escapeAttr(row.source)}">${escapeHtml(row.source)}</td>
            <td>${escapeHtml(row.sourceKind)}</td>
            <td class="num">${number(row.itemCount)}</td>
            <td class="num">${number(row.scenarioCount)}</td>
            <td>${escapeHtml(row.maps)}</td>
            <td>${escapeHtml(row.diffs)}</td>
            <td><button class="open-source" data-source="${encodeURIComponent(row.source)}" data-kind="${encodeURIComponent(row.sourceKind)}">Open</button></td>
          </tr>
        `).join("");
        [...$("sourceRows").querySelectorAll("tr")].forEach((tr) => {
          tr.addEventListener("dblclick", () => openScenarioModal(decodeURIComponent(tr.dataset.source), decodeURIComponent(tr.dataset.kind)));
        });
        [...$("sourceRows").querySelectorAll("button.open-source")].forEach((button) => {
          button.addEventListener("click", (event) => {
            event.stopPropagation();
            openScenarioModal(decodeURIComponent(button.dataset.source), decodeURIComponent(button.dataset.kind));
          });
        });
      } catch (error) {
        if (requestId !== state.sourceRequest) return;
        $("sourceRows").classList.remove("loading");
        $("sourceCount").textContent = "Could not load sources";
        setPager("source", 0, 0, 0);
        tableMessage("sourceRows", 7, error.message);
      }
    }

    function itemQuery(forCsv = false) {
      const params = new URLSearchParams();
      params.set("search", $("itemSearch").value);
      params.set("source", $("itemSource").value);
      params.set("map", $("itemMap").value || "All");
      params.set("diff", $("itemDiff").value || "All");
      params.set("category", $("itemCategory").value || "All");
      params.set("rarity", $("itemRarity").value || "All");
      params.set("limit", forCsv ? "5000" : ($("itemLimit").value || "700"));
      params.set("offset", forCsv ? "0" : state.itemOffset);
      params.set("sort", state.itemSort.key);
      params.set("dir", state.itemSort.dir);
      return params;
    }

    async function loadItems() {
      if (!state.ready) return;
      const requestId = ++state.itemRequest;
      $("itemCount").textContent = "Loading items...";
      $("itemRows").classList.add("loading");
      try {
        const data = await api(`/api/items?${itemQuery().toString()}`);
        if (requestId !== state.itemRequest) return;
        $("itemRows").classList.remove("loading");
        const itemLabel = data.grouped ? "grouped rows" : "rows";
        $("itemCount").textContent = rangeLabel(data.total, data.offset, data.rows.length, itemLabel);
        setPager("item", data.offset, data.rows.length, data.total);
        if (!data.rows.length) {
          tableMessage("itemRows", 6, "No items match the current filters.");
          return;
        }
        $("itemRows").innerHTML = data.rows.map((row) => `
          <tr data-source="${encodeURIComponent(row.source)}" data-kind="${encodeURIComponent(row.sourceKind)}" data-item="${encodeURIComponent(row.item)}">
            <td>${escapeHtml(row.item)}</td>
            <td>${escapeHtml(row.rarity)}</td>
            <td>${escapeHtml(row.category)}</td>
            <td title="${escapeAttr(row.maps.join(', '))}">${escapeHtml(row.map)}</td>
            <td title="${escapeAttr(row.diffs.join(', '))}">${escapeHtml(row.diff)}</td>
            <td><button class="open-item-source" data-source="${encodeURIComponent(row.source)}" data-kind="${encodeURIComponent(row.sourceKind)}" data-item="${encodeURIComponent(row.item)}" data-source-count="${row.sourceCount}">${row.sourceCount > 1 ? "Sources" : "Open"}</button></td>
          </tr>
        `).join("");
        [...$("itemRows").querySelectorAll("button.open-item-source")].forEach((button) => {
          button.addEventListener("click", () => {
            const itemName = decodeURIComponent(button.dataset.item);
            if (Number(button.dataset.sourceCount || 0) > 1) {
              activateTab("sources");
              $("sourceSearch").value = "";
              $("sourceItem").value = itemName;
              $("sourceMap").value = $("itemMap").value || "All";
              $("sourceDiff").value = $("itemDiff").value || "All";
              $("sourceRarity").value = $("itemRarity").value || "All";
              state.sourceOffset = 0;
              loadSources();
              return;
            }
            openScenarioModal(decodeURIComponent(button.dataset.source), decodeURIComponent(button.dataset.kind), itemName);
          });
        });
      } catch (error) {
        if (requestId !== state.itemRequest) return;
        $("itemRows").classList.remove("loading");
        $("itemCount").textContent = "Could not load items";
        setPager("item", 0, 0, 0);
        tableMessage("itemRows", 6, error.message);
      }
    }

    async function openScenarioModal(source, kind, itemFilter = "") {
      state.selectedSource = { source, kind };
      $("modalTitle").textContent = source;
      const selectedItemFilter = itemFilter || $("sourceItem").value;
      $("modalItem").value = selectedItemFilter;
      const params = new URLSearchParams({ source, kind });
      if (selectedItemFilter) params.set("item", selectedItemFilter);
      $("scenarioModal").classList.add("active");
      $("modalOpen").disabled = true;
      $("modalCount").textContent = "Loading scenario choices...";
      try {
        const data = await api(`/api/source-options?${params.toString()}`);
        fillModalSelect("modalMap", data.maps);
        fillModalSelect("modalDiff", data.diffs);
        $("modalCount").textContent = `${number(data.total)} matching rows before scenario filters`;
        $("modalOpen").disabled = !data.total;
      } catch (error) {
        $("modalCount").textContent = error.message;
        setStatus("Could not load scenario choices.", true);
      }
    }

    function fillModalSelect(id, values) {
      const select = $(id);
      select.innerHTML = "";
      ["All", ...values].forEach((value) => {
        const option = document.createElement("option");
        option.value = value;
        option.textContent = value;
        select.appendChild(option);
      });
    }

    async function openDetail() {
      const selected = state.selectedSource;
      if (!selected) return;
      state.selectedScenario = {
        source: selected.source,
        kind: selected.kind,
        map: $("modalMap").value || "All",
        diff: $("modalDiff").value || "All",
        item: $("modalItem").value || "",
      };
      state.detailOffset = 0;
      $("detailSearch").value = state.selectedScenario.item;
      $("scenarioModal").classList.remove("active");
      $("sourceDetail").classList.add("active");
      await loadDetail();
      $("sourceDetail").scrollIntoView({ block: "start", behavior: "smooth" });
    }

    function detailQuery(forCsv = false) {
      const scenario = state.selectedScenario;
      const params = new URLSearchParams();
      params.set("source", scenario.source);
      params.set("kind", scenario.kind);
      params.set("map", scenario.map);
      params.set("diff", scenario.diff);
      params.set("item", $("detailSearch").value || scenario.item || "");
      params.set("rarity", $("detailRarity").value || "All");
      params.set("limit", forCsv ? "5000" : ($("detailLimit").value || "500"));
      params.set("offset", forCsv ? "0" : state.detailOffset);
      params.set("sort", state.detailSort.key);
      params.set("dir", state.detailSort.dir);
      return params;
    }

    async function loadDetail() {
      if (!state.selectedScenario) return;
      const requestId = ++state.detailRequest;
      $("detailMeta").textContent = "Loading drops...";
      $("detailRows").classList.add("loading");
      try {
        const data = await api(`/api/source-drops?${detailQuery().toString()}`);
        if (requestId !== state.detailRequest) return;
        $("detailRows").classList.remove("loading");
        const sc = state.selectedScenario;
        $("detailTitle").textContent = sc.source;
        $("detailMeta").textContent = `${sc.map} / ${sc.diff} - ${rangeLabel(data.total, data.offset, data.rows.length, "drops")}`;
        setPager("detail", data.offset, data.rows.length, data.total);
        if (!data.rows.length) {
          tableMessage("detailRows", 11, "No drops match this scenario.");
          return;
        }
        $("detailRows").innerHTML = data.rows.map((row) => {
          const scenario = `${row.mapCode} ${row.map} | ${row.diff} | ${row.group}`;
          return `
            <tr>
              <td>${escapeHtml(scenario)}</td>
              <td>${escapeHtml(row.item)}</td>
              <td>${escapeHtml(row.rarity)}</td>
              <td>${escapeHtml(row.category)}</td>
              <td class="num">G${row.grade}</td>
              <td class="num">${row.itemCount}</td>
              <td class="num">${row.rolls}</td>
              <td class="num">${row.baseAtLeastOne}</td>
              <td class="num">${row.dynAtLeastOne}</td>
              <td>${escapeHtml(row.lootTable)}</td>
              <td>${escapeHtml(row.rateTable)}</td>
            </tr>
          `;
        }).join("");
      } catch (error) {
        if (requestId !== state.detailRequest) return;
        $("detailRows").classList.remove("loading");
        $("detailMeta").textContent = "Could not load drops";
        setPager("detail", 0, 0, 0);
        tableMessage("detailRows", 11, error.message);
      }
    }

    function sourceHasFocusedQuery() {
      return Boolean(
        $("sourceSearch").value.trim()
        || $("sourceItem").value.trim()
        || ($("sourceMap").value && $("sourceMap").value !== "All")
        || ($("sourceDiff").value && $("sourceDiff").value !== "All")
        || ($("sourceRarity").value && $("sourceRarity").value !== "All")
      );
    }

    async function reloadActiveViewAfterLuck() {
      if ($("sourceDetail").classList.contains("active") && state.selectedScenario) {
        await loadDetail();
        return "Current drops updated.";
      }
      if (state.activeTab === "items") {
        await loadItems();
        return "Current item search updated.";
      }
      if (state.activeTab === "sources" && sourceHasFocusedQuery()) {
        await loadSources();
        return "Filtered source search updated.";
      }
      $("sourceCount").textContent = "Luck saved. Open a source or search/filter to calculate focused chances.";
      return "Open a source or filter the list to calculate focused chances.";
    }

    async function saveCache() {
      if (!state.ready) return;
      try {
        setStatus("Saving scan cache...");
        const data = await api("/api/cache/save", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({}),
        });
        state.lastStatus = data;
        updateCacheStatus(data.cache || {});
        pollCacheSave();
      } catch (error) {
        setStatus(error.message, true);
      }
    }

    async function pollCacheSave() {
      try {
        const data = await api("/api/status");
        state.lastStatus = data;
        updateCacheStatus(data.cache || {});
        if (data.cache && data.cache.saving) {
          setTimeout(pollCacheSave, 700);
        } else if (data.cache && data.cache.error) {
          $("warnings").textContent = data.cache.error;
          setStatus("Cache save failed. See Scan Info.", true);
        } else if (data.cache && data.cache.exists) {
          setStatus("Scan cache saved.");
        }
      } catch (error) {
        setStatus(error.message, true);
      }
    }

    async function startScan() {
      try {
        state.sourceOffset = 0;
        state.itemOffset = 0;
        state.detailOffset = 0;
        setStatus("Starting scan...");
        await api("/api/scan", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ root: $("rootInput").value, luck: Number($("luckInput").value || 0) }),
        });
        await loadStatus(true);
      } catch (error) {
        setStatus(error.message, true);
      }
    }

    async function applyLuck() {
      if (!state.ready) return;
      try {
        const luck = Number($("luckInput").value || 0);
        setStatus("Applying luck to the current view...");
        await api("/api/luck", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ luck }),
        });
        await loadStatus(false);
        const note = await reloadActiveViewAfterLuck();
        setStatus(`Luck set to ${luck}. ${note}`);
      } catch (error) {
        setStatus(error.message, true);
      }
    }

    function escapeHtml(value) {
      return String(value ?? "").replace(/[&<>"']/g, (char) => ({
        "&": "&amp;",
        "<": "&lt;",
        ">": "&gt;",
        '"': "&quot;",
        "'": "&#39;",
      }[char]));
    }

    function escapeAttr(value) {
      return escapeHtml(value).replace(/`/g, "&#96;");
    }

    function activateTab(name) {
      state.activeTab = name;
      document.querySelectorAll(".tab").forEach((button) => button.classList.toggle("active", button.dataset.tab === name));
      document.querySelectorAll("main > section").forEach((section) => section.classList.toggle("active", section.id === name));
      if (state.ready) {
        if (name === "sources") loadSources();
        if (name === "items") loadItems();
      }
    }

    const refreshSources = debounce(() => {
      state.sourceOffset = 0;
      loadSources();
      updateSortHeaders();
    });
    const refreshItems = debounce(() => {
      state.itemOffset = 0;
      loadItems();
      updateSortHeaders();
    });
    const refreshDetail = debounce(() => {
      state.detailOffset = 0;
      loadDetail();
      updateSortHeaders();
    });

    document.querySelectorAll(".tab").forEach((button) => button.addEventListener("click", () => activateTab(button.dataset.tab)));
    document.querySelectorAll("th.sortable").forEach((th) => {
      th.addEventListener("click", () => setSort(th.dataset.table, th.dataset.sort));
      th.tabIndex = 0;
      th.addEventListener("keydown", (event) => {
        if (event.key === "Enter" || event.key === " ") {
          event.preventDefault();
          setSort(th.dataset.table, th.dataset.sort);
        }
      });
    });
    $("scanButton").addEventListener("click", startScan);
    $("luckButton").addEventListener("click", applyLuck);
    $("saveCacheButton").addEventListener("click", saveCache);
    $("refreshButton").addEventListener("click", () => loadStatus(true).catch((error) => setStatus(error.message, true)));
    ["sourceSearch", "sourceItem", "sourceMap", "sourceDiff", "sourceRarity", "sourceLimit"].forEach((id) => $(id).addEventListener("input", refreshSources));
    ["itemSearch", "itemSource", "itemMap", "itemDiff", "itemCategory", "itemRarity", "itemLimit"].forEach((id) => $(id).addEventListener("input", refreshItems));
    ["detailSearch", "detailRarity", "detailLimit"].forEach((id) => $(id).addEventListener("input", refreshDetail));
    $("sourcePrev").addEventListener("click", () => {
      state.sourceOffset = Math.max(0, state.sourceOffset - Number($("sourceLimit").value || 600));
      loadSources();
    });
    $("sourceNext").addEventListener("click", () => {
      state.sourceOffset += Number($("sourceLimit").value || 600);
      loadSources();
    });
    $("itemPrev").addEventListener("click", () => {
      state.itemOffset = Math.max(0, state.itemOffset - Number($("itemLimit").value || 700));
      loadItems();
    });
    $("itemNext").addEventListener("click", () => {
      state.itemOffset += Number($("itemLimit").value || 700);
      loadItems();
    });
    $("detailPrev").addEventListener("click", () => {
      state.detailOffset = Math.max(0, state.detailOffset - Number($("detailLimit").value || 500));
      loadDetail();
    });
    $("detailNext").addEventListener("click", () => {
      state.detailOffset += Number($("detailLimit").value || 500);
      loadDetail();
    });
    $("clearSourceFilters").addEventListener("click", () => {
      ["sourceSearch", "sourceItem"].forEach((id) => $(id).value = "");
      ["sourceMap", "sourceDiff", "sourceRarity"].forEach((id) => $(id).value = "All");
      state.sourceOffset = 0;
      loadSources();
    });
    $("clearItemFilters").addEventListener("click", () => {
      ["itemSearch", "itemSource"].forEach((id) => $(id).value = "");
      ["itemMap", "itemDiff", "itemCategory", "itemRarity"].forEach((id) => $(id).value = "All");
      state.itemOffset = 0;
      loadItems();
    });
    $("modalCancel").addEventListener("click", () => $("scenarioModal").classList.remove("active"));
    $("modalOpen").addEventListener("click", openDetail);
    $("closeDetail").addEventListener("click", () => $("sourceDetail").classList.remove("active"));
    $("exportDetail").addEventListener("click", () => {
      if (!state.selectedScenario) return;
      window.location.href = `/api/export/source-drops.csv?${detailQuery(true).toString()}`;
    });
    $("exportItems").addEventListener("click", () => {
      if (!state.ready) return;
      window.location.href = `/api/export/items.csv?${itemQuery(true).toString()}`;
    });
    window.addEventListener("keydown", (event) => {
      if (event.key === "Escape") $("scenarioModal").classList.remove("active");
    });

    updateSortHeaders();
    loadStatus().catch((error) => setStatus(error.message, true));
  </script>
</body>
</html>
"""


class LootWebHandler(BaseHTTPRequestHandler):
    state: AppState

    def log_message(self, format: str, *args) -> None:
        return

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)
        try:
            if parsed.path in ("/", "/index.html"):
                self.send_bytes(INDEX_HTML.encode("utf-8"), "text/html; charset=utf-8")
            elif parsed.path == "/api/status":
                self.send_json(self.state.snapshot())
            elif parsed.path == "/api/items":
                self.handle_items(params)
            elif parsed.path == "/api/sources":
                self.handle_sources(params)
            elif parsed.path == "/api/source-options":
                self.handle_source_options(params)
            elif parsed.path == "/api/source-drops":
                self.handle_source_drops(params)
            elif parsed.path == "/api/export/source-drops.csv":
                index, result, luck = self.state.current_data()
                rows = sort_detail_rows(detail_summary(rows_with_luck(filter_exact_source_rows(index, params), result, luck)), param(params, "sort", "dyn"), param(params, "dir", "desc") != "asc") if index else []
                filename = quote("source_drops.csv")
                self.send_bytes(csv_rows(rows), "text/csv; charset=utf-8", extra_headers={"Content-Disposition": f"attachment; filename={filename}"})
            elif parsed.path == "/api/export/items.csv":
                index, result, luck = self.state.current_data()
                rows = item_results_for(index, result, luck, params) if index else []
                filename = quote("item_results.csv")
                self.send_bytes(csv_rows(rows), "text/csv; charset=utf-8", extra_headers={"Content-Disposition": f"attachment; filename={filename}"})
            else:
                self.send_error(HTTPStatus.NOT_FOUND)
        except Exception:
            self.send_text(traceback.format_exc(), HTTPStatus.INTERNAL_SERVER_ERROR)

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        try:
            if parsed.path not in ("/api/scan", "/api/luck", "/api/cache/save"):
                self.send_error(HTTPStatus.NOT_FOUND)
                return
            length = int(self.headers.get("Content-Length", "0") or 0)
            payload = json.loads(self.rfile.read(length).decode("utf-8") or "{}")
            if parsed.path == "/api/scan":
                root = Path(payload.get("root") or self.state.root)
                luck = int(payload.get("luck", self.state.luck) or 0)
                started = self.state.start_scan(root, luck)
            elif parsed.path == "/api/luck":
                luck = int(payload.get("luck", self.state.luck) or 0)
                started = self.state.start_recalculate_luck(luck)
            else:
                path = payload.get("path")
                started = self.state.start_save_cache(Path(path) if path else None)
            self.send_json({"started": started, **self.state.snapshot()})
        except Exception:
            self.send_text(traceback.format_exc(), HTTPStatus.INTERNAL_SERVER_ERROR)

    def handle_items(self, params: dict[str, list[str]]) -> None:
        index, result, luck = self.state.current_data()
        if not index:
            self.send_json({"total": 0, "offset": 0, "limit": DEFAULT_LIMIT, "rows": []})
            return
        grouped_items = True
        rows = item_results_for(index, result, luck, params)
        selected, total, offset, limit = page(rows, params)
        self.send_json({"total": total, "offset": offset, "limit": limit, "grouped": grouped_items, "rows": [compact_row(row) for row in selected]})

    def handle_sources(self, params: dict[str, list[str]]) -> None:
        index, result, luck = self.state.current_data()
        if not index:
            self.send_json({"total": 0, "offset": 0, "limit": DEFAULT_LIMIT, "rows": []})
            return
        summaries = source_summaries_for(index, result, luck, params)
        summaries = sort_source_rows(summaries, param(params, "sort", "source"), param(params, "dir", "asc") != "asc")
        selected, total, offset, limit = page(summaries, params)
        self.send_json({"total": total, "offset": offset, "limit": limit, "rows": selected})

    def handle_source_options(self, params: dict[str, list[str]]) -> None:
        index = self.state.current_index()
        rows = filter_exact_source_rows(index, params) if index else []
        maps = visible_map_values({map_name for row in rows for map_name in row["maps"]})
        diffs = visible_diff_values({diff for row in rows for diff in row["diffs"]})
        self.send_json({"total": len(rows), "maps": maps, "diffs": diffs})

    def handle_source_drops(self, params: dict[str, list[str]]) -> None:
        index, result, luck = self.state.current_data()
        if not index:
            self.send_json({"total": 0, "offset": 0, "limit": DEFAULT_LIMIT, "rows": []})
            return
        rows = detail_summary(rows_with_luck(filter_exact_source_rows(index, params), result, luck))
        rows = sort_detail_rows(rows, param(params, "sort", "dyn"), param(params, "dir", "desc") != "asc")
        selected, total, offset, limit = page(rows, params)
        self.send_json({"total": total, "offset": offset, "limit": limit, "rows": [compact_row(row) for row in selected]})

    def send_json(self, value: dict, status: HTTPStatus = HTTPStatus.OK) -> None:
        self.send_bytes(json.dumps(value, ensure_ascii=False).encode("utf-8"), "application/json; charset=utf-8", status)

    def send_text(self, value: str, status: HTTPStatus = HTTPStatus.OK) -> None:
        self.send_bytes(value.encode("utf-8"), "text/plain; charset=utf-8", status)

    def send_bytes(self, body: bytes, content_type: str, status: HTTPStatus = HTTPStatus.OK, extra_headers: dict[str, str] | None = None) -> None:
        self.send_response(status.value)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        for key, value in (extra_headers or {}).items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(body)


def make_handler(state: AppState):
    class Handler(LootWebHandler):
        pass

    Handler.state = state
    return Handler


def running_server_status(host: str, port: int) -> dict | None:
    try:
        with urllib.request.urlopen(f"http://{host}:{port}/api/status", timeout=0.8) as response:
            return json.loads(response.read().decode("utf-8"))
    except (OSError, ValueError, urllib.error.URLError):
        return None


def create_server(host: str, port: int, state: AppState) -> tuple[ThreadingHTTPServer | None, int, bool]:
    handler = make_handler(state)
    for candidate in range(port, port + 25):
        existing = running_server_status(host, candidate)
        if existing:
            return None, candidate, True
        try:
            return ThreadingHTTPServer((host, candidate), handler), candidate, False
        except OSError:
            continue
    raise OSError(f"Could not bind a local server on ports {port}-{port + 24}.")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=f"{WEB_APP_TITLE} {APP_VERSION}")
    parser.add_argument("root", nargs="?", default=".", help="Export root, Content folder, or Generated/V2 folder")
    parser.add_argument("--luck", type=int, default=500)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--cache", type=Path, default=DEFAULT_CACHE_FILE, help="Saved scan cache file")
    parser.add_argument("--no-cache-load", action="store_true", help="Do not load the saved scan cache on startup")
    parser.add_argument("--auto-scan", action="store_true", help="Start scanning as soon as the server launches")
    parser.add_argument("--open", action="store_true", help="Open the browser automatically")
    args = parser.parse_args(argv)

    state = AppState(Path(args.root).resolve(), args.luck, args.cache)
    loaded_cache = False
    if not args.no_cache_load:
        loaded_cache = state.load_cache(args.cache)
    server, port, reused_existing = create_server(args.host, args.port, state)
    url = f"http://{args.host}:{port}/"
    if reused_existing:
        print(f"{WEB_APP_TITLE} is already running: {url}")
        if args.open:
            webbrowser.open(url)
        return 0

    if args.auto_scan and not loaded_cache:
        state.start_scan(state.root, args.luck)

    print(f"{WEB_APP_TITLE} {APP_VERSION}: {url}")
    if loaded_cache:
        print(f"Loaded scan cache: {args.cache}")
    print("Press Ctrl+C to stop the server.")
    if args.open:
        threading.Timer(0.8, lambda: webbrowser.open(url)).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping server.")
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
