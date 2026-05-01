from __future__ import annotations

import argparse
import csv
import json
import math
import os
import re
import sys
import threading
import traceback
from collections import Counter, defaultdict
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

try:
    from tkinter import filedialog, messagebox
    import tkinter as tk
    from tkinter import ttk
except Exception:
    filedialog = None
    messagebox = None
    tk = None
    ttk = None

try:
    from tkinterdnd2 import DND_FILES, TkinterDnD
except Exception:
    DND_FILES = None
    TkinterDnD = None


APP_TITLE = "DungeonCrawler Loot Spawn Analyzer"
CONFIG_FILE = "loot_spawn_analyzer_config.json"
MAX_LUCK_GRADE = 8

RARITY_CODES = {
    "1001": "Junk",
    "2001": "Common",
    "3001": "Uncommon",
    "4001": "Rare",
    "5001": "Epic",
    "6001": "Legendary",
    "7001": "Unique",
    "8001": "Artifact",
}

DIFFICULTY_CODES = {
    "1": "PVE",
    "2": "Normal",
    "3": "High Roller",
    "4": "Squire",
}

AREA_CODES = {
    "01": "Goblin Caves",
    "02": "Fire Deep",
    "11": "Ice Cavern",
    "12": "Ice Abyss",
    "21": "Ruins",
    "22": "Crypts",
    "23": "Inferno",
    "31": "Ship Graveyard",
}

AREA_DISPLAY_NAMES = {
    "Goblin Caves": "Goblin Caves",
    "Fire Deep": "Fire Deep",
    "Ice Cavern": "Ice Cavern",
    "Ice Abyss": "Ice Abyss",
    "Ruins": "Ruins",
    "Crypts": "Crypts",
    "Inferno": "Inferno",
    "Ship Graveyard": "Ship Graveyard",
}

DUNGEON_CODES = {
    "1001": ("PVE", "Goblin Caves"),
    "1002": ("PVE", "Fire Deep"),
    "1011": ("PVE", "Ice Cavern"),
    "1012": ("PVE", "Ice Abyss"),
    "1021": ("PVE", "Ruins"),
    "1022": ("PVE", "Crypts"),
    "1023": ("PVE", "Inferno"),
    "1031": ("PVE", "Ship Graveyard"),
    "2001": ("Normal", "Goblin Caves"),
    "2002": ("Normal", "Fire Deep"),
    "2011": ("Normal", "Ice Cavern"),
    "2012": ("Normal", "Ice Abyss"),
    "2021": ("Normal", "Ruins"),
    "2022": ("Normal", "Crypts"),
    "2023": ("Normal", "Inferno"),
    "2031": ("Normal", "Ship Graveyard"),
    "3001": ("High Roller", "Goblin Caves"),
    "3002": ("High Roller", "Fire Deep"),
    "3011": ("High Roller", "Ice Cavern"),
    "3012": ("High Roller", "Ice Abyss"),
    "3021": ("High Roller", "Ruins"),
    "3022": ("High Roller", "Crypts"),
    "3023": ("High Roller", "Inferno"),
    "3031": ("High Roller", "Ship Graveyard"),
    "4001": ("Squire", "Goblin Caves"),
    "4002": ("Squire", "Fire Deep"),
    "4011": ("Squire", "Ice Cavern"),
    "4012": ("Squire", "Ice Abyss"),
    "4021": ("Squire", "Ruins"),
    "4022": ("Squire", "Crypts"),
    "4023": ("Squire", "Inferno"),
    "4031": ("Squire", "Ship Graveyard"),
}

DIFFICULTY_ORDER = {"Global": -1, "PVE": 0, "Normal": 1, "High Roller": 2, "Squire": 3}
MAP_ORDER = {
    "Global/Default": -1,
    "Goblin Caves": 0,
    "Fire Deep": 1,
    "Ice Cavern": 2,
    "Ice Abyss": 3,
    "Ruins": 4,
    "Crypts": 5,
    "Inferno": 6,
    "Ship Graveyard": 7,
}

DEFAULT_LUCK_500_SCALARS = [0.5, 0.5, 0.75, 1.0, 1.752, 2.584, 3.28, 3.705, 4.213]
MAX_TREE_ROWS = 10000
DEFAULT_SOURCE_ROWS_PER_SOURCE = 25

# Compressed anchors from the long LuckGrade04 curve in the original script.
# Other grades use the exported 500-luck scalar as the endpoint.
GRADE4_ANCHORS = [
    (0, 1.000),
    (13, 1.039),
    (30, 1.087),
    (50, 1.143),
    (75, 1.208),
    (100, 1.270),
    (125, 1.329),
    (157, 1.398),
    (200, 1.481),
    (250, 1.563),
    (300, 1.631),
    (350, 1.684),
    (400, 1.721),
    (450, 1.744),
    (500, 1.752),
]

EQUIPMENT_WORDS = {
    "aketon",
    "amulet",
    "armet",
    "armlet",
    "axe",
    "band",
    "bangle",
    "bardiche",
    "baselard",
    "boots",
    "bow",
    "brigandine",
    "buckler",
    "cap",
    "chapel",
    "chaperon",
    "chest",
    "cloth",
    "cowl",
    "crossbow",
    "dagger",
    "doublet",
    "dress",
    "falchion",
    "felling",
    "flanged",
    "frock",
    "gambeson",
    "gloves",
    "halberd",
    "hat",
    "heater",
    "helmet",
    "hood",
    "hose",
    "jazerant",
    "lantern",
    "leather",
    "longbow",
    "longsword",
    "mace",
    "mask",
    "morion",
    "necklace",
    "orb",
    "pants",
    "pendant",
    "plate",
    "poupoint",
    "rapier",
    "ring",
    "rondel",
    "sallet",
    "shield",
    "shirt",
    "shoes",
    "spellbook",
    "staff",
    "sword",
    "tunic",
    "war",
    "windlass",
    "wizard",
    "zweihander",
}

CONSUMABLE_WORDS = {
    "ale",
    "bandage",
    "campfire",
    "clarity",
    "healing",
    "invisibility",
    "luck",
    "potion",
    "protection",
    "surgical",
    "utility",
}

MATERIAL_WORDS = {
    "coin",
    "dust",
    "gem",
    "ingot",
    "ore",
    "powder",
    "token",
}

SPECIAL_WORDS = {
    "artifact",
    "crown",
    "currency",
    "event",
    "key",
    "quest",
    "skull",
    "soul",
}


@dataclass(frozen=True)
class LootChoice:
    item_asset: str
    item_name: str
    rarity: str
    grade: int
    item_count: int
    is_empty: bool


@dataclass(frozen=True)
class AssetInfo:
    name: str
    rarity: str = ""
    category: str = ""
    grade: str = ""


@dataclass(frozen=True)
class DungeonMeta:
    diff: str
    map_name: str
    code: str
    display: str = ""


@dataclass(frozen=True)
class ChoiceStat:
    item_asset: str
    item_name: str
    rarity: str
    grade: int
    item_count: int
    choice_count: int
    grade_total: int
    category: str


@dataclass
class LootDropTable:
    asset: str
    display: str
    grade_totals: Counter
    real_choices: list[LootChoice]
    choice_stats: list[ChoiceStat]
    empty_choices: int
    path: Path


@dataclass
class RateTable:
    asset: str
    display: str
    rates: list[float]
    path: Path


@dataclass(frozen=True)
class GroupEntry:
    dungeon_grade: int
    loot_asset: str
    rate_asset: str
    rolls: int


@dataclass
class LootGroup:
    asset: str
    display: str
    entries: list[GroupEntry]
    path: Path


@dataclass(frozen=True)
class SpawnerEntry:
    spawner_asset: str
    source_entity: str
    source_kind: str
    group_asset: str
    dungeon_grades: tuple[int, ...]
    spawn_rate: float


@dataclass
class ScanResult:
    rows: list[dict]
    stats: dict
    warnings: list[str]
    maps: list[str]
    diffs: list[str]
    categories: list[str]
    rarities: list[str]
    rate_weights: dict[str, list[float]]


@dataclass
class AssetResolver:
    items: dict[str, AssetInfo]
    props: dict[str, AssetInfo]
    monsters: dict[str, AssetInfo]

    def resolve_item(self, asset: str, fallback_name: str, fallback_rarity: str) -> AssetInfo:
        info = self.items.get(asset.lower())
        if info:
            return info
        return AssetInfo(
            name=fallback_name,
            rarity=fallback_rarity,
            category=categorize_item(fallback_name),
        )

    def resolve_source(self, monster_asset: str, props_asset: str, lookup_asset: str, spawner_asset: str) -> tuple[str, str]:
        if monster_asset:
            info = self.monsters.get(monster_asset.lower())
            if info:
                return info.name, "Monster"
            return humanize_asset(monster_asset), "Monster"
        if props_asset:
            info = self.props.get(props_asset.lower())
            if info:
                return info.name, "Prop"
            return humanize_asset(props_asset), "Prop"
        if lookup_asset:
            info = self.items.get(lookup_asset.lower()) or self.props.get(lookup_asset.lower()) or self.monsters.get(lookup_asset.lower())
            if info:
                return info.name, "Lookup"
            return humanize_asset(lookup_asset), "Lookup"
        return humanize_asset(spawner_asset), "Spawner"


def read_json_asset(path: Path) -> dict:
    with path.open("r", encoding="utf-8-sig") as handle:
        data = json.load(handle)
    if isinstance(data, list):
        return data[0] if data else {}
    return data if isinstance(data, dict) else {}


def asset_name(value) -> str:
    if isinstance(value, dict):
        value = value.get("AssetPathName", "")
    if not isinstance(value, str) or not value:
        return ""
    tail = value.replace("\\", "/").rsplit("/", 1)[-1]
    if "." in tail:
        tail = tail.rsplit(".", 1)[-1]
    return tail.strip()


def asset_key(value) -> str:
    return asset_name(value).lower()


def localized_text(value) -> str:
    if not isinstance(value, dict):
        return ""
    for key in ("LocalizedString", "SourceString", "Key"):
        text = value.get(key)
        if isinstance(text, str) and text and not text.startswith("Text_DesignData_"):
            return text
    return ""


def tag_leaf(value) -> str:
    if isinstance(value, dict):
        value = value.get("TagName", "")
    if not isinstance(value, str) or not value:
        return ""
    return value.rsplit(".", 1)[-1].replace("_", " ")


def title_tag(value: str) -> str:
    if not value:
        return ""
    return re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", value).strip().title()


def rarity_from_tag(value) -> str:
    leaf = title_tag(tag_leaf(value))
    aliases = {
        "Poor": "Junk",
        "Normal": "Common",
        "High Grade": "Uncommon",
        "Legend": "Legendary",
    }
    return aliases.get(leaf, leaf)


def strip_known_prefixes(name: str) -> str:
    cleaned = name
    prefixes = [
        r"^ID_",
        r"^Id_",
        r"^Lootdrop_",
        r"^LootDrop_",
        r"^LootDropGroup_",
        r"^Droprate_",
        r"^Item_",
        r"^Monster_",
        r"^Props_",
        r"^Spawner_",
        r"^New_",
        r"^Drop_",
        r"^Spawn_",
        r"^NPC_",
    ]
    changed = True
    while changed:
        changed = False
        for prefix in prefixes:
            new_value = re.sub(prefix, "", cleaned, flags=re.IGNORECASE)
            if new_value != cleaned:
                cleaned = new_value
                changed = True
    return cleaned.strip("_")


@lru_cache(maxsize=20000)
def humanize_asset(name: str) -> str:
    if not name:
        return "Unknown"
    text = strip_known_prefixes(name)
    text = re.sub(r"_(\d{4})$", r" \1", text)
    text = text.replace("_", " ")
    text = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text or name


@lru_cache(maxsize=20000)
def normalize_lookup_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", strip_known_prefixes(name).lower())


def parse_item_ref(value) -> tuple[str, str, str, bool]:
    name = asset_name(value)
    if not name:
        return "", "(No item)", "None", True
    raw = re.sub(r"^Id_Item_", "", name, flags=re.IGNORECASE)
    match = re.search(r"^(.*?)_(\d{4})$", raw)
    if match:
        item_name = match.group(1)
        rarity = RARITY_CODES.get(match.group(2), match.group(2))
    else:
        item_name = raw
        rarity = "None"
    return name, humanize_asset(item_name), rarity, False


@lru_cache(maxsize=20000)
def categorize_item(item_name: str) -> str:
    words = set(re.findall(r"[a-z]+", item_name.lower()))
    compact = item_name.replace(" ", "").lower()
    if words & SPECIAL_WORDS:
        return "Special/Quest"
    if words & MATERIAL_WORDS:
        return "Material/Currency"
    if words & CONSUMABLE_WORDS:
        return "Consumable"
    if words & EQUIPMENT_WORDS:
        return "Equipment"
    if any(piece in compact for piece in ("treasure", "goblet", "chalice", "bangle", "jewel", "ruby", "diamond")):
        return "Treasure"
    return "Other"


def category_from_item_props(props: dict, item_name: str) -> str:
    item_type = str(props.get("ItemType", "")).rsplit("::", 1)[-1]
    misc_type = tag_leaf(props.get("MiscType", ""))
    equip_type = tag_leaf(props.get("EquipType", ""))
    combined = " ".join([item_type, misc_type, equip_type, item_name]).lower()
    if any(word in combined for word in ("weapon", "armor", "shield", "accessory", "instrument")):
        return "Equipment"
    if any(word in combined for word in ("potion", "consumable", "utility", "bandage", "campfire")):
        return "Consumable"
    if any(word in combined for word in ("currency", "coin", "ore", "ingot", "powder", "gem", "material")):
        return "Material/Currency"
    if any(word in combined for word in ("quest", "huntingloot", "hunting loot", "key")):
        return "Special/Quest"
    if any(word in combined for word in ("treasure", "misc")):
        guessed = categorize_item(item_name)
        return "Treasure" if guessed == "Other" else guessed
    return categorize_item(item_name)


@lru_cache(maxsize=256)
def dungeon_info(code: int | str) -> tuple[str, str, str]:
    try:
        code_int = int(code)
    except (TypeError, ValueError):
        return "Unknown", f"Dungeon {code}", str(code)
    if code_int == 0:
        return "Global", "Global/Default", "0"
    code_str = f"{code_int:04d}"
    exact = DUNGEON_CODES.get(code_str)
    if exact:
        return exact[0], exact[1], code_str
    diff = DIFFICULTY_CODES.get(code_str[0], f"Tier {code_str[0]}")
    area = AREA_CODES.get(code_str[2:], f"Dungeon {code_str[2:]}")
    area = AREA_DISPLAY_NAMES.get(area, area)
    return diff, area, code_str


def is_known_dungeon_code(code: int | str) -> bool:
    try:
        code_int = int(code)
    except (TypeError, ValueError):
        return False
    return code_int == 0 or f"{code_int:04d}" in DUNGEON_CODES


def normalize_difficulty_label(value: str) -> str:
    value = value.strip().replace("_", " ")
    aliases = {
        "Adventure": "PVE",
        "PVE": "PVE",
        "High-Roller": "High Roller",
        "High Roller": "High Roller",
        "High-Roller Adventure": "High Roller",
        "High Roller Adventure": "High Roller",
        "HR": "High Roller",
        "S2R": "Squire",
        "Squires To Riches": "Squire",
        "SquiresToRiches": "Squire",
        "Squire Royale": "Squire",
    }
    return aliases.get(value, value)


def split_dungeon_display(display: str, fallback_diff: str, fallback_map: str) -> tuple[str, str]:
    match = re.match(r"^(.*?)\s*\(([^()]*)\)\s*$", display or "")
    if not match:
        return fallback_diff, fallback_map
    map_name = re.sub(r"\s+", " ", match.group(1)).strip()
    diff = normalize_difficulty_label(match.group(2))
    return diff or fallback_diff, map_name or fallback_map


def resolve_dungeon_info(code: int | str, lookup: dict[int, DungeonMeta] | None = None) -> tuple[str, str, str]:
    try:
        code_int = int(code)
    except (TypeError, ValueError):
        return dungeon_info(code)
    if is_known_dungeon_code(code_int):
        return dungeon_info(code_int)
    if lookup:
        meta = lookup.get(code_int)
        if meta:
            return meta.diff, meta.map_name, meta.code
    return dungeon_info(code_int)


def map_sort_key(map_name: str) -> tuple[int, str]:
    return (MAP_ORDER.get(map_name, 999), map_name)


def difficulty_sort_key(diff: str) -> tuple[int, str]:
    return (DIFFICULTY_ORDER.get(diff, 999), diff)


@lru_cache(maxsize=4096)
def luck_scalar(luck: int, grade: int) -> float:
    luck = max(0, min(int(luck), 500))
    if grade < 0 or grade > MAX_LUCK_GRADE:
        return 1.0
    if grade == 4:
        if luck <= GRADE4_ANCHORS[0][0]:
            return GRADE4_ANCHORS[0][1]
        for (left_luck, left_value), (right_luck, right_value) in zip(GRADE4_ANCHORS, GRADE4_ANCHORS[1:]):
            if left_luck <= luck <= right_luck:
                span = right_luck - left_luck
                if span <= 0:
                    return right_value
                return left_value + (right_value - left_value) * ((luck - left_luck) / span)
        return GRADE4_ANCHORS[-1][1]
    target = DEFAULT_LUCK_500_SCALARS[grade]
    return 1.0 + (target - 1.0) * (luck / 500.0)


def grade_probabilities(rates: list[float], luck: int) -> tuple[list[float], list[float]]:
    clean_rates = [(float(value) if value else 0.0) for value in rates[: MAX_LUCK_GRADE + 1]]
    clean_rates.extend([0.0] * ((MAX_LUCK_GRADE + 1) - len(clean_rates)))
    base_total = sum(max(0.0, value) for value in clean_rates)
    weighted = [max(0.0, value) * luck_scalar(luck, grade) for grade, value in enumerate(clean_rates)]
    dyn_total = sum(weighted)
    base = [(value / base_total) if base_total else 0.0 for value in clean_rates]
    dyn = [(value / dyn_total) if dyn_total else 0.0 for value in weighted]
    return base, dyn


def percent(value: float) -> str:
    return f"{value * 100.0:.4f}%"


def summarize_values(values, limit: int = 3) -> str:
    ordered = sorted(str(value) for value in values if value is not None and str(value))
    if not ordered:
        return ""
    if len(ordered) <= limit:
        return ", ".join(ordered)
    return ", ".join(ordered[:limit]) + f" +{len(ordered) - limit}"


def short_path(path: Path, width: int = 54) -> str:
    value = str(path)
    if len(value) <= width:
        return value
    return "..." + value[-(width - 3) :]


def find_generated_root(start: Path) -> Path | None:
    start = Path(start)
    candidates = [
        start,
        start / "Content" / "DungeonCrawler" / "Data" / "Generated" / "V2",
        start / "DungeonCrawler" / "Data" / "Generated" / "V2",
        start / "Data" / "Generated" / "V2",
        start / "Generated" / "V2",
        start / "V2",
    ]
    for candidate in candidates:
        if (candidate / "LootDrop" / "LootDrop").is_dir() and (candidate / "LootDrop" / "LootDropGroup").is_dir():
            return candidate.resolve()
    for candidate in start.rglob("V2"):
        if (candidate / "LootDrop" / "LootDrop").is_dir() and (candidate / "LootDrop" / "LootDropGroup").is_dir():
            return candidate.resolve()
    return None


def load_dungeon_lookup(generated_root: Path) -> tuple[dict[int, DungeonMeta], list[str]]:
    warnings: list[str] = []
    candidates: dict[int, list[tuple[str, str]]] = defaultdict(list)
    dungeon_dir = generated_root / "Dungeon" / "Dungeon"
    if not dungeon_dir.is_dir():
        return {}, warnings

    for path in dungeon_dir.rglob("*.json"):
        try:
            obj = read_json_asset(path)
            props = obj.get("Properties", {})
            code = props.get("DefaultDungeonGrade")
            if not isinstance(code, int) or code == 0:
                continue
            display = localized_text(props.get("Name")) or humanize_asset(obj.get("Name") or path.stem)
            tag = ""
            id_tag = props.get("IdTag")
            if isinstance(id_tag, dict):
                tag = id_tag.get("TagName", "") or ""
            candidates[code].append((display, tag))
        except Exception as exc:
            warnings.append(f"Could not parse dungeon metadata {path}: {exc}")

    lookup: dict[int, DungeonMeta] = {}
    for code, values in candidates.items():
        fallback_diff, fallback_map, code_str = dungeon_info(code)
        non_arena = [
            (display, tag)
            for display, tag in values
            if tag != "Id.Arena" and not display.lower().endswith(" arena")
        ]
        pool = non_arena or values
        display = Counter(display for display, _tag in pool).most_common(1)[0][0]
        diff, map_name = split_dungeon_display(display, fallback_diff, fallback_map)
        lookup[code] = DungeonMeta(diff=diff, map_name=map_name, code=code_str, display=display)
    return lookup, warnings


def load_asset_resolver(generated_root: Path) -> tuple[AssetResolver, list[str]]:
    warnings: list[str] = []
    items: dict[str, AssetInfo] = {}
    props: dict[str, AssetInfo] = {}
    monsters: dict[str, AssetInfo] = {}

    item_dirs = [
        generated_root / "Item" / "Item",
        generated_root.parent / "DT_Item" / "Item",
    ]
    for item_dir in item_dirs:
        if not item_dir.is_dir():
            continue
        for path in item_dir.rglob("*.json"):
            try:
                obj = read_json_asset(path)
                asset = obj.get("Name") or path.stem
                raw_props = obj.get("Properties", {})
                item_props = raw_props.get("Item", {}) if isinstance(raw_props.get("Item"), dict) else raw_props
                display = localized_text(item_props.get("Name")) or humanize_asset(asset)
                rarity = rarity_from_tag(item_props.get("RarityType")) or parse_item_ref(asset)[2]
                info = AssetInfo(
                    name=display,
                    rarity=rarity or "None",
                    category=category_from_item_props(item_props, display),
                )
                items[asset.lower()] = info
                items[path.stem.lower()] = info
            except Exception as exc:
                warnings.append(f"Could not parse item name {path}: {exc}")

    props_dirs = [
        generated_root / "Props" / "Props",
        generated_root.parent / "DT_Props" / "Props",
    ]
    for props_dir in props_dirs:
        if not props_dir.is_dir():
            continue
        for path in props_dir.rglob("*.json"):
            try:
                obj = read_json_asset(path)
                asset = obj.get("Name") or path.stem
                raw_props = obj.get("Properties", {})
                prop_props = raw_props.get("Item", {}) if isinstance(raw_props.get("Item"), dict) else raw_props
                display = localized_text(prop_props.get("Name")) or humanize_asset(asset)
                grade = title_tag(tag_leaf(prop_props.get("GradeType")))
                label = display if not grade or grade == "Normal" else f"{display} ({grade})"
                info = AssetInfo(name=label, grade=grade)
                props[asset.lower()] = info
                props[path.stem.lower()] = info
            except Exception as exc:
                warnings.append(f"Could not parse prop name {path}: {exc}")

    monster_dirs = [
        generated_root / "Monster" / "Monster",
        generated_root.parent / "DT_Monster" / "Monster",
    ]
    for monster_dir in monster_dirs:
        if not monster_dir.is_dir():
            continue
        for path in monster_dir.rglob("*.json"):
            try:
                obj = read_json_asset(path)
                asset = obj.get("Name") or path.stem
                monster_props = obj.get("Properties", {})
                item_props = monster_props.get("Item", {}) if isinstance(monster_props.get("Item"), dict) else monster_props
                display = localized_text(item_props.get("Name")) or humanize_asset(asset)
                grade = title_tag(tag_leaf(item_props.get("GradeType")))
                label = display if not grade else f"{display} ({grade})"
                info = AssetInfo(name=label, grade=grade)
                monsters[asset.lower()] = info
                monsters[path.stem.lower()] = info
            except Exception as exc:
                warnings.append(f"Could not parse monster name {path}: {exc}")

    return AssetResolver(items=items, props=props, monsters=monsters), warnings


def parse_loot_drop(path: Path, resolver: AssetResolver | None = None) -> LootDropTable:
    obj = read_json_asset(path)
    asset = obj.get("Name") or path.stem
    props = obj.get("Properties", {})
    grade_totals: Counter = Counter()
    real_choices: list[LootChoice] = []
    empty_choices = 0

    for item in props.get("LootDropItemArray", []) or []:
        grade = int(item.get("LuckGrade", 0) or 0)
        item_asset, item_name, rarity, is_empty = parse_item_ref(item.get("ItemId", {}))
        if resolver and item_asset:
            item_info = resolver.resolve_item(item_asset, item_name, rarity)
            item_name = item_info.name
            rarity = item_info.rarity or rarity
        item_count = int(item.get("ItemCount", 1) or 0)
        grade_totals[grade] += 1
        if is_empty:
            empty_choices += 1
            continue
        real_choices.append(
            LootChoice(
                item_asset=item_asset,
                item_name=item_name,
                rarity=rarity,
                grade=grade,
                item_count=item_count,
                is_empty=False,
            )
        )

    counts = Counter(
        (choice.item_asset, choice.item_name, choice.rarity, choice.grade, choice.item_count)
        for choice in real_choices
        if 0 <= choice.grade <= MAX_LUCK_GRADE
    )
    choice_stats = [
        ChoiceStat(
            item_asset=item_asset,
            item_name=item_name,
            rarity=rarity,
            grade=grade,
            item_count=item_count,
            choice_count=choice_count,
            grade_total=grade_totals.get(grade, 0),
            category=(resolver.resolve_item(item_asset, item_name, rarity).category if resolver else categorize_item(item_name)),
        )
        for (item_asset, item_name, rarity, grade, item_count), choice_count in counts.items()
    ]

    return LootDropTable(
        asset=asset,
        display=humanize_asset(asset),
        grade_totals=grade_totals,
        real_choices=real_choices,
        choice_stats=choice_stats,
        empty_choices=empty_choices,
        path=path,
    )


def parse_rate_table(path: Path) -> RateTable:
    obj = read_json_asset(path)
    asset = obj.get("Name") or path.stem
    rates = [0.0] * (MAX_LUCK_GRADE + 1)
    for item in obj.get("Properties", {}).get("LootDropRateItemArray", []) or []:
        grade = item.get("LuckGrade")
        if isinstance(grade, int) and 0 <= grade <= MAX_LUCK_GRADE:
            rates[grade] = float(item.get("DropRate", 0) or 0)
    return RateTable(asset=asset, display=humanize_asset(asset), rates=rates, path=path)


def parse_loot_group(path: Path) -> LootGroup:
    obj = read_json_asset(path)
    asset = obj.get("Name") or path.stem
    entries: list[GroupEntry] = []
    for item in obj.get("Properties", {}).get("LootDropGroupItemArray", []) or []:
        rolls = int(item.get("LootDropCount", 0) or 0)
        if rolls <= 0:
            continue
        loot_asset = asset_name(item.get("LootDropId", {}))
        rate_asset = asset_name(item.get("LootDropRateId", {}))
        if not loot_asset or not rate_asset:
            continue
        entries.append(
            GroupEntry(
                dungeon_grade=int(item.get("DungeonGrade", 0) or 0),
                loot_asset=loot_asset,
                rate_asset=rate_asset,
                rolls=rolls,
            )
        )
    return LootGroup(asset=asset, display=humanize_asset(asset), entries=entries, path=path)


def source_from_spawner_item(spawner_asset: str, item: dict, resolver: AssetResolver | None = None) -> tuple[str, str]:
    monster = asset_name(item.get("MonsterId", {}))
    props = asset_name(item.get("PropsId", {}))
    lookup = asset_name(item.get("LookupId", {}))
    if resolver:
        return resolver.resolve_source(monster, props, lookup, spawner_asset)
    if monster:
        return humanize_asset(monster), "Monster"
    if props:
        return humanize_asset(props), "Prop"
    if lookup:
        return humanize_asset(lookup), "Lookup"
    return humanize_asset(spawner_asset), "Spawner"


def parse_spawner(path: Path, resolver: AssetResolver | None = None) -> list[SpawnerEntry]:
    obj = read_json_asset(path)
    spawner_asset = obj.get("Name") or path.stem
    rows: list[SpawnerEntry] = []
    for item in obj.get("Properties", {}).get("SpawnerItemArray", []) or []:
        group_asset = asset_name(item.get("LootDropGroupId", {}))
        if not group_asset:
            continue
        spawn_rate = float(item.get("SpawnRate", 0) or 0)
        if spawn_rate <= 0:
            continue
        grades = tuple(int(value) for value in (item.get("DungeonGrades", []) or []) if isinstance(value, int))
        source, source_kind = source_from_spawner_item(spawner_asset, item, resolver)
        rows.append(
            SpawnerEntry(
                spawner_asset=spawner_asset,
                source_entity=source,
                source_kind=source_kind,
                group_asset=group_asset,
                dungeon_grades=grades,
                spawn_rate=spawn_rate,
            )
        )
    return rows


def load_assets(folder: Path, parser) -> tuple[dict[str, object], list[str]]:
    assets: dict[str, object] = {}
    warnings: list[str] = []
    if not folder.is_dir():
        warnings.append(f"Missing folder: {folder}")
        return assets, warnings
    for path in folder.rglob("*.json"):
        try:
            parsed = parser(path)
            key = getattr(parsed, "asset", path.stem).lower()
            assets[key] = parsed
        except Exception as exc:
            warnings.append(f"Could not parse {path}: {exc}")
    return assets, warnings


def build_database(root: Path, luck: int = 500) -> ScanResult:
    root = Path(root)
    generated_root = find_generated_root(root)
    if not generated_root:
        raise FileNotFoundError("Could not find Content/DungeonCrawler/Data/Generated/V2 under the selected folder.")

    loot_dir = generated_root / "LootDrop" / "LootDrop"
    rate_dir = generated_root / "LootDrop" / "LootDropRate"
    group_dir = generated_root / "LootDrop" / "LootDropGroup"
    spawner_dir = generated_root / "Spawner" / "Spawner"

    resolver, warnings = load_asset_resolver(generated_root)
    dungeon_lookup, dungeon_warnings = load_dungeon_lookup(generated_root)
    warnings.extend(dungeon_warnings)
    drops, drop_warnings = load_assets(loot_dir, lambda path: parse_loot_drop(path, resolver))
    warnings.extend(drop_warnings)
    rates, rate_warnings = load_assets(rate_dir, parse_rate_table)
    groups, group_warnings = load_assets(group_dir, parse_loot_group)
    warnings.extend(rate_warnings)
    warnings.extend(group_warnings)

    spawner_entries: list[SpawnerEntry] = []
    if not spawner_dir.is_dir():
        warnings.append(f"Missing folder: {spawner_dir}")
    else:
        for path in spawner_dir.rglob("*.json"):
            try:
                spawner_entries.extend(parse_spawner(path, resolver))
            except Exception as exc:
                warnings.append(f"Could not parse {path}: {exc}")

    aggregate_rows: dict[tuple, dict] = {}
    missing_groups: Counter = Counter()
    missing_drops: Counter = Counter()
    missing_rates: Counter = Counter()

    group_entries_by_grade: dict[str, dict[int, list[GroupEntry]]] = {}
    for key, group in groups.items():
        by_grade: dict[int, list[GroupEntry]] = defaultdict(list)
        for entry in group.entries:
            by_grade[entry.dungeon_grade].append(entry)
        group_entries_by_grade[key] = by_grade

    rate_prob_cache = {
        key: grade_probabilities(rate.rates, luck)
        for key, rate in rates.items()
    }

    for spawner in spawner_entries:
        group_key = spawner.group_asset.lower()
        group = groups.get(group_key)
        if not group:
            missing_groups[spawner.group_asset] += 1
            continue

        grade_map = group_entries_by_grade.get(group_key, {})
        active_grades = set(spawner.dungeon_grades)
        if not active_grades:
            active_grades = set(grade_map.keys())
        if not active_grades:
            active_grades = {0}

        for dungeon_grade in sorted(active_grades):
            if not is_known_dungeon_code(dungeon_grade):
                continue
            entries = grade_map.get(dungeon_grade)
            if not entries and dungeon_grade != 0:
                entries = grade_map.get(0)
            if not entries:
                continue

            diff, map_name, map_code = resolve_dungeon_info(dungeon_grade, dungeon_lookup)
            for group_entry in entries:
                drop = drops.get(group_entry.loot_asset.lower())
                rate = rates.get(group_entry.rate_asset.lower())
                if not drop:
                    missing_drops[group_entry.loot_asset] += 1
                    continue
                if not rate:
                    missing_rates[group_entry.rate_asset] += 1
                    continue

                base_probs, dyn_probs = rate_prob_cache[rate.asset.lower()]

                for choice in drop.choice_stats:
                    grade = choice.grade
                    grade_total = choice.grade_total
                    if grade_total <= 0:
                        continue
                    per_choice = choice.choice_count / grade_total
                    base_per_roll = base_probs[grade] * per_choice
                    dyn_per_roll = dyn_probs[grade] * per_choice
                    if base_per_roll <= 0 and dyn_per_roll <= 0:
                        continue
                    rolls = max(1, int(group_entry.rolls))
                    base_at_least_one = 1.0 - math.pow(max(0.0, 1.0 - base_per_roll), rolls)
                    dyn_at_least_one = 1.0 - math.pow(max(0.0, 1.0 - dyn_per_roll), rolls)
                    category = choice.category
                    key = (
                        choice.item_asset,
                        choice.rarity,
                        category,
                        spawner.source_entity,
                        spawner.source_kind,
                        map_code,
                        diff,
                        map_name,
                        group.asset,
                        drop.asset,
                        rate.asset,
                        grade,
                        choice.item_count,
                        choice.choice_count,
                        grade_total,
                        rolls,
                        round(base_per_roll, 14),
                        round(dyn_per_roll, 14),
                        round(base_at_least_one, 14),
                        round(dyn_at_least_one, 14),
                    )
                    row = aggregate_rows.get(key)
                    if not row:
                        row = {
                            "item": choice.item_name,
                            "item_asset": choice.item_asset,
                            "rarity": choice.rarity,
                            "cat": category,
                            "source": spawner.source_entity,
                            "source_kind": spawner.source_kind,
                            "spawners": set(),
                            "spawner": "",
                            "group": group.display,
                            "group_asset": group.asset,
                            "loot_table": drop.display,
                            "loot_asset": drop.asset,
                            "rate_tables": set(),
                            "rate_table": "",
                            "rate_asset": "",
                            "rate_key": rate.asset.lower(),
                            "maps": set(),
                            "map": "",
                            "diffs": set(),
                            "diff": "",
                            "map_codes": set(),
                            "map_code": "",
                            "grade": grade,
                            "choice_count": choice.choice_count,
                            "grade_choices": grade_total,
                            "empty_choices": drop.empty_choices,
                            "item_count": choice.item_count,
                            "rolls": rolls,
                            "spawn_rate": 0.0,
                            "choice_fraction": per_choice,
                            "base_per_roll": base_per_roll,
                            "dyn_per_roll": dyn_per_roll,
                            "base_at_least_one": base_at_least_one,
                            "dyn_at_least_one": dyn_at_least_one,
                            "base_expected": base_per_roll * rolls,
                            "dyn_expected": dyn_per_roll * rolls,
                            "merged_rows": 0,
                        }
                        aggregate_rows[key] = row
                    row["maps"].add(map_name)
                    row["diffs"].add(diff)
                    row["map_codes"].add(map_code)
                    row["rate_tables"].add(rate.display)
                    row["spawners"].add(humanize_asset(spawner.spawner_asset))
                    row["spawn_rate"] += spawner.spawn_rate
                    row["merged_rows"] += 1

    if missing_groups:
        warnings.append(f"Missing {len(missing_groups)} referenced loot groups.")
    if missing_drops:
        warnings.append(f"Missing {len(missing_drops)} referenced loot drop tables.")
    if missing_rates:
        warnings.append(f"Missing {len(missing_rates)} referenced rate tables.")

    rows = list(aggregate_rows.values())
    for row in rows:
        row["map"] = summarize_values(row["maps"], limit=4)
        row["diff"] = summarize_values(row["diffs"], limit=4)
        row["map_code"] = summarize_values(row["map_codes"], limit=8)
        row["rate_table"] = summarize_values(row["rate_tables"], limit=2)
        row["rate_asset"] = row["rate_table"]
        row["spawner"] = summarize_values(row["spawners"], limit=2)

    maps = sorted({map_name for row in rows for map_name in row["maps"]}, key=map_sort_key)
    diffs = sorted({diff for row in rows for diff in row["diffs"]}, key=difficulty_sort_key)
    categories = sorted({row["cat"] for row in rows})
    rarities = [rarity for rarity in RARITY_CODES.values() if any(row["rarity"] == rarity for row in rows)]
    if any(row["rarity"] == "None" for row in rows):
        rarities.append("None")

    stats = {
        "generated_root": str(generated_root),
        "loot_tables": len(drops),
        "rate_tables": len(rates),
        "groups": len(groups),
        "dungeon_codes": len(dungeon_lookup),
        "item_names": len(resolver.items),
        "prop_names": len(resolver.props),
        "monster_names": len(resolver.monsters),
        "sources": len({row["source"] for row in rows}),
        "items": len({row["item_asset"] for row in rows}),
        "spawner_entries": len(spawner_entries),
        "rows": len(rows),
        "warnings": len(warnings),
        "luck": luck,
    }
    rate_weights = {key: list(rate.rates) for key, rate in rates.items()}
    return ScanResult(
        rows=rows,
        stats=stats,
        warnings=warnings,
        maps=maps,
        diffs=diffs,
        categories=categories,
        rarities=rarities,
        rate_weights=rate_weights,
    )


def apply_luck_to_result(result: ScanResult, luck: int) -> None:
    dyn_prob_cache = {
        key: grade_probabilities(weights, luck)[1]
        for key, weights in result.rate_weights.items()
    }
    for row in result.rows:
        rate_key = row.get("rate_key")
        grade = int(row.get("grade", 0) or 0)
        dyn_probs = dyn_prob_cache.get(rate_key)
        if not dyn_probs or grade < 0 or grade >= len(dyn_probs):
            continue
        choice_fraction = float(row.get("choice_fraction", 0.0) or 0.0)
        rolls = max(1, int(row.get("rolls", 1) or 1))
        dyn_per_roll = dyn_probs[grade] * choice_fraction
        dyn_at_least_one = 1.0 - math.pow(max(0.0, 1.0 - dyn_per_roll), rolls)
        row["dyn_per_roll"] = dyn_per_roll
        row["dyn_at_least_one"] = dyn_at_least_one
        row["dyn_expected"] = dyn_per_roll * rolls
    result.stats["luck"] = luck


def export_rows_to_csv(path: Path, rows: list[dict], luck: int) -> None:
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
        "rolls",
        "choice_count",
        "grade_choices",
        "item_count",
        "base_per_roll",
        "dyn_per_roll",
        "base_at_least_one",
        "dyn_at_least_one",
        "base_expected",
        "dyn_expected",
        "spawn_rate",
        "group",
        "loot_table",
        "rate_table",
        "spawner",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["luck"] + columns)
        writer.writeheader()
        for row in rows:
            output = {"luck": luck}
            output.update({column: row.get(column, "") for column in columns})
            writer.writerow(output)


if tk is not None:
    TkBase = TkinterDnD.Tk if TkinterDnD else tk.Tk
else:
    class TkBase:
        def __init__(self, *args, **kwargs):
            raise RuntimeError("Tkinter is not available in this Python build.")


class LootAnalyzerApp(TkBase):
    def __init__(self):
        super().__init__()
        self.title(APP_TITLE)
        self.geometry("1350x850")
        self.minsize(1120, 720)
        self.configure(bg="#1e1e1e")

        self.root_dir = Path.cwd()
        self.scan_result: ScanResult | None = None
        self.filtered_item_rows: list[dict] = []
        self.filtered_source_rows: list[dict] = []
        self.source_summary_rows: list[dict] = []
        self.source_tree_rows: dict[str, dict] = {}
        self.filter_after_id = None
        self.single_drop: LootDropTable | None = None
        self.single_rate: RateTable | None = None
        self.single_current_items: list[dict] = []

        self._configure_style()
        self._build_ui()
        self._load_config()

    def _configure_style(self) -> None:
        style = ttk.Style(self)
        style.theme_use("clam")
        style.configure("TFrame", background="#1e1e1e")
        style.configure("TLabel", background="#1e1e1e", foreground="#d4d4d4", font=("Segoe UI", 10))
        style.configure("Header.TLabel", background="#1e1e1e", foreground="#569cd6", font=("Segoe UI", 12, "bold"))
        style.configure("TButton", font=("Segoe UI", 10))
        style.configure("TNotebook", background="#1e1e1e", borderwidth=0)
        style.configure("TNotebook.Tab", background="#2d2d30", foreground="white", padding=[10, 5])
        style.map("TNotebook.Tab", background=[("selected", "#094771")])
        style.configure("Treeview", background="#252526", foreground="#d4d4d4", fieldbackground="#252526", rowheight=24)
        style.map("Treeview", background=[("selected", "#094771")])
        style.configure("Treeview.Heading", background="#2d2d30", foreground="white", font=("Segoe UI", 10, "bold"))

    def _build_ui(self) -> None:
        self.notebook = ttk.Notebook(self)
        self.notebook.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        self.tab_scan = ttk.Frame(self.notebook)
        self.tab_items = ttk.Frame(self.notebook)
        self.tab_sources = ttk.Frame(self.notebook)
        self.tab_single = ttk.Frame(self.notebook)

        self.notebook.add(self.tab_scan, text="Scan")
        self.notebook.add(self.tab_items, text="Item Search")
        self.notebook.add(self.tab_sources, text="Mob/Chest Search")
        self.notebook.add(self.tab_single, text="Single Table")

        self._build_scan_tab()
        self._build_item_tab()
        self._build_source_tab()
        self._build_single_tab()

    def _load_config(self) -> None:
        path = Path(CONFIG_FILE)
        if path.exists():
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                configured = Path(data.get("root_dir", ""))
                if configured.exists():
                    self.root_dir = configured
            except Exception:
                pass
        self.root_var.set(str(self.root_dir))

    def _save_config(self) -> None:
        data = {"root_dir": str(self.root_dir)}
        try:
            Path(CONFIG_FILE).write_text(json.dumps(data, indent=2), encoding="utf-8")
        except Exception:
            pass

    def _build_scan_tab(self) -> None:
        frame = ttk.Frame(self.tab_scan)
        frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=16)

        ttk.Label(frame, text="Export Root", style="Header.TLabel").pack(anchor=tk.W)
        root_row = ttk.Frame(frame)
        root_row.pack(fill=tk.X, pady=(8, 12))
        self.root_var = tk.StringVar(value=str(self.root_dir))
        tk.Entry(root_row, textvariable=self.root_var, bg="#252526", fg="white", insertbackground="white").pack(
            side=tk.LEFT, fill=tk.X, expand=True, ipady=4
        )
        ttk.Button(root_row, text="Browse...", command=self.browse_root).pack(side=tk.LEFT, padx=(8, 0))

        controls = ttk.Frame(frame)
        controls.pack(fill=tk.X, pady=(4, 12))
        ttk.Label(controls, text="Luck:").pack(side=tk.LEFT)
        self.luck_var = tk.StringVar(value="500")
        self.luck_var.trace_add("write", lambda *_: self.recalculate_views())
        tk.Entry(controls, textvariable=self.luck_var, width=8, bg="#252526", fg="white", justify="center").pack(
            side=tk.LEFT, padx=(6, 16), ipady=3
        )
        self.scan_button = tk.Button(
            controls,
            text="Scan Export",
            command=self.scan_async,
            bg="#094771",
            fg="white",
            activebackground="#0e639c",
            activeforeground="white",
            font=("Segoe UI", 10, "bold"),
            relief=tk.FLAT,
            padx=16,
            pady=6,
        )
        self.scan_button.pack(side=tk.LEFT)
        ttk.Button(controls, text="Export Filtered CSV...", command=self.export_filtered_csv).pack(side=tk.LEFT, padx=8)

        self.status_var = tk.StringVar(value="Ready.")
        ttk.Label(frame, textvariable=self.status_var, foreground="#4ec9b0").pack(anchor=tk.W, pady=(0, 8))

        stats_frame = ttk.Frame(frame)
        stats_frame.pack(fill=tk.X)
        self.stats_labels = {}
        for idx, label in enumerate(["Generated Root", "Loot Tables", "Rate Tables", "Groups", "Dungeon Codes", "Spawner Entries", "Sources", "Items", "Rows", "Warnings"]):
            ttk.Label(stats_frame, text=f"{label}:", foreground="#c586c0").grid(row=idx, column=0, sticky=tk.W, pady=2)
            var = tk.StringVar(value="-")
            self.stats_labels[label] = var
            ttk.Label(stats_frame, textvariable=var).grid(row=idx, column=1, sticky=tk.W, padx=8, pady=2)
        stats_frame.grid_columnconfigure(1, weight=1)

        ttk.Label(frame, text="Warnings", style="Header.TLabel").pack(anchor=tk.W, pady=(18, 6))
        self.warning_text = tk.Text(frame, height=12, bg="#252526", fg="#d4d4d4", insertbackground="white", wrap=tk.WORD)
        self.warning_text.pack(fill=tk.BOTH, expand=True)

    def _build_item_tab(self) -> None:
        self.item_search_var = tk.StringVar()
        self.item_map_var = tk.StringVar(value="All")
        self.item_diff_var = tk.StringVar(value="All")
        self.item_cat_var = tk.StringVar(value="All")
        self.item_rarity_var = tk.StringVar(value="All")

        top = ttk.Frame(self.tab_items)
        top.pack(fill=tk.X, padx=20, pady=12)
        self._filter_entry(top, "Search Item:", self.item_search_var, 0, 0)
        self.item_map_combo = self._combo(top, "Map:", self.item_map_var, ["All"], 0, 2)
        self.item_diff_combo = self._combo(top, "Difficulty:", self.item_diff_var, ["All"], 0, 4)
        self.item_cat_combo = self._combo(top, "Category:", self.item_cat_var, ["All"], 1, 0)
        self.item_rarity_combo = self._combo(top, "Rarity:", self.item_rarity_var, ["All"], 1, 2)

        for var in [self.item_search_var, self.item_map_var, self.item_diff_var, self.item_cat_var, self.item_rarity_var]:
            var.trace_add("write", lambda *_: self.schedule_filters())

        columns = ("item", "rarity", "cat", "source", "map", "diff", "grade", "rolls", "dyn_one", "dyn_roll", "base_one", "table")
        headings = {
            "item": "Item",
            "rarity": "Rarity",
            "cat": "Category",
            "source": "Source",
            "map": "Map",
            "diff": "Difficulty",
            "grade": "Grade",
            "rolls": "Rolls",
            "dyn_one": "Dyn At Least One",
            "dyn_roll": "Dyn Per Roll",
            "base_one": "Base At Least One",
            "table": "Loot / Rate Table",
        }
        widths = {
            "item": 170,
            "rarity": 82,
            "cat": 112,
            "source": 170,
            "map": 130,
            "diff": 110,
            "grade": 58,
            "rolls": 56,
            "dyn_one": 116,
            "dyn_roll": 104,
            "base_one": 116,
            "table": 260,
        }
        self.item_tree = self._make_tree(self.tab_items, columns, headings, widths)

    def _build_source_tab(self) -> None:
        self.source_search_var = tk.StringVar()
        self.source_item_var = tk.StringVar()
        self.source_map_var = tk.StringVar(value="All")
        self.source_diff_var = tk.StringVar(value="All")
        self.source_rarity_var = tk.StringVar(value="All")

        top = ttk.Frame(self.tab_sources)
        top.pack(fill=tk.X, padx=20, pady=12)
        self._filter_entry(top, "Search Source:", self.source_search_var, 0, 0)
        self._filter_entry(top, "Item Filter:", self.source_item_var, 0, 2)
        self.source_map_combo = self._combo(top, "Map:", self.source_map_var, ["All"], 0, 4)
        self.source_diff_combo = self._combo(top, "Difficulty:", self.source_diff_var, ["All"], 1, 0)
        self.source_rarity_combo = self._combo(top, "Rarity:", self.source_rarity_var, ["All"], 1, 2)
        ttk.Button(top, text="Open Selected", command=self.open_selected_source).grid(row=1, column=4, sticky=tk.W, padx=(0, 14), pady=5)

        for var in [self.source_search_var, self.source_item_var, self.source_map_var, self.source_diff_var, self.source_rarity_var]:
            var.trace_add("write", lambda *_: self.schedule_filters())

        columns = ("source", "kind", "items", "scenarios", "maps", "diffs", "best_dyn", "top_item")
        headings = {
            "source": "Source",
            "kind": "Kind",
            "items": "Items",
            "scenarios": "Scenarios",
            "maps": "Maps",
            "diffs": "Difficulties",
            "best_dyn": "Best Dyn",
            "top_item": "Best Item",
        }
        widths = {
            "source": 240,
            "kind": 80,
            "items": 80,
            "scenarios": 92,
            "maps": 240,
            "diffs": 180,
            "best_dyn": 100,
            "top_item": 260,
        }
        self.source_tree = self._make_tree(self.tab_sources, columns, headings, widths)
        self.source_tree.bind("<Double-1>", lambda _event: self.open_selected_source())

    def _build_single_tab(self) -> None:
        frame = ttk.Frame(self.tab_single)
        frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=14)

        drop_frame = ttk.Frame(frame)
        drop_frame.pack(fill=tk.X, pady=(0, 12))
        self.rate_drop_label = self._drop_label(drop_frame, "Drop or browse a LootDropRate JSON", self.open_single_rate)
        self.rate_drop_label.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 6))
        self.loot_drop_label = self._drop_label(drop_frame, "Drop or browse a LootDrop JSON", self.open_single_loot)
        self.loot_drop_label.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(6, 0))

        browse_row = ttk.Frame(frame)
        browse_row.pack(fill=tk.X, pady=(0, 12))
        ttk.Button(browse_row, text="Browse Rate...", command=lambda: self.browse_single("rate")).pack(side=tk.LEFT)
        ttk.Button(browse_row, text="Browse Loot...", command=lambda: self.browse_single("loot")).pack(side=tk.LEFT, padx=8)

        columns = ("grade", "rate", "scalar", "base", "dyn")
        headings = {"grade": "Grade", "rate": "Weight", "scalar": "Luck Scalar", "base": "Base Prob", "dyn": "Dyn Prob"}
        widths = {"grade": 80, "rate": 120, "scalar": 120, "base": 120, "dyn": 120}
        self.single_rate_tree = self._make_tree(frame, columns, headings, widths, height=9)
        self.single_rate_tree.pack_forget()

        ttk.Label(frame, text="Items", style="Header.TLabel").pack(anchor=tk.W, pady=(8, 6))
        columns = ("item", "rarity", "count", "grade", "choices", "base_roll", "dyn_roll", "table")
        headings = {
            "item": "Item",
            "rarity": "Rarity",
            "count": "Count",
            "grade": "Grade",
            "choices": "Choices",
            "base_roll": "Base Per Roll",
            "dyn_roll": "Dyn Per Roll",
            "table": "Loot Table",
        }
        widths = {
            "item": 220,
            "rarity": 90,
            "count": 65,
            "grade": 70,
            "choices": 90,
            "base_roll": 120,
            "dyn_roll": 120,
            "table": 260,
        }
        self.single_item_tree = self._make_tree(frame, columns, headings, widths, height=12)

    def _filter_entry(self, parent, label: str, var: tk.StringVar, row: int, column: int) -> None:
        ttk.Label(parent, text=label).grid(row=row, column=column, sticky=tk.E, padx=(0, 6), pady=5)
        tk.Entry(parent, textvariable=var, bg="#252526", fg="white", insertbackground="white").grid(
            row=row, column=column + 1, sticky=tk.EW, padx=(0, 14), pady=5
        )
        parent.grid_columnconfigure(column + 1, weight=1)

    def _combo(self, parent, label: str, var: tk.StringVar, values: list[str], row: int, column: int) -> ttk.Combobox:
        ttk.Label(parent, text=label).grid(row=row, column=column, sticky=tk.E, padx=(0, 6), pady=5)
        combo = ttk.Combobox(parent, textvariable=var, values=values, state="readonly", width=18)
        combo.grid(row=row, column=column + 1, sticky=tk.EW, padx=(0, 14), pady=5)
        combo.bind("<<ComboboxSelected>>", lambda _event: self.schedule_filters())
        parent.grid_columnconfigure(column + 1, weight=1)
        return combo

    def _make_tree(self, parent, columns, headings, widths, height=20) -> ttk.Treeview:
        container = ttk.Frame(parent)
        container.pack(fill=tk.BOTH, expand=True, padx=20 if parent in (self.tab_items, self.tab_sources) else 0, pady=(0, 20))
        tree = ttk.Treeview(container, columns=columns, show="headings", height=height)
        yscroll = ttk.Scrollbar(container, orient=tk.VERTICAL, command=tree.yview)
        xscroll = ttk.Scrollbar(container, orient=tk.HORIZONTAL, command=tree.xview)
        tree.configure(yscrollcommand=yscroll.set, xscrollcommand=xscroll.set)
        for column in columns:
            tree.heading(column, text=headings.get(column, column), command=lambda col=column, tr=tree: self.sort_tree(tr, col))
            anchor = tk.CENTER if column in {"rarity", "grade", "rolls", "count", "choices", "base", "dyn", "rate", "scalar", "items", "scenarios", "best_dyn"} else tk.W
            tree.column(column, width=widths.get(column, 100), anchor=anchor, stretch=True)
        tree.grid(row=0, column=0, sticky="nsew")
        yscroll.grid(row=0, column=1, sticky="ns")
        xscroll.grid(row=1, column=0, sticky="ew")
        container.grid_rowconfigure(0, weight=1)
        container.grid_columnconfigure(0, weight=1)
        return tree

    def _drop_label(self, parent, text: str, command) -> tk.Label:
        label = tk.Label(
            parent,
            text=text,
            bg="#252526",
            fg="#d4d4d4",
            font=("Segoe UI", 11),
            relief="solid",
            borderwidth=1,
            height=4,
            cursor="hand2",
        )
        label.bind("<Button-1>", lambda _event: command())
        if DND_FILES:
            label.drop_target_register(DND_FILES)
            label.dnd_bind("<<Drop>>", lambda event: command(Path(str(event.data).strip("{}"))))
        return label

    def browse_root(self) -> None:
        selected = filedialog.askdirectory(initialdir=str(self.root_dir))
        if selected:
            self.root_dir = Path(selected)
            self.root_var.set(str(self.root_dir))
            self._save_config()

    def current_luck(self) -> int:
        try:
            return int(self.luck_var.get() or 0)
        except ValueError:
            return 0

    def scan_async(self) -> None:
        self.root_dir = Path(self.root_var.get() or ".")
        self._save_config()
        self.scan_button.config(text="Scanning...", state=tk.DISABLED, bg="#dcdcaa", fg="#1e1e1e")
        self.status_var.set("Reading JSON files and building group-aware database...")
        self.warning_text.delete("1.0", tk.END)

        def worker():
            try:
                result = build_database(self.root_dir, self.current_luck())
                self.after(0, lambda: self.scan_finished(result, None))
            except Exception:
                self.after(0, lambda: self.scan_finished(None, traceback.format_exc()))

        threading.Thread(target=worker, daemon=True).start()

    def scan_finished(self, result: ScanResult | None, error: str | None) -> None:
        self.scan_button.config(text="Scan Export", state=tk.NORMAL, bg="#094771", fg="white")
        if error:
            self.status_var.set("Scan failed.")
            self.warning_text.insert(tk.END, error)
            messagebox.showerror(APP_TITLE, error)
            return
        assert result is not None
        self.scan_result = result
        self.update_stats()
        self.update_filter_values()
        self.apply_filters()
        self.status_var.set(f"Scan complete: {result.stats['rows']:,} item rows from {result.stats['groups']:,} loot groups.")

    def update_stats(self) -> None:
        if not self.scan_result:
            return
        stats = self.scan_result.stats
        label_map = {
            "Generated Root": short_path(Path(stats["generated_root"])),
            "Loot Tables": f"{stats['loot_tables']:,}",
            "Rate Tables": f"{stats['rate_tables']:,}",
            "Groups": f"{stats['groups']:,}",
            "Dungeon Codes": f"{stats.get('dungeon_codes', 0):,}",
            "Spawner Entries": f"{stats['spawner_entries']:,}",
            "Sources": f"{stats.get('sources', 0):,}",
            "Items": f"{stats.get('items', 0):,}",
            "Rows": f"{stats['rows']:,}",
            "Warnings": f"{stats['warnings']:,}",
        }
        for label, value in label_map.items():
            self.stats_labels[label].set(value)
        self.warning_text.delete("1.0", tk.END)
        for warning in self.scan_result.warnings[:300]:
            self.warning_text.insert(tk.END, warning + "\n")
        if len(self.scan_result.warnings) > 300:
            self.warning_text.insert(tk.END, f"... {len(self.scan_result.warnings) - 300} more warnings\n")

    def update_filter_values(self) -> None:
        if not self.scan_result:
            return
        maps = ["All"] + self.scan_result.maps
        diffs = ["All"] + self.scan_result.diffs
        cats = ["All"] + self.scan_result.categories
        rarities = ["All"] + self.scan_result.rarities
        self.item_map_combo.config(values=maps)
        self.source_map_combo.config(values=maps)
        self.item_diff_combo.config(values=diffs)
        self.source_diff_combo.config(values=diffs)
        self.item_cat_combo.config(values=cats)
        self.item_rarity_combo.config(values=rarities)
        self.source_rarity_combo.config(values=rarities)

    def recalculate_views(self) -> None:
        if self.scan_result:
            self.status_var.set("Luck changed. Rescan to rebuild dynamic probabilities from the source rates.")
        self.update_single_tables()

    def apply_filters(self) -> None:
        self.filter_after_id = None
        if not self.scan_result:
            return
        item_search = self.item_search_var.get().strip().lower()
        item_map = self.item_map_var.get()
        item_diff = self.item_diff_var.get()
        item_cat = self.item_cat_var.get()
        item_rarity = self.item_rarity_var.get()

        source_search = self.source_search_var.get().strip().lower()
        source_item = self.source_item_var.get().strip().lower()
        source_map = self.source_map_var.get()
        source_diff = self.source_diff_var.get()
        source_rarity = self.source_rarity_var.get()

        item_rows = []
        source_rows = []
        for row in self.scan_result.rows:
            if (
                (not item_search or item_search in row["item"].lower() or item_search in row["item_asset"].lower())
                and (item_map == "All" or item_map in row["maps"])
                and (item_diff == "All" or item_diff in row["diffs"])
                and (item_cat == "All" or row["cat"] == item_cat)
                and (item_rarity == "All" or row["rarity"] == item_rarity)
            ):
                item_rows.append(row)

            if (
                (not source_search or source_search in row["source"].lower() or source_search in row["spawner"].lower())
                and (not source_item or source_item in row["item"].lower() or source_item in row["item_asset"].lower())
                and (source_map == "All" or source_map in row["maps"])
                and (source_diff == "All" or source_diff in row["diffs"])
                and (source_rarity == "All" or row["rarity"] == source_rarity)
            ):
                source_rows.append(row)

        self.filtered_item_rows = sorted(item_rows, key=lambda row: (-row["dyn_at_least_one"], row["item"], row["source"]))[:MAX_TREE_ROWS]
        self.filtered_source_rows = source_rows
        self.source_summary_rows = self.summarize_source_rows(source_rows)
        self.fill_item_tree()
        self.fill_source_tree()

    def schedule_filters(self) -> None:
        if self.filter_after_id is not None:
            self.after_cancel(self.filter_after_id)
        self.filter_after_id = self.after(180, self.apply_filters)

    def limit_source_rows(self, rows: list[dict], source_search: str, source_item: str) -> list[dict]:
        if source_search or source_item:
            return sorted(rows, key=lambda row: (-row["dyn_at_least_one"], row["source"], row["item"]))[:MAX_TREE_ROWS]

        grouped: dict[str, list[dict]] = defaultdict(list)
        for row in rows:
            grouped[row["source"]].append(row)

        limited: list[dict] = []
        per_source = min(DEFAULT_SOURCE_ROWS_PER_SOURCE, max(1, MAX_TREE_ROWS // max(1, len(grouped))))
        for source in sorted(grouped):
            best_rows = sorted(grouped[source], key=lambda row: (-row["dyn_at_least_one"], row["item"]))
            limited.extend(best_rows[:per_source])

        return sorted(limited, key=lambda row: (row["source"], -row["dyn_at_least_one"], row["item"]))

    def summarize_source_rows(self, rows: list[dict]) -> list[dict]:
        grouped: dict[tuple[str, str], dict] = {}
        for row in rows:
            key = (row["source"], row["source_kind"])
            summary = grouped.get(key)
            if not summary:
                summary = {
                    "source": row["source"],
                    "source_kind": row["source_kind"],
                    "rows": [],
                    "items": set(),
                    "maps": set(),
                    "diffs": set(),
                    "scenarios": set(),
                    "best_row": row,
                }
                grouped[key] = summary
            summary["rows"].append(row)
            summary["items"].add(row["item_asset"])
            summary["maps"].update(row["maps"])
            summary["diffs"].update(row["diffs"])
            summary["scenarios"].add(self.scenario_key(row))
            if row["dyn_at_least_one"] > summary["best_row"]["dyn_at_least_one"]:
                summary["best_row"] = row

        summaries = []
        for summary in grouped.values():
            best = summary["best_row"]
            summaries.append(
                {
                    "source": summary["source"],
                    "source_kind": summary["source_kind"],
                    "item_count": len(summary["items"]),
                    "scenario_count": len(summary["scenarios"]),
                    "maps": summarize_values(summary["maps"], limit=4),
                    "diffs": summarize_values(summary["diffs"], limit=4),
                    "best_dyn": best["dyn_at_least_one"],
                    "top_item": best["item"],
                    "rows": summary["rows"],
                }
            )

        return sorted(summaries, key=lambda row: (row["source"], row["source_kind"]))

    def scenario_key(self, row: dict) -> tuple:
        return (
            row["map"],
            row["diff"],
            row["group"],
            row["loot_table"],
            row["rate_table"],
            row["rolls"],
        )

    def fill_item_tree(self) -> None:
        self.item_tree.delete(*self.item_tree.get_children())
        for row in self.filtered_item_rows:
            self.item_tree.insert(
                "",
                tk.END,
                values=(
                    row["item"],
                    row["rarity"],
                    row["cat"],
                    row["source"],
                    row["map"],
                    row["diff"],
                    f"G{row['grade']}",
                    row["rolls"],
                    percent(row["dyn_at_least_one"]),
                    percent(row["dyn_per_roll"]),
                    percent(row["base_at_least_one"]),
                    f"{row['loot_table']} / {row['rate_table']}",
                ),
            )

    def fill_source_tree(self) -> None:
        self.source_tree.delete(*self.source_tree.get_children())
        self.source_tree_rows.clear()
        for index, row in enumerate(self.source_summary_rows):
            item_id = f"source-{index}"
            self.source_tree_rows[item_id] = row
            self.source_tree.insert(
                "",
                tk.END,
                iid=item_id,
                values=(
                    row["source"],
                    row["source_kind"],
                    row["item_count"],
                    row["scenario_count"],
                    row["maps"],
                    row["diffs"],
                    percent(row["best_dyn"]),
                    row["top_item"],
                ),
            )

    def open_selected_source(self) -> None:
        selected = self.source_tree.selection()
        if not selected:
            messagebox.showinfo(APP_TITLE, "Select a source first.")
            return
        summary = self.source_tree_rows.get(selected[0])
        if not summary:
            return
        self.open_source_scenario_picker(summary)

    def open_source_scenario_picker(self, summary: dict) -> None:
        rows = summary["rows"]
        maps = sorted({map_name for row in rows for map_name in row["maps"]}, key=map_sort_key)
        all_maps_label = "All Maps"
        all_diffs_label = "All Difficulties"

        dialog = tk.Toplevel(self)
        dialog.title(f"{summary['source']} Scenario")
        dialog.geometry("460x230")
        dialog.configure(bg="#1e1e1e")
        dialog.transient(self)
        dialog.grab_set()

        body = ttk.Frame(dialog)
        body.pack(fill=tk.BOTH, expand=True, padx=18, pady=16)
        ttk.Label(body, text=summary["source"], style="Header.TLabel").grid(row=0, column=0, columnspan=2, sticky=tk.W, pady=(0, 14))

        map_var = tk.StringVar(value=maps[0] if maps else all_maps_label)
        diff_var = tk.StringVar(value="")

        ttk.Label(body, text="Map:").grid(row=1, column=0, sticky=tk.E, padx=(0, 8), pady=6)
        map_combo = ttk.Combobox(body, textvariable=map_var, values=[all_maps_label] + maps, state="readonly")
        map_combo.grid(row=1, column=1, sticky=tk.EW, pady=6)

        ttk.Label(body, text="Difficulty:").grid(row=2, column=0, sticky=tk.E, padx=(0, 8), pady=6)
        diff_combo = ttk.Combobox(body, textvariable=diff_var, values=[all_diffs_label], state="readonly")
        diff_combo.grid(row=2, column=1, sticky=tk.EW, pady=6)

        count_var = tk.StringVar()
        ttk.Label(body, textvariable=count_var, foreground="#4ec9b0").grid(row=3, column=1, sticky=tk.W, pady=(4, 8))
        body.grid_columnconfigure(1, weight=1)

        def matching_rows_for_choices() -> list[dict]:
            chosen_map = map_var.get()
            chosen_diff = diff_var.get()
            return [
                row
                for row in rows
                if (chosen_map == all_maps_label or chosen_map in row["maps"])
                and (chosen_diff == all_diffs_label or chosen_diff in row["diffs"])
            ]

        def refresh_difficulties(_event=None) -> None:
            chosen_map = map_var.get()
            matching = [row for row in rows if chosen_map == all_maps_label or chosen_map in row["maps"]]
            diffs = sorted({diff for row in matching for diff in row["diffs"]}, key=difficulty_sort_key)
            diff_values = [all_diffs_label] + diffs
            diff_combo.config(values=diff_values)
            if diff_var.get() not in diff_values:
                diff_var.set(diffs[0] if diffs else all_diffs_label)
            count_var.set(f"{len(matching_rows_for_choices()):,} rows match")

        def refresh_count(_event=None) -> None:
            count_var.set(f"{len(matching_rows_for_choices()):,} rows match")

        def open_detail() -> None:
            selected_rows = matching_rows_for_choices()
            if not selected_rows:
                messagebox.showinfo(APP_TITLE, "No drops match that map and difficulty.")
                return
            selected_map = map_var.get()
            selected_diff = diff_var.get()
            dialog.destroy()
            self.open_source_detail(summary, selected_rows, selected_map, selected_diff)

        map_combo.bind("<<ComboboxSelected>>", refresh_difficulties)
        diff_combo.bind("<<ComboboxSelected>>", refresh_count)
        button_row = ttk.Frame(body)
        button_row.grid(row=4, column=0, columnspan=2, sticky=tk.E, pady=(12, 0))
        ttk.Button(button_row, text="Cancel", command=dialog.destroy).pack(side=tk.RIGHT)
        ttk.Button(button_row, text="Open Drops", command=open_detail).pack(side=tk.RIGHT, padx=(0, 8))

        refresh_difficulties()
        dialog.bind("<Return>", lambda _event: open_detail())
        dialog.bind("<Escape>", lambda _event: dialog.destroy())
        map_combo.focus_set()

    def open_source_detail(self, summary: dict, selected_rows: list[dict], selected_map: str, selected_diff: str) -> None:
        rows = sorted(
            selected_rows,
            key=lambda row: (
                row["map"],
                row["diff"],
                row["group"],
                row["loot_table"],
                row["rate_table"],
                -row["dyn_at_least_one"],
                row["item"],
            ),
        )
        window = tk.Toplevel(self)
        window.title(f"{summary['source']} Drops")
        window.geometry("1320x720")
        window.configure(bg="#1e1e1e")

        header = ttk.Frame(window)
        header.pack(fill=tk.X, padx=14, pady=(12, 8))
        ttk.Label(header, text=summary["source"], style="Header.TLabel").pack(side=tk.LEFT)
        filter_label = f"{selected_map} / {selected_diff}"
        scenario_count = len({self.scenario_key(row) for row in rows})
        header_info = tk.StringVar(value=f"{filter_label} | {len(rows):,} rows | {scenario_count:,} scenarios")
        ttk.Label(header, textvariable=header_info, foreground="#4ec9b0").pack(side=tk.LEFT, padx=14)

        visible_state = {"rows": rows}
        ttk.Button(header, text="Export CSV...", command=lambda: self.export_specific_rows(visible_state["rows"], summary["source"])).pack(side=tk.RIGHT)

        search_frame = ttk.Frame(window)
        search_frame.pack(fill=tk.X, padx=14, pady=(0, 8))
        ttk.Label(search_frame, text="Search:").pack(side=tk.LEFT)
        detail_search_var = tk.StringVar()
        tk.Entry(search_frame, textvariable=detail_search_var, bg="#252526", fg="white", insertbackground="white").pack(
            side=tk.LEFT, fill=tk.X, expand=True, padx=(8, 0), ipady=3
        )

        columns = (
            "scenario",
            "item",
            "rarity",
            "cat",
            "grade",
            "count",
            "rolls",
            "dyn_one",
            "dyn_roll",
            "base_one",
            "spawn",
            "loot",
            "rate",
        )
        headings = {
            "scenario": "Scenario",
            "item": "Item",
            "rarity": "Rarity",
            "cat": "Category",
            "grade": "Grade",
            "count": "Count",
            "rolls": "Rolls",
            "dyn_one": "Dyn At Least One",
            "dyn_roll": "Dyn Per Roll",
            "base_one": "Base At Least One",
            "spawn": "Spawn Wt",
            "loot": "Loot Table",
            "rate": "Rate Table",
        }
        widths = {
            "scenario": 320,
            "item": 190,
            "rarity": 82,
            "cat": 120,
            "grade": 62,
            "count": 60,
            "rolls": 60,
            "dyn_one": 116,
            "dyn_roll": 104,
            "base_one": 116,
            "spawn": 78,
            "loot": 220,
            "rate": 220,
        }
        container = ttk.Frame(window)
        container.pack(fill=tk.BOTH, expand=True, padx=14, pady=(0, 14))
        tree = ttk.Treeview(container, columns=columns, show="headings")
        yscroll = ttk.Scrollbar(container, orient=tk.VERTICAL, command=tree.yview)
        xscroll = ttk.Scrollbar(container, orient=tk.HORIZONTAL, command=tree.xview)
        tree.configure(yscrollcommand=yscroll.set, xscrollcommand=xscroll.set)
        for column in columns:
            tree.heading(column, text=headings[column], command=lambda col=column, tr=tree: self.sort_tree(tr, col))
            anchor = tk.CENTER if column in {"rarity", "grade", "count", "rolls", "dyn_one", "dyn_roll", "base_one", "spawn"} else tk.W
            tree.column(column, width=widths[column], anchor=anchor, stretch=True)
        tree.grid(row=0, column=0, sticky="nsew")
        yscroll.grid(row=0, column=1, sticky="ns")
        xscroll.grid(row=1, column=0, sticky="ew")
        container.grid_rowconfigure(0, weight=1)
        container.grid_columnconfigure(0, weight=1)

        def row_matches_search(row: dict, search_text: str) -> bool:
            if not search_text:
                return True
            haystack = " ".join(
                [
                    row["item"],
                    row["rarity"],
                    row["cat"],
                    row["map"],
                    row["diff"],
                    row["map_code"],
                    row["group"],
                    row["loot_table"],
                    row["rate_table"],
                ]
            ).lower()
            return all(term in haystack for term in search_text.split())

        def populate_detail(*_args) -> None:
            search_text = detail_search_var.get().strip().lower()
            visible_rows = [row for row in rows if row_matches_search(row, search_text)]
            visible_state["rows"] = visible_rows
            tree.delete(*tree.get_children())
            for row in visible_rows[:MAX_TREE_ROWS]:
                scenario = f"{row['map_code']} {row['map']} | {row['diff']} | {row['group']}"
                tree.insert(
                    "",
                    tk.END,
                    values=(
                        scenario,
                        row["item"],
                        row["rarity"],
                        row["cat"],
                        f"G{row['grade']}",
                        row["item_count"],
                        row["rolls"],
                        percent(row["dyn_at_least_one"]),
                        percent(row["dyn_per_roll"]),
                        percent(row["base_at_least_one"]),
                        f"{row['spawn_rate']:.0f}",
                        row["loot_table"],
                        row["rate_table"],
                    ),
                )
            suffix = f" | showing {min(len(visible_rows), MAX_TREE_ROWS):,}/{len(visible_rows):,}" if len(visible_rows) > MAX_TREE_ROWS else f" | showing {len(visible_rows):,}"
            header_info.set(f"{filter_label} | {len(rows):,} rows | {scenario_count:,} scenarios{suffix}")

        detail_search_var.trace_add("write", populate_detail)
        populate_detail()

    def export_specific_rows(self, rows: list[dict], source_name: str) -> None:
        safe_name = re.sub(r"[^A-Za-z0-9_-]+", "_", source_name).strip("_") or "source"
        path = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV", "*.csv")],
            initialfile=f"loot_spawn_{safe_name}.csv",
        )
        if path:
            export_rows_to_csv(Path(path), rows, self.current_luck())
            self.status_var.set(f"Exported {len(rows):,} rows to {path}")

    def sort_tree(self, tree: ttk.Treeview, column: str) -> None:
        values = [(tree.set(item, column), item) for item in tree.get_children("")]

        def sortable(value: str):
            clean = value.replace("%", "").replace(",", "")
            if clean.startswith("G") and clean[1:].isdigit():
                return int(clean[1:])
            try:
                return float(clean)
            except ValueError:
                return value.lower()

        descending = getattr(tree, "_sort_column", None) == column and not getattr(tree, "_sort_desc", False)
        values.sort(key=lambda pair: sortable(pair[0]), reverse=descending)
        for index, (_value, item) in enumerate(values):
            tree.move(item, "", index)
        tree._sort_column = column
        tree._sort_desc = descending

    def export_filtered_csv(self) -> None:
        rows = self.filtered_item_rows if self.notebook.index(self.notebook.select()) == 1 else self.filtered_source_rows
        if not rows:
            messagebox.showinfo(APP_TITLE, "No filtered rows to export.")
            return
        path = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV", "*.csv")],
            initialfile="loot_spawn_filtered.csv",
        )
        if path:
            export_rows_to_csv(Path(path), rows, self.current_luck())
            self.status_var.set(f"Exported {len(rows):,} rows to {path}")

    def browse_single(self, kind: str) -> None:
        initial = self.root_dir
        generated = find_generated_root(initial)
        if generated:
            initial = generated / "LootDrop" / ("LootDropRate" if kind == "rate" else "LootDrop")
        path = filedialog.askopenfilename(initialdir=str(initial), filetypes=[("JSON", "*.json")])
        if path:
            if kind == "rate":
                self.open_single_rate(Path(path))
            else:
                self.open_single_loot(Path(path))

    def open_single_rate(self, path: Path | None = None) -> None:
        if path is None:
            self.browse_single("rate")
            return
        try:
            self.single_rate = parse_rate_table(Path(path))
            self.rate_drop_label.config(text=f"Rate: {self.single_rate.display}")
            self.update_single_tables()
        except Exception as exc:
            messagebox.showerror(APP_TITLE, f"Could not read rate file:\n{exc}")

    def open_single_loot(self, path: Path | None = None) -> None:
        if path is None:
            self.browse_single("loot")
            return
        try:
            self.single_drop = parse_loot_drop(Path(path))
            self.loot_drop_label.config(text=f"Loot: {self.single_drop.display}")
            self.update_single_tables()
        except Exception as exc:
            messagebox.showerror(APP_TITLE, f"Could not read loot file:\n{exc}")

    def update_single_tables(self) -> None:
        for tree in [getattr(self, "single_rate_tree", None), getattr(self, "single_item_tree", None)]:
            if tree:
                tree.delete(*tree.get_children())
        if self.single_rate:
            base_probs, dyn_probs = grade_probabilities(self.single_rate.rates, self.current_luck())
            for grade, rate in enumerate(self.single_rate.rates):
                self.single_rate_tree.insert(
                    "",
                    tk.END,
                    values=(f"G{grade}", f"{rate:.0f}", f"{luck_scalar(self.current_luck(), grade):.4f}", percent(base_probs[grade]), percent(dyn_probs[grade])),
                )
        if not self.single_rate or not self.single_drop:
            return
        base_probs, dyn_probs = grade_probabilities(self.single_rate.rates, self.current_luck())
        counts = Counter(
            (choice.item_asset, choice.item_name, choice.rarity, choice.grade, choice.item_count)
            for choice in self.single_drop.real_choices
            if 0 <= choice.grade <= MAX_LUCK_GRADE
        )
        for (item_asset, item_name, rarity, grade, item_count), choice_count in sorted(counts.items(), key=lambda pair: (pair[0][3], pair[0][1])):
            grade_total = self.single_drop.grade_totals.get(grade, 0)
            if grade_total <= 0:
                continue
            base = base_probs[grade] * choice_count / grade_total
            dyn = dyn_probs[grade] * choice_count / grade_total
            if base <= 0 and dyn <= 0:
                continue
            self.single_item_tree.insert(
                "",
                tk.END,
                values=(item_name, rarity, item_count, f"G{grade}", f"{choice_count}/{grade_total}", percent(base), percent(dyn), self.single_drop.display),
            )


def run_scan_only(root: Path, luck: int, csv_path: Path | None) -> int:
    result = build_database(root, luck)
    print(f"Generated root: {result.stats['generated_root']}")
    print(f"Loot tables: {result.stats['loot_tables']:,}")
    print(f"Rate tables: {result.stats['rate_tables']:,}")
    print(f"Loot groups: {result.stats['groups']:,}")
    print(f"Dungeon codes: {result.stats.get('dungeon_codes', 0):,}")
    print(f"Resolved item names: {result.stats.get('item_names', 0):,}")
    print(f"Resolved prop names: {result.stats.get('prop_names', 0):,}")
    print(f"Resolved monster names: {result.stats.get('monster_names', 0):,}")
    print(f"Spawner entries: {result.stats['spawner_entries']:,}")
    print(f"Unique sources: {result.stats.get('sources', 0):,}")
    print(f"Unique items: {result.stats.get('items', 0):,}")
    print(f"Item rows: {result.stats['rows']:,}")
    if result.warnings:
        print(f"Warnings: {len(result.warnings):,}")
        for warning in result.warnings[:20]:
            print(f"  - {warning}")
    if csv_path:
        export_rows_to_csv(csv_path, result.rows, luck)
        print(f"CSV exported: {csv_path}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=APP_TITLE)
    parser.add_argument("root", nargs="?", default=".", help="Export root, Content folder, or Generated/V2 folder")
    parser.add_argument("--luck", type=int, default=500, help="Luck value for dynamic probabilities")
    parser.add_argument("--scan-only", action="store_true", help="Build the database in the console and exit")
    parser.add_argument("--csv", type=Path, help="Export scan rows to CSV")
    args = parser.parse_args(argv)

    if args.scan_only or args.csv:
        return run_scan_only(Path(args.root), args.luck, args.csv)

    try:
        app = LootAnalyzerApp()
        app.mainloop()
    except (RuntimeError, getattr(tk, "TclError", RuntimeError)) as exc:
        print("Could not start the Tkinter GUI.")
        print("Install/use a Python build with Tkinter enabled, or run with --scan-only for console validation.")
        print(exc)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
