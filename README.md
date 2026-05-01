# DungeonCrawler Loot Browser

Local loot search app for generated DungeonCrawler export files.

## Quick Start

1. Keep the exported `Content` folder beside these files.
2. Double-click `run_loot_spawn_web.bat`.
3. Open the browser page it prints or opens automatically: `http://127.0.0.1:8765/`.
4. Wait for the scan to finish, then search from `Mob/Chest Search` or `Item Search`.

The app auto-detects exports shaped like:

```text
Content/DungeonCrawler/Data/Generated/V2
```

You can also paste a different export root into the `Export root` box and press `Scan`.

## Main Features

- Search mobs, chests, props, and spawners by readable names.
- Hide raw `Spawner` fallback sources so results focus on real monsters, props, and loot sources.
- Double-click a source to choose map and difficulty before opening its drops.
- Search items across all sources, maps, and difficulties.
- Save the scanned data with `Save Scan Cache`, then reopen the app later without rescanning.
- Export current item results or source drops to CSV.

## Requirements

- Windows
- Python 3.10 or newer

The browser app uses Python's standard library. The older Tkinter desktop analyzer is still present in `loot_spawn_analyzer.py`, but the recommended distributable app is `run_loot_spawn_web.bat`.

## Notes

- The first scan can take around 30-60 seconds on a large export.
- Luck changes reuse the loaded scan and update only the current view.
- If a new scan fails because the folder is wrong, the browser keeps the last successful results loaded.
- The app auto-loads `loot_spawn_cache.pkl.gz` on startup when it exists beside `loot_spawn_web.py`.
- Dungeon grade codes use the explicit game table: `100x = PVE`, `200x = Normal`, `300x = High Roller`, `400x = Squire`.
