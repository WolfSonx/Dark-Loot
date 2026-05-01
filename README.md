# DungeonCrawler Loot Browser
A local browser app for exploring loot tables from generated Dark And Darker export files.

The app scans generated JSON exports, links loot groups, drop-rate tables, mobs, chests, props, items, maps, difficulties, rarities, and luck scaling into one searchable interface. It runs on your own machine at http://127.0.0.1:8765/ and does not require a public server.

## Features
- Search mobs, chests, props, and loot sources.
- Search items across all sources, maps, and difficulties.
- Open an item to see every source that can drop it.
- Double-click or open a source, then choose the map and difficulty before viewing drops.
- Search inside a source's drop list.
- Group duplicate item results so common items do not flood the table.
- Sort tables by clicking column headers.
- Apply luck instantly to the current view without rescanning files.
- Compare a drop table against another luck value.
- Favorite sources and items.
- Save a scan cache so future launches do not need to rescan the generated files.
- Export item results and source drops to CSV.

```
Content/DungeonCrawler/Data/Generated/V2
```
You can also paste a different export root into the Export root box and press Scan.

## Quick Start:

1. Run build_exe.bat.
2. Open the browser page it prints or opens automatically:
```http://127.0.0.1:8765/```
