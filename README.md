# PikudHaOref Alert Pipeline

Parse and analyse Israeli Home Front Command (Pikud HaOref) rocket and UAV alerts sourced from Telegram — measuring warning lead times and shelter durations at city level.

---

## What it does

Pikud HaOref sends four types of alert messages via Telegram:

| Type | Hebrew trigger | Meaning |
|---|---|---|
| `pre_warning` | בדקות הקרובות צפויות להתקבל התרעות | Pre-warning: alerts expected shortly |
| `missiles` | ירי רקטות / ירי טילים | Rocket or missile fire |
| `uav` | חדירת כלי טיס עוין | Hostile UAV infiltration |
| `ended` | האירוע הסתיים | Event ended / all-clear |

Each message lists the affected areas and cities in Hebrew. The pipeline:

1. **Downloads** alert messages from a Telegram channel into a CSV
2. **Classifies** each message by type
3. **Expands** each message to one row per city using an official district mapping
4. **Sorts** the resulting dataframe by city and datetime
5. **Links** each missile/UAV alert to its immediate neighbors — the alert that came just before it and the alert that came just after it, within the same city
6. **Produces** two output tables: warning lead times and shelter durations, one record per city per alert

The neighbor lookup is purely positional: no clustering, no scoring. If a missile alert's preceding row (same city) is a `pre_warning`, that's a warning link. If its following row is an `ended`, that's an ended link. Threshold filters are applied afterwards as a simple range filter on `gap_min`.

---

## Repo structure

```
├── telegram_scraper.py          # Download alerts from Telegram channel
├── pakar_pipeline_v3.py         # Main analysis pipeline
├── districts_eng_with_hebrew_areas.json   # Official city ↔ district mapping
└── README.md
```

---

## Requirements

```
pip install pandas telethon
```

---

## Usage

### 1. Download from Telegram

```bash
python telegram_scraper.py --channel PikudHaOref --output PikudHaOref_alerts.csv
```

Produces a CSV with columns `date, text`.

### 2. Run the pipeline

```bash
python pakar_pipeline_v3.py \
  --input      PikudHaOref_alerts.csv \
  --mapping    districts_eng_with_hebrew_areas.json \
  --output-dir ./output
```

#### All options

| Argument | Default | Description |
|---|---|---|
| `--input` | `PikudHaOref_alerts.csv` | Input CSV from Telegram scraper |
| `--mapping` | `districts_eng_with_hebrew_areas.json` | City/district mapping file |
| `--output-dir` | `.` | Directory for output CSVs |
| `--min-pre` | `0.5` | Minimum gap (minutes) to count a warning link |
| `--max-pre` | `30.0` | Maximum gap (minutes) to count a warning link |
| `--min-post` | `2.0` | Minimum gap (minutes) to count an ended link |
| `--max-post` | `90.0` | Maximum gap (minutes) to count an ended link |

---

## Outputs

| File | Description |
|---|---|
| `working_df.csv` | Master dataframe — one row per `(datetime, alert_type, city)` |
| `warning_records_raw.csv` | All warning → missile pairs, before threshold filter |
| `warning_records_valid.csv` | Warning pairs within `--min-pre` / `--max-pre` |
| `ended_records_raw.csv` | All missile → ended pairs, before threshold filter |
| `ended_records_valid.csv` | Ended pairs within `--min-post` / `--max-post` |

### `working_df.csv` schema

| Column | Description |
|---|---|
| `date` | Date of the alert |
| `time` | Time of the alert (HH:MM:SS) |
| `datetime` | Full timestamp |
| `alert_type` | `pre_warning`, `missiles`, `uav`, or `ended` |
| `city` | English city name |
| `area` | English area/district name |

### `warning_records_valid.csv` schema

| Column | Description |
|---|---|
| `date` | Date |
| `warning_time` | Timestamp of the pre-warning |
| `missile_time` | Timestamp of the missile/UAV alert |
| `gap_min` | Lead time in minutes (warning → missile) |
| `city` | City |
| `area` | Area/district |
| `missile_type` | `missiles` or `uav` |

### `ended_records_valid.csv` schema

| Column | Description |
|---|---|
| `date` | Date |
| `missile_time` | Timestamp of the missile/UAV alert |
| `ended_time` | Timestamp of the all-clear |
| `gap_min` | Shelter duration in minutes (missile → ended) |
| `city` | City |
| `area` | Area/district |
| `missile_type` | `missiles` or `uav` |

---

## Sample output

Warning lead times by area (partial dataset, Mar 10–11 2026):

```
Area                        n   median     mean     Q25     Q75        range
──────────────────────────────────────────────────────────────────────────
Golan                      15      2.8      2.9     2.8     2.8      2.8–3.4
Jerusalem                  50      4.3      4.7     4.0     6.2      3.2–6.2
Jordan Valley              98      4.5      5.2     4.1     6.7      2.5–8.4
Greater Tel Aviv          238      5.3      5.7     4.6     6.0      3.9–9.9
Sharon                    472      5.3      6.2     4.8     7.2     2.9–11.2
Judea & Samaria           433      5.8      5.7     4.5     6.7     3.0–11.3
Lachish Region            161      6.0      6.2     5.5     7.2     4.0–18.9
Haifa Region              143      6.4      6.4     6.2     6.4      6.2–8.6
Galilee                   161      8.8      9.2     8.8     9.9     2.9–10.6

  Median of area medians: 5.5 min
  Overall range:          2.5 – 18.9 min
```

---

## Design notes

**Why neighbor lookup instead of clustering?** Alert messages arrive in bursts — a single barrage can produce dozens of messages within seconds. Clustering those bursts into events and then linking events introduces two sources of error: the cluster boundaries, and the inter-cluster matching logic. The neighbor approach sidesteps both: within a city's sorted timeline, a pre-warning followed immediately by a missile alert *is* a linked pair by definition, with no intermediate decisions required.

**Why city level?** A single Telegram message can cover 50+ cities across multiple districts. Aggregating to event level obscures variation — a warning may arrive 3 minutes before impact in Tel Aviv and 10 minutes before impact in Galilee within the same salvo. City-level records preserve that signal.

**The `_raw` vs `_valid` split** keeps the threshold filter out of the core logic. You can reload `_raw` and re-filter at any threshold without re-running the pipeline.
