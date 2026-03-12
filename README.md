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
├── download_from_pakar.py          # Download alerts from Telegram channel
├── analyze_pakar_alerts.py         # Main analysis pipeline
├── districts_eng_with_hebrew_areas.json   # Official city ↔ district mapping
└── README.md
```

---

## Requirements
```bash
conda env create -f environment.yml
```

---

## Usage

### 1. Download from Telegram

```bash
python download_from_pakar.py \
    --channel PikudHaOref_all \
    --api_id YOUR_API_ID --api_hash YOUR_API_HASH \
    --start_date 2026-02-28 --end_date 2026-03-12 \ # end_date is optional.
    --output PikudHaOref_alerts.csv 
```

Produces a CSV with columns `date, text`.

### 2. Run the pipeline

```bash
python analyze_pakar_alerts.py \
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
| `--min-pre` | `1.0` | Minimum gap (minutes) to count a warning link |
| `--max-pre` | `30.0` | Maximum gap (minutes) to count a warning link |
| `--min-post` | `5.0` | Minimum gap (minutes) to count an ended link |
| `--max-post` | `60.0` | Maximum gap (minutes) to count an ended link |

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

## 28/2 - 12/02 output
```
Warning → Missiles (gap in minutes)
Area                        n   cities  evt/city   median     mean     Q25     Q75        range
─────────────────────────────────────────────────────────────────────────────────────────────────
Jerusalem                 395       16     24.69      5.7      7.0     4.0     7.0     1.3–29.4
Greater Tel Aviv         1884       47     40.09      5.8      6.1     4.4     7.1     1.0–24.6
Judea & Samaria          4095      185     22.14      6.1      7.6     4.7     7.7     1.2–29.2
Lachish Region           1559      107     14.57      6.2      7.4     5.1     7.6     1.3–28.2
Haifa Region             1100       92     11.96      6.2      6.3     4.5     7.3     1.2–17.9
Jordan Valley             990       71     13.94      6.5      7.5     4.6     8.4     1.2–27.6
Sharon                   3907      127     30.76      6.5      7.7     5.3     7.8     1.2–24.7
Golan                     419       48      8.73      6.6      7.4     4.3     8.8     2.5–18.4
Jezreel Valley            860       92      9.35      6.8      6.8     4.7     7.9     1.2–18.3
Galilee                  2074      137     15.14      6.8      6.9     4.5     7.9     2.0–24.2
Negev                    1067       88     12.12      7.0      7.1     6.3     7.7     4.7–29.6
Conf. Line                756       89      8.49      7.3      8.6     4.6    11.1     1.8–28.2
Gaza Envelope             289       42      6.88      7.5      8.8     6.7     8.2     2.7–29.6
Arava                       2        1      2.00      8.0      8.0     7.0     9.0     6.0–10.0

  Mean of area medians:   6.6 min
  Median of area medians: 6.6 min
  Overall range:          1.1 – 29.6 min

Missiles → Ended   (gap in minutes)
Area                        n   cities  evt/city   median     mean     Q25     Q75        range
─────────────────────────────────────────────────────────────────────────────────────────────────
Eilat                       1        1      1.00      7.5      7.5     7.5     7.5      7.5–7.5
Conf. Line               1590       89     17.87     11.0     12.9     9.8    14.0     5.0–44.7
Arava                      11        9      1.22     11.1     10.9    10.4    12.0     7.8–12.6
Golan                     349       49      7.12     11.2     12.4    10.4    12.2     5.4–56.5
Haifa Region              764       97      7.88     11.5     12.1    10.8    12.3     8.1–21.2
Jordan Valley             724       71     10.20     11.5     12.7    10.7    13.0     5.6–29.0
Galilee                  1313      139      9.45     11.6     12.4    10.7    12.8     5.2–34.2
Shephelah                  10       10      1.00     11.7     11.5    11.5    11.7    10.4–11.7
Jezreel Valley            514       92      5.59     11.7     12.5    11.4    12.7     7.4–34.5
Gaza Envelope             216       43      5.02     11.9     13.2    11.3    12.4     5.1–26.6
Negev                     747       87      8.59     11.9     12.8    11.3    13.5     5.1–24.6
Lachish Region           1190      106     11.23     12.0     13.8    11.3    14.0     8.0–28.4
Sharon                   2526      127     19.89     12.4     13.0    11.3    14.0     8.7–19.6
Greater Tel Aviv         1371       51     26.88     12.5     13.1    11.4    13.8     9.1–31.9
Judea & Samaria          2991      182     16.43     12.6     13.8    11.5    14.6     6.6–29.5
Jerusalem                 328       16     20.50     13.0     13.9    11.7    14.8     9.5–25.2

  Mean of area medians:   11.6 min
  Median of area medians: 11.7 min
  Overall range:          5.0 – 56.5 min
```
---

## Design notes

**Why neighbor lookup instead of clustering?** Alert messages arrive in bursts — a single barrage can produce dozens of messages within seconds. Clustering those bursts into events and then linking events introduces two sources of error: the cluster boundaries, and the inter-cluster matching logic. The neighbor approach sidesteps both: within a city's sorted timeline, a pre-warning followed immediately by a missile alert *is* a linked pair by definition, with no intermediate decisions required.

**Why city level?** A single Telegram message can cover 50+ cities across multiple districts. Aggregating to event level obscures variation — a warning may arrive 3 minutes before impact in Tel Aviv and 10 minutes before impact in Galilee within the same salvo. City-level records preserve that signal.

**The `_raw` vs `_valid` split** keeps the threshold filter out of the core logic. You can reload `_raw` and re-filter at any threshold without re-running the pipeline.
