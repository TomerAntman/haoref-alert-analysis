import argparse
import json
import re
from pathlib import Path

import pandas as pd

# ── CLI ────────────────────────────────────────────────────────────────────────
parser = argparse.ArgumentParser()
parser.add_argument("--input",       default="PikudHaOref_alerts.csv")
parser.add_argument("--mapping",     default="districts_eng_with_hebrew_areas.json")
parser.add_argument("--output-dir",  default="./output_csvs")
parser.add_argument("--min-pre",  type=float, default=1.0,  help="Min minutes: warning → missile")
parser.add_argument("--max-pre",  type=float, default=30.0, help="Max minutes: warning → missile")
parser.add_argument("--min-post", type=float, default=5.0,  help="Min minutes: missile → ended")
parser.add_argument("--max-post", type=float, default=60.0, help="Max minutes: missile → ended")
args = parser.parse_args()

outdir = Path(args.output_dir)
outdir.mkdir(parents=True, exist_ok=True)


# ── 1. AREA MAPPINGS ──────────────────────────────────────────────────────────
AREA_MERGE = {
    "North Golan":        "Golan",
    "South Golan":        "Golan",
    "Lachish":            "Lachish Region",
    "West Lachish":       "Lachish Region",
    "South Negev":        "Negev",
    "Center Negev":       "Negev",
    "West Negev":         "Negev",
    "Upper Galilee":      "Galilee",
    "Lower Galilee":      "Galilee",
    "Center Galilee":     "Galilee",
    "Confrontation Line": "Conf. Line",
    "Yehuda":             "Judea & Samaria",
    "Shomron":            "Judea & Samaria",
    "Shfelat Yehuda":     "Judea & Samaria",
    "Beit She'an Valley": "Jordan Valley",
    "Bika'a":             "Jordan Valley",
    "Dead Sea":           "Jordan Valley",
    "Dan":                "Greater Tel Aviv",
    "Yarkon":             "Greater Tel Aviv",
    "HaCarmel":           "Haifa Region",
    "HaMifratz":          "Haifa Region",
    "Wadi Ara":           "Haifa Region",
    "Menashe":            "Haifa Region",
    "HaAmakim":           "Jezreel Valley",
    "HaShfela":           "Shephelah",
    "Shfela":             "Shephelah",
    "Gaza Envelope":      "Gaza Envelope",
}

SUBLOCATION_MAP = {
    "חוף אכזיב": ("Rosh HaNikra", "Galilee"),
    "קלע אלון":  ("Rosh HaNikra", "Galilee"),
    "רמת טראמפ": ("Neve Ativ",    "Golan"),
}


# ── 2. RESOLVER ───────────────────────────────────────────────────────────────
class Resolver:
    def __init__(self, mapping_path: str):
        with open(mapping_path, encoding="utf-8") as f:
            raw = json.load(f)
        self.city_map = (
            pd.DataFrame(raw)[["label", "label_he", "areaname", "areaname_he"]]
            .rename(columns={"label": "city_en", "label_he": "city_he",
                             "areaname": "area_en", "areaname_he": "area_he"})
            .drop_duplicates()
        )
        self.city_map["area_merged"] = self.city_map["area_en"].replace(AREA_MERGE)
        self.known_cities_he = set(self.city_map["city_he"].dropna())

        self.area_he_to_en = (
            self.city_map[["area_he", "area_en"]].drop_duplicates()
            .set_index("area_he")["area_en"].to_dict()
        )
        self.area_he_to_en.update({
            "דן": "Dan", "ירקון": "Yarkon", "בקעה": "Bika'a",
            "בקעת בית שאן": "Beit She'an Valley", "גולן": "North Golan",
            "קצרין": "South Golan", "שומרון": "Shomron", "יהודה": "Yehuda",
            "מרכז הנגב": "Center Negev", "דרום הנגב": "South Negev",
            "מערב הנגב": "West Negev", "עוטף עזה": "Gaza Envelope",
            "ים המלח": "Dead Sea", "ערבה": "Arava", "אילת": "Eilat",
            "שרון": "Sharon", "ירושלים": "Jerusalem", "בית שמש": "Shfelat Yehuda",
            "קריות": "HaMifratz", "תבור": "HaAmakim", "השפלה": "HaShfela", "שפלה": "HaShfela",
            "דרום השפלה": "Lachish", "מערב לכיש": "West Lachish", "לכיש": "Lachish",
            "חוף הכרמל": "HaCarmel", "חיפה": "Menashe", "מנשה": "Wadi Ara",
            "ואדי ערה": "Wadi Ara", "קו העימות": "Confrontation Line",
            "גליל עליון": "Upper Galilee", "גליל תחתון": "Lower Galilee",
            "חפר": "Sharon", "יערות הכרמל": "HaCarmel",
            "עמק יזרעאל": "HaAmakim", "עמק החולה": "Upper Galilee",
        })

    def resolve_token(self, token: str):
        """Hebrew city/area token → list of (city_en, area_merged)."""
        token = token.strip()
        if not token:
            return []
        rows = self.city_map[self.city_map["city_he"] == token]
        if not rows.empty:
            return [(r["city_en"], r["area_merged"]) for _, r in rows.iterrows()]
        en = self.area_he_to_en.get(token)
        if en:
            merged = AREA_MERGE.get(en, en)
            area_rows = self.city_map[
                (self.city_map["area_en"] == en) |
                (self.city_map["area_merged"] == merged)
            ]
            return [(r["city_en"], r["area_merged"]) for _, r in area_rows.iterrows()]
        if " ו" in token:
            results = []
            for p in re.split(r"\s+ו", token):
                results.extend(self.resolve_token(p.strip()))
            if results:
                return results
        if token in SUBLOCATION_MAP:
            city_en, area_en = SUBLOCATION_MAP[token]
            return [(city_en, area_en)]
        return []


# ── 3. TEXT HELPERS ───────────────────────────────────────────────────────────
def strip_markdown(text) -> str:
    if pd.isna(text):
        return ""
    text = re.sub(r'\[([^\]]*)\]\([^)]*\)', r'\1', str(text))
    return re.sub(r'\*\*|__', '', text)


def classify(text: str) -> str:
    if "האירוע הסתיים" in text:                                   return "ended"
    if "בדקות הקרובות צפויות להתקבל התרעות" in text or \
       "ייתכן ויופעלו התרעות" in text:                           return "pre_warning"
    if "ירי רקטות" in text or "ירי טילים" in text:               return "missiles"
    if "חדירת כלי טיס" in text:                                   return "uav"
    return "other"


SKIP_KW = ("היכנסו", "על תושבי", "בדקות הקרובות", "האירוע הסתיים",
           "השוהים", "במקרה של", "עדכון", "מבזק", "ירי רקטות",
           "חדירת כלי", "בעת קבלת")

def parse_area_blocks(text: str):
    """Extract [(city_he, area_he), ...] from any אזור X / city-list message."""
    results, current_area = [], None
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        m = re.match(r"^אזור\s+(.+)$", line)
        if m:
            current_area = m.group(1).strip()
            continue
        if any(kw in line for kw in SKIP_KW):
            continue
        if current_area is None:
            continue
        city_line = re.sub(r"\s*\(.*?\)\s*$", "", line).strip()
        for part in city_line.split(","):
            city_he = part.strip().rstrip(".")
            if city_he:
                results.append((city_he, current_area))
    return results


# ── 4. LOAD & CLASSIFY ────────────────────────────────────────────────────────
df_raw = pd.read_csv(args.input)
df_raw["date"] = pd.to_datetime(df_raw["date"], errors="coerce").dt.tz_localize(None)
df_raw = df_raw[df_raw["date"].notna()].sort_values("date").reset_index(drop=True)
df_raw["text"] = df_raw["text"].apply(strip_markdown)
df_raw["alert_type"] = df_raw["text"].apply(classify)
df_raw = df_raw[df_raw["alert_type"] != "other"].copy()
print(f"Messages: {df_raw['alert_type'].value_counts().to_dict()}")

resolver = Resolver(args.mapping)


# ── 5. EXPAND TO CITY-LEVEL ROWS ──────────────────────────────────────────────
# Each message → one row per city mentioned.
# All four alert types use the same אזור X / city-list format.

all_rows = []
for _, msg in df_raw.iterrows():
    for city_he, area_he in parse_area_blocks(msg["text"]):
        for city_en, area_en in resolver.resolve_token(city_he):
            all_rows.append({
                "datetime":   msg["date"],
                "alert_type": msg["alert_type"],
                "city":       city_en,
                "area":       area_en,
            })

working_df = (
    pd.DataFrame(all_rows)
    .drop_duplicates(subset=["datetime", "alert_type", "city"])
    .assign(
        date=lambda d: d["datetime"].dt.date,
        time=lambda d: d["datetime"].dt.strftime("%H:%M:%S"),
    )
    [["date", "time", "datetime", "alert_type", "city", "area"]]
    .sort_values(["city", "datetime"])
    .reset_index(drop=True)
)

print(f"Working df: {len(working_df):,} rows | {working_df['city'].nunique():,} cities")
# working_df_full: full working df with datetime column, saved before event construction
working_df.to_csv(outdir / "working_df_full.csv", index=False)


# ── 6. EVENT CONSTRUCTION ─────────────────────────────────────────────────────
# Walk each city's timeline in chronological order.
# An event is a contiguous run of missile/uav rows. Consecutive missile rows
# with no pre_warning or ended between them belong to the same event.
# The event's pre_warning = the row immediately before the run (if pre_warning).
# The event's ended       = the row immediately after the run (if ended).
# Orphan pre_warnings (no missile follows) and orphan endeds are silently skipped.

ATTACK = {"missiles", "uav"}

def build_events(city_df: pd.DataFrame, city: str, area: str) -> list:
    rows = city_df.to_dict("records")
    n    = len(rows)
    events = []
    i = 0
    while i < n:
        r = rows[i]
        if r["alert_type"] not in ATTACK:
            i += 1
            continue
        # Optional pre_warning immediately before this run
        pre = rows[i - 1] if i > 0 and rows[i - 1]["alert_type"] == "pre_warning" else None
        # Consume all contiguous missile/uav rows
        missiles = []
        while i < n and rows[i]["alert_type"] in ATTACK:
            missiles.append(rows[i])
            i += 1
        # Optional ended immediately after the run
        ended = rows[i] if i < n and rows[i]["alert_type"] == "ended" else None
        attack_types = sorted({m["alert_type"] for m in missiles})
        events.append({
            "city":           city,
            "area":           area,
            "date":           missiles[0]["datetime"].date(),
            "alert_type":     " | ".join(attack_types),
            "pre_time":       pre["datetime"]       if pre   else pd.NaT,
            "first_missile":  missiles[0]["datetime"],
            "last_missile":   missiles[-1]["datetime"],
            "n_missiles":     len(missiles),
            "ended_time":     ended["datetime"]     if ended else pd.NaT,
        })
    return events


all_events = []
for (city, area), grp in working_df.groupby(["city", "area"], sort=False):
    all_events.extend(build_events(grp.sort_values("datetime"), city, area))

events_df = pd.DataFrame(all_events)

# ── Derived columns ───────────────────────────────────────────────────────────
def minutes(a, b):
    delta = b - a
    secs  = delta.dt.total_seconds()
    return secs / 60

events_df["warn_gap_min"]  = minutes(events_df["pre_time"],     events_df["first_missile"])
events_df["end_gap_min"]   = minutes(events_df["last_missile"],  events_df["ended_time"])
events_df["total_dur_min"] = minutes(events_df["pre_time"],      events_df["ended_time"])
# total_dur falls back to missile-span when pre/ended are missing
events_df["missile_dur_min"] = minutes(events_df["first_missile"], events_df["last_missile"])

print(f"\nEvents built: {len(events_df):,} total")
print(f"  with pre_warning:  {events_df['warn_gap_min'].notna().sum():,}")
print(f"  with ended:        {events_df['end_gap_min'].notna().sum():,}")
print(f"  full triplets:     {(events_df['warn_gap_min'].notna() & events_df['end_gap_min'].notna()).sum():,}")
print(f"  multi-missile:     {(events_df['n_missiles'] > 1).sum():,}")


# ── 7. DERIVED FLAT RECORDS (backward-compatible) ─────────────────────────────
# These mirror the v3 output format so existing scripts keep working.

# warning_records: one row per event that has a pre_warning
warn_mask = events_df["warn_gap_min"].notna()
warning_records = (
    events_df[warn_mask]
    .rename(columns={"warn_gap_min": "gap_min", "alert_type": "missile_type"})
    [["date", "pre_time", "first_missile", "gap_min", "city", "area", "missile_type"]]
    .rename(columns={"pre_time": "warning_time", "first_missile": "missile_time"})
    .reset_index(drop=True)
)

# ended_records: one row per event that has an ended
end_mask = events_df["end_gap_min"].notna()
ended_records = (
    events_df[end_mask]
    .rename(columns={"end_gap_min": "gap_min", "alert_type": "missile_type"})
    [["date", "last_missile", "ended_time", "gap_min", "city", "area", "missile_type"]]
    .rename(columns={"last_missile": "missile_time"})
    .reset_index(drop=True)
)

# Apply thresholds
warning_valid = warning_records[
    warning_records["gap_min"].between(args.min_pre,  args.max_pre)
].copy()
ended_valid = ended_records[
    ended_records["gap_min"].between(args.min_post, args.max_post)
].copy()

print(f"\nWarning records (raw):   {len(warning_records):,}")
print(f"Warning records (valid): {len(warning_valid):,}  "
      f"(gap {args.min_pre}–{args.max_pre} min)")
print(f"\nEnded records (raw):     {len(ended_records):,}")
print(f"Ended records (valid):   {len(ended_valid):,}  "
      f"(gap {args.min_post}–{args.max_post} min)")

# Full-triplet events with thresholds applied
events_valid = events_df[
    events_df["warn_gap_min"].between(args.min_pre,  args.max_pre)  &
    events_df["end_gap_min"].between(args.min_post, args.max_post)
].copy()
print(f"\nFull triplet events (valid thresholds): {len(events_valid):,}")


# ── 8. SUMMARY STATISTICS ─────────────────────────────────────────────────────
def area_stats(label, df_in, gap_col):
    df = df_in[[gap_col, "area", "city"]].dropna()
    if df.empty:
        print(f"\n{label}: no records")
        return

    by_city = df.groupby(["area", "city"])[gap_col].median()
    stats = (
        df.groupby("area")[gap_col]
        .agg(n="count", median="median", mean="mean",
             q25=lambda x: x.quantile(0.25),
             q75=lambda x: x.quantile(0.75),
             min="min", max="max")
        .join(by_city.groupby("area").count().rename("cities"))
        .assign(evt_per_city=lambda d: (d["n"] / d["cities"]).round(2))
        .sort_values("median")
        .reset_index()
    )
    stats["range"] = stats["min"].round(1).astype(str) + "–" + stats["max"].round(1).astype(str)

    print(f"\n{label}")
    w = 97
    print(f"{'Area':<22} {'n':>6} {'cities':>7} {'evt/city':>9} {'median':>8} {'mean':>8} {'Q25':>7} {'Q75':>7} {'range':>12}")
    print("─" * w)
    for _, r in stats.iterrows():
        print(f"{r['area']:<22} {r['n']:>6.0f} {r['cities']:>7.0f} {r['evt_per_city']:>9.2f} "
              f"{r['median']:>8.1f} {r['mean']:>8.1f} {r['q25']:>7.1f} {r['q75']:>7.1f} {r['range']:>12}")

    medians = stats["median"]
    print(f"\n  Mean of area medians:   {medians.mean():.1f} min")
    print(f"  Median of area medians: {medians.median():.1f} min")
    print(f"  Overall range:          {df[gap_col].min():.1f} – {df[gap_col].max():.1f} min")


print("\n=== SUMMARY STATISTICS ===")
area_stats("Warning → Missiles (gap in minutes)",    warning_valid, "gap_min")
area_stats("Missiles → Ended   (gap in minutes)",    ended_valid,   "gap_min")
area_stats("Warning → Ended    (total duration min)", events_valid,  "total_dur_min")


# ── 9. EXPORT ─────────────────────────────────────────────────────────────────
# working_df_full.csv already written above (with datetime)
working_df.drop(columns=["datetime"]).to_csv(
                           outdir / "working_df.csv",             index=False)
events_df.to_csv(          outdir / "events_raw.csv",             index=False)
events_valid.to_csv(       outdir / "events_valid.csv",           index=False)
warning_records.to_csv(    outdir / "warning_records_raw.csv",    index=False)
warning_valid.to_csv(      outdir / "warning_records_valid.csv",  index=False)
ended_records.to_csv(      outdir / "ended_records_raw.csv",      index=False)
ended_valid.to_csv(        outdir / "ended_records_valid.csv",    index=False)

print(f"\nOutputs written to: {outdir.resolve()}")
print("  working_df.csv              — one row per (datetime, alert_type, city)")
print("  events_raw.csv              — one row per event per city (all events)")
print("  events_valid.csv            — full triplets within threshold windows")
print("  warning_records_raw.csv     — one row per event with a pre_warning (pre-filter)")
print("  warning_records_valid.csv   — filtered by --min-pre / --max-pre")
print("  ended_records_raw.csv       — one row per event with an ended (pre-filter)")
print("  ended_records_valid.csv     — filtered by --min-post / --max-post")