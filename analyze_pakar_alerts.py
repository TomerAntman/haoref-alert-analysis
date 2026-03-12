import argparse
import json
import re
from pathlib import Path

import pandas as pd

# ── CLI ────────────────────────────────────────────────────────────────────────
parser = argparse.ArgumentParser()
parser.add_argument("--input",       default="PikudHaOref_alerts.csv")
parser.add_argument("--mapping",     default="districts_eng_with_hebrew_areas.json")
parser.add_argument("--output-dir",  default=".")
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
    "Confrontation Line": "Galilee",
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
working_df.to_csv(outdir / "working_df.csv", index=False)


# ── 6. NEIGHBOR LOOKUP ────────────────────────────────────────────────────────
# Sort by city + datetime. For every missile/uav row, the immediate previous
# and next rows within the same city are its natural neighbors.
# No clustering, no scoring — just adjacency.

grp = working_df.groupby("city", sort=False)
working_df["prev_type"] = grp["alert_type"].shift(1)
working_df["prev_time"] = grp["datetime"].shift(1)
working_df["next_type"] = grp["alert_type"].shift(-1)
working_df["next_time"] = grp["datetime"].shift(-1)

attack_rows = working_df[working_df["alert_type"].isin(["missiles", "uav"])].copy()


# ── 7. WARNING RECORDS ────────────────────────────────────────────────────────
# A warning record exists when a missile row's immediate predecessor (same city)
# is a pre_warning alert.

warning_df = attack_rows[attack_rows["prev_type"] == "pre_warning"].copy()
warning_df["gap_min"] = (
    warning_df["datetime"] - warning_df["prev_time"]
).dt.total_seconds() / 60

warning_records = (
    warning_df
    .rename(columns={
        "prev_time": "warning_time",
        "datetime":  "missile_time",
        "date":      "date",
        "area":      "area",
        "city":      "city",
        "alert_type": "missile_type",
    })
    [["date", "warning_time", "missile_time", "gap_min", "city", "area", "missile_type"]]
    .reset_index(drop=True)
)

# Apply thresholds
warning_valid = warning_records[
    (warning_records["gap_min"] >= args.min_pre) &
    (warning_records["gap_min"] <= args.max_pre)
].copy()

print(f"\nWarning records (raw):   {len(warning_records):,}")
print(f"Warning records (valid): {len(warning_valid):,}  "
      f"(gap {args.min_pre}–{args.max_pre} min)")


# ── 8. ENDED RECORDS ──────────────────────────────────────────────────────────
# An ended record exists when a missile row's immediate successor (same city)
# is an ended alert.

ended_df = attack_rows[attack_rows["next_type"] == "ended"].copy()
ended_df["gap_min"] = (
    ended_df["next_time"] - ended_df["datetime"]
).dt.total_seconds() / 60

ended_records = (
    ended_df
    .rename(columns={
        "datetime":  "missile_time",
        "next_time": "ended_time",
        "date":      "date",
        "area":      "area",
        "city":      "city",
        "alert_type": "missile_type",
    })
    [["date", "missile_time", "ended_time", "gap_min", "city", "area", "missile_type"]]
    .reset_index(drop=True)
)

# Apply thresholds
ended_valid = ended_records[
    (ended_records["gap_min"] >= args.min_post) &
    (ended_records["gap_min"] <= args.max_post)
].copy()

print(f"\nEnded records (raw):     {len(ended_records):,}")
print(f"Ended records (valid):   {len(ended_valid):,}  "
      f"(gap {args.min_post}–{args.max_post} min)")


# ── 9. SUMMARY STATISTICS ─────────────────────────────────────────────────────
def area_stats(label, df_in, gap_col):
    df = df_in[[gap_col, "area", "city"]].dropna()
    if df.empty:
        print(f"\n{label}: no records")
        return

    stats = (
        df.groupby("area")[gap_col]
        .agg(n="count", median="median", mean="mean",
             q25=lambda x: x.quantile(0.25),
             q75=lambda x: x.quantile(0.75),
             min="min", max="max")
        .sort_values("median")
        .reset_index()
    )
    stats["range"] = stats["min"].round(1).astype(str) + "–" + stats["max"].round(1).astype(str)

    print(f"\n{label}")
    print(f"{'Area':<22} {'n':>6} {'median':>8} {'mean':>8} {'Q25':>7} {'Q75':>7} {'range':>12}")
    print("─" * 74)
    for _, r in stats.iterrows():
        print(f"{r['area']:<22} {r['n']:>6.0f} {r['median']:>8.1f} {r['mean']:>8.1f} "
              f"{r['q25']:>7.1f} {r['q75']:>7.1f} {r['range']:>12}")

    medians = stats["median"]
    print(f"\n  Mean of area medians:   {medians.mean():.1f} min")
    print(f"  Median of area medians: {medians.median():.1f} min")
    print(f"  Overall range:          {df[gap_col].min():.1f} – {df[gap_col].max():.1f} min")


print("\n=== SUMMARY STATISTICS ===")
area_stats("Warning → Missiles (gap in minutes)", warning_valid, "gap_min")
area_stats("Missiles → Ended   (gap in minutes)", ended_valid,   "gap_min")


# ── 10. EXPORT ────────────────────────────────────────────────────────────────
working_df.drop(columns=["prev_type","prev_time","next_type","next_time"]).to_csv(
    outdir / "working_df.csv", index=False)
warning_records.to_csv(outdir / "warning_records_raw.csv",   index=False)
warning_valid.to_csv(  outdir / "warning_records_valid.csv", index=False)
ended_records.to_csv(  outdir / "ended_records_raw.csv",     index=False)
ended_valid.to_csv(    outdir / "ended_records_valid.csv",   index=False)

print(f"\nOutputs written to: {outdir.resolve()}")
print("  working_df.csv              — one row per (datetime, alert_type, city)")
print("  warning_records_raw.csv     — all warning→missile pairs (pre threshold filter)")
print("  warning_records_valid.csv   — filtered by --min-pre / --max-pre")
print("  ended_records_raw.csv       — all missile→ended pairs (pre threshold filter)")
print("  ended_records_valid.csv     — filtered by --min-post / --max-post")