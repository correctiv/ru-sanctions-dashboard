# flake8: noqa


import json
import logging
import os
import sys
from datetime import datetime

import pandas as pd
from slugify import slugify

# setup logger
root = logging.getLogger()
root.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
log_format = "%(levelname)s - %(message)s"
formatter = logging.Formatter(log_format)
handler.setFormatter(formatter)
root.addHandler(handler)
log = logging.getLogger(__name__)


# some defaults
NOW = datetime.now()
DATABASE_URI = os.getenv("FTM_STORE_URI")

BASE_URL = "https://correctiv.github.io/ru-sanctions-dashboard/public"
COR_BASE_URL = "https://correctiv.org/wp-content/uploads/2022/02"

ICONS = {
    "Person": f"![Person]({BASE_URL}/img/person.svg)",
    "Company": f"![Company]({BASE_URL}/img/company.svg)",
    "Airplane": f"![Airplane]({BASE_URL}/img/plane.svg)",
    "Vessel": f"![Vessel]({BASE_URL}/img/ship.svg)",
    "Other": f"![Other]({BASE_URL}/img/institution.svg)",
}

ICONS_RED = {
    "Person": f"![Person]({COR_BASE_URL}/user-red.svg)",
    "Company": f"![Company]({COR_BASE_URL}/building-red.svg)",
    "Airplane": f"![Airplane]({COR_BASE_URL}/plane-red.svg)",
    "Vessel": f"![Vessel]({COR_BASE_URL}/anchor-red.svg)",
    "Other": f"![Other]({COR_BASE_URL}/landmark-red.svg)",
}

AUTHORITIES = {
    "European External Action Service": "eu",
    "United Nations Security Council (UN SC)": "uno",
    "UN; Office of Financial Sanctions Implementation": "uno",
    "World Bank": "uno",
}


# helpers


def load_data():
    return pd.read_sql(
        """
        select
            s.id as sanction_id,
            e.id as entity_id,
            s.entity ->> 'caption' as caption,
            s.entity -> 'properties' -> 'program' as program,
            s.entity -> 'properties' -> 'reason' as reason,
            s.entity -> 'properties' -> 'country' as origin,
            s.entity -> 'properties' -> 'authority' as authority,
            s.entity -> 'properties' -> 'sourceUrl' as sourceUrl,
            s.entity -> 'properties' -> 'startDate' as startDate,
            s.entity -> 'properties' -> 'endDate' as endDate,
            s.entity -> 'properties' -> 'date' as date,
            e.entity ->> 'schema' as schema,
            e.entity -> 'properties' -> 'name' as name,
            e.entity -> 'properties' -> 'country' as countries
        from ftm_opensanctions s
        join ftm_opensanctions e on
            s.entity @> '{"schema": "Sanction"}' and
            s.entity -> 'properties' -> 'entity' ->> 0 = e.id
        """,
        DATABASE_URI,
    )


def clean_data(df):
    def get_start(row):
        if row["startdate"] is not None:
            return min(row["startdate"])
        if row["date"] is not None:
            return min(row["date"])

    def get_end(row):
        if row["enddate"] is not None:
            return max(row["enddate"])

    def get_active(row):
        if row["start"] and row["start"] < NOW:
            if not pd.isna(row["end"]):
                return row["end"] > NOW
            return True
        return False

    df["start"] = pd.to_datetime(df.apply(get_start, axis=1), errors="coerce")
    df["end"] = pd.to_datetime(df.apply(get_end, axis=1), errors="coerce")
    df["active"] = df.apply(get_active, axis=1)
    df["origin"] = df["origin"].map(lambda x: x[0] if x else x)

    return df.sort_values("start", ascending=False)


def clean_table(df):
    def unpack(value):
        if value is None:
            return
        return "; ".join(sorted(value, key=lambda x: len(x)))

    def clean_date(value):
        if value is None:
            return
        return value.date()

    def clean_authority(row):
        value = row.get("authority")
        if value is None:
            return
        origin = row.get("origin")
        if origin is not None:
            return f":{origin}: {origin.upper()} | {value}"
        origin = AUTHORITIES.get(value)
        if origin in ("eu", "uno"):
            return f"![{origin}]({BASE_URL}/img/{origin}.svg) {origin[:2].upper()} | {value}"
        return value

    def markdown_url(value):
        if value is None:
            return
        return f"[URL]({value})"

    df_table = df.copy()
    df_table["program"] = df_table["program"].map(unpack)
    df_table["authority"] = df_table["authority"].map(unpack)
    df_table["name"] = df_table["name"].map(unpack)
    # brute force transliteration
    df_table["name"] = df_table["name"].map(lambda x: slugify(x, separator=" "))
    df_table["countries"] = df_table["countries"].map(unpack)
    df_table["reason"] = df_table["reason"].map(unpack)
    df_table["sourceurl"] = (
        df_table["sourceurl"]
        .map(lambda x: x[0] if x is not None else x)
        .map(markdown_url)  # noqa
    )
    df_table["start"] = df_table["start"].map(clean_date)
    df_table["end"] = df_table["end"].map(clean_date)
    df_table["icon"] = df_table["schema"].map(lambda x: ICONS.get(x, ICONS["Other"]))
    df_table["authority"] = df_table.apply(clean_authority, axis=1)

    df_table = df_table[["name", "icon", "start", "end", "authority", "sourceurl"]]

    df_table = df_table.drop_duplicates()
    return df_table


if __name__ == "__main__":

    # load data
    df = load_data()
    df = clean_data(df)
    log.info(f"Total entries: `{len(df)}`")

    # filter for active sanctions
    df = df[df["active"]]
    log.info(f"Active entries: `{len(df)}`")

    # filter for russia and >= 2014
    df = df[df["countries"].fillna("").map(lambda x: "ru" in x)]
    df = df[df["start"] > "2014"]
    log.info(f"Entries against russian targets since 2014: `{len(df)}`")

    df.index = pd.DatetimeIndex(df["start"])

    # generate table csv
    df_table = clean_table(df)
    df_table.fillna("").to_csv(
        "./src/data/sanctions_2014-2022.csv", index=False
    )  # noqa

    # generate timeline csv
    pd.DataFrame(df.resample("1M")["sanction_id"].count()).to_csv(
        "./src/data/sanctions_timeline_2014-2022.csv"
    )

    # generate recent aggregations
    df_recent = df[df["start"] > "2022-02-21"]
    log.info(f"Entries since 2022-02-22: `{len(df_recent)}`")

    # per schema - table with 1st row as icon header
    df_recent_schema = df_recent.copy()
    df_recent_schema["schema"] = df_recent_schema["schema"].map(
        lambda x: x if x in ICONS_RED else "Other"
    )
    df_recent_schema = (
        df_recent_schema.groupby("schema")
        .resample("1D")["sanction_id"]
        .count()
        .reset_index()
    )
    df_recent_schema["sanction_id"] = df_recent_schema["sanction_id"].map(
        lambda x: "" if pd.isna(x) or x < 1 else str(int(x))
    )
    df_recent_schema = df_recent_schema.pivot("start", "schema", "sanction_id")
    df_recent_schema = df_recent_schema.sort_values("start")
    df_recent_schema.index = df_recent_schema.index.map(lambda x: x.date())
    df_recent_schema.loc[""] = df_recent_schema.columns.map(
        lambda x: ICONS_RED.get(x, ICONS_RED["Other"])
    )
    df_recent_schema.iloc[::-1].fillna("").to_csv(
        "./src/data/recent_schema_aggregation_table.csv"
    )

    # per origin - table with 1st row as flag icon header
    def get_icon(origin):
        if origin == "uno":
            return f"![{origin}]({BASE_URL}/img/{origin}.svg)"
        if origin == "eu":
            return f"![{origin}]({COR_BASE_URL}/eu-flag-crop.svg)"
        return f":{origin}:"

    df_recent_origin = (
        df_recent.groupby("origin").resample("1D")["sanction_id"].count().reset_index()
    )
    df_recent_origin["sanction_id"] = df_recent_origin["sanction_id"].map(
        lambda x: "" if pd.isna(x) or x < 1 else str(int(x))
    )
    df_recent_origin = df_recent_origin.pivot("start", "origin", "sanction_id")
    df_recent_origin.index = df_recent_origin.index.map(lambda x: x.date()).map(str)
    df_recent_origin.loc[""] = df_recent_origin.columns.map(get_icon)
    df_recent_origin.iloc[::-1].fillna("").to_csv(
        "./src/data/recent_origin_aggregation_table.csv"
    )

    # meta data to inject into page via js
    df["old"] = (df["start"] < "2022-02-22").map(int)
    df["recent"] = (df["start"] > "2022-02-21").map(int)
    df["all"] = 1

    old_sanctions = df["old"].sum()
    old_entities = len(df[df["old"].map(bool)]["entity_id"].unique())
    new_sanctions = df["recent"].sum()
    new_entities = len(df[df["recent"].map(bool)]["entity_id"].unique())
    all_sanctions = len(df)
    all_entities = len(df["entity_id"].unique())

    df_meta = pd.DataFrame(
        (
            (old_sanctions, new_sanctions, all_sanctions),
            (old_entities, new_entities, all_entities),
        ),
        columns=("old", "recent", "all"),
        index=("sanctions", "entities"),
    )

    df_meta = pd.concat(
        (
            df_meta,
            df.groupby("origin")[["old", "recent", "all"]].sum(),
            df.groupby("schema")[["old", "recent", "all"]].sum(),
        )
    )

    meta = df_meta.to_dict()
    meta["last_updated"] = NOW.isoformat()

    with open("./src/data/meta.json", "w") as f:
        json.dump(meta, f)
