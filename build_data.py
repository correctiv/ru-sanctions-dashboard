# flake8: noqa


import json
import logging
import os
import sys
from datetime import datetime

import pandas as pd

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

ICONS = {
    "Person": "![Person](https://banner2.cleanpng.com/20180426/gkw/kisspng-user-computer-icons-download-person-icon-5ae1dbfbb675d7.5784016215247513557474.jpg)",  # noqa
    "Company": "![Company](https://w7.pngwing.com/pngs/38/685/png-transparent-computer-icons-company-youtube-business-corporation-youtube-angle-building-company.png)",  # noqa
    "LegalEntity": "![Company](https://w7.pngwing.com/pngs/38/685/png-transparent-computer-icons-company-youtube-business-corporation-youtube-angle-building-company.png)",  # noqa
}

AUTHORITIES = {
    "Office of Foreign Assets Control (OFAC)": "us",
    "National Security and Defense Council": "ua",
    "Entity List (EL) - Bureau of Industry and Security": "us",
    "European External Action Service": "eu",
    "UK; Office of Financial Sanctions Implementation": "uk",
    "Military End User (MEU) List - Bureau of Industry and Security": "us",
    "Ministry of Finance": "jp",
    "Державна служба фінансового моніторингу України (Держфінмоніторинг)": "ua",  # noqa
    "Nonproliferation Sanctions (ISN) - State Department": "us",
    "United Nations Security Council (UN SC)": "un",
    "UN; Office of Financial Sanctions Implementation": "un",
    "Ministry of Justice and Human Rights": "ar",
    "Minister of Defense - Mr. Avigdor Liberman; National Bureau for Counter Terror Financing": "il",  # noqa
    "World Bank": "xx",
    "Asian Development Bank": "as",
    "African Development Bank Group": "af",
    "WBG cross debarment; Inter-American Development Bank": "sa",
    "The State Security Cabinet (SSC); National Bureau for Counter Terror Financing": "il",  # noqa
}

COUNTRIES = {
    "us": "USA",
    "ua": "Ukraine",
    # "eu": "EU",
    "uk": "UK",
    "jp": "Japan",
    "ar": "Argentinien",
    "il": "Israel",
    "ch": "Schweiz",
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

    return df


def clean_table(df):
    def unpack(value):
        if value is None:
            return
        return "; ".join(sorted(value, key=lambda x: len(x)))

    def clean_date(value):
        if value is None:
            return
        return value.date()

    def clean_authority(value):
        if value is None:
            return
        iso = AUTHORITIES.get(value)
        if iso is not None and iso in COUNTRIES:
            cname = COUNTRIES[iso]
            return f":{iso}: {cname} | {value}"
        return value

    def markdown_url(value):
        if value is None:
            return
        return f"[Quelle]({value})"

    df_table = df.copy()
    df_table["program"] = df_table["program"].map(unpack)
    df_table["authority"] = df_table["authority"].map(unpack)
    df_table["name"] = df_table["name"].map(unpack)
    df_table["countries"] = df_table["countries"].map(unpack)
    df_table["reason"] = df_table["reason"].map(unpack)
    df_table["sourceurl"] = (
        df_table["sourceurl"]
        .map(lambda x: x[0] if x is not None else x)
        .map(markdown_url)  # noqa
    )
    df_table["start"] = df_table["start"].map(clean_date)
    df_table["end"] = df_table["end"].map(clean_date)
    df_table["icon"] = df_table["schema"].map(lambda x: ICONS.get(x))
    df_table["authority"] = df_table["authority"].map(clean_authority)

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

    # per schema
    SCHEMA_DE = {
        "Person": "Person",
        "Company": "Firma",
    }
    SCHEMA_EN = {
        "Person": "Person",
        "Company": "Company",
    }
    df_recent["schema_de"] = df_recent["schema"].map(
        lambda x: SCHEMA_DE.get(x, "Sonstige")
    )
    df_recent["schema_en"] = df_recent["schema"].map(
        lambda x: SCHEMA_EN.get(x, "Other")
    )
    df_recent_schema = (
        df_recent.groupby("schema_de")
        .resample("1D")["sanction_id"]
        .count()
        .reset_index()
    )
    df_recent_schema = df_recent_schema.pivot("start", "schema_de", "sanction_id")
    df_recent_schema.fillna(0).to_csv("./src/data/recent_schema_aggregation.csv")
    df_recent_schema_en = (
        df_recent.groupby("schema_en")
        .resample("1D")["sanction_id"]
        .count()
        .reset_index()
    )
    df_recent_schema_en = df_recent_schema_en.pivot("start", "schema_en", "sanction_id")
    df_recent_schema_en.fillna(0).to_csv("./src/data/recent_schema_aggregation_en.csv")

    # per origin
    df_recent_origin = (
        df_recent.groupby("origin").resample("1D")["sanction_id"].count().reset_index()
    )
    df_recent_origin["origin"] = df_recent_origin["origin"].str.upper()
    df_recent_origin = df_recent_origin.pivot("start", "origin", "sanction_id")
    df_recent_origin.fillna(0).to_csv("./src/data/recent_origin_aggregation.csv")

    # meta data to inject into page via js
    df["old"] = (df["start"] < "2022-02-22").map(int)
    df["new"] = (df["start"] > "2022-02-21").map(int)
    df["all"] = 1

    old_sanctions = df["old"].sum()
    old_entities = len(df[df["old"].map(bool)]["entity_id"].unique())
    new_sanctions = df["new"].sum()
    new_entities = len(df[df["new"].map(bool)]["entity_id"].unique())
    all_sanctions = len(df)
    all_entities = len(df["entity_id"].unique())

    df_meta = pd.DataFrame(
        (
            (old_sanctions, new_sanctions, all_sanctions),
            (old_entities, new_entities, all_entities),
        ),
        columns=("old", "new", "all"),
        index=("sanctions", "entities"),
    )

    df_meta = pd.concat(
        (
            df_meta,
            df.groupby("origin")[["old", "new", "all"]].sum(),
            df.groupby("schema")[["old", "new", "all"]].sum(),
        )
    )

    meta = df_meta.to_dict()
    meta["last_updated"] = NOW.isoformat()

    with open("./src/data/meta.json", "w") as f:
        json.dump(meta, f)
