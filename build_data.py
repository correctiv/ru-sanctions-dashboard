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
    df_table["origin"] = df_table["authority"].map(lambda x: AUTHORITIES[x])
    df_table["authority"] = df_table["authority"].map(clean_authority)

    df_table = df_table[
        ["name", "icon", "start", "end", "origin", "authority", "sourceurl"]
    ]

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

    # filter for russia and >= 2013
    df = df[df["countries"].fillna("").map(lambda x: "ru" in x)]
    df = df[df["start"] > "2013"]
    log.info(f"Entries against russian targets since 2013: `{len(df)}`")

    df.index = pd.DatetimeIndex(df["start"])

    # generate table csv
    df_table = clean_table(df)
    df_table.fillna("").to_csv(
        "./src/data/sanctions_2013-2022.csv", index=False
    )  # noqa

    # generate timeline csv
    pd.DataFrame(df.resample("1M")["sanction_id"].count()).to_csv(
        "./src/data/sanctions_timeline_2013-2022.csv"
    )
