import os
import random
import re
import time
from io import StringIO
from typing import Any, Dict

import duckdb
import pandas as pd
from bs4 import BeautifulSoup


def random_sleep(min_s=1.0, max_s=3.0):
    delay = random.uniform(min_s, max_s)
    time.sleep(delay)


def get_season_url(soup: BeautifulSoup, db_path: str) -> list[str]:
    hrefs_match_report = [
        ("https://fbref.com" + a["href"]) if a["href"].startswith("/") else a["href"]
        for a in soup.select('table[id^="sched_"] td[data-stat="match_report"] a[href]')
        if a.get_text(strip=True) == "Match Report"
    ]

    scraped_urls: list[str] = []
    if db_path and os.path.exists(db_path):
        try:
            con = duckdb.connect(db_path)
            try:
                result = con.execute(
                    "SELECT DISTINCT url FROM fbref.match_stats"
                ).fetchall()
                scraped_urls = [row[0] for row in result]
            finally:
                con.close()
        except duckdb.Error:
            pass

    scraped_set = set(scraped_urls)
    only_in_A = [x for x in hrefs_match_report if x not in scraped_set]
    return only_in_A


def get_match_infos(soup: BeautifulSoup) -> Dict[str, Any]:
    match_infos = {}
    scores = [score.text.strip() for score in soup.find_all("div", class_="score")]
    if len(scores) == 2:
        match_infos["goals_home"], match_infos["goals_away"] = (
            scores[0],
            scores[1],
        )

    managers = [
        manager.text.split(": ")[1]
        for manager in soup.find_all("div", class_="datapoint")
        if "Manager" in manager.text
    ]
    if len(managers) == 2:
        match_infos["manager_home"], match_infos["manager_away"] = (
            managers[0],
            managers[1],
        )

    captains = [
        captain.a.text.replace("\xa0", " ")
        for captain in soup.find_all("div", class_="datapoint")
        if "Captain" in captain.text
    ]
    if len(captains) == 2:
        match_infos["captain_home"], match_infos["captain_away"] = (
            captains[0],
            captains[1],
        )

    venue_time = soup.find("span", class_="venuetime")
    if venue_time:
        venue_date = venue_time.get("data-venue-date")
        venue_time_value = venue_time.get("data-venue-time")
        match_infos["date"], match_infos["hour"] = venue_date, venue_time_value
    else:
        print("La balise 'venuetime' n'a pas été trouvée.")

    return match_infos


def get_match_tables(soup: BeautifulSoup) -> Dict[str, Any]:

    table_stats = {}

    TABLE_TYPES = [
        "summary",
        "passing",
        "passing_types",
        "defense",
        "keeper",
        "possession",
        "misc",
        "shots",
    ]

    for table_type in TABLE_TYPES:
        if table_type == "shots":
            tables = soup.find_all("table", id="shots_all")
        elif table_type == "keeper":
            tables = soup.find_all("table", id=re.compile(r"^keeper_stats"))
        else:
            tables = soup.find_all("table", id=re.compile(rf"^stats.*{table_type}$"))

        if tables:

            for idx, table in enumerate(tables):
                # Utiliser StringIO pour encapsuler la chaîne HTML
                html_string = StringIO(str(table))
                df = pd.read_html(html_string)[0]

                # Aplatir les colonnes multi-niveaux
                df.columns = [
                    " ".join(col).strip() if isinstance(col, tuple) else col
                    for col in df.columns
                ]

                if idx == 0:
                    df["Team"] = "Home"
                else:
                    df["Team"] = "Away"

                # Convertir le DataFrame en dictionnaire
                table_dict = df.to_dict(orient="records")

                table_stats.setdefault(table_type, []).extend(table_dict)

        else:
            print(f"La table '{table_type}' n'a pas été trouvée.")

    return table_stats
