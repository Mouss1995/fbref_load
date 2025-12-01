from typing import Any, Dict, Iterator

import dlt
from bs4 import BeautifulSoup
from curl_cffi import requests

from fbref_utils import get_match_infos, get_match_tables, get_season_url, random_sleep


@dlt.resource(name="match_stats", write_disposition="append")
def get_match_stats(matchs_url: list) -> Iterator[Dict[str, Any]]:
    """Extrait les statistiques des tables d'un match"""

    for match_url in matchs_url:

        match_data = {}

        print(f"Scraping match URL: {match_url}")

        random_sleep(1.5, 4.0)

        response = requests.get(
            match_url,
            impersonate="chrome",
        )

        print(f"Response status code: {response.status_code} for URL: {match_url}")

        soup = BeautifulSoup(response.text, "html.parser")

        match_data.update(get_match_infos(soup))
        match_data.update(get_match_tables(soup))
        match_data.update({"url": match_url})

        yield match_data


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="load_fbref_pipeline",
        destination="duckdb",
        dataset_name="fbref",
        # progress="log",
    )

    LEAGUES = [
        "https://fbref.com/en/comps/12/schedule/La-Liga-Scores-and-Fixtures",
        "https://fbref.com/en/comps/9/schedule/Premier-League-Scores-and-Fixtures",
    ]

    match_urls = []
    for league in LEAGUES:
        random_sleep(1.5, 4.0)
        response = requests.get(
            league,
            impersonate="chrome",
        )
        soup = BeautifulSoup(response.text, "html.parser")
        season_urls = get_season_url(soup, db_path="load_fbref_pipeline.duckdb")
        print(f"League: {league} - New match reports to scrape: {len(season_urls)}\n")
        match_urls.extend(season_urls)

    match_urls = match_urls[:5]  # Limiter pour les tests
    # Exécuter le pipeline
    load_info = pipeline.run(get_match_stats(match_urls))

    print(load_info)
