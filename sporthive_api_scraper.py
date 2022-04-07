import requests
from typing import Union, List
import datetime as dt
import pandas as pd


class SporthiveAPIScraper:
    """
    Class to scrape data from the Sporthive API. Example usage:

    ```
    from sporthive_api_scraper import SporthiveAPIScraper

    event = 6855879561074155264
    race = 480016

    api_scraper = SporthiveAPIScraper(
        event=event,
        race=race,
        read_splits=False
    )
    data = api_scraper.read_data_as_json()
    ```

    """

    def __init__(
        self,
        event: Union[int, str],
        race: Union[int, str],
        read_splits: bool = True,
        verbose: bool = True,
    ):
        """
        Args:
            event: event id. Can be obtained from the sporthive URL.
            race: race id. Can also be obtained from the sporthive URL.
            read_splits: Also read the split times from the API, if available/
            verbose: Print the numbe rof participants loaded after reading each batch in the API.
        """
        self.api_url = f"https://eventresults-api.sporthive.com/api/events/{event}/races/{race}/classifications/"
        self.read_splits = read_splits
        self.verbose = verbose

    def scrape(self) -> List[dict]:
        data = self._read_data_from_api()
        return data

    def scrape_data_as_dataframe(self) -> pd.DataFrame:
        json_data = self._read_data_from_api()
        df = self._convert_to_dataframe(json_data)
        return df

    def _read_data_from_api(self, batch_size: int = 50) -> List[dict]:
        results = []
        number_of_athletes_loaded = 0
        all_results_loaded = False
        while not all_results_loaded:
            if self.verbose:
                print(f"loaded: {number_of_athletes_loaded}")
            results_json = self._get_batch_from_api(
                batch_size, number_of_athletes_loaded
            )
            number_of_new_athletes_loaded = len(results_json["fullClassifications"])
            number_of_athletes_loaded += number_of_new_athletes_loaded

            if number_of_new_athletes_loaded < batch_size:
                all_results_loaded = True

            if number_of_new_athletes_loaded > 0:
                for athlete in results_json["fullClassifications"]:
                    results.append(self._parse_athlete_results(athlete))

        return results

    def _get_batch_from_api(self, batch_size: int, offset: int) -> List[dict]:
        return requests.get(
            f"{self.api_url}/search?count={batch_size}&offset={offset}"
        ).json()

    def _parse_athlete_results(self, athlete: dict) -> dict:
        data = {
            "name": athlete["athlete"]["name"],
            "bib": athlete["classification"]["bib"],
            "gender": athlete["classification"]["gender"],
            "category": athlete["classification"]["category"],
            "rank": athlete["classification"]["rank"],
            "genderRank": athlete["classification"]["genderRank"],
            "categoryRank": athlete["classification"]["categoryRank"],
            "countryCode": athlete["classification"]["countryCode"],
            "chipTime": self._timestamp_to_seconds(
                athlete["classification"]["chipTime"]
            ),
        }
        if self.read_splits:
            data["splits"] = self._parse_splits(athlete["classification"]["splits"])
        return data

    def _parse_splits(self, splits: List[dict]) -> List[dict]:
        splits_parsed = [
            {
                "name": split["name"],
                "time": self._timestamp_to_seconds(split["cumulativeTime"]),
            }
            for split in splits
        ]
        return splits_parsed

    @staticmethod
    def _timestamp_to_seconds(timestamp: str) -> int:
        try:
            return (
                dt.datetime.strptime(timestamp, "%H:%M:%S") - dt.datetime(1900, 1, 1)
            ).seconds
        except:
            return None

    def _convert_to_dataframe(self, data: List[dict]) -> pd.DataFrame:
        if self.read_splits:
            data = [
                {
                    **{
                        key: value
                        for key, value in athlete.items()
                        if key not in ["splits"]
                    },
                    **{
                        f"split_{split['name']}": split["time"]
                        for split in athlete["splits"]
                    },
                }
                for athlete in data
            ]
            return pd.DataFrame(data)
        else:
            return pd.DataFrame(data)
