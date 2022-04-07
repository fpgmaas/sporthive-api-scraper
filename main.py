from sporthive_api_scraper import SporthiveAPIScraper
import json
from dotenv import dotenv_values

config = dotenv_values(".env")

api_scraper = SporthiveAPIScraper(
    event=config["EVENT"], race=config["RACE"], read_splits=False
)
data = api_scraper.scrape()

with open(f'data/{config["EVENT"]}_{config["RACE"]}.json', "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False, indent=4)
