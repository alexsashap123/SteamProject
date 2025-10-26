API_key = '18b2f98f889d42b8b08cc02fafa8806b'
import concurrent.futures
import requests
import time
import json
from datetime import datetime
import logging
import os
import sys
import pandas as pd

class FastAPIClient:
    def __init__(self, api_key, verbose=False, quiet=False):
        self.api_key = api_key
        self.base_url = "https://api.rawg.io/api/games"
        self._setup_logging(verbose, quiet)
        self.logger = logging.getLogger(__name__)

    def _setup_logging(self, verbose, quiet):
        if verbose:
            level = logging.DEBUG
        elif quiet:
            level = logging.ERROR
        else:
            level = logging.INFO

        # Очищаем предыдущие настройки
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)

        logging.basicConfig(
            level=level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            filename="api_client.log",
            filemode="w",
            encoding='utf-8'
        )

    def get_years_parallel(self, start_year=2000, end_year=2019):
        years = list(range(start_year, end_year + 1))
        all_games = []

        self.logger.info(f"Начинаем загрузку данных за {start_year}-{end_year} годы...")
        self.logger.info(f"Всего лет для загрузки: {len(years)}")

        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            future_to_year = {
                executor.submit(self._get_games_for_year, year): year
                for year in years
            }

            completed = 0
            total = len(years)

            for future in concurrent.futures.as_completed(future_to_year):
                year = future_to_year[future]
                try:
                    year_games = future.result()
                    all_games.extend(year_games)
                    completed += 1
                    self.logger.info(f"Успешно для {year} год: {len(year_games)} игр | Прогресс: {completed}/{total}")
                except Exception as e:
                    completed += 1
                    self.logger.error(f"Ошибка для {year} года: {e} | Прогресс: {completed}/{total}")

        return all_games

    def _get_games_for_year(self, year):
        games = []
        page = 1
        max_pages = 25

        while page <= max_pages:
            params = {
                'key': self.api_key,
                'dates': f"{year}-01-01,{year}-12-31",
                'page': page,
                'page_size': 40
            }

            try:
                response = requests.get(self.base_url, params=params, timeout=30)

                if response.status_code == 502:
                    self.logger.warning(f"Ошибка 502 для {year} года, страница {page}, ждем")
                    time.sleep(3)
                    continue

                response.raise_for_status()
                data = response.json()

                page_games = data.get('results', [])
                if not page_games:
                    break

                games.extend(page_games)

                if not data.get('next'):
                    break

                page += 1
                time.sleep(0.8)

            except requests.exceptions.RequestException as e:
                self.logger.error(f"Ошибка для {year} года, страница {page}: {e}")
                break
            except Exception as e:
                self.logger.error(f"Ошибка для {year} года: {e}")
                break

        return games

def run_in_jupyter(start_year=2017, end_year=2019, verbose=True, quiet=False):
    api_key = "18b2f98f889d42b8b08cc02fafa8806b"
    client = FastAPIClient(api_key, verbose=verbose, quiet=quiet)

    client.logger.info("Запуск загрузки данных")
    start_time = time.time()

    # Вызов основного метода
    all_games = client.get_years_parallel(start_year, end_year)

    end_time = time.time()
    total_time = end_time - start_time

    client.logger.info(f"Загрузка завершена!")
    client.logger.info(f"Собрано {len(all_games)} игр")
    client.logger.info(f"Затрачено времени: {total_time:.1f} секунд")
    client.logger.info(f"Скорость: {len(all_games)/total_time:.1f} игр/секунду")

    # Сохраняем результаты
    # timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f'games_parallel_{start_year}_{end_year}.json'

    # with open(filename, 'w', encoding='utf-8') as f:
    #     json.dump(all_games, f, ensure_ascii=False, indent=2)

    df = pd.DataFrame(all_games)
    df.to_csv(filename, index=False, encoding='utf-8-sig')

    client.logger.info(f"Данные сохранены в файл: {filename}")

    return all_games
