import asyncio
import lxml
import random
import re
import csv

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from pathlib import Path



async def fetch_html(page, app_id: int) -> str | None:
    url = f"https://steamdb.info/app/{app_id}/"
    
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_selector("td:has-text('App ID')", timeout=60000)
        html = await page.content()
        
        return html
        
    except Exception as e:
        print(f"app_id={app_id}: {e}")
        
        return None



class SteamDBBlockParser:
    def parse(self, soup: BeautifulSoup) -> dict:
        raise NotImplementedError



class StoreInfoParser(SteamDBBlockParser):
    def parse(self, soup: BeautifulSoup) -> dict:
        table = soup.select_one("div.span8 table")
        if not table:
            return {}

        data = {
            "app_id": None,
            "app_type": None,
            "developer": [],
            "publisher": [],
            "supported_systems": [],
            "release_date": None,
        }

        for row in table.select("tr"):
            tds = row.select("td")
            if len(tds) != 2:
                continue

            label = tds[0].get_text(strip=True)
            value = tds[1]

            if label == "App ID":
                try:
                    data["app_id"] = int(value.get_text(strip=True))

                except ValueError:
                    pass

            elif label == "App Type":
                data["app_type"] = value.get_text(strip=True)

            elif label == "Developer":
                data["developer"] = [a.get_text(strip=True) for a in value.select("a")]

            elif label == "Publisher":
                data["publisher"] = [a.get_text(strip=True) for a in value.select("a")]

            elif label == "Supported Systems":
                systems = []
                
                if value.select_one(".octicon-windows"):
                    systems.append("Windows")
                    
                if value.select_one(".octicon-linux"):
                    systems.append("Linux")
                    
                if value.select_one(".octicon-apple"):
                    systems.append("macOS")
                    
                data["supported_systems"] = systems

            elif label == "Release Date":
                text = value.get_text(" ", strip=True)
                match = re.search(r"\d{1,2} \w+ \d{4} – \d{2}:\d{2}:\d{2} UTC", text,)

                if match:
                    data["release_date"] = match.group(0)

        return data



class RatingParser(SteamDBBlockParser):
    def parse(self, soup: BeautifulSoup) -> dict:
        data = {
            "rating_percent": None,
            "reviews_count": None,
        }

        block = soup.select_one('a[itemprop="aggregateRating"]')
        if not block:
            return data

        rating = block.select_one('meta[itemprop="ratingValue"]')
        reviews = block.select_one('meta[itemprop="reviewCount"]')

        if rating:
            data["rating_percent"] = float(rating["content"])

        if reviews:
            data["reviews_count"] = int(reviews["content"])

        return data



class TagsParser(SteamDBBlockParser):
    def parse(self, soup: BeautifulSoup) -> dict:
        # Приоритет — полный список тегов
        tags_block = soup.select_one("div.store-tags")

        # Иначе - краткий список в шапке
        if not tags_block:
            tags_block = soup.select_one("div.header-app-tags")

        if not tags_block:
            return {"tags": []}

        tags = []

        for a in tags_block.select("a[href^='/tag/']"):
            text = a.get_text(strip=True)

            # Убираем emoji
            match = re.match(r"([\W_]+)?\s*(.+)", text)
            name = match.group(2).strip() if match else text

            if name:
                tags.append(name)

        if not tags:
            return {"tags": tags}

        return {"tags": tags}



class NamedCategoriesParser(SteamDBBlockParser):
    def __init__(self, title: str, result_key: str):
        self.title = title
        self.result_key = result_key

    def parse(self, soup: BeautifulSoup) -> dict:
        header = soup.find(lambda tag: tag.name in ("h2", "h3") and self.title in tag.get_text())
        if not header:
            return {}

        block = header.find_next_sibling("div", class_="store-categories")
        if not block:
            return {}

        items = [span.get_text(strip=True) for span in block.select("a.btn span")]
        
        return {self.result_key: items} if items else {}



class PricesParser(SteamDBBlockParser):
    def __init__(self, currencies=None):
        self.currencies = currencies or ["CIS - U.S. Dollar", "U.S. Dollar", "Euro"]

    def parse(self, soup: BeautifulSoup) -> dict:
        prices_data = {}
        
        table = soup.select_one("table.table-prices")
        if not table:
            return prices_data

        for row in table.select("tbody tr"):
            currency_td = row.select_one("td.price-line")
            if not currency_td:
                continue

            currency_name = currency_td.get_text(strip=True)
            if " " in currency_name and currency_name.split()[0].endswith("-"):
                currency_name = " ".join(currency_name.split()[1:])

            if currency_name not in self.currencies:
                continue

            # Current Price
            current_price_td = row.select_one("td:nth-of-type(2)")
            current_price = current_price_td.get_text(strip=True) if current_price_td else None

            # Lowest Recorded Price
            lowest_price_td = row.select("td")[-1] if row.select("td") else None
            lowest_price = lowest_price_td.get_text(strip=True) if lowest_price_td else None

            prices_data[currency_name] = {
                "current_price": current_price,
                "lowest_recorded_price": lowest_price,
            }

        return {"prices": prices_data}



class SteamDBPageParser:
    def __init__(self):
        self.parsers = [
            StoreInfoParser(),
            RatingParser(),
            TagsParser(),
            NamedCategoriesParser("Categories", "categories"),
            NamedCategoriesParser("Hardware", "hardware_categories"),
            NamedCategoriesParser("Accessibility", "accessibility_categories"),
            PricesParser(),
        ]

    def parse(self, html: str) -> dict:
        soup = BeautifulSoup(html, "lxml")

        result = {}
        for parser in self.parsers:
            result.update(parser.parse(soup))

        return result



def load_app_ids(csv_path: str, start_from: int | None = None) -> list[int]:
    app_ids = []

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            try:
                app_id = int(row["app_id"])
                app_ids.append(app_id)

            except (KeyError, ValueError):
                continue

    if start_from is None:
        return app_ids

    if start_from not in app_ids:
        raise ValueError(f"start_from app_id={start_from} не найден в файле!")

    index = app_ids.index(start_from)

    return app_ids[index:]



def join_list(value):
    return " | ".join(value) if isinstance(value, list) else ""



def flatten(data: dict) -> dict:
    prices = data.get("prices", {})

    return {
        # --- Store info ---
        "app_id": data.get("app_id"),
        "app_type": data.get("app_type"),
        "developer": join_list(data.get("developer")),
        "publisher": join_list(data.get("publisher")),
        "supported_systems": join_list(data.get("supported_systems")),
        "release_date": data.get("release_date"),

        # --- Rating ---
        "rating_percent": data.get("rating_percent"),
        "reviews_count": data.get("reviews_count"),

        # --- Tags & categories ---
        "tags": join_list(data.get("tags")),
        "categories": join_list(data.get("categories")),
        "hardware_categories": join_list(data.get("hardware_categories")),
        "accessibility_categories": join_list(data.get("accessibility_categories")),

        # --- Prices ---
        "price_usd_current": prices.get("U.S. Dollar", {}).get("current_price"),
        "price_usd_lowest": prices.get("U.S. Dollar", {}).get("lowest_recorded_price"),
        
        "price_eur_current": prices.get("Euro", {}).get("current_price"),
        "price_eur_lowest": prices.get("Euro", {}).get("lowest_recorded_price"),
        
        "price_cis_current": prices.get("CIS - U.S. Dollar", {}).get("current_price"),
        "price_cis_lowest": prices.get("CIS - U.S. Dollar", {}).get("lowest_recorded_price"),
    }



async def main():
    app_ids = load_app_ids("SteamDB/data/charts.csv", 323470)
    parser = SteamDBPageParser()

    output_path = Path("SteamDB/data/store_info.csv")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    file_exists = output_path.exists()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ",
            locale="en-US",
            extra_http_headers={
                "Accept-Language": "en-US,en;q=0.9",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
            }
        )
    
        await context.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', {
            get: () => undefined
        });
        """)
    
        page = await context.new_page()
    
        await page.goto("https://steamdb.info/")
        await page.wait_for_timeout(5000)

        # Короткая задержка между играми (ms)
        SHORT_DELAY_RANGE = (5000, 10000)

        # Пауза каждые BLOCK_SIZE игр
        BLOCK_SIZE = 50
        
        # Длительность паузы
        LONG_PAUSE = 10 * 60

        with open(output_path, "a", newline="", encoding="utf-8") as f:
            writer = None
        
            for i, app_id in enumerate(app_ids, start=1):
                print(f"Парсинг: ID {app_id}")
        
                html = await fetch_html(page, app_id)
                if not html:
                    continue
        
                data = parser.parse(html)
                row = flatten(data)
        
                if not row:
                    continue
        
                if writer is None:
                    writer = csv.DictWriter(f, fieldnames=row.keys())
                    if not file_exists:
                        writer.writeheader()
                        file_exists = True
        
                writer.writerow(row)
                f.flush()
        
                await page.wait_for_timeout(random.randint(*SHORT_DELAY_RANGE))
        
                if i % BLOCK_SIZE == 0:
                    print(f"Долгая пауза в {LONG_PAUSE / 60} минут после {i} игр...")
                    await asyncio.sleep(LONG_PAUSE)
            
        await browser.close()

    print("Парсинг завершен!")



if __name__ == "__main__":
    asyncio.run(main())