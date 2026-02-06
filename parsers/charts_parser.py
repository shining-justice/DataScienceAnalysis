import sys
import asyncio
import re
import random
import pandas as pd

from playwright.async_api import async_playwright



async def parse_steamdb_charts(page):
    data = []

    await page.goto("https://steamdb.info/charts/", timeout=60000)
    await page.wait_for_selector("table.table-products")

    await page.wait_for_selector("#dt-length-0")
    await page.select_option("#dt-length-0", value="-1")
    await page.wait_for_timeout(3000)

    rows = await page.query_selector_all("table.table-products tbody tr")

    for row in rows:
        cells = await row.query_selector_all("td")

        data.append({
            "app_id": await row.get_attribute("data-appid"),
            "rank": await cells[0].inner_text(),
            "name": await cells[2].inner_text(),
            "current_players": await cells[3].inner_text(),
            "24h_peak": await cells[4].inner_text(),
            "all_time_peak": await cells[5].inner_text(),
        })

    return pd.DataFrame(data)



async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,
            slow_mo=50,
        )

        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ",
        )

        page = await context.new_page()

        df_charts = await parse_steamdb_charts(page)
        df_charts.to_csv("charts.csv", index=False)
        print("Данные сохранены в файл charts.csv!")

        await browser.close()

        return df_charts



if __name__ == "__main__":
    asyncio.run(main())