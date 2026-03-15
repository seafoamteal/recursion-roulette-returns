import requests
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor

months = [
    (year, month)
    for year in range(2020, 2026)
    for month in range(1, 13)
    if not (year == 2025 and month == 12)
]


def fetch_data(period):
    year, month = period
    url = f"https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{year}_{month}.zip"
    res = requests.get(url, stream=True)
    res.raise_for_status()

    total = int(res.headers.get("content-length", 0))
    with open(f"{year}_{month}.zip", "wb+") as f:
        with tqdm(
            total=total, unit="B", unit_scale=True, desc=f"{year}/{month}", leave=False
        ) as pbar:
            for chunk in res.iter_content(chunk_size=8192):
                f.write(chunk)
                pbar.update(len(chunk))


with ThreadPoolExecutor(max_workers=8) as executor:
    list(tqdm(executor.map(fetch_data, months), total=len(months)))
