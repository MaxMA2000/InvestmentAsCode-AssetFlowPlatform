from typing import List, Dict, Any
from datetime import date

def add_date_to_data(data: List[Dict[str, Any]]):
    today = date.today().strftime("%Y-%m-%d")
    for item in data:
        item['date'] = today
    return data

def find_min_max_dates(data: List[Dict[str, Any]], date_key: str):
  dates = [item[date_key] for item in data]

  if dates:
    min_date = min(dates)
    max_date = max(dates)
    return min_date, max_date
  else:
      raise ValueError(f"No '{date_key}' found in the list.")
