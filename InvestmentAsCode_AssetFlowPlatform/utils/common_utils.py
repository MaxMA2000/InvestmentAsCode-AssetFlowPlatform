from datetime import date

def add_date_to_data(data):
    today = date.today().strftime("%Y-%m-%d")
    for item in data:
        item['date'] = today
    return data
