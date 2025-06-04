
import requests
from datetime import datetime
import json

BASE = "http://127.0.0.1:8029"

def print_response(response):
    print("Status:", response.status_code)
    try:
        print(json.dumps(response.json(), indent=2))
    except:
        print(response.text)
    print("="*40)

def test():
    data = {{
        "location": "Pune",
        "temp_celsius": 30.0,
        "moisture_pct": 60,
        "wind_kph": 12.5,
        "summary": "Sunny day"
    }}
    print("Create Record:")
    res = requests.post(BASE + "/records/", json=data)
    print_response(res)
    record_id = res.json().get("id")

    print("Get All Records:")
    print_response(requests.get(BASE + "/records/"))

    print("Get One Record:")
    print_response(requests.get(BASE + f"/records/{{record_id}}"))

    print("Update Record:")
    update = {{"temp_celsius": 33.0}}
    print_response(requests.put(BASE + f"/records/{{record_id}}", json=update))

    print("Get Avg Temp:")
    print_response(requests.get(BASE + f"/records/location/Pune/avg/temp"))

    print("Get Avg Moisture:")
    print_response(requests.get(BASE + f"/records/location/Pune/avg/moisture"))

    print("Delete Record:")
    print_response(requests.delete(BASE + f"/records/{{record_id}}"))

if __name__ == "__main__":
    test()
