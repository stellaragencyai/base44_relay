import requests

url = " https://osseously-nondepraved-crysta.ngrok-free.dev/bybit/subuids"  # replace with your current ngrok URL
headers = {"Authorization": "Bearer 73f2c8667fe887ce1a0a6969a511ea27f6e807f51b0680a6268c30b787ab5f87 "}      # replace with your real relay token

r = requests.get(url, headers=headers, timeout=20)
print(r.status_code)
print(r.text)



