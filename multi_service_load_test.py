import requests
import time
import random

# Конфигурация
BASE_URLS = {
    "events": "http://localhost:8001",
    "features": "http://localhost:8002",
    "recommendations": "http://localhost:8003"
}

# Симуляция
USER_IDS = list(range(1000000, 1000100))
ITEM_IDS = list(range(12345, 12375))

def test_events_service():
    user_id = random.choice(USER_IDS)
    item_id = random.choice(ITEM_IDS)

    # Test /put
    try:
        put_resp = requests.post(f"{BASE_URLS['events']}/put", params={"user_id": user_id, "item_id": item_id})
        print(f"[events /put] Status: {put_resp.status_code}, Response: {put_resp.json()}")
    except Exception as e:
        print(f"[events /put] Error: {e}")

    # Test /get
    try:
        get_resp = requests.post(f"{BASE_URLS['events']}/get", params={"user_id": user_id, "k": 3})
        print(f"[events /get] Status: {get_resp.status_code}, Response: {get_resp.json()}")
    except Exception as e:
        print(f"[events /get] Error: {e}")

def test_features_service():
    item_id = random.choice(ITEM_IDS)

    try:
        resp = requests.post(f"{BASE_URLS['features']}/similar_items", params={"item_id": item_id, "k": 3})
        print(f"[features /similar_items] Status: {resp.status_code}, Response: {resp.json()}")
    except Exception as e:
        print(f"[features /similar_items] Error: {e}")

def test_recommendations_service():
    user_id = random.choice(USER_IDS)

    try:
        resp_offline = requests.post(f"{BASE_URLS['recommendations']}/recommendations_offline", params={"user_id": user_id, "k": 3})
        print(f"[recs /recommendations_offline] Status: {resp_offline.status_code}, Response: {resp_offline.json()}")
    except Exception as e:
        print(f"[recs /recommendations_offline] Error: {e}")

    try:
        resp_online = requests.post(f"{BASE_URLS['recommendations']}/recommendations_online", params={"user_id": user_id, "k": 3})
        print(f"[recs /recommendations_online] Status: {resp_online.status_code}, Response: {resp_online.json()}")
    except Exception as e:
        print(f"[recs /recommendations_online] Error: {e}")

    try:
        resp_combined = requests.post(f"{BASE_URLS['recommendations']}/recommendations", params={"user_id": user_id, "k": 3})
        print(f"[recs /recommendations] Status: {resp_combined.status_code}, Response: {resp_combined.json()}")
    except Exception as e:
        print(f"[recs /recommendations] Error: {e}")

# --- Run Test Loop ---
NUM_ITERATIONS = 20

for i in range(NUM_ITERATIONS):
    print(f"\n--- Iteration {i+1} ---")

    test_func = random.choice([test_events_service, test_features_service, test_recommendations_service])
    test_func()

    # Random pause between requests
    time.sleep(random.uniform(0.5, 1.5))