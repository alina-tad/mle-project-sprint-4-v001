# test_service.py
"""
Тестирование сервиса рекомендаций.

Покрывает три кейса:
1) пользователь без персональных рекомендаций,
2) пользователь с офлайн-рекомендациями, но без онлайн-истории,
3) пользователь с офлайн- и онлайн-рекомендациями.

Скрипт самодостаточен: не требует ручного подбора item_id.
"""

import requests
from requests.exceptions import RequestException
from json import JSONDecodeError
from datetime import datetime

# URLs сервисов
recommendations_store_url = "http://127.0.0.1:8000"
features_store_url = "http://127.0.0.1:8010"
events_store_url = "http://127.0.0.1:8020"

headers = {"Content-type": "application/json", "Accept": "text/plain"}

def log(msg: str):
    """Печать логов с таймстампом."""
    print(f"[{datetime.now().isoformat(timespec='seconds')}] {msg}")

def call(url: str, params: dict | None = None):
    """
    Универсальный POST-вызов сервиса.

    Возвращает:
        (status_code, json_body | error_text)
    """
    params = params or {}
    try:
        resp = requests.post(url, headers=headers, params=params, timeout=10)
    except RequestException as e:
        return 0, {"error": f"request failed: {e}"}

    try:
        body = resp.json()
    except JSONDecodeError:
        body = {"error": resp.text}

    return resp.status_code, body

def assert_ok(name: str, resp):
    """Проверка, что сервис ответил 200 OK."""
    code, body = resp
    if code != 200:
        raise RuntimeError(f"{name} failed: status={code}, body={body}")

# -------- Event Store --------

def put_event(user_id: int, item_id: int):
    """Записать онлайн-событие пользователя."""
    return call(
        events_store_url + "/put",
        {"user_id": user_id, "item_id": item_id},
    )

def get_events(user_id: int, k: int = 10):
    """Получить последние k событий пользователя."""
    return call(
        events_store_url + "/get",
        {"user_id": user_id, "k": k},
    )

# -------- Recommendation Store --------

def get_offline(user_id: int, k: int = 10):
    """Офлайн-рекомендации (ALS / popular)."""
    return call(
        recommendations_store_url + "/recommendations_offline",
        {"user_id": user_id, "k": k},
    )

def get_online(user_id: int, k: int = 10):
    """Онлайн-рекомендации (i2i по последним событиям)."""
    return call(
        recommendations_store_url + "/recommendations_online",
        {"user_id": user_id, "k": k},
    )

def get_blended(user_id: int, k: int = 10):
    """Смешанные рекомендации (offline + online)."""
    return call(
        recommendations_store_url + "/recommendations",
        {"user_id": user_id, "k": k},
    )

# -------- Feature Store --------

def sample_item_id() -> int:
    """
    Получить item_id, который ГАРАНТИРОВАННО присутствует в Feature Store.

    Почему это важно:
    - при обучении ALS мы использовали срез данных
      (subset пользователей и треков);
    - далеко не каждый item_id из исходного датасета
      присутствует в similar.parquet;
    - использование /sample_item делает тест
      детерминированным и воспроизводимым.
    """
    resp = call(features_store_url + "/sample_item")
    assert_ok("features/sample_item", resp)

    item_id = resp[1].get("item_id")
    if item_id is None:
        raise RuntimeError(f"/sample_item returned no item_id: {resp}")

    return int(item_id)

# -------- Main --------

def main():
    k = 10

    # sanity check
    log("SANITY: checking services availability")
    assert_ok("offline", get_offline(999999999, 1))
    assert_ok("events", get_events(999999999, 1))
    assert_ok("features", call(features_store_url + "/sample_item"))
    log("SANITY: ok\n")

    # CASE 1: no personal recs
    user_no_personal = 999_999_999
    log("CASE 1: user without personal recs")
    log(f"offline -> {get_offline(user_no_personal, k)}")
    log(f"online  -> {get_online(user_no_personal, k)}")
    log(f"blend   -> {get_blended(user_no_personal, k)}")

    # CASE 2: personal offline, no online history
    user_personal_only = 23
    log("\nCASE 2: user with personal offline, no online history")
    log(f"offline -> {get_offline(user_personal_only, k)}")
    log(f"online  -> {get_online(user_personal_only, k)}")
    log(f"blend   -> {get_blended(user_personal_only, k)}")

    # CASE 3: personal + online history
    log("\nCASE 3: user with personal + online history")
    item_ids = [sample_item_id(), sample_item_id(), sample_item_id()]
    log(f"picked feature-store items -> {item_ids}")

    for item_id in item_ids:
        resp = put_event(user_personal_only, item_id)
        log(f"put event {item_id} -> {resp}")
        assert_ok("events/put", resp)

    log(f"events  -> {get_events(user_personal_only, 3)}")
    log(f"offline -> {get_offline(user_personal_only, k)}")
    log(f"online  -> {get_online(user_personal_only, k)}")
    log(f"blend   -> {get_blended(user_personal_only, k)}")


if __name__ == "__main__":
    main()