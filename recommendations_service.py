# recommendations_service.py
import io
import os
import logging
from contextlib import asynccontextmanager

import boto3
import pandas as pd
import requests
from fastapi import FastAPI
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger("uvicorn.error")

# адреса других сервисов
features_store_url = "http://127.0.0.1:8010"
events_store_url = "http://127.0.0.1:8020"

# S3 config
S3_BUCKET = os.environ["S3_BUCKET_NAME"]
S3_ENDPOINT = os.environ["MLFLOW_S3_ENDPOINT_URL"]
AWS_KEY = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET = os.environ["AWS_SECRET_ACCESS_KEY"]
HTTP_TIMEOUT_SEC = float(os.getenv("HTTP_TIMEOUT_SEC", "2.0"))
EVENTS_K_LAST = int(os.getenv("EVENTS_K_LAST", "3"))

ONLINE_WEIGHT = int(os.getenv("ONLINE_WEIGHT", "1"))   # сколько online подряд
OFFLINE_WEIGHT = int(os.getenv("OFFLINE_WEIGHT", "1")) # сколько offline подряд

OFFLINE_KEY = "recsys/recommendations/recommendations.parquet"   # финальные офлайн
TOP_KEY = "recsys/recommendations/top_popular.parquet"          # дефолт

def make_s3():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET,
    )

def read_parquet_from_s3(s3, bucket: str, key: str, columns=None) -> pd.DataFrame:
    obj = s3.get_object(Bucket=bucket, Key=key)
    data = io.BytesIO(obj["Body"].read())
    return pd.read_parquet(data, columns=columns)

def dedup_ids(ids):
    seen = set()
    return [x for x in ids if not (x in seen or seen.add(x))]

class Recommendations:
    def __init__(self):
        self._recs = {"personal": None, "default": None}
        self._stats = {"request_personal_count": 0, "request_default_count": 0}

    def load_personal(self, df: pd.DataFrame | None):
        if df is None or df.empty:
            logger.warning("Personal recs are empty: fallback will always use default")
            self._recs["personal"] = None
            return
        self._recs["personal"] = df.set_index("user_id").sort_index()

    def load_default(self, df: pd.DataFrame | None):
        if df is None or df.empty:
            raise ValueError("Default recs (top_popular) are empty — service can't work")
        self._recs["default"] = df

    def get_offline(self, user_id: int, k: int = 100):
        try:
            if self._recs["personal"] is not None:
                recs = self._recs["personal"].loc[user_id]
                if isinstance(recs, pd.Series):
                    recs = recs.to_frame().T
                recs = recs["item_id"].tolist()[:k]
                self._stats["request_personal_count"] += 1
                return recs

            raise KeyError  # сразу уходим в default

        except KeyError:
            recs = self._recs["default"]["item_id"].to_list()[:k]
            self._stats["request_default_count"] += 1
            return recs
        except Exception:
            logger.exception("Offline recs error")
            return []

    def stats(self):
        logger.info("Stats for recommendations")
        for k, v in self._stats.items():
            logger.info(f"{k:<30} {v}")

rec_store = Recommendations()
s3 = make_s3()

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting recommendations service: loading offline recs from S3...")

    personal = read_parquet_from_s3(
        s3, S3_BUCKET, OFFLINE_KEY,
        columns=["user_id", "item_id"],
    )
    top = read_parquet_from_s3(
        s3, S3_BUCKET, TOP_KEY,
        columns=["item_id"],
    )

    rec_store.load_personal(personal)
    rec_store.load_default(top)

    logger.info("Ready!")
    yield
    rec_store.stats()
    logger.info("Stopping")

app = FastAPI(title="recommendations", lifespan=lifespan)

@app.post("/recommendations_offline")
async def recommendations_offline(user_id: int, k: int = 100):
    recs = rec_store.get_offline(user_id, k)
    return {"recs": recs}

@app.post("/recommendations_online")
async def recommendations_online(user_id: int, k: int = 100):
    headers = {"Content-type": "application/json", "Accept": "text/plain"}

    # 1) берём 3 последних события
    resp = requests.post(
        events_store_url + "/get",
        headers=headers,
        params={"user_id": user_id, "k": EVENTS_K_LAST},
        timeout=HTTP_TIMEOUT_SEC,
    )
    if resp.status_code != 200:
        return {"recs": []}

    events = resp.json().get("events", [])
    if not events:
        return {"recs": []}

    # 2) собираем кандидатов i2i
    items, scores = [], []
    for item_id in events:
        resp_sim = requests.post(
            features_store_url + "/similar_items",
            headers=headers,
            params={"item_id": int(item_id), "k": k},
            timeout=HTTP_TIMEOUT_SEC,
        )
        if resp_sim.status_code != 200:
            continue

        sim = resp_sim.json()
        logger.info(f"user={user_id} item={item_id} sim_count={len(sim.get('item_id_2', []))}")
        i2 = sim.get("item_id_2", [])
        sc = sim.get("score", [])

        # защита от “не списков”
        if not isinstance(i2, list) or not isinstance(sc, list):
            continue

        # защита от разной длины
        m = min(len(i2), len(sc))
        items += i2[:m]
        scores += sc[:m]

    if not items:
        return {"recs": []}

    combined = sorted(zip(items, scores), key=lambda x: x[1], reverse=True)
    recs = dedup_ids([item for item, _ in combined])

    return {"recs": recs[:k]}

def weighted_blend(online: list[int], offline: list[int], k: int) -> list[int]:
    out = []
    i = j = 0
    while len(out) < k and (i < len(online) or j < len(offline)):
        for _ in range(ONLINE_WEIGHT):
            if i < len(online) and len(out) < k:
                out.append(online[i]); i += 1
        for _ in range(OFFLINE_WEIGHT):
            if j < len(offline) and len(out) < k:
                out.append(offline[j]); j += 1
    return dedup_ids(out)[:k]

@app.post("/recommendations")
async def recommendations(user_id: int, k: int = 100):
    # отдельно
    recs_offline = (await recommendations_offline(user_id, k))["recs"]
    recs_online = (await recommendations_online(user_id, k))["recs"]

    # blending: нечётные — online, чётные — offline
    blended = []
    min_len = min(len(recs_offline), len(recs_online))
    for i in range(min_len):
        blended.append(recs_online[i])
        blended.append(recs_offline[i])

    if len(recs_online) > min_len:
        blended.extend(recs_online[min_len:])
    if len(recs_offline) > min_len:
        blended.extend(recs_offline[min_len:])

    blended = weighted_blend(recs_online, recs_offline, k)
    return {"recs": blended}