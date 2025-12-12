# # features_service.py
# import io
# import logging
# from contextlib import asynccontextmanager

# import boto3
# import pandas as pd
# from fastapi import FastAPI
# from dotenv import load_dotenv

# load_dotenv()
# logger = logging.getLogger("uvicorn.error")

# # S3 config
# import os
# S3_BUCKET = os.environ["S3_BUCKET_NAME"]
# S3_ENDPOINT = os.environ["MLFLOW_S3_ENDPOINT_URL"]
# AWS_KEY = os.environ["AWS_ACCESS_KEY_ID"]
# AWS_SECRET = os.environ["AWS_SECRET_ACCESS_KEY"]

# SIMILAR_KEY = "recsys/recommendations/similar.parquet"

# def make_s3():
#     return boto3.client(
#         "s3",
#         endpoint_url=S3_ENDPOINT,
#         aws_access_key_id=AWS_KEY,
#         aws_secret_access_key=AWS_SECRET,
#     )

# def read_parquet_from_s3(s3, bucket: str, key: str, columns=None) -> pd.DataFrame:
#     obj = s3.get_object(Bucket=bucket, Key=key)
#     data = io.BytesIO(obj["Body"].read())
#     return pd.read_parquet(data, columns=columns)

# class SimilarItems:
#     def __init__(self):
#         self._df = None

#     def load(self, df: pd.DataFrame):
#         # Ожидаем колонки: item_id_1, item_id_2, score
#         self._df = df.set_index("item_id_1").sort_index()

#     def get(self, item_id: int, k: int = 10):
#         try:
#             part = self._df.loc[item_id].head(k)
#             return part[["item_id_2", "score"]].to_dict(orient="list")
#         except KeyError:
#             logger.error("No similar items found")
#             return {"item_id_2": [], "score": []}

# sim_items_store = SimilarItems()
# s3 = make_s3()

# @asynccontextmanager
# async def lifespan(app: FastAPI):
#     logger.info("Loading similar items from S3...")
#     df = read_parquet_from_s3(
#         s3, S3_BUCKET, SIMILAR_KEY,
#         columns=["item_id_1", "item_id_2", "score"],
#     )
#     sim_items_store.load(df)
#     logger.info("Features service ready")
#     yield
#     logger.info("Features service stopping")

# app = FastAPI(title="features", lifespan=lifespan)

# @app.post("/similar_items")
# async def similar_items(item_id: int, k: int = 10):
#     return sim_items_store.get(item_id, k)

# features_service.py
import io
import os
import logging
from contextlib import asynccontextmanager

import boto3
import pandas as pd
from fastapi import FastAPI
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger("uvicorn.error")

# --- S3 config ---
S3_BUCKET = os.environ["S3_BUCKET_NAME"]
S3_ENDPOINT = os.environ["MLFLOW_S3_ENDPOINT_URL"]
AWS_KEY = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET = os.environ["AWS_SECRET_ACCESS_KEY"]

SIMILAR_KEY = "recsys/recommendations/similar.parquet"


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


class SimilarItems:
    def __init__(self):
        self._df: pd.DataFrame | None = None

    def load(self, df: pd.DataFrame):
        """
        Ожидаем колонки: item_id_1, item_id_2, score
        """
        required = {"item_id_1", "item_id_2", "score"}
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f"similar.parquet missing columns: {missing}")

        # типы (чтобы loc не страдал от float/int и т.п.)
        df = df.copy()
        df["item_id_1"] = df["item_id_1"].astype("int64")
        df["item_id_2"] = df["item_id_2"].astype("int64")
        df["score"] = df["score"].astype("float32")

        # ВАЖНО: сортируем по score внутри item_id_1, чтобы head(k) был top-k по релевантности
        df = df.sort_values(["item_id_1", "score"], ascending=[True, False])

        # Индексируем по item_id_1 для быстрого доступа
        self._df = df.set_index("item_id_1")
    
        # быстрый семпл без unique() на всём индексе
        self._sample_ids = (
            df["item_id_1"]
            .drop_duplicates()
            .head(1000)
            .astype(int)
            .tolist()
        )
        logger.info(
            f"Loaded similar items: rows={len(df):,}, unique item_id_1={df['item_id_1'].nunique():,}"
        )

    def sample_item_id(self) -> int | None:
        if not self._sample_ids:
            return None
        return self._sample_ids[0]

    def get(self, item_id: int, k: int = 10):
        if self._df is None:
            logger.error("Similar items store not loaded yet")
            return {"item_id_2": [], "score": []}

        try:
            part = self._df.loc[int(item_id)]
        except KeyError:
            # не считаем это ошибкой сервиса — это нормальный кейс
            return {"item_id_2": [], "score": []}

        # part может быть Series (если одна строка) или DataFrame (если несколько)
        if isinstance(part, pd.Series):
            part = part.to_frame().T

        part = part.head(k)
        return part[["item_id_2", "score"]].to_dict(orient="list")


sim_items_store = SimilarItems()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Loading similar items from S3...")
    s3 = make_s3()

    df = read_parquet_from_s3(
        s3,
        S3_BUCKET,
        SIMILAR_KEY,
        columns=["item_id_1", "item_id_2", "score"],
    )

    sim_items_store.load(df)
    logger.info("Features service ready")
    yield
    logger.info("Features service stopping")


app = FastAPI(title="features", lifespan=lifespan)


@app.post("/similar_items")
async def similar_items(item_id: int, k: int = 10):
    return sim_items_store.get(item_id, k)

@app.post("/sample_item")
async def sample_item():
    item_id = sim_items_store.sample_item_id()
    if item_id is None:
        return {"item_id": None}
    return {"item_id": int(item_id)}