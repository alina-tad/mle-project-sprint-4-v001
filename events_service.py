# events_service.py
from typing import Dict, List
from fastapi import FastAPI, HTTPException

class EventStore:
    def __init__(self, max_events_per_user: int = 10):
        self.events: Dict[int, List[int]] = {}
        self.max_events_per_user = max_events_per_user

    def put(self, user_id: int, item_id: int) -> None:
        if user_id <= 0 or item_id <= 0:
            raise ValueError("user_id and item_id must be positive")
        user_events = self.events.get(user_id, [])
        # max_events_per_user строго соблюдаем
        self.events[user_id] = [item_id] + user_events[: self.max_events_per_user - 1]

    def get(self, user_id: int, k: int = 10) -> List[int]:
        if user_id <= 0:
            raise ValueError("user_id must be positive")
        if k <= 0:
            return []
        return self.events.get(user_id, [])[:k]

events_store = EventStore(max_events_per_user=10)
app = FastAPI(title="events")

@app.post("/put")
async def put(user_id: int, item_id: int):
    try:
        events_store.put(user_id, item_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"result": "ok"}

@app.post("/get")
async def get(user_id: int, k: int = 10):
    try:
        return {"events": events_store.get(user_id, k)}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/users")
async def users():
    return {"user_ids": list(events_store.events.keys())}