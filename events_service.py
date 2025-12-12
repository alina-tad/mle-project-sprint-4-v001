# events_service.py
from fastapi import FastAPI

class EventStore:
    def __init__(self, max_events_per_user: int = 10):
        self.events = {}
        self.max_events_per_user = max_events_per_user

    def put(self, user_id: int, item_id: int):
        user_events = self.events.get(user_id, [])
        self.events[user_id] = [item_id] + user_events[: self.max_events_per_user]

    def get(self, user_id: int, k: int = 10):
        user_events = self.events.get(user_id, [])
        return user_events[:k]

events_store = EventStore(max_events_per_user=10)

app = FastAPI(title="events")

@app.post("/put")
async def put(user_id: int, item_id: int):
    events_store.put(user_id, item_id)
    return {"result": "ok"}

@app.post("/get")
async def get(user_id: int, k: int = 10):
    return {"events": events_store.get(user_id, k)}

@app.post("/users")
async def users():
    return {"user_ids": list(events_store.events.keys())}