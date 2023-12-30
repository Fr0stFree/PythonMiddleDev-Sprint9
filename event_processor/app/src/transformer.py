from src.schemas import UserEvent
from datetime import datetime

class EventTransformer:
    def transform(self, events):
        user_events = [UserEvent(**event) for event in events]
        return [(x.user_id, x.event_type.value, x.event_time, x.movie_id) for x in user_events]

