from src.schemas import UserEvent


class EventTransformer:
    def transform(self, events):
        user_events = [UserEvent(**event) for event in events]
        return [(x.user_id, x.event_type.value, x.timestamp, x.movie_id) for x in user_events]
