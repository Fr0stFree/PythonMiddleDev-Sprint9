import datetime as dt
from collections import defaultdict

from protocol.events_pb2 import EventStream, Click, MovieWatched, MovieQualityChanged, PageViewed, FiltersApplied


class EventTransformer:
    def serialize_stream(self, blob: bytes) -> dict[str, list[dict]]:
        sorted_events = defaultdict(list)

        for event in EventStream.FromString(blob).events:
            match event.WhichOneof('event'):
                case 'click':
                    sorted_events['clicks'].append(self._serialize_click(event.click))

                case 'movie_watched':
                    sorted_events['movie_viewings'].append(self._serialize_movie_watched(event.movie_watched))

                case 'movie_quality_changed':
                    sorted_events['movie_quality_switches'].append(
                        self._serialize_movie_quality_changed(event.movie_quality_changed))

                case 'page_viewed':
                    sorted_events['page_views'].append(self._serialize_page_view(event.page_viewed))

                case 'filters_applied':
                    sorted_events['filters_applications'].append(self._serialize_filters_applied(event.filters_applied))

        return sorted_events

    def _serialize_click(self, click: Click) -> dict:
        return {
            'user_id': click.info.user_id,
            'happened_at': dt.datetime.fromtimestamp(click.info.happened_at),
            'element_id': click.element_id,
            'element_type': Click.ElementType.Name(click.element_type).upper(),
        }

    def _serialize_page_view(self, page_view: PageViewed) -> dict:
        return {
            'user_id': page_view.info.user_id,
            'happened_at': dt.datetime.fromtimestamp(page_view.info.happened_at),
            'duration': page_view.duration,
            'page_type': PageViewed.PageType.Name(page_view.page_type).upper(),
        }

    def _serialize_movie_watched(self, movie_watched: MovieWatched) -> dict:
        return {
            'user_id': movie_watched.info.user_id,
            'happened_at': dt.datetime.fromtimestamp(movie_watched.info.happened_at),
            'movie_id': movie_watched.movie_id,
        }

    def _serialize_movie_quality_changed(self, movie_quality_changed: MovieQualityChanged) -> dict:
        return {
            'user_id': movie_quality_changed.info.user_id,
            'happened_at': dt.datetime.fromtimestamp(movie_quality_changed.info.happened_at),
            'movie_id': movie_quality_changed.movie_id,
            'before': MovieQualityChanged.Quality.Name(movie_quality_changed.before).upper(),
            'after': MovieQualityChanged.Quality.Name(movie_quality_changed.after).upper(),
        }

    def _serialize_filters_applied(self, filters_applied: FiltersApplied) -> dict:
        return {
            'user_id': filters_applied.info.user_id,
            'happened_at': dt.datetime.fromtimestamp(filters_applied.info.happened_at),
            'query': filters_applied.query,
        }
