from datetime import datetime, timezone, timedelta

from logingestor import conf

class Indexer:

    def __init__(self, elasticsearch):
        self.elasticsearch = elasticsearch

    async def index(self, document):
        pass

    async def bulk(self, documents):
        pass


class AppIndexer(Indexer):

    def _index_name(self, fluentd_key):
        app_name_with_namespace = fluentd_key.replace("errors.", "", 1) \
                                             .replace("asgard.app.", "", 1) \
                                             .replace(".", "-")
        data_part = datetime.utcnow().strftime("%Y-%m-%d")
        return f"asgard-app-logs-{app_name_with_namespace}-{data_part}"

    def _prepare_document(self, raw_document):
        utcnow = datetime.now(timezone.utc)
        timestamp = datetime.utcfromtimestamp(raw_document['timestamp']).replace(tzinfo=timezone.utc)
        processing_delay = utcnow - timestamp
        return {'asgard_index_delay': processing_delay.total_seconds()}
