from datetime import datetime, timezone

from logingestor.indexer import Indexer
from logingestor import conf

class StatsIndexer(Indexer):

    def _prepare_document(self, document):
        original_timestamp = datetime.utcfromtimestamp(document['timestamp']).replace(tzinfo=timezone.utc)
        document['timestamp'] = original_timestamp.isoformat()
        return document

    def _index_name(self, document):
        data_part = datetime.utcnow().strftime("%Y-%m-%d-%H")
        return f"{conf.STATS_INDEX_PREFIX}-{data_part}"

    def _extract_appname(self, document):
        return document["appname"]
