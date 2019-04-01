from datetime import datetime, timezone

from logingestor import conf
from logingestor.indexer import Indexer


class StatsIndexer(Indexer):
    def _prepare_document(self, document):
        doc_copy = document.copy()
        original_timestamp = datetime.utcfromtimestamp(
            document["timestamp"]
        ).replace(tzinfo=timezone.utc)
        doc_copy["timestamp"] = original_timestamp.isoformat()
        return doc_copy

    def _index_name(self, document):
        app_part = document["appname"].strip("/").replace("/", "-")
        data_part = datetime.utcnow().strftime("%Y-%m-%d-%H")
        return f"{conf.STATS_INDEX_PREFIX}-{app_part}-{data_part}"

    def _extract_appname(self, document):
        return document["appname"]
