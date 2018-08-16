from datetime import datetime, timezone

from logingestor.indexer import Indexer

class  FluentdMonitoringIndexer(Indexer):

    def _index_name(self, doc):
        data_part = datetime.utcnow().strftime("%Y-%m-%d-%H")
        return f"asgard-fluentd-monitoring-{data_part}"

    def _prepare_document(self, document):
        original_timestamp = datetime.utcfromtimestamp(document['timestamp']).replace(tzinfo=timezone.utc)
        return {
            **document.get("payload", {}),
            "key": document["key"],
            "timestamp": original_timestamp.isoformat()
        }
