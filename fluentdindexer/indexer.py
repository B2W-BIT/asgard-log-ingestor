from datetime import datetime

from logingestor.indexer import Indexer

class  FluentdMonitoringIndexer(Indexer):

    def _index_name(self, doc):
        data_part = datetime.utcnow().strftime("%Y-%m-%d-%H")
        return f"asgard-fluentd-monitoring-{data_part}"

    def _prepare_document(self, document):
        return {
            **document.get("payload", {}),
            "key": document["key"],
            "timestamp": document["timestamp"]
        }
