from datetime import datetime

from logingestor.indexer import Indexer

class StatsIndexer(Indexer):

    def _prepare_document(self, document):
        return document

    def _index_name(self, document):
        data_part = datetime.utcnow().strftime("%Y-%m-%d-%H")
        return f"asgard-app-stats-{data_part}"
