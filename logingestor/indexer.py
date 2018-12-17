import json
from typing import List
from datetime import datetime, timezone, timedelta

from asyncworker.rabbitmq.message import RabbitMQMessage
from logingestor import conf

class Indexer:

    def __init__(self, elasticsearch, logger):
        self.elasticsearch = elasticsearch
        self.logger = logger

    async def index(self, document):
        pass

    async def bulk(self, documents: List[RabbitMQMessage]):
        total_messages = len(documents)
        rejected = 0
        accepted = total_messages

        _bulk = []
        for doc in documents:
            _bulk.append({ "index" : { "_index" : self._index_name(doc.body), "_type" : "logs"}})
            _bulk.append(self._prepare_document(doc.body))
        result = await self.elasticsearch.bulk(_bulk, request_timeout=conf.BULK_INSERT_TIMEOUT)

        if result["errors"]:
            _second_bulk = []
            for idx, item in enumerate(result['items']):
                if item['index'].get("error"):
                    rejected += 1
                    original_document = documents[idx]
                    _second_bulk.append({ "index" : { "_index" : self._index_name(original_document.body), "_type" : "logs"}})
                    prepared_document = self.prepare_document(original_document.body)
                    new_document = {
                        "asgard": {
                            "original": {
                                "msg": json.dumps(original_document.body['payload'])
                            },
                            "error": item["index"]["error"],
                            "index_error": True,
                        },
                        "timestamp": prepared_document["timestamp"],
                        "appname": self._app_name_with_namespace(original_document.body),
                        "asgard_index_delay": prepared_document["asgard_index_delay"]
                    }
                    _second_bulk.append(new_document)
            await self.elasticsearch.bulk(_second_bulk, request_timeout=conf.BULK_INSERT_TIMEOUT)
        await self.logger.info({"messages-processed": total_messages, "accepted-messages": accepted - rejected, "rejected": rejected, "errors": result['errors']})
        return result

    def prepare_document(self, document):
        final_document = self._prepare_document(document)

        utcnow = datetime.now(timezone.utc)
        timestamp = datetime.utcfromtimestamp(document['timestamp']).replace(tzinfo=timezone.utc)
        processing_delay = utcnow - timestamp
        final_document.update({
            'asgard_index_delay': processing_delay.total_seconds(),
            'timestamp': timestamp.isoformat(),
            'appname': self._app_name_with_namespace(document),
        })
        return final_document

    def _prepare_document(self, document):
        return dict(document)

    def _index_name(self, doc):
        raise NotImplementedError

    def _extract_appname(self, document):
        raise NotImplementedError

    def _app_name_with_namespace(self, document):
        return self._extract_appname(document)


class AppIndexer(Indexer):


    def _extract_appname(self, document):
        app_name_with_namespace = document['key'].replace("errors.", "", 1) \
                                             .replace("asgard.app.", "", 1) \
                                             .replace(".", "/")
        return f"/{app_name_with_namespace}"


    def _index_name(self, document):
        app_name_with_namespace = self._app_name_with_namespace(document).strip("/").replace("/", "-")
        data_part = datetime.utcnow().strftime("%Y-%m-%d-%H")
        return f"asgard-app-logs-{app_name_with_namespace}-{data_part}"

    def _prepare_document(self, raw_document):
        final_document = {}
        final_document.update(raw_document['payload'])
        return final_document
