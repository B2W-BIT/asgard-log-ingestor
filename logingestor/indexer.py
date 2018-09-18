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
        should_log_error = True
        total_messages = len(documents)
        rejected = 0
        accepted = total_messages

        _bulk = []
        for doc in documents:
            _bulk.append({ "index" : { "_index" : self._index_name(doc.body), "_type" : "logs"}})
            _bulk.append(self._prepare_document(doc.body))
        result = await self.elasticsearch.bulk(_bulk, request_timeout=conf.BULK_INSERT_TIMEOUT)

        if result["errors"]:
            for idx, item in enumerate(result['items']):
                if item['index'].get("error"):
                    documents[idx].reject()
                    rejected += 1
                    if should_log_error:
                        await self.logger.error({**item['index']['error'], "original-message-str": json.dumps(documents[idx].body['payload'])})
                        should_log_error = False #Logamos apenas um erro por batch
        await self.logger.info({"messages-processed": total_messages, "accepted-messages": accepted - rejected, "rejected": rejected, "errors": result['errors']})
        return result

    def _prepare_document(self, document):
        return document

    def _index_name(self, doc):
        raise NotImplementedError


class AppIndexer(Indexer):


    def _app_name_with_namespace(self, document):
        app_name_with_namespace = document['key'].replace("errors.", "", 1) \
                                             .replace("asgard.app.", "", 1) \
                                             .replace(".", "/")
        return app_name_with_namespace


    def _index_name(self, document):
        app_name_with_namespace = self._app_name_with_namespace(document).replace("/", "-")
        data_part = datetime.utcnow().strftime("%Y-%m-%d-%H")
        return f"asgard-app-logs-{app_name_with_namespace}-{data_part}"

    def _prepare_document(self, raw_document):
        final_document = {}
        utcnow = datetime.now(timezone.utc)
        timestamp = datetime.utcfromtimestamp(raw_document['timestamp']).replace(tzinfo=timezone.utc)
        processing_delay = utcnow - timestamp
        final_document.update(raw_document['payload'])
        final_document.update({
            'asgard_index_delay': processing_delay.total_seconds(),
            'timestamp': timestamp.isoformat(),
            'appname': f"/{self._app_name_with_namespace(raw_document)}",
        })
        return final_document
