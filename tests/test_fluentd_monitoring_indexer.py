import asynctest
from asynctest.mock import CoroutineMock, ANY
from freezegun import freeze_time

from fluentdindexer.indexer import FluentdMonitoringIndexer


class FluentdMonitoringIndexerTest(asynctest.TestCase):

    def setUp(self):
        self.elasticsearch_mock = CoroutineMock(index=CoroutineMock(), bulk=CoroutineMock())
        self.indexer = FluentdMonitoringIndexer(self.elasticsearch_mock, CoroutineMock())

    @freeze_time("2018-06-27T10:00:00-03:00")
    def test_test_generate_correct_index_name(self):
        self.assertEqual("asgard-fluentd-monitoring-2018-06-27-13", self.indexer._index_name({"key": "fluentd.internal.monitoring"}))
        self.assertEqual("asgard-fluentd-monitoring-2018-06-27-13", self.indexer._index_name({"key": "fluentd.internal.monitoring"}))

    async def test_prepare_document_for_indexing(self):
        original_payload = {
                  "buffer_queue_length" : 0,
                  "plugin_id" : "app-logs-out-rabbitmq",
                  "buffer_total_queued_size" : 75,
                  "plugin_category" : "output",
                  "retry_count" : 0,
                  "output_plugin" : True,
                  "type" : "amqp"
        }

        document = {
               "payload" : original_payload,
               "key" : "fluentd.internal.monitoring",
               "timestamp" : 1531223488
        }
        expected_document = {
            **original_payload,
            "key": "fluentd.internal.monitoring",
            "timestamp": "2018-07-10T11:51:28+00:00"
        }

        prepared_document = self.indexer._prepare_document(document)
        prepared_document['timestamp'] = ANY
        self.assertEqual(expected_document, prepared_document)
        self.assertEqual(expected_document['timestamp'], "2018-07-10T11:51:28+00:00")

