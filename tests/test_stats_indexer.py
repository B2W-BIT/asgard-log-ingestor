import asynctest
from asynctest.mock import CoroutineMock, ANY
from freezegun import freeze_time

from statsindexer.indexer import StatsIndexer


class StatsIndexerTest(asynctest.TestCase):

    def setUp(self):
        self.elasticsearch_mock = CoroutineMock(index=CoroutineMock(), bulk=CoroutineMock())
        self.indexer = StatsIndexer(self.elasticsearch_mock, CoroutineMock())

    @freeze_time("2018-06-27T10:00:00-03:00")
    def test_test_generate_correct_index_name(self):
        """
        Index name: asgard-app-stats-<namespace>-<appname>
        Trocamos "/" por "-"
        Sempre geramos parte da data no nome do índice usanto UTC.
        """
        self.assertEqual("asgard-app-stats-2018-06-27-13", self.indexer._index_name({"appname": "/infra/app/in/some/inner/folder"}))
        self.assertEqual("asgard-app-stats-2018-06-27-13", self.indexer._index_name({"appname": "/dev/other/app-with-dashes"}))

    async def test_prepare_document_returns_same_document(self):
        document = {"some-key": "some-value", "appname": "/dev/foo", "timestamp": 1531222358}
        expected_document = self.indexer._prepare_document(document)
        expected_document['timestamp'] = ANY
        self.assertEqual(expected_document, document)

    async def test_prepare_document_format_timestamp(self):
        document = {"some-key": "some-value", "appname": "/dev/foo", "timestamp": 1531223488}
        expected_document = self.indexer._prepare_document(document)
        self.assertEqual(expected_document['timestamp'], "2018-07-10T11:51:28+00:00")
