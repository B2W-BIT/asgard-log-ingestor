import asyncio
import json
from datetime import datetime

from asyncworker import App
from asyncworker.options import Options

from logingestor import conf
from aioelasticsearch import helpers

app = App(host=conf.RABBITMQ_HOST, user=conf.RABBITMQ_USER, password=conf.RABBITMQ_PWD, prefetch_count=conf.RABBITMQ_PREFETCH)

