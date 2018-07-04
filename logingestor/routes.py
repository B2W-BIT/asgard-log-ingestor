from datetime import datetime

from asyncworker import App
from asyncworker.options import Options

from logingestor import conf
from logingestor.indexer import AppIndexer

app = App(host=conf.RABBITMQ_HOST, user=conf.RABBITMQ_USER, password=conf.RABBITMQ_PWD, prefetch_count=conf.RABBITMQ_PREFETCH)

indexer = AppIndexer(conf.elasticsearch)

@app.route(conf.LOGS_QUEUE_NAMES, vhost=conf.RABBITMQ_VHOST, options = {Options.BULK_SIZE: conf.LOGS_BULK_SIZE})
async def generic_app_log_indexer(messages):
    should_log_error = True
    total_messages = len(messages)
    rejected = 0
    accepted = total_messages
    result = await indexer.bulk((m.body for m in messages))
    if result["errors"]:
        for idx, item in enumerate(result['items']):
            if item['index'].get("error"):
                messages[idx].reject()
                rejected += 1
                if should_log_error:
                    await conf.logger.error({**item['index']['error'], "original-message": messages[idx].body})
                    should_log_error = False #Logamos apenas um erro por batch
    await conf.logger.info({"messages-processed": total_messages, "accepted-messages": accepted - rejected, "rejected": rejected, "errors": result['errors']})

