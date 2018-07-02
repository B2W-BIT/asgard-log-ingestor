# Asgard Log Ingestor

Esse é o ingestor de logs de (quase) todas as apps que rodam no Asgard. Quase pois algumas apps precisam de um
processamento especial nos logs, por exemplo, Proxy reverso.

Esse código serve para apps que não precisam de nenhum tratamento nos logs, ou seja, basta indexar em algum lugar.

## Env vars

Essas são as envs que o cóidigo precisa para rodar:

### Obrigatórias

* LOGS_RABBITMQ_HOST: Host onde vamos nos conectar para pegar mensagens. Não inclui a porta, apenas o IP;
* LOGS_RABBITMQ_USER: Usuário que se conectrá com a fila;
* LOGS_RABBITMQ_PWD: Senha que será usada;
* LOGS_QUEUE_NAMES: Lista separada por vírgula de todas as filas que serão consumidas.

### Opcionais


* LOGS_RABBITMQ_VHOST: Vhost da fila que será usado na conexão com a fila. Se nçao passado, será usado vhost `/`;
* ASGARD_LOGINGESTOR_LOGLEVEL: Log level que o código usará em seus logs: Valores possíveis são os levels do Python: "INFO", "ERROR", ...
* LOGS_RABBITMQ_PREFETCH: Prefetch usado para pegar mensagens da fila. Esse número de msgs é usado para **cada** fila que está sendo consumida.
* LOGS_BULK_SIZE: Tamanho do lote de mensagens que os handlers vão receber. O framework `async-worker` acumula mensagens até chegar esse numero e depois disso ele entrega **todas** as mensagens para o handler.


Nota sobre BULK_SIZE: O valor do BULK_SIZE sempre é escolhido com a fórmula: `min(BULK_SIZE, PREFRETCH)`. Isso para evitar que o código fique em um deadlock, onde ao mesmo tempo que ele aguarda o bulk encher para poder pegar mais mensagens da fila, ele está aguardando o bulk esvaziar para pegar mais mensagens da fila.

## Handlers implementados

```python
@app.route(conf.LOGS_QUEUE_NAMES, vhost=conf.RABBITMQ_VHOST, options = {Options.BULK_SIZE: conf.LOGS_BULK_SIZE})
async def generic_app_log_indexer(messages)
```
