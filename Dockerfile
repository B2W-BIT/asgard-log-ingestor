FROM python:3.6.5-alpine

#Tag: sieve/infra/asgard-log-ingestor
#Version: 0.7.2

WORKDIR /opt/app

RUN pip install -U pip \
    && pip install pipenv==2018.05.18

COPY . /opt/app

RUN apk -U add --virtual .deps gcc g++ make python3-dev \
&& pipenv install --system --deploy --ignore-pipfile \
&& apk --purge del .deps

CMD ["python", "-m", "logingestor"]
