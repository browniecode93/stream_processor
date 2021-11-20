FROM python:3.8-slim-buster

ENV PYTHONIOENCODING=utf-8
ENV PYTHONUNBUFFERED 1
ENV PYTHONDONTWRITEBYTECODE 1

RUN addgroup --system stream && adduser --system --ingroup stream stream

COPY ./requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt && rm -rf requirements.txt

COPY ./streams /app/streams
COPY ./compose/scripts /app

RUN chown -R stream:stream /app
USER stream

WORKDIR /app

