FROM python:3.8-slim as base

ARG APP_NAME=open_meteo_exporter
ARG HOME="/app"

ENV HOME=${HOME}
ENV APP_NAME=${APP_NAME}

WORKDIR ${HOME}
COPY requirements.txt .
RUN /usr/local/bin/python -m pip install --upgrade pip
RUN pip3 install -r requirements.txt

FROM base as src

COPY ${APP_NAME} ./${APP_NAME}
COPY __main__.py .
COPY bin ./bin
COPY setup.py .
RUN pip install -e .

# COPY tests ./tests
# RUN ./bin/entrypoint test

FROM src as prod
ENTRYPOINT ["/bin/bash", "./bin/entrypoint"]
