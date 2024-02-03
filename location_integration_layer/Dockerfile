FROM python:3.9-slim

ENV DAGSTER_HOME=/opt/dagster/dagster_home/
ENV DAGSTER_CWD=/opt/dagster/app/

WORKDIR ${DAGSTER_CWD}

RUN mkdir -p $DAGSTER_HOME

COPY ./location_integration_layer/requirements.txt ${DAGSTER_CWD}
RUN pip install -r requirements.txt

COPY ./location_integration_layer ${DAGSTER_CWD}

EXPOSE 4001
CMD ["dagster", "api", "grpc", "-f", "./definitions.py", "-h", "0.0.0.0", "-p", "4001"]