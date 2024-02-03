FROM python:3.9-slim

ENV DAGSTER_HOME=/opt/dagster/dagster_home/
ENV DAGSTER_CWD=/opt/dagster/app/
# Need the Motherduck token when compiling the dbt project. Env_var is passed in at build time with a build arg defined in the docker-compose file

WORKDIR ${DAGSTER_CWD}

RUN mkdir -p $DAGSTER_HOME


COPY location_dbt_layer/requirements.txt ${DAGSTER_CWD}
RUN pip install -r ${DAGSTER_CWD}/requirements.txt
COPY dagster.yaml workspace.yaml  ${DAGSTER_CWD}

ARG MOTHERDUCK_PATH_ARG 
ENV MOTHERDUCK_PATH ${MOTHERDUCK_PATH_ARG}
ARG DUCKDB_DB_ARG 
ENV DUCKDB_DB ${DUCKDB_DB_ARG}
ARG ENVIRONMENT_ARG 
ENV ENVIRONMENT ${ENVIRONMENT_ARG}

COPY location_dbt_layer ${DAGSTER_CWD}/location_dbt_layer


RUN dbt --no-populate-cache compile --project-dir ./location_dbt_layer/dbt_transformations --profiles-dir ./location_dbt_layer/dbt_transformations --target ${ENVIRONMENT_ARG}

EXPOSE 4002
CMD ["dagster", "api", "grpc", "-f", "./location_dbt_layer/definitions.py", "-h", "0.0.0.0", "-p", "4002"]