# docker build -f tap-mssql.Dockerfile -t dataopstk/tapdance:tap-mssql .

FROM dataopstk/tapdance:tap-mssql-raw as tap


FROM python:3.7

ARG prerelease=false

RUN apt-get update && apt-get install -y default-jre
RUN apt-get update && apt-get install -y leiningen

COPY --from=tap /home/tap-mssql /venv/tap-mssql

WORKDIR /venv/tap-mssql

RUN pip install boto3 s3fs

# Install tapdance
RUN if [ "${prerelease}" = "false" ]; then \
    echo "Installing tapdance libraries... " && \
    pip install --upgrade tapdance; \
    else \
    echo "Installing pre-release tapdance libraries... " && \
    pip install --upgrade --pre tapdance; \
    fi

# RUN echo "#!/bin/bash" > tap-mssql && \
#     echo "cd /venv/tap-mssql" >> tap-mssql && \
#     echo "bin/tap-mssql \"$@\"" >> tap-mssql

ENV PATH "/venv/tap-mssql/bin:${PATH}"

# RUN chmod 770 ./tap-mssql
RUN tap-mssql

ENTRYPOINT [ "tap-mssql" ]
