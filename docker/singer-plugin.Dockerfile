ARG source_image=python:3.8
FROM ${source_image}

#anything other than false will trigger a pre-release build
ARG prerelease=false

RUN pip install boto3 s3fs

RUN if [ "${prerelease}" = "false" ]; then \
    echo "Installing tapdance libraries... " && \
    pip install --upgrade tapdance; \
    else \
    echo "Installing pre-release tapdance libraries... " && \
    pip install --upgrade --pre tapdance; \
    fi

ARG PLUGIN_NAME=tap-pardot
ARG PLUGIN_SOURCE=${PLUGIN_NAME}
ARG PLUGIN_ALIAS=${PLUGIN_NAME}

RUN tapdance install ${PLUGIN_NAME} ${PLUGIN_SOURCE} ${PLUGIN_ALIAS}

ENV PLUGIN_NAME=${PLUGIN_NAME} \
    PLUGIN_SOURCE=${PLUGIN_SOURCE} \
    PLUGIN_ALIAS=${PLUGIN_ALIAS} \
    VENV_VOLUME=/venv/${PLUGIN_ALIAS} \
    VENV_CMD=/venv/${PLUGIN_ALIAS}/bin/${PLUGIN_NAME}

RUN ln -s /venv/${PLUGIN_ALIAS}/bin/${PLUGIN_NAME} /venv/${PLUGIN_ALIAS}/${PLUGIN_ALIAS}
ENV PATH="/venv/${PLUGIN_ALIAS}:${PATH}"

# Check that the plugin is running and on the PATH
RUN test -e $(which ${PLUGIN_ALIAS}) || exit 1
# RUN ${PLUGIN_ALIAS} --help

CMD [ "tapdance plan ${PLUGIN_ALIAS} --config-file=.secrets/${PLUGIN_ALIAS}-config.json" ]
