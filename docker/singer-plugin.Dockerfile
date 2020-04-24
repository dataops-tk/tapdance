ARG source_image=python:3.8
FROM ${source_image}

#anything other than false will trigger a pre-release build
ARG prerelease=false

RUN pip install boto3 s3fs

RUN if [ "${prerelease}" = "false" ]; then \
    echo "Installing slalom.dataops libraries... " && \
    pip install --upgrade slalom.dataops; \
    else \
    echo "Installing pre-release slalom.dataops libraries... " && \
    pip install --upgrade --pre slalom.dataops; \
    fi

ARG PLUGIN_NAME=tap-pardot
ARG PLUGIN_SOURCE=${PLUGIN_NAME}
ARG PLUGIN_ALIAS=${PLUGIN_NAME}

# RUN s-tap install ${PLUGIN_NAME} --source=${PLUGIN_SOURCE} --alias=${PLUGIN_ALIAS}
RUN s-tap install ${PLUGIN_NAME} ${PLUGIN_SOURCE} ${PLUGIN_ALIAS}

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

CMD [ "s-tap plan ${PLUGIN_ALIAS} --config-file=.secrets/${PLUGIN_ALIAS}-config.json" ]
