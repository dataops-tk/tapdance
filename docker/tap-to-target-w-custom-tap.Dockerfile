ARG tap_alias
ARG target_alias=target-csv
ARG source_image_suffix

FROM dataopstk/tapdance:target-${target_alias}${source_image_suffix} as target

ARG tap_alias
ARG target_alias
ARG source_image_suffix

RUN echo "Building from *custom* source tap image:  dataopstk/tapdance:${tap_alias}${source_image_suffix}"
RUN echo "Building from source target image:  dataopstk/tapdance:${target_alias}${source_image_suffix}"

FROM dataopstk/tapdance:tap-${tap_alias}${source_image_suffix} as tap

ARG tap_alias
ARG target_alias
ARG source_image_suffix

COPY --from=target /venv/target-${target_alias} /venv/target-${target_alias}

ENV PATH="/venv/target-${target_alias}:${PATH}"

# Check that both plugins are running and on the PATH
RUN if [ ! -e $(which tap-${tap_alias}) ]; then \
    echo "ERROR: count not find tap-${tap_alias} on path" && \
    exit 1; \
    fi;
RUN if [ ! -e $(which target-${target_alias}) ]; then \
    echo "ERROR: count not find target-${target_alias} on path" && \
    exit 1; \
    fi;

# Fails on postgres (missing apt-get packages)
# RUN tap-${tap_alias} --help && \
#     target-${target_alias} --help

ENTRYPOINT []
CMD [ "tapdance sync ${tap_alias} --config_file=.secrets/${tap_alias}-config.json ${target_alias} --target_config_file=.secrets/${target_alias}-config.json" ]
