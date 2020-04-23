ARG tap_alias
ARG target_alias=target-csv
ARG source_image_suffix

FROM slalomggp/singer:tap-${tap_alias}${source_image_suffix} as tap

ARG tap_alias
ARG source_image_suffix

RUN echo "Building from source tap image:  slalomggp/singer:${tap_alias}${source_image_suffix}"


FROM slalomggp/singer:target-${target_alias}${source_image_suffix} as target

ARG tap_alias
ARG target_alias
ARG source_image_suffix

RUN echo "Building from source target image:  slalomggp/singer:${target_alias}${source_image_suffix}"

COPY --from=tap /venv/tap-${tap_alias} /venv/tap-${tap_alias}

ENV PATH="/venv/tap-${tap_alias}:${PATH}"

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

CMD [ "s-tap sync ${tap_alias} --config_file=.secrets/${tap_alias}-config.json ${target_alias} --target_config_file=.secrets/${target_alias}-config.json" ]
