FROM debian:trixie-slim

ARG LEAN_TOOLCHAIN=leanprover/lean4:stable
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
      bash ca-certificates curl git \
      libgmp10 \
      unzip xz-utils zstd \
    && rm -rf /var/lib/apt/lists/*

# --- Install Lean toolchain (download via elan), then vendor it into /opt/lean ---
ENV ELAN_HOME=/tmp/elan
ENV PATH=${ELAN_HOME}/bin:$PATH
RUN curl -sSfL https://raw.githubusercontent.com/leanprover/elan/master/elan-init.sh \
      | sh -s -- -y --default-toolchain "${LEAN_TOOLCHAIN}" \
    && elan toolchain install "${LEAN_TOOLCHAIN}" \
    && LEAN_BIN="$(elan which lean)" \
    && [ -n "${LEAN_BIN}" ] \
    && [ -x "${LEAN_BIN}" ] \
    && TOOLCHAIN_DIR="$(dirname "$(dirname "${LEAN_BIN}")")" \
    && case "${TOOLCHAIN_DIR}" in "${ELAN_HOME}"/toolchains/*) ;; *) echo "Unexpected elan toolchain dir: ${TOOLCHAIN_DIR}" >&2; exit 1 ;; esac \
    && mkdir -p /opt/lean \
    && cp -a "${TOOLCHAIN_DIR}/." /opt/lean/ \
    && /opt/lean/bin/lean --version \
    && /opt/lean/bin/lake --version \
    && rm -rf "${ELAN_HOME}"
ENV PATH=/opt/lean/bin:$PATH

WORKDIR /app
COPY . .
RUN lake build

CMD ./.lake/build/bin/bv_check
