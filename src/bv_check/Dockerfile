FROM debian:trixie-slim AS builder

ARG LEAN_TOOLCHAIN=leanprover/lean4:stable
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
      bash ca-certificates curl git \
      libgmp10 \
      unzip xz-utils zstd zsh \
    && rm -rf /var/lib/apt/lists/*

# Install Lean toolchain (via elan), then vendor it into /opt/lean.
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
RUN lake build \
    && install -D /app/.lake/build/bin/bv_check /out/usr/local/bin/bv_check \
    && install -D /opt/lean/lib/lean/libleanshared.so /out/opt/lean/lib/lean/libleanshared.so \
    && ldd /opt/lean/lib/lean/libleanshared.so \
      | awk '{ if ($1 ~ /^\/opt\/lean\/lib\//) print $1; else if ($2 == "=>" && $3 ~ /^\/opt\/lean\/lib\//) print $3 }' \
      | sort -u \
      | xargs -r -I{} sh -c 'install -D "{}" "/out{}"'

FROM debian:trixie-slim AS runtime

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
      libgmp10 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /out/usr/local/bin/bv_check /usr/local/bin/bv_check
COPY --from=builder /out/opt/lean/lib /opt/lean/lib
ENV LD_LIBRARY_PATH=/opt/lean/lib:/opt/lean/lib/lean

CMD ["/usr/local/bin/bv_check"]
