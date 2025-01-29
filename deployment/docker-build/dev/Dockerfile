FROM ubuntu:22.04 as base

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update && apt-get -y upgrade && apt-get install -y software-properties-common
RUN add-apt-repository -y ppa:deadsnakes/ppa

# Runtime + build packages
RUN apt-get update && apt-get -y upgrade && apt-get install -y \
    git \
    libgmp-dev \
    libpq5 \
    python3.12

FROM base as builder

RUN openssl version
RUN cat /etc/ssl/openssl.cnf
RUN echo "$OPENSSL_CONF"

# Build-only packages
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    pkg-config \
    python3.12-dev \
    python3.12-venv \
    libpq-dev \
    software-properties-common

# Install Rust to build Python packages
RUN curl https://sh.rustup.rs > rustup-installer.sh
RUN sh rustup-installer.sh -y
ENV PATH="/root/.cargo/bin:${PATH}"

# Some packages (py-ed25519-bindings, required by substrate-interface) need the nightly
# Rust toolchain to be built at this time
RUN rustup default nightly

# Create virtualenv
RUN python3.12 -m venv /opt/venv

# Install pip
ENV PIP_NO_CACHE_DIR yes
RUN /opt/venv/bin/python3.12 -m pip install --upgrade pip wheel
ENV PATH="/opt/venv/bin:${PATH}"

WORKDIR /opt/pyaleph
COPY alembic.ini pyproject.toml ./
COPY LICENSE.txt README.md ./
COPY deployment/migrations ./deployment/migrations
COPY deployment/scripts ./deployment/scripts
COPY .git ./.git
COPY src ./src

RUN pip install -e .[linting]
RUN pip install hatch

FROM base

COPY --from=builder /opt/venv /opt/venv
COPY --from=builder /opt/pyaleph /opt/pyaleph

RUN apt-get update && apt-get install -y \
    libsodium23 \
    libsodium-dev \
    libgmp-dev

# OpenSSL 3 disabled some hash algorithms by default. They must be reenabled
# by enabling the "legacy" providers in /etc/ssl/openssl.cnf.
COPY ./deployment/docker-build/openssl.cnf.patch /etc/ssl/openssl.cnf.patch
RUN patch /etc/ssl/openssl.cnf /etc/ssl/openssl.cnf.patch

RUN mkdir /var/lib/pyaleph

ENV PATH="/opt/venv/bin:${PATH}"
WORKDIR /opt/pyaleph

RUN hatch build
ENTRYPOINT ["bash", "deployment/scripts/run_aleph_ccn.sh"]
