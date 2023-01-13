# Monolithic Docker image for easy setup of an Aleph.im node in demo scenarios.

FROM ubuntu:20.04

ENV DEBIAN_FRONTEND noninteractive

# Install Python dependencies
RUN apt-get update && apt-get -y upgrade && apt-get install -y \
     python3 \
     python3-dev \
     python3-pip \
     python3-venv \
     build-essential \
     git && \
     rm -rf /var/lib/apt/lists/*

# Install system dependencies
RUN apt-get update && apt-get -y upgrade && apt-get install -y \
     libsnappy-dev \
     zlib1g-dev \
     libbz2-dev \
     libgflags-dev \
     liblz4-dev \
     libgmp-dev \
     libsecp256k1-dev \
     pkg-config \
     libssl-dev \
     libleveldb-dev \
     libyaml-dev && \
     rm -rf /var/lib/apt/lists/*

# ===  Create unprivileged users ===

# - User 'source' to install code and dependencies -
RUN useradd -s /bin/bash source
RUN mkdir /opt/venv
RUN chown source:source /opt/venv

RUN mkdir /opt/build
RUN chown source:source /opt/build
# - Installed Python libraries will be saved in this file
RUN touch /opt/build-frozen-requirements.txt
RUN chown source:source /opt/build-frozen-requirements.txt

# - User 'aleph' to run the code itself
RUN useradd -s /bin/bash aleph
RUN mkdir /opt/pyaleph
RUN chown aleph:aleph /opt/pyaleph

# === Install Python environment and dependencies ===
USER source

# Create virtualenv
RUN python3 -m venv /opt/venv

# Install pip
ENV PIP_NO_CACHE_DIR yes
RUN /opt/venv/bin/python3 -m pip install --upgrade pip wheel
ENV PATH="/opt/venv/bin:${PATH}"

# === Install CCN dependencies ===
# Install dependencies early to cache them and accelerate incremental builds.
COPY setup.cfg /opt/pyaleph/
COPY deployment/scripts /opt/pyaleph/deployment/scripts
RUN /opt/venv/bin/python3 /opt/pyaleph/deployment/scripts/extract_requirements.py /opt/pyaleph/setup.cfg -o /opt/build/requirements.txt
RUN /opt/venv/bin/pip install --no-cache-dir -r /opt/build/requirements.txt
RUN rm /opt/build/requirements.txt

# === Install the CCN itself ===
COPY setup.py /opt/pyaleph/
COPY src /opt/pyaleph/src
# Git data is used to determine the version of the CCN
COPY .git /opt/pyaleph/.git

USER root
RUN chown -R source:source /opt/pyaleph/

USER source
WORKDIR /opt/pyaleph

RUN /opt/venv/bin/pip install --no-cache-dir "."

# Save installed Python requirements for debugging
RUN /opt/venv/bin/pip freeze > /opt/build-frozen-requirements.txt

USER root
RUN chown -R aleph:aleph /opt/pyaleph/

USER aleph
ENTRYPOINT ["bash", "deployment/scripts/run_aleph_ccn.sh"]

# CCN API
EXPOSE 8000
