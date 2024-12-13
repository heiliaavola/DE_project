# Use the official MongoDB image as the base image
FROM mongo:latest

# Install Python and pip using apt
RUN apt-get update && apt-get install -y \
    python3-pip \
    python3-venv \
    && rm -rf /var/lib/apt/lists/*

# Create and activate virtual environment
ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Install Python packages in the virtual environment
RUN pip3 install --no-cache-dir \
    pymongo \
    pandas

# Create directory for initialization scripts
RUN mkdir -p /docker-entrypoint-initdb.d

# Set working directory
WORKDIR /data

# MongoDB default port
EXPOSE 27017

# Use the default entrypoint from mongo image
CMD ["mongod"]