FROM docker.getcollate.io/openmetadata/server:1.3.0

USER root

# Install additional dependencies if needed
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Switch back to non-root user
USER openmetadata

# The default command from the base image will be used