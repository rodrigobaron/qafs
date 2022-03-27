FROM openjdk:8

# Install Python
RUN \
    apt-get update && \
    apt-get install -y python3 python3-dev python3-pip && \
    rm -rf /var/lib/apt/lists/*

# Install PySpark and Numpy
RUN \
    python3 -m pip install --upgrade pip && \
    python3 -m pip install numpy && \
    python3 -m pip install pyspark

# Define working directory
WORKDIR /app
ADD . /app

RUN python3 -m pip install /app

# Define default command
CMD ["bash"]