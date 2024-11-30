FROM --platform=linux/amd64 nvidia/cuda:11.8.0-base-ubuntu22.04

# Set working directory
WORKDIR /opt/ml/code

# Install Python and basic dependencies
RUN apt-get update && apt-get install -y \
    python3-pip \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Create necessary directories
RUN mkdir -p /opt/ml/processing/input/code && \
    mkdir -p /opt/ml/processing/output && \
    chmod -R 777 /opt/ml/processing

# Copy files individually to maintain structure
COPY esgtools ./esgtools
COPY setup.py .
COPY lambda_requirements.txt .
COPY preprocessing.py .
COPY requirements.txt .

# Install Python packages from requirements.txt
RUN pip3 install -r requirements.txt

# Install the package
RUN pip3 install -e .

# Make both python3 and python available
RUN ln -sf /usr/bin/python3 /usr/bin/python

# Make everything accessible
RUN chmod -R 755 /opt/ml/code

# Optional: Print directory contents for debugging
RUN echo "Contents of /opt/ml/code:" && \
    ls -la /opt/ml/code && \
    echo "Python path:" && \
    python3 -c "import sys; print('\n'.join(sys.path))"

ENV PYTHONPATH=/opt/ml/code

CMD ["python3", "/opt/ml/code/preprocessing.py"]