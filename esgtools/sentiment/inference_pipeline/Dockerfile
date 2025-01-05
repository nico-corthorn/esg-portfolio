FROM --platform=linux/amd64 nvidia/cuda:12.1.0-cudnn8-runtime-ubuntu22.04

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
    mkdir -p /opt/ml/model && \
    chmod -R 777 /opt/ml/processing

# Copy files
COPY requirements.txt .

# Install Python packages (excluding esgtools)
RUN pip3 install --timeout 3000 -r requirements.txt

# Make both python3 and python available
RUN ln -sf /usr/bin/python3.9 /usr/bin/python

# Make everything accessible
RUN chmod -R 755 /opt/ml/code

# Set environment variables for CUDA
ENV NVIDIA_VISIBLE_DEVICES all
ENV NVIDIA_DRIVER_CAPABILITIES compute,utility
ENV PYTHONPATH=/opt/ml/code

# Optional: Print directory contents and CUDA info for debugging
RUN echo "Contents of /opt/ml/code:" && \
    ls -la /opt/ml/code && \
    echo "Python path:" && \
    python3 -c "import sys; print('\n'.join(sys.path))" && \
    echo "CUDA info:" && \
    python3 -c "import torch; print(f'CUDA available: {torch.cuda.is_available()}')"

COPY script_runner.py .
RUN chmod +x script_runner.py

# Create the serve command that SageMaker will look for
RUN echo '#!/bin/bash' > /usr/local/bin/serve && \
    echo 'python3 /opt/ml/code/script_runner.py serve "$@"' >> /usr/local/bin/serve && \
    chmod +x /usr/local/bin/serve

CMD ["python3", "script_runner.py"]