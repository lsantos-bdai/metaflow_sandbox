FROM us-docker.pkg.dev/engineering-380817/bdai/bdai_maple:main

# Environment variables
ENV BDAI="/workspaces/bdai"
ENV ROOT_HOME="/root"

# Copy the key.txt file into the container
COPY key.txt /workspace/key.txt

# Authenticate with GitHub and configure git
RUN gh auth login --with-token < /workspace/key.txt \
    && git config --global credential.helper '!gh auth git-credential'

# Install mcap CLI tool
RUN curl -L https://github.com/foxglove/mcap/releases/latest/download/mcap-linux-amd64 -o /usr/local/bin/mcap && \
    chmod +x /usr/local/bin/mcap

# Install system dependencies first
RUN apt-get update && apt-get install -y \
    nvidia-cuda-toolkit \
    nvidia-cuda-dev \
    libglew-dev \
    && rm -rf /var/lib/apt/lists/*

# Install PyTorch and other dependencies
RUN pip install --no-cache-dir \
    torch==2.1.0+cu121 \
    torchvision==0.16.0+cu121 \
    -f https://download.pytorch.org/whl/torch_stable.html \
    fvcore \
    iopath \
    ninja \
    setuptools

# CUDA setup
ENV FORCE_CUDA=1
ENV TORCH_CUDA_ARCH_LIST="6.0;6.1;7.0;7.5;8.0;8.6+PTX"

# Install PyTorch3D from source with specific compiler flags
RUN pip uninstall -y pytorch3d
RUN FORCE_CUDA=1 \
    CPATH=/usr/local/cuda/include:$CPATH \
    git clone https://github.com/facebookresearch/pytorch3d.git && \
    cd pytorch3d && \
    git checkout v0.7.7 && \
    FORCE_CUDA=1 \
    TORCH_CUDA_ARCH_LIST="6.0;6.1;7.0;7.5;8.0;8.6+PTX" \
    pip install -e .

# Clone and setup BDAI
RUN git clone https://github.com/bdaiinstitute/bdai.git /workspaces/bdai
WORKDIR /workspaces/bdai
RUN $BDAI/scripts/setup_repo.sh

# BDAI setup
RUN echo "y" | bdai clean
RUN echo "y" | bdai update -co
RUN bdai update --force

# Build packages
RUN /bin/bash -c "source /opt/ros/humble/setup.bash && bdai build-packages video_tools -n -1"
RUN /bin/bash -c "source /opt/ros/humble/setup.bash && bdai build-packages equidiff_ros data_collection -n -1"

# Add important environment setup from the second Dockerfile
RUN echo ". /opt/ros/humble/setup.sh" >> $ROOT_HOME/.profile
RUN echo ". $BDAI/_build/install/local_setup.sh" >> $ROOT_HOME/.profile
ENV BASH_ENV="/root/.profile"

# Setup library path
ENV LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/usr/lib/x86_64-linux-gnu/"
RUN ldconfig /usr/lib/x86_64-linux-gnu/

CMD [ "bash" ]
