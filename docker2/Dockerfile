FROM us-docker.pkg.dev/engineering-380817/bdai/bdai_maple:main


# Copy the key.txt file into the container
COPY key.txt /workspace/key.txt

# Authenticate with GitHub and configure git to use gh credentials
RUN gh auth login --with-token < /workspace/key.txt \
    && git config --global credential.helper '!gh auth git-credential'

# Install mcap CLI tool
RUN curl -L https://github.com/foxglove/mcap/releases/latest/download/mcap-linux-amd64 -o /usr/local/bin/mcap && \
    chmod +x /usr/local/bin/mcap

# Install NVIDIA CUB
RUN mkdir -p /usr/local/cub && \
    cd /usr/local/cub && \
    curl -L https://github.com/NVIDIA/cub/archive/refs/tags/2.1.0.tar.gz | tar -xz && \
    mv cub-2.1.0 cub
ENV CUB_HOME=/usr/local/cub/cub

# Set CUDA architecture flags and force CUDA build
ENV FORCE_CUDA=1
ENV TORCH_CUDA_ARCH_LIST="6.0;6.1;7.0;7.5;8.0;8.6+PTX"
ENV CUDA_HOME=/usr/local/cuda
ENV MAX_JOBS=4
ENV FORCE_CUDA=1
ENV PYTORCH3D_NO_NINJA=1

RUN pip install --no-cache-dir \
    torch \
    torchvision \
    fvcore \
    iopath \
    ninja \
    setuptools

RUN pip install --no-cache-dir pytorch3d || \
    pip install --no-cache-dir "git+https://github.com/facebookresearch/pytorch3d.git@v0.7.7"

RUN git clone https://github.com/bdaiinstitute/bdai.git /workspaces/bdai

WORKDIR /workspaces/bdai

# Set environment variables
ENV BDAI=/workspaces/bdai
ENV PATH=/root/.local/bin:$PATH

# Run the build script
RUN $BDAI/scripts/setup_repo.sh

RUN echo "y" | bdai clean
RUN echo "y" | bdai update -co
RUN bdai update --force

RUN /bin/bash -c "source /opt/ros/humble/setup.bash && bdai build-packages video_tools -n -1"
RUN /bin/bash -c "source /opt/ros/humble/setup.bash && bdai build-packages equidiff_ros data_collection -n -1"
