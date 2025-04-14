ARG BDAI_BASE_IMAGE="us-docker.pkg.dev/engineering-380817/bdai/bdai_maple:main"
FROM $BDAI_BASE_IMAGE AS builder

# Environment variables
ENV BASH_ENV="/root/.profile"
ENV BDAI="/workspaces/bdai"
ENV ROOT_HOME="/root"
ENV CUDA_HOME=/usr/local/cuda
ENV PATH=${CUDA_HOME}/bin:${PATH}
ENV LD_LIBRARY_PATH=${CUDA_HOME}/lib64:${LD_LIBRARY_PATH}
ENV FORCE_CUDA=1
ENV TORCH_CUDA_ARCH_LIST="6.0;6.1;7.0;7.5;8.0;8.6;9.0+PTX"

# Copy the key.txt file into the container
COPY key.txt /workspace/key.txt

FROM builder AS bdai_cli
# Configure git authentication with provided key
RUN gh auth login --with-token < /workspace/key.txt \
    && git config --global credential.helper '!gh auth git-credential'

# Clone and setup BDAI
RUN git clone https://github.com/bdaiinstitute/bdai.git $BDAI
WORKDIR $BDAI
RUN /bin/bash -c "$BDAI/scripts/setup_repo.sh"

FROM bdai_cli AS update_image
RUN /bin/bash -c "bdai update --clean --force"

FROM update_image AS tool_setup
# Install mcap CLI tool
RUN curl -L https://github.com/foxglove/mcap/releases/download/v0.0.53/mcap-linux-amd64 -o /usr/local/bin/mcap && \
    chmod +x /usr/local/bin/mcap

# Fix existing package issues and install libglew-dev
RUN apt-get update && apt-get install -y --fix-broken && \
    apt-get install -y libglew-dev=2.2.0-4 && \
    rm -rf /var/lib/apt/lists/*

# Install specific commit of Ruckig from source
RUN apt remove -y ros-humble-ruckig
RUN git clone https://github.com/pantor/ruckig.git && \
    cd ruckig && \
    git checkout 82c9c37004dce52e46b9514b295f929622de4c56 && \
    mkdir -p build && \
    cd build && \
    cmake -DCMAKE_BUILD_TYPE=Release .. && \
    make -j$(nproc) && \
    make install && \
    cd ../../ && \
    rm -rf ruckig

# Install CUDA 12.4
RUN apt-get update && apt-get install -y wget && \
    wget https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2204/x86_64/cuda-keyring_1.1-1_all.deb && \
    dpkg -i cuda-keyring_1.1-1_all.deb && \
    apt-get update && \
    apt-get install -y cuda-toolkit-12-4 && \
    rm cuda-keyring_1.1-1_all.deb && \
    rm -rf /var/lib/apt/lists/*

FROM tool_setup AS build_projects
# Add important environment setup
RUN echo ". /opt/ros/humble/setup.bash" >> $ROOT_HOME/.profile
RUN echo ". $BDAI/_build/install/local_setup.bash" >> $ROOT_HOME/.profile

# Setup library path
ENV LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/usr/lib/x86_64-linux-gnu/"
RUN ldconfig /usr/lib/x86_64-linux-gnu/

# Build packages
RUN /bin/bash -c "source /opt/ros/humble/setup.bash && bdai build-packages video_tools -n -1"

ENV CMAKE_POLICY_VERSION_MINIMUM=3.5
RUN /bin/bash -c "source /opt/ros/humble/setup.bash && bdai build-packages equidiff_ros data_collection -n -1 -sd"

FROM build_projects AS pytorch3d_setup
# Setup pytorch3d with PyTorch 2.4 compatible with CUDA 12.4
RUN pip uninstall -y torch torchvision pytorch3d && \
    pip install torch==2.4.1 torchvision==0.19.1 --index-url https://download.pytorch.org/whl/cu124 && \
    pip install 'fvcore==0.1.5.post20221221' 'iopath==0.1.10' && \
    FORCE_CUDA=1 pip install "git+https://github.com/facebookresearch/pytorch3d.git@v0.7.7"

CMD [ "bash" ]
