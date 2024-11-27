FROM us-docker.pkg.dev/engineering-380817/batch-processing/maple_deploy@sha256:fd761e02867e0a77586ff584a4f285b5cd013ef6586d939216ffd7d2688b3bc6

# Install mcap CLI tool
RUN curl -L https://github.com/foxglove/mcap/releases/latest/download/mcap-linux-amd64 -o /usr/local/bin/mcap && \
    chmod +x /usr/local/bin/mcap

RUN bdai build bdai -n -1

# Install NVIDIA CUB
RUN mkdir -p /usr/local/cub && \
    cd /usr/local/cub && \
    curl -L https://github.com/NVIDIA/cub/archive/refs/tags/2.1.0.tar.gz | tar -xz && \
    mv cub-2.1.0 cub
ENV CUB_HOME=/usr/local/cub/cub

# Set CUDA architecture flags and force CUDA build
ENV FORCE_CUDA=1
ENV TORCH_CUDA_ARCH_LIST="6.0;6.1;7.0;7.5;8.0;8.6+PTX"

# Install dependencies first
RUN pip install --no-cache-dir \
    fvcore \
    iopath \
    ninja \
    setuptools

# Clone and install PyTorch3D 
RUN pip uninstall -y pytorch3d
RUN pip install --no-cache-dir git+https://github.com/facebookresearch/pytorch3d.git@v0.7.7
