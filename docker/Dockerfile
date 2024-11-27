FROM us-docker.pkg.dev/engineering-380817/batch-processing/maple_deploy@sha256:fd761e02867e0a77586ff584a4f285b5cd013ef6586d939216ffd7d2688b3bc6

# Install mcap CLI tool
RUN curl -L https://github.com/foxglove/mcap/releases/latest/download/mcap-linux-amd64 -o /usr/local/bin/mcap && \
    chmod +x /usr/local/bin/mcap

RUN bdai build bdai -n -1

# Install pytorch3d from source with CUDA support
RUN pip install --no-cache-dir git+https://github.com/facebookresearch/pytorch3d.git@v0.7.7
