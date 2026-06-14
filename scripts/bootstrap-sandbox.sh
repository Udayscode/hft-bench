#!/usr/bin/env bash
set -eo pipefail

echo "==> Bootstrapping HFT Bench Sandbox Dependencies..."
mkdir -p sandbox

# 1. Download Firecracker
FC_VERSION="v1.7.0"
if [ ! -f "sandbox/firecracker-bin" ]; then
    echo "--> Downloading Firecracker ${FC_VERSION}..."
    curl -fsSL "https://github.com/firecracker-microvm/firecracker/releases/download/${FC_VERSION}/firecracker-${FC_VERSION}-x86_64.tgz" | tar -xz
    mv "release-${FC_VERSION}-x86_64/firecracker-${FC_VERSION}-x86_64" sandbox/firecracker-bin
    chmod +x sandbox/firecracker-bin
    rm -rf "release-${FC_VERSION}-x86_64"
else
    echo "--> Firecracker already exists."
fi

# 2. Download Kernel
if [ ! -f "sandbox/vmlinux.bin" ]; then
    echo "--> Downloading minimal Linux kernel..."
    curl -fsSL -o sandbox/vmlinux.bin "https://s3.amazonaws.com/spec.ccfc.min/img/quickstart_guide/x86_64/kernels/vmlinux.bin"
else
    echo "--> Kernel already exists."
fi

# 3. Build RootFS
if [ ! -f "sandbox/rootfs.ext4" ]; then
    echo "--> Building Alpine Linux rootfs..."
    # Use docker to export a clean alpine filesystem
    docker pull alpine:latest
    CONTAINER_ID=$(docker create alpine:latest)
    docker export $CONTAINER_ID -o sandbox/alpine.tar
    docker rm $CONTAINER_ID

    # Create a 50MB ext4 image
    truncate -s 50M sandbox/rootfs.ext4
    mkfs.ext4 -F sandbox/rootfs.ext4
    
    mkdir -p /tmp/my-rootfs
    sudo mount -o loop sandbox/rootfs.ext4 /tmp/my-rootfs
    sudo tar -xf sandbox/alpine.tar -C /tmp/my-rootfs
    
    # Inject our custom init
    sudo bash -c 'cat << "EOF" > /tmp/my-rootfs/sbin/init
#!/bin/sh
mount -t proc none /proc
mount -t sysfs none /sys
mount -t tmpfs none /tmp
mount -t tmpfs none /run

# Mount the submission drive
mkdir -p /mnt/submission
mount /dev/vdb /mnt/submission

# Execute the submission run script
if [ -f /mnt/submission/run.sh ]; then
    cd /mnt/submission
    chmod +x run.sh
    ./run.sh
else
    echo "[GUEST] ERROR: /mnt/submission/run.sh not found."
fi

# Halt to trigger VM shutdown
reboot -f
EOF'
    sudo chmod +x /tmp/my-rootfs/sbin/init
    sudo umount /tmp/my-rootfs
    rm sandbox/alpine.tar
    echo "--> Rootfs created successfully."
else
    echo "--> Rootfs already exists."
fi

echo "==> Bootstrap complete! Sandbox is ready."
