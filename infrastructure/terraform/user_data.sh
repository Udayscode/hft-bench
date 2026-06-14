#!/bin/bash
set -e

echo "Starting IICPC HFT-Bench Worker Node Initialization..."

# 1. Install Dependencies
apt-get update
apt-get install -y git curl build-essential golang cargo acl

# 2. Setup Firecracker Networking (Bridge + NAT)
ip link add name br0 type bridge
ip addr add 172.16.0.1/24 dev br0
ip link set dev br0 up

# Enable IP Forwarding for the microVMs to reach the internet/control plane
sysctl -w net.ipv4.ip_forward=1
iptables -t nat -A POSTROUTING -o eth0 -j MASQUERADE
iptables -A FORWARD -m conntrack --ctstate RELATED,ESTABLISHED -j ACCEPT
iptables -A FORWARD -i br0 -o eth0 -j ACCEPT

# 3. Download Firecracker Binary
FC_VERSION="v1.7.0"
curl -L -o firecracker https://github.com/firecracker-microvm/firecracker/releases/download/$${FC_VERSION}/firecracker-$${FC_VERSION}-x86_64
chmod +x firecracker
mv firecracker /usr/local/bin/firecracker

# 4. Clone Platform Repository (In a real setup, pull pre-compiled binaries from S3)
mkdir -p /opt/iicpc
cd /opt/iicpc
git clone https://github.com/your-org/hft-bench.git .

# 5. Build Sandbox Orchestrator
cd services/sandbox-orchestrator
go build -o orchestrator .
mv orchestrator /usr/local/bin/

# 6. Build Bot-Fleet
cd ../bot-fleet
cargo build --release
mv target/release/bot-fleet /usr/local/bin/

# 7. Start the Worker Node Daemon via Systemd
cat <<EOF > /etc/systemd/system/hft-orchestrator.service
[Unit]
Description=HFT Sandbox Orchestrator
After=network.target

[Service]
ExecStart=/usr/local/bin/orchestrator
Environment="DATABASE_URL=${DATABASE_URL}"
Environment="KAFKA_BROKER=${REDPANDA_BROKERS}"
Restart=always
User=root

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable hft-orchestrator
systemctl start hft-orchestrator

echo "Worker Node Successfully Bootstrapped!"
