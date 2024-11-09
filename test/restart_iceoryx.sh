#!/bin/bash

echo "Stopping all IceOryx processes..."
pkill -f iox-roudi
sleep 1

echo "Cleaning up IPC resources..."
sudo rm -rf /tmp/iceoryx2
sudo rm -rf /private/tmp/iox*
sudo rm -rf /tmp/iox*
sudo rm -rf /dev/shm/iox*

echo "Creating directories..."
sudo mkdir -p /tmp/iceoryx2/services
sudo mkdir -p /etc/iceoryx

echo "Setting up config..."
sudo tee /etc/iceoryx/roudi_config.toml > /dev/null << 'EOF'
[general]
version = 1
service_registry_capacity = 256

[[segment]]
name = "default"
size = 104857600  # 100MB

  [[segment.mempool]]
  size = 128
  count = 10000

  [[segment.mempool]]
  size = 1024
  count = 5000

  [[segment.mempool]]
  size = 16384
  count = 1000

  [[segment.mempool]]
  size = 131072
  count = 200

  [[segment.mempool]]
  size = 524288
  count = 50

  [[segment.mempool]]
  size = 1048576
  count = 30

  [[segment.mempool]]
  size = 4194304
  count = 10

[[segment.access]]
reader = ".*"
writer = ".*"
EOF

echo "Setting permissions..."
sudo chmod -R 777 /tmp/iceoryx2
sudo chown -R $(whoami) /tmp/iceoryx2
sudo chmod 644 /etc/iceoryx/roudi_config.toml

echo "Starting IceOryx daemon..."
iox-roudi &
sleep 2

echo "Setting final permissions..."
sudo chmod -R 777 /tmp/iceoryx2/services
sudo chown -R $(whoami) /tmp/iceoryx2/services

echo "Verifying IceOryx setup..."
if [ ! -d "/tmp/iceoryx2/services" ]; then
    echo "Error: IceOryx services directory not created"
    exit 1
fi

echo "IceOryx reset complete!"