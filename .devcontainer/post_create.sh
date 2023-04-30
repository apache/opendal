#!/bin/bash

set -e

# Update apt repo
sudo apt update

# Setup for ruby binding
sudo apt install -y ruby-dev libclang-dev

# Setup for python binding
sudo apt install -y python3-dev

# Setup for nodejs binding
curl -fsSL https://deb.nodesource.com/setup_lts.x | sudo -E bash - && sudo apt-get install -y nodejs
sudo corepack enable
corepack prepare yarn@stable --activate
