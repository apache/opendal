#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -e

# Update apt repo
sudo apt update

# Setup for ruby binding
sudo apt install -y ruby-dev libclang-dev

# Setup for python binding
sudo apt install -y python3-dev python3-pip python3-venv

# Setup for nodejs binding
curl -fsSL https://deb.nodesource.com/setup_lts.x | sudo -E bash - && sudo apt-get install -y nodejs
sudo corepack enable
corepack prepare yarn@stable --activate

# Setup for java binding
sudo apt install -y default-jdk
echo "export JAVA_HOME=$(readlink -f /usr/bin/java | sed "s:/bin/java::")" | sudo tee /etc/profile.d/java_home.sh
sudo ln -s /usr/lib/jvm/default-java /usr/lib/jvm/default

# Setup for C binding
sudo apt install -y libgtest-dev cmake
cd /usr/src/gtest
sudo cmake CMakeLists.txt
sudo make
sudo cp lib/*.a /usr/lib
sudo ln -s /usr/lib/libgtest.a /usr/local/lib/libgtest.a
sudo ln -s /usr/lib/libgtest_main.a /usr/local/lib/libgtest_main.a

# Setup for Zig binding
sudo apt install -y wget
wget -q https://github.com/marler8997/zigup/releases/download/v2022_08_25/zigup.ubuntu-latest-x86_64.zip
unzip zigup.ubuntu-latest-x86_64.zip -d /usr/bin
chmod +x /usr/bin/zigup
zigup master #TODO: replace to 0.11.0 (stable)