#!/bin/bash
sudo yum -y install git
rm -rf cse*
git clone https://lujie1996:lu490603562@github.com/lujie1996/cse223b-project.git
cd cse223b-project
sudo yum -y install python3
sudo python3 -m ensurepip --default-pip
sudo python3 -m pip install --upgrade pip
sudo python3 -m pip install grpcio
sudo python3 -m pip install grpcio-tools
# python3 server.py config.txt ec2-52-32-163-134.us-west-2.compute.amazonaws.com 5001 &
# python3 server.py config.txt ec2-52-34-48-36.us-west-2.compute.amazonaws.com 5001 &
# python3 server.py config.txt ec2-35-164-213-69.us-west-2.compute.amazonaws.com 5001 &
# python3 server.py config.txt ec2-52-25-35-204.us-west-2.compute.amazonaws.com 5001 &
# python3 server.py config.txt ec2-34-216-70-235.us-west-2.compute.amazonaws.com 5001 &
