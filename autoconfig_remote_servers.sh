#!/bin/bash 

ips="ec2-34-218-222-160.us-west-2.compute.amazonaws.com ec2-34-217-105-63.us-west-2.compute.amazonaws.com ec2-34-217-113-33.us-west-2.compute.amazonaws.com ec2-34-216-220-150.us-west-2.compute.amazonaws.com ec2-54-202-138-67.us-west-2.compute.amazonaws.com ec2-54-185-243-252.us-west-2.compute.amazonaws.com ec2-34-222-62-168.us-west-2.compute.amazonaws.com ec2-54-190-60-161.us-west-2.compute.amazonaws.com"

start_one_server(){
	ip=$1
	echo "-----------------------------------------------------"
	echo $ip
	echo "-----------------------------------------------------"
	expect -c "
		set timeout 20

		spawn ssh -i cse223b-19sp-j4lu.pem ec2-user@$ip

		send \"sudo yum -y install git\r\"
		sleep 5

		send \"rm -rf *cse*\r\"
		sleep 1

		send \"git clone https://github.com/cse223b-team5/kv-storage-system-based-on-raft.git\r\"
		sleep 5

		send \"cd kv-storage-system-based-on-raft\r\"
		send \"ls\r\"

		send \"sudo yum -y install python3\r\"
		sleep 10
		send \"sudo python3 -m ensurepip --default-pip\r\"
		sleep 2
		send \"sudo python3 -m pip install --upgrade pip\r\"
		sleep 20
		send \"sudo python3 -m pip install grpcio\r\"
		sleep 3
		send \"sudo python3 -m pip install grpcio-tools\r\"
		sleep 3

		send \"python3 server.py config.txt $ip 5001 &\r\"

		send \"exit\r\"

		expect eof
	"
}

for ip in $ips; do
	start_one_server $ip
done
