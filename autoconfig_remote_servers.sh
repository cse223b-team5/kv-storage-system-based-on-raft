#!/bin/bash 

#ips="ec2-34-218-222-160.us-west-2.compute.amazonaws.com ec2-34-217-105-63.us-west-2.compute.amazonaws.com ec2-34-217-113-33.us-west-2.compute.amazonaws.com ec2-34-216-220-150.us-west-2.compute.amazonaws.com ec2-54-202-138-67.us-west-2.compute.amazonaws.com ec2-54-185-243-252.us-west-2.compute.amazonaws.com ec2-34-222-62-168.us-west-2.compute.amazonaws.com ec2-54-190-60-161.us-west-2.compute.amazonaws.com"

ips="ec2-34-222-121-74.us-west-2.compute.amazonaws.com ec2-34-217-108-155.us-west-2.compute.amazonaws.com ec2-52-37-113-151.us-west-2.compute.amazonaws.com ec2-34-220-172-239.us-west-2.compute.amazonaws.com"

start_one_server(){
	ip=$1
	port_start=$2
	num_of_nodes_in_each_aws=$3
	echo "in start_one_server"
	echo 
	echo "-----------------------------------------------------"
	echo $port_start
	echo $num_of_nodes_in_each_aws
	echo $ip
	echo "-----------------------------------------------------"
	expect -c "
		puts \"in expect script!!!\"

		set timeout -1

		spawn ssh -i cse223b-19sp-j4lu.pem ec2-user@$ip

		sleep 20

		send \"sudo yum -y install git\r\"
		sleep 5

		send \"rm -rf *kv*\r\"
		sleep 1

		send \"pkill python\r\"

		send \"git clone https://github.com/cse223b-team5/kv-storage-system-based-on-raft.git\r\"
		sleep 5

		send \"cd kv-storage-system-based-on-raft\r\"
		send \"ls\r\"

		send \"sudo yum -y install python3\r\"
		sleep 10
		send \"sudo python3 -m ensurepip --default-pip\r\"
		sleep 2
		send \"sudo python3 -m pip install --upgrade pip\r\"
		sleep 15
		send \"sudo python3 -m pip install grpcio --upgrade\r\"
		sleep 3
		send \"sudo python3 -m pip install grpcio-tools\r\"
                send \"exit\r\" 
                expect eof 
              
                spawn scp -i cse223b-19sp-j4lu.pem config.txt ec2-user@$ip:kv-storage-system-based-on-raft/
                sleep 3

                spawn ssh -i cse223b-19sp-j4lu.pem ec2-user@$ip
                sleep 20

	        send \"python3 utils.py $ip\r\"
		send \"sh start_server.sh\r\"
                sleep 3
		send \"exit\r\"

		expect eof
	"
}

i=0
num_of_nodes_in_each_aws=0
port_start=0
while read line
do
	line=(${line//:/})

	if [ $i == 1 ]
	then
	   num_of_nodes_in_each_aws=${line[1]}
	fi

	if [ $i == 2 ]
	then
	   port_start=${line[1]}
	fi

	let i+=1
done < config.txt

for ip in $ips; do
	echo "in for loop"
	#start_one_server $ip $port_start $num_of_nodes_in_each_aws 
	start_one_server $ip 5001 1 
done
