#!/usr/bin/expect

set timeout 120

set ips [list "ec2-52-32-163-134.us-west-2.compute.amazonaws.com" \
	"ec2-52-34-48-36.us-west-2.compute.amazonaws.com" \
	"ec2-35-164-213-69.us-west-2.compute.amazonaws.com" \
	"ec2-52-25-35-204.us-west-2.compute.amazonaws.com" \
	"ec2-34-216-70-235.us-west-2.compute.amazonaws.com"]

proc start_one_server { ip } {
	spawn ssh -i "cse223b-19sp-j4lu.pem" ec2-user@$ip

	send "sudo yum -y install git\r"
	sleep 5

	send "rm -rf cse*\r"
	sleep 1

	send "git clone https://lujie1996:lu490603562@github.com/lujie1996/cse223b-project.git\r"
	send "cd cse223b-project\r"
	send "ls\r"

	send "sudo yum -y install python3\r"
	sleep 10
	send "sudo python3 -m ensurepip --default-pip\r"
	sleep 2
	send "sudo python3 -m pip install --upgrade pip\r"
	sleep 20
	send "sudo python3 -m pip install grpcio\r"
	sleep 3
	send "sudo python3 -m pip install grpcio-tools\r"
	sleep 3

	send "python3 server.py config.txt $ip 5001 &\r"

	send "exit\r"
}


foreach ip $ips {
	#start_one_server $ip
	start_one_server $ip
}


expect eof
# send "bash start_server.sh\r"
# send "python3 server.py config.txt localhost 5001\r"
# send "python3 client.py config.txt get 3"