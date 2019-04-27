#!/usr/bin/expect

set timeout 120
# spawn scp -i cse223b-19sp-j4lu.pem -r cse223b-project-master ec2-user@35.164.96.70:.
spawn ssh -i cse223b-19sp-j4lu.pem ec2-user@ec2-52-34-48-36.us-west-2.compute.amazonaws.com
# spawn ssh -i cse223b-19sp-j4lu.pem ec2-user@35.164.96.70

send "sudo yum -y install git\r"
sleep 5

send "rm -rf cse*\r"
sleep 1

send "git clone https://lujie1996:lu490603562@github.com/lujie1996/cse223b-project.git\r"

send "cd cse223b-project\r"
send "ls\r"

send "sudo yum -y install python34\r"
sleep 10
send "sudo python3 -m ensurepip --default-pip\r"
sleep 2
send "sudo python3 -m pip install --upgrade pip\r"
sleep 20
send "sudo python3 -m pip install grpcio\r"
sleep 3
send "sudo python3 -m pip install grpcio-tools\r"
sleep 3

# send "bash start_server.sh\r"
# send "python3 server.py config.txt localhost 5001\r"
# send "python3 client.py config.txt get 3"


sleep 10
send "exit\r"



expect eof