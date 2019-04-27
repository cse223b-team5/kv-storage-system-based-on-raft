#!/bin/bash

ips="ec2-52-32-163-134.us-west-2.compute.amazonaws.com
	ec2-52-34-48-36.us-west-2.compute.amazonaws.com
	ec2-35-164-213-69.us-west-2.compute.amazonaws.com
	ec2-52-25-35-204.us-west-2.compute.amazonaws.com
	ec2-34-216-70-235.us-west-2.compute.amazonaws.com"


for ip in $ips
do 
	#start_one_server $ip
	start_one_server $ip
done

python chaos_client.py upload config.txt matrix