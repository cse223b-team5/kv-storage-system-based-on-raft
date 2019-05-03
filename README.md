# kv-storage-system-based-on-raft


A fault-tolerant distributed k/v storage system based on RAFT

Team member:
1. Jie Lu
1. Ling Hong
3. Shuwei Liang

Related site: http://cseweb.ucsd.edu/~gmporter/classes/sp19/cse223b/techreport/

## User Guide

**Start Servers** 

COMMAND: 

+ `bash start_server.sh`  start servers locally

Run start_server.sh to initialize the servers. Specifically, it i) starts 10
servers (specified by config.txt) and automatically configures their IPs and ports. It also ii) 
uploads the Connection Matrix (location: matrix) to all servers. One can modify the 
config.txt and matrix to simulate using servers with different configurations and expect
to see different performance.

+ `bash autoconfig_remote_servers.sh`  start servers remotely

Run autoconfig_remote_servers.sh to initialize the servers. Specifically, it i) starts 10
servers (specified by config.txt) on AWS and automatically configures their IPs and ports. It also ii) 
uploads the Connection Matrix (location: matrix) to all servers. One can modify the 
config.txt and matrix to simulate using servers with different configurations and expect
to see different performance.

**Client Request**

COMMAND:

+ `python client.py CONFIG OPERATION ARGVs`

Examples:

1. `python client.py config.txt put 3 2`
   
2. `python client.py config.txt get 3`

3. `python client.py config.txt debug all`

To start a request from client, run client.py and input parameters. To get the value of a certain key,
input `get` and followed by the `key` (shown as example1); to put a key/value pair into to dictionary, 
input `put` and followed by the k/v pair (shown as example2); to debug the server, input `debug` followed by 
debug parameters and optional ip/port (shown as example3). When the input pattern does not follow the above
priciples, it will output "Invalid operation".




