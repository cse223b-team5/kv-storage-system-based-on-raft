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
input `get` followed by the `key` (shown as example1); to put a key/value pair into to dictionary, 
input `put` followed by the k/v pair (shown as example2); to debug the server, input `debug` followed by 
debug parameters and optional ip/port (shown as example3). When the input pattern does not follow the above
principles, it will output "Invalid operation".

**Chaosmonkey Client Request**

COMMAND:

+ `python chaos_client.py CONFIG OPERATION ARGVs`

The command allows interactively upload/modify the Connection Matrix (ConnMat) and kill a specified node 
to simulate the dynamic network conditions. Also, it can get the Connection Matrix (ConnMat) by setting the 
command parameter.

Examples:

1. `python chaos_client.py config.txt upload matrix`

2. `python chaos_client.py config.txt edit 3 4 0.6`

3. `python chaos_client.py config.txt get`

4. `python chaos_client.py config.txt kill 1`

To start a request from chaosmonkey client, run chaos_client.py and input parameters. To upload a matrix to
servers, input `upload` followed by the path of matrix file; to edit/update an entry in the Connection Matrix,
input `edit` followed by the position and value of target entry; to get the current Connection Matrix, input
`get`; to kill a specified server, input `kill` followed by node index. When the input pattern does not follow 
the above principles, it will output "Invalid operation".

**Static and Dynamic Test**

COMMAND:

+ `python test.py OPERATION`

+ `python concurrent_test.py OPERATION`

The command allows statically and dynamically test the performance and correctness of the distributed k/v storage
system. The tester sends hundreds of get requests or put requests within limited time. 

Examples:

1. `python test.py static`

2. `python test.py dynamic`

3. `python concurrent_test.py static`

4. `python concurrent_test.py dynamic`

To start the test, run test.py or concurrent_test.py and input parameters. To start a static test, input `static`; 
to start a dynamic test, input `dynamic`. When the input pattern does not follow the above principles, it will 
output "Invalid operation".







