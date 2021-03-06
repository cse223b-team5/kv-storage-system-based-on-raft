import logging
import sys
import grpc
import random
import storage_service_pb2
import storage_service_pb2_grpc
from utils import load_config

PRINT_RESULT = False


class Client:
    def __init__(self, config_path):
        self.configs = load_config(config_path)
        self.leader_ip, self.leader_port = self.configs['nodes'][0]

    def get_once(self, key):
        # return if_succeed, value, (ip, port)
        #   if_succeed:
        #       0 for succeeded
        #       1 for redirect
        #       2 for key_not_exist
        #       3 for unknown
        with grpc.insecure_channel(self.leader_ip+':'+self.leader_port) as channel:
            stub = storage_service_pb2_grpc.KeyValueStoreStub(channel)
            try:
                response = stub.Get(storage_service_pb2.GetRequest(key=key))
                if response.ret == 0:
                    return 0, response.value, (0, 0)
                elif response.ret == 1:
                    if response.leader_ip != '' and response.leader_port != '':
                        return 1, 0, (response.leader_ip, response.leader_port)
                elif response.ret == 2:
                    return 2, 0, (0, 0)
                else:
                    return 3, 0, (0, 0)
            except Exception as e:
                print(str(e))
                return -1, 0, (0, 0)

    def put_once(self, key, value):
        # return if_succeed, (ip, port)
        #   if_succeed:
        #       0 for succeeded
        #       1 for redirect
        #       2 for unknown

        #   RPC Put():
        #   return if_succeeded
        #       1 for none_request
        #       2 for cur_server_is_not_leader, tailed with leader_ip, leader_port
        #       3 exceed_time_limit
        with grpc.insecure_channel(self.leader_ip + ':' + self.leader_port) as channel:
            stub = storage_service_pb2_grpc.KeyValueStoreStub(channel)
            try:
                response = stub.Put(storage_service_pb2.PutRequest(
                    key=key, value=value, serial_no=str(random.randint(0, 10000))))
                if response.ret == 0:
                    return 0, (0, 0)  # if_succeed, (ip, port)
                elif response.ret == 2 and response.leader_ip != '' and response.leader_port != '':
                    return 1, (response.leader_ip, response.leader_port)
                else:
                    return 2, (0, 0)
            except Exception as e:
                print(str(e))
                return -1, (0, 0)

    def debug_get_variable(self, variable, ip=None, port=None):
        if not ip:
            ip = self.leader_ip
            port = self.leader_port
        with grpc.insecure_channel(ip + ':' + port) as channel:
            stub = storage_service_pb2_grpc.KeyValueStoreStub(channel)
            try:
                response = stub.DEBUG_GetVariable(storage_service_pb2.DEBUG_GetVariable_Resquest(variable=variable))
                print(response.value)
            except Exception as e:
                print('RPC call failed!\n' + str(e))

    def put(self, key, value):
        # return if_succeed
        #   0 for succeeded,
        #   1 for unknown_error
        #   2 for connection_error
        #   3 for failed_after_many_attempts
        key = str(key)
        value = str(value)
        for _ in range(3):
            once_ret = self.put_once(key, value)
            if once_ret[0] == 0:
                if PRINT_RESULT:
                    print('Success!')
                return 0
            elif once_ret[0] == 1:
                # print('New leader addr is: {}, current leader is: {}:{}'.
                #       format(once_ret[1], self.leader_ip, self.leader_port),)
                self.leader_ip, self.leader_port = once_ret[1]
                continue
            elif once_ret[0] == 2:
                if PRINT_RESULT:
                    print('Unknown error!')
                return 1
            else:
                if PRINT_RESULT:
                    print('Connection failed! once_ret: {}'.format(once_ret))
                return 2
        return 3

    def get(self, key):
        # return if_succeed, value
        #   if_success:
        #       0 for succeeded
        #       1 for key_doesnt_exist
        #       2 for unknown_error
        #       3 for connection_error
        #       4 for failed_after_many_attempts
        key = str(key)
        for _ in range(3):
            once_ret = self.get_once(key)
            if once_ret[0] == 0:
                if PRINT_RESULT:
                    print(once_ret[1])
                return 0, once_ret[1]
            elif once_ret[0] == 1:
                # redirect
                self.leader_ip, self.leader_port = once_ret[2]
                continue
            elif once_ret[0] == 2:
                if PRINT_RESULT:
                    print('Key doesn\'t exist!')
                return 1, 0
            elif once_ret[0] == 3:
                if PRINT_RESULT:
                    print('Unknown error!')
                return 2, 0
            else:
                if PRINT_RESULT:
                    print('Connection failed! once_ret is: {}'.format(once_ret))
                return 3, 0
        print('Failed after many attempts!')
        return 4, 0

    def get_leader(self):
        # return leader_index, leader_ip, leader_port
        #   if_succeed: 0 for succeeded, 1 for failed
        # IMPORTANT: the leader this function returns might expire at the time it is returned.
        # If this function is called right after a Put/Get operation, it is expected that the leader is very up-to-date.
        leader_index = 0
        for ip, port in self.configs['nodes']:
            if ip == self.leader_ip and port == self.leader_port:
                break
            leader_index += 1
        return leader_index, self.leader_ip, self.leader_port

    def partition_network(self, num_of_nodes_with_leader):
        # make the first num_of_nodes_with_leader nodes(or num_of_nodes_with_leader - 1) in the same partition as the leader
        all_correct = True
        for ip, port in self.configs['nodes']:
            with grpc.insecure_channel(ip + ':' + port) as channel:
                stub = storage_service_pb2_grpc.KeyValueStoreStub(channel)
                response = stub.Partition(storage_service_pb2.PartitionRequest(num_of_nodes_with_leader = num_of_nodes_with_leader))
                if response.ret == 1:
                    all_correct = False
        if not all_correct:
            return 1
        return 0


if __name__ == '__main__':
    logging.basicConfig()
    config_path = sys.argv[1]
    client = Client(config_path)

    operation = sys.argv[2]
    if operation == 'get':
        key = sys.argv[3]
        if_print = sys.argv[4]
        client.get(key)
        if if_print == "true":
            PRINT_RESULT = True
        else:
            PRINT_RESULT = False
    elif operation == 'put':
        key = sys.argv[3]
        value = sys.argv[4]
        if_print = sys.argv[5]
        client.put(key, value)
        client.get(key)
        if if_print == "true":
            PRINT_RESULT = True
        else:
            PRINT_RESULT = False
    elif operation == 'debug':
        # python client.py config.txt debug all localhost 5001
        variable = sys.argv[3]
        if len(sys.argv) >= 5:
            ip = sys.argv[4]
            port = sys.argv[5]
            client.debug_get_variable(variable, ip, port)
        else:
            client.debug_get_variable(variable)
    elif operation == 'partition':
        num_of_nodes_with_leader = int(sys.argv[3])
        ret = client.partition_network(num_of_nodes_with_leader)
        if ret == 0:
            print('Partition Success!')
        else:
            print('Partition Failed!')
    else:
        print("Invalid operation")