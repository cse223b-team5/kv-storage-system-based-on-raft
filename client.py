import logging
import sys
import grpc
import random
import storage_service_pb2
import storage_service_pb2_grpc
from utils import load_config


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
                    if response.ip is not None and response.port is not None:
                        return 1, 0, (response.ip, response.port)
                    else:
                        return 2, 0, (0, 0)
                else:
                    return 3, 0, (0, 0)
            except Exception as e:
                return -1, 0, (0, 0)

    def put_once(self, key, value):
        # return if_succeed, (ip, port)
        #   if_succeed:
        #       0 for succeeded
        #       1 for redirect
        #       2 for unknown
        with grpc.insecure_channel(self.leader_ip + ':' + self.leader_port) as channel:
            stub = storage_service_pb2_grpc.KeyValueStoreStub(channel)
            try:
                response = stub.Put(storage_service_pb2.PutRequest(
                    key=key, value=value, serial_no=str(random.randint(0,10000))))
                if response.ret == 0:
                    return 0, (0, 0)  # if_succeed, (ip, port)
                elif response.ret == 1 and response.ip is not None and response.port is not None:
                    return 1, (response.ip, response.port)
                else:
                    return 2, (0, 0)
            except Exception as e:
                return -1, (0, 0)

    def debug_get_variable(self, variable):
        with grpc.insecure_channel(self.leader_ip + ':' + self.leader_port) as channel:
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
                print('Success!')
                return 0
            elif once_ret[0] == 1:
                self.leader_ip, self.leader_port = once_ret[1]
                continue
            elif once_ret[0] == 2:
                print('Unknown error!')
                return 1
            else:
                print('Connection failed!')
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
                print(once_ret[1])
                return 0, once_ret[1]
            elif once_ret[0] == 1:
                # redirect
                self.leader_ip, self.leader_port = once_ret[2]
                continue
            elif once_ret[0] == 2:
                print('Key doesn\'t exist!')
                return 1, 0
            elif once_ret[0] == 3:
                print('Unknown error!')
                return 2, 0
            else:
                print('Connection failed!')
                return 3, 0
        return 4, 0


if __name__ == '__main__':
    logging.basicConfig()
    config_path = sys.argv[1]
    client = Client(config_path)

    operation = sys.argv[2]
    if operation == 'get':
        key = sys.argv[3]
        client.get(key)
    elif operation == 'put':
        key = sys.argv[3]
        value = sys.argv[4]
        client.put(key, value)
    elif operation == 'debug':
        variable = sys.argv[3]
        client.debug_get_variable(variable)
    else:
        print("Invalid operation")