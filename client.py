import logging
import sys
import grpc
import random
import storage_service_pb2
import storage_service_pb2_grpc
from utils import load_config

global configs


def get_leader_addr():
    return configs['node'][0]


def get(key):
    ip, port = get_leader_addr()
    with grpc.insecure_channel(ip+':'+port) as channel:
        stub = storage_service_pb2_grpc.KeyValueStoreStub(channel)
        response = stub.Get(storage_service_pb2.GetRequest(key=key))
        if response.ret == 0:
            print(response.value)
        else:
            print('Failed!')


def put(key, value):
    ip, port = get_leader_addr()
    with grpc.insecure_channel(ip+':'+port) as channel:
        stub = storage_service_pb2_grpc.KeyValueStoreStub(channel)
        try:
            response = stub.Put(storage_service_pb2.PutRequest(
                key=key, value=value, serial_no=str(random.randint(0,10000))))
            if response.ret == 0:
                print('Success!')
        except Exception as e:
            print('RPC call failed!\n' + str(e))


def debug_get_variable(variable):
    ip, port = get_leader_addr()
    with grpc.insecure_channel(ip + ':' + port) as channel:
        stub = storage_service_pb2_grpc.KeyValueStoreStub(channel)
        try:
            response = stub.DEBUG_GetVariable(storage_service_pb2.DEBUG_GetVariable_Resquest(variable=variable))
            print(response.value)
        except Exception as e:
            print('RPC call failed!\n' + str(e))


if __name__ == '__main__':
    logging.basicConfig()
    config_path = sys.argv[1]
    configs = load_config(config_path)
    operation = sys.argv[2]
    if operation == 'get':
        key = sys.argv[3]
        get(key)
    elif operation == 'put':
        key = sys.argv[3]
        value = sys.argv[4]
        put(key, value)
    elif operation == 'debug':
        variable = sys.argv[3]
        debug_get_variable(variable)
    else:
        print("Invalid operation")