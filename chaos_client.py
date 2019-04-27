import logging
import sys
import grpc
import random
import storage_service_pb2
import storage_service_pb2_grpc
import chaosmonkey_pb2
import chaosmonkey_pb2_grpc
from utils import load_config
from utils import load_matrix

global configs


def pickANodeRandomly():
    n = len(configs['nodes'])
    index = random.randint(0, n-1)
    print('response from server #:' + str(index))
    return configs['nodes'][index]


def get(key):
    ip, port = pickANodeRandomly()
    with grpc.insecure_channel(ip+':'+port) as channel:
        stub = storage_service_pb2_grpc.KeyValueStoreStub(channel)
        response = stub.Get(storage_service_pb2.GetRequest(key=key))
        if response.ret == 1:
            print(response.value)
        else:
            print('Failed!')


def put(key, value):
    ip, port = pickANodeRandomly()
    with grpc.insecure_channel(ip+':'+port) as channel:
        stub = storage_service_pb2_grpc.KeyValueStoreStub(channel)
        response = stub.Put(storage_service_pb2.PutRequest(key=key, value=value))
        if response.ret == 1:
            print('Success!')


def _upload_to_server(configs, matrix):
    print('start uploading conn_matrix to other nodes')
    for ip, port in configs['nodes']:
        print('Addr to connect: ' + ip + ":" + port)
        with grpc.insecure_channel(ip + ':' + port) as channel:
            stub = chaosmonkey_pb2_grpc.ChaosMonkeyStub(channel)
            response = stub.UploadMatrix(matrix)
            print('Response from port' + str(port) + ":" + str(response.ret))


def uploadMatrix(config_path, matrix_path):
    configs = load_config(config_path)
    matrix_list = load_matrix(matrix_path)
    conn_matrix = chaosmonkey_pb2.ConnMatrix()
    for row in matrix_list:
        matrix_row = conn_matrix.rows.add()
        for col in row:
            matrix_row.vals.append(col)
    _upload_to_server(configs, conn_matrix)


def editMatrix(config_path, row, col, val):
    configs = load_config(config_path)
    for ip, port in configs['nodes']:
        print('Addr to connect: ' + ip + ":" + port)
        with grpc.insecure_channel(ip + ':' + port) as channel:
            stub = chaosmonkey_pb2_grpc.ChaosMonkeyStub(channel)
            response = stub.UpdateValue(chaosmonkey_pb2.MatValue(row=int(row), col=int(col), val=float(val)))
            print('Response from port' + str(port) + ":" + str(response.ret))


if __name__ == '__main__':
    logging.basicConfig()
    operation = sys.argv[1]
    config_path = sys.argv[2]
    if operation == 'upload':
        matrix_path = sys.argv[3]
        uploadMatrix(config_path, matrix_path)
    elif operation == 'edit':
        row = sys.argv[3]
        col = sys.argv[4]
        val = sys.argv[5]
        editMatrix(config_path, row, col, val)
    else:
        print('Invalid opeartion')
