import logging
import sys
import grpc
import random
import chaosmonkey_pb2
import chaosmonkey_pb2_grpc
from utils import load_config
from utils import load_matrix
import numpy as np


class ChaosMonkey():
    def __init__(self, config_path):
        self.configs = load_config(config_path)

    def _upload_to_server(self, configs, matrix):
        # print('start uploading conn_matrix to other nodes')
        for ip, port in configs['nodes']:
            # print('Addr to connect: ' + ip + ":" + port)
            with grpc.insecure_channel(ip + ':' + port) as channel:
                stub = chaosmonkey_pb2_grpc.ChaosMonkeyStub(channel)
                response = stub.UploadMatrix(matrix)
                # print('Response from port' + str(port) + ":" + str(response.ret))

    def uploadMatrix(self, matrix_path):
        matrix_list = load_matrix(matrix_path)
        conn_matrix = chaosmonkey_pb2.ConnMatrix()
        for row in matrix_list:
            matrix_row = conn_matrix.rows.add()
            for col in row:
                matrix_row.vals.append(col)
        self._upload_to_server(self.configs, conn_matrix)

    def editMatrix(self, row, col, val):
        for ip, port in self.configs['nodes']:
            # print('Addr to connect: ' + ip + ":" + port)
            with grpc.insecure_channel(ip + ':' + port) as channel:
                stub = chaosmonkey_pb2_grpc.ChaosMonkeyStub(channel)
                response = stub.UpdateValue(chaosmonkey_pb2.MatValue(row=int(row), col=int(col), val=float(val)))
                # print('Response from port' + str(port) + ":" + str(response.ret))

    def get_current_connMatrix(self, node_index):
        # return if_succeed, matrix
        #   if_succeed: 0 for succeeded, 1 for failed
        ip, port = self.configs['nodes'][node_index]
        with grpc.insecure_channel(ip + ':' + port) as channel:
            stub = chaosmonkey_pb2_grpc.ChaosMonkeyStub(channel)
            try:
                response = stub.GetMatrix(chaosmonkey_pb2.Empty())
                matrix = list()
                for row in response.rows:
                    to_row = list()
                    for e in row.vals:
                        to_row.append(e)
                    matrix.append(to_row)
                return 0, matrix
            except Exception as e:
                return 1, list()

    def kill_a_node(self, node_id):
        # return if_succeed
        #   0 for succeeded, 1 for failed
        if node_id < 0 or node_id >= len(self.configs['nodes']):
            print('Invalid node_id.')
            return 1
        # for id in range(len(self.configs['nodes'])):
        #     self.editMatrix(id, node_id, 1.0)
        #     self.editMatrix(node_id, id, 1.0)

        all_correct = True
        for ip, port in self.configs['nodes']:
            with grpc.insecure_channel(ip + ':' + port) as channel:
                stub = chaosmonkey_pb2_grpc.ChaosMonkeyStub(channel)
                response = stub.KillANode(chaosmonkey_pb2.KillANodeRequest(node_index=node_id))
                if response.ret == 1:
                    all_correct = False
        if not all_correct:
            print('Kill node failed!')
        return 0

    def kill_a_node_randomly(self):
        # return if_succeed, node_killed
        #   if_succeed: 0 for succeeded, 1 for failed
        node_id = random.choice(range(len(self.configs['nodes'])))
        ret = self.kill_a_node(node_id)
        if ret == 0:
            return 0, node_id
        else:
            return 1, -1

    def revive_a_node(self, node_id):
        # return if_succeed
        #   0 for succeeded, 1 for failed
        if node_id < 0 or node_id >= len(self.configs['nodes']):
            print('Invalid node_id.')
            return 1
        for id in range(len(self.configs['nodes'])):
            # print('ID: {}, node: {}'.format(id, node_id))
            self.editMatrix(id, node_id, 0.0)
            # print('first edited')
            self.editMatrix(node_id, id, 0.0)
        return 0

    # def partition_network(self, num_of_nodes_with_leader):
    #     # make the first num_of_nodes_with_leader nodes(or num_of_nodes_with_leader - 1) in the same partition as the leader
    #     all_correct = True
    #     for ip, port in self.configs['nodes']:
    #         with grpc.insecure_channel(ip + ':' + port) as channel:
    #             stub = chaosmonkey_pb2_grpc.ChaosMonkeyStub(channel)
    #             response = stub.Partition(chaosmonkey_pb2.PartitionRequest(num_of_nodes_with_leader = num_of_nodes_with_leader))
    #             if response.ret == 1:
    #                 all_correct = False
    #     if not all_correct:
    #         return 1
    #     return 0


if __name__ == '__main__':
    logging.basicConfig()
    config_path = sys.argv[1]
    chaosmonkey = ChaosMonkey(config_path)

    operation = sys.argv[2]
    if operation == 'upload':
        matrix_path = sys.argv[3]
        chaosmonkey.uploadMatrix(matrix_path)
    elif operation == 'edit':
        row = sys.argv[3]
        col = sys.argv[4]
        val = sys.argv[5]
        chaosmonkey.editMatrix(row, col, val)
    elif operation == 'get':
        node_index = 0
        if len(sys.argv) == 4:
            node_index = int(sys.argv[3])
        ret = chaosmonkey.get_current_connMatrix(node_index)
        if ret[0] == 1:
            print('Failed!')
        else:
            mat = ret[1]
            mat = np.around(mat, 2)
            for row in mat:
                print(row)
    elif operation == 'kill':
        node_id = int(sys.argv[3])
        ret = chaosmonkey.kill_a_node(node_id)
        if ret == 0:
            print('Success!')
        else:
            print('Failed!')
    # elif operation == 'partition':
    #     num_of_nodes_with_leader = sys.argv[3]
    #     ret = chaosmonkey.partition_network(num_of_nodes_with_leader)
    #     if ret == 0:
    #         print('Partition Success!')
    #     else:
    #         print('Partition Failed!')
    else:
        print('Invalid opeartion')

    # example:
    # python chaos_client.py config.txt edit 2 1 0.5
