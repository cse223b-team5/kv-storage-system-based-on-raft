import chaosmonkey_pb2
import chaosmonkey_pb2_grpc
from utils import load_matrix


class ChaosServer(chaosmonkey_pb2_grpc.ChaosMonkeyServicer):
    def __init__(self):
        global conn_mat
        conn_mat = load_matrix('matrix')

    def UploadMatrix(self, request, context):
        global conn_mat

        mat = list()
        for row in request.rows:
            to_row = list()
            for e in row.vals:
                to_row.append(e)
            mat.append(to_row)

        conn_mat = mat
        return chaosmonkey_pb2.Status(ret=0)

    def UpdateValue(self, request, context):
        global conn_mat
        if request.row >= len(conn_mat) or request.col >= len(conn_mat):
            return chaosmonkey_pb2.Status(ret=1)
        conn_mat[request.row][request.col] = request.val
        return chaosmonkey_pb2.Status(ret=0)

    def GetMatrix(self, request, context):
        global conn_mat
        to_mat = chaosmonkey_pb2.ConnMatrix()
        for from_row in conn_mat:
            to_row = to_mat.rows.add()
            for element in from_row:
                to_row.vals.append(element)
        return to_mat

    def KillANode(self, request, context):
        global conn_mat
        node_index = request.node_index
        N = len(conn_mat)
        if node_index < 0 or node_index >= N:
            return chaosmonkey_pb2.Status(ret=1)
        for id in range(N):
            conn_mat[id][node_index] = 1.0
            conn_mat[node_index][id] = 1.0
        return chaosmonkey_pb2.Status(ret=0)
