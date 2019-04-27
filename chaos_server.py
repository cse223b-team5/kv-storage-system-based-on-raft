from concurrent import futures
import time
import sys
import logging
import grpc
import chaosmonkey_pb2
import chaosmonkey_pb2_grpc


class ChaosServer(chaosmonkey_pb2_grpc.ChaosMonkeyServicer):
    def UploadMatrix(self, request, context):
        self.conn_mat = request
        return chaosmonkey_pb2.Status(ret=0)

    def UpdateValue(self, request, context):
        if request.row >= len(self.conn_mat.rows) or request.col >= len(self.conn_mat.rows[request.row].vals):
            return chaosmonkey_pb2.Status(ret=1)

        self.conn_mat.rows[request.row].vals[request.col] = request.val
        return chaosmonkey_pb2.Status(ret=0)