from concurrent import futures
import time
import sys
import logging
import random
import grpc
import storage_service_pb2
import storage_service_pb2_grpc
import chaosmonkey_pb2
import chaosmonkey_pb2_grpc
from utils import load_config
from chaos_server import ChaosServer
from logging import Logger, StreamHandler, Formatter


_ONE_DAY_IN_SECONDS = 60 * 60 * 24


class StorageServer(storage_service_pb2_grpc.KeyValueStoreServicer):
    def __init__(self, config_path, myIp, myPort):
        self.configs = load_config(config_path)
        self.myIp = myIp
        self.myPort = myPort
        self.leaderId = ""
        self.serverId = ""
        self.storage = {}
        self.term = 0
        self.logger = self.set_log()

        self.node_index = 0
        for t in self.configs['nodes']:
            if t[0] == myIp and t[1] == myPort:
                break
            self.node_index += 1

    def Get(self, request, context):
        if self.is_leader():
            return storage_service_pb2.GetResponse(leaderId=self.leaderId, ret=1)
        self.heartbeat_once()
        if self.is_leader() and request.key in self.storage:
            return storage_service_pb2.GetResponse(value=str(self.storage[request.key]), leaderId="", ret=0)
        else:
            return storage_service_pb2.GetResponse(ret=1)

    def heartbeat_once(self, node):
        self.logger.info("sending heartbeat to {}".format(node))
        try:
            term, success = self.conns[node].root.append_entries(self.term, self.serverId)
            if term > self.term:
                self.convert_to_follower(term)
        except Exception as e:
            self.logger.error(e)

    def heartbeat_once_to_all(self):



    def Put(self, request, context):
        global conn_mat
        try:
            conn_mat
            print('ConnMax exists')
        except Exception as e:
            print('ConnMax does not exist')
        self.storage[request.key] = request.value
        self.broadcast_to_all_nodes(request)
        return storage_service_pb2.PutResponse(ret=0)

    def is_leader(self):
        return self.leaderId == self.serverId

    def set_log(self):
        logger = Logger(self.serverId)
        # logger.addHandler(FileHandler("{}_{}.log".format(PERSISTENT_PATH_PREFIC, self.name)))
        ch = StreamHandler()
        ch.setFormatter(Formatter("%(asctime)s %(levelname)s %(message)s"))
        logger.addHandler(ch)
        logger.setLevel("INFO")
        return logger


class ChaosServer(chaosmonkey_pb2_grpc.ChaosMonkeyServicer):
    def UploadMatrix(self, request, context):
        global conn_mat
        conn_mat = request
        print('New ConnMat uploaded')
        return chaosmonkey_pb2.Status(ret=0)

    def UpdateValue(self, request, context):
        global conn_mat
        if request.row >= len(conn_mat.rows) or request.col >= len(conn_mat.rows[request.row].vals):
            return chaosmonkey_pb2.Status(ret=1)
        conn_mat.rows[request.row].vals[request.col] = request.val
        print('New edit to ConnMat')
        return chaosmonkey_pb2.Status(ret=0)


def serve(config_path, myIp, myPort):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    storage_service_pb2_grpc.add_KeyValueStoreServicer_to_server(StorageServer(config_path, myIp, myPort), server)
    chaosmonkey_pb2_grpc.add_ChaosMonkeyServicer_to_server(ChaosServer(), server)

    server.add_insecure_port(myIp+':'+myPort)
    try:
        server.start()
    except Exception as e:
        print('Server start failed!')
        print(str(e))

    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == '__main__':
    logging.basicConfig()
    config_path = sys.argv[1]
    myIp = sys.argv[2]
    myPort = sys.argv[3]
    serve(config_path, myIp, myPort)
