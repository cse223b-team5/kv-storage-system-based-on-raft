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

_ONE_DAY_IN_SECONDS = 60 * 60 * 24


class StorageServer(storage_service_pb2_grpc.KeyValueStoreServicer):
    def __init__(self, config_path, myIp, myPort):
        self.configs = load_config(config_path)
        self.myIp = myIp
        self.myPort = myPort
        self.storage = {}

        self.currentTerm = 0
        self.votedFor = None
        self.log = list() # [(key, value)] list of tuples
        self.log_term = list()
        self.commitIndex = 0
        self.lastApplied = 0
        self.nextIndex = list() #key: ip, value: corresponding nextIndex
        self.matchIndex = list() #key: ip, value: corresponding matchIndex

        self.node_index = 0
        for t in self.configs['nodes']:
            if t[0] == myIp and t[1] == myPort:
                break
            self.node_index += 1

    def Get(self, request, context):
        if request.key in self.storage:
            return storage_service_pb2.GetResponse(value=str(self.storage[request.key]), ret=0)
        else:
            return storage_service_pb2.GetResponse(ret=1)

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

    def AppendEntries(self, request, context):
        # 1
        if request.term < self.currentTerm:
            return storage_service_pb2.AppendEntriesResponse(term = self.currentTerm, success = False, failed_for_term = True) # present leader is not real leader

        # 2
        if len(self.log) - 1 < request.prevLogIndex or len(self.log) - 1 >= request.prevLogIndex and self.log_term[request.prevLogIndex] != request.prevLogTerm:
            return storage_service_pb2.AppendEntriesResponse(term=self.currentTerm, success=False,
                                                             failed_for_term=False)  # inconsistency

        # 3
        i = len(self.log) - 1
        while i > request.prevLogIndex:
            self.log.pop(i)
            i -= 1

        # 4
        for entry in request.entries:
            self.log.append(entry)
            self.log_term.append(request.term)
            self.currentTerm = request.term

        # 5
        self.commitIndex = min(request.leaderCommit, len(self.log) - 1)

        return storage_service_pb2.AppendEntriesResponse(term=self.currentTerm, success=True)


    def RequestVote(self, request, context):
        # 1
        if request.term < self.currentTerm:
            return storage_service_pb2.RequestVoteResponse(term=self.currentTerm, voteGranted=False)

        # 2
        if self.votedFor != None:
            return storage_service_pb2.RequestVoteResponse(term=self.currentTerm, voteGranted=False)

        #server的最新term是比较log_term的最后一个entry还是currentTerm；request.term = self.log_term[len(self.log)-1]的情况
        if not (request.lastLogIndex > len(self.log) - 1 and request.term >= self.log_term[len(self.log)-1] \
            or request.lastLogIndex <= len(self.log) - 1 and request.term > self.log_term[request.lastLogIndex]):
            return storage_service_pb2.RequestVoteResponse(term=self.currentTerm, voteGranted=False)

        self.votedFor = request.candidateId
        self.currentTerm = request.term # 存疑，是否应该加
        return storage_service_pb2.RequestVoteResponse(term=self.currentTerm, voteGranted=True)



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
