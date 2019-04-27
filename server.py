from concurrent import futures
import time
import sys
import logging
import random
import threading
import functools
import grpc
import storage_service_pb2
import storage_service_pb2_grpc
import chaosmonkey_pb2
import chaosmonkey_pb2_grpc
from utils import load_config
from chaos_server import ChaosServer
from logging import Logger, StreamHandler, Formatter


lock_append_log = threading.Lock()
lock_ae_succeed_cnt = threading.Lock()
lock_decrement_nextIndex = threading.Lock()
lock_try_extend_nextIndex = threading.Lock()
lock_try_extend_matchIndex = threading.Lock()


def synchronized(lock):
    """ Synchronization decorator """
    def wrap(f):
        @functools.wraps(f)
        def new_function(*args, **kw):
            with lock:
                return f(*args, **kw)
        return new_function
    return wrap


_ONE_DAY_IN_SECONDS = 60 * 60 * 24


class StorageServer(storage_service_pb2_grpc.KeyValueStoreServicer):
    def __init__(self, config_path, myIp, myPort):
        self.configs = load_config(config_path)
        self.myIp = myIp
        self.myPort = myPort
        self.storage = {}
        self.term = 0
        self.logger = self.set_log()
        self.node_index = self.get_node_index_by_addr(myIp, myPort)

        self.currentTerm = 0
        self.voteFor = 0
        self.log = list()  # [(k,v)] list of tuples
        self.log_term = list()  # []
        self.commitIndex = 0 # has been committed
        self.lastApplied = 0
        self.nextIndex = list()  # has not yet been appended
        self.matchIndex = list()  # has been matched
        # TODO: call a function to properly initialize these variables

        self.leaderIndex = 0
        self.last_commit_history = dict() # client_id -> (serial_no, result)
        self.ae_succeed_cnt = 0
        self.apply_thread = None

    def init_states(self):
        self.revoke_apply_thread()

    @synchronized(lock_append_log)
    def append_to_local_log(self, key, value):
        self.log.append((key, value))

    @synchronized(lock_ae_succeed_cnt)
    def increment_ae_succeed_cnt(self):
        self.ae_succeed_cnt += 1

    @synchronized(lock_decrement_nextIndex)
    def decrement_nextIndex(self, node_index):
        self.node_index[node_index] -= 1

    @synchronized(lock_try_extend_nextIndex)
    def try_extend_nextIndex(self, node_index, new_nextIndex):
        if new_nextIndex > self.nextIndex[node_index]:
            self.nextIndex[node_index] = new_nextIndex

    @synchronized(lock_try_extend_matchIndex)
    def try_extend_matchIndex(self, node_index, new_matchIndex):
        if new_matchIndex > self.matchIndex[node_index]:
            self.matchIndex[node_index] = new_matchIndex

    def check_is_leader(self):
        return self.node_index == self.leaderIndex

    def get_node_index_by_addr(self, ip, port):
        index = 0
        for t in self.configs['nodes']:
            if t[0] == ip and t[1] == port:
                break
            index += 1
        return index

    def get_leader_ip_port(self):
        ip = self.configs['node'][self.leaderIndex][0]
        port = self.configs['node'][self.leaderIndex][1]
        return ip, port

    def Get(self, request, context):
        if self.check_is_leader():
            ip, port = self.get_leader_ip_port()
            return storage_service_pb2.PutResponse(leader_ip=ip, leader_port=port, ret=1)
        # no need to sync entries
        self.heartbeat_once_to_all(False)
        if self.check_is_leader() and request.key in self.storage:
            return storage_service_pb2.GetResponse(value=str(self.storage[request.key]), ret=0)
        else:
            return storage_service_pb2.GetResponse(ret=1)

    def heartbeat_once_to_one(self, ip, port, node_index, is_sync_entry):
        self.logger.info("sending heartbeat to node_{}_{}_{}".format(node_index, ip, port))
        with grpc.insecure_channel(ip + ':' + port) as channel:
            stub = storage_service_pb2_grpc.KeyValueStoreStub(channel)
            request, new_nextIndex = self.generate_append_entry_request(node_index)
            try:
                response = stub.AppendEntries(request, timeout=self.configs['timeout'])
            except TimeoutError:
                self.logger.error("Timeout error when heartbeat to {}".format(node_index))
                return

            if response.success:
                self.update_nextIndex_and_matchIndex(node_index, new_nextIndex)
            elif response.failed_for_term:
                # TODO: step down to follower
                pass
            else:
                # AppendEntries failed because of log inconsistency
                if not is_sync_entry:
                    # when there is no need to sync entries to followers in heartbeart
                    return
                self.decrement_nextIndex(node_index)
                self.heartbeat_once_to_one(ip, port, node_index)

    def heartbeat_once_to_all(self, is_sync_entry):
        threads = []
        for ip_port_tuple, node_index in enumerate(self.configs['node']):
            ip = ip_port_tuple[0]
            port = ip_port_tuple[1]
            if ip == self.myIp:
                continue
            ae_thread = threading.Thread(target=self.heartbeat_once_to_one, args=(ip, port, node_index, is_sync_entry))
            threads.append(ae_thread)
            ae_thread.start()

        for t in threads:
            t.join()

    def generate_append_entry_request(self, node_index):
        request = storage_service_pb2.AppendEntriesRequest()
        request.term = self.currentTerm
        request.leaderId = self.node_index

        entry_start_index = self.nextIndex[node_index]
        request.prevLogIndex = entry_start_index - 1
        request.prevLogTerm = self.log_term[request.prevLogIndex]
        request.leaderCommit = self.commitIndex

        # here there is no new entry to send in some situations like heartbeat
        # just send empty entry
        new_nextIndex = len(self.log)
        if entry_start_index < len(self.log):
            es = self.log[entry_start_index:]
            for e in es:
                entry = request.entries.add()
                entry.key = str(e[0])
                entry.value = str(e[1])

            new_nextIndex = entry_start_index + len(es)
        return request, new_nextIndex

    def generate_request_to(self, ip, port):
        request = storage_service_pb2.AppendEntriesRequest()
        request.term = self.currentTerm
        request.leaderId = self.node_index

        node_index = self.get_node_index_by_addr(ip, port)
        entry_start_index = self.nextIndex[node_index]
        request.prevLogIndex = entry_start_index - 1
        request.prevLogTerm = self.log_term[request.prevLogIndex]
        es = self.log[entry_start_index:]
        for e in es:
            entry = request.entries.add()
            entry.key = str(e[0])
            entry.value = str(e[1])

        request.leaderCommit = self.commitIndex

        new_nextIndex = entry_start_index + len(es)
        return request, new_nextIndex

    def update_nextIndex_and_matchIndex(self, node_index, new_nextIndex):
        # since you can't tell if the 'success' reply from AE is covered by any later AE, you need to
        # manually pass the new_nextIndex as arg to this function
        self.try_extend_nextIndex(node_index, new_nextIndex)
        # same case for matchIndex
        self.try_extend_matchIndex(node_index, new_nextIndex - 1)

    def replicate_log_entries_to_one(self, ip, port):
        with grpc.insecure_channel(ip + ':' + port) as channel:
            stub = storage_service_pb2_grpc.KeyValueStoreStub(channel)
            request, new_nextIndex = self.generate_request_to(ip, port)
            try:
                response = stub.AppendEntries(request, timeout=1)
            except TimeoutError:
                # TODO: once timeout we suppose it has failed?
                return

            if response.success:
                self.increment_ae_succeed_cnt()
                receiver_index = self.get_node_index_by_addr(ip,port)
                self.update_nextIndex_and_matchIndex(receiver_index, new_nextIndex)
            elif response.failed_for_term:
                # TODO: step down to follower
                pass
            else:
                # AppendEntries failed because of log inconsistency
                receiver_index = self.get_node_index_by_addr(ip, port)
                self.decrement_nextIndex(receiver_index)
                self.replicate_log_entries_to_one(ip, port)

    def replicate_log_entries_to_all(self):
        for ip, port in self.configs['node']:
            ae_thread = threading.Thread(target=self.replicate_log_entries_to_one, args=(ip, port))
            ae_thread.start()

    def Put(self, request, context):
        #  check if current node is leader. if not, help client redirect
        if not self.check_is_leader():
            ip = self.configs['node'][self.leaderIndex][0]
            port = self.configs['node'][self.leaderIndex][1]
            return storage_service_pb2.PutResponse(leader_ip=ip, leader_port=port)

        # to guarantee the 'at-most-once' rule; check commit history
        client_id = str(context.peer())
        if client_id in self.last_commit_history:
            if request.serial_no == self.last_commit_history[client_id][0] and self.last_commit_history[client_id][1]:
                return storage_service_pb2.PutResponse(ret=0)

        self.append_to_local_log(request.key, request.value)

        self.replicate_log_entries_to_all()

        majority_cnt = len(self.configs['node']) // 2 + 1
        while self.ae_succeed_cnt < majority_cnt:
            if not self.check_is_leader():
                return storage_service_pb2.PutResponse(ret=1)
                # TODO: is redirecting to actual leader necessary?
            continue

        if self.ae_succeed_cnt < majority_cnt:
            # client request failed
            return storage_service_pb2.PutResponse(ret=1)

        # update commitIndex
        i = self.commitIndex + 1
        while True:
            cnt = self.matchIndex.count(i)
            if cnt < majority_cnt:
                break
            i += 1
        i -= 1  # i is the max number to have appeared for more than majority_cnt times
        if self.log[i] == self.currentTerm and self.check_is_leader():
            self.commitIndex = i

        # apply to state machine

        # record in history
        self.last_commit_history[client_id] = (request.serial_no, True)

        # respond to client
        return storage_service_pb2.PutResponse(ret=0)

    def update_commit_index(self):
        # TODO: generate the commit index based on match
        pass

    def set_log(self):
        logger = Logger(self.leaderIndex)
        # logger.addHandler(FileHandler("{}_{}.log".format(PERSISTENT_PATH_PREFIC, self.name)))
        ch = StreamHandler()
        ch.setFormatter(Formatter("%(asctime)s %(levelname)s %(message)s"))
        logger.addHandler(ch)
        logger.setLevel("INFO")
        return logger

    def write_to_state(self, log_index):
        k, v = self.log[log_index]
        self.storage[k] = v

    def apply(self):
        while True:
            if not self.check_is_leader():
                break
            if self.lastApplied < self.commitIndex:
                self.write_to_state(self.lastApplied)
                self.lastApplied += 1
            time.sleep(0.5)

    def revoke_apply_thread(self):
        if not self.apply_thread:
            self.apply_thread = threading.Thread(target=self.apply, args=())
        self.apply_thread.start()



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
