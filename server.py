from concurrent import futures
import time
import sys
import os
import logging
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

lock_persistent_operations = threading.Lock()
lock_decrement_nextIndex = threading.Lock()
lock_try_extend_nextIndex = threading.Lock()
lock_try_extend_matchIndex = threading.Lock()
lock_state_machine = threading.Lock()
lock_try_extend_commitIndex = threading.Lock()
lock_update_vote_cnt = threading.Lock()
# The memory operations on voteFor, log[]/log_term[], and state, and their persistency is protected by a single lock

PERSISTENT_PATH_PREFIC = "/tmp/223b_raft_"


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
        self.state = 0 # 0: follower, 1: candidate 2: leader
        self.voteFor = None
        self.currentTerm = 0
        self.node_index = self.get_node_index_by_addr(myIp, myPort)
        self.commitIndex = -1  # has been committed
        self.lastApplied = 0

        self.storage = {}
        self.log = list()  # [(k,v)] list of tuples
        self.log_term = list()  # []
        self.nextIndex = list()  # has not yet been appended
        self.matchIndex = list()  # has been matched

        self.leaderIndex = 0
        self.is_leader = (self.node_index == 0)
        self.last_commit_history = dict()  # client_id -> (serial_no, result)
        self.ae_succeed_cnt = 0
        self.voteCnt = 0

        self.logger = self.set_log()
        self.apply_thread = None
        self.heartbeat_timer = None  # used for convert_to_candidate and requestVote periodically
        self.election_timer = None # used for convert_to_candidate and requestVote periodically
        self._lock = threading.Lock()

        self.start()

    def init_states(self):
        history = self.load_history()
        if not history:
            self.logger.info("No history states stored locally")
        else:
            self.logger.info("Recover from history with role: {}".format(self.state))
            self.voteFor = history['voteFor']
            self.state = history['state']
            self.currentTerm = int(history['term'])
            self.log = history['log']

        log_size = len(self.log)
        for i in range(len(self.configs['nodes'])):
            self.nextIndex.append(log_size)
            self.matchIndex.append(-1)

    def start(self):
        self.init_states()
        if self.is_leader:
            self.logger.info('This node is leader.')
            self.set_heartbeat_timer()
        else:
            self.set_election_timer()
            self.revoke_apply_thread()

    def get_persist_path(self):
        return "{}_{}_persistent.txt".format(PERSISTENT_PATH_PREFIC, self.node_index)

    def set_heartbeat_timer(self):
        self.heartbeat_once_to_all()
        self.heartbeat_timer = threading.Timer(float(self.configs['heartbeat_timeout']), self.set_heartbeat_timer)
        self.heartbeat_timer.start()

    def set_election_timer(self):
        self.election_timer = threading.Timer(self.get_random_timeout(), self.convert_to_candidate)
        self.election_timer.start()

    def cancel_election_timer(self):
        if self.election_timer:
            self.election_timer.cancel()

    def reset_election_timer(self):
        if self.elec.tion_timer:
            self.election_timer.cancel()
        self.set_election_timer()

    def append_to_local_log(self, key, value):
        self.log.append((key, value))
        self.log_term.append(self.currentTerm)
        self.persist()
        # print('(port:{})Local log: {}'.format(self.myPort, str(self.log)))
        
    @synchronized(lock_persistent_operations)
    def update_voteFor(self, new_voteFor):
        if not self.voteFor:
            self.voteFor = new_voteFor
            self.persist()

    @synchronized(lock_persistent_operations)
    def update_state(self, new_state):
        self.state = new_state
        self.persist()

    @synchronized(lock_persistent_operations)
    def update_term(self, currentTerm):
        self.currentTerm = currentTerm
        self.persist()

    @synchronized(lock_update_vote_cnt)
    def update_vote_cnt(self, reset=False):
        if reset:
            self.voteCnt = 0
        else:
            self.voteCnt += 1

    @synchronized(lock_decrement_nextIndex)
    def decrement_nextIndex(self, node_index):
        self.next_index[node_index] -= 1

    @synchronized(lock_try_extend_nextIndex)
    def try_extend_nextIndex(self, node_index, new_nextIndex):
        if new_nextIndex > self.nextIndex[node_index]:
            self.nextIndex[node_index] = new_nextIndex

    @synchronized(lock_try_extend_matchIndex)
    def try_extend_matchIndex(self, node_index, new_matchIndex):
        if new_matchIndex > self.matchIndex[node_index]:
            self.matchIndex[node_index] = new_matchIndex

    @synchronized(lock_try_extend_commitIndex)
    def try_extend_commitIndex(self, new_commitIndex):
        if new_commitIndex > self.commitIndex:
            self.commitIndex = new_commitIndex

    @synchronized(lock_state_machine)
    def modify_state_machine(self, key, value):
        self.storage[key] = value
        self.lastApplied = self.commitIndex

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
        ip = self.configs['nodes'][self.leaderIndex][0]
        port = self.configs['nodes'][self.leaderIndex][1]
        return ip, port

    def DEBUG_GetVariable(self, request, context):
        response = storage_service_pb2.DEBUG_GetVariable_Response()

        if request.variable == 'matchIndex':
            response.value = str(self.matchIndex)
        elif request.variable == 'nextIndex':
            response.value = str(self.nextIndex)
        elif request.variable == 'log':
            response.value = str(self.storage)
        elif request.variable == 'all':
            response.value = str(self.__dict__)
        else:
            response.value = 'Invaid variable.'

        return response

    def Get(self, request, context):
        if not self.check_is_leader():
            ip, port = self.get_leader_ip_port()
            return storage_service_pb2.GetResponse(leader_ip=ip, leader_port=port, ret=1)
        # no need to sync entries
        self.heartbeat_once_to_all(False)
        # this is a synchronous function call, return when heartbeats to all nodes have returned
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
                response = stub.AppendEntries(request, timeout=float(self.configs['rpc_timeout']))
            except Exception:
                self.logger.error("Timeout error when heartbeat to {}".format(node_index))
                return

            if response.success:
                self.update_nextIndex_and_matchIndex(node_index, new_nextIndex)
                self.logger.info('Heartbeat to {} succeeded.'.format(node_index))
            elif response.failed_for_term:
                # TODO: step down to follower
                self.convert_to_follower(response.term)
            else:
                # AppendEntries failed because of log inconsistency
                if not is_sync_entry:
                    # when there is no need to sync entries to followers in heartbeart
                    return
                self.logger.error('When heartbeat to node{}, inconsistency detected.'.format(node_index))
                self.decrement_nextIndex(node_index)
                self.heartbeat_once_to_one(ip, port, node_index)

    def heartbeat_once_to_all(self, is_sync_entry=True):
        self.logger.info('Start sending heartbeat_once_to_all.')
        threads = []
        for node_index, ip_port_tuple in enumerate(self.configs['nodes']):
            ip = ip_port_tuple[0]
            port = ip_port_tuple[1]
            if ip == self.myIp and port == self.myPort:
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

        # print('log_term and preLogIndex: {},{}'.format(self.log_term, request.prevLogIndex))
        
        if request.prevLogIndex < 0:
            request.prevLogTerm = 0
        else:
            request.prevLogTerm = self.log_term[request.prevLogIndex]
        request.leaderCommit = self.commitIndex

        # here there is no new entry to send in some situations like heartbeat
        # just send empty entry
        new_nextIndex = len(self.log)
        if entry_start_index < len(self.log):
            es = self.log[entry_start_index:new_nextIndex]
            for e in es:
                entry = request.entries.add()
                entry.key = str(e[0])
                entry.value = str(e[1])

        return request, new_nextIndex

    def update_nextIndex_and_matchIndex(self, node_index, new_nextIndex):
        # since you can't tell if the 'success' reply from AE has already covered by any later AE, you need to
        # manually pass the new_nextIndex as arg to this function
        self.try_extend_nextIndex(node_index, new_nextIndex)
        # same case for matchIndex
        self.try_extend_matchIndex(node_index, new_nextIndex - 1)

    def replicate_log_entries_to_one(self, ip, port, receiver_index, ae_succeed_cnt, lock_ae_succeed_cnt):
        with grpc.insecure_channel(ip + ':' + port) as channel:
            stub = storage_service_pb2_grpc.KeyValueStoreStub(channel)
            request, new_nextIndex = self.generate_append_entry_request(receiver_index)
            try:
                response = stub.AppendEntries(request, timeout=float(self.configs['rpc_timeout']))
            except Exception:
                self.logger.info('AppendEntries call to node #'+str(receiver_index)+' timed out.')
                return

            if response.success:
                self.update_nextIndex_and_matchIndex(receiver_index, new_nextIndex)
                # increment ae_succeed_cnt
                with lock_ae_succeed_cnt:
                    ae_succeed_cnt[0] += 1
            elif response.failed_for_term:
                # TODO: step down to follower
                self.convert_to_follower(response.term)
            else:
                # AppendEntries failed because of log inconsistency
                self.decrement_nextIndex(receiver_index)
                self.replicate_log_entries_to_one(ip, port, receiver_index)

    def replicate_log_entries_to_all(self, ae_succeed_cnt):
        lock_ae_succeed_cnt = threading.Lock()
        for node_index, ip_port_tuple in enumerate(self.configs['nodes']):
            if node_index != self.node_index:
                ae_thread = threading.Thread(target=self.replicate_log_entries_to_one,
                    args=(ip_port_tuple[0], ip_port_tuple[1], node_index, ae_succeed_cnt, lock_ae_succeed_cnt))
                ae_thread.start()

    def Put(self, request, context):
        #  check if current node is leader. if not, help client redirect
        if not self.check_is_leader():
            ip = self.configs['nodes'][self.leaderIndex][0]
            port = self.configs['nodes'][self.leaderIndex][1]
            return storage_service_pb2.PutResponse(leader_ip=ip, leader_port=port)

        # to guarantee the 'at-most-once' rule; check commit history
        client_id = str(context.peer())
        if client_id in self.last_commit_history:
            if request.serial_no == self.last_commit_history[client_id][0] and self.last_commit_history[client_id][1]:
                return storage_service_pb2.PutResponse(ret=0)

        print('----------------------------------------------------------------------')

        self.append_to_local_log(request.key, request.value)

        ae_succeed_cnt = list()
        ae_succeed_cnt.append(0)  # use list so that when pass-by-reference
        self.replicate_log_entries_to_all(ae_succeed_cnt)

        majority_cnt = len(self.configs['nodes']) // 2 + 1
        while ae_succeed_cnt[0] < majority_cnt:
            if not self.check_is_leader():
                return storage_service_pb2.PutResponse(ret=1)
            continue

        if ae_succeed_cnt[0] < majority_cnt:
            # client request failed
            return storage_service_pb2.PutResponse(ret=1)

        # update commitIndex
        print('majority_cnt: {}'.format(majority_cnt))
        self.update_commit_index()

        # apply to state machine
        self.modify_state_machine(request.key, request.value)

        # record in history
        self.last_commit_history[client_id] = (request.serial_no, True)

        print('Leader matchIndex: ' + str(self.matchIndex))
        print('Leader nextIndex: ' + str(self.nextIndex))
        print(str(self.__dict__))
        print('----------------------------------------------------------------------')
        # respond to client
        return storage_service_pb2.PutResponse(ret=0)

    def update_commit_index(self):
        tmp_matchIndex = sorted(self.matchIndex)
        n = len(tmp_matchIndex)
        print('matchIndex:' + str(self.matchIndex))
        new_commitIndex = tmp_matchIndex[(n-1)//2]
        # i is the max number to have appeared for more than majority_cnt times
        if self.check_is_leader():
            print('Leader wants to update commitIndex to {}.'.format(new_commitIndex))
            self.try_extend_commitIndex(new_commitIndex)

    def set_log(self):
        logger = Logger(self.leaderIndex)
        # logger.addHandler(FileHandler("{}_{}.log".format(PERSISTENT_PATH_PREFIC, self.name)))
        ch = StreamHandler()
        ch.setFormatter(Formatter("%(asctime)s %(levelname)s %(message)s"))
        logger.addHandler(ch)
        logger.setLevel("ERROR")
        return logger

    def write_to_state(self, log_index):
        k, v = self.log[log_index]
        self.storage[k] = v

    def apply(self):
        while True:
            if self.check_is_leader():
                break
            if self.lastApplied <= self.commitIndex:
                self.write_to_state(self.lastApplied)
                self.lastApplied += 1
            time.sleep(0.02)

    def revoke_apply_thread(self):
        if not self.apply_thread:
            self.apply_thread = threading.Thread(target=self.apply, args=())
        self.apply_thread.start()
        
    def AppendEntries(self, request, context):
        self.logger.info('{} received AppendEntries call from node (term:{}, leaderId:{})'.format(
            self.node_index, request.term, request.leaderId))
        
        # print('Leader\'s commitIndex is: {}'.format(request.leaderCommit))

        # 1
        if request.term < self.currentTerm:
            return storage_service_pb2.AppendEntriesResponse(
                term=self.currentTerm, success=False, failed_for_term=True)  # present leader is not real leader

        # 2 when request.prevLogIndex < 0, it should be regarded as log consistent
        if len(self.log) - 1 < request.prevLogIndex or \
                len(self.log) - 1 >= request.prevLogIndex >= 0 and self.log_term[request.prevLogIndex] != request.prevLogTerm:
            return storage_service_pb2.AppendEntriesResponse(
                term=self.currentTerm, success=False, failed_for_term=False)  # inconsistency

        # TODO: step down to follower
        self.convert_to_follower(request.term)

        # if candidate or leader then step down to follower
        i = len(self.log) - 1
        while i > request.prevLogIndex:
            self.log.pop(i)
            i -= 1

        # 4
        self.currentTerm = request.term
        for entry in request.entries:
            self.append_to_local_log(entry.key, entry.value)

        # 5
        self.commitIndex = min(request.leaderCommit, len(self.log) - 1)
        return storage_service_pb2.AppendEntriesResponse(term=self.currentTerm, success=True)

    def RequestVote(self, request, context):
        # TODO: need to modify RequestVote() according to Ling's exposed_request_vote()
        # 1
        if request.term < self.currentTerm:
            return storage_service_pb2.RequestVoteResponse(term=self.currentTerm, voteGranted=False)

        # 2
        #server的最新term是比较log_term的最后一个entry还是currentTerm；request.term = self.log_term[len(self.log)-1]的情况
        if not (request.lastLogIndex > len(self.log) - 1 and request.term >= self.log_term[len(self.log)-1] \
            or request.lastLogIndex <= len(self.log) - 1 and request.term > self.log_term[request.lastLogIndex]):
            return storage_service_pb2.RequestVoteResponse(term=self.currentTerm, voteGranted=False)

        # TODO: step down to follower, call convert_to_follower() and update configuration variables
        self.votedFor = request.candidateId
        self.currentTerm = request.term # 存疑，是否应该加
        return storage_service_pb2.RequestVoteResponse(term=self.currentTerm, voteGranted=True)

    def persist(self):
        history = dict()
        history['term'] = self.currentTerm
        history['state'] = self.state
        history['voteFor'] = self.voteFor
        history['logTerm'] = self.log_term
        history['log'] = self.log

        history_str = str(history)
        with open(self.get_persist_path(), 'w') as f:
            f.write(history_str)
            f.flush()
            os.fsync(f.fileno())

    def load_history(self):
        # load history from  persistent file; check if the persistent file is valid
        history = None
        persistent_path = self.get_persist_path()
        if os.path.isfile(persistent_path):
            with open(persistent_path) as f:
                history = eval(f.read())
        return history

    def to_string(self):
        s = []
        s.append("term: {}".format(self.currentTerm))
        s.append("state: {}".format(self.state))
        s.append("voteFor: {}".format(self.voteFor))
        s.append("logTerm: {}".format(str(self.log_term)))
        s.append("log: {}".format(str(self.log)))
        return "\n".join(s)

    def convert_to_follower(self, term):
        self.logger.info('{} converts to follower in term {}'.format(self.node_index, term))

        # TODO: invoke Jie's stateIntnitialize function: currentTerm, count and persist, ensure atomization
        self.update_state(0)
        self.update_voteFor(None)

        # TODO: voteCnt reset
        self.persist()
        self.reset_election_timer()
        # if the previous state is leader, then set the timer
        if self.heartbeat_timer:
            self.heartbeat_timer.cancel()

    def convert_to_candidate(self):
        # TODO: increment term by 1, set voteCnt and state to 1, set voteFor to self.node_index
        self.persist()
        self.logger.info('{} converts to candidate in term {}'.format(self.node_index, self.currentTerm))
        self.reset_election_timer()
        self.ask_for_vote_to_all()

    def convert_to_leader(self):
        self.logger.info('{} converts to leader in term {}'.format(self.node_index, self.currentTerm))
        # TODO: add cancel_election_timer() function
        self.cancel_election_timer()
        # TODO: set state to 2
        self.persist()
        # TODO: is run_heartbeat_timer() same as set_heart_beat() which is not defined
        self.run_heartbeat_timer()

    def ask_for_vote_to_all(self):
        # send RPC requests to all other nodes
        for t in self.configs['nodes']:
            if t[0] == self.ip and t[1] == self.port:
                continue
            try:
                node_index = self.get_node_index_by_addr(t[0], t[1])
                requestVote_thread = threading.Thread(target=self.ask_for_vote_to_one, args=(t[0], t[1], node_index))
                requestVote_thread.start()
            except Exception as e:
                self.logger.error("node{} in term {} ask_for_vote error".format(self.node_index, self.currentTerm))

    def ask_for_vote_to_one(self, ip, port, node_index):
        self.logger.info('{} ask vote from {} in term {}'.format(self.node_index, node_index, self.currentTerm))
        # TODO: consider whether we need to consider the current state, 如果此时不是candidate，不requestVote
        with grpc.insecure_channel(ip + ':' + port) as channel:
            stub = storage_service_pb2_grpc.KeyValueStoreStub(channel)
            request = self.generate_RequestVote_request()
            try:
                response = stub.RequestVote(request, timeout=float(self.configs['rpc_timeout']))
            except Exception:
                self.logger.error("Timeout error when requestVote to {}".format(node_index))
                return
            if response.term < self.currentTerm:
                return
            elif request.term > self.currentTerm:
                self.convert_to_follower(request.term)
            else:
                if request.voteGranted:
                    # TODO: increment voteCnt by 1
                    #with self._lock:
                    #    self.cnt += 1
                    self.persist()
                    self.logger.info("get one vote from node {}, current voteCnt is {}".format(node_index, self.voteCnt))
                    majority_cnt = len(self.configs['nodes']) // 2 + 1
                    if self.state != 2 and self.voteCnt >= majority_cnt:
                        self.convert_to_leader()

    def generate_RequestVote_request(self):
        request = storage_service_pb2.RequestVoteRequest()
        request.term = self.currentTerm
        request.candidateId = self.node_index
        # consider the length of log equal to 0, i.e., no entries in candidate's log.
        request.lastLogIndex = len(self.log) - 1
        if request.lastLogIndex < 0:
            request.lastLogTerm = 0
        else:
            request.lastLogTerm = self.log_term[-1]
        return request


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
