from concurrent import futures
import time
import sys
import os
import logging
import threading
import random
import functools
import grpc
import storage_service_pb2
import storage_service_pb2_grpc
import chaosmonkey_pb2
import chaosmonkey_pb2_grpc
from utils import load_config, load_matrix
from logging import Logger, StreamHandler, Formatter


PRINT_RESULT = False

# The memory operations on votedFor, log[]/log_term[], and state, and their persistency is protected by a single lock

conn_mat = None
# PERSISTENT_PATH_PREFIX = "/tmp/223b_raft_"
PERSISTENT_PATH_PREFIX  = "tmp_raft_"


def synchronized(lock):
    """ Synchronization decorator """
    def wrap(f):
        @functools.wraps(f)
        def new_function(*args, **kw):
            with lock:
                return f(*args, **kw)
        return new_function
    return wrap


# Note: This function is not used for RPC calls by ChaosMonkey client. It is only used among storage servers/clients.
def network(func):
    def wrapper_network(obj, request, context):
        sender_index = 0
        if isinstance(request, storage_service_pb2.AppendEntriesRequest):
            sender_index = request.senderIndex
        elif isinstance(request, storage_service_pb2.RequestVoteRequest):
            sender_index = request.candidateId

        if random.random() < float(conn_mat[sender_index][obj.node_index]):
            # TODO: what about the connection between clients and servers?
            # ignore this message
            time.sleep(1)
            return func(obj, None, context)
        else:
            # if random.random() < float(conn_mat[obj.node_index][sender_index]):
            #     time.sleep(1)

            # add delay for exp3.4
            # time.sleep(0.0)

            return func(obj, request, context)
    return wrapper_network


_ONE_DAY_IN_SECONDS = 60 * 60 * 24


class StorageServer(storage_service_pb2_grpc.KeyValueStoreServicer):
    def __init__(self, config_path, myIp, myPort):
        self.configs = load_config(config_path)
        self.myIp = myIp
        self.myPort = myPort
        self.state = 0  # 0: follower, 1: candidate 2: leader
        self.votedFor = None
        self.currentTerm = 0
        self.node_index = self.get_node_index_by_addr(myIp, myPort)
        self.commitIndex = -1  # has been committed
        self.lastApplied = 0

        self.storage = {}
        self.log = list()  # [(k,v)] list of tuples
        self.log_term = list()  # []
        self.nextIndex = list()  # has not yet been appended
        self.matchIndex = list()  # has been matched

        self.leaderIndex = -1
        self.is_leader = (self.node_index == self.leaderIndex)
        self.last_commit_history = dict()  # client_id -> (serial_no, result)
        self.ae_succeed_cnt = 0
        self.voteCnt = 0

        self.logger = self.set_log()
        self.apply_thread = None
        self.heartbeat_timer = None  # used for convert_to_candidate and requestVote periodically
        self.election_timer = None # used for convert_to_candidate and requestVote periodically

        # lock
        self.lock_persistent_operations = None
        self.lock_decrement_nextIndex = None
        self.lock_try_extend_nextIndex = None
        self.lock_try_extend_matchIndex = None
        self.lock_state_machine = None
        self.lock_try_extend_commitIndex = None
        self.lock_update_vote_cnt = None

        self.start()

    def init_locks(self):
        self.lock_persistent_operations = threading.Lock()
        self.lock_decrement_nextIndex = threading.Lock()
        self.lock_try_extend_nextIndex = threading.Lock()
        self.lock_try_extend_matchIndex = threading.Lock()
        self.lock_state_machine = threading.Lock()
        self.lock_try_extend_commitIndex = threading.Lock()
        self.lock_update_vote_cnt = threading.Lock()

    def init_states(self):
        history = self.load_history()
        if not history:
            self.logger.info("No history states stored locally")
        else:
            self.logger.info("Recover from history with role: {}".format(self.state))
            if 'votedFor' in history and history['votedFor']:
                self.votedFor = int(history['votedFor'])
            if 'state' in history:
                self.state = int(history['state'])
            if 'term' in history:
                self.currentTerm = int(history['term'])
            if 'log' in history:
                self.log = history['log']
            if 'logTerm' in history:
                self.log_term = history['logTerm']

        log_size = len(self.log)
        for i in range(len(self.configs['nodes'])):
            self.nextIndex.append(log_size)
            self.matchIndex.append(-1)

    def start(self):
        self.init_states()
        self.init_locks()
        if self.is_leader:
            self.logger.info('This node is leader.')
            self.set_heartbeat_timer()
        else:
            self.set_election_timer()
            self.evoke_apply_thread()

    def get_persist_path(self):
        return "{}_{}_persistent.txt".format(PERSISTENT_PATH_PREFIX, self.node_index)

    def set_heartbeat_timer(self):
        # send heartbeat as soon as one becomes to leader for heartbeat and also for updating commitIndex in leader
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
        if self.election_timer:
            self.election_timer.cancel()
        self.set_election_timer()

    def get_random_timeout(self):
        return random.randint(float(self.configs['election_timeout_start']), float(self.configs['election_timeout_end']))

    def append_to_local_log(self, key, value, term=None):
        self.log.append((key, value))
        if term:
            self.log_term.append(term)
        else:
            self.log_term.append(self.currentTerm)
        self.persist()
        # print('(port:{})Local log: {}'.format(self.myPort, str(self.log)))

    def update_persistent_values(self, **kwargs):
        # modify multiple variables within a lock to improve efficiency
        if "votedFor" in kwargs:
            self.votedFor = kwargs['votedFor']
        if "currentTerm" in kwargs:
            self.currentTerm = kwargs['currentTerm']
        if "state" in kwargs:
            self.state = kwargs['state']

        self.persist()

    #@synchronized(self.lock_persistent_operations)
    def update_votedFor(self, new_votedFor):
        with self.lock_persistent_operations:
            if not self.votedFor:
                self.votedFor = new_votedFor
                self.persist()

    #@synchronized(self.lock_persistent_operations)
    def update_state(self, new_state):
        with self.lock_persistent_operations:
            self.state = new_state
            self.persist()

    #@synchronized(lock_persistent_operations)
    def update_term(self, currentTerm):
        with self.lock_persistent_operations:
            self.currentTerm = currentTerm
            self.persist()

    #@synchronized(lock_update_vote_cnt)
    def update_vote_cnt(self, reset=False):
        with self.lock_update_vote_cnt:
            if reset:
                self.voteCnt = 0
            else:
                self.voteCnt += 1

    #@synchronized(lock_update_vote_cnt)
    def decrement_nextIndex(self, node_index):
        with self.lock_update_vote_cnt:
            self.nextIndex[node_index] -= 1

    #@synchronized(lock_try_extend_nextIndex)
    def try_extend_nextIndex(self, node_index, new_nextIndex):
        with self.lock_try_extend_nextIndex:
            if new_nextIndex > self.nextIndex[node_index]:
                self.nextIndex[node_index] = new_nextIndex

    #@synchronized(lock_try_extend_matchIndex)
    def try_extend_matchIndex(self, node_index, new_matchIndex):
        with self.lock_try_extend_matchIndex:
            if new_matchIndex > self.matchIndex[node_index]:
                self.matchIndex[node_index] = new_matchIndex

    #@synchronized(lock_try_extend_commitIndex)
    def try_extend_commitIndex(self, new_commitIndex):
        with self.lock_try_extend_commitIndex:
            if new_commitIndex > self.commitIndex:
                self.commitIndex = new_commitIndex

    #@synchronized(lock_state_machine)
    def modify_state_machine(self, key, value):
        with self.lock_state_machine:
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
        elif request.variable == 'conn_mat':
            response.value = str(conn_mat)
        elif request.variable == 'all':
            response.value = str(self.__dict__)
        else:
            response.value = 'Invaid variable.'

        return response

    def Get(self, request, context):
        # return if_succeed
        #   0 for succeeded, tailed with value
        #   1 for redirect, tailed with leader_ip and leader_port
        #   2 for key_not_found / none request
        if request is None:
            return storage_service_pb2.GetResponse(ret=2)

        if not self.check_is_leader():
            ip, port = self.get_leader_ip_port()
            return storage_service_pb2.GetResponse(leader_ip=ip, leader_port=port, ret=1)
        # no need to sync entries, ensure own leadership

        hb_success_error_cnt = list()
        hb_success_error_cnt.append(1) # heartbeat successfully
        hb_success_error_cnt.append(0) # heartbeat unsuccessfully

        hb_success_error_lock = threading.Lock()
        self.heartbeat_once_to_all(hb_success_error_cnt, hb_success_error_lock, False)

        majority_cnt = len(self.configs['nodes']) // 2 + 1
        start_time = time.time()
        while hb_success_error_cnt[0] < majority_cnt and hb_success_error_cnt[1] == 0:
            if not self.check_is_leader() or time.time() - start_time > 1:
                break
            continue

        # this is a synchronous function call, return when heartbeats to all nodes have returned
        if self.check_is_leader() and request.key in self.storage:
            return storage_service_pb2.GetResponse(value=str(self.storage[request.key]), ret=0)
        else:
            return storage_service_pb2.GetResponse(ret=2)

    def heartbeat_once_to_one(self, ip, port, node_index, hb_success_error_cnt, hb_success_error_lock, is_sync_entry=True):
        self.logger.info("sending heartbeat to node_{}_{}_{}".format(node_index, ip, port))
        with grpc.insecure_channel(ip + ':' + port) as channel:
            stub = storage_service_pb2_grpc.KeyValueStoreStub(channel)
            request, new_nextIndex = self.generate_append_entry_request(node_index)
            try:
                response = stub.AppendEntries(request, timeout=float(self.configs['rpc_timeout']))
            except Exception:
                self.logger.info("(Node#{})Timeout error when heartbeat to {}".format(self.node_index, node_index))
                return

            if response.success:
                self.update_nextIndex_and_matchIndex(node_index, new_nextIndex)
                self.logger.info('Heartbeat to {} succeeded.'.format(node_index))
                if hb_success_error_lock:
                    with hb_success_error_lock:
                        hb_success_error_cnt[0] += 1
            elif response.failed_for_term:
                # voter's term is larger than the leader, so leader changes to follower
                if hb_success_error_lock:
                    with hb_success_error_lock:
                        hb_success_error_lock[1] += 1
                self.convert_to_follower(response.term, node_index)
            else:
                # AppendEntries failed because of log inconsistency
                if not is_sync_entry:
                    # when there is no need to sync entries to followers in heartbeart
                    return
                self.logger.error('Node #{} When heartbeat to node{}, inconsistency detected.'.
                                  format(self.node_index, node_index))
                self.decrement_nextIndex(node_index)
                self.heartbeat_once_to_one(ip, port, node_index, hb_success_error_cnt, hb_success_error_lock)

    def heartbeat_once_to_all(self, hb_success_error_cnt=list(), hb_success_error_lock=None, is_sync_entry=True):
        self.logger.info('Start sending heartbeat_once_to_all.')
        for node_index, ip_port_tuple in enumerate(self.configs['nodes']):
            ip = ip_port_tuple[0]
            port = ip_port_tuple[1]
            if ip == self.myIp and port == self.myPort:
                continue
            ae_thread = threading.Thread(target=self.heartbeat_once_to_one, args=(ip, port, node_index, hb_success_error_cnt,
                                                                                  hb_success_error_lock, is_sync_entry))
            ae_thread.start()

    def generate_append_entry_request(self, node_index):
        request = storage_service_pb2.AppendEntriesRequest()
        request.term = self.currentTerm
        request.leaderId = self.node_index

        #with self.lock_persistent_operations:
        entry_start_index = self.nextIndex[node_index]
        request.prevLogIndex = entry_start_index - 1

        # print('len(self.log): {}, receiver with node_index: {}, nextIndex[node_index]: {}, request.prevLogIndex: {}.'.
        #       format(len(self.log), node_index, self.nextIndex[node_index], request.prevLogIndex))
        if request.prevLogIndex < 0:
            request.prevLogTerm = 0
        else:
            # print('Node #{}: len(log_term) and preLogIndex: {}, {}'.
            #       format(self.node_index, len(self.log_term), request.prevLogIndex))
            request.prevLogTerm = self.log_term[request.prevLogIndex]
        request.leaderCommit = self.commitIndex

        # here there is no new entry to send in some situations like heartbeat
        # just send empty entry
        new_nextIndex = len(self.log)
        if entry_start_index < len(self.log):
            es = self.log[entry_start_index:new_nextIndex]
            terms = self.log_term[entry_start_index:new_nextIndex]
            for i in range(len(es)):
                entry = request.entries.add()
                entry.key = str(es[i][0])
                entry.value = str(es[i][1])
                entry.term = int(terms[i])
        request.senderIndex = self.node_index
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
                self.convert_to_follower(response.term, receiver_index)
            else:
                # AppendEntries failed because of log inconsistency
                self.decrement_nextIndex(receiver_index)
                self.replicate_log_entries_to_one(ip, port, receiver_index, ae_succeed_cnt, lock_ae_succeed_cnt)

    def replicate_log_entries_to_all(self, ae_succeed_cnt):
        lock_ae_succeed_cnt = threading.Lock()
        for node_index, ip_port_tuple in enumerate(self.configs['nodes']):
            if node_index != self.node_index:
                ae_thread = threading.Thread(target=self.replicate_log_entries_to_one,
                    args=(ip_port_tuple[0], ip_port_tuple[1], node_index, ae_succeed_cnt, lock_ae_succeed_cnt))
                ae_thread.start()

    def Put(self, request, context):
        # return if_succeeded
        #   1 for none_request
        #   2 for cur_server_is_not_leader, tailed with leader_ip, leader_port
        #   3 exceed_time_limit
        # print('server {}: enter Put RPC'.format(self.node_index))
        if request is None:
            # print('server: empty request')
            return storage_service_pb2.PutResponse(ret=1)

        #  check if current node is leader. if not, help client redirect
        if not self.check_is_leader():
            # print('Currrent node: {}, leaderIndex: {}'.format(self.node_index, self.leaderIndex))
            ip = self.configs['nodes'][self.leaderIndex][0]
            port = self.configs['nodes'][self.leaderIndex][1]
            # print('server {}: redirect'.format(self.node_index))
            return storage_service_pb2.PutResponse(ret=2, leader_ip=ip, leader_port=port)

        # to guarantee the 'at-most-once' rule; check commit history
        client_id = str(context.peer())
        if client_id in self.last_commit_history:
            if request.serial_no == self.last_commit_history[client_id][0] and self.last_commit_history[client_id][1]:
                # print('server: already submitted')
                return storage_service_pb2.PutResponse(ret=0)

        self.append_to_local_log(request.key, request.value)

        ae_succeed_cnt = list()
        ae_succeed_cnt.append(1)  # use list so that when pass-by-reference
        self.replicate_log_entries_to_all(ae_succeed_cnt)

        majority_cnt = len(self.configs['nodes']) // 2 + 1

        start_time = time.time()
        while ae_succeed_cnt[0] < majority_cnt:
            if not self.check_is_leader():
                ip = self.configs['nodes'][self.leaderIndex][0]
                port = self.configs['nodes'][self.leaderIndex][1]
                # print('server: redirect')
                return storage_service_pb2.PutResponse(ret=2, leader_ip=ip, leader_port=port)
            elif time.time() - start_time > 1:
                # print('server time out.')
                return storage_service_pb2.PutResponse(ret=3)
            continue

        # update commitIndex
        self.update_commit_index()

        # apply to state machine
        self.modify_state_machine(request.key, request.value)

        # record in history
        self.last_commit_history[client_id] = (request.serial_no, True)

        if PRINT_RESULT:
            print('----------------------------------------------------------------------')
            for key in self.__dict__:
                print(key, self.__dict__[key])
            print('----------------------------------------------------------------------')

        # respond to client
        # print('Server: succeed!')
        return storage_service_pb2.PutResponse(ret=0)

    def update_commit_index(self):
        tmp_matchIndex = sorted(self.matchIndex)
        n = len(tmp_matchIndex)
        new_commitIndex = tmp_matchIndex[(n-1)//2]
        # i is the max number to have appeared for more than majority_cnt times
        if self.check_is_leader():
            self.try_extend_commitIndex(new_commitIndex)

    def set_log(self):
        logger = Logger(self.leaderIndex)
        # logger.addHandler(FileHandler("{}_{}.log".format(PERSISTENT_PATH_PREFIC, self.name)))
        ch = StreamHandler()
        ch.setFormatter(Formatter("%(asctime)s %(levelname)s %(message)s"))
        logger.addHandler(ch)
        logger.setLevel("WARNING")
        return logger

    def write_to_state(self, log_index):
        # print('Node: #{}, log len: {}, log_index: {}, commitIndex: {}'.
        #       format(self.node_index, len(self.log), log_index, self.commitIndex))
        k, v = self.log[log_index]
        self.storage[k] = v

    def apply(self):
        while True:
            if self.check_is_leader():
                break
            if self.lastApplied <= self.commitIndex and self.lastApplied < len(self.log):
                self.write_to_state(self.lastApplied)
                self.lastApplied += 1
            time.sleep(0.02)
        self.apply_thread = None

    def evoke_apply_thread(self):
        # apply thread only used in follower and leader so that to apply the logs in the background
        if not self.apply_thread:
            self.apply_thread = threading.Thread(target=self.apply, args=())
            self.apply_thread.start()

    @network
    def AppendEntries(self, request, context):
        # @network will pass a None request to simulate network connection failure
        # TODO: make use of delay(sleep) in replacement of the false response
        if request is None:
            return storage_service_pb2.AppendEntriesResponse(success=False)

        self.logger.info('{} received AppendEntries call from node (term:{}, leaderId:{})'.format(
            self.node_index, request.term, request.leaderId))
        with self.lock_persistent_operations:
            # 1 term failure( # present leader is not real leader)
            if request.term < self.currentTerm:
                return storage_service_pb2.AppendEntriesResponse(
                    term=self.currentTerm, success=False, failed_for_term=True)
            #if len(request.entries) == 0:
            #    return  storage_service_pb2.AppendEntriesResponse(term=self.currentTerm, success=True)
            # 2 when request.prevLogIndex < 0, it should be regarded as log consistent
            # same index with the same term can make sure the log is the same one, so just compare index & term is enough
            if len(self.log) - 1 < request.prevLogIndex or \
                    len(self.log) - 1 >= request.prevLogIndex >= 0 and \
                    self.log_term[request.prevLogIndex] != request.prevLogTerm:
                return storage_service_pb2.AppendEntriesResponse(
                    term=self.currentTerm, success=False, failed_for_term=False)  # inconsistency

            i = len(self.log) - 1
            while i > request.prevLogIndex:
                # print('Node #{} pop log, its commitIndex is {}'.format(self.node_index, self.commitIndex))
                self.log.pop(i)
                self.log_term.pop(i)
                i -= 1

            # 4
            self.currentTerm = request.term

            for entry in request.entries:
                self.append_to_local_log(entry.key, entry.value, entry.term)

            # 5 when there is partial partition and two leader has different commitIndex so use the outside "max"
            self.commitIndex = max(self.commitIndex, min(request.leaderCommit, len(self.log) - 1))

            self.leaderIndex = request.leaderId
            self.convert_to_follower(request.term, request.leaderId)
            return storage_service_pb2.AppendEntriesResponse(term=self.currentTerm, success=True)

    #@synchronized(lock_persistent_operations)
    @network
    def RequestVote(self, request, context):
        if request is None:
            return storage_service_pb2.RequestVoteResponse(voteGranted=False)
        self.logger.info('Node #{} received RequestVote from Node #{}.'.format(self.node_index, request.candidateId))

        with self.lock_persistent_operations:
            # 1. candidate's term is smaller than current term
            if request.term < self.currentTerm:
                return storage_service_pb2.RequestVoteResponse(term=self.currentTerm, voteGranted=False)

            # 2 has vote for others
            if request.term == self.currentTerm and self.votedFor and self.votedFor != request.candidateId:
                return storage_service_pb2.RequestVoteResponse(term=self.currentTerm, voteGranted=False)

            # 3 when the receiver's log is more up-to-date than the candidate's log
            if len(self.log_term) > 0 and (request.lastLogTerm < self.log_term[-1] or \
                    (request.lastLogTerm == self.log_term[-1] and request.lastLogIndex < len(self.log) - 1)):
                return storage_service_pb2.RequestVoteResponse(term=self.currentTerm, voteGranted=False)

            # 4 convert to follower
            self.votedFor = request.candidateId
            self.convert_to_follower(request.term, request.candidateId, False)
            self.persist()
            self.logger.info('Node #{}, term:{}, votedfor:{}'.format(self.node_index, self.currentTerm, self.votedFor))
            return storage_service_pb2.RequestVoteResponse(term=self.currentTerm, voteGranted=True)

    def persist(self):
        history = dict()
        history['term'] = self.currentTerm
        history['state'] = self.state
        history['votedFor'] = self.votedFor
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
                # print(f.read().strip() != "")
                s = f.read().rstrip()
                if s != "":
                    history = eval(s)
        return history

    def to_string(self):
        s = []
        s.append("term: {}".format(self.currentTerm))
        s.append("state: {}".format(self.state))
        s.append("votedFor: {}".format(self.votedFor))
        s.append("logTerm: {}".format(str(self.log_term)))
        s.append("log: {}".format(str(self.log)))
        return "\n".join(s)

    def convert_to_follower(self, term, new_leader_id, is_persist=True):
        if self.state > 0:
            self.logger.warning('{} converts to follower in term {}'.format(self.node_index, term))

        # do we need to reset the votedFor value? No
        self.state = 0
        self.voteCnt = 0
        self.currentTerm = term
        # self.leaderIndex = new_leader_id

        if is_persist:
            self.persist()
        self.reset_election_timer()
        # if the previous state is leader, then set the timer
        if self.heartbeat_timer:
            self.heartbeat_timer.cancel()
        self.evoke_apply_thread()

    def convert_to_candidate(self):
        prev_state = self.state
        self.state = 1
        self.votedFor = self.node_index
        self.currentTerm += 1
        self.voteCnt = 1
        self.persist()
        if prev_state != 1:
            self.logger.warning('{} converts to candidate in term {}'.format(self.node_index, self.currentTerm))
        self.reset_election_timer()
        self.ask_for_vote_to_all()

    def convert_to_leader(self): 
        self.cancel_election_timer()
        self.state = 2
        self.leaderIndex = self.node_index
        self.is_leader = True  #add on 5/12
        self.persist()
        self.set_heartbeat_timer()
        self.logger.warning('{} converts to leader in term {}'.format(self.node_index, self.currentTerm))

    def ask_for_vote_to_all(self):
        # send RPC requests to all other nodes
        if len(self.configs['nodes']) == 1:
            self.convert_to_leader()

        for t in self.configs['nodes']:
            if t[0] == self.myIp and t[1] == self.myPort:
                continue
            try:
                node_index = self.get_node_index_by_addr(t[0], t[1])
                requestVote_thread = threading.Thread(target=self.ask_for_vote_to_one, args=(t[0], t[1], node_index))
                requestVote_thread.start()
            except Exception as e:
                self.logger.error("node{} in term {} ask_for_vote error".format(self.node_index, self.currentTerm))

    def ask_for_vote_to_one(self, ip, port, node_index):
        self.logger.info('{} ask vote from {} in term {}'.format(self.node_index, node_index, self.currentTerm))
        with grpc.insecure_channel(ip + ':' + port) as channel:
            stub = storage_service_pb2_grpc.KeyValueStoreStub(channel)
            request = self.generate_requestVote_request()
            try:
                self.logger.info('Node #{} asks for the vote by {}'.format(self.node_index, node_index))
                response = stub.RequestVote(request, timeout=float(self.configs['rpc_timeout']))
                # when the response packets delay and detour in the network so than this response is an stale
                if not response.voteGranted:
                    return
                # print(response.term)
                if response.term < self.currentTerm:  # expired vote
                    return
                elif response.term > self.currentTerm:
                    self.convert_to_follower(request.term, node_index)
                else:
                    if response.voteGranted:
                        with self.lock_persistent_operations:
                            self.voteCnt += 1

                            self.logger.info(
                                "get one vote from node {}, current voteCnt is {}".format(node_index, self.voteCnt))
                            majority_cnt = len(self.configs['nodes']) // 2 + 1
                            if self.state != 2 and self.voteCnt >= majority_cnt:
                                self.convert_to_leader()

            except Exception as e:
                self.logger.info("Node #{} Timeout error when requestVote to {}".format(self.node_index, node_index))

    def generate_requestVote_request(self):
        request = storage_service_pb2.RequestVoteRequest()
        request.term = self.currentTerm
        request.candidateId = self.node_index
        # consider the length of log equal to 0, i.e., no entries in candidate's log.
        request.lastLogIndex = len(self.log) - 1
        if request.lastLogIndex < 0:
            request.lastLogTerm = 0
        else:
            request.lastLogTerm = int(self.log_term[-1])
        return request

    def Partition(self, request, context):
        self.logger.warning('RPC Partition() called.')
        num_of_nodes_with_leader = request.num_of_nodes_with_leader

        if self.leaderIndex < num_of_nodes_with_leader:
            if self.node_index < num_of_nodes_with_leader:
                # set the matrix value of node_index from num_of_nodes_with_leader to n-1 to 1
                for node_index in range(num_of_nodes_with_leader, len(self.configs['nodes'])):
                    ret = self.partition_two_nodes(node_index)
                    if ret == 1:
                        return storage_service_pb2.PartitionResponse(ret=1)
            else:
                # set the matrix value of node_index from 0 to num_of_nodes_with_leader-1 to 1
                for node_index in range(num_of_nodes_with_leader):
                    ret = self.partition_two_nodes(node_index)
                    if ret == 1:
                        return storage_service_pb2.PartitionResponse(ret=1)
        else:
            if self.node_index < num_of_nodes_with_leader - 1:
                # set the matrix value of node_index from num_of_nodes_with_leader-1 to n-1 except leaderIndex to 1
                for node_index in range(num_of_nodes_with_leader-1, len(self.configs['nodes'])):
                    if node_index == self.leaderIndex:
                        continue
                    ret = self.partition_two_nodes(node_index)
                    if ret == 1:
                        return storage_service_pb2.PartitionResponse(ret=1)
            else:
                # set the matrix value of node_index from 0 to num_of_nodes_with_leader-2 and leaderIndex to 1
                if self.is_leader:
                    for node_index in range(num_of_nodes_with_leader - 1, len(self.configs['nodes'])):
                        if node_index == self.leaderIndex:
                            continue
                        ret = self.partition_two_nodes(node_index)
                        if ret == 1:
                            return storage_service_pb2.PartitionResponse(ret=1)
                else: # not leader
                    for node_index in range(num_of_nodes_with_leader-1):
                        ret = self.partition_two_nodes(node_index)
                        if ret == 1:
                            return storage_service_pb2.PartitionResponse(ret=1)
                    if self.node_index != self.leaderIndex:
                        ret = self.partition_two_nodes(self.leaderIndex)
                        if ret == 1:
                            return storage_service_pb2.PartitionResponse(ret=1)
        return storage_service_pb2.PartitionResponse(ret=0)

    def partition_two_nodes(self, node_index):
        global conn_mat
        N = len(conn_mat)
        if node_index < 0 or node_index >= N:
            return 1
        for id in range(N):
            conn_mat[self.node_index][node_index] = 1.0
            conn_mat[node_index][self.node_index] = 1.0
        return 0


class ChaosServer(chaosmonkey_pb2_grpc.ChaosMonkeyServicer):
    def __init__(self):
        global conn_mat
        conn_mat = load_matrix('matrix')
        self.logger = self.set_log()

    def set_log(self):
        logger = Logger(-1)
        # logger.addHandler(FileHandler("{}_{}.log".format(PERSISTENT_PATH_PREFIC, self.name)))
        ch = StreamHandler()
        ch.setFormatter(Formatter("%(asctime)s %(levelname)s %(message)s"))
        logger.addHandler(ch)
        logger.setLevel("WARNING")
        return logger

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
        # self.logger.warning('RPC KillANode() called.')
        global conn_mat
        node_index = request.node_index
        N = len(conn_mat)
        if node_index < 0 or node_index >= N:
            return chaosmonkey_pb2.Status(ret=1)
        for id in range(N):
            conn_mat[id][node_index] = 1.0
            conn_mat[node_index][id] = 1.0
        return chaosmonkey_pb2.Status(ret=0)


def serve(config_path, myIp, myPort):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=200))
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


def show_thread_cnt_func():
    while True:
        print('Active threads: {}.'.format(threading.active_count()))
        time.sleep(2)


def show_thread_cnt():
    x = threading.Thread(target=show_thread_cnt_func)
    x.start()


if __name__ == '__main__':
    logging.basicConfig()
    config_path = sys.argv[1]
    myIp = sys.argv[2]
    myPort = sys.argv[3]

    # show_thread_cnt()

    serve(config_path, myIp, myPort)
