import os
import rpyc
import sys
import random
from threading import Timer, Lock, Thread
from logging import Logger, StreamHandler, Formatter
'''
A RAFT RPC server class.

Please keep the signature of the is_leader() method unchanged (though
implement the body of that function correctly.  You will need to add
other methods to implement ONLY the leader election part of the RAFT
protocol.
'''

ELECTION_TIMEOUT_START = 1.5
ELECTION_TIMEOUT_END = 2.5
HEARTBEAT_TIMEOUT = 0.7
PERSISTENT_PATH_PREFIC = "/tmp/hl_"


class RaftNode(rpyc.Service):

	"""
        Initialize the class using the config file provided and also initialize
        any datastructures you may need.
	"""
	def __init__(self, config, id):
		# set basic states
		self.cnt = 0
		self.term = 0
		self.voteFor = ""
		self.state = 0   # 0: follower, 1: candidate 2: leader
		self.name = "node{}".format(id)

		# set functional variances
		self._lock = Lock()
		self.logger = self.set_log()
		self.configs = self.load_config(config)
		self.conns = {}
		self.heartbeat_timer = None
		self.timer = None
		# start running the node
		self.start()

	def start(self):
		states = self.load_history()
		if not states:
			self.logger.info("No history states stored locally")
			self.set_election_timer()
		else:
			self.init_states(int(states['state']), int(states['cnt']), int(states['term']), states['voteFor'])
			if states['state'] == 2:
				# was a leader in the previous state
				self.set_heart_beat()
			else:
				# was a follower or a candidate in history
				self.set_election_timer()

	def init_states(self, state, cnt, term, voteFor):
		self.state = state
		self.cnt = cnt
		self.term = term
		self.voteFor = voteFor

	def set_election_timer(self):
		self.timer = Timer(self.get_random_timeout(), self.convert_to_candidate)
		self.timer.start()

	def cancel_election_timer(self):
		if self.timer:
			self.timer.cancel()

	def reset_election_timer(self):
		if self.timer:
			self.timer.cancel()
		self.set_election_timer()

	def set_log(self):
		logger = Logger(self.name)
		# logger.addHandler(FileHandler("{}_{}.log".format(PERSISTENT_PATH_PREFIC, self.name)))
		ch = StreamHandler()
		ch.setFormatter(Formatter("%(asctime)s %(levelname)s %(message)s"))
		logger.addHandler(ch)
		logger.setLevel("INFO")
		return logger

	def get_random_timeout(self):
		return random.randint(ELECTION_TIMEOUT_START * 1000, ELECTION_TIMEOUT_END * 1000) * 1.0 / 1000

	def get_persist_path(self):
		return "{}_{}_persistent.txt".format(PERSISTENT_PATH_PREFIC, self.name)

	def convert_to_follower(self, term, is_persist = True):
		# do we need to reset the voteFor value? YES
		with self._lock:
			self.state = 0
			self.voteFor = ""
			self.term = term
			self.cnt = 0
			if is_persist:
				self.persist(self.to_string())
		self.reset_election_timer()
		# if the previous state is leader, then set the timer
		if self.heartbeat_timer:
			self.heartbeat_timer.cancel()

	def convert_to_candidate(self):
		self.logger.info("{} converts to candidate with term {}".format(self.name, self.term))
		with self._lock:
			self.term += 1
			self.cnt = 1
			self.state = 1
			self.voteFor = self.name
			self.persist(self.to_string())
		self.reset_election_timer()
		self.ask_for_vote()

	def persist(self, state_str):
		with open(self.get_persist_path(), 'w') as f:
			f.write(state_str)
			f.flush()
			os.fsync(f.fileno())

	def ask_for_vote(self):
		# send RPC requests to all other nodes
		for node in self.configs:
			if node == self.name:
				continue
			try:
				if node not in self.conns:
					self.conns[node] = rpyc.connect(self.configs[node][0], self.configs[node][1])
				Thread(target=self.ask_for_vote_once, args=[node]).start()
			except Exception as e:
				pass
				# self.logger.error("failure when asking for vote from {}".format(node))
				# print(e)

	def ask_for_vote_once(self, node):
		self.logger.info("@###  ask vote from {} in term {}".format(node, self.term))
		try:
			term, vote_granted = self.conns[node].root.request_vote(self.term, self.name)
			print("1111 {}, {}".format(term, vote_granted))
			if term < self.term:
				return
			elif term > self.term:
				self.convert_to_follower(term)
			else:
				if vote_granted:
					with self._lock:
						self.cnt += 1
						self.persist(self.to_string())
					self.logger.info("from {} vote + 1 to be {}, node count : {}".format(node, self.cnt, self.nodes_count))
					print(self.to_string())
					if self.state != 2 and self.cnt >= int(self.nodes_count / 2 + 1):
						# change to leader
							self.evoke_leader()
		except Exception as e:
			print(e)
			# self.logger.error("failure when asking for vote")

	def set_heart_beat(self):
		for node in self.configs:
			if node == self.name:
				continue
			try:
				if node not in self.conns:
					self.conns[node] = rpyc.connect(self.configs[node][0], self.configs[node][1])
				Thread(target=self.set_heart_beat_once, args=[node]).start()
			except Exception as e:
				pass
				# self.logger.error("Failure when sending heart beat to {}".format(node))

		self.heartbeat_timer = Timer(HEARTBEAT_TIMEOUT, self.set_heart_beat)
		self.heartbeat_timer.start()

	def set_heart_beat_once(self, node):
		self.logger.info("sending heartbeat to {}".format(node))
		try:
			term, success = self.conns[node].root.append_entries(self.term, self.name)
			if term > self.term:
				self.convert_to_follower(term)
		except Exception as e:
			pass
			# self.logger.error("Failure when sending heart beat")

	def evoke_leader(self):
		self.logger.info("{} is voted to leader with votes {}".format(self.name, self.cnt))
		self.cancel_election_timer()
		self.logger.info("leader state :{}".format(self.state))
		with self._lock:
			self.state = 2
			self.persist(self.to_string())
		# send hearbeat at once
		self.logger.info("leader state :{}".format(self.state))
		self.set_heart_beat()

	def load_config(self, config_path):
		configs = {}
		with open(config_path) as f:
			line = f.readline()
			while line:
				item = line.rstrip().split(": ")
				if item[0] == "N":
					self.nodes_count = int(item[1])
				else:
					addrs = item[1].split(":")
					configs[item[0]] = (addrs[0], addrs[1])
				line = f.readline()
		return configs

	def load_history(self):
		states = {}
		history_path = self.get_persist_path()
		if not os.path.isfile(history_path):
			return states
		with open(self.get_persist_path()) as f:
			line = f.readline()
			while line:
				item = line.strip().split(":")
				print(item)
				if len(item) == 1:
					states[item[0]] = ""
				else:
					states[item[0]] = item[1].strip()
				line = f.readline()
		return states

	def to_string(self):
		return "node:{}\nstate:{}\nvoteFor:{}\nterm:{}\ncnt:{}".format(self.name, self.state, self.voteFor, self.term, self.cnt)

	def exposed_append_entries(self, term, leader_id):
		if term < self.term:
			return self.term, False

		if term > self.term:
			# update current node
			self.convert_to_follower(term)
			return self.term, True

		if self.state == 0:
			# was a follower, so set the timer
			self.reset_election_timer()
		else:
			self.convert_to_follower(term)
		return self.term, True

	def exposed_request_vote(self, term, name):
		if self.state == 2:
			return self.term, False

		print("get request to vote to {} with  term {}".format(name, term))
		if term < self.term or (term == self.term and self.voteFor != name):
			return self.term, False

		if term == self.term and self.voteFor == name:
			return self.term, True

		if term > self.term:
			self.convert_to_follower(term, False)
		with self._lock:
			self.voteFor = name
			self.persist(self.to_string())
		return self.term, True

	'''
        x = is_leader(): returns True or False, depending on whether
        this node is a leader

        As per rpyc syntax, addi
        ng the prefix 'exposed_' will expose this
        method as an RPC call

        CHANGE THIS METHOD TO RETURN THE APPROPRIATE RESPONSE
    '''
	def exposed_is_leader(self):
		return self.state == 2


if __name__ == '__main__':
	from rpyc.utils.server import ThreadPoolServer
	server = ThreadPoolServer(RaftNode(sys.argv[1], sys.argv[2]), port = int(sys.argv[3]))
	server.start()

# python raftnode.py config.txt 0 5001