import random
import time
import sys
from client import Client
from chaos_client import ChaosMonkey
from utils import load_config
import matplotlib.pyplot as plt

NO_of_PUTS = 100
NO_of_GETS = 100  # in seconds
LOOP_CNT = 10  # kill and revive nodes for LOOP_CNT times, should be even

client = Client('config.txt')
chaosmonkey = ChaosMonkey('config.txt')

configs = load_config('config.txt')
nodes = configs['nodes']

put_records = dict()
keys = list()


def generate_kv():
    key = random.randint(0, 99999)
    value = random.randint(0, 99999)
    return key, value


def get_a_random_key():
    key = random.choice(keys)
    return key


# def put(self, key, value):
# return if_succeed
#   if_success:
#       0 for succeeded,
#       1 for unknown_error
#       2 for connection_error
#       3 for failed_after_many_attempts

# def get(self, key):
# return if_succeed, value
#   if_success:
#       0 for succeeded
#       1 for key_doesnt_exist
#       2 for unknown_error
#       3 for connection_error
#       4 for failed_after_many_attempts
class PutStats:
    def __init__(self):
        self.put_succeed_cnt = 0
        self.put_unknown_error_cnt = 0
        self.put_connection_error_cnt = 0
        self.put_failed_after_many_attempts_cnt = 0

        self.duration = 10
        self.no_of_put = 1

    def update(self, ret):
        if ret == 0:
            self.put_succeed_cnt += 1
        elif ret == 1:
            self.put_unknown_error_cnt += 1
        elif ret == 2:
            self.put_connection_error_cnt += 1
        elif ret == 3:
            self.put_failed_after_many_attempts_cnt += 1

    def report(self):
        print('Total elapsed time for PUT: {}ms'.format(int(1000 * self.duration)))
        print('Average elapsed time for one PUT request: {}ms'.format(int(1000 * self.duration / self.no_of_put)))
        print('put_succeed_cnt: {}'.format(self.put_succeed_cnt))
        print('put_unknown_error_cnt: {}'.format(self.put_unknown_error_cnt))
        print('put_connection_error_cnt: {}'.format(self.put_connection_error_cnt))
        print('put_failed_after_many_attempts_cnt: {}'.format(self.put_failed_after_many_attempts_cnt))


class GetStats:
    def __init__(self):
        self.get_succeed_cnt = 0
        self.get_succeed_and_correct_cnt = 0
        self.get_key_doesnt_exist_cnt = 0
        self.get_unknown_error_cnt = 0
        self.get_connection_error_cnt = 0
        self.get_failed_after_many_attempts = 0

        self.duration = 10
        self.no_of_get = 1

    def update(self, ret, key):
        if ret[0] == 0:
            self.get_succeed_cnt += 1
            if ret[1] == put_records[key]:
                self.get_succeed_and_correct_cnt += 1
        elif ret[0] == 1:
            self.get_key_doesnt_exist_cnt += 1
        elif ret[0] == 2:
            self.get_unknown_error_cnt += 1
        elif ret[0] == 3:
            self.get_connection_error_cnt += 1
        elif ret[0] == 4:
            self.get_failed_after_many_attempts += 1

    def report(self):
        print('Total elapsed time for GET: {}ms'.format(int(self.duration * 1000)))
        print('Average elapsed time for one GET request: {}ms'.format(int(1000 * self.duration / self.no_of_get)))
        print('get_succeed_cnt: {}'.format(self.get_succeed_cnt))
        print('get_succeed_and_correct_cnt: {}'.format(self.get_succeed_and_correct_cnt))
        print('get_key_doesnt_exist_cnt: {}'.format(self.get_key_doesnt_exist_cnt))
        print('get_unknown_error_cnt: {}'.format(self.get_unknown_error_cnt))
        print('get_connection_error_cnt: {}'.format(self.get_connection_error_cnt))
        print('get_failed_after_many_attempts: {}'.format(self.get_failed_after_many_attempts))


def test_and_report():
    print('---------------------------------------------------------------------')
    duration = 5
    no_of_put, no_of_get = 0, 0
    put_stats = PutStats()
    get_stats = GetStats()
    time_when_entering_loop = time.time()
    while time.time() - time_when_entering_loop < duration:
        key, value = generate_kv()
        ret = client.put(key, value)
        if ret == 0:
            # succeeded
            keys.append(key)
            put_records[key] = value
        put_stats.update(ret)
        no_of_put += 1
    put_stats.duration = duration
    put_stats.no_of_put = no_of_put
    if no_of_put != 0:
        put_stats.report()
    print('---------------------------------------------------------------------')
    time_when_entering_loop = time.time()
    while time.time() - time_when_entering_loop < duration:
        key = get_a_random_key()
        ret = client.get(key)
        get_stats.update(ret, key)
        no_of_get += 1
    get_stats.duration = duration
    get_stats.no_of_get = no_of_get
    if no_of_get != 0:
        get_stats.report()
    print('---------------------------------------------------------------------')


def start_static_test():
    print('---------------------------------------------------------------------')
    print('Static network condition. All nodes are alive.')
    print('NO_of_PUTS: {}'.format(NO_of_PUTS))
    print('NO_of_GETS: {}'.format(NO_of_GETS))
    print('---------------------------------------------------------------------')
    print('Test of {} times of PUT'.format(NO_of_PUTS))
    put_stats = PutStats()

    start = time.time()
    for _ in range(NO_of_PUTS):
        key, value = generate_kv()
        ret = client.put(key, value)
        if ret == 0:
            # succeeded
            keys.append(key)
            put_records[key] = value
        put_stats.update(ret)
        # time.sleep(0.1)
    end = time.time()

    put_stats.duration = end - start
    put_stats.no_of_put = NO_of_PUTS
    put_stats.report()
    print('---------------------------------------------------------------------')
    print('Test of {} times of GET'.format(NO_of_GETS))
    get_stats = GetStats()

    start = time.time()
    for _ in range(NO_of_GETS):
        key = get_a_random_key()
        ret = client.get(key)
        get_stats.update(ret, key)
        # time.sleep(0.01)
    end = time.time()

    get_stats.duration = end - start
    get_stats.no_of_get = NO_of_GETS
    get_stats.report()
    print('---------------------------------------------------------------------')


def start_dynamic_test():
    print('Test with dynamic network condition. Nodes might die and revive.')

    node_killed = -1
    loop_cnt = 0

    while loop_cnt < LOOP_CNT:
        loop_cnt += 1
        duration = 5
        no_of_put, no_of_get = 0, 0
        put_stats = PutStats()
        get_stats = GetStats()
        print('=====================================================================')

        time_when_entering_loop = time.time()
        while time.time() - time_when_entering_loop < duration:
            key, value = generate_kv()
            ret = client.put(key, value)
            if ret == 0:
                # succeeded
                keys.append(key)
                put_records[key] = value
            put_stats.update(ret)
            no_of_put += 1
        put_stats.duration = duration
        put_stats.no_of_put = no_of_put
        if no_of_put != 0:
            put_stats.report()

        print('---------------------------------------------------------------------')

        time_when_entering_loop = time.time()
        while time.time() - time_when_entering_loop < duration:
            key = get_a_random_key()
            ret = client.get(key)
            get_stats.update(ret, key)
            no_of_get += 1
        get_stats.duration = duration
        get_stats.no_of_get = no_of_get
        if no_of_get != 0:
            get_stats.report()

        print('Process: {} / {}'.format(loop_cnt, LOOP_CNT))
        leader_index, leader_ip, leader_port = client.get_leader()
        print('Present leader is node #{} at {}:{}'.format(leader_index, leader_ip, leader_port))

        print('---------------------------------------------------------------------')

        if node_killed == -1:
            # no node is killed, so now kill one
            ret = chaosmonkey.kill_a_node_randomly()
            print('Selected node #{} to kill.'.format(ret[1]))
            if ret[0] == 0:
                node_killed = ret[1]
                print('Killed node {}.'.format(ret[1]))
                if node_killed == leader_index:
                    print('************* Killed node is leader *************')
            else:
                print('Failed to kill a node.')
        else:
            # one node is already killed, so noe revive it
            print('Selected node #{} to revive.'.format(node_killed))
            ret = chaosmonkey.revive_a_node(node_killed)
            if ret == 0:
                print('Node {} is revived.'.format(node_killed))
                node_killed = -1
            else:
                print('Node {} cannot be revived.'.format(node_killed))

        print('Sleep for 2 sec for new election.')
        time.sleep(5)
    print('=====================================================================')


def test_kill_leader():
    print('=====================================================================')
    print('Test of killing leader.')

    test_and_report()

    leader_index, leader_ip, leader_port = client.get_leader()
    print('Present leader is node #{} at {}:{}'.format(leader_index, leader_ip, leader_port))
    ret = chaosmonkey.kill_a_node(leader_index)
    if ret == 0:
        print('Leader has been killed')
    else:
        print('Failed to kill a node.')

    print('Sleep for 5 sec for new election.')  # election timer is expected to be around 2-3 seconds
    time.sleep(5)
    print('---------------------------------------------------------------------')
    print('After killing the leader and slept for 5 seconds, retest the service.')
    test_and_report()

    leader_index, leader_ip, leader_port = client.get_leader()
    print('New leader is node #{} at {}:{}'.format(leader_index, leader_ip, leader_port))
    print('=====================================================================')


def get_and_measure_individual(elapsed_time):
    for i in range(NO_of_GETS):
        key = get_a_random_key()
        t1 = time.time()
        ret = client.get(key)
        t2 = time.time()
        # print('ret is: {}, put_records[key]={}.'.format(ret, put_records[key]))
        if ret[0] == 0 and int(ret[1]) == put_records[key]:
            elapsed_time.append(round((t2-t1)*1000.0, 2))
        else:
            elapsed_time.append(0)


def put_and_measure_individual(elapsed_time):
    for i in range(NO_of_PUTS):
        key, value = generate_kv()
        t1 = time.time()
        ret = client.put(key, value)
        t2 = time.time()
        # print('ret is: {}, put_records[key]={}.'.format(ret, put_records[key]))
        if ret == 0:
            elapsed_time.append(round((t2-t1)*1000.0, 2))
        else:
            elapsed_time.append(0)


def show_elapsed_time(elapsed_time, n):
    xs = range(len(elapsed_time))
    plt.plot(xs, elapsed_time)
    plt.show()

    filename = 'exp5_put_#nodes_killed_'+str(n)+'.txt'
    with open(filename, 'w') as f:
        f.write(str(elapsed_time))


def start_exp5(number_of_nodes):
    elapsed_time = exp5_kill_k_nodes(number_of_nodes)
    show_elapsed_time(elapsed_time, number_of_nodes)


def exp5_kill_k_nodes(number_of_nodes_to_kill=1):
    start_static_test()  # first put some data to log
    elapsed_time = list()

    # get_and_measure_individual(elapsed_time)
    put_and_measure_individual(elapsed_time)

    t1 = time.time()
    ret = chaosmonkey.kill_k_nodes_randomly(number_of_nodes_to_kill)
    t2 = time.time()
    print('Kill nodes used time: {}'.format(t2-t1))
    if ret == 0:
        print('{} nodes killed'.format(number_of_nodes_to_kill))
    else:
        print('Failed to kill nodes!')

    # get_and_measure_individual(elapsed_time)
    put_and_measure_individual(elapsed_time)

    failed_cnt = 0
    for x in elapsed_time:
        if x == 0:
            failed_cnt += 1
    print('{}/{} failed.'.format(failed_cnt, len(elapsed_time)))
    print('---------------------------------------------------------------------')
    return elapsed_time


if __name__ == '__main__':
    test_type = sys.argv[1]
    if test_type == 'static':
        start_static_test()
    elif test_type == 'dynamic':
        start_dynamic_test()
    elif test_type == 'exp5':
        number_of_nodes = 1
        if len(sys.argv) > 2:
            number_of_nodes = sys.argv[2]
        start_exp5(number_of_nodes)
    else:
        print("Invalid operation")

    # static_test()
    #dynamic_test()
    # test_kill_leader()
