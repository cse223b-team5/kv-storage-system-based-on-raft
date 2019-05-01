from client import Client
from chaos_client import ChaosMonkey
from utils import load_config
import random
import time

NO_of_PUTS = 100
NO_of_GETS = 1000
SIMULATION_DURATION = 60  # in seconds

client = Client('config.txt')
chaosmonkey = ChaosMonkey()

configs = load_config('config.txt')
nodes = configs['nodes']

put_records = dict()
keys = list()


def generate_kv():
    key = random.randint(0, 99999)
    value = random.randint(0, 99999)
    put_records[key] = value
    return key, value


def get_a_random_key():
    key = random.choice(keys)
    return key

# A set of stats
duration = 0

put_succeed_cnt = 0
put_unknown_error_cnt = 0
put_connection_error_cnt = 0
put_failed_after_many_attempts_cnt = 0

get_succeed_cnt = 0
get_succeed_and_correct_cnt = 0
get_key_doesnt_exist_cnt = 0
get_unknown_error_cnt = 0
get_connection_error_cnt = 0
get_failed_after_many_attempts = 0


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
def update_put_stats(ret):
    global duration, put_succeed_cnt, put_unknown_error_cnt, put_connection_error_cnt, put_failed_after_many_attempts_cnt
    if ret == 0:
        put_succeed_cnt += 1
    elif ret == 1:
        put_unknown_error_cnt += 1
    elif ret == 2:
        put_connection_error_cnt += 1
    elif ret == 3:
        put_failed_after_many_attempts_cnt += 1


def update_get_stats(ret):
    global duration, get_succeed_cnt, get_succeed_and_correct_cnt, get_key_doesnt_exist_cnt, get_unknown_error_cnt
    global get_connection_error_cnt, get_failed_after_many_attempts
    if ret[0] == 0:
        get_succeed_cnt += 1
        if ret[1] == put_records[key]:
            get_succeed_and_correct_cnt += 1
    elif ret[0] == 1:
        get_key_doesnt_exist_cnt += 1
    elif ret[0] == 2:
        get_unknown_error_cnt += 1
    elif ret[0] == 3:
        get_connection_error_cnt += 1
    elif ret[0] == 4:
        get_failed_after_many_attempts += 1


def report_put_stats(duration, no_of_put):
    global put_succeed_cnt, put_unknown_error_cnt, put_connection_error_cnt, put_failed_after_many_attempts_cnt
    print('Total elapsed time for PUT: {}ms'.format(int(1000 * duration)))
    print('Average elapsed time for one PUT request: {}ms'.format(int(1000 * duration / no_of_put)))
    print('put_succeed_cnt: {}'.format(put_succeed_cnt))
    print('put_unknown_error_cnt: {}'.format(put_unknown_error_cnt))
    print('put_connection_error_cnt: {}'.format(put_connection_error_cnt))
    print('put_failed_after_many_attempts_cnt: {}'.format(put_failed_after_many_attempts_cnt))


def report_get_stats(duration, no_of_get):
    global get_succeed_cnt, get_succeed_and_correct_cnt, get_key_doesnt_exist_cnt, get_unknown_error_cnt
    global get_connection_error_cnt, get_failed_after_many_attempts
    print('Total elapsed time for GET: {}ms'.format(int(duration * 1000)))
    print('Average elapsed time for one GET request: {}ms'.format(int(1000 * duration / no_of_get)))
    print('get_succeed_cnt: {}'.format(get_succeed_cnt))
    print('get_succeed_and_correct_cnt: {}'.format(get_succeed_and_correct_cnt))
    print('get_key_doesnt_exist_cnt: {}'.format(get_key_doesnt_exist_cnt))
    print('get_unknown_error_cnt: {}'.format(get_unknown_error_cnt))
    print('get_connection_error_cnt: {}'.format(get_connection_error_cnt))
    print('get_failed_after_many_attempts: {}'.format(get_failed_after_many_attempts))


def clear_put_stats():
    global put_succeed_cnt, put_unknown_error_cnt, put_connection_error_cnt, put_failed_after_many_attempts_cnt
    duration, put_succeed_cnt, put_unknown_error_cnt, put_connection_error_cnt, put_failed_after_many_attempts_cnt = 0


def clear_get_stats():
    global get_succeed_cnt, get_succeed_and_correct_cnt, get_key_doesnt_exist_cnt, get_unknown_error_cnt
    global get_connection_error_cnt, get_failed_after_many_attempts
    duration, get_succeed_cnt, get_succeed_and_correct_cnt, get_key_doesnt_exist_cnt, get_unknown_error_cnt = 0
    get_connection_error_cnt, get_failed_after_many_attempts = 0


print('---------------------------------------------------------------------')
print('Static network condition. All nodes are alive.')
print('NO_of_PUTS: {}'.format(NO_of_PUTS))
print('NO_of_GETS: {}'.format(NO_of_GETS))
print('---------------------------------------------------------------------')
print('Test of {} times of PUT'.format(NO_of_PUTS))
start = time.time()
for _ in range(NO_of_PUTS):
    key, value = generate_kv()
    ret = client.put(key, value)
    keys.append(key)
    update_put_stats(ret)
end = time.time()
duration = end - start
report_put_stats(duration, NO_of_PUTS)
print('---------------------------------------------------------------------')
print('Test of {} times of GET'.format(NO_of_GETS))
start = time.time()
for _ in range(NO_of_GETS):
    key = get_a_random_key()
    ret = client.get(key)
    update_get_stats(ret)
end = time.time()
duration = end - start
report_get_stats(duration, NO_of_GETS)
print('---------------------------------------------------------------------')
clear_put_stats()
clear_get_stats()

print('60sec test with dynamic network condition. Nodes might die and revive.')

start = time.time()
node_killed = -1

while time.time() - start < SIMULATION_DURATION:
    time_when_entering_loop = time.time()
    duration = random.randint(5, 10)
    no_of_put, no_of_get = 0
    while time.time() - time_when_entering_loop < duration * 0.5:
        key, value = generate_kv()
        ret = client.put(key, value)
        keys.append(key)
        update_put_stats(ret)
        no_of_put += 1
    while time.time() - time_when_entering_loop < duration:
        key = get_a_random_key()
        ret = client.get(key)
        update_get_stats(ret)
        no_of_get += 1

    print('{} PUT and {} GET are performed, total elapsed time: {}ms'.format(no_of_put, no_of_get, int(duration*1000)))
    report_put_stats(duration*0.5, no_of_put)
    report_get_stats(duration*0.5, no_of_get)

    if node_killed == -1:
        # no node is killed, so now kill one
        ret = chaosmonkey.kill_a_node_randomly()
        if ret[0] == 0:
            node_killed = ret[1]
            print('Killed node {}.'.format(ret[1]))
        else:
            print('Failed to kill a node.')
    else:
        # one node is already killed, so noe revive it
        ret = chaosmonkey.revive_a_node(node_killed)
        if ret == 0:
            print('Node {} is revived.'.format(node_killed))
            node_killed = -1
        else:
            print('Node {} cannot be revived.'.format(node_killed))
print('---------------------------------------------------------------------')

