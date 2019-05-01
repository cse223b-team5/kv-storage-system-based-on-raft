from client import Client
from chaos_server import ChaosServer
from utils import load_config
import random
import time

NO_of_PUTS = 100
NO_of_GETS = 1000

client = Client('config.txt')
chaosmonkey = ChaosServer()

configs = load_config('config.txt')
nodes = configs['nodes']

put_records = dict()
def generate_kv():
    key = random.randint(0, 99999)
    value = random.randint(0, 99999)
    put_records[key] = value
    return key, value

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

def report_put_stats():
    global duration, put_succeed_cnt, put_unknown_error_cnt, put_connection_error_cnt, put_failed_after_many_attempts_cnt
    print('Total elapsed time for PUT: {}ms'.format(int(1000 * duration)))
    print('Average elapsed time for one PUT request: {}ms'.format(int(1000 * duration / NO_Of_PUTS)))
    print('put_succeed_cnt: {}'.format(put_succeed_cnt))
    print('put_unknown_error_cnt: {}'.format(put_unknown_error_cnt))
    print('put_connection_error_cnt: {}'.format(put_connection_error_cnt))
    print('put_failed_after_many_attempts_cnt: {}'.format(put_failed_after_many_attempts_cnt))

def report_get_stats():
    global duration, get_succeed_cnt, get_succeed_and_correct_cnt, get_key_doesnt_exist_cnt, get_unknown_error_cnt
    global get_connection_error_cnt, get_failed_after_many_attempts
    print('Total elapsed time for GET: {}ms'.format(int(duration * 1000)))
    print('Average elapsed time for one GET request: {}ms'.format(int(1000 * duration / NO_of_GETS)))
    print('get_succeed_cnt: {}'.format(get_succeed_cnt))
    print('get_succeed_and_correct_cnt: {}'.format(get_succeed_and_correct_cnt))
    print('get_key_doesnt_exist_cnt: {}'.format(get_key_doesnt_exist_cnt))
    print('get_unknown_error_cnt: {}'.format(get_unknown_error_cnt))
    print('get_connection_error_cnt: {}'.format(get_connection_error_cnt))
    print('get_failed_after_many_attempts: {}'.format(get_failed_after_many_attempts))

def clear_put_stats():
    global duration, put_succeed_cnt, put_unknown_error_cnt, put_connection_error_cnt, put_failed_after_many_attempts_cnt
    duration, put_succeed_cnt, put_unknown_error_cnt, put_connection_error_cnt, put_failed_after_many_attempts_cnt = 0

def clear_get_stats():
    global duration, get_succeed_cnt, get_succeed_and_correct_cnt, get_key_doesnt_exist_cnt, get_unknown_error_cnt
    global get_connection_error_cnt, get_failed_after_many_attempts
    duration, get_succeed_cnt, get_succeed_and_correct_cnt, get_key_doesnt_exist_cnt, get_unknown_error_cnt = 0
    get_connection_error_cnt, get_failed_after_many_attempts = 0


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
    update_put_stats(ret)
end = time.time()
duration = end - start
report_put_stats()
print('---------------------------------------------------------------------')
print('Test of {} times of GET'.format(NO_of_GETS))
keys = list()
for key in put_records:
    keys.append(key)

start = time.time()
for _ in range(NO_of_GETS):
    key = random.choice(keys)
    ret = client.get(key)
    update_get_stats(ret)
end = time.time()
duration = end - start
report_get_stats()
print('---------------------------------------------------------------------')
clear_put_stats()
clear_get_stats()
duration = 30

active_nodes = range(len(nodes))
dead_nodes = list()
print('30sec test with dynamic network condition. Nodes might die and revive.')

start = time.time()
# while time.time() - start < 30:


print('---------------------------------------------------------------------')

