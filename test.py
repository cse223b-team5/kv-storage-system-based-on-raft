from client import Client
import random
import time

NO_Of_PUTS = 10
NO_of_GETS = 100

client = Client('config.txt')

put_records = dict()
for i in range(NO_Of_PUTS):
    put_records[i] = random.randint(0, 99999)

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

print('NO_Of_PUTS: {}'.format(NO_Of_PUTS))
print('NO_of_GETS: {}'.format(NO_of_GETS))

start = time.time()
for key in put_records:
    value = put_records[key]
    ret = client.put(key, value)
    if ret == 0:
        put_succeed_cnt += 1
    elif ret == 1:
        put_unknown_error_cnt += 1
    elif ret == 2:
        put_connection_error_cnt += 1
    elif ret == 3:
        put_failed_after_many_attempts_cnt += 1
end = time.time()
duration = end - start
print('Total elapsed time for PUT: {}'.format(duration))
print('Average elapsed time for one PUT request: {}'.format(duration/NO_Of_PUTS))
print('put_succeed_cnt: {}'.format(put_succeed_cnt))
print('put_unknown_error_cnt: {}'.format(put_unknown_error_cnt))
print('put_connection_error_cnt: {}'.format(put_connection_error_cnt))
print('put_failed_after_many_attempts_cnt: {}'.format(put_failed_after_many_attempts_cnt))


keys = list()
for key in put_records:
    keys.append(key)

start = time.time()
for _ in range(NO_of_GETS):
    key = random.choice(keys)
    ret = client.get(key)
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
end = time.time()
duration = end - start
print('Total elapsed time for GET: {}'.format(duration))
print('Average elapsed time for one GET request: {}'.format(duration/NO_of_GETS))
print('get_succeed_cnt: {}'.format(get_succeed_cnt))
print('get_succeed_and_correct_cnt: {}'.format(get_succeed_and_correct_cnt))
print('get_key_doesnt_exist_cnt: {}'.format(get_key_doesnt_exist_cnt))
print('get_unknown_error_cnt: {}'.format(get_unknown_error_cnt))
print('get_connection_error_cnt: {}'.format(get_connection_error_cnt))
print('get_failed_after_many_attempts: {}'.format(get_failed_after_many_attempts))

