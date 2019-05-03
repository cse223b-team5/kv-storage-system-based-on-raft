import time
import random
import threading
import sys
from client import Client
from utils import load_config
from chaos_client import ChaosMonkey

NO_of_CONCURRENCY = 5
TIME_of_TEST = 5  # s
CONCURRENT_TYPES = {0: "concurrent_put", 1:"concurrent_get",
                    2: "concurrent_put_get_orderly", 3: "current_put_get_by_ratio"}
TEST_TYPES = {0: "static_test", 1: "dynamic_test"}

chaosmonkey = ChaosMonkey('config.txt')
configs = load_config('config.txt')
nodes = configs['nodes']


class NewPutStats:
    def __init__(self):
        self.put_succeed_cnt = 0
        self.put_unknown_error_cnt = 0
        self.put_connection_error_cnt = 0
        self.put_failed_after_many_attempts_cnt = 0

        self.duration = 10
        self.no_of_put = 0

    def update(self, ret):
        self.no_of_put += 1
        if ret == 0:
            self.put_succeed_cnt += 1
        elif ret == 1:
            self.put_unknown_error_cnt += 1
        elif ret == 2:
            self.put_connection_error_cnt += 1
        elif ret == 3:
            self.put_failed_after_many_attempts_cnt += 1


class NewGetStats:
    def __init__(self, put_records=dict()):
        self.get_succeed_cnt = 0
        self.get_succeed_and_correct_cnt = 0
        self.get_key_doesnt_exist_cnt = 0
        self.get_unknown_error_cnt = 0
        self.get_connection_error_cnt = 0
        self.get_failed_after_many_attempts = 0

        self.duration = 10
        self.no_of_get = 0
        self.put_records = put_records

    def update(self, ret, key):
        self.no_of_get += 1
        if ret[0] == 0:
            self.get_succeed_cnt += 1
            if key in self.put_records and ret[1] == self.put_records[key]: 
                self.get_succeed_and_correct_cnt += 1
        elif ret[0] == 1 and key not in self.put_records:
            self.get_succeed_and_correct_cnt += 1
        elif ret[0] == 1 and key in self.put_records:
            self.get_key_doesnt_exist_cnt += 1
        elif ret[0] == 2:
            self.get_unknown_error_cnt += 1
        elif ret[0] == 3:
            self.get_connection_error_cnt += 1
        elif ret[0] == 4:
            self.get_failed_after_many_attempts += 1


class Tester:
    def __init__(self, test_type, concurrent_type, test_duration, key_start, key_end, get_ratio=0, put_records={}):
        self.test_type = test_type  # 0:static test, 1:dynamic test
        self.concurrent_type = concurrent_type
        self.get_ratio = get_ratio  # ratio of get request occurs; used for randomly put get test
        self.total_cnt = 0
        self.success_cnt = 0
        self.test_duration = test_duration
        self.end_time = None
        self.key_start = key_start # each client test within different key range [key_start, key_end)
        self.key_end = key_end
        self.put_records = put_records
        self.get_stats = NewGetStats(self.put_records)
        self.put_stats = NewPutStats()

        self.client = Client('config.txt')

    def test(self):
        if self.test_type == 0:
            self.static_test()
        else:
            self.dynamic_test()
        self.update()

    def update(self):
        self.total_cnt = self.put_stats.no_of_put + self.get_stats.no_of_get
        self.success_cnt = self.put_stats.put_succeed_cnt + self.get_stats.get_succeed_cnt

    def static_test(self):
        start_time = time.time()
        while time.time() - start_time < self.test_duration:
            # continue the test
            self.run_one_test()
        self.end_time = time.time()

    def dynamic_test(self):
        num = random.random()

    def run_one_test(self):
        if self.concurrent_type == 0:
            self.run_concurrent_put()
        elif self.concurrent_type == 1:
            self.concurrent_get()
        elif self.concurrent_type == 2:
            self.concurrent_put_get_orderly()
        else:
            self.current_put_get_by_ratio()

    def run_concurrent_put(self):
        # only run concurrent_put	
        t1 = time.time()
        key, value = self.generate_kv()
        ret = self.client.put(key, value)
        self.put_stats.update(ret)
        t2 = time.time()
        if ret == 0:
            self.put_records[key] = value
        print("thread {} put key:{} value:{} ret:{} time:{} ms".format(self.key_start, key, value, ret, 1000 * (t2-t1)))

    def concurrent_get(self):
        # only run concurrent_put
        t1 = time.time()
        key = random.randint(self.key_start, self.key_end)
        ret = self.client.get(key)
        self.get_stats.update(ret, key)
        t2 = time.time()
        print("thread {} get key:{} ret:{} time:{} ms".format(self.key_start, key,  ret, 1000 * (t2-t1)))
    
    def concurrent_put_get_orderly(self):
        # one get by one put
        key, value = self.generate_kv()
        ret = self.client.put(key, value)
        self.put_stats.update(ret)
        if ret == 0:
            self.put_records[key] = value

        ret = self.client.get(key)
        self.get_stats.update(ret, key)

    def current_put_get_by_ratio(self):
        if random.random() < self.get_ratio:
            # get request
            key = random.randint(self.key_start, self.key_end)
            ret = self.client.get(key)
            self.get_stats.update(ret, key)
        else:
            key, value = self.generate_kv()
            ret = self.client.put(key, value)
            self.put_stats.update(ret)
            if ret == 0:
                self.put_records[key] = value

    def generate_kv(self):
        key = random.randint(self.key_start, self.key_end)
        value = random.randint(self.key_start, self.key_end)
        return key, value

    def get_a_random_key(self):
        key = random.choice(self.keys)
        return key


class ConcurrentTester:
    def __init__(self, test_type, concurrent_type, clients_cnt, test_duration):
        self.total_cnt = 0
        self.success_cnt = 0
        self.start_time = 0
        self.end_time = 0
        self.clients_cnt = clients_cnt
        self.test_duration = test_duration # s/ for each test client
        self.test_type = test_type        # {0: "static_test", 1: "dynamic_test"}
        self.concurrent_type = concurrent_type # as CONCURRENT_TYPES shows
        self._lock = threading.Lock()
        self.put_records_all = {}

    def test(self):
        cts = []
        N = 5
        self.start_time = time.time()
        for i in range(self.clients_cnt):
            try:
                key_start = i * N
                key_end = (i + 1) * N
                client_thread = threading.Thread(target=self.run_one_client, args=(key_start, key_end))
                cts.append(client_thread)
                client_thread.start()
            except Exception:
                print("client error")

        #for ct in cts:
        #    ct.join()
        #self.report()

    def run_one_client(self, key_start, key_end):
        key_range = "{}_{}".format(key_start, key_end)
        put_record = self.put_records_all[key_range] if key_range in self.put_records_all else {}
        tester = Tester(self.test_type, self.concurrent_type, self.test_duration,
                        key_start, key_end, put_record)
        tester.test()
        with self._lock:
            self.success_cnt += tester.success_cnt
            self.total_cnt += tester.total_cnt
            self.end_time = max(self.end_time, tester.end_time)
        if self.concurrent_type == 0:
            # save put results for get test
            self.put_records_all[key_range] = tester.put_records

    def report(self):
        print(str(self.end_time - self.start_time))
        print('---------------------------------------------------------------------')
        print("========== {}, {}".format(self.test_type, CONCURRENT_TYPES.get(self.concurrent_type)))
        print('Concurrent clients test for {} s. All nodes are alive.'.format(self.end_time - self.start_time))
        print('No_of_CLIENTS: {}'.format(self.clients_cnt))
        print("No_of_Total_Request: {}".format(self.total_cnt))
        print("No_of_Success_Request: {}".format(self.success_cnt))
        print("The average response time for a request: {} ms".format(
            int(self.end_time - self.start_time) * 1000 * 1.0 / self.total_cnt))
        print("The average QPS is {}".format(self.success_cnt / int(self.end_time - self.start_time)))
        print('---------------------------------------------------------------------\n')


def start_static_test():
    # static concurrent put test
    static_put_ct = ConcurrentTester(0, 0, NO_of_CONCURRENCY, TIME_of_TEST)
    static_put_ct.test()

    # # static concurrent get test
    static_get_ct = ConcurrentTester(0, 1, NO_of_CONCURRENCY, TIME_of_TEST)
    static_get_ct.put_records_all = static_put_ct.put_records_all
    static_get_ct.test()
    #
    # static concurrent_put_get_orderly
    static_get_ct = ConcurrentTester(0, 2, NO_of_CONCURRENCY, TIME_of_TEST)
    static_get_ct.put_records_all = static_put_ct.put_records_all
    static_get_ct.test()
    #
    # static concurrent_put_get_by_ratio
    static_get_ct = ConcurrentTester(0, 3, NO_of_CONCURRENCY, TIME_of_TEST)
    static_get_ct.put_records_all = static_put_ct.put_records_all
    static_get_ct.test()

def start_dynamic_test():
    pass

if __name__ == '__main__':
    test_type = sys.argv[1]
    if test_type == 'static':
        start_static_test()
    elif test_type == 'dynamic':
        start_dynamic_test()
    else:
        print("Invalid operation")