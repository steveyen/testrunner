from threading import Thread
import unittest
import time
from TestInput import TestInputSingleton
import logger
from membase.api.rest_client import RestConnection, RestHelper
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.rebalance_helper import RebalanceHelper
from memcached.helper.data_helper import MemcachedClientHelper

class DrainRateTests(unittest.TestCase):
    def setUp(self):
        self.log = logger.Logger.get_logger()
        self.input = TestInputSingleton.input
        self.assertTrue(self.input, msg="input parameters missing...")
        self.master = self.input.servers[0]
        self.bucket = "default"
        self.number_of_items = -1
        self._create_default_bucket()
        self.drained_in_seconds = -1

    def _create_default_bucket(self):
        rest = RestConnection(self.master)
        helper = RestHelper(RestConnection(self.master))
        if not helper.bucket_exists(self.bucket):
            node_ram_ratio = BucketOperationHelper.base_bucket_ratio([self.master])
            info = rest.get_nodes_self()
            available_ram = info.mcdMemoryReserved * node_ram_ratio
            rest.create_bucket(bucket=self.bucket, ramQuotaMB=int(available_ram))
            ready = BucketOperationHelper.wait_for_memcached(self.master, self.bucket)
            self.assertTrue(ready, msg="wait_for_memcached failed")
        self.assertTrue(helper.bucket_exists(self.bucket),
                        msg="unable to create {0} bucket".format(self.bucket))

    def _load_data_for_buckets(self):
        rest = RestConnection(self.master)
        buckets = rest.get_buckets()
        distribution = {512: 1.0}
        for bucket in buckets:
            MemcachedClientHelper.load_bucket(name=self.bucket,
                                              servers=[self.master],
                                              value_size_distribution=distribution,
                                              number_of_threads=4,
                                              number_of_items=self.number_of_items,
                                              write_only=True,
                                              moxi=False)

    def _monitor_drain_queue(self):
        #start whenever drain_queue is > 0
        rest = RestConnection(self.master)
        start = time.time()
        while (time.time() - start) < 10:
            stats = rest.get_bucket_stats(self.bucket)
            if stats and "ep_queue_size" in stats and stats["ep_queue_size"] > 0:
                self.log.info(stats["ep_queue_size"])
                start = time.time()
                RebalanceHelper.wait_for_stats(self.master, self.bucket, 'ep_queue_size', 0, verbose=False)
                self.drained_in_seconds = time.time() - start
                break

    def _test_drain(self):
        loader = Thread(target=self._load_data_for_buckets)
        wait_for_queue = Thread(target=self._monitor_drain_queue)
        loader.start()
        wait_for_queue.start()
        self.log.info("waiting for loader thread to insert {0} items".format(self.number_of_items))
        loader.join()
        self.log.info("waiting for ep_queue == 0")
        wait_for_queue.join()
        self.log.info("took {0} seconds to drain {1} items".format(self.drained_in_seconds, self.number_of_items))


    def test_drain_10k_items(self):
        self.number_of_items = 10 * 1000
        self._test_drain()

    def test_drain_100k_items(self):
        self.number_of_items = 100 * 1000
        self._test_drain()


    def test_drain_1M_items(self):
        self.number_of_items = 1 * 1000 * 1000
        self._test_drain()

