import unittest
import math
from TestInput import TestInputSingleton
import mc_bin_client
import time
import uuid
import logger
import crc32
from membase.api.rest_client import RestConnection
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from memcached.helper.data_helper import MemcachedClientHelper

class MemcapableTestBase(object):
    log = None
    keys = None
    servers = None
    input = None
    test = None
    bucket_port = None
    bucket_name = None

    def setUp_bucket(self, bucket_name, port, bucket_type, unittest):

        self.log = logger.Logger.get_logger()
        self.input = TestInputSingleton.input
        unittest.assertTrue(self.input, msg="input parameters missing...")
        self.test = unittest
        self.servers = self.input.servers
        self.bucket_port = port
        self.bucket_name = bucket_name
        ClusterOperationHelper.cleanup_cluster(self.servers)
        BucketOperationHelper.delete_all_buckets_or_assert(self.servers, self.test)

        for serverInfo in self.servers:
            rest = RestConnection(serverInfo)
            info = rest.get_nodes_self()
            rest.init_cluster(username = serverInfo.rest_username,
                              password = serverInfo.rest_password)
            rest.init_cluster_memoryQuota(memoryQuota=info.mcdMemoryReserved)
            bucket_ram = info.mcdMemoryReserved * 2 / 3
            if bucket_name != 'default' and self.bucket_port == 11211:
                rest.create_bucket(bucket=bucket_name,
                                   bucketType=bucket_type,
                                   ramQuotaMB=bucket_ram,
                                   replicaNumber=1,
                                   proxyPort=self.bucket_port,
                                   authType='sasl',
                                   saslPassword='password')
                msg = 'create_bucket succeeded but bucket "default" does not exist'
                self.test.assertTrue(BucketOperationHelper.wait_for_bucket_creation(bucket_name, rest), msg=msg)
                BucketOperationHelper.wait_till_memcached_is_ready_or_assert([serverInfo],
                                                                             self.bucket_port,
                                                                             test=unittest,
                                                                             bucket_name=self.bucket_name,
                                                                             bucket_password='password')

            else:
                rest.create_bucket(bucket=bucket_name,
                                   bucketType=bucket_type,
                                   ramQuotaMB=bucket_ram,
                                   replicaNumber=1,
                                   proxyPort=self.bucket_port)
                msg = 'create_bucket succeeded but bucket "default" does not exist'
                self.test.assertTrue(BucketOperationHelper.wait_for_bucket_creation(bucket_name, rest), msg=msg)
                BucketOperationHelper.wait_till_memcached_is_ready_or_assert([serverInfo],
                                                                            self.bucket_port,
                                                                             test=unittest,
                                                                             bucket_name=self.bucket_name)


#    def tearDown_bucket(self):
#        BucketOperationHelper.delete_all_buckets_or_assert(self.servers, self.test)

    # Instead of checking the value before incrementing, 
    # you can simply ADD it instead before incrementing each time.
    # If it's already there, your ADD is ignored, and if it's not there, it's set.
    def incr_test(self, key, exp, flags, value, incr_amt, decr_amt, incr_time):
        for serverInfo in self.servers:
            client = mc_bin_client.MemcachedClient(host=serverInfo.ip,
                                                          port=self.bucket_port)
#            self.log.info('Waitting 15 seconds for memcached started')
#            time.sleep(15)
#            BucketOperationHelper.wait_till_memcached_is_ready_or_assert([serverInfo],
#                                                              self.bucket_port,
#                                                              test=unittest,
#                                                              bucket_name=self.bucket_name,
#                                                              bucket_password='password')
            if key != 'no_key':
                client.set(key, exp , flags, value)
            if exp != 0:
                self.log.info('Wait {0} seconds for the key expired' .format(exp + 2))
                time.sleep(exp + 2)
            if decr_amt != 0:
                c, d = client.decr(key, decr_amt)
                self.log.info('decr amt {0}' .format(c))
            try:
                i = 0
                while i < incr_time:
                    update_value, cas = client.incr(key, incr_amt)
                    i += 1
                self.log.info('incr {0} times with value {1}'.format(incr_time, incr_amt))
                return update_value
            except mc_bin_client.MemcachedError as error:
                    self.log.info('memcachedError : {0}'.format(error.status))
                    self.test.fail("unable to increment value: {0}".format(incr_amt))


    def decr_test(self, key, exp, flags, value, incr_amt, decr_amt, decr_time):
        for serverInfo in self.servers:
            client = mc_bin_client.MemcachedClient(host=serverInfo.ip,
                                                          port=self.bucket_port)
#            self.log.info('Waitting 15 seconds for memcached started')
#            time.sleep(15)
#            BucketOperationHelper.wait_till_memcached_is_ready_or_assert([serverInfo],
#                                                              self.bucket_port,
#                                                              test=unittest,
#                                                              bucket_name=self.bucket_name,
#                                                              bucket_password='password')
            if key != 'no_key':
                client.set(key, exp , flags, value)
            if exp != 0:
                self.log.info('Wait {0} seconds for the key expired' .format(exp + 2))
                time.sleep(exp + 2)
            if incr_amt != 0:
                c, d = client.incr(key, incr_amt)
                self.log.info('incr amt {0}' .format(c))
            i = 0
            while i < decr_time:
                update_value, cas = client.decr(key, decr_amt)
                i += 1
            self.log.info('decr {0} times with value {1}'.format(decr_time, decr_amt))
        return update_value

    
class SimpleIncrMembaseBucketDefaultPort(unittest.TestCase):
    memcapableTestBase = None
    log = logger.Logger.get_logger()

    def setUp(self):
        self.memcapableTestBase = MemcapableTestBase()
        self.memcapableTestBase.setUp_bucket('default', 11211, 'membase', self)

    def test_incr_an_exist_key_never_exp(self):
        key_test = 'has_key'
        value = '10'
        decr_amt = 0
        incr_amt = 5
        incr_time = 10
        update_v = self.memcapableTestBase.incr_test(key_test, 0, 0, value, incr_amt, decr_amt, incr_time)
        if update_v == (int(value) + incr_amt*incr_time):
            self.log.info('Value update correctly.  Expected value {0}.  Tested value {1}' \
                           .format((int(value) + incr_amt*incr_time), update_v))                   
        else:
            self.test.fail("FAILED test_incr_an_exist_key_never_exp. Original value %s. \
                            Expected value %d" % (value, int(value) + incr_amt*incr_time))

    def test_incr_non_exist_key(self):
        key_test = 'no_key'
        value = '10'
        decr_amt = 0
        incr_amt = 5
        incr_time = 10
        update_v = self.memcapableTestBase.incr_test(key_test, 0, 0, value, incr_amt, decr_amt, incr_time)
        if update_v == incr_amt*(incr_time - 1):
            self.log.info('Value update correctly.  Expected value {0}.  Tested value {1}' \
                          .format(incr_amt*(incr_time - 1), update_v))               
        else:
            self.test.fail("FAILED test_incr_non_exist_key")

    def test_incr_with_exist_key_and_expired(self):
        key_test = 'expire_key'
        value = '10'
        exp_time = 5
        decr_amt = 0
        incr_amt = 5
        incr_time = 10
        update_v = self.memcapableTestBase.incr_test(key_test, exp_time, 0, value, incr_amt, decr_amt, incr_time)
        if update_v == incr_amt*(incr_time - 1):
            self.log.info('Value update correctly.  Expected value{0}.  Tested value {1}' \
                           .format(incr_amt*(incr_time - 1), update_v))                  
        else:
            self.test.fail("FAILED test_incr_with_exist_key_and_expired")

    def test_incr_with_exist_key_decr_then_incr_never_expired(self):
        key_test = 'has_key'
        value = '101'
        exp_time = 0
        decr_amt = 10
        incr_amt = 5
        incr_time = 10
        update_v = self.memcapableTestBase.incr_test(key_test, exp_time, 0, value, incr_amt, decr_amt, incr_time)
        if update_v == (int(value) - decr_amt + incr_amt*incr_time):
            self.log.info('Value update correctly.  Expected value {0}.  Tested value {1}' \
                          .format(int(value) - decr_amt + incr_amt*(incr_time), update_v))
        else:
            self.test.fail("FAILED test_incr_with_exist_key_and_expired")
            
## this test will fail as expected
#    def test_incr_with_non_int_key(self):
#        key_test = 'has_key'
#        value = 'abcd'
#        exp_time = 0
#        decr_amt = 0
#        incr_amt = 5
#        incr_time = 10
#        self.assertRaises(self.memcapableTestBase.incr_test(key_test, exp_time, 0, value, incr_amt, decr_amt, incr_time))
##        self.assertRaises('Expected FAILED.  Can not incr with string value')

class SimpleDecrMembaseBucketDefaultPort(unittest.TestCase):
    memcapableTestBase = None
    log = logger.Logger.get_logger()

    def setUp(self):
        self.memcapableTestBase = MemcapableTestBase()
        self.memcapableTestBase.setUp_bucket('default', 11211, 'membase', self)

    def test_decr_an_exist_key_never_exp(self):
        key_test = 'has_key'
        value = '100'
        exp_time = 0
        decr_amt = 5
        incr_amt = 0
        decr_time = 10
        update_v = self.memcapableTestBase.decr_test(key_test, exp_time, 0, value, incr_amt, decr_amt, decr_time)
        if update_v == (int(value) - decr_amt*decr_time):
            self.log.info('Value update correctly.  Expected value {0}.  Tested value {1}' \
                          .format((int(value) - decr_amt*decr_time), update_v))                   
        else:
            self.test.fail("FAILED test_decr_an_exist_key_never_exp. Original value %s. \
                            Expected value %d" % (value, int(value) - decr_amt*decr_time))

    def test_decr_non_exist_key(self):
        key_test = 'no_key'
        value = '100'
        exp_time = 0
        decr_amt = 5
        incr_amt = 0
        decr_time = 10
        update_v = self.memcapableTestBase.decr_test(key_test, exp_time, 0, value, incr_amt, decr_amt, decr_time)
        if update_v == 0:
            self.log.info('Value update correctly.  Expected value 0.  Tested value {0}' \
                           .format(update_v))         
        else:
            self.test.fail('FAILED test_decr_non_exist_key. Expected value 0') 

    def test_decr_with_exist_key_and_expired(self):
        key_test = 'has_key'
        value = '100'
        exp_time = 5
        decr_amt = 5
        incr_amt = 0
        decr_time = 10
        update_v = self.memcapableTestBase.decr_test(key_test, exp_time, 0, value, incr_amt, decr_amt, decr_time)
        if update_v == 0:
            self.log.info('Value update correctly.  Expected value 0.  Tested value {0}'  \
                          .format(update_v))                  
        else:
            self.test.fail('FAILED test_decr_with_exist_key_and_expired.  Expected value 0')            

    def test_decr_with_exist_key_incr_then_decr_never_expired(self):
        key_test = 'has_key'
        value = '100'
        exp_time = 0
        decr_amt = 5
        incr_amt = 50
        decr_time = 10
        update_v = self.memcapableTestBase.decr_test(key_test, exp_time, 0, value, incr_amt, decr_amt, decr_time)
        if update_v == (int(value) + incr_amt - decr_amt*decr_time):
            self.log.info('Value update correctly.  Expected value {0}.  Tested value {1}'  \
                          .format(int(value) + incr_amt - decr_amt*(decr_time), update_v))                                   
        else:
            self.test.fail("Expected value %d.  Test result %d" % (int(value) + incr_amt - decr_amt*(decr_time), update_v)) 