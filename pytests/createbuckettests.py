import unittest
import uuid
from TestInput import TestInput, TestInputSingleton
import logger
import time
from membase.api.exception import BucketCreationException
from membase.api.rest_client import RestConnection
from membase.helper.bucket_helper import BucketOperationHelper

class CreateMembaseBucketsTests(unittest.TestCase):

    version = None
    servers = None
    input = TestInput
    log = None


    #as part of the setup let's delete all the existing buckets
    def setUp(self):
        self.log = logger.Logger.get_logger()
        self.input = TestInputSingleton.input
        self.assertTrue(self.input, msg="input parameters missing...")
        self.servers = self.input.servers
        BucketOperationHelper.delete_all_buckets_or_assert(servers=self.servers, test_case=self)

    def tearDown(self):
        BucketOperationHelper.delete_all_buckets_or_assert(servers=self.servers, test_case=self)
        pass

    # read each server's version number and compare it to self.version
    def default_moxi(self):
        name = 'default'
        for serverInfo in self.servers:
            rest = RestConnection(serverInfo)
            proxyPort = rest.get_nodes_self().moxi + 1000
            rest.create_bucket(bucket=name, ramQuotaMB=200, proxyPort=proxyPort)
            msg = 'create_bucket succeeded but bucket {0} does not exist'.format(name)
            self.assertTrue(BucketOperationHelper.wait_for_bucket_creation(name, rest), msg=msg)


    def default_case_sensitive_dedicated(self):
        name = 'Default'
        for serverInfo in self.servers:
            rest = RestConnection(serverInfo)
            proxyPort = rest.get_nodes_self().moxi
            rest.create_bucket(bucket=name,
                               ramQuotaMB=200,
                               authType='sasl',
                               saslPassword='test_non_default',
                               proxyPort=proxyPort)
            msg = 'create_bucket succeeded but bucket {0} does not exist'.format(name)
            self.assertTrue(BucketOperationHelper.wait_for_bucket_creation(name, rest), msg=msg)

            name = 'default'

            try:
                rest.create_bucket(bucket=name,
                                   ramQuotaMB=200,
                                   proxyPort=11221,
                                   authType='sasl',
                                   saslPassword='test_non_default')
                msg = "create_bucket created two buckets in different case : {0},{1}".format('default', 'Default')
                self.fail(msg)
            except BucketCreationException as ex:
            #check if 'default' and 'Default' buckets exist
                self.log.info('BucketCreationException was thrown as expected')
                self.log.info(ex.message)

    def default_on_non_default_port(self):
        name = 'default'
        for serverInfo in self.servers:
            rest = RestConnection(serverInfo)
            proxyPort = rest.get_nodes_self().moxi + 1000
            rest.create_bucket(bucket=name,
                               ramQuotaMB=200,
                               proxyPort=proxyPort,
                               authType='sasl',
                               saslPassword='test_non_default')
            msg = 'create_bucket succeeded but bucket {0} does not exist'.format(name)
            self.assertTrue(BucketOperationHelper.wait_for_bucket_creation(name, rest), msg=msg)


    def non_default_moxi(self):
        name = 'test_non_default'
        for serverInfo in self.servers:
            rest = RestConnection(serverInfo)
            proxyPort = rest.get_nodes_self().moxi + 400
            rest.create_bucket(bucket=name,
                               ramQuotaMB=200,
                               proxyPort=proxyPort)
            msg = 'create_bucket succeeded but bucket {0} does not exist'.format(name)
            self.assertTrue(BucketOperationHelper.wait_for_bucket_creation(name, rest), msg=msg)

    def default_case_sensitive_different_ports(self):
        name = 'default'
        for serverInfo in self.servers:
            rest = RestConnection(serverInfo)
            proxyPort = rest.get_nodes_self().moxi + 500
            rest.create_bucket(bucket=name,
                               ramQuotaMB=200,
                               proxyPort=proxyPort)
            msg = 'create_bucket succeeded but bucket {0} does not exist'.format(name)
            self.assertTrue(BucketOperationHelper.wait_for_bucket_creation(name, rest), msg=msg)

            try:
                name = 'DEFAULT'
                rest.create_bucket(bucket=name, ramQuotaMB=200, proxyPort=proxyPort + 1000)
                msg = "create_bucket created two buckets in different case : {0},{1}".format('default', 'DEFAULT')
                self.fail(msg)
            except BucketCreationException as ex:
                #check if 'default' and 'Default' buckets exist
                self.log.info('BucketCreationException was thrown as expected')
                self.log.info(ex.message)


    def non_default_case_sensitive_different_port(self):
        postfix = uuid.uuid4()
        lowercase_name = 'uppercase_{0}'.format(postfix)
        for serverInfo in self.servers:
            rest = RestConnection(serverInfo)
            proxyPort = rest.get_nodes_self().moxi + 500
            rest.create_bucket(bucket=lowercase_name,
                               ramQuotaMB=200,
                               proxyPort=proxyPort)
            msg = 'create_bucket succeeded but bucket {0} does not exist'.format(lowercase_name)
            self.assertTrue(BucketOperationHelper.wait_for_bucket_creation(lowercase_name, rest), msg=msg)

            uppercase_name = 'UPPERCASE_{0}'.format(postfix)
            try:
                rest.create_bucket(bucket=uppercase_name,
                                   ramQuotaMB=200,
                                   proxyPort=proxyPort + 1000)
                msg = "create_bucket created two buckets in different case : {0},{1}".format(lowercase_name,
                                                                                             uppercase_name)
                self.fail(msg)
            except BucketCreationException as ex:
                #check if 'default' and 'Default' buckets exist
                self.log.info('BucketCreationException was thrown as expected')
                self.log.info(ex.message)

    def non_default_case_sensitive_same_port(self):
        postfix = uuid.uuid4()
        name = 'uppercase_{0}'.format(postfix)
        for serverInfo in self.servers:
            rest = RestConnection(serverInfo)
            proxyPort = rest.get_nodes_self().moxi + 100
            rest.create_bucket(bucket=name,
                               ramQuotaMB=200,
                               proxyPort=proxyPort)
            msg = 'create_bucket succeeded but bucket {0} does not exist'.format(name)
            self.assertTrue(BucketOperationHelper.wait_for_bucket_creation(name, rest), msg=msg)

            self.log.info("user should not be able to create a new bucket on a an already used port")
            name = 'UPPERCASE{0}'.format(postfix)
            try:
                rest.create_bucket(bucket=name,
                                   ramQuotaMB=200,
                                   proxyPort=proxyPort)
                self.fail('create-bucket did not throw exception while creating a new bucket on an already used port')
            #make sure it raises bucketcreateexception
            except BucketCreationException as ex:
                self.log.error(ex)

    def less_than_minimum_memory_quota(self):
        postfix = uuid.uuid4()
        name = 'minmemquota_{0}'.format(postfix)
        for serverInfo in self.servers:
            rest = RestConnection(serverInfo)
            proxyPort = rest.get_nodes_self().moxi
            try:
                rest.create_bucket(bucket=name, ramQuotaMB=99, authType='sasl', proxyPort=proxyPort)
                self.fail('create-bucket did not throw exception while creating a new bucket with 99 MB quota')
            #make sure it raises bucketcreateexception
            except BucketCreationException as ex:
                self.log.error(ex)

            try:
                rest.create_bucket(bucket=name, ramQuotaMB=0,
                                   authType='sasl', proxyPort=proxyPort)

                self.fail('create-bucket did not throw exception while creating a new bucket with 0 MB quota')
            #make sure it raises bucketcreateexception
            except BucketCreationException as ex:
                self.log.info(ex)

    def max_memory_quota(self):
        postfix = uuid.uuid4()
        name = 'maxquota_{0}'.format(postfix)
        for serverInfo in self.servers:
            rest = RestConnection(serverInfo)
            info = rest.get_nodes_self()
            proxyPort = rest.get_nodes_self().moxi
            bucket_ram = info.mcdMemoryReserved
            try:
                rest.create_bucket(bucket=name,
                                   ramQuotaMB=bucket_ram,
                                   authType='sasl',
                                   proxyPort=proxyPort)
            except BucketCreationException as ex:
                self.log.error(ex)
                self.fail('failed to create bucket with max ram per node')

            msg = 'failed to start up bucket with max ram per node'
            self.assertTrue(BucketOperationHelper.wait_for_bucket_creation(name, rest), msg=msg)

    def negative_replica(self):
        postfix = uuid.uuid4()
        name = '-1replica_{0}'.format(postfix)
        for serverInfo in self.servers:
            rest = RestConnection(serverInfo)
            proxyPort = rest.get_nodes_self().moxi
            try:
                rest.create_bucket(bucket=name,
                                   ramQuotaMB=200,
                                   replicaNumber=-1,
                                   authType='sasl',
                                   proxyPort=proxyPort)
                self.fail('bucket create succeded even with a negative replica count')
            except BucketCreationException as ex:
                self.log.info(ex)

    def zero_replica(self):
        postfix = uuid.uuid4()
        name = '0replica_{0}'.format(postfix)
        for serverInfo in self.servers:
            rest = RestConnection(serverInfo)
            proxyPort = rest.get_nodes_self().moxi
            try:
                rest.create_bucket(bucket=name,
                                   ramQuotaMB=200,
                                   replicaNumber=0,
                                   authType='sasl', proxyPort=proxyPort)
            except BucketCreationException as ex:
                self.log.error(ex)
                self.fail('failed to create bucket with 0 replicas')

            msg = 'failed to start up bucket with 0 replicas'
            self.assertTrue(BucketOperationHelper.wait_for_bucket_creation(name, rest), msg=msg)

    def one_replica(self):
        postfix = uuid.uuid4()
        name = '0replica_{0}'.format(postfix)
        for serverInfo in self.servers:
            rest = RestConnection(serverInfo)
            proxyPort = rest.get_nodes_self().moxi
            try:
                rest.create_bucket(bucket=name, ramQuotaMB=200, authType='sasl', proxyPort=proxyPort)
            except BucketCreationException as ex:
                self.log.error(ex)
                self.fail('failed to create bucket with 1 replicas')

            msg = 'failed to start up bucket with 1 replicas'
            self.assertTrue(BucketOperationHelper.wait_for_bucket_creation(name, rest), msg=msg)

    def two_replica(self):
        postfix = uuid.uuid4()
        name = '2replica_{0}'.format(postfix)
        for serverInfo in self.servers:
            rest = RestConnection(serverInfo)
            proxyPort = rest.get_nodes_self().moxi
            try:
                rest.create_bucket(bucket=name, ramQuotaMB=200, replicaNumber=2,
                                   authType='sasl', proxyPort=proxyPort)
            except BucketCreationException as ex:
                self.log.error(ex)
                self.fail('failed to create bucket with 2 replicas')

            msg = 'failed to start up bucket with 2 replicas'
            self.assertTrue(BucketOperationHelper.wait_for_bucket_creation(name, rest), msg=msg)

    def three_replica(self):
        postfix = uuid.uuid4()
        name = '3replica_{0}'.format(postfix)
        for serverInfo in self.servers:
            rest = RestConnection(serverInfo)
            proxyPort = rest.get_nodes_self().moxi
            try:
                rest.create_bucket(bucket=name, ramQuotaMB=200, replicaNumber=3,
                                   authType='sasl', proxyPort=proxyPort)
            except BucketCreationException as ex:
                self.log.error(ex)
                self.fail('failed to create bucket with 3 replicas')

            msg = 'failed to start up bucket with 3 replicas'
            self.assertTrue(BucketOperationHelper.wait_for_bucket_creation(name, rest), msg=msg)

    def four_replica(self):
        postfix = uuid.uuid4()
        name = '4replica_{0}'.format(postfix)
        for serverInfo in self.servers:
            rest = RestConnection(serverInfo)
            proxyPort = rest.get_nodes_self().moxi
            try:
                rest.create_bucket(bucket=name, ramQuotaMB=200, replicaNumber=4,
                                   authType='sasl', proxyPort=proxyPort)
                self.fail('created bucket with 4 replicas')
            except BucketCreationException as ex:
                self.log.info(ex)

    # Bucket name can only contain characters in range A-Z, a-z, 0-9 as well as underscore, period, dash & percent. Consult the documentation.
    def valid_chars(self):
        name = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-.%'
        for serverInfo in self.servers:
            rest = RestConnection(serverInfo)
            proxyPort = rest.get_nodes_self().moxi
            try:
                rest.create_bucket(bucket=name, ramQuotaMB=200,
                                   authType='sasl', proxyPort=proxyPort)
            except BucketCreationException as ex:
                self.log.error(ex)
                self.fail('could not create bucket with all valid characters')

            msg = 'failed to start up bucket with all valid characters'
            self.assertTrue(BucketOperationHelper.wait_for_bucket_creation(name, rest), msg=msg)

    # Bucket name can only contain characters in range A-Z, a-z, 0-9 as well as underscore, period, dash & percent. Consult the documentation.
    # only done on the first server
    def invalid_chars(self):
        postfix = uuid.uuid4()
        for char in ['~','!','@','#','$','^','&','*','(',')',':',',',';','"','\'','<','>','?','/']:
            name = '{0}invalid_{1}'.format(postfix,char)
            for serverInfo in [self.servers[0]]:
                rest = RestConnection(serverInfo)
                proxyPort = rest.get_nodes_self().moxi
                try:
                    rest.create_bucket(bucket=name, ramQuotaMB=200, replicaNumber=2,
                                       authType='sasl', proxyPort=proxyPort)
                    self.fail('created a bucket with invalid characters')
                except BucketCreationException as ex:
                    self.log.info(ex)

    # create maximum number of buckets (server memory / 100MB)
    # only done on the first server
    def max_buckets(self):
        log = logger.Logger.get_logger()
        serverInfo = self.servers[0]
        log.info('picking server : {0} as the master'.format(serverInfo))
        rest = RestConnection(serverInfo)
        proxyPort = rest.get_nodes_self().moxi
        info = rest.get_nodes_self()
        rest.init_cluster(username=serverInfo.rest_username,
                          password=serverInfo.rest_password)
        rest.init_cluster_memoryQuota(memoryQuota=info.mcdMemoryReserved)
        bucket_ram = 100
        bucket_count = info.mcdMemoryReserved / bucket_ram

        for i in range(bucket_count):
            bucket_name = 'max_buckets-{0}'.format(uuid.uuid4())
            rest.create_bucket(bucket=bucket_name,
                               ramQuotaMB=bucket_ram,
                               authType='sasl', proxyPort=proxyPort)
            ready = BucketOperationHelper.wait_for_memcached(serverInfo, bucket_name)
            self.assertTrue(ready, "wait_for_memcached failed")

        buckets = []
        try:
            buckets = rest.get_buckets()
        except Exception:
            log.info('15 seconds sleep before calling get_buckets again...')
            time.sleep(15)
            buckets = rest.get_buckets()
        if len(buckets) != bucket_count:
            msg = 'tried to create {0} buckets, only created {1}'.format(bucket_count, len(buckets))
            log.error(msg)
            self.fail(msg=msg)


    # create maximum number of buckets + 1 (server memory / 100MB)
    # only done on the first server
    # negative test
    def more_than_max_buckets(self):
        log = logger.Logger.get_logger()
        serverInfo = self.servers[0]
        log.info('picking server : {0} as the master'.format(serverInfo))
        rest = RestConnection(serverInfo)
        proxyPort = rest.get_nodes_self().moxi
        info = rest.get_nodes_self()
        rest.init_cluster(username=serverInfo.rest_username,
                          password=serverInfo.rest_password)
        rest.init_cluster_memoryQuota(memoryQuota=info.mcdMemoryReserved)
        bucket_ram = 100
        bucket_count = info.mcdMemoryReserved / bucket_ram

        for i in range(bucket_count):
            bucket_name = 'max_buckets-{0}'.format(uuid.uuid4())
            rest.create_bucket(bucket=bucket_name, ramQuotaMB=bucket_ram,
                               authType='sasl', proxyPort=proxyPort)

            ready = BucketOperationHelper.wait_for_memcached(serverInfo, bucket_name)
            self.assertTrue(ready, "wait_for_memcached failed")

        bucket_name = 'max_buckets-{0}'.format(uuid.uuid4())
        try:
            rest.create_bucket(bucket=bucket_name,
                               ramQuotaMB=bucket_ram,
                               authType='sasl'
            )
            msg = 'bucket creation did not fail even though system was overcommited'
            log.error(msg)
            self.fail(msg)
        except BucketCreationException as ex:
            self.log.info('BucketCreationException was thrown as expected')
            self.log.info(ex.message)

        buckets = []
        try:
            buckets = rest.get_buckets()
        except Exception:
            log.info('15 seconds sleep before calling get_buckets again...')
            time.sleep(15)
            buckets = rest.get_buckets()
        if len(buckets) != bucket_count:
            msg = 'tried to create {0} buckets, created {1}'.format(bucket_count, len(buckets))
            log.error(msg)
            self.fail(msg=msg)