#-*- mode: python; coding: utf-8 -*-

"""
Client
"""

# common conde
import os

# tornado testing stuff
from tornado import testing
from torncache import client as memcache


class ClientTest(testing.AsyncTestCase):

    def setUp(self):
        super(ClientTest, self).setUp()
        servers = os.environ.get('MEMCACHED_URL', 'mc://127.0.0.1:11211')
        self.pool = memcache.ClientPool(servers=servers, ioloop=self.io_loop)

    def test_set(self):
        k = 'runtests.test_set'
        v = 'this is a test'

        def callback(value):
            self.assertTrue(isinstance(value, int))
            self.assertNotEqual(value, 0)
            self.stop()

        self.pool.set(k, v, callback=callback)
        self.wait()

    def test_setget(self):
        k = 'runtests.test_setget'
        v = 'something something'

        def callback(value):
            self.assertEqual(v, value)
            self.stop()

        self.pool.set(k, v, callback=lambda x: self.stop())
        self.wait()
        self.pool.get(k, callback=callback)
        self.wait()

    def test_delete(self):
        k = 'runtests.test_delete'
        v = 'deleteme'

        def set_cb(value):
            #print 'set_cb value: %s' % repr(value)
            self.assertNotEqual(value, 0)
            self.stop()

        def delete_cb(value):
            #print 'delete_cb value: %s' % repr(value)
            self.assertTrue(isinstance(value, int))
            self.assertNotEqual(value, 0)
            self.stop()

        def get_pre_cb(value):
            #print 'get_pre_cb: %s' % repr(value)
            self.assertEqual(value, v)
            self.stop()

        def get_post_cb(value):
            #print 'get_post_cb value: %s' % repr(value)
            self.stop()

        self.pool.set(k, v, callback=set_cb)
        self.wait()
        self.pool.get(k, callback=get_pre_cb)
        self.wait()
        self.pool.delete(k, callback=delete_cb)
        self.wait()
        self.pool.get(k, callback=get_post_cb)
        self.wait()
