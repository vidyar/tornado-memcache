#-*- mode: python; coding: utf-8 -*-

"""
Client
"""

# common conde
import os
import json

# tornado testing stuff
from tornado import testing
from torncache import client as memcache


class ClientTest(testing.AsyncTestCase):

    def setUp(self):
        def _ser(key, value):
            if isinstance(value, dict):
                return json.dumps(value), 4
            return value, 0

        def _des(key, value, flags):
            if flags == 4:
                return json.loads(value)
            return value

        super(ClientTest, self).setUp()
        servers = os.environ.get('MEMCACHED_URL', 'mc://127.0.0.1:11211')
        self.pool = memcache.ClientPool(
            servers=servers,
            ioloop=self.io_loop,
            serializer=_ser,
            deserializer=_des)

        # Clean primary key used on test
        self.pool.delete('key', noreply=True, callback=self.stop)
        self.wait()

    def test_set_success(self):
        self.pool.set('key', 'value', noreply=False, callback=self.stop)
        result = self.wait()
        self.assertTrue(result)

    def test_set_unicode_key(self):
        with self.assertRaises(memcache.MemcacheIllegalInputError):
            self.pool.set(u'\u0FFF', 'value', noreply=False)

    def test_set_unicode_value(self):
        with self.assertRaises(memcache.MemcacheIllegalInputError):
            self.pool.set('key', u'\u0FFF', noreply=False)

    def test_set_noreply(self):
        self.pool.set('key', 'value', noreply=True, callback=self.stop)
        result = self.wait()
        self.assertTrue(result)

    def test_set_many_success(self):
        self.pool.set_many({'key': 'value'}, noreply=False, callback=self.stop)
        result = self.wait()
        self.assertTrue(result['key'])

    def test_add_stored(self):
        self.pool.add('key', 'value', noreply=False, callback=self.stop)
        result = self.wait()
        self.assertTrue(result)

    def test_add_not_stored(self):
        self.pool.add('key', 'value', noreply=False, callback=self.stop)
        result = self.wait()
        self.assertTrue(result)

        self.pool.add('key', 'value', noreply=False, callback=self.stop)
        result = self.wait()
        self.assertFalse(result)

    def test_get_not_found(self):
        self.pool.get('key_not_found', callback=self.stop)
        result = self.wait()
        self.assertEqual(result, None)

    def test_get_found(self):
        self.pool.set('key', 'value', noreply=False)
        self.pool.get('key', callback=self.stop)
        result = self.wait()
        self.assertEqual(result, 'value')

    def test_get_many_none_found(self):
        self.pool.get_many(['key1', 'key2'], callback=self.stop)
        result = self.wait()
        self.assertEqual(result, {})

    def test_get_many_some_found(self):
        self.pool.set('key1', 'value1', noreply=False, callback=self.stop)
        self.wait()
        self.pool.get_many(['key1', 'key2'], callback=self.stop)
        result = self.wait()
        self.pool.delete('key1', noreply=False, callback=self.stop)
        self.wait()
        self.assertEqual(result, {'key1': 'value1'})

    def test_get_many_all_found(self):
        self.pool.set('key1', 'value1', noreply=False, callback=self.stop)
        self.wait()
        self.pool.set('key2', 'value2', noreply=False, callback=self.stop)
        self.wait()
        self.pool.get_many(['key1', 'key2'], callback=self.stop)
        result = self.wait()
        self.pool.delete_many(['key1', 'key2'], noreply=False, callback=self.stop)
        self.wait()
        self.assertEqual(result, {'key1': 'value1', 'key2': 'value2'})

    def test_get_unicode_key(self):
        with self.assertRaises(memcache.MemcacheIllegalInputError):
            self.pool.get(u'\u0FFF')

    def test_delete_not_found(self):
        self.pool.delete('key_not_found', noreply=False, callback=self.stop)
        result = self.wait()
        self.assertEqual(result, False)

    def test_delete_found(self):
        self.pool.add('key', 'value', noreply=False, callback=self.stop)
        self.wait()
        self.pool.delete('key', noreply=False, callback=self.stop)
        result = self.wait()
        self.assertTrue(result)

    def test_delete_noreply(self):
        self.pool.delete('key', noreply=True, callback=self.stop)
        result = self.wait()
        self.assertTrue(result)

    def test_incr_not_found(self):
        self.pool.incr('key', 1, noreply=False, callback=self.stop)
        result = self.wait()
        self.assertFalse(result)

    def test_incr_found(self):
        self.pool.set('key', 0, noreply=False, callback=self.stop)
        self.wait()

        self.pool.incr('key', 1, noreply=False, callback=self.stop)
        result = self.wait()
        self.assertEqual(result, 1)

    def test_incr_noreply(self):
        self.pool.set('key', 0, noreply=False, callback=self.stop)
        self.wait()

        self.pool.incr('key', 1, noreply=True, callback=self.stop)
        result = self.wait()
        self.assertTrue(result)

    def test_decr_not_found(self):
        self.pool.decr('key', 1, noreply=False, callback=self.stop)
        result = self.wait()
        self.assertFalse(result)

    def test_decr_found(self):
        self.pool.set('key', 2, noreply=False, callback=self.stop)
        self.wait()

        self.pool.decr('key', 1, noreply=False, callback=self.stop)
        result = self.wait()
        self.assertEqual(result, 1)

    def test_decr_noreply(self):
        self.pool.set('key', 2, noreply=False, callback=self.stop)
        self.wait()

        self.pool.decr('key', 1, noreply=True, callback=self.stop)
        result = self.wait()
        self.assertTrue(result)

    def test_append_stored(self):
        self.pool.set('key', 'value', noreply=True, callback=self.stop)
        self.wait()

        self.pool.append('key', '1', noreply=False, callback=self.stop)
        result = self.wait()
        self.assertTrue(result)

        self.pool.get('key', callback=self.stop)
        result = self.wait()
        self.assertEqual(result, 'value1')

    def test_prepend_stored(self):
        self.pool.set('key', 'value', noreply=True, callback=self.stop)
        self.wait()

        self.pool.prepend('key', '1', noreply=False, callback=self.stop)
        result = self.wait()
        self.assertTrue(result)

        self.pool.get('key', callback=self.stop)
        result = self.wait()
        self.assertEqual(result, '1value')

    def test_cas_stored(self):
        self.pool.set('key', 'value', noreply=True, callback=self.stop)
        self.wait()
        self.pool.gets('key', callback=self.stop)
        _, cas = self.wait()
        self.pool.cas('key', 'value', cas, noreply=False, callback=self.stop)
        result = self.wait()
        self.assertTrue(result)

    def test_cas_exists(self):
        self.pool.set('key', 'value', noreply=True, callback=self.stop)
        self.wait()
        self.pool.gets('key', callback=self.stop)
        _, cas = self.wait()
        self.pool.set('key', 'other_value', noreply=True, callback=self.stop)
        self.wait()
        self.pool.cas('key', 'value', cas, noreply=False, callback=self.stop)
        result = self.wait()
        self.assertFalse(result)

    def test_cas_not_found(self):
        self.pool.cas('key', 'value', 0, noreply=False, callback=self.stop)
        result = self.wait()
        self.assertEquals(result, None)

    def test_flush_all(self):
        self.pool.flush_all('key', noreply=False, callback=self.stop)
        result = self.wait()
        self.assertTrue(result)

    def test_gets_not_found(self):
        self.pool.gets('key', callback=self.stop)
        result = self.wait()
        self.assertEqual(result, (None, None))

    def test_gets_found(self):
        self.pool.set('key', 'value', noreply=True, callback=self.stop)
        self.wait()
        self.pool.gets('key', callback=self.stop)
        result = self.wait()
        self.pool.delete('key', noreply=True, callback=self.stop)
        self.wait()
        self.assertEqual(result[0], 'value')
        self.assertTrue(result[1].isdigit())

    def test_gets_many_none_found(self):
        self.pool.gets_many(['key1', 'key2'], callback=self.stop)
        result = self.wait()
        self.assertEqual(result, {})

    def test_gets_many_some_found(self):
        self.pool.set('key1', 'value1', noreply=False, callback=self.stop)
        self.wait()
        self.pool.gets_many(['key1', 'key2'], callback=self.stop)
        result = self.wait()
        self.pool.delete('key1', noreply=False, callback=self.stop)
        self.wait()
        self.assertTrue('key1' in result and len(result) == 1)
        self.assertTrue(len(result['key1']) == 2 and result['key1'][0] == 'value1')

    def test_gets_many_all_found(self):
        self.pool.set('key1', 'value1', noreply=False, callback=self.stop)
        self.wait()
        self.pool.set('key2', 'value2', noreply=False, callback=self.stop)
        self.wait()
        self.pool.gets_many(['key1', 'key2'], callback=self.stop)
        result = self.wait()
        self.pool.delete_many(['key1', 'key2'], noreply=False, callback=self.stop)
        self.wait()
        self.assertEqual(result.keys(), {'key1': 'value1', 'key2': 'value2'}.keys())

    def test_touch_not_found(self):
        self.pool.touch('key', noreply=False, callback=self.stop)
        result = self.wait()
        self.assertFalse(result)

    def test_touch_found(self):
        self.pool.set('key', 'value', noreply=False, callback=self.stop)
        self.wait()
        self.pool.touch('key', noreply=False, callback=self.stop)
        result = self.wait()
        self.assertTrue(result)

    def test_quit(self):
        self.pool.quit('key', callback=self.stop)
        result = self.wait()
        self.assertTrue(result)

    def test_replace_stored(self):
        # store value
        self.pool.set('key', 'value', noreply=True, callback=self.stop)
        self.wait()
        # now try to replace it
        self.pool.replace('key', 'value', noreply=False, callback=self.stop)
        result = self.wait()
        self.assertTrue(result)

    def test_replace_not_stored(self):
        self.pool.replace('key', 'value', noreply=False, callback=self.stop)
        result = self.wait()
        self.assertFalse(result)

    def test_serialization(self):
        def _ser(key, value):
            return json.dumps(value), 0

        dct = {'a': 'b', 'c': 'd'}
        self.pool.set('key', dct, callback=self.stop)
        self.wait()
        self.pool.get('key', callback=self.stop)
        result = self.wait()
        self.assertEqual(result, dct)

    def test_stats(self):
        self.pool.stats('key', callback=self.stop)
        result = self.wait()
        self.assertTrue('version' in result)

    def test_stats_with_args(self):
        self.pool.stats('key', 'settings', callback=self.stop)
        result = self.wait()
        self.assertTrue(result and 'version' not in result)

    def test_broadcast(self):
        self.pool.broadcast.flush_all(noreply=False, callback=self.stop)
        result = self.wait()
        self.assertTrue(len(result) > 0)
        self.assertTrue(isinstance(result, dict))
        self.assertTrue(all(result.iteritems()))

    def test_broadcast_with_no_port(self):
        pass
