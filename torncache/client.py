# -*- mode: python; coding: utf-8 -*-

"""
Tornado Memcached
"""

import weakref
import socket
import time
import logging
import itertools
import functools
import collections

# For MC url parsing
try:
    import urlparse  # py2
except ImportError:
    basestring = str
    import urllib.parse as urlparse  # py3

from tornado import iostream
from tornado import stack_context
from tornado.ioloop import IOLoop
from tornado.gen import engine, Task

VALID_STORE_RESULTS = {
    'set':     ('STORED',),
    'add':     ('STORED', 'NOT_STORED'),
    'replace': ('STORED', 'NOT_STORED'),
    'append':  ('STORED', 'NOT_STORED'),
    'prepend': ('STORED', 'NOT_STORED'),
    'cas':     ('STORED', 'EXISTS', 'NOT_FOUND'),
}


# Some of the values returned by the "stats" command
# need mapping into native Python types
STAT_TYPES = {
    # General stats
    'version': str,
    'rusage_user': lambda value: float(value.replace(':', '.')),
    'rusage_system': lambda value: float(value.replace(':', '.')),
    'hash_is_expanding': lambda value: int(value) != 0,
    'slab_reassign_running': lambda value: int(value) != 0,

    # Settings stats
    'inter': str,
    'evictions': lambda value: value == 'on',
    'growth_factor': float,
    'stat_key_prefix': str,
    'umask': lambda value: int(value, 8),
    'detail_enabled': lambda value: int(value) != 0,
    'cas_enabled': lambda value: int(value) != 0,
    'auth_enabled_sasl': lambda value: value == 'yes',
    'maxconns_fast': lambda value: int(value) != 0,
    'slab_reassign': lambda value: int(value) != 0,
    'slab_automove': lambda value: int(value) != 0,
}


class MemcacheError(Exception):
    "Base exception class"


class MemcachePoolError(MemcacheError):
    """Raised when number of clients excees size"""


class MemcacheTimeoutError(MemcacheError):
    """Timeout when connecting or running and operation"""


class MemcacheClientError(MemcacheError):
    """Raised when memcached fails to parse the arguments to a request, likely
    due to a malformed key and/or value, a bug in this library, or a version
    mismatch with memcached."""


class MemcacheUnknownCommandError(MemcacheClientError):
    """Raised when memcached fails to parse a request, likely due to a bug in
    this library or a version mismatch with memcached."""


class MemcacheIllegalInputError(MemcacheClientError):
    """Raised when a key or value is not legal for Memcache (see the class docs
    for Client for more details)."""


class MemcacheServerError(MemcacheError):
    """Raised when memcached reports a failure while processing a request,
    likely due to a bug or transient issue in memcached."""


class MemcacheUnknownError(MemcacheError):
    """Raised when this library receives a response from memcached that it
    cannot parse, likely due to a bug in this library or a version mismatch
    with memcached."""


class MemcacheUnexpectedCloseError(MemcacheServerError):
    "Raised when the connection with memcached closes unexpectedly."


class ClientPool(object):
    """A Pool of clients"""

    def __init__(self, servers, size=0, **kwargs):
        self._servers = servers
        self._size = size
        self._used = collections.deque()
        self._clients = collections.deque()
        # Client arguments
        self._kwargs = kwargs

    def _create_clients(self, n):
        servers = self._servers
        if isinstance(servers, basestring):
            servers = []
            for server in self._servers.split(','):
                # parse url form 'mc://host:port?<weight>='
                if self._servers.startswith('mc'):
                    url = urlparse.urlsplit(server)
                    server = url.netloc
                    if url.query:
                        weight = urlparse.parse_qs(url.query).get('weight', 1)
                        server = [server, weight]
                servers.append(server)
        # create clients
        return [Client(servers, **self._kwargs) for x in xrange(n)]

    def _invoke(self, cmd, *args, **kwargs):
        def on_finish(response, c, _cb, **kwargs):
            self._used.remove(c)
            self._clients.append(c)
            _cb and _cb(response, **kwargs)
        if not self._clients:
            # Add a new client
            total_clients = len(self._clients) + len(self._used)
            if self._size > 0 and total_clients >= self._size:
                error = "Max of %d clients is already reached" % self._size
                raise MemcachePoolError(error)
            self._clients.append(*self._create_clients(1))
        # fetch available one
        client = self._clients.popleft()
        self._used.append(client)
        # override used callback to
        cb = kwargs.get('callback')
        kwargs['callback'] = functools.partial(on_finish, c=client, _cb=cb)
        getattr(client, cmd)(*args, **kwargs)

    def __getattr__(self, name):
        if hasattr(Client, name):
            return functools.partial(self._invoke, name)
        # raise error
        raise AttributeError(name)


class Client(object):
    """
    Object representing a pool of memcache servers.
    """

    CLIENTS = weakref.WeakKeyDictionary()

    def __init__(self, servers, ioloop=None,
                 serializer=None, deserializer=None,
                 connect_timeout=5, timeout=1, no_delay=True,
                 ignore_exc=False, dead_retry=30,
                 server_retries=10):

        # Watcher to destroy client when ioloop expires
        self._ioloop = ioloop or IOLoop.instance()
        self.CLIENTS[self._ioloop] = self

        self._server_retries = server_retries
        self._server_args = {
            'ioloop': self._ioloop,
            'serializer': serializer,
            'deserializer': deserializer,
            'connect_timeout': connect_timeout,
            'timeout': timeout,
            'no_delay': no_delay,
            'ignore_exc': ignore_exc,
            'dead_retry': dead_retry
        }

        # servers
        self._servers = []
        self._buckets = []
        # Servers can be passed in two forms:
        #    1. Strings of the form C{"host:port"}, which implies a
        #    default weight of 1.
        #    2. Tuples of the form C{("host:port", weight)}, where C{weight} is
        #    an integer weight value.
        for server in servers:
            server = Connection(server, **self._server_args)
            for i in xrange(server.weight):
                self._buckets.append(server)
            self._servers.append(server)

    def _get_server(self, key):
        """Fetch valid MC for this key"""
        serverhash = 0
        if isinstance(key, tuple):
            serverhash, key = key[:2]
        elif len(self._buckets) > 1:
            serverhash = hash(key)
        # get pair server, key
        return (self._buckets[serverhash % len(self._buckets)], key)

    def set(self, key, value, expire=0, noreply=True, callback=None):
        """
        The memcached "set" command.

        Args:
          key: str, see class docs for details.
          value: str, see class docs for details.
          expire: optional int, number of seconds until the item is expired
                  from the cache, or zero for no expiry (the default).
          noreply: optional bool, True to not wait for the reply (the default).

        Returns:
          If no exception is raised, always returns True. If an exception is
          raised, the set may or may not have occurred. If noreply is True,
          then a successful return does not guarantee a successful set.
        """
        # Fetch memcached connection
        server, key = self._get_server(key)
        if not server:
            callback and callback(None)
            return
        # invoke
        server.store_cmd('set', key, expire, noreply, value, None, callback)

    def set_many(self, values, expire=0, noreply=True, callback=None):
        """A convenience function for setting multiple values.

        Args:
          values: dict(str, str), a dict of keys and values, see class docs
                  for details.
          expire: optional int, number of seconds until the item is expired
                  from the cache, or zero for no expiry (the default).
          noreply: optional bool, True to not wait for the reply (the default).

        Returns:
          Returns a dictionary of keys and operations result
          values. For each entry, if no exception is raised, always
          returns True. If an exception is raised, the set may or may
          not have occurred. If noreply is True, then a successful
          return does not guarantee a successful set. If no server is
          present, None is returned.
        """
        # response handler
        def on_response(key, result):
            retval[key] = result
            if len(retval) == len(values):
                callback and callback(retval)

        # init vars
        retval, servers = dict(), dict()
        for key, value in values.iteritems():
            server, key = self._get_server(key)
            servers[key] = server
        # set it
        for key, server in servers.iteritems():
            if server is None:
                on_response(key, False)
                continue
            cb = stack_context.wrap(functools.partial(on_response, key))
            self.set(key, value, expire, noreply, callback=cb)

    def add(self, key, value, expire=0, noreply=True, callback=None):
        """
        The memcached "add" command.

        Args:
          key: str, see class docs for details.
          value: str, see class docs for details.
          expire: optional int, number of seconds until the item is expired
                  from the cache, or zero for no expiry (the default).
          noreply: optional bool, True to not wait for the reply (the default).

        Returns:
          If noreply is True, the return value is always True. Otherwise the
          return value is True if the value was stgored, and False if it was
          not (because the key already existed).
        """
        # Fetch memcached connection
        server, key = self._get_server(key)
        if not server:
            callback and callback(None)
            return
        # invoke
        server.store_cmd('add', key, expire, noreply, value, None, callback)

    def replace(self, key, value, expire=0, noreply=True, callback=None):
        """
        The memcached "replace" command.

        Args:
          key: str, see class docs for details.
          value: str, see class docs for details.
          expire: optional int, number of seconds until the item is expired
                  from the cache, or zero for no expiry (the default).
          noreply: optional bool, True to not wait for the reply (the default).

        Returns:
          If noreply is True, always returns True. Otherwise returns True if
          the value was stored and False if it wasn't (because the key didn't
          already exist).
        """
        # Fetch memcached connection
        server, key = self._get_server(key)
        if not server:
            callback and callback(None)
            return
        # invoke
        server.store_cmd('replace', key, expire, noreply, value, None, callback)

    def append(self, key, value, expire=0, noreply=True):
        """
        The memcached "append" command.

        Args:
          key: str, see class docs for details.
          value: str, see class docs for details.
          expire: optional int, number of seconds until the item is expired
                  from the cache, or zero for no expiry (the default).
          noreply: optional bool, True to not wait for the reply (the default).

        Returns:
          True.
        """
        return self._store_cmd('append', key, expire, noreply, value)

    def prepend(self, key, value, expire=0, noreply=True):
        """
        The memcached "prepend" command.

        Args:
          key: str, see class docs for details.
          value: str, see class docs for details.
          expire: optional int, number of seconds until the item is expired
                  from the cache, or zero for no expiry (the default).
          noreply: optional bool, True to not wait for the reply (the default).

        Returns:
          True.
        """
        return self._store_cmd('prepend', key, expire, noreply, value)

    def cas(self, key, value, cas, expire=0, noreply=False):
        """
        The memcached "cas" command.

        Args:
          key: str, see class docs for details.
          value: str, see class docs for details.
          cas: int or str that only contains the characters '0'-'9'.
          expire: optional int, number of seconds until the item is expired
                  from the cache, or zero for no expiry (the default).
          noreply: optional bool, False to wait for the reply (the default).

        Returns:
          If noreply is True, always returns True. Otherwise returns None if
          the key didn't exist, False if it existed but had a different cas
          value and True if it existed and was changed.
        """
        return self._store_cmd('cas', key, expire, noreply, value, cas)

    def get(self, key, callback):
        """
        The memcached "get" command, but only for one key, as a convenience.

        Args:
          key: str, see class docs for details.

        Returns:
          The value for the key, or None if the key wasn't found.
        """
        server, key = self._get_server(key)
        if not server:
            callback(None)
            return

        cb = lambda x: callback(x.get(key, None))
        server.fetch_cmd('get', [key], False, callback=cb)

    def get_many(self, keys):
        """
        The memcached "get" command.

        Args:
          keys: list(str), see class docs for details.

        Returns:
          A dict in which the keys are elements of the "keys" argument list
          and the values are values from the cache. The dict may contain all,
          some or none of the given keys.
        """
        if not keys:
            return {}

        return self._fetch_cmd('get', keys, False)

    def gets(self, key):
        """
        The memcached "gets" command for one key, as a convenience.

        Args:
          key: str, see class docs for details.

        Returns:
          A tuple of (key, cas), or (None, None) if the key was not found.
        """
        return self._fetch_cmd('gets', [key], True).get(key, (None, None))

    def gets_many(self, keys):
        """
        The memcached "gets" command.

        Args:
          keys: list(str), see class docs for details.

        Returns:
          A dict in which the keys are elements of the "keys" argument list and
          the values are tuples of (value, cas) from the cache. The dict may
          contain all, some or none of the given keys.
        """
        if not keys:
            return {}

        return self._fetch_cmd('gets', keys, True)

     # def gets(self, keys, callback):
     #     '''Retrieves several keys from the memcache.

     #     @return: The value list
     #     '''
     #     servers = dict()
     #     for key in keys:
     #         server, key = self._get_server(key)
     #         if not server:
     #             raise _Error('No available server for %s' % key)
     #         if server in servers:
     #             servers[server].append(key)
     #         else:
     #             servers[server] = [key]

     #     self._statlog('gets')

     #     gets_stat = {'server':servers.keys(), 'finished':0, 'result':{} }

     #     for server in servers:
     #         server.send("get %s" % ' '.join(servers[server]), functools.partial(self._gets_send_cb, server=server, status=gets_stat, callback=self._set_timeout(server, callback)))

     # def _gets_send_cb(self, server, status, callback):
     #     self._expectvalue(server, line=None, callback=functools.partial(self._gets_expectval_cb, server=server, status=status, callback=callback))

     # def _gets_expectval_cb(self, rkey, flags, rlen, server, status, callback):
     #     if not rkey:
     #         status['finished'] += 1
     #         if status['finished'] == len(status['server']):
     #             self.finish(functools.partial(callback,status['result']))
     #         return
     #     self._recv_value(server, flags, rlen, functools.partial(self._gets_recv_cb, key=rkey, server=server, status=status, callback=callback))

     # def _gets_recv_cb(self, value, key, server, status, callback):
     #     status['result'][key] = value
     #     self._expectvalue(server, line=None, callback=functools.partial(self._gets_expectval_cb, server=server, status=status, callback=callback))

    def _expect(self, data, expected, callback):
        if isinstance(data, basestring) and data.endswith('\r\n'):
            data = data[:-2]
        callback(data == expected)

    # def gets_many(self, keys, callback):
    #     """
    #     The memcached "gets" command.

    #     Args:
    #       keys: list(str), see class docs for details.

    #     Returns:
    #       A dict in which the keys are elements of the "keys" argument list and
    #       the values are tuples of (value, cas) from the cache. The dict may
    #       contain all, some or none of the given keys.
    #     """
    #     if not keys:
    #         callback({})
    #         return

    #     return self._fetch_cmd('gets', keys, True)

    # def gets(self, keys, callback):
    #     '''Retrieves several keys from the memcache.

    #     @return: The value list
    #     '''
    #     servers = dict()

    #     for key in keys:
    #         server, key = self._get_server(key)
    #         if not server:
    #             logging.warning('No available server for %s' % key)
    #         servers.setdefault(server, [])
    #         servers[server].append(key)

    #      gets_stat = {
    #          'server':servers.keys(),
    #          'finished':0,
    #          'result':{}
    #      }

    #      for server in servers:
    #          cmd = "get %s" % ' '.join(servers[server])
    #          server.fetch_cmd(cmd)
    #          server.send(cmd), functools.partial(self._gets_send_cb, server=server, status=gets_stat, callback=self._set_timeout(server, callback)))

    def delete(self, key, time=0, noreply=True, callback=None):
        """
        The memcached "delete" command.

        Args:
          key: str, see class docs for details.

        Returns:
          If noreply is True, always returns True. Otherwise returns True if
          the key was deleted, and False if it wasn't found.
        """
        # Fetch memcached connection
        server, key = self._get_server(key)
        if not server:
            callback and callback(None)
            return
        # compute command
        timearg = ' {0}'.format(time) if time else ''
        replarg = ' noreply' if noreply else ''
        cmd = 'delete {0}{1}{2}\r\n'.format(key, timearg, replarg)

        # invoke
        cb = lambda x: self._expect(x, 'DELETED', callback)
        cb = callback if noreply else cb
        server.misc_cmd(cmd, 'delete', noreply, callback=cb)

    def delete_many(self, keys, noreply=True):
        """
        A convenience function to delete multiple keys.

        Args:
          keys: list(str), the list of keys to delete.

        Returns:
          True. If an exception is raised then all, some or none of the keys
          may have been deleted. Otherwise all the keys have been sent to
          memcache for deletion and if noreply is False, they have been
          acknowledged by memcache.
        """
        if not keys:
            return True

        # TODO: make this more performant by sending all keys first, then
        # waiting for all values.
        for key in keys:
            self.delete(key, noreply)

        return True

    def incr(self, key, value, noreply=False):
        """
        The memcached "incr" command.

        Args:
          key: str, see class docs for details.
          value: int, the amount by which to increment the value.
          noreply: optional bool, False to wait for the reply (the default).

        Returns:
          If noreply is True, always returns None. Otherwise returns the new
          value of the key, or None if the key wasn't found.
        """
        cmd = "incr {0} {1}{2}\r\n".format(
            key,
            str(value),
            ' noreply' if noreply else '')
        result = self._misc_cmd(cmd, 'incr', noreply)
        if noreply:
            return None
        if result == 'NOT_FOUND':
            return None
        return int(result)

    def decr(self, key, value, noreply=False):
        """
        The memcached "decr" command.

        Args:
          key: str, see class docs for details.
          value: int, the amount by which to increment the value.
          noreply: optional bool, False to wait for the reply (the default).

        Returns:
          If noreply is True, always returns None. Otherwise returns the new
          value of the key, or None if the key wasn't found.
        """
        cmd = "decr {0} {1}{2}\r\n".format(
            key,
            str(value),
            ' noreply' if noreply else '')
        result = self._misc_cmd(cmd, 'decr', noreply)
        if noreply:
            return None
        if result == 'NOT_FOUND':
            return None
        return int(result)

    def touch(self, key, expire=0, noreply=True):
        """
        The memcached "touch" command.

        Args:
          key: str, see class docs for details.
          expire: optional int, number of seconds until the item is expired
                  from the cache, or zero for no expiry (the default).
          noreply: optional bool, True to not wait for the reply (the default).

        Returns:
          True if the expiration time was updated, False if the key wasn't
          found.
        """
        cmd = "touch {0} {1}{2}\r\n".format(
            key,
            expire,
            ' noreply' if noreply else '')
        result = self._misc_cmd(cmd, 'touch', noreply)
        if noreply:
            return True
        return result == 'TOUCHED'

    def stats(self, *args):
        """
        The memcached "stats" command.

        The returned keys depend on what the "stats" command returns.
        A best effort is made to convert values to appropriate Python
        types, defaulting to strings when a conversion cannot be made.

        Args:
          *arg: extra string arguments to the "stats" command. See the
                memcached protocol documentation for more information.

        Returns:
          A dict of the returned stats.
        """
        result = self._fetch_cmd('stats', args, False)

        for key, value in result.iteritems():
            converter = STAT_TYPES.get(key, int)
            try:
                result[key] = converter(value)
            except Exception:
                pass

        return result

    def flush_all(self, delay=0, noreply=True):
        """
        The memcached "flush_all" command.

        Args:
          delay: optional int, the number of seconds to wait before flushing,
                 or zero to flush immediately (the default).
          noreply: optional bool, True to not wait for the response (the default).

        Returns:
          True.
        """
        cmd = "flush_all {0}{1}\r\n".format(delay, ' noreply' if noreply else '')
        result = self._misc_cmd(cmd, 'flush_all', noreply)
        if noreply:
            return True
        return result == 'OK'

    def quit(self):
        """
        The memcached "quit" command.

        This will close the connection with memcached. Calling any other
        method on this object will re-open the connection, so this object can
        be re-used after quit.
        """
        cmd = "quit\r\n"
        self._misc_cmd(cmd, 'quit', True)
        self.close()


class Connection:
    """ A Client connection to a Server"""

    def __init__(self, host, ioloop=None, serializer=None, deserializer=None,
                 connect_timeout=5, timeout=1, no_delay=True, ignore_exc=False,
                 dead_retry=30):

        # Parse host conf and weight
        self.weight = 1
        if isinstance(host, tuple):
            host, self.weight = host

        # Parse host port
        self.ip, self.port = host, 11211
        if ":" in host:
            self.ip, _, self.port = host.partition(":")
            self.port = int(self.port)

        # Protected data
        self._ioloop = ioloop or IOLoop.instance()
        self._ignore_exc = ignore_exc

        # Timeouts
        self._timeout = None
        self._request_timeout = timeout
        self._connect_timeout = connect_timeout

        # Data
        self._serializer = serializer
        self._deserializer = deserializer

        # Connections properites
        self._stream = None
        self._no_delay = no_delay
        self._dead_until = 0
        self._dead_retry = dead_retry
        self._connect_callbacks = []

    def __str__(self):
        retval = "%s:%d" % (self.ip, self.port)
        if self._dead_until:
            retval += " (dead until %d)" % self._dead_until
        return retval

    def _raise_errors(self, line, name):
        if line.startswith('ERROR'):
            raise MemcacheUnknownCommandError(name)

        if line.startswith('CLIENT_ERROR'):
            error = line[line.find(' ') + 1:]
            raise MemcacheClientError(error)

        if line.startswith('SERVER_ERROR'):
            error = line[line.find(' ') + 1:]
            raise MemcacheServerError(error)

    def _add_timeout(self, reason):
        """Add a timeout handler"""
        def on_timeout():
            self._timeout = None
            self.mark_dead(reason)
            raise MemcacheTimeoutError(reason)

        if self._request_timeout:
            self._clear_timeout()
            self._timeout = self._ioloop.add_timeout(
                time.time() + self._request_timeout,
                stack_context.wrap(on_timeout))

    def _clear_timeout(self):
        if self._timeout is not None:
            self._ioloop.remove_timeout(self._timeout)
            self._timeout = None

    def mark_dead(self, reason):
        """Quarintine MC server for a period of time"""
        logging.warning("Marking dead %s: '%s'" % (self, reason))
        self._dead_until = time.time() + self._dead_retry
        self._clear_timeout()
        self.close()

    def connect(self, callback=None):
        """Open a connection to MC server"""

        def on_timeout(reason):
            self._timeout = None
            self.mark_dead(reason)
            raise MemcacheTimeoutError(reason)

        def on_close():
            if self._stream and self._stream.error:
                logging.error(self._stream.error)
            self._stream = None
            self._connect_callbacks = []

        def on_connect():
            self._clear_timeout()
            for callback in self._connect_callbacks:
                callback and callback(self)
            self._connect_callbacks = None

        # Check if server is dead
        if self._dead_until > time.time():
            msg = "Server {0} will stay dead next {1} secs"
            msg = msg.format(self, self._dead_until - time.time())
            raise MemcacheClientError(msg)
        self._dead_until = 0

        # Check we are already connected
        if self._connect_callbacks is None:
            callback and callback(self)
            return
        self._connect_callbacks.append(callback)
        if self._stream and not self._stream.closed():
            return

        # Connection closed. clean and start again
        self.close()

        # Set timeout
        if self._connect_timeout:
            timeout_func = functools.partial(on_timeout, "Connection Timeout")
            self._timeout = self._ioloop.add_timeout(
                time.time() + self._connect_timeout,
                stack_context.wrap(timeout_func))

        # now connect
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if self._no_delay:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self._stream = iostream.IOStream(sock, io_loop=self._ioloop)
        self._stream.set_close_callback(on_close)
        self._stream.connect((self.ip, self.port), callback=on_connect)

    def send(self, cmd, callback):
        """Send a MC command"""
        self._stream.write(cmd + "\r\n", callback)

    @engine
    def fetch_cmd(self, name, keys, expect_cas, callback):
        # build command
        try:
            key_strs = []
            for key in keys:
                key = str(key)
                if ' ' in key:
                    error = "Key contains spaces: {0}".format(key)
                    raise MemcacheIllegalInputError(error)
                key_strs.append(key)
        except UnicodeEncodeError as e:
            raise MemcacheIllegalInputError(str(e))

        # Open connection if required
        if self.closed:
            yield Task(self.connect)

        # Add timeout for this request
        self._add_timeout("Timeout on fetch '{0}'".format(name))

        try:
            # send command
            cmd = '{0} {1}\r\n'.format(name, ' '.join(key_strs))
            _ = yield Task(self._stream.write, cmd)

            # parse response
            result = {}
            while True:
                line = yield Task(self._stream.read_until, "\r\n")
                line = line[:-2]
                self._raise_errors(line, name)

                if line == 'END':
                    break
                elif line.startswith('VALUE'):
                    if expect_cas:
                        _, key, flags, size, cas = line.split()
                    else:
                        _, key, flags, size = line.split()
                    # read also \r\n
                    value = yield Task(self._stream.read_bytes, int(size) + 2)
                    value = value[:-2]
                    if self._deserializer:
                        value = self._deserializer(key, value, int(flags))
                    if expect_cas:
                        result[key] = (value, cas)
                    else:
                        result[key] = value
                elif name == 'stats' and line.startswith('STAT'):
                    _, key, value = line.split()
                    result[key] = value
                else:
                    raise MemcacheUnknownError(line[:32])
        except Exception as err:
            self.mark_dead(err.message)
            if self._ignore_exc:
                self._clear_timeout()
                callback({})
                return
            raise
        #return result
        self._clear_timeout()
        callback(result)

    @engine
    def store_cmd(self, name, key, expire, noreply, data,
                  cas=None, callback=None):
        try:
            # process key
            key = str(key)
            if ' ' in key:
                raise MemcacheIllegalInputError("Key contains spaces: %s", key)
            # process data
            flags = 0
            if self._serializer:
                data, flags = self.serializer(key, data)
            data = str(data)
        except UnicodeEncodeError as e:
            raise MemcacheIllegalInputError(str(e))

        # Open connection if required
        if self.closed:
            yield Task(self.connect)

        # Add timeout for this request
        self._add_timeout("Timeout on fetch '{0}'".format(name))

        if cas is not None and noreply:
            extra = ' {0} noreply'.format(cas)
        if cas is not None and not noreply:
            extra = ' {0}'.format(cas)
        elif cas is None and noreply:
            extra = ' noreply'
        else:
            extra = ''

        cmd = '{0} {1} {2} {3} {4}{5}\r\n{6}\r\n'.format(
            name, key, flags, expire, len(data), extra, data)

        try:
            yield Task(self._stream.write, cmd)
            if noreply:
                self._clear_timeout()
                callback and callback(True)
                return

            line = yield Task(self._stream.read_until, "\r\n")
            line = line[:-2]
            self._raise_errors(line, name)
            self._clear_timeout()

            if line in VALID_STORE_RESULTS[name]:
                if line == 'STORED':
                    callback(True)
                elif line == 'NOT_STORED':
                    callback(False)
                elif line == 'NOT_FOUND':
                    callback(None)
                elif line == 'EXISTS':
                    callback(False)
            else:
                raise MemcacheUnknownError(line[:32])
        except Exception as err:
            self.mark_dead(err.message)
            raise

    @engine
    def misc_cmd(self, cmd, cmd_name, noreply, callback=None):
        # Open connection if required
        if self.closed:
            yield Task(self.connect)

        # Add timeout for this request
        self._add_timeout("Timeout on misc '{0}'".format(cmd_name))

        try:
            # send command
            yield Task(self._stream.write, cmd)

            if noreply:
                self._clear_timeout()
                callback and callback(True)
                return

            # wait for response
            line = yield Task(self._stream.read_until, "\r\n")
            self._raise_errors(line, cmd_name)
            self._clear_timeout()
        except Exception as err:
            self.mark_dead(err.message)
            raise
        # invoke
        callback and callback(line)

    def read(self, rlen, callback):
        """Read operation"""
        self._stream.read_bytes(rlen, callback)

    def readline(self, callback):
        """Read a line"""
        self._stream.read_until("\r\n", callback)

    def expect(self, text, callback):
        """Read a line and compare response with text"""
        def _on_response(data):
            if data[:-2] != text:
                msg = "'%s' expected but '%s' received" % (text, data)
                logging.warning(msg)
            callback(data)
        self.readline(_on_response)

    def close(self):
        """Close connection to MC"""
        self._stream and self._stream.close()

    def closed(self):
        """Check connection status"""
        if not self._stream:
            return True
        return self._stream and self._stream.closed()
