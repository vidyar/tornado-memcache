torncache
=========

[![Build Status](https://travis-ci.org/inean/tornado-memcache.png?branch=master)](https://travis-ci.org/inean/tornado-memcache)

A comprehensive, fast, async pure-Python memcached client library for
tornado.

Basic Usage:
------------

 from pymemcache.client import Client

 client = Client(('localhost', 11211))
 client.set('some_key', 'some_value')
 result = client.get('some_key')


Serialization:
--------------

 import json
 from pymemcache.client import Client

 def json_serializer(key, value):
     if type(value) == str:
         return value, 1
     return json.dumps(value), 2

 def json_deserializer(key, value, flags):
     if flags == 1:
         return value
     if flags == 2:
         return json.loads(value)
     raise Exception("Unknown serialization format")

 client = Client(('localhost', 11211), serializer=json_serializer,
                 deserializer=json_deserializer)
 client.set('key', {'a':'b', 'c':'d'})
 result = client.get('key')


Best Practices:
---------------

 - Always set the connect_timeout and timeout arguments in the constructor to
   avoid blocking your process when memcached is slow.
 - Use the "noreply" flag for a significant performance boost. The "noreply"
   flag is enabled by default for "set", "add", "replace", "append", "prepend",
   and "delete". It is disabled by default for "cas", "incr" and "decr". It
   obviously doesn't apply to any get calls.
 - Use get_many and gets_many whenever possible, as they result in less
   round trip times for fetching multiple keys.
 - Use the "ignore_exc" flag to treat memcache/network errors as cache misses
   on calls to the get* methods. This prevents failures in memcache, or network
   errors, from killing your web requests. Do not use this flag if you need to
   know about errors from memcache, and make sure you have some other way to
   detect memcache server failures.

Keys and Values:
----------------

Keys must have a __str__() method which should return a str with no
more than 250 ASCII characters and no whitespace or control
characters. Unicode strings must be encoded (as UTF-8, for example)
unless they consist only of ASCII characters that are neither
whitespace nor control characters.

Values must have a __str__() method to convert themselves to a byte
string. Unicode objects can be a problem since str() on a Unicode
object will attempt to encode it as ASCII (which will fail if the
value contains code points larger than U+127). You can fix this will a
serializer or by just calling encode on the string (using UTF-8, for
instance).

If you intend to use anything but str as a value, it is a good idea to
use a serializer and deserializer. The pymemcache.serde library has
some already implemented serializers, including one that is compatible
with the python-memcache library.

Serialization and Deserialization:
----------------------------------

The constructor takes two optional functions, one for "serialization"
of values, and one for "deserialization". The serialization function
takes two arguments, a key and a value, and returns a tuple of two
elements, the serialized value, and an integer in the range 0-65535
(the "flags"). The deserialization function takes three parameters, a
key, value and flags and returns the deserialized value.

Here is an example using JSON for non-str values:

    def serialize_json(key, value):
        if type(value) == str:
            return value, 1
        return json.dumps(value), 2

    def deserialize_json(key, value, flags):
        if flags == 1:
            return value
        if flags == 2:
            return json.loads(value)
        raise Exception("Unknown flags for value: {1}".format(flags))

Error Handling:
---------------

All of the methods in this class that talk to memcached can throw one
of the following exceptions:

 * MemcacheUnknownCommandError
 * MemcacheClientError
 * MemcacheServerError
 * MemcacheUnknownError
 * MemcacheUnexpectedCloseError
 * MemcacheIllegalInputError
 * socket.timeout
 * socket.error

Instances of this class maintain a persistent connection to memcached
which is terminated when any of these exceptions are raised. The next
call to a method on the object will result in a new connection being
made to memcached.
