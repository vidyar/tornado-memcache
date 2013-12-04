from tox._config import parseconfig

print "language: python"
print "python: 2.7"
print "services:"
print "  - memcached"
print "env:"
for env in parseconfig(None, 'tox').envlist:
    print "  - MEMCACHED_URL=%s TOX_ENV=%s" % ('mc://localhost:11211', env)
print "install:"
print "  - pip install tox"
print "script:"
print "  - tox -e $TOX_ENV"
