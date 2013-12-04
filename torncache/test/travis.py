from tox._config import parseconfig

print "language: python"
print "python: 2.7"
print "services:"
print "  - memcached"
print "env:"
print "  - MEMCACHED_URL=%s" % 'mc://localhost:11211?protocol=text&weight=1'
for env in parseconfig(None, 'tox').envlist:
    print "  - TOX_ENV=%s" % env
print "install:"
print "  - pip install tox"
print "script:"
print "  - tox -e $TOX_ENV"
