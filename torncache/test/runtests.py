#!/usr/bin/env python
# -*- mode: python; coding: utf-8 -*-

from __future__ import absolute_import

import locale
import textwrap
import tornado.testing

# tornado testing deps
from tornado.options import define
from tornado.test.util import unittest

TEST_MODULES = [
    'torncache.test.test_hello',
]


def all():
    """Force load of all avaliable tests"""
    return unittest.defaultTestLoader.loadTestsFromNames(TEST_MODULES)


class TornadoTextTestRunner(unittest.TextTestRunner):
    def run(self, test):
        result = super(TornadoTextTestRunner, self).run(test)
        if result.skipped:
            skip_reasons = set(reason for (test, reason) in result.skipped)
            self.stream.write(textwrap.fill(
                "Some tests were skipped because: %s" %
                ", ".join(sorted(skip_reasons))))
            self.stream.write("\n")
        return result

if __name__ == '__main__':
    # Allow to override locale
    define('locale', type=str,
           default=None,
           callback=lambda x: locale.setlocale(locale.LC_ALL, x))
    # run tests
    tornado.testing.main(testRunner=TornadoTextTestRunner)
