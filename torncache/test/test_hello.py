# -*- mode: python; coding: utf-8 -*-

from tornado.testing import AsyncTestCase


class Hello(AsyncTestCase):
    def test_hello(self):
        """Simple Hello World TC"""
        self.assertTrue(True)
