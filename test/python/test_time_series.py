#
# Copyright 2014-2018 Neueda Ltd.
#
from helium import Ts, HE_O_CREATE, HE_O_VOLUME_CREATE
from cdr import Cdr
import unittest
import os


class TestTimeSeries(unittest.TestCase):
    def setUp(self):
        os.system('truncate -s 2g /tmp/test-ts')
        flags = HE_O_CREATE | HE_O_VOLUME_CREATE
        self.hdb = Ts(url="he://.//tmp/test-ts",
                      datastore='helium',
                      key_type='i',
                      val_type='C', index_field=52,
                      flags=flags)

    def tearDown(self):
        self.hdb.cleanup()
        if os.path.exists('/tmp/test-ts'):
            os.remove('/tmp/test-ts')

    def _check_cdr_match(self, a, b):
        self.assertEqual(sorted(a.keys()), sorted(b.keys()))

        self.assertEqual(a.serialize(), b.serialize())

    def test_insert_one_basic(self):
        d = Cdr()
        d[55] = "AAPL"
        d[52] = 1000
        d[56] = 100

        self.hdb.insert_one(d)

        self.assertEqual(self.hdb.keys(), [1000])
        self._check_cdr_match(self.hdb[1000][0][0], d)

    def test_insert_one_bucket_assignment(self):
        d = Cdr()
        d[55] = "AAPL"
        d[52] = 1005
        d[56] = 100

        self.hdb.insert_one(d)

        self.assertEqual(self.hdb.keys(), [1000])
        self._check_cdr_match(self.hdb[1000][0][0], d)

    def test_insert_one_two_items(self):
        d = Cdr()
        d[55] = "AAPL"
        d[52] = 1005
        d[56] = 100

        e = Cdr()
        e[55] = "FB"
        e[52] = 1006
        e[56] = 100

        self.hdb.insert_one(d)
        self.hdb.insert_one(e)

        self.assertEqual(self.hdb.keys(), [1000])

        self._check_cdr_match(self.hdb[1000][0][0], d)
        self._check_cdr_match(self.hdb[1000][0][1], e)

    def test_find_one(self):
        d = Cdr()
        d[55] = "AAPL"
        d[52] = 1005
        d[56] = 100

        e = Cdr()
        e[55] = "FB"
        e[52] = 1006
        e[56] = 100

        self.hdb.insert_many([d, e])

        x = self.hdb.find({55: "AAPL"})

        self.assertEqual(len(x), 1)

        self._check_cdr_match(x[0], d)

    def test_find(self):
        d = Cdr()
        d[55] = "AAPL"
        d[52] = 1005
        d[56] = 100

        e = Cdr()
        e[55] = "FB"
        e[52] = 1006
        e[56] = 100

        self.hdb.insert_many([d, e])

        x = self.hdb.find({55: "AAPL"})
        self.assertEqual(len(x), 1)

        self._check_cdr_match(x[0], d)

    def test_delete(self):
        d = Cdr()
        d[55] = "AAPL"
        d[52] = 1005
        d[56] = 100

        e = Cdr()
        e[55] = "FB"
        e[52] = 1006
        e[56] = 100

        self.hdb.insert_many([d, e])

        self.hdb.delete({55: "AAPL"})

        x = self.hdb[1000]

        self.assertEqual(len(x[0]), 1)

        y = x[0][0]

        self._check_cdr_match(e, y)
