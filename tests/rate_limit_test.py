#!/usr/bin/env python
# -*- coding: utf-8 -*-
import multiprocessing
import unittest
import time

import redis_rate_limit

from redis_rate_limit import RateLimit, TooManyRequests


class TestRedisRateLimit(unittest.TestCase):
    def setUp(self):
        """
        Initialises Rate Limit class and delete all keys from Redis.
        """
        self.rate_limit = RateLimit(resource='test', client='localhost', blocking=False,
                                    max_requests=10)
        self.rate_limit._reset()

    def _make_10_requests(self):
        """
        Increments usage ten times.
        """
        for x in range(0, 10):
            with self.rate_limit:
                pass

    def test_limit_10_max_request(self):
        """
        Should raise TooManyRequests Exception when trying to increment for the
        eleventh time.
        """
        self.assertEqual(self.rate_limit.get_usage(), 0)
        self.assertEqual(self.rate_limit.has_been_reached(), False)

        self._make_10_requests()
        self.assertEqual(self.rate_limit.get_usage(), 10)
        self.assertEqual(self.rate_limit.has_been_reached(), True)

        with self.assertRaises(TooManyRequests):
            with self.rate_limit:
                pass

        self.assertEqual(self.rate_limit.get_usage(), 10)
        self.assertEqual(self.rate_limit.has_been_reached(), True)

    def test_expire(self):
        """
        Should not raise TooManyRequests Exception when trying to increment for
        the eleventh time after the expire time.
        """
        self._make_10_requests()
        time.sleep(1)
        with self.rate_limit:
            pass

    def test_not_expired(self):
        """
        Should raise TooManyRequests Exception when the expire time has not
        been reached yet.
        """
        self.rate_limit = RateLimit(resource='test', client='localhost', blocking=False,
                                    max_requests=10, expire=2)
        self._make_10_requests()
        time.sleep(1)
        with self.assertRaises(TooManyRequests):
            with self.rate_limit:
                pass

    def test_acquire_with_blocking(self):
        """
        Should not raise TooManyRequests Exception when the quota is over, instead,
        must wait for quota to restore and re-acquire from it
        """
        self.rate_limit = RateLimit(resource='test', client='localhost',
                                    max_requests=10)
        self._make_10_requests()
        with self.rate_limit:
            pass
            self.assertLessEqual(self.rate_limit.get_usage(), 10)

    def another_thread(self):
        self.rate_limit.blocking = True
        self._make_10_requests()

    def test_timeout_when_acquire_with_blocking(self):
        """
        Should raise Timeout Exception when the quota is over and
        we wait too long for it to become available
        """
        self.rate_limit = RateLimit(resource='test', client='localhost',
                                    max_requests=10, expire=5, blocking=False)
        self.rate_limit_2 = RateLimit(resource='test', client='localhost',
                                      max_requests=10, expire=5)
        self.rate_limit_2._acquire_check_interval = 3
        self.rate_limit_2._acquire_timeout = 5

        with self.rate_limit:  # start quota
            pass
        time.sleep(3)
        with self.assertRaises(TooManyRequests):
            self._make_10_requests()  # exceed quota
        # By this time there is 100% quota usage and 2 secs before quota recovery
        self.assertEqual(self.rate_limit.acquire_attempt, 0)

        proc = multiprocessing.Process(target=self.another_thread)
        proc.start()

        with self.assertRaises(redis_rate_limit.QuotaTimeout):
            with self.rate_limit_2:
                # Try to acquire. first attempt fails, next attempt in 3 secs
                # But after 2 secs quota restores and another instance use it up immediately
                # So after another second this instance checks again and see no quota available
                # Next time it checks is after another 3 secs 3+3=6 > 5 (max wait time)
                pass
        proc.join()

    def test_nested_not_allowed(self):
        """
        Should raise GaveUp Exception when tryig to enter the context when already in the context for the same instance
        """
        with self.assertRaises(redis_rate_limit.GaveUp):
            with self.rate_limit:
                with self.rate_limit:
                    pass

    def test_acquire_callbacks(self):
        def cb(rl):
            rl.callback_called = True

        self.rate_limit.add_post_enter_callback(cb)
        with self.rate_limit:
            self.assertEquals(self.rate_limit.callback_called, True)
        self.assertEquals(self.rate_limit.post_enter_callbacks, set())



if __name__ == '__main__':
    unittest.main()

