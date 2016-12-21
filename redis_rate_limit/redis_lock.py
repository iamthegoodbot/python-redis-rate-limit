import time


class Lock(object):
    def __init__(self, connection, key, expires=60, timeout=10, check_interval=1):
        """
        Distributed locking using Redis SETNX and GETSET.

        Usage::

            with Lock('my_lock'):
                print "Critical section"

        :param  expires     We consider any existing lock older than
                            ``expires`` seconds to be invalid in order to
                            detect crashed clients. This value must be higher
                            than it takes the critical section to execute.
        :param  timeout     If another client has already obtained the lock,
                            sleep for a maximum of ``timeout`` seconds before
                            giving up. A value of 0 means we never wait.
        :param  check_interval While waiting for lock, check each this many seconds
        """

        self.key = key
        self.timeout = timeout
        self.expires = expires
        self.check_interval = check_interval
        self.r = connection

    def acquire(self):
        timeout = self.timeout
        while timeout >= 0:
            expires = time.time() + self.expires + 1

            if self.r.setnx(self.key, expires):
                # We gained the lock; enter critical section
                return

            current_value = self.r.get(self.key)

            # We found an expired lock and nobody raced us to replacing it
            if current_value and float(current_value) < time.time() and \
               self.r.getset(self.key, expires) == current_value:
                    return

            timeout -= self.check_interval
            time.sleep(self.check_interval)

        raise LockTimeout("Timeout whilst waiting for lock")

    def __enter__(self):
        return self.acquire()

    def release(self):
        self.r.delete(self.key)

    def __exit__(self, exc_type, exc_value, tb):
        self.release()
        if not exc_type:
            return True
        return False


class LockTimeout(Exception):
    pass
