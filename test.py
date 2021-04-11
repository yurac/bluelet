#!/usr/bin/env python

import os
import time
import warnings
import unittest
import bluelet

class TestBluelet(unittest.TestCase):
    def test_signal(self):
        def func1(sig):
            yield bluelet.sleep(3)
            yield sig.set()

        def func2(sig):
            yield sig.wait()

        def body():
            sig = bluelet.Signal()
            handle1 = func1(sig)
            handle2 = func2(sig)
            yield bluelet.spawn(handle1, daemon=False)
            yield bluelet.spawn(handle2, daemon=False)
            yield bluelet.join(handle2)

        bluelet.run(body())

    def test_sem(self):
        def func1(sem):
            yield bluelet.sleep(3)
            yield sem.release()

        def func2(sem):
            yield sem.acquire()

        def body():
            sem = bluelet.Semaphore(0)
            handle1 = func1(sem)
            handle2 = func2(sem)
            yield bluelet.spawn(handle1, daemon=False)
            yield bluelet.spawn(handle2, daemon=False)
            yield bluelet.join(handle2)

        bluelet.run(body())

    def test_cond(self):
        def func1(cond):
            yield bluelet.sleep(3)
            yield cond.acquire()
            yield cond.notify_all()
            yield cond.release()

        def func2(cond):
            yield cond.acquire()
            yield cond.wait()
            yield cond.release()

        def body():
            cond = bluelet.Condition()
            handle1 = func1(cond)
            handle2 = func2(cond)
            yield bluelet.spawn(handle1, daemon=False)
            yield bluelet.spawn(handle2, daemon=False)
            yield bluelet.join(handle2)

        bluelet.run(body())

    def test_lock_autorelease(self):
        def func1(lock):
            yield lock.acquire()
            yield bluelet.sleep(100)
            yield lock.release()

        def func2(lock):
            yield bluelet.sleep(1)
            yield lock.acquire()
            yield bluelet.sleep(1)
            yield lock.release()

        def body():
            lock = bluelet.Lock()
            handle1 = func1(lock)
            handle2 = func2(lock)
            yield bluelet.spawn(handle1)
            yield bluelet.spawn(handle2)
            yield bluelet.sleep(1)
            yield bluelet.kill(handle1)
            yield bluelet.sleep(1)
            yield bluelet.join(handle2)

        bluelet.run(body())

    def test_wait_for(self):
        def func1(param):
            yield bluelet.sleep(param)
            yield bluelet.end(1)

        def interrupt_func(sig, param):
            yield bluelet.sleep(param)
            yield sig.set()

        def body():
            t = time.time()
            res, status = yield bluelet.wait_for(func1(100), timeout=1)
            assert res == False and status is None
            passed = time.time() - t
            assert passed > 1 and passed < 2

            t = time.time()

            # Spawn an interrupt task
            sig = bluelet.Signal()
            sig.clear()
            handle = interrupt_func(sig, 0.5)
            yield bluelet.spawn(handle, daemon=False)

            res, status = yield bluelet.wait_for(func1(1), timeout=2, interrupt=sig)
            assert res == False and status == None
            passed = time.time() - t
            assert passed > 0.5 and passed < 1
            yield bluelet.kill(handle)

            t = time.time()
            res, status = yield bluelet.wait_for(func1(0.5), timeout=1)
            assert res == True and status == 1
            passed = time.time() - t
            assert passed > 0.5 and passed < 1

        bluelet.run(body())

if __name__ == "__main__":
        unittest.main()
