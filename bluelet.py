"""Extremely simple pure-Python implementation of coroutine-style
asynchronous socket I/O. Inspired by, but inferior to, Eventlet.
Bluelet can also be thought of as a less-terrible replacement for
asyncore.

Bluelet: easy concurrency without all the messy parallelism
Author: Adrian Sampson <adrian@radbox.org>
License: MIT

This code is a modified version of Bluelet
"""

import socket
import select
import sys
import types
import errno
import traceback
import time
import collections
import weakref

# A little bit of "six" (Python 2/3 compatibility): cope with PEP 3109 syntax changes
PY3 = sys.version_info[0] == 3
if PY3:
    def _reraise(typ, exc, tb):
        raise exc.with_traceback(tb)
else:
    exec("""
def _reraise(typ, exc, tb):
    raise typ, exc, tb
""")

# Basic events used for thread scheduling
class Event(object):
    """Just a base class identifying Bluelet events. An event is an
    object yielded from a Bluelet thread coroutine to suspend operation
    and communicate with the scheduler
    """
    pass

class WaitableEvent(Event):
    """A waitable event is one encapsulating an action that can be
    waited for using a select() call. That is, it's an event with an
    associated file descriptor
    """
    def waitables(self):
        """Return "waitable" objects to pass to select(). Should return
        three iterables for input readiness, output readiness, and
        exceptional conditions (i.e., the three lists passed to
        select())
        """
        return (), (), ()

    def fire(self):
        """Called when an associated file descriptor becomes ready,
        i.e., is returned from a select() call
        """
        pass

class ReadableEvent(WaitableEvent):
    def __init__(self, fd):
        self.fd = fd
    def waitables(self):
        return (self.fd,), (), ()

class WritableEvent(WaitableEvent):
    def __init__(self, fd):
        self.fd = fd
    def waitables(self):
        return (), (self.fd,), ()

class ValueEvent(Event):
    """An event that does nothing but return a fixed value"""
    def __init__(self, value):
        self.value = value

class ExceptionEvent(Event):
    """Raise an exception at the yield point. Used internally"""
    def __init__(self, exc_info):
        self.exc_info = exc_info

class SpawnEvent(Event):
    """Add a new coroutine thread to the scheduler"""
    def __init__(self, coro, daemon):
        self.spawned = coro
        self.daemon = daemon

class JoinEvent(Event):
    """Suspend the thread until the specified child thread has
    completed
    """
    def __init__(self, child):
        self.child = child

class KillEvent(Event):
    """Unschedule a child thread"""
    def __init__(self, child):
        self.child = child

class DelegationEvent(Event):
    """Suspend execution of the current thread, start a new thread and,
    once the child thread finished, return control to the parent
    thread
    """
    def __init__(self, coro):
        self.spawned = coro

class ReturnEvent(Event):
    """Return a value the current thread's delegator at the point of
    delegation. Ends the current (delegate) thread
    """
    def __init__(self, value):
        self.value = value

class SleepEvent(WaitableEvent):
    """Suspend the thread for a given duration
    """
    def __init__(self, duration):
        self.wakeup_time = time.time() + duration

    def time_left(self):
        return max(self.wakeup_time - time.time(), 0.0)

class ReadEvent(WaitableEvent):
    """Reads from a file-like object"""
    def __init__(self, fd, bufsize):
        self.fd = fd
        self.bufsize = bufsize

    def waitables(self):
        return (self.fd,), (), ()

    def fire(self):
        return self.fd.read(self.bufsize)

class WriteEvent(WaitableEvent):
    """Writes to a file-like object"""
    def __init__(self, fd, data):
        self.fd = fd
        self.data = data

    def waitable(self):
        return (), (self.fd,), ()

    def fire(self):
        self.fd.write(self.data)

# Core scheduler logic
def event_select(events):
    """Perform a select() over all the Events provided, returning the
    ones ready to be fired. Only WaitableEvents (including SleepEvents)
    matter here; all other events are ignored (and thus postponed)
    """

    # Gather waitables and wakeup times
    waitable_to_event = {}
    rlist, wlist, xlist = [], [], []
    earliest_wakeup = None
    for event in events:
        if isinstance(event, SleepEvent):
            if not earliest_wakeup:
                earliest_wakeup = event.wakeup_time
            else:
                earliest_wakeup = min(earliest_wakeup, event.wakeup_time)
        if isinstance(event, WaitableEvent):
            r, w, x = event.waitables()
            rlist += r
            wlist += w
            xlist += x
            for waitable in r:
                waitable_to_event[('r', waitable)] = event
            for waitable in w:
                waitable_to_event[('w', waitable)] = event
            for waitable in x:
                waitable_to_event[('x', waitable)] = event

    # If we have a any sleeping threads, determine how long to sleep
    if earliest_wakeup:
        timeout = max(earliest_wakeup - time.time(), 0.0)
    else:
        timeout = None

    # Perform select() if we have any waitables
    if rlist or wlist or xlist:
        rready, wready, xready = select.select(rlist, wlist, xlist, timeout)
    else:
        rready, wready, xready = (), (), ()
        if timeout:
            time.sleep(timeout)

    # Gather ready events corresponding to the ready waitables
    ready_events = set()
    for ready in rready:
        ready_events.add(waitable_to_event[('r', ready)])
    for ready in wready:
        ready_events.add(waitable_to_event[('w', ready)])
    for ready in xready:
        ready_events.add(waitable_to_event[('x', ready)])

    # Gather any finished sleeps
    for event in events:
        if isinstance(event, SleepEvent) and event.time_left() == 0.0:
            ready_events.add(event)

    return ready_events

class ThreadException(Exception):
    def __init__(self, coro, exc_info):
        self.coro = coro
        self.exc_info = exc_info
    def reraise(self):
        _reraise(self.exc_info[0], self.exc_info[1], self.exc_info[2])

# Special sentinel placeholder for suspended threads
SUSPENDED = Event()

class Delegated(Event):
    """Placeholder indicating that a thread has delegated execution to a
    different thread
    """
    def __init__(self, child):
        self.child = child

def run(root_coro):
    """Schedules a coroutine, running it to completion. This
    encapsulates the Bluelet scheduler, which the root coroutine can
    add to by spawning new coroutines
    """
    # The "threads" dictionary keeps track of all the currently-
    # executing and suspended coroutines. It maps coroutines to their
    # currently "blocking" event. The event value may be SUSPENDED if
    # the coroutine is waiting on some other condition: namely, a
    # delegated coroutine or a joined coroutine. In this case, the
    # coroutine should *also* appear as a value in one of the below
    # dictionaries `delegators` or `joiners`
    threads = {root_coro: ValueEvent(None)}

    # Maps child coroutines to delegating parents
    delegators = {}

    # Maps child coroutines to joining (exit-waiting) parents
    joiners = collections.defaultdict(list)

    # Auto join
    spawned = collections.defaultdict(list)

    # Auto cleanup
    cleanup = collections.defaultdict(collections.deque)

    # Ancestors
    ancestors = {root_coro: root_coro}

    # History of spawned coroutines for joining of already completed
    # coroutines
    history = weakref.WeakKeyDictionary({root_coro: None})

    def complete_thread(coro, return_value, is_killed=False):
        """Remove a coroutine from the scheduling pool, awaking
        delegators and joiners as necessary and returning the specified
        value to any delegating parent
        """

        # If we are terminated abruptly, auto release locks
        if is_killed:
            for cb in cleanup[coro]:
                cb()

        del threads[coro]
        del ancestors[coro]

        # Recursive auto join
        for task in spawned[coro]:
            if task in threads:
                kill_thread(task)
        del spawned[coro]

        # Resume delegator
        if coro in delegators:
            threads[delegators[coro]] = ValueEvent(return_value)
            del delegators[coro]

        # Resume joiners
        if coro in joiners:
            for parent in joiners[coro]:
                threads[parent] = ValueEvent(None)
            del joiners[coro]

    def advance_thread(coro, value, is_exc=False):
        """After an event is fired, run a given coroutine associated with
        it in the threads dict until it yields again. If the coroutine
        exits, then the thread is removed from the pool. If the coroutine
        raises an exception, it is reraised in a ThreadException. If
        is_exc is True, then the value must be an exc_info tuple and the
        exception is thrown into the coroutine
        """
        try:
            if is_exc:
                next_event = coro.throw(*value)
            else:
                next_event = coro.send(value)
        except StopIteration:
            # Thread is done
            complete_thread(coro, None)
        except:
            # Thread raised some other exception
            del threads[coro]
            raise ThreadException(coro, sys.exc_info())
        else:
            if isinstance(next_event, types.GeneratorType):
                # Automatically invoke sub-coroutines.
                # This is a shorthand for explicit bluelet.call()
                next_event = DelegationEvent(next_event)
            threads[coro] = next_event

    def kill_thread(coro):
        """Unschedule this thread and its (recursive) delegates
        """
        # Collect all coroutines in the delegation stack
        coros = [coro]
        while isinstance(threads[coro], Delegated):
            coro = threads[coro].child
            coros.append(coro)

        # Complete each coroutine from the top to the bottom of the stack
        for coro in reversed(coros):
            complete_thread(coro, None, True)
            coro.close()

    # FIXME: Maybe move this to class Lock. In this case we need to define an EventLoop structure
    def lock_set_owner(coro, lock):
        coro = ancestors[coro]
        lock._owner = coro
        cleanup[coro].appendleft(lambda: lock_auto_release(coro, lock))

    def lock_acquire(coro, lock):
        if not lock._locked:
            lock._locked = True
            lock_set_owner(coro, lock)
            # Continue running immediately
            threads[coro] = ValueEvent(None)
            return

        assert ancestors[coro] != lock._owner, "Attempt to acquire lock twice"

        lock._waiters.append(coro)
        threads[coro] = SUSPENDED

    def lock_release(coro, lock):
        assert ancestors[coro] == lock._owner, "Attempt to release lock from thread that does not own it"

        # Pick up a new owner
        while len(lock._waiters):
            waiter = lock._waiters.popleft()

            if waiter in threads:
                # Set waiter as the new owner
                lock_set_owner(waiter, lock)
                threads[waiter] = ValueEvent(None)
                threads[coro] = ValueEvent(None)
                return

        # If no one is interested, release lock
        lock._locked = False
        lock._owner = None
        threads[coro] = ValueEvent(None)

    def lock_auto_release(coro, lock):
        if coro == lock._owner:
            lock_release(coro, lock)

    # Continue advancing threads until root thread exits
    exit_te = None
    while threads:
        try:
            # Look for events that can be run immediately. Continue
            # running immediate events until nothing is ready
            while True:
                have_ready = False
                for coro, event in list(threads.items()):
                    # Note that recursive kill could remove multiple threads
                    if coro not in threads:
                        continue
                    if isinstance(event, SpawnEvent):
                        threads[event.spawned] = ValueEvent(None)
                        ancestors[event.spawned] = event.spawned
                        history[event.spawned] = None
                        if not event.daemon:
                            spawned[coro].append(event.spawned)
                        advance_thread(coro, event.spawned)
                        have_ready = True
                    elif isinstance(event, ValueEvent):
                        advance_thread(coro, event.value)
                        have_ready = True
                    elif isinstance(event, ExceptionEvent):
                        advance_thread(coro, event.exc_info, True)
                        have_ready = True
                    elif isinstance(event, DelegationEvent):
                        threads[coro] = Delegated(event.spawned)
                        threads[event.spawned] = ValueEvent(None)
                        ancestors[event.spawned] = ancestors[coro]
                        history[event.spawned] = None
                        delegators[event.spawned] = coro
                        have_ready = True
                    elif isinstance(event, ReturnEvent):
                        # Thread is done
                        complete_thread(coro, event.value)
                        have_ready = True
                    elif isinstance(event, JoinEvent):
                        if event.child not in threads and event.child in history:
                            threads[coro] = ValueEvent(None)
                        else:
                            threads[coro] = SUSPENDED
                            joiners[event.child].append(coro)
                        have_ready = True
                    elif isinstance(event, KillEvent):
                        threads[coro] = ValueEvent(None)
                        if event.child in threads:
                            assert ancestors[event.child] == event.child, "Can only kill a spawned thread"
                            kill_thread(event.child)
                        have_ready = True
                    elif isinstance(event, LockEvent):
                        have_ready = True
                        if event.is_acquire:
                            lock_acquire(coro, event.lock)
                        else:
                            lock_release(coro, event.lock)
                    elif isinstance(event, SemaphoreEvent):
                        have_ready = True
                        sem = event.sem
                        assert sem.locked()
                        if event.is_acquire:
                            sem._waiters.append(coro)
                            threads[coro] = SUSPENDED
                        else:
                            while sem._waiters:
                                w = sem._waiters.popleft()
                                if w in threads:
                                    threads[w] = ValueEvent(None)
                                    break
                            else:
                                sem._value += 1
                            threads[coro] = ValueEvent(None)
                    elif isinstance(event, ConditionEvent):
                        have_ready = True
                        cond = event.cond
                        if event.is_notify:
                            n = event.n_notify
                            if n == 0:
                                n = len(cond._waiters)
                            while cond._waiters and n:
                                w = cond._waiters.popleft()
                                if w in threads:
                                    n -= 1
                                    lock_acquire(w, cond._lock)
                            threads[coro] = ValueEvent(None)
                        else:
                            lock_release(coro, cond._lock)
                            cond._waiters.append(coro)
                            threads[coro] = SUSPENDED

                    elif isinstance(event, SignalEvent):
                        have_ready = True
                        signal = event.signal
                        if event.is_set:
                            signal.value = True
                            while signal._waiters:
                                w = signal._waiters.popleft()
                                if w in threads:
                                    threads[w] = ValueEvent(None)
                            threads[coro] = ValueEvent(None)
                        else:
                            signal._waiters.append(coro)
                            threads[coro] = SUSPENDED

                # Only start the select when nothing else is ready
                if not have_ready:
                    break

            # Wait and fire
            event2coro = dict((v,k) for k,v in threads.items())
            for event in event_select(threads.values()):
                # Run the IO operation, but catch socket errors
                try:
                    value = event.fire()
                except socket.error as exc:
                    if isinstance(exc.args, tuple) and exc.args[0] == errno.EPIPE:
                        # Broken pipe. Remote host disconnected
                        pass
                    elif isinstance(exc.args, tuple) and exc.args[0] == errno.ECONNRESET:
                        # Connection was reset by peer
                        pass
                    else:
                        traceback.print_exc()
                    # Abort the coroutine
                    threads[event2coro[event]] = ReturnEvent(None)
                else:
                    advance_thread(event2coro[event], value)

        except ThreadException as te:
            # Exception raised from inside a thread
            event = ExceptionEvent(te.exc_info)
            if te.coro in delegators:
                # The thread is a delegate. Raise exception in its
                # delegator
                threads[delegators[te.coro]] = event
                del delegators[te.coro]
            else:
                # The thread is root-level. Raise in client code
                exit_te = te
                break

        except:
            # For instance, KeyboardInterrupt during select(). Raise
            # into root thread and terminate others
            threads = {root_coro: ExceptionEvent(sys.exc_info())}

    # If any threads still remain, kill them
    for coro in threads:
        coro.close()

    # If we're exiting with an exception, raise it in the client
    if exit_te:
        exit_te.reraise()

# Sockets and their associated events
class SocketClosedError(Exception):
    pass

class Listener(object):
    def __init__(self, sock):
        self.closed = False
        self.sock = sock

    def accept(self):
        """An event that waits for a connection on the listening socket.
        When a connection is made, the event returns a Connection
        object
        """
        if self.closed:
            raise SocketClosedError()
        return AcceptEvent(self)

    def close(self):
        """Immediately close the listening socket. (Not an event)
        """
        self.closed = True
        self.sock.close()

class Connection(object):
    """A socket wrapper object for connected sockets.
    """
    def __init__(self, sock, addr):
        self.sock = sock
        self.addr = addr
        self.buf = b''
        self.closed = False

    def close(self):
        """Close the connection"""
        self.closed = True
        self.sock.close()

    def recv(self, size):
        """Read at most size bytes of data from the socket"""
        if self.closed:
            raise SocketClosedError()

        if self.buf:
            # We already have data read previously
            out = self.buf[:size]
            self.buf = self.buf[size:]
            return ValueEvent(out)
        else:
            return ReceiveEvent(self.sock, size)

    def send(self, data):
        """Sends data on the socket, returning the number of bytes
        successfully sent
        """
        if self.closed:
            raise SocketClosedError()
        return SendEvent(self.sock, data)

    def sendall(self, data):
        """Send all of data on the socket"""
        if self.closed:
            raise SocketClosedError()
        return SendEvent(self.sock, data, True)

    def readline(self, terminator=b"\n", bufsize=1024):
        """Reads a line (delimited by terminator) from the socket"""
        if self.closed:
            raise SocketClosedError()

        while True:
            if terminator in self.buf:
                line, self.buf = self.buf.split(terminator, 1)
                line += terminator
                yield ReturnEvent(line)
                break
            data = yield ReceiveEvent(self.sock, bufsize)
            if data:
                self.buf += data
            else:
                line = self.buf
                self.buf = b''
                yield ReturnEvent(line)
                break

class AcceptEvent(WaitableEvent):
    """An event for Listener objects (listening sockets) that suspends
    execution until the socket gets a connection
    """
    def __init__(self, listener):
        self.listener = listener

    def waitables(self):
        return (self.listener.sock,), (), ()

    def fire(self):
        sock, addr = self.listener.sock.accept()
        return Connection(sock, addr)

class ReceiveEvent(WaitableEvent):
    """An event for Connection objects (connected sockets) for
    asynchronously reading data
    """
    def __init__(self, sock, bufsize):
        self.sock = sock
        self.bufsize = bufsize

    def waitables(self):
        return (self.sock,), (), ()

    def fire(self):
        return self.sock.recv(self.bufsize)

class SendEvent(WaitableEvent):
    """An event for Connection objects (connected sockets) for
    asynchronously writing data
    """
    def __init__(self, sock, data, sendall=False):
        self.sock = sock
        self.data = data
        self.sendall = sendall

    def waitables(self):
        return (), (self.sock,), ()

    def fire(self):
        if self.sendall:
            return self.sock.sendall(self.data)
        else:
            return self.sock.send(self.data)

# Locking primitives
class Lock(object):
    def __init__(self):
        self._waiters = collections.deque()
        self._locked = False
        self._owner = None

    def locked(self):
        return self._locked

    def acquire(self):
        yield LockEvent(self, True)

    def release(self):
        assert self._locked and self._owner, "Attempt to release a free lock"
        yield LockEvent(self, False)

class LockEvent(object):
    def __init__(self, lock, is_acquire):
        self.lock = lock
        self.is_acquire = is_acquire

class Condition(object):
    def __init__(self, lock=None):
        if lock is None:
            lock = Lock()
        self._lock = lock

        self.locked = lock.locked
        self.acquire = lock.acquire
        self.release = lock.release

        self._waiters = collections.deque()

    def wait(self):
        assert self.locked()
        yield ConditionEvent(self, is_notify=False)

    def wait_for(self, predicate):
        result = predicate()
        while not result:
            yield self.wait()
            result = predicate()

    def notify(self, n=1):
        assert self.locked()
        yield ConditionEvent(self, is_notify=True, n_notify=n)

    def notify_all(self):
        assert self.locked()
        yield ConditionEvent(self, is_notify=True, n_notify=0)

class ConditionEvent(object):
    def __init__(self, cond, is_notify, n_notify=0):
        self.cond = cond
        self.is_notify = is_notify
        self.n_notify = n_notify

class Semaphore(object):
    def __init__(self, value=1, bound=1):
        self._waiters = collections.deque()
        self._value = value
        self._bound = bound

    def locked(self):
        return self._value == 0

    def acquire(self):
        if self._value > 0:
            self._value -= 1
            return
        yield SemaphoreEvent(self, True)

    def release(self):
        if not self._waiters:
            assert self._value < self._bound
            self._value += 1
            return
        yield SemaphoreEvent(self, False)

class SemaphoreEvent(object):
    def __init__(self, sem, is_acquire):
        self.sem = sem
        self.is_acquire = is_acquire

# Corresponds to the event primitive
class Signal(object):
    def __init__(self):
        self._waiters = collections.deque()
        self.value = False

    def is_set(self):
        return self._value

    def set(self):
        yield SignalEvent(self, True)

    def clear(self):
        self.value = False

    def wait(self):
        if self.value:
            return
        yield SignalEvent(self, False)

class SignalEvent(object):
    def __init__(self, signal, is_set):
        self.signal = signal
        self.is_set = is_set

# Wait for a system call with an optional timeout and an otional interrupt signal
def wait_for(coro, timeout=None, interrupt=None):
    if timeout is None and interrupt is None:
        res = yield coro
        yield end((True, res))

    sig = Signal()
    cond = [(False, None)]

    def wait_task(sig, cond):
        try:
            res = yield coro
            cond[0] = (True, res)
            yield sig.set()
        finally:
            pass

    yield spawn(wait_task(sig, cond), daemon=False)

    if timeout is not None:
        def timeout_task(sig):
            try:
                yield sleep(timeout)
                yield sig.set()
            finally:
                pass
        yield spawn(timeout_task(sig), daemon=False)

    if interrupt is not None:
        def interrupt_task(sig):
            try:
                yield interrupt.wait()
                yield sig.set()
            finally:
                pass
        yield spawn(interrupt_task(sig), daemon=False)

    yield sig.wait()
    yield end(cond[0])

# Public interface for threads; each returns an event object that
# can immediately be yielded
def null():
    """Event: yield to the scheduler without doing anything special
    """
    return ValueEvent(None)

def spawn(coro, daemon=True):
    """Event: add another coroutine to the scheduler. Both the parent
    and child coroutines run concurrently
    """
    if not isinstance(coro, types.GeneratorType):
        raise ValueError('%s is not a coroutine' % str(coro))
    return SpawnEvent(coro, daemon)

def call(coro):
    """Event: delegate to another coroutine. The current coroutine
    is resumed once the sub-coroutine finishes. If the sub-coroutine
    returns a value using end(), then this event returns that value
    """
    if not isinstance(coro, types.GeneratorType):
        raise ValueError('%s is not a coroutine' % str(coro))
    return DelegationEvent(coro)

def end(value=None):
    """Event: ends the coroutine and returns a value to its
    delegator
    """
    return ReturnEvent(value)

def read(fd, bufsize=None):
    """Event: read from a file descriptor asynchronously"""
    if bufsize is None:
        # Read all
        def reader():
            buf = []
            while True:
                data = yield read(fd, 1024)
                if not data:
                    break
                buf.append(data)
            yield ReturnEvent(''.join(buf))
        return DelegationEvent(reader())
    else:
        return ReadEvent(fd, bufsize)

def write(fd, data):
    """Event: write to a file descriptor asynchronously"""
    return WriteEvent(fd, data)

def send(sock, data):
    yield SendEvent(sock, data, False)

def sendall(sock, data):
    yield SendEvent(sock, data, True)

def connect(host, port):
    """Event: connect to a network address and return a Connection
    object for communicating on the socket
    """
    addr = (host, port)
    sock = socket.create_connection(addr)
    return ValueEvent(Connection(sock, addr))

def sleep(duration):
    """Event: suspend the thread for ``duration`` seconds
    """
    return SleepEvent(duration)

def join(coro):
    """Suspend the thread until another, previously `spawn`ed thread
    completes
    """
    return JoinEvent(coro)

def kill(coro):
    """Halt the execution of a different `spawn`ed thread
    """
    return KillEvent(coro)

# Convenience function for running socket servers
def server(host, port, func):
    """A coroutine that runs a network server. Host and port specify the
    listening address. func should be a coroutine that takes a single
    parameter, a Connection object. The coroutine is invoked for every
    incoming connection on the listening socket
    """
    def handler(conn):
        try:
            yield func(conn)
        finally:
            conn.close()

    listener = Listener(host, port)
    try:
        while True:
            conn = yield listener.accept()
            yield spawn(handler(conn))
    except KeyboardInterrupt:
        pass
    finally:
        listener.close()
