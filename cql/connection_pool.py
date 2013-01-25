
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import with_statement
from contextlib import contextmanager
from Queue import Queue, Empty
from threading import Thread
from time import sleep
from util import UnicodeMixin
from warnings import warn

__all__ = ['create_pool', 'ConnectionPool']

def create_pool(hosts, port=None, keyspace=None, user=None, password=None,
                cql_version=None, native=False, compression=None, consistency_level="ONE",
                transport=None, **pool_params):
    """Creates a basic connection pool for connections to a host or set of hosts.
       if host is not a string it should be a sequence of hosts from which one will be selected at random each time.
       This is a convenience method that provides a simple connector for ConnectionPool"""
    if native:
        from native import NativeConnection
        connclass = NativeConnection
        if port is None:
            port = 8000
    else:
        from thrifteries import ThriftConnection
        connclass = ThriftConnection
        if port is None:
            port = 9160

    if isinstance(hosts, basestring):
        hosts = (hosts,)

    import random
    
    def connector():
        """Basic connector made with create_pool."""
        selected_host = random.choice(hosts)
            
        return connclass(selected_host, port, keyspace, user, password,
                         cql_version=cql_version, compression=compression,
                         consistency_level=consistency_level, transport=transport)
    return ConnectionPool(connector, **pool_params)

class ConnectionPool(UnicodeMixin):
    """
    Simple connection-caching pool implementation.

    ConnectionPool provides the simplest possible connection pooling,
    lazily creating new connections if needed as `borrow_connection' is
    called.  Connections are re-added to the pool by `return_connection',
    unless doing so would exceed the maximum pool size.
    
    Example usage:
    >>> pool = ConnectionPool(connector =  lambda x: Connection("localhost", 9160, "Keyspace1"))
    >>> conn = pool.borrow_connection()
    >>> conn.execute(...)
    >>> pool.return_connection(conn)
    """
    def __init__(self, connector, max_conns=25, max_idle=5, eviction_delay=10000, name=None):
        if (isinstance(connector, basestring)):
            warn('ConnectionPool initialized using deprecated interface, attempting to provide basic implementation, but this may not work.')
            from thrifteries import ThriftConnection
            connector = lambda x: ThriftConnection(connector)
        self.connector = connector
        self.max_conns = max_conns
        self.max_idle = max_idle
        self.eviction_delay = eviction_delay
        
        self.connections = Queue()
        self.connections.put(self.__create_connection())
        self.open = True
        self.name = name
        self.eviction = Eviction(self)

    def close(self, timeout=0):
        """Closes a pool. Returns True if it has completed closing, otherwise false."""
        self.open = False
        self.eviction.join(timeout)
        return not self.eviction.isAlive()

    def __create_connection(self):
        return self.connector()

    @contextmanager
    def borrow_managed_connection(self):
        """Returns a managed connection that returns itself to the pool when it drops out of scope.
           Example:
           with pool.borrow_connection() as connection:
               connection.execute(...)
           #connection automagically returned to the pool
        """
        connection = self.borrow_connection(managed=False)
        try:
            yield connection
        finally:
            self.return_connection(connection)
            
    def borrow_connection(self, managed=True):
        """Returns a connection. If managed is True, it will be a connection wrapped with a context manager."""
        if managed:
            return self.borrow_managed_connection()

        if not self.open:
            raise RuntimeError(' '.join(('Cannot borrow a connection from', str(self))))
                               
        try:
            connection = self.connections.get(block=False)
        except Empty:
            connection = self.__create_connection()
        return connection
    
    def return_connection(self, connection):
        if self.connections.qsize() > self.max_conns:
            connection.close()
            return
        if not connection.is_open():
            return
        self.connections.put(connection)

    def __unicode__(self):
        return u' '.join(((u'[open]' if self.open else u'[closed]'), self.__class__.__name__) + ((unicode(self.name),) if self.name else tuple()))

    def __repr__(self):
        return ''.join((self.__class__.__name__, '(connector=', repr(self.connector),  ', max_conns=',  repr(self.max_conns), ', eviction_delay=', repr(self.eviction_delay), ', name=', repr(self.name), ')'))

class Eviction(Thread):
    def __init__(self, pool):
        Thread.__init__(self)

        self.pool = pool
        
        self.setDaemon(True)
        self.setName("EVICTION-THREAD for %s" % str(self.pool))
        self.start()
        
    def run(self):
        while(self.pool.open):
            while(self.pool.connections.qsize() > self.pool.max_idle):
                connection = self.pool.connections.get(block=False)
                if connection:
                    if connection.is_open():
                        connection.close()
            sleep(self.pool.eviction_delay/1000)
