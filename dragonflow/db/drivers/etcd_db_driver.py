#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

from contextlib import contextmanager
from socket import timeout as SocketTimeout

import etcd3gw as etcd
from oslo_log import log
import six
import urllib3
from urllib3 import connection
from urllib3 import exceptions

from dragonflow.common import exceptions as df_exceptions
from dragonflow.db import db_api
from dragonflow.db import db_common

LOG = log.getLogger(__name__)

# Monkey patch urllib3 to close connections that time out.  Otherwise,
# etcd will leak socket handles when we time out watches.

ETCD_READ_TIMEOUT = 20


@contextmanager
def _error_catcher(self):
    """
    Catch low-level python exceptions, instead re-raising urllib3
    variants, so that low-level exceptions are not leaked in the
    high-level api.
    On exit, release the connection back to the pool.
    """
    try:
        try:
            yield

        except SocketTimeout:
            # FIXME: Ideally we'd like to include the url in the
            # ReadTimeoutError but there is yet no clean way to
            # get at it from this context.
            raise exceptions.ReadTimeoutError(
                self._pool, None, 'Read timed out.')

        except connection.BaseSSLError as e:
            # FIXME: Is there a better way to differentiate between SSLErrors?
            if 'read operation timed out' not in str(e):  # Defensive:
                # This shouldn't happen but just in case we're missing an edge
                # case, let's avoid swallowing SSL errors.
                raise

            raise exceptions.ReadTimeoutError(
                self._pool, None, 'Read timed out.')

        except connection.HTTPException as e:
            # This includes IncompleteRead.
            raise exceptions.ProtocolError('Connection broken: %r' % e, e)
    except Exception:
        # The response may not be closed but we're not going to use it anymore
        # so close it now to ensure that the connection is released back to the
        #  pool.
        if self._original_response and not self._original_response.isclosed():
            self._original_response.close()

        # Before returning the socket, close it.  From the server's
        # point of view,
        # this socket is in the middle of handling an SSL handshake/HTTP
        # request so it we were to try and re-use the connection later,
        #  we'd see undefined behaviour.
        #
        # Still return the connection to the pool (it will be
        # re-established next time it is used).
        self._connection.close()

        raise
    finally:
        if self._original_response and self._original_response.isclosed():
            self.release_conn()
urllib3.HTTPResponse._error_catcher = _error_catcher

#通过etcd来支持table创建及表项的增删改
class EtcdDbDriver(db_api.DbApi):

    def __init__(self):
        super(EtcdDbDriver, self).__init__()
        self.client = None
        self.current_key = 0
        self.notify_callback = None

    def initialize(self, db_ip, db_port, **args):
        #初始化db client(采用etcd进行存储）
        self.client = etcd.client(host=db_ip, port=db_port)

    def create_table(self, table):
        # Not needed in etcd
        #由于不支持创建表，故此函数留空，通过构造含table的key来解决table的隔离
        pass

    def delete_table(self, table):
        self.client.delete_prefix(self._make_key(table))

    @staticmethod
    def _make_key(table, obj_id=None):
        if obj_id:
            #含id时的key
            key = '/{}/{}'.format(table, obj_id)
        else:
            #仅含table时的key
            key = '/{}/'.format(table)
        return key

    def get_key(self, table, key, topic=None):
        #获取key值
        key = self._get_key(self._make_key(table, key), key)
        if not six.PY2:
            key = key.decode("utf-8")
        return key

    def _get_key(self, table_key, key):
        value = self.client.get(table_key)
        if len(value) > 0:
            #如果有多个，则返回一个
            return value.pop()
        raise df_exceptions.DBKeyNotFound(key=key)

    def set_key(self, table, key, value, topic=None):
        #存入key，value
        self.client.put(self._make_key(table, key), value)

    def create_key(self, table, key, value, topic=None):
        #存入key,value
        self.client.put(self._make_key(table, key), value)

    def delete_key(self, table, key, topic=None):
        #移除key
        deleted = self.client.delete(self._make_key(table, key))
        if not deleted:
            raise df_exceptions.DBKeyNotFound(key=key)

    def get_all_entries(self, table, topic=None):
        res = []
        #获取所有包含table前缀的value
        directory = self.client.get_prefix(self._make_key(table))
        for entry in directory:
            value = entry[0]
            if value:
                res.append(value)
        return res

    def get_all_keys(self, table, topic=None):
        #返回所有包含table前缀的所有key
        res = []
        directory = self.client.get_prefix(self._make_key(table))
        table_name_size = len(table) + 2
        for entry in directory:
            _key = entry[1]["key"]
            key = _key[table_name_size:]
            if not six.PY2:
                key = key.decode("utf-8")
            res.append(key)
        return res

    def _allocate_unique_key(self, table):
        table_key = self._make_key(db_common.UNIQUE_KEY_TABLE, table)
        prev_value = 0
        try:
            #尝试读取之前设置的值
            prev_value = int(self._get_key(table_key, table))
        except df_exceptions.DBKeyNotFound:
            if prev_value == 0:
                # Create new key
                # 之前未设置，首次，创建对应的key
                if self.client.create(table_key, "1"):
                    return 1
            raise RuntimeError()  # Error occurred. Restart the allocation

        # 产生新的序号，并更新
        new_unique = prev_value + 1
        if self.client.replace(table_key, str(prev_value), str(new_unique)):
            return new_unique
        #原子更新失败？
        raise RuntimeError()  # Error occurred. Restart the allocation

    def allocate_unique_key(self, table):
        while True:
            try:
                #生成一个唯一的key
                return self._allocate_unique_key(table)
            except RuntimeError:
                pass

    def process_ha(self):
        # Not needed in etcd
        pass
