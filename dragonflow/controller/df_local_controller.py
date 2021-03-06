# Copyright (c) 2015 OpenStack Foundation.
# All Rights Reserved.
#
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

import collections
import itertools
import sys
import time

from eventlet import queue
from oslo_log import log
from oslo_service import loopingcall

from dragonflow.common import exceptions
from dragonflow.common import utils as df_utils
from dragonflow import conf as cfg
from dragonflow.controller.common import constants as ctrl_const
from dragonflow.controller import df_config
from dragonflow.controller import service
from dragonflow.controller import topology
from dragonflow.db import api_nb
from dragonflow.db import db_common
from dragonflow.db import db_store
from dragonflow.db import model_framework
from dragonflow.db import model_proxy
from dragonflow.db.models import core
from dragonflow.db.models import l2
from dragonflow.db.models import mixins
from dragonflow.db import sync


LOG = log.getLogger(__name__)


class DfLocalController(object):

    def __init__(self, chassis_name, nb_api):
        #用于缓存obj
        self.db_store = db_store.get_instance()
        self._queue = queue.PriorityQueue()
        # pending_id -> (model, pender_id)
        #       'pending_id' is the ID of the object for which we are waiting.
        #       'model' and 'pender_id' are the model and the ID of the object
        #       which is waiting for the object described by 'pending_id'
        self._pending_objects = collections.defaultdict(set)

        self.chassis_name = chassis_name
        #北向api
        self.nb_api = nb_api
        #指定database change event的处理函数
        self.nb_api.set_db_change_callback(self.db_change_callback)
        #指明本主机ip地址
        self.ip = cfg.CONF.df.local_ip
        # Virtual tunnel port support multiple tunnel types together
        self.tunnel_types = cfg.CONF.df.tunnel_types
        self.neutron_notifier = None
        if cfg.CONF.df.enable_neutron_notifier:
            self.neutron_notifier = df_utils.load_driver(
                     cfg.CONF.df.neutron_notifier,
                     df_utils.DF_NEUTRON_NOTIFIER_DRIVER_NAMESPACE)
        #加载switch_backend
        self.switch_backend = df_utils.load_driver(
            cfg.CONF.df.switch_backend,
            df_utils.DF_SWITCH_BACKEND_DRIVER_NAMESPACE,
            nb_api, cfg.CONF.df.management_ip)

        #switch_backend初始化
        self.switch_backend.initialize(self.db_change_callback,
                                       self.neutron_notifier)
        self.topology = None
        self.enable_selective_topo_dist = \
            cfg.CONF.df.enable_selective_topology_distribution
        self._sync = sync.Sync(
            nb_api=self.nb_api,
            #指明同步对应的update,delete回调
            update_cb=self.update,
            delete_cb=self.delete,
            selective=self.enable_selective_topo_dist,
        )
        #周期性产生controller_sync事件
        self._sync_pulse = loopingcall.FixedIntervalLoopingCall(
            self._submit_sync_event)

        self.sync_rate_limiter = df_utils.RateLimiter(
                max_rate=1, time_unit=db_common.DB_SYNC_MINIMUM_INTERVAL)

    def db_change_callback(self, table, key, action, value, topic=None):
        #依据参数构造database update obj
        update = db_common.DbUpdate(table, key, action, value, topic=topic)
        LOG.debug("Pushing Update to Queue: %s", update)
        #将更新信息存入队列
        self._queue.put(update)
        time.sleep(0)

    def process_changes(self):
        while True:
            #自队列中拿出update
            next_update = self._queue.get(block=True)
            LOG.debug("Event update: %s", next_update)
            #将更新交给nb_api
            self.nb_api._notification_cb(next_update)
            self._queue.task_done()

    def run(self):
        #注册nb_api的通知回调（完成self._queue中event update的处理，见process_changes)
        self.nb_api.register_notification_callback(self._handle_update)
        if self.neutron_notifier:
            self.neutron_notifier.initialize(nb_api=self.nb_api)
        #????
        self.topology = topology.Topology(self,
                                          self.enable_selective_topo_dist)
        #开启定时器，周期性触发controller sync事件
        self._sync_pulse.start(
            interval=cfg.CONF.df.db_sync_time,
            initial_delay=cfg.CONF.df.db_sync_time,
        )

        self.switch_backend.start()
        self._register_models()
        # 更新自身
        self.register_chassis()
        self.sync()
        # 自队列中取出更新event传入cb处理
        self.process_changes()

    def _submit_sync_event(self):
        #产生controller_sync事件
        self.db_change_callback(None, None,
                                ctrl_const.CONTROLLER_SYNC, None)

    def _register_models(self):
        #注册要同步的model
        ignore_models = self.switch_backend.sync_ignore_models()
        for model in model_framework.iter_models_by_dependency_order():
            # FIXME (dimak) generalize sync to support non-northbound models
            if model not in ignore_models:
                #指明要同步的model
                self._sync.add_model(model)

    def sync(self):
        self.topology.check_topology_info()
        self._sync.sync()

    def register_topic(self, topic):
        self.nb_api.subscriber.register_topic(topic)
        self._sync.add_topic(topic)

    def unregister_topic(self, topic):
        self.nb_api.subscriber.unregister_topic(topic)
        self._sync.remove_topic(topic)

    def _get_ports_by_chassis(self, chassis):
        return self.db_store.get_all(
            l2.LogicalPort(
                binding=l2.PortBinding(
                    type=l2.BINDING_CHASSIS,
                    chassis=chassis.id,
                ),
            ),
            index=l2.LogicalPort.get_index('chassis_id'),
        )

    def update_chassis(self, chassis):
        self.db_store.update(chassis)
        remote_chassis_name = chassis.id
        if self.chassis_name == remote_chassis_name:
            return

        # Notify about remote port update
        for port in self._get_ports_by_chassis(chassis):
            self.update(port)

    def delete_chassis(self, chassis):
        LOG.info("Deleting remote ports in remote chassis %s", chassis.id)
        # Chassis is deleted, there is no reason to keep the remote port
        # in it.
        for port in self._get_ports_by_chassis(chassis):
            self.delete(port)
        self.db_store.delete(chassis)

    def register_chassis(self):
        # Get all chassis from nb db to db store.
        # 获取所有的chassis,更新到cache
        for c in self.nb_api.get_all(core.Chassis):
            self.db_store.update(c)

        #取出缓存的自身chassis
        old_chassis = self.db_store.get_one(
            core.Chassis(id=self.chassis_name))

        #用当前新的配置更新自身chassis
        chassis = core.Chassis(
            id=self.chassis_name,
            ip=self.ip,
            tunnel_types=self.tunnel_types,
        )
        if cfg.CONF.df.external_host_ip:
            chassis.external_host_ip = cfg.CONF.df.external_host_ip

        #更新至cache中
        self.db_store.update(chassis)

        # REVISIT (dimak) Remove skip_send_event once there is no bind conflict
        # between publisher service and the controoler, see bug #1651643
        # 触发create or update event
        if old_chassis is None:
            self.nb_api.create(chassis, skip_send_event=True)
        elif old_chassis != chassis:
            self.nb_api.update(chassis, skip_send_event=True)

    def update_publisher(self, publisher):
        self.db_store.update(publisher)
        LOG.info('Registering to new publisher: %s', str(publisher))
        self.nb_api.subscriber.register_listen_address(publisher.uri)

    def delete_publisher(self, publisher):
        LOG.info('Deleting publisher: %s', str(publisher))
        self.nb_api.subscriber.unregister_listen_address(publisher.uri)
        self.db_store.delete(publisher)

    def switch_sync_finished(self):
        # 执行switch sync完成
        self.switch_backend.switch_sync_finished()

    def switch_sync_started(self):
        # 开始执行 switch sync
        self.switch_backend.switch_sync_started()

    def _is_newer(self, obj, cached_obj):
        '''Check whether obj is newer than cached_obj.

        If obj is a subtype of Version mixin we can use is_newer_than,
        otherwise we assume that our NB-received object is always newer than
        the cached copy.
        '''
        try:
            return obj.is_newer_than(cached_obj)
        except AttributeError:
            return obj != cached_obj

    #通过缓存的obj来区分是更新，还是删除(通用的update)
    def update_model_object(self, obj):
        #取之前缓存的obj
        original_obj = self.db_store.get_one(obj)

        #如果obj版本号并不如original_obj大，则不处理
        if not self._is_newer(obj, original_obj):
            return

        #obj更新一些，则缓存obj
        self.db_store.update(obj)

        if original_obj is None:
            #原来没有缓存obj且obj为event,则触发创建事件
            if _has_basic_events(obj):
                obj.emit_created()
        else:
            if _has_basic_events(obj):
                #执行obj更新
                obj.emit_updated(original_obj)

    #通用的delete obj函数
    def delete_model_object(self, obj):
        # Retrieve full object (in case we only got Model(id='id'))
        org_obj = self.db_store.get_one(obj)
        if org_obj:
            #原来存在，则删除
            if _has_basic_events(org_obj):
                org_obj.emit_deleted()
            self.db_store.delete(org_obj)
        else:
            # NOTE(nick-ma-z): Ignore the null object because
            # it has been deleted before.
            pass

    def get_nb_api(self):
        return self.nb_api

    def get_chassis_name(self):
        return self.chassis_name

    # TODO(snapiri): This should be part of the interface
    def notify_port_status(self, switch_port, status):
        self.switch_backend.notify_port_status(switch_port, status)

    def _get_delete_handler(self, table):
        method_name = 'delete_{0}'.format(table)
        return getattr(self, method_name, self.delete_model_object)

    def update(self, obj):
        #调用update_xx或者 update_model_obj
        handler = getattr(
            self,
            'update_{0}'.format(obj.table_name),
            self.update_model_object,
        )
        return handler(obj)

    def delete(self, obj):
        #通过调用delete_xx来完成删除
        handler = self._get_delete_handler(obj.table_name)
        return handler(obj)

    def delete_by_id(self, model, obj_id):
        # FIXME (dimak) Probably won't be needed once we're done porting
        return self.delete(model(id=obj_id))

    def _handle_update(self, update):
        #处理controller收到的update event
        try:
            self._handle_db_change(update)
        except Exception as e:
            #如果发生异常，视情况进行同步操作
            if "port_num is 0" not in str(e):
                LOG.exception(e)
            if not self.sync_rate_limiter():
                self.sync()

    def _handle_db_change(self, update):
        action = update.action
        if action == ctrl_const.CONTROLLER_REINITIALIZE:
            #重新初始化controller
            self.db_store.clear()
            self.switch_backend.initialize(self.db_change_callback,
                                           self.neutron_notifier)
            self.sync()
        elif action == ctrl_const.CONTROLLER_SYNC:
            #执行同步
            self.sync()
        elif action == ctrl_const.CONTROLLER_DBRESTART:
            #db重启事件
            self.nb_api.db_recover_callback()
        elif action == ctrl_const.CONTROLLER_SWITCH_SYNC_FINISHED:
            #switch同步结束
            self.switch_sync_finished()
        elif action == ctrl_const.CONTROLLER_SWITCH_SYNC_STARTED:
            #switch同步开始
            self.switch_sync_started()
        elif action == ctrl_const.CONTROLLER_LOG:
            #显示contrller log
            LOG.info('Log event: %s', str(update))
        elif update.table is not None:
            #收到module 更新event
            try:
                model_class = model_framework.get_model(update.table)
            except KeyError:
                # Model class not found, possibly update was not about a model
                LOG.warning('Unknown table %s', update.table)
            else:
                if action == 'delete':
                    #执行obj删除
                    self.delete_by_id(model_class, update.key)
                else:
                    #执行obj更新
                    obj = model_class.from_json(update.value)
                    self._send_updates_for_object(obj)
        else:
            LOG.warning('Unfamiliar update: %s', str(update))

    def _get_model(self, obj):
        if model_proxy.is_model_proxy(obj):
            return obj.get_proxied_model()
        return type(obj)

    def _send_updates_for_object(self, obj):
        try:
            references = self.get_model_references_deep(obj)
        except exceptions.ReferencedObjectNotFound as e:
            proxy = e.kwargs['proxy']
            reference_id = proxy.id
            model = self._get_model(obj)
            self._pending_objects[reference_id].add((model, obj.id))
        else:
            queue = itertools.chain(reversed(references), (obj,))
            #触发self.update
            self._send_update_events(queue)
            self._send_pending_events(obj)

    def _send_pending_events(self, obj):
        try:
            pending = self._pending_objects.pop(obj.id)
        except KeyError:
            return  # Nothing to do
        for model, item_id in pending:
            lean_obj = model(id=item_id)
            item = self.nb_api.get(lean_obj)
            self._send_updates_for_object(item)

    def get_model_references_deep(self, obj):
        """
        Return a tuple of all model instances referenced by the given model
        instance, including indirect references. e.g. if an lport references
        a network, and that network references a QoS policy, then both are
        returned in the iterator.

        Raises a ReferencedObjectNotFound exception if a referenced model
        cannot be found in the db_store or NB DB.

        :param obj: Model instance
        :type obj:  model_framework.ModelBase
        :return:    iterator
        :raises:    exceptions.ReferencedObjectNotFound
        """
        return tuple(self.iter_model_references_deep(obj))

    def iter_model_references_deep(self, obj):
        """
        Return an iterator on all model instances referenced by the given model
        instance, including indirect references. e.g. if an lport references
        a network, and that network references a QoS policy, then both are
        returned in the iterator.

        Raises a ReferencedObjectNotFound exception upon a referenced model
        cannot be found in the db_store or NB DB. The exception is thrown
        when that object is reached by the iterator, allowing other objects
        to be processed. If you need to verify all objects can be referenced,
        use 'get_model_references_deep'.

        :param obj: Model instance
        :type obj:  model_framework.ModelBase
        :return:    iterator
        :raises:    exceptions.ReferencedObjectNotFound
        """
        seen = set()
        queue = collections.deque((obj,))
        while queue:
            item = queue.pop()
            if item.id in seen:
                continue
            seen.add(item.id)
            if model_proxy.is_model_proxy(item):
                # NOTE(oanson) _dereference raises an exception for unknown
                # objectes, i.e. objects not found in the NB DB.
                item = self._dereference(item)
                yield item
            for submodel in item.iter_submodels():
                queue.append(submodel)

    def _dereference(self, reference):
        """
        Dereference a model proxy object. Return first from the db_store, and
        if it is not there, from the NB DB. Raise a ReferencedObjectNotFound
        exception if it is not in the NB DB either.

        :param reference: Model instance
        :type reference:  model_framework.ModelBase
        :return:          iterator
        :raises:          exceptions.ReferencedObjectNotFound
        """
        item = reference.get_object()
        if item is None:
            item = self.nb_api.get(reference)
        if item is None:
            raise exceptions.ReferencedObjectNotFound(proxy=reference)
        return item

    def _send_update_events(self, iterable):
        for instance in iterable:
            self.update(instance)


def _has_basic_events(obj):
    return isinstance(obj, mixins.BasicEvents)


# Run this application like this:
# python df_local_controller.py <chassis_unique_name>
# <local ip address> <southbound_db_ip_address>
def main():
    chassis_name = cfg.CONF.host
    df_config.init(sys.argv)

    #创建北向api
    nb_api = api_nb.NbApi.get_instance()
    controller = DfLocalController(chassis_name, nb_api)
    #注册此service,实现service保活
    service.register_service('df-local-controller', nb_api)
    controller.run()
