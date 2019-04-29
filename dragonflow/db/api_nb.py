# Copyright (c) 2015 OpenStack Foundation.
#
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

import copy
import time
import traceback

from jsonmodels import errors
from oslo_config import cfg
from oslo_log import log
from oslo_utils import excutils

import dragonflow.common.exceptions as df_exceptions
from dragonflow.common import utils as df_utils
from dragonflow.db import db_common
from dragonflow.db import model_proxy as mproxy
from dragonflow.db.models import core


LOG = log.getLogger(__name__)
_nb_api = None

#获得remote_db_hosts中首个主机及端口
def get_db_ip_port():
    #记录所有北向database主机及端口列表
    hosts = cfg.CONF.df.remote_db_hosts
    if not hosts:
        LOG.warning("Deprecated: remote_db_ip and remote_db_port are "
                    "deprecated for removal. Use remote_db_hosts instead")
        ip = cfg.CONF.df.remote_db_ip
        port = cfg.CONF.df.remote_db_port
        return ip, port
    #使用一个主机及端口
    host = hosts[0]
    ip, port = host.split(':')
    return ip, port

#取obj对应的topic
def _get_topic(obj):
    try:
        return getattr(obj, 'topic', None)
    except errors.ValidationError:
        return None


class NbApi(object):

    def __init__(self, db_driver):
        super(NbApi, self).__init__()
        #北向database驱动
        self.driver = db_driver
        self.controller = None
        self.use_pubsub = cfg.CONF.df.enable_df_pub_sub
        self.publisher = None
        self.subscriber = None
        self.enable_selective_topo_dist = \
            cfg.CONF.df.enable_selective_topology_distribution

    @staticmethod
    def get_instance():
        global _nb_api
        if _nb_api is None:
            #加载北向database驱动
            nb_driver = df_utils.load_driver(
                cfg.CONF.df.nb_db_class,
                df_utils.DF_NB_DB_DRIVER_NAMESPACE)
            nb_api = NbApi(nb_driver)
            #获取首个北向库的ip及port,并进行北向api的初始化
            ip, port = get_db_ip_port()
            nb_api._initialize(db_ip=ip, db_port=port)
            _nb_api = nb_api
        #返回北向数据库api
        return _nb_api

    def _initialize(self, db_ip='127.0.0.1', db_port=4001):
        #与database建立连接
        self.driver.initialize(db_ip, db_port, config=cfg.CONF.df)
        if self.use_pubsub:
            #开启了publish及subscriber，初始化相应对象
            self.publisher = self._get_publisher()
            self.subscriber = self._get_subscriber()
            #publisher完成初始化监听
            self.publisher.initialize()
            # Start a thread to detect DB failover in Plugin
            # 故障恢复相关
            self.publisher.set_publisher_for_failover(
                self.publisher,
                self.db_recover_callback)
            self.publisher.start_detect_for_failover()

    def set_db_change_callback(self, db_change_callback):
        if self.use_pubsub:
            # This is here to not allow multiple subscribers to be started
            # under the same process. One should be more than enough.
            if not self.subscriber.is_running:
                #启动订阅者
                self._start_subscriber(db_change_callback)
                # Register for DB Failover detection in NB Plugin
                self.subscriber.set_subscriber_for_failover(
                    self.subscriber,
                    db_change_callback)
                self.subscriber.register_hamsg_for_db()
            else:
                LOG.warning('Subscriber is already initialized, ignoring call')

    def close(self):
        if self.publisher:
            self.publisher.close()
        if self.subscriber:
            self.subscriber.close()

    def db_recover_callback(self):
        # 执行故障恢复
        # only db with HA func can go in here
        self.driver.process_ha()
        self.publisher.process_ha()
        self.subscriber.process_ha()
        self.controller.sync()

    def _get_publisher(self):
        #装载pub，sub驱动,并返回publisher对象
        pub_sub_driver = df_utils.load_driver(
            cfg.CONF.df.pub_sub_driver,
            df_utils.DF_PUBSUB_DRIVER_NAMESPACE)
        return pub_sub_driver.get_publisher()

    def _get_subscriber(self):
        #装载驱动，并返回subscriber对象
        pub_sub_driver = df_utils.load_driver(
            cfg.CONF.df.pub_sub_driver,
            df_utils.DF_PUBSUB_DRIVER_NAMESPACE)
        return pub_sub_driver.get_subscriber()

    def _start_subscriber(self, db_change_callback):
        #连接到publisher,并设置消息处理回调
        self.subscriber.initialize(db_change_callback)
        #注册all topic
        self.subscriber.register_topic(db_common.SEND_ALL_TOPIC)
        #获取自已可连接的publishers列表
        publishers_ips = cfg.CONF.df.publishers_ips
        uris = {'%s://%s:%s' % (
                cfg.CONF.df.publisher_transport,
                ip,
                cfg.CONF.df.publisher_port) for ip in publishers_ips}
        #取出表Publisher中的所有成员
        publishers = self.get_all(core.Publisher)
        
        #取所有已知的publisher全集
        uris |= {publisher.uri for publisher in publishers}
        #促使订阅这些publisher
        for uri in uris:
            self.subscriber.register_listen_address(uri)
        #开始接收
        self.subscriber.daemonize()

    def support_publish_subscribe(self):
        #是否支持发布订阅
        return self.use_pubsub

    #触发database 事件
    def _send_db_change_event(self, table, key, action, value, topic):
        if not self.use_pubsub:
            return

        if not self.enable_selective_topo_dist or topic is None:
            topic = db_common.SEND_ALL_TOPIC
        #构造并发送database update 消息
        update = db_common.DbUpdate(table, key, action, value, topic=topic)
        #将update转换为message,并投递给订阅者
        self.publisher.send_event(update)
        time.sleep(0)

    def register_notification_callback(self, notification_cb):
        #注册nb的通知回调
        self._notification_cb = notification_cb

    def register_listener_callback(self, cb, topic):
        """Register a callback for Neutron listener

        This method is used to register a callback for Neutron listener
        to handle the message from Dragonflow controller. It should only be
        called from Neutron side and only once.

        :param: a callback method to handle the message from Dragonflow
                controller
        :param topic: the topic this neutron listener cares about, e.g the
                      hostname of the node
        """
        if not self.use_pubsub:
            return
        #设置消息处理cb,订阅topic,开始接收消息
        self.subscriber.initialize(cb)
        self.subscriber.register_topic(topic)
        self.subscriber.daemonize()

    def create(self, obj, skip_send_event=False):
        """Create the provided object in the database and publish an event
           about its creation.
        """
        model = type(obj)
        #触发on_create_pre hook
        obj.on_create_pre()
        serialized_obj = obj.to_json()
        #取obj对应的topic
        topic = _get_topic(obj)
        #写obj到database
        self.driver.create_key(model.table_name, obj.id,
                               serialized_obj, topic)
        #触发database create event，并投递给订阅者
        if not skip_send_event:
            self._send_db_change_event(model.table_name, obj.id, 'create',
                                       serialized_obj, topic)

    def update(self, obj, skip_send_event=False):
        """Update the provided object in the database and publish an event
           about the change.

           This method reads the existing object from the database and updates
           any non-empty fields of the provided object. Retrieval happens by
           id/topic fields.
        """
        model = type(obj)
        #装载obj
        full_obj = self.get(obj)
        db_obj = copy.copy(full_obj)

        if full_obj is None:
            raise df_exceptions.DBKeyNotFound(key=obj.id)

        #执行obj更新
        changed_fields = full_obj.update(obj)

        if not changed_fields:
            #无变换
            return

        #触发 on_update_pre
        full_obj.on_update_pre(db_obj)
        serialized_obj = full_obj.to_json()
        topic = _get_topic(full_obj)

        #重新写回到database
        self.driver.set_key(model.table_name, full_obj.id,
                            serialized_obj, topic)
        #触发database set event，并投递给订阅者
        if not skip_send_event:
            self._send_db_change_event(model.table_name, full_obj.id, 'set',
                                       serialized_obj, topic)

    def delete(self, obj, skip_send_event=False):
        """Delete the provided object from the database and publish the event
           about its deletion.

           The provided object does not have to have all the fields filled,
           just the ID / topic (if applicable) of the object we wish to delete.
        """
        model = type(obj)
        obj.on_delete_pre()
        topic = _get_topic(obj)
        try:
            #自database中移除
            self.driver.delete_key(model.table_name, obj.id, topic)
        except df_exceptions.DBKeyNotFound:
            with excutils.save_and_reraise_exception():
                LOG.debug(
                    'Could not find object %(id)s to delete in %(table)s',
                    {'id': obj.id, 'table': model.table_name})

        #触发databse delete event
        if not skip_send_event:
            self._send_db_change_event(model.table_name, obj.id, 'delete',
                                       None, topic)

    def get(self, lean_obj):
        """Retrieve a model instance from the database. This function uses
           lean_obj to deduce ID and model type

           >>> nb_api.get(Chassis(id="one"))
           Chassis(id="One", ip="192.168.121.22", tunnel_types=["vxlan"])

        """
        if mproxy.is_model_proxy(lean_obj):
            lean_obj = lean_obj.get_proxied_model()(id=lean_obj.id)
        model = type(lean_obj)
        try:
            #自database中查询obj
            serialized_obj = self.driver.get_key(
                model.table_name,
                lean_obj.id,
                _get_topic(lean_obj),
            )
        except df_exceptions.DBKeyNotFound:
            exception_tb = traceback.format_exc()
            LOG.debug(
                'Could not get object %(id)s from table %(table)s',
                {'id': lean_obj.id, 'table': model.table_name})
            LOG.debug('%s', (exception_tb,))
        else:
            #返回此对象（自json重建）
            return model.from_json(serialized_obj)

    def get_all(self, model, topic=None):
        """Get all instances of provided model, can be limited to instances
           with a specific topic.
        """
        #自database中拿出所有model.table_name的数据
        all_values = self.driver.get_all_entries(model.table_name, topic)
        all_objects = [model.from_json(e) for e in all_values]
        #触发 on_get_all_post hook点
        return model.on_get_all_post(all_objects)
