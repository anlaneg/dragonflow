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

from oslo_log import log
import stevedore

from dragonflow.common import exceptions

LOG = log.getLogger(__name__)

#加载app，并代理所有app的调用入口
class AppDispatcher(object):
    def __init__(self, app_list):
        #记录app list
        self.apps_list = app_list
        self.apps = {}

    def load(self, *args, **kwargs):
        #装载app_lists指出的所有app class
        mgr = stevedore.NamedExtensionManager(
            'dragonflow.controller.apps',
            self.apps_list,
            invoke_on_load=True,
            invoke_args=args,
            invoke_kwds=kwargs,
        )

        for ext in mgr:
            #指明扩展与class对象间的映射
            self.apps[ext.name] = ext.obj

    def dispatch(self, method, *args, **kwargs):
        errors = []
        for app in self.apps.values():
            #遍历已加载的所有app,检查是否存在method,如果存在，则我触发method调用
            #否则尝试下一个app
            handler = getattr(app, method, None)
            if handler is not None:
                try:
                    handler(*args, **kwargs)
                except Exception as e:
                    app_name = app.__class__.__name__
                    LOG.exception("Dragonflow application '%(name)s' "
                                  "failed in %(method)s",
                                  {'name': app_name, 'method': method})
                    errors.append(e)

        if errors:
            raise exceptions.DFMultipleExceptions(errors)
