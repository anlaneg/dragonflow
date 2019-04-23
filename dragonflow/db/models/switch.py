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
from jsonmodels import fields

from dragonflow.common import constants
import dragonflow.db.field_types as df_fields
import dragonflow.db.model_framework as mf
from dragonflow.db.models import l2
from dragonflow.db.models import mixins


@mf.register_model
@mf.construct_nb_db_model
class SwitchPort(mf.ModelBase, mixins.BasicEvents, mixins.Name):
    #定义表结构
    table_name = 'switch_port'

    #port编号
    port_num = fields.IntField()
    #admin状态
    admin_state = df_fields.EnumField(('up', 'down'))
    lport = df_fields.ReferenceField(l2.LogicalPort)
    #接口类型
    type = df_fields.EnumField(
        (
            constants.SWITCH_BRIDGE_INTERFACE,
            constants.SWITCH_PATCH_INTERFACE,
            constants.SWITCH_COMPUTE_INTERFACE,
            constants.SWITCH_TUNNEL_INTERFACE,
            constants.SWITCH_UNKNOWN_INTERFACE,
        ),
    )
    #对端信息
    peer = fields.StringField()
    #接口mac地址
    mac_in_use = df_fields.MacAddressField()
    #
    attached_mac = df_fields.MacAddressField()
    #隧道类型
    tunnel_type = fields.StringField()
