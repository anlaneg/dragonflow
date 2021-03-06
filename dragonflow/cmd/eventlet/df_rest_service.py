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

import argparse

import bottle
from http import HTTPStatus  # Only from Python 3.5
from oslo_serialization import jsonutils

import dragonflow.common.exceptions as df_exceptions
from dragonflow.common import utils
from dragonflow.db import api_nb
from dragonflow.db import model_framework as mf
from dragonflow.db.models import all  # noqa


schema_file = None


def nbapi_decorator(f):
    # f(nbapi, ...) -> f(...)
    def wrapper(*args, **kwargs):
        # REVISIT(oanson) We might get away with using functools, but we
        # need nbapi to be instantiated after argument parsing.
        nbapi = api_nb.NbApi.get_instance()
        return f(nbapi, *args, **kwargs)
    return wrapper


def model_decorator(f):
    def wrapper(*args, **kwargs):
        name = kwargs['name']
        try:
            model = mf.get_model(name)
        except KeyError:
            bottle.abort(HTTPStatus.NOT_FOUND.value,
                         "Model '%s' not found" % (name,))
        return f(model, *args, **kwargs)
    return wrapper


@bottle.get('/<name>')
@model_decorator
@nbapi_decorator
def get_all(nbapi, model, name):
    instances = nbapi.get_all(model)
    result = [i.to_struct() for i in instances]
    bottle.response.content_type = 'application/json'
    return jsonutils.dumps(result)


@bottle.post('/<name>')
@model_decorator
@nbapi_decorator
def create(nbapi, model, name):
    """POST is create! Create a new instance"""
    json_data_dict = bottle.request.json
    if not json_data_dict:
        bottle.abort(HTTPStatus.PRECONDITION_FAILED.value,
                     "JSON content required")
    instance = model(**json_data_dict)
    nbapi.create(instance)
    bottle.response.status = HTTPStatus.CREATED.value


@bottle.put('/<name>')
@model_decorator
@nbapi_decorator
def update(nbapi, model, name):
    """PUT is update! Update an existing instance"""
    json_data_dict = bottle.request.json
    if not json_data_dict:
        bottle.abort(HTTPStatus.PRECONDITION_FAILED.value,
                     "JSON content required")
    instance = model(**json_data_dict)
    try:
        nbapi.update(instance)
    except df_exceptions.DBKeyNotFound:
        bottle.abort(HTTPStatus.NOT_FOUND.value,
                     "Model instance '%s/%s' not found" % (name, instance.id))
    bottle.response.status = HTTPStatus.NO_CONTENT.value


@bottle.get('/<name>/<id_>')
@model_decorator
@nbapi_decorator
def get(nbapi, model, name, id_):
    instance = nbapi.get(model(id=id_))
    if not instance:
        bottle.abort(HTTPStatus.NOT_FOUND.value,
                     "Model instance '%s/%s' not found" % (name, id_))
    bottle.response.content_type = 'application/json'
    return instance.to_json()


@bottle.delete('/<name>/<id_>')
@model_decorator
@nbapi_decorator
def delete(nbapi, model, name, id_):
    instance = nbapi.get(model(id=id_))
    if not instance:
        bottle.abort(HTTPStatus.NOT_FOUND.value,
                     "Model instance '%s/%s' not found" % (name, id_))
    nbapi.delete(instance)
    bottle.response.status = HTTPStatus.NO_CONTENT.value


@bottle.get('/schema.json')
def schema():
    if not schema_file:
        bottle.abort(HTTPStatus.NOT_FOUND.value)
    return bottle.static_file(schema_file, '/')


def main():
    parser = argparse.ArgumentParser(description='Dragonflow REST server')
    parser.add_argument('--host', type=str, default='127.0.0.1',
                        help='Host to listen on (127.0.0.1)')
    parser.add_argument('--port', type=int, default=8080,
                        help='Port to listen on (8080)')
    parser.add_argument('--config', type=str,
                        default='/etc/dragonflow/dragonflow.ini',
                        help=('Dragonflow config file '
                              '(/etc/dragonflow/dragonflow.ini)'))
    parser.add_argument('--json', type=str,
                        default=None,
                        help=('JSON schema file (None)'))
    args = parser.parse_args()
    global schema_file
    schema_file = args.json
    utils.config_init(None, [args.config])
    bottle.run(host=args.host, port=args.port)
