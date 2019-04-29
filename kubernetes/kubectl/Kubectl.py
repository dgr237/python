from kubernetes.client.rest import ApiException
from kubectl.registry.GroupVersionKind import create_group_version_kind
from kubectl.registry.ModelRegistry import ModelRegistry
from kubectl.utils.Parser import Parser
import yaml
from kubernetes.client.models.v1_delete_options import V1DeleteOptions


class Kubectl(object):
    def __init__(self, api_client):

        self.model_registry = ModelRegistry()
        self.model_registry.build_core_register(api_client)
        self.parser = Parser(self.model_registry)

    def register_custom_resource(self, group_version_kind, model_clazz, api_clazz=None):
        self.model_registry.register_custom_resource(group_version_kind, model_clazz, api_clazz)

    def parse_model(self, body):
        if body is None:
            raise Exception('body parameter not supplied')

        if isinstance(body, str):
            body = yaml.load(body)

        if isinstance(body, dict):
            model = self.parser.parse(body)
        else:
            model = body
        gvk = create_group_version_kind(model=model)
        return gvk, model

    def create_resource(self, body, namespace='default', **kwargs):
        gvk, model = self.parse_model(body)
        return self.__create_resource(gvk, model, namespace, **kwargs)

    def __create_resource(self, gvk,  model, **kwargs):
        try:
            if self.model_registry.requires_namespace(gvk):
                ns = model.metadata.namespace
                if ns is not None:
                    namespace = ns
                return self.model_registry.invoke_create_api(gvk, namespace=namespace, body=model, **kwargs)
            else:
                return self.model_registry.invoke_create_api(gvk, body=model, **kwargs)
        except ApiException, ex:
            model = self.parser.parse(ex.body)
            return model

    def update_resource(self, body, namespace='default', **kwargs):
        gvk, model = self.parse_model(body)
        return self.__update_resource(gvk, model, namespace, **kwargs)

    def __update_resource(self, gvk, model, namespace='default', **kwargs):
        try:
            if self.model_registry.requires_namespace(gvk):
                ns = model.metadata.namespace
                if ns is not None:
                    namespace = ns
                return self.model_registry.invoke_replace_api(gvk,
                                                              name=model.metadata.name,
                                                              namespace=namespace,
                                                              body=model,
                                                              **kwargs)
            else:
                return self.model_registry.invoke_replace_api(gvk,
                                                              name=model.metadata.name,
                                                              body=model,
                                                              **kwargs)
        except ApiException, ex:
            model = self.parser.parse(ex.body)
            return model

    def read_resource(self, kind=None, name=None, namespace='default', body=None, **kwargs):
        if body is not None:
            gvk, model = self.parse_model(body)
            ns = model.metadata.namespace
            if ns is not None:
                namespace = ns
            name = model.metadata.name
        elif kind is not None and name is not None:
            gvk = self.model_registry.kind_map.get(kind, None)
        else:
            raise Exception('name and/or kind not specified')
        return self.__read_resource(gvk, name, namespace, **kwargs)

    def __read_resource(self, gvk, name, namespace, **kwargs):
        try:
            if self.model_registry.requires_namespace(gvk):
                return self.model_registry.invoke_get_api(gvk,
                                                          name=name,
                                                          namespace=namespace,
                                                          **kwargs)
            else:
                return self.model_registry.invoke_get_api(gvk,
                                                          name=name,
                                                          **kwargs)
        except ApiException, ex:
            model = self.parser.parse(ex.body)
            return model

    def delete_resource(self, kind=None, name=None, namespace='default', body=None, **kwargs):
        if body is not None:
            gvk, model = self.parse_model(body)
            ns = model.metadata.namespace
            if ns is not None:
                namespace = ns

            name = model.metadata.name
        elif kind is not None and name is not None:
            gvk = self.model_registry.kind_map.get(kind, None)
        else:
            raise Exception('name and/or kind not specified')
        return self.__delete_resource(gvk, name=name, namespace=namespace, **kwargs)

    def __delete_resource(self, gvk, name, namespace='default', **kwargs):
        try:
            delete_options = V1DeleteOptions()
            if self.model_registry.requires_namespace(gvk):
                if namespace is None:
                    namespace = 'default'
                return self.model_registry.invoke_delete_api(gvk,
                                                             name=name,
                                                             namespace=namespace,
                                                             body=delete_options,
                                                             **kwargs)
            else:
                return self.model_registry.invoke_delete_api(gvk,
                                                             name=name,
                                                             body=delete_options,
                                                             **kwargs)
        except ApiException, ex:
            model = self.parser.parse(ex.body)
            return model

    def list_resource(self, kind=None, namespace='default', **kwargs):
        if kind is not None:
            gvk = self.model_registry.kind_map.get(kind, None)
        else:
            raise Exception('kind not specified')
        return self.__list_resource(gvk, namespace=namespace, **kwargs)

    def __list_resource(self, gvk, namespace, **kwargs):
        try:
            if self.model_registry.requires_namespace(gvk):
                if namespace is None:
                    namespace = 'default'
                return self.model_registry.invoke_list_api(gvk,
                                                           namespace=namespace,
                                                           **kwargs)
            else:
                return self.model_registry.invoke_list_api(gvk,
                                                           **kwargs)
        except ApiException, ex:
            model = self.parser.parse(ex.body)
            return model

    def list_resource_all_namespaces(self, kind=None, **kwargs):
        if kind is not None:
            gvk = self.model_registry.kind_map.get(kind, None)
        else:
            raise Exception('kind not specified')
        return self.__list_resource_all_namespaces(gvk, **kwargs)

    def __list_resource_all_namespaces(self, gvk, **kwargs):
        try:
            return self.model_registry.invoke_list_for_all_namespaces_api(gvk,
                                                                          **kwargs)
        except ApiException, ex:
            model = self.parser.parse(ex.body)
            return model

    def apply(self, body):
        gvk, model = self.parse_model(body)
        result = self.__read_resource(gvk, name=model.metadata.name, namespace=model.metadata.namespace)

        if result.kind == 'Status' and result.reason == 'NotFound':
            action = 'created'
            result = self.__create_resource(gvk, model)
        else:
            model.metadata = result.metadata
            action = 'updated'
            result = self.__update_resource(gvk, model)

        if result.kind != 'Status':
            print "{0}.{1} \"{2}\" {3}".format(result.kind.lower(), result.api_version, result.metadata.name, action)
        else:
            print result.message

        return result
