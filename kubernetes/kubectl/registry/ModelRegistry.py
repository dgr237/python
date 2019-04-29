from kubectl.registry.ApiDefinition import ApiDefinition
from kubernetes.client import models
from kubernetes.client import apis
from kubectl.registry.GroupVersionKind import GroupVersionKind, GroupVersion
import re
from kubectl.registry.ModelDefinition import create_model_definition


method_reg_ex = re.compile('(?P<action>delete|read|create|replace|list)_(?P<with_namespace>namespaced_)?(?P<kind>.*)')


def get_apis(group_version, action_map, clazz, api_client):
    all_methods = dir(clazz)

    for method in all_methods:
        if not method.startswith("__") and not method.endswith('_with_http_info'):
            match = method_reg_ex.search(method)
            if match is not None:
                action = match.group('action')
                kind = match.group('kind')
                if not kind.startswith('collection'):
                    if kind.endswith('_for_all_namespaces'):
                        action = "{0}_for_all_namespaces".format(action)
                        kind = kind.replace('_for_all_namespaces', '')
                    kind = ''.join([part.capitalize() for part in kind.split('_')])
                    current_versions = action_map.get(kind, {})
                    action_map[kind] = current_versions
                    actions = current_versions.get(group_version.version, ApiDefinition(group_version.with_kind(kind), clazz(api_client)))
                    current_versions[group_version.version] = actions
                    if action == 'list_for_all_namespaces':
                        actions.requires_namespace = True
                    actions.add_action(action, method)

def get_supported_kinds(api_client):
    api_objects = apis.__dict__
    supported_kinds = {}
    for name, clazz in api_objects.iteritems():
        if isinstance(clazz, type):
            parts = re.findall('[A-Z][^A-Z]*', name)
            parts = parts[0:-1]
            if len(parts) > 1:
                if parts[0].startswith('V'):
                    gv = GroupVersion(version=parts[0])
                else:
                    gv = GroupVersion(group="".join(parts[0:-1]), version=parts[-1])

                get_apis(gv, supported_kinds, clazz, api_client)

    keys = supported_kinds.keys()
    for kind in keys:
        remove_apis_which_dont_implement_all_methods(kind, supported_kinds)
    return supported_kinds


def remove_apis_which_dont_implement_all_methods(kind, supported_kinds):
    current_action_map = supported_kinds[kind]
    version_map = dict(current_action_map)
    for version, actions in version_map.iteritems():
        if len(actions.actions) < 5:
            del current_action_map[version]
    if len(supported_kinds[kind]) == 0:
        del supported_kinds[kind]


def resolve_group_version_kind(instance_type):
    temp_type = re.sub('([a-z0-9])([A-Z])', r'\1_\2', instance_type)
    parts = temp_type.split('_')
    if parts[0].startswith('V'):
        group = 'Core'
        version = parts[0]
        kind = "".join(parts[1:])
    else:
        group = parts[0]
        version = parts[1]
        kind = "".join(parts[2:])
    return GroupVersionKind(group=group, version=version, kind=kind)


def sort_group_version_kind_by_version(gvk):
    return gvk.version


class ModelRegistry(object):
    def __init__(self):
        self.models = {}
        self.apis = {}
        self.kind_map = {}

    def build_core_register(self, api_client):
        supported_kinds = get_supported_kinds(api_client)
        core_models = models.__dict__
        for name, clazz in core_models.iteritems():
            if isinstance(clazz, type):
                gvk = resolve_group_version_kind(name)
                api_versions = supported_kinds.get(gvk.kind, {})
                api_definition = api_versions.get(gvk.version, None)
                if api_definition is not None:
                    gvk = api_definition.group_version_kind
                    self.apis[gvk] = api_definition

                self.models[gvk] = create_model_definition(clazz)
        kind_map = {}
        for gvk in self.apis.keys():
            gv_by_kind = kind_map.get(gvk.kind, [])
            gv_by_kind.append(gvk)
            kind_map[gvk.kind] = gv_by_kind

        for kind, gvks in kind_map.iteritems():
            gvks.sort(key=sort_group_version_kind_by_version)
            self.kind_map[kind] = gvks[-1]

    def register_custom_resource(self, group_version_kind, model_clazz, api_clazz=None):
        self.models[group_version_kind] = create_model_definition(model_clazz)
        if api_clazz is not None:
            action_map = {}
            get_apis(group_version_kind.get_group_version(), action_map, api_clazz, self.api_client)
            remove_apis_which_dont_implement_all_methods(group_version_kind.kind, action_map)
            api_definition=action_map.get(group_version_kind.kind, {}).get(group_version_kind.version, None)
            if api_definition is not None:
                self.apis[group_version_kind] = api_definition
                self.kind_map[group_version_kind.kind] = group_version_kind

    def get_model_definition(self, gvk):
        return self.models.get(gvk, None)

    def resolve_model_for_type(self, instance_type):
        gvk = resolve_group_version_kind(instance_type)
        return self.models.get(gvk, None)

    def __resolve_method(self, gvk, action):
        api_definition = self.apis.get(gvk, None)
        if api_definition is None:
            raise Exception('This registry is not supported')
        action_method = api_definition.actions.get(action, None)
        if action_method is None:
            raise Exception('{0} method is not supported'.format(action.capitalize()))
        return action_method, api_definition.api_clazz

    def invoke_create_api(self, gvk, namespace=None, **kwargs):
        action_method, api_clazz = self.__resolve_method(gvk, 'create')
        if self.requires_namespace(gvk):
            return getattr(api_clazz, action_method)(namespace=namespace, **kwargs)
        else:
            return getattr(api_clazz, action_method)(**kwargs)

    def invoke_replace_api(self, gvk, name, namespace=None, **kwargs):
        action_method, api_clazz = self.__resolve_method(gvk, 'replace')
        if self.requires_namespace(gvk):
            return getattr(api_clazz, action_method)(name=name, namespace=namespace, **kwargs)
        else:
            return getattr(api_clazz, action_method)(name=name, **kwargs)

    def invoke_delete_api(self, gvk, name, namespace=None, **kwargs):
        action_method, api_clazz = self.__resolve_method(gvk, 'delete')
        if self.requires_namespace(gvk):
            return getattr(api_clazz, action_method)(name, namespace, **kwargs)
        else:
            return getattr(api_clazz, action_method)(name, **kwargs)

    def invoke_get_api(self, gvk, name, namespace=None, **kwargs):
        action_method, api_clazz = self.__resolve_method(gvk, 'read')
        if self.requires_namespace(gvk):
            return getattr(api_clazz, action_method)(name=name, namespace=namespace, **kwargs)
        else:
            return getattr(api_clazz, action_method)(name=name, **kwargs)

    def invoke_list_api(self, gvk, namespace=None, **kwargs):
        action_method, api_clazz = self.__resolve_method(gvk, 'list')
        if self.requires_namespace(gvk):
            return getattr(api_clazz, action_method)(namespace, **kwargs)
        else:
            return getattr(api_clazz, action_method)(**kwargs)

    def invoke_list_for_all_namespaces_api(self, gvk, **kwargs):
        action_method, api_clazz = self.__resolve_method(gvk, 'list_for_all_namespaces')
        return getattr(api_clazz, action_method)(**kwargs)

    def requires_namespace(self, gvk):
        api_definition = self.apis.get(gvk, None)
        if api_definition is None:
            raise Exception('This registry is not supported')
        return api_definition.requires_namespace

