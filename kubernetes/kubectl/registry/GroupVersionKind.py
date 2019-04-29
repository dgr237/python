import yaml

def create_group_version_kind(**kwargs):
    if 'kind' in kwargs and 'api_version' in kwargs or 'registry' in kwargs:
        if 'kind' in kwargs and 'api_version':
            kind = kwargs['kind']
            api_version = kwargs['api_version']
        else:
            model = kwargs['registry']
            if isinstance(model, dict):
                kind = model.get('kind')
                api_version = model.get('apiVersion')
            elif isinstance(model, str):
                model = yaml.load(model)
                kind = model.get('kind')
                api_version = model.get('apiVersion')
            else:
                kind = model.kind
                api_version = model.api_version
        group, _, version = api_version.partition('/')
        version = version.capitalize()
        if version == "":
            version = group.capitalize()
            group = 'Core'
        group = "".join(group.rsplit(".k8s.io", 1))
        group = "".join(word.capitalize() for word in group.split('.'))
    elif 'kind' in kwargs and 'group' in kwargs and 'version' in kwargs:
        kind = kwargs['kind']
        group = kwargs['group']
        version = kwargs['version'].capitalize()
    return GroupVersionKind(group, version, kind)


class GroupVersion(object):
    def __init__(self, group, version):
        self.version = version
        self.group = group

    def with_kind(self, kind):
        return GroupVersionKind(self.group, version=self.version, kind=kind)

    def __str__(self):
        return "{0}.{1}".format(self.group, self.version)

    def __eq__(self, other):
        if other is None:
            return False

        return self.group == other.group and self.version == other.version

    def __hash__(self):
        return self.__str__().__hash__()


class GroupVersionKind(GroupVersion):
    def __init__(self, group, version, kind):
        super(GroupVersionKind, self).__init__(group, version)
        self.kind = kind

    def get_group_version(self):
        return GroupVersion(self.group, self.version)

    def __str__(self):
        return "{0}/{1}.{2}".format(self.kind, self.group, self.version)

    def __eq__(self, other):
        if other is None:
            return False

        return self.kind == other.kind and self.group == other.group and self.version == other.version

    def __hash__(self):
        return self.__str__().__hash__()

    def __repr__(self):
        return self.__str__()




