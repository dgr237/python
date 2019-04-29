from kubectl.registry.GroupVersionKind import  GroupVersionKind


class ApiDefinition(object):
    def __init__(self, group_version_kind, api_clazz, requires_namespace=False):
        """

        :type requires_namespace: bool
        :type api_clazz: type
        :type group_version_kind: GroupVersionKind

        """
        self.group_version_kind = group_version_kind
        self.api_clazz = api_clazz
        self.requires_namespace = requires_namespace
        self.actions = {}

    def add_action(self, action_type, action_method):
        """

        :param action_method: str
        :type action_type: str
        """
        self.actions[action_type] = action_method


