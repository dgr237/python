from kubernetes.client.rest import ApiException
from kubectl.registry.GroupVersionKind import create_group_version_kind
import yaml


class Parser(object):
    primitives = {'str': '_Parser__deserialize_object',
                  'int': '_Parser__deserialize_primitive',
                  'datetime': '_Parser__deserialize_datetime',
                  'date': '_Parser__deserialize_date',
                  'bool': '_Parser__deserialize_primitive',
                  'object': '_Parser__deserialize_object',
                  'dict(str, str)': '_Parser__deserialize_object'}

    def __init__(self, model_registry):
        self.type_definitions = {}
        self.model_registry = model_registry

    def parse(self, source):
        """

        :param model_definition: ModelDefinition
        :type source: dict, str
        """
        if isinstance(source, str):
            source = yaml.safe_load(source)
        gvk = create_group_version_kind(model=source)
        model_definition = self.model_registry.get_model_definition(gvk)
        return self.__deserialize_model(model_definition, source)

    def __deserialize_list(self, field_type, values):
        instance_type = field_type[5:-1]
        return [self.__deserialize_item(instance_type, value) for value in values]

    def __deserialize_model(self, model_definition, value):
        if model_definition is not None and value is not None:
            kwargs = {}
            for attribute, attribute_type in model_definition.swagger_types.iteritems():
                field_name = model_definition.attribute_map.get(attribute)
                result = value.get(field_name, None)
                if result is not None:
                    kwargs[attribute] = self.__deserialize_item(attribute_type, result)

            return model_definition.create_model(**kwargs)
        return None

    def __deserialize_class(self, instance_type, value):
        model_definition = self.model_registry.resolve_model_for_type(instance_type)
        return self.__deserialize_model(model_definition, value)

    def __deserialize_item(self, field_type, value):
        if field_type.startswith('list'):
            return self.__deserialize_list(field_type, value)
        else:
            deserializer = self.primitives.get(field_type, '_Parser__deserialize_class')
            return getattr(self, deserializer)(field_type, value)

    def __deserialize_primitive(self, field_type, value):
        # type: (str, object) -> object
        """
        Deserializes string to primitive type.
        :param field_type: str.
        :param value: object
        :return: int, long, float, bool.
        """
        try:
            klass=eval(field_type)
            value = klass(value)
        except UnicodeEncodeError:
            value = unicode(value)
        except TypeError:
            value = value
        return value

    def __deserialize_date(self, field_type, value):
        """
        Deserializes string to date.
        :param field_type: str.
        :param value: str.
        :return: date.
        """
        if not value:
            return None
        try:
            from dateutil.parser import parse
            return parse(value).date()
        except ImportError:
            return value
        except ValueError:
            raise ApiException(
                status=0,
                reason="Failed to parse `{0}` into a date object"
                .format(value)
            )

    def __deserialize_datetime(self, field_type, value):
        """
        Deserializes string to datetime.
        The string should be in iso8601 datetime format.
        :param field_type: str.
        :param value: str.
        :return: datetime.
        """
        if not value:
            return None
        try:
            from dateutil.parser import parse
            return parse(value)
        except ImportError:
            return value
        except ValueError:
            raise ApiException(
                status=0,
                reason="Failed to parse `{0}` into a datetime object".
                format(value)
            )

    def __deserialize_object(self, field_type, value):
        return value
