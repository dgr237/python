def create_model_definition(clazz):
    return ModelDefinition(clazz.swagger_types, clazz.attribute_map, clazz)


class ModelDefinition(object):
    def __init__(self, swagger_types, attribute_map, model_clazz):
        self.swagger_types = swagger_types
        self.attribute_map = attribute_map
        self.model_clazz = model_clazz

    def create_model(self, **kwargs):
        return self.model_clazz(**kwargs)

