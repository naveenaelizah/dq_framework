# rules/base_rule.py
class BaseRule:
    def __init__(self, name, description, validation_fn, params=None):
        self.name = name
        self.description = description
        self.validation_fn = validation_fn
        self.params = params or {}

    def validate(self, data):
        return self.validation_fn(data, **self.params)
