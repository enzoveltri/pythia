import json


class DeltaAmbiguous:

    def __init__(self, schema, attr1, attr2, predicted, real):
        self.schema = schema
        self.attr1 = attr1
        self.attr2 = attr2
        self.predicted = predicted
        self.real = real

    def toJSON(self):
        return {'schema': self.schema,
                'attr1': self.attr1,
                'attr2': self.attr2,
                'predicted': self.predicted,
                'real': self.real}

