class T5EngineMock:
    def __init__(self):
        self.ambiguitiesFromT5 = {}
        self._updateAbalone()

    def _updateAbalone(self):
        ambiguities_from_t5 = [('diameter', 'height', 'dimension'),
         ('height', 'diameter', 'dimension'),
         ('diameter', 'length', 'dimension'),
         ('length', 'diameter', 'dimension'),
         ('height', 'length', 'distance'),
         ('length', 'height', 'distance'),
         ('shell weight', 'shucked weight', 'weight'),
         ('shucked weight', 'shell weight', 'weight'),
         ('shell weight', 'viscera weight', 'weight'),
         ('viscera weight', 'shell weight', 'weight'),
         ('shell weight', 'whole weight', 'weight'),
         ('whole weight', 'shell weight', 'weight'),
         ('shucked weight', 'viscera weight', 'weight'),
         ('viscera weight', 'shucked weight', 'weight'),
         ('shucked weight', 'whole weight', 'weight'),
         ('whole weight', 'shucked weight', 'weight'),
         ('viscera weight', 'whole weight', 'weight'),
         ('whole weight', 'viscera weight', 'weight')]
        for attr1, attr2, label in ambiguities_from_t5:
            key = self._toKeyModel(attr1, attr2)
            self.ambiguitiesFromT5[key] = label

    def _toKeyModel(self, val1, val2):
        return val1+"***"+val2

    def makePrediction(self, input):
        input = input.lower()
        tokens = input.split("attr1: ")
        attributes = tokens[1].split("attr2: ")
        key = self._toKeyModel(attributes[0].strip(), attributes[1].strip())
        return self.ambiguitiesFromT5.get(key)
