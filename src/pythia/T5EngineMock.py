class T5EngineMock:
    def __init__(self):
        self.ambiguitiesFromT5 = {}
        self._updateAbalone()
        self._updateSoccer()

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

    def _updateSoccer(self):
        ambiguities_from_t5 = [
            ('tournamentid', 'tournamentregionid', 'tournament'),
            ('seasonname', 'tournamentname', 'name'),
            ('yellowcard', 'redcard', 'card'),
            ('lastname', 'teamname', 'name'),
            ('tournamentregionname', 'regioncode', 'region'),
            ('tournamentid', 'tournamentshortname', 'tournament'),
            ('tournamentname', 'tournamentshortname', 'tournament'),
            ('playedpositions', 'playedpositionsshort', 'playedposition'),
            ('seasonname', 'tournamentregionname', 'name'),
            ('tournamentid', 'tournamentregionname', 'tournament'),
            ('tournamentregioncode', 'tournamentshortname', 'tournament'),
            ('tournamentregionname', 'tournamentshortname', 'tournament'),
            ('seasonid', 'seasonname', 'season'),
            ('tournamentregioncode', 'teamregionname', 'region'),
            ('teamname', 'teamregionname', 'team'),
            ('age', 'weight', 'measurement'),
            ('teamid', 'teamname', 'team'),
            ('tournamentregionid', 'tournamentregionname', 'tournament'),
            ('shotspergame', 'aerialwonpergame', 'game'),
            ('ranking', 'positiontext', 'position'),
            ('tournamentregionid', 'regioncode', 'region'),
            ('tournamentname', 'teamname', 'name'),
            ('teamid', 'teamregionname', 'team'),
            ('tournamentregionname', 'teamregionname', 'region'),
            ('firstname', 'lastname', 'middle name'),
            ('tournamentregionid', 'tournamentshortname', 'tournament'),
            ('ranking', 'weight', 'measure'),
            ('weight', 'rating', 'measurement'),
            ('ranking', 'rating', 'rate'),
            ('tournamentregionid', 'teamregionname', 'region'),
            ('tournamentregionname', 'tournamentname', 'tournament'),
            ('height', 'weight', 'measurement'),
            ('tournamentregionid', 'tournamentregioncode', 'tournament'),
            ('lastname', 'name', 'family name'),
            ('tournamentid', 'tournamentname', 'tournament'),
            ('index', 'weight', 'measure'),
            ('tournamentregioncode', 'tournamentregionname', 'tournament'),
            ('firstname', 'teamname', 'name'),
            ('tournamentid', 'tournamentregioncode', 'tournament'),
            ('tournamentregionid', 'tournamentname', 'tournament'),
            ('regioncode', 'teamregionname', 'region'),
            ('playedpositions', 'positiontext', 'position'),
            ('manofthematch', 'ismanofthematch', 'match'),
            ('index', 'rating', 'rate'),
            ('seasonname', 'lastname', 'name'),
            ('firstname', 'name', 'forename'),
            ('positiontext', 'playedpositionsshort', 'position'),
            ('age', 'rating', 'measurement'),
            ('index', 'age', 'measurement'),
            ('age', 'height', 'measurement'),
            ('seasonname', 'teamname', 'name'),
            ('playerid', 'teamid', 'id'),
            ('tournamentregioncode', 'tournamentname', 'tournament'),
            ('index', 'ranking', 'rate'),
        ]
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
