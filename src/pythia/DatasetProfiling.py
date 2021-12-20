########################################################
## WE ASSUME WE GOT PROFILING DATA FROM EXTERNAL SOURCES
## DEPLOYING T5 REQUIRES CLOUD VMs SO WE CACHED PREDICTIONS IN <a1,a2,label> LIST --> AMBIGUITIES
########################################################

from Constants import CATEGORICAL, NUMERICAL

def _getAttribute(attributes, name):
    for attribute in attributes:
        if attribute[0] == name:
            return attribute
    return None

def _addType(ambiguities, attributes):
    ambiguitiesWithType = []
    for ambiguity in ambiguities:
        attribute1 = _getAttribute(attributes, ambiguity[0])
        attribute2 = _getAttribute(attributes, ambiguity[1])
        if attribute1 is None or attribute2 is None:
            print("*** ERROR: ", ambiguity[0], "or", ambiguity[1], "not found")
        ambWithType = (attribute1, attribute2, ambiguity[2])
        ambiguitiesWithType.append(ambWithType)
    return ambiguitiesWithType

def _irisProfiling(tableName):
    ## Pythia predictions
    ambiguities = []
    ambiguities.append(('petal length', 'petal width', 'petal'))
    ambiguities.append(('petal length', 'sepal length', 'length'))
    ambiguities.append(('petal length', 'sepal width', 'sepal'))
    ambiguities.append(('petal width', 'sepal length', 'sepal'))
    ambiguities.append(('petal width', 'sepal width', 'width'))
    ambiguities.append(('sepal length', 'sepal width', 'sepal'))
    ## Profiling information
    tb = tableName
    attributes = [('index', CATEGORICAL), ('sepal length', NUMERICAL), ('sepal width', NUMERICAL),
                  ('petal length', NUMERICAL), ('petal width', NUMERICAL), ('class',CATEGORICAL)]
    pk = 'index'
    fds = []
    compositeKeys = []
    ambiguitiesWithType = _addType(ambiguities, attributes)
    return (ambiguitiesWithType, pk, fds, compositeKeys, tb, attributes)

def _basketFullProfiling():
    ## Pythia predictions
    ambiguities = []
    ambiguities.append(('team_city', 'town', 'city'))
    ambiguities.append(('first_name', 'player_name', 'name'))
    ambiguities.append(('first_name', 'second_name', 'name'))
    ambiguities.append(('player_name', 'second_name', 'name'))
    ambiguities.append(('3 point field goal made', 'field goal made', 'field goal'))
    ambiguities.append(('3 point field goal attempted', '3 point field goal percentage', 'point'))
    ambiguities.append(('free throws attempted', 'free throws made', 'throw'))
    ambiguities.append(('free throws attempted', 'free throws percentage', 'throw'))
    ambiguities.append(('free throws made', 'free throws percentage', 'throw'))
    ambiguities.append(('3 point field goal percentage', 'field goals percentage', 'field goal'))
    ambiguities.append(('3 point field goal percentage', 'field goals percentage', 'percentage'))
    ambiguities.append(('3 point field goal percentage', 'free throws percentage', 'percentage'))
    ambiguities.append(('defensive rebounds', 'offensive rebounds', 'rebound'))
    ambiguities.append(('defensive rebounds', 'rebounds', 'rebound'))
    ambiguities.append(('offensive rebounds', 'rebounds', 'rebound'))
    ambiguities.append(('3 point field goal made', '3 point field goal percentage', 'point'))
    ambiguities.append(('field goal made', '3 point field goal percentage', 'field goal'))
    ambiguities.append(('field goal made', '3 point field goal attempted', 'field goal'))
    ambiguities.append(('field goal made', 'free throws made', 'made'))
    ambiguities.append(('field goal made', 'field goals percentage', 'field goal'))
    ambiguities.append(('rebounds', 'assists', 'rebound'))
    ambiguities.append(('rebounds', 'blocks', 'rebound'))
    ambiguities.append(('assists', 'steals', 'game sheet'))
    ambiguities.append(('blocks', 'town', 'city'))
    ambiguities.append(('field goal attempts', 'free throws attempted', 'attempt'))
    ## Profiling information
    tableName = 'basket_full_names'
    attributes = [('index', CATEGORICAL),
                  ('player_name', CATEGORICAL),
                  ('first_name', CATEGORICAL),
                  ('minutes', NUMERICAL),
                  ('field goal made', NUMERICAL),
                  ('rebounds', NUMERICAL),
                  ('3 point field goal attempted', NUMERICAL),
                  ('assists', NUMERICAL),
                  ('3 point field goal made', NUMERICAL),
                  ('offensive rebounds', 'turnovers', NUMERICAL),
                  ('start_position', CATEGORICAL),
                  ('personal fouls', NUMERICAL),
                  ('points', NUMERICAL),
                  ('field goal attempts', NUMERICAL),
                  ('steals', NUMERICAL),
                  ('free throws attempted', NUMERICAL),
                  ('blocks', NUMERICAL),
                  ('defensive rebounds', NUMERICAL),
                  ('free throws made', NUMERICAL),
                  ('free throws percentage', NUMERICAL),
                  ('field goals percentage', NUMERICAL),
                  ('3 point field goal percentage', NUMERICAL),
                  ('second_name', CATEGORICAL),
                  ('team_city', CATEGORICAL),
                  ('team', CATEGORICAL),
                  ('town', CATEGORICAL)]
    pk = 'player_name'
    ## FDS team --> town; team --> team_city
    ## fd (LHS,RHS,[printValues])
    fds = [('team', 'town', ['in', 'has', 'players']), ('team', 'team_city', ['in', 'has', 'players'])]
    ## CKs  ID = player_name+team
    compositeKeys = [('player_name', 'team')]
    ambiguitiesWithType = _addType(ambiguities, attributes)
    return (ambiguitiesWithType, pk, fds, compositeKeys, tableName, attributes)

def _basketAcronymsProfiling():
    ## Pythia predictions
    ambiguities = []
    ambiguities.append(('dreb', 'oreb', 'reb'))
    ambiguities.append(('dreb', 'reb', 'reb'))
    ambiguities.append(('fg3_pct', 'fg3a', 'fg'))
    ambiguities.append(('fg3_pct', 'fg3m', 'fg'))
    ambiguities.append(('fg3_pct', 'fg_pct', 'pct'))
    ambiguities.append(('fg3_pct', 'fga', 'fg'))
    ambiguities.append(('fg3_pct', 'ft_pct', 'pct'))
    ambiguities.append(('fg3a', 'fg3_pct', 'fg'))
    ambiguities.append(('fg3a', 'fg3m', 'fg'))
    ambiguities.append(('fg3a', 'fga', 'fg'))
    ambiguities.append(('fg3m', 'fg3_pct', 'fg'))
    ambiguities.append(('fg3m', 'fg3a', 'fg'))
    ambiguities.append(('fg3m', 'fg_pct', 'fg'))
    ambiguities.append(('fg3m', 'fga', 'fg'))
    ambiguities.append(('fg_pct', 'fg3_pct', 'pct'))
    ambiguities.append(('fg_pct', 'fg3a', 'fg'))
    ambiguities.append(('fg_pct', 'fg3m', 'fg'))
    ambiguities.append(('fg_pct', 'fga', 'fg'))
    ambiguities.append(('fg_pct', 'ft_pct', 'pct'))
    ambiguities.append(('fga', 'fg3a', 'fg'))
    ambiguities.append(('fga', 'fg3m', 'fg'))
    ambiguities.append(('fga', 'fg_pct', 'fg'))
    ambiguities.append(('fga', 'fta', 'fg'))
    ambiguities.append(('fgm', 'fg3a', 'fg'))
    ambiguities.append(('fgm', 'fg3m', 'fg'))
    ambiguities.append(('fgm', 'fg_pct', 'fg'))
    ambiguities.append(('fgm', 'fga', 'fg'))
    ambiguities.append(('first_name', 'player_name', 'name'))
    ambiguities.append(('first_name', 'second_name', 'name'))
    ambiguities.append(('ft_pct', 'fg3_pct', 'pct'))
    ambiguities.append(('ft_pct', 'fg_pct', 'pct'))
    ambiguities.append(('ft_pct', 'fta', 'ft'))
    ambiguities.append(('ft_pct', 'ftm', 'ft'))
    ambiguities.append(('fta', 'ft_pct', 'ft'))
    ambiguities.append(('fta', 'ftm', 'ft'))
    ambiguities.append(('ftm', 'ft_pct', 'ft'))
    ambiguities.append(('ftm', 'fta', 'ft'))
    ambiguities.append(('oreb', 'dreb', 'reb'))
    ambiguities.append(('oreb', 'reb', 'reb'))
    ambiguities.append(('player_name', 'first_name', 'name'))
    ambiguities.append(('player_name', 'second_name', 'name'))
    ambiguities.append(('reb', 'dreb', 'reb'))
    ambiguities.append(('reb', 'oreb', 'reb'))
    ambiguities.append(('team_city', 'town', 'city'))
    ambiguities.append(('to', 'town', 'city'))
    ambiguities.append(('town', 'team_city', 'city'))
    ## Profiling information
    tableName = 'basket_acronyms'
    attributes = [('index', CATEGORICAL),
                  ('player_name', CATEGORICAL),
                  ('first_name', CATEGORICAL),
                  ('min', NUMERICAL),
                  ('fgm', NUMERICAL),
                  ('reb', NUMERICAL),
                  ('fg3a', NUMERICAL),
                  ('ast', NUMERICAL),
                  ('fg3m', NUMERICAL),
                  ('oreb', NUMERICAL),
                  ('to', NUMERICAL),
                  ('start_position', CATEGORICAL),
                  ('pf', NUMERICAL),
                  ('pts', NUMERICAL),
                  ('fga', NUMERICAL),
                  ('stl', NUMERICAL),
                  ('fta', NUMERICAL),
                  ('blk', NUMERICAL),
                  ('dreb', NUMERICAL),
                  ('ftm', NUMERICAL),
                  ('ft_pct', NUMERICAL),
                  ('fg_pct', NUMERICAL),
                  ('fg3_pct', NUMERICAL),
                  ('second_name', CATEGORICAL),
                  ('team_city', CATEGORICAL),
                  ('team', CATEGORICAL),
                  ('town', CATEGORICAL)]
    pk = 'player_name'
    ## FDS TEAM -> TOWN; TEAM -> TEAM_CITY
    fds = [('team', 'town', ['in', 'has', 'players']), ('team', 'team_city', ['in', 'has', 'players'])]
    # CKS: ID = PLAYER_NAME + TEAM
    compositeKeys = [('player_name', 'team')]
    ambiguitiesWithType = _addType(ambiguities, attributes)
    return (ambiguitiesWithType, pk, fds, compositeKeys, tableName, attributes)

def _soccerProfiling():
    ## Pythia predictions
    ambiguities = []
    ambiguities.append(('tournamentid', 'tournamentregionid', 'tournament'))
    ambiguities.append(('seasonname', 'tournamentname', 'name'))
    ambiguities.append(('yellowcard', 'redcard', 'card'))
    ambiguities.append(('lastname', 'teamname', 'name'))
    ambiguities.append(('tournamentregionname', 'regioncode', 'region'))
    ambiguities.append(('tournamentid', 'tournamentshortname', 'tournament'))
    ambiguities.append(('tournamentname', 'tournamentshortname', 'tournament'))
    ambiguities.append(('playedpositions', 'playedpositionsshort', 'playedposition'))
    ambiguities.append(('seasonname', 'tournamentregionname', 'name'))
    ambiguities.append(('tournamentid', 'tournamentregionname', 'tournament'))
    ambiguities.append(('tournamentregioncode', 'tournamentshortname', 'tournament'))
    ambiguities.append(('tournamentregionname', 'tournamentshortname', 'tournament'))
    ambiguities.append(('seasonid', 'seasonname', 'season'))
    ambiguities.append(('tournamentregioncode', 'teamregionname', 'region'))
    ambiguities.append(('teamname', 'teamregionname', 'team'))
    ambiguities.append(('age', 'weight', 'measurement'))
    ambiguities.append(('teamid', 'teamname', 'team'))
    ambiguities.append(('tournamentregionid', 'tournamentregionname', 'tournament'))
    ambiguities.append(('shotspergame', 'aerialwonpergame', 'game'))
    ambiguities.append(('ranking', 'positiontext', 'position'))
    ambiguities.append(('tournamentregionid', 'regioncode', 'region'))
    ambiguities.append(('tournamentname', 'teamname', 'name'))
    ambiguities.append(('teamid', 'teamregionname', 'team'))
    ambiguities.append(('tournamentregionname', 'teamregionname', 'region'))
    ambiguities.append(('firstname', 'lastname', 'middle name'))
    ambiguities.append(('tournamentregionid', 'tournamentshortname', 'tournament'))
    ambiguities.append(('ranking', 'weight', 'measure'))
    ambiguities.append(('weight', 'rating', 'measurement'))
    ambiguities.append(('ranking', 'rating', 'rate'))
    ambiguities.append(('tournamentregionid', 'teamregionname', 'region'))
    ambiguities.append(('tournamentregionname', 'tournamentname', 'tournament'))
    ambiguities.append(('height', 'weight', 'measurement'))
    ambiguities.append(('tournamentregionid', 'tournamentregioncode', 'tournament'))
    ambiguities.append(('lastname', 'name', 'family name'))
    ambiguities.append(('tournamentid', 'tournamentname', 'tournament'))
    ambiguities.append(('index', 'weight', 'measure'))
    ambiguities.append(('tournamentregioncode', 'tournamentregionname', 'tournament'))
    ambiguities.append(('firstname', 'teamname', 'name'))
    ambiguities.append(('tournamentid', 'tournamentregioncode', 'tournament'))
    ambiguities.append(('tournamentregionid', 'tournamentname', 'tournament'))
    ambiguities.append(('regioncode', 'teamregionname', 'region'))
    ambiguities.append(('playedpositions', 'positiontext', 'position'))
    ambiguities.append(('manofthematch', 'ismanofthematch', 'match'))
    ambiguities.append(('index', 'rating', 'rate'))
    ambiguities.append(('seasonname', 'lastname', 'name'))
    ambiguities.append(('firstname', 'name', 'forename'))
    ambiguities.append(('positiontext', 'playedpositionsshort', 'position'))
    ambiguities.append(('age', 'rating', 'measurement'))
    ambiguities.append(('index', 'age', 'measurement'))
    ambiguities.append(('age', 'height', 'measurement'))
    ambiguities.append(('seasonname', 'teamname', 'name'))
    ambiguities.append(('playerid', 'teamid', 'id'))
    ambiguities.append(('tournamentregioncode', 'tournamentname', 'tournament'))
    ambiguities.append(('index', 'ranking', 'rate'))
    ## Profiling information
    tableName = 'soccer'
    attributes = [('index', CATEGORICAL),
                  ('ranking', CATEGORICAL),
                  ('seasonid', CATEGORICAL),
                  ('seasonname', CATEGORICAL),
                  ('tournamentid', CATEGORICAL),
                  ('tournamentregionid', CATEGORICAL),
                  ('tournamentregioncode', CATEGORICAL),
                  ('tournamentregionname', CATEGORICAL),
                  ('regioncode', CATEGORICAL),
                  ('tournamentname', CATEGORICAL),
                  ('tournamentshortname', CATEGORICAL),
                  ('firstname', CATEGORICAL),
                  ('lastname', CATEGORICAL),
                  ('playerid', CATEGORICAL),
                  ('isactive', NUMERICAL),
                  ('isopta', NUMERICAL),
                  ('teamid', CATEGORICAL),
                  ('teamname', CATEGORICAL),
                  ('teamregionname', CATEGORICAL),
                  ('playedpositions', CATEGORICAL),
                  ('age', NUMERICAL),
                  ('height', NUMERICAL),
                  ('weight', NUMERICAL),
                  ('positiontext', CATEGORICAL),
                  ('apps', NUMERICAL),
                  ('subon', NUMERICAL),
                  ('minsplayed', NUMERICAL),
                  ('rating', NUMERICAL),
                  ('goal', NUMERICAL),
                  ('assisttotal', NUMERICAL),
                  ('yellowcard', NUMERICAL),
                  ('redcard', NUMERICAL),
                  ('shotspergame', NUMERICAL),
                  ('aerialwonpergame', NUMERICAL),
                  ('manofthematch', NUMERICAL),
                  ('ismanofthematch', NUMERICAL),
                  ('playedpositionsshort', CATEGORICAL),
                  ('name', CATEGORICAL),
                  ('passsuccess', NUMERICAL)]
    pk = 'name'
    #FDs: teamname --> tournamentregionname
    fds = [('teamname', 'tournamentregionname', ['in', 'has', 'players'])]
    compositeKeys = []
    ambiguitiesWithType = _addType(ambiguities, attributes)
    return (ambiguitiesWithType, pk, fds, compositeKeys, tableName, attributes)

def _abaloneProfiling(tableName):
    ambiguities = []
    ambiguities.append(('diameter', 'height', 'dimension'))
    ambiguities.append(('height', 'diameter', 'dimension'))
    ambiguities.append(('diameter', 'length', 'dimension'))
    ambiguities.append(('length', 'diameter', 'dimension'))
    ambiguities.append(('height', 'length', 'distance'))
    ambiguities.append(('length', 'height', 'distance'))
    ambiguities.append(('weight.shell', 'weight.shucked', 'weight'))
    ambiguities.append(('weight.shucked', 'weight.shell', 'weight'))
    ambiguities.append(('weight.shell', 'weight.viscera', 'weight'))
    ambiguities.append(('weight.viscera', 'weight.shell', 'weight'))
    ambiguities.append(('weight.shell', 'weight.whole', 'weight'))
    ambiguities.append(('weight.whole', 'weight.shell', 'weight'))
    ambiguities.append(('weight.shucked', 'weight.viscera', 'weight'))
    ambiguities.append(('weight.viscera', 'weight.shucked', 'weight'))
    ambiguities.append(('weight.shucked', 'weight.whole', 'weight'))
    ambiguities.append(('weight.whole', 'weight.shucked', 'weight'))
    ambiguities.append(('weight.viscera', 'weight.whole', 'weight'))
    ambiguities.append(('weight.whole', 'weight.viscera', 'weight'))
    #tableName = 'abalone_short'  # the name of the table in the DB
    tb = tableName
    attributes = [('sex', CATEGORICAL),
                  ('length', NUMERICAL),
                  ('diameter', NUMERICAL),
                  ('height', NUMERICAL),
                  ('weight.whole', NUMERICAL),
                  ('weight.shucked', NUMERICAL),
                  ('weight.viscera', NUMERICAL),
                  ('weight.shell', NUMERICAL),
                  ('rings', NUMERICAL)]
    pk = 'index'  # a unique id
    fds = []
    compositeKeys = []
    ambiguitiesWithType = _addType(ambiguities, attributes)
    return (ambiguitiesWithType, pk, fds, compositeKeys, tb, attributes)

def _adultsProfiling(tableName):
    ambiguities = []
    ambiguities.append(('occupation', 'workclass', 'work'))
    ambiguities.append(('workclass', 'occupation', 'work'))
    ambiguities.append(('capital-gain', 'capital-loss', 'capital'))
    ambiguities.append(('capital-loss', 'capital-gain', 'capital'))
    ambiguities.append(('workclass', 'education', 'class'))
    ambiguities.append(('education', 'education-num', 'education'))
    ambiguities.append(('education', 'occupation', 'work'))
    ambiguities.append(('occupation', 'salary', 'work'))
    ambiguities.append(('relationship', 'sex', 'sexual activity'))
    ambiguities.append(('race', 'sex', 'human sexuality'))

    #tableName = 'adults_short'  # the name of the table in the DB
    tb = tableName
    attributes = [('age', NUMERICAL),
                  ('workclass', CATEGORICAL),
                  ('fnlwgt', CATEGORICAL),
                  ('education', CATEGORICAL),
                  ('education-num', CATEGORICAL),
                  ('marital-status', CATEGORICAL),
                  ('occupation', CATEGORICAL),
                  ('relationship', CATEGORICAL),
                  ('race', CATEGORICAL),
                  ('sex', CATEGORICAL),
                  ('capital-gain', NUMERICAL),
                  ('capital-loss', NUMERICAL),
                  ('hours-per-week', NUMERICAL),
                  ('native-country', CATEGORICAL),
                  ('salary', NUMERICAL)]
    pk = 'fnlwgt'  # a unique id
    # FDs in the form of tuple (LHS, RHS) #we are using only one attribute per LHS and RHS
    fds = [('education', 'education-num', ['Person with ', 'has', 'number'])]
    compositeKeys = []
    ambiguitiesWithType = _addType(ambiguities, attributes)
    return (ambiguitiesWithType, pk, fds, compositeKeys, tb, attributes)

def _mushroomProfiling(tableName):
    #######################################
    ambiguities = []
    # ambiguities.append(('attribute1', 'attribute2', 'label'))
    ambiguities.append(('veil-color', 'veil-type', 'veil'))
    ambiguities.append(('veil-type', 'veil-color', 'veil'))
    ambiguities.append(('ring-number', 'ring-type', 'ring'))
    ambiguities.append(('ring-type', 'ring-number', 'ring'))
    ambiguities.append(('ring-number', 'stalk-color-above-ring', 'ring'))
    ambiguities.append(('ring-number', 'stalk-color-below-ring', 'ring'))
    ambiguities.append(('ring-type', 'stalk-color-above-ring', 'ring'))
    ambiguities.append(('stalk-color-above-ring', 'ring-type', 'ring'))
    ambiguities.append(('ring-type', 'stalk-color-below-ring', 'ring'))
    ambiguities.append(('stalk-surface-above-ring', 'ring-type', 'ring'))
    ambiguities.append(('stalk-color-above-ring', 'stalk-color-below-ring', 'stalk'))
    ambiguities.append(('stalk-color-below-ring', 'stalk-color-above-ring', 'stalk'))
    ambiguities.append(('stalk-color-above-ring', 'stalk-surface-above-ring', 'stalk'))
    ambiguities.append(('stalk-surface-above-ring', 'stalk-color-above-ring', 'stalk'))
    ambiguities.append(('stalk-color-above-ring', 'stalk-surface-below-ring', 'stalk'))
    ambiguities.append(('stalk-surface-below-ring', 'stalk-color-above-ring', 'stalk'))
    ambiguities.append(('stalk-color-below-ring', 'stalk-surface-above-ring', 'stalk'))
    ambiguities.append(('stalk-surface-above-ring', 'stalk-color-below-ring', 'stalk'))
    ambiguities.append(('stalk-color-below-ring', 'stalk-surface-below-ring', 'stalk'))
    ambiguities.append(('stalk-surface-below-ring', 'stalk-color-below-ring', 'stalk'))
    ambiguities.append(('stalk-surface-above-ring', 'stalk-surface-below-ring', 'stalk'))
    ambiguities.append(('stalk-surface-below-ring', 'stalk-surface-above-ring', 'stalk'))
    ambiguities.append(('stalk-root', 'stalk-shape', 'stalk'))
    ambiguities.append(('stalk-shape', 'stalk-root', 'stalk'))
    ambiguities.append(('gill-attachment', 'gill-color', 'gill'))
    ambiguities.append(('gill-color', 'gill-attachment', 'gill'))
    ambiguities.append(('gill-attachment', 'gill-size', 'gill'))
    ambiguities.append(('gill-size', 'gill-attachment', 'gill'))
    ambiguities.append(('gill-attachment', 'gill-spacing', 'gill'))
    ambiguities.append(('gill-spacing', 'gill-attachment', 'gill'))
    ambiguities.append(('gill-color', 'gill-size', 'gill'))
    ambiguities.append(('gill-size', 'gill-color', 'gill'))
    ambiguities.append(('gill-color', 'gill-spacing', 'gill'))
    ambiguities.append(('gill-spacing', 'gill-color', 'gill'))
    ambiguities.append(('gill-size', 'gill-spacing', 'gill'))
    ambiguities.append(('gill-spacing', 'gill-size', 'gill'))
    ambiguities.append(('cap-color', 'cap-shape', 'cap'))
    ambiguities.append(('cap-shape', 'cap-color', 'cap'))
    ambiguities.append(('cap-color', 'cap-surface', 'cap'))
    ambiguities.append(('cap-surface', 'cap-color', 'cap'))
    ambiguities.append(('cap-shape', 'cap-surface', 'cap'))
    ambiguities.append(('cap-surface', 'cap-shape', 'cap'))
    ambiguities.append(('cap-color', 'gill-color', 'color'))
    ambiguities.append(('gill-color', 'cap-color', 'color'))
    ambiguities.append(('cap-color', 'spore-print-color', 'color'))
    ambiguities.append(('spore-print-color', 'cap-color', 'color'))
    ambiguities.append(('cap-color', 'veil-color', 'color'))
    ambiguities.append(('veil-color', 'cap-color', 'color'))
    ambiguities.append(('gill-color', 'spore-print-color', 'color'))
    ambiguities.append(('spore-print-color', 'gill-color', 'color'))
    ambiguities.append(('gill-color', 'veil-color', 'color'))
    ambiguities.append(('veil-color', 'gill-color', 'color'))
    ambiguities.append(('spore-print-color', 'veil-color', 'color'))
    ambiguities.append(('veil-color', 'spore-print-color', 'color'))
    ambiguities.append(('class', 'population', 'group'))
    ambiguities.append(('cap-shape', 'stalk-shape', 'shape'))
    ambiguities.append(('cap-surface', 'stalk-surface-above-ring', 'surface'))
    ambiguities.append(('cap-surface', 'stalk-surface-below-ring', 'surface'))
    ambiguities.append(('cap-color', 'stalk-color-above-ring', 'color'))
    ambiguities.append(('cap-color', 'stalk-color-below-ring', 'color'))
    ambiguities.append(('gill-color', 'stalk-color-above-ring', 'color'))
    ambiguities.append(('gill-color', 'stalk-color-below-ring', 'color'))
    ambiguities.append(('stalk-shape', 'stalk-surface-above-ring', 'stalk'))
    ambiguities.append(('stalk-shape', 'stalk-surface-below-ring', 'stalk'))
    ambiguities.append(('stalk-shape', 'stalk-color-above-ring', 'stalk'))
    ambiguities.append(('stalk-shape', 'stalk-color-below-ring', 'stalk'))
    ambiguities.append(('stalk-root', 'stalk-surface-above-ring', 'stalk'))
    ambiguities.append(('stalk-root', 'stalk-surface-below-ring', 'stalk'))
    ambiguities.append(('stalk-root', 'stalk-color-above-ring', 'stalk'))
    ambiguities.append(('stalk-root', 'stalk-color-below-ring', 'stalk'))
    ambiguities.append(('stalk-color-above-ring', 'veil-color', 'color'))
    ambiguities.append(('stalk-color-above-ring', 'spore-print-color', 'color'))
    ambiguities.append(('stalk-color-below-ring', 'veil-color', 'color'))
    ambiguities.append(('stalk-color-below-ring', 'spore-print-color', 'color'))
    ambiguities.append(('veil-type', 'ring-type', 'type'))
    #tableName = 'mushroom_short'  # the name of the table in the DB
    tb = tableName
    attributes = [('class', CATEGORICAL),
                  ('cap-shape', CATEGORICAL),
                  ('cap-surface', CATEGORICAL),
                  ('cap-color', CATEGORICAL),
                  ('bruises?', CATEGORICAL),
                  ('odor', CATEGORICAL),
                  ('gill-attachment', CATEGORICAL),
                  ('gill-spacing', CATEGORICAL),
                  ('gill-size', CATEGORICAL),
                  ('gill-color', CATEGORICAL),
                  ('stalk-shape', CATEGORICAL),
                  ('stalk-root', CATEGORICAL),
                  ('stalk-surface-above-ring', CATEGORICAL),
                  ('stalk-surface-below-ring', CATEGORICAL),
                  ('stalk-color-above-ring', CATEGORICAL),
                  ('stalk-color-below-ring', CATEGORICAL),
                  ('veil-type', CATEGORICAL),
                  ('veil-color', CATEGORICAL),
                  ('ring-number', CATEGORICAL),
                  ('ring-type', CATEGORICAL),
                  ('spore-print-color', CATEGORICAL),
                  ('population', CATEGORICAL),
                  ('habitat', CATEGORICAL)]
    pk = 'index'  # a unique id
    fds = []
    compositeKeys = []
    ambiguitiesWithType = _addType(ambiguities, attributes)
    return (ambiguitiesWithType, pk, fds, compositeKeys, tb, attributes)


def loadTable(dataset):
    if dataset == 'iris' or dataset == 'iris_short':
        return _irisProfiling(dataset)
    if dataset == 'basket_full':
        return _basketFullProfiling()
    if dataset == 'basket_acronyms':
        return _basketAcronymsProfiling()
    if dataset == 'soccer':
        return _soccerProfiling()
    if dataset == 'abalone_short' or dataset == 'abalone':
        return _abaloneProfiling(dataset)
    if dataset == 'adult_short' or dataset == 'adult':
        return _adultsProfiling(dataset)
    if dataset == 'mushroom_short' or dataset == 'mushroom':
        return _mushroomProfiling(dataset)
    print("*** ERROR: no dataset found: ", dataset)
    return None

