from src.datasets.Adult import Adult
from src.datasets.BasketComplete import BasketComplete
from src.datasets.Covid import Covid
from src.datasets.SoccerPlayers import SoccerPlayers
from src.datasets.Abalone import Abalone
from src.datasets.Iris import Iris
from src.datasets.Mushroom import Mushroom

import subprocess
import sys

def install(package):
    print("Install: ", package)
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])

if __name__ == '__main__':
    #install('uci-dataset')
    #datasetLoader = BasketComplete(fullNames=False) ## with acronyms
    #datasetLoader = BasketComplete(fullNames=True)  ## with full names
    #datasetLoader = Covid()
    #datasetLoader = SoccerPlayers()
    #datasetLoader = Abalone()
    #datasetLoader = Adult(False)
    #datasetLoader = Iris()
    datasetLoader = Mushroom()
    datasetLoader.retrieve()
    datasetLoader.toSQL()
    print("*** IMPORT COMPLETE")
