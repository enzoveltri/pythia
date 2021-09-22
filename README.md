# pythia

Natural language is oftentimes ambiguous, but most datasets focus on precise and complete sentences, thus leaving the target applications, such as computational fact checking, unprepared on how to handle vague sentences or questions. Differently from existing datasets, we focus on the generation of factual sentences that contain ambiguities with respect to the given data, such as "Carter has higher shooting percentage than Smith" with a dataset that contains both regular and 3-point Field Goal percentage, or "Carter scored 0 PT" in a dataset with players named "Carter" in two teams.
We present Pythia, a system that, given a relational table, returns the queries that lead to such ambiguous factual sentences. We first formulate the novel problem of generating sentences with data ambiguities and propose SQL queries to achieve such task. We then introduce deep learning methods over the relational data to automatically identify possible ambiguities, so that any dataset can be used as input. To show the impact of a corpus of ambiguous sentences from Pythia, we extend state-of-the-art baselines in two target NLP applications: table-to-text generation and fact checking. While existing methods fail on sentences that contain ambiguity, extending their training with sentences from Pythia enables them to radically improve performance.


# Repository
- Data folder contains all the datasets (compressed) used in the paper.
- Notebooks folder contains the following notebooks:
	1) The notebooks to generate the training data for schema-task (or task1) and data-task (or task3). The notebooks are Task1(3)-Dataset Generation WDC
	2) The notebook to measure the performances of the baselines wrt the test set for task 1. SIGMOD Task1-Baselines
	3) The notebooks to measure the performance of our proposed model using T5 for task 1 and task 3. SIGMOD-T5-LabelAmbiguity-Task1(3)-colab
	4) The notebook to compare Totto and AmbTotto. SIGMOD-AmbTotto-colab.
	Please note that for all colab notebooks is required a google cloud storage bucket to store the model and the data.
- src/pythia contains the code that runs Algorithm 1 in the paper (Pythia.py) and the code to generate the training data for AmbTotto (AmbTotto.py). Please note that some extensions are been used in the Pythia algorithm to support the creation of the AmbTotto training data
