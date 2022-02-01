# pythia

Applications such as computational fact checking and data-to-text generation exploit the relationship between relational data and natural language text. Despite promising results in these areas, state of the art solutions simply fail in managing "data-ambiguity", i.e., the case when there are multiple interpretations of the relationship between text and data. 
The problem stems from the existing training corpora, which focus on examples with precise and complete sentences, thus leaving the target applications unprepared on how to handle ambiguous cases. To tackle this problem, we present Pythia, a system that, given a table D, generates sentences that contain factual ambiguities w.r.t. the data in D. We first formulate the novel problem of generating sentences with data ambiguities and propose SQL queries to achieve such task. We then introduce deep learning methods over the relational data to automatically identify ambiguities, so that any table can be used as input. To show the positive impact of corpora of ambiguous sentences, we extend state-of-the-art target NLP applications for data-to-text generation and fact checking. A large set of experiments with real tables shows that extending the training data with Pythia's sentences enables the applications to drastically improve their qualitative performance.


# Repository
- Data folder contains all the datasets (compressed) used in the paper.
- Notebooks folder contains the following notebooks:
	1) The notebooks to generate the training data for schema-task (or task1) and data-task (or task3). The notebooks are Task1(3)-Dataset Generation WDC
	2) The notebook to measure the performances of the baselines wrt the test set for task 1. SIGMOD Task1-Baselines
	3) The notebooks to measure the performance of our proposed model using T5 for task 1 and task 3. SIGMOD-T5-LabelAmbiguity-Task1(3)-colab
	4) The notebook to compare Totto and AmbTotto. SIGMOD-AmbTotto-colab.
	Please note that for all colab notebooks is required a google cloud storage bucket to store the model and the data.
- src/pythia contains the code that runs Algorithm 1 in the paper (Pythia.py) and the code to generate the training data for AmbTotto (AmbTotto.py). Scripts to fine-tune and test a T5-based annotator are available in ```src/pythia/T5Annotator/```. Please note that some extensions are been used in the Pythia algorithm to support the creation of the AmbTotto training data


# Demo
Coming Soon

[![Coming Soon](https://img.youtube.com/vi/gLqu_Mvtj9w/maxresdefault.jpg)](https://youtu.be/gLqu_Mvtj9w)

