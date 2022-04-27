# Pythia

Several applications, such as text-to-SQL and computational fact checking, exploit the relationship between relational data and natural language text. However, state of the art solutions simply fail in managing "data-ambiguity", i.e., the case when there are multiple interpretations of the relationship between text and data. 
Given the ambiguity in language, text can be mapped to different subsets of data, but existing training corpora only have examples in which every sentence/question is annotated precisely w.r.t. the relation.  This unrealistic assumption leaves the target applications unable to handle ambiguous cases. 

To tackle this problem, we present an end-to-end solution that, given a table D, generates examples that consist of text, annotated with its data evidence, with factual ambiguities w.r.t. the data in D. We formulate the problem of profiling relational tables to identify row and attribute data ambiguity. For the latter, we propose a deep learning method that identifies every pair of data ambiguous attributes and a label that describes both columns. Such metadata is then used to generate examples with data ambiguities for any input table.
To enable scalability, we finally introduce a SQL approach that allows the generation of millions of examples in seconds.
We show the high accuracy of our solution in profiling relational tables and report on how our automatically generated examples lead to drastic quality improvements in two fact-checking applications, including a website with thousands of users, and in a text-to-SQL system.


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
Front-end available at https://github.com/antoniovizzuso/pythia-frontend
To execute the backend make sure you have a Postresql instance installed.
0) Install the requirements.txt
1) Create a postgresql database (e.g. pythia)
2) Run src/StartConfiguration.py and follow the instruction. The script will create an admin user. Feel free to change the admin password in the script.
3) Start the Rest server in src/rest/ServerRest.py
4) Start the front-end following the instruction at https://github.com/antoniovizzuso/pythia-frontend

[![Coming Soon](https://img.youtube.com/vi/gLqu_Mvtj9w/maxresdefault.jpg)](https://youtu.be/gLqu_Mvtj9w)

