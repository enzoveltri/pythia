# Pythia

Several applications, such as text-to-SQL and computational fact checking, exploit the relationship between relational data and natural language text. However, state of the art solutions simply fail in managing "data-ambiguity", i.e., the case when there are multiple interpretations of the relationship between text and data. 
Given the ambiguity in language, text can be mapped to different subsets of data, but existing training corpora only have examples in which every sentence/question is annotated precisely w.r.t. the relation.  This unrealistic assumption leaves the target applications unable to handle ambiguous cases. 

To tackle this problem, we present an end-to-end solution that, given a table D, generates examples that consist of text, annotated with its data evidence, with factual ambiguities w.r.t. the data in D. We formulate the problem of profiling relational tables to identify row and attribute data ambiguity. For the latter, we propose a deep learning method that identifies every pair of data ambiguous attributes and a label that describes both columns. Such metadata is then used to generate examples with data ambiguities for any input table.
To enable scalability, we finally introduce a SQL approach that allows the generation of millions of examples in seconds.
We show the high accuracy of our solution in profiling relational tables and report on how our automatically generated examples lead to drastic quality improvements in two fact-checking applications, including a website with thousands of users, and in a text-to-SQL system.

# News
- 2022-06-16 [BEST DEMO PAPER - SIGMOD 2022](https://dl.acm.org/doi/10.1145/3514221.3520164)

# Repository
- Data used in the paper:
	1) Annotations: contains the singolar individual annotation from our user study and the evaluation metrics. Such annotation are the used to generate the test data for the Ambiguity discovery module. 
	2) Ambiguity discovery. Ambiguity dataset (train and test) for schema and data available at:
		- Schema: https://drive.google.com/drive/folders/1VLDywO4zD1fbzFBiEbB-m-BHOv0_BBIB?usp=sharing. The dataset is generated with the ```notebooks/Schema-Dataset Generation WDC```
		- Data: https://drive.google.com/drive/folders/1C2qw52QKl--9PiyDoN1FXsQjwzq8prqQ?usp=sharing. The dataset is generated with the ```notebooks/Data-Dataset Generation WDC```
- Notebooks folder contains the following notebooks:
	1) Schema ambiguity model can be trained using the following notebook changing the DATA_DIR path: ```notebooks/T5-LabelAmbiguity-Schema-colab```
	2) Data ambiguity model can be trained using the following notebook changing the DATA_DIR path: ```notebooks/T5-LabelAmbiguity-Schema-colab```
	3) Pythia_FEVEROUS experiments. The notebook also contains the links to the data used in it. The data should be available on your google drive.
	4) Pythia_NL2SQL experiments. The notebook also contains the links to the data used in it. The data should be available on your google drive.

Please note that for all colab notebooks is required a google cloud storage bucket to store the model and the data. 

Trained models can be found here:

- Schema ambiguity model: https://drive.google.com/drive/folders/190eDcbIZ0r3cWvioBieeQAtXOBoy6X5Y?usp=sharing. Put the downloaded file in a AMB_MODEL_DIR folder
- Sentence generator model: https://drive.google.com/drive/folders/1V8hWGW6U69BFt6NgWb_ZvnVouUWQstzE?usp=sharing. Put the downloaded file in a SENTENCE_GEN_DIR folder

# Pythia API
- src/pythia contains the API. The main algorithm is implemented in PythiaT5.py. Change the MODEL_POSITION variable in T5Engine class to your AMB_MODEL_DIR. Change the MODEL_POSITION variavle in T5SentenceGenerator to your SENTENCE_GEN_DIR. The template based algorithm is implemented in Pythia.py. PythiaT5 will be integrated with the front-end in the next version.
- Scripts to fine-tune and test a T5-based annotator are available in ```src/pythia/T5Annotator/```.


# Demo
Front-end available at https://github.com/antoniovizzuso/pythia-frontend
To run the backend make sure you have a Postresql instance installed.
1) Install the requirements.txt
2) Create a postgresql database (e.g. pythia)
3) Run ```src/StartConfiguration.py``` and follow the instruction. The script will create an admin user. Feel free to change the admin password in the script.
4) Start the Rest server in ```src/rest/ServerRest.py```
5) Start the front-end following the instruction at https://github.com/antoniovizzuso/pythia-frontend

[![Coming Soon](https://img.youtube.com/vi/gLqu_Mvtj9w/maxresdefault.jpg)](https://youtu.be/gLqu_Mvtj9w)


# References
[[1] Unsupervised Generation of Ambiguous Textual Claims from Relational Data; Veltri Enzo, Santoro Donatello, Badaro Gilbert, Saeed Mohammed and Papotti Paolo. SIGMOD, 2022. Demo Paper](https://dl.acm.org/doi/10.1145/3514221.3520164)
