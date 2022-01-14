import subprocess
import sys

import torch
from tqdm import tqdm
import random
import csv
from transformers import Trainer, TrainingArguments, RobertaForSequenceClassification, RobertaTokenizer
from sklearn.metrics import accuracy_score, precision_recall_fscore_support, classification_report
import os

def install(package):
    print("Install: ", package)
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])

def readTSVFile(file):
  texts = []
  labels = []
  labelToUse = 0 ## CONTRADICTING / NEI
  if 'uniform_true' in file:
    labelToUse = 1 ## SUPPORTS
  if 'uniform_false' in file:
    labelToUse = 2 ## REFUTES
  with open(file) as fd:
    rd = csv.reader(fd, delimiter="\t", quotechar='"')
    for row in rd:
      texts.append(row[0])
      labels.append(labelToUse)
  return texts, labels

class FEVEROUSDataset(torch.utils.data.Dataset):
    def __init__(self, encodings, labels, use_labels = True):
        self.encodings = encodings
        self.labels = labels
        self.use_labels = use_labels

    def __getitem__(self, idx):
        item = {key: torch.tensor(val[idx]) for key, val in self.encodings.items()}
        if self.use_labels:
            item['labels'] = torch.tensor(self.labels[idx])
        return item

    def __len__(self):
        return len(self.labels)

def compute_metrics(pred):
    labels = pred.label_ids
    preds = pred.predictions.argmax(-1)
    precision, recall, f1, _ = precision_recall_fscore_support(labels, preds, average='micro')
    acc = accuracy_score(labels, preds)
    class_rep = classification_report(labels, preds, target_names= ['NOT ENOUGH INFO', 'SUPPORTS', 'REFUTES'], output_dict=True)
    print(class_rep)
    print("Acc: {}, Recall: {}, Precision: {}, F1: {}".format(acc, recall, precision, f1))
    return {
        'accuracy': acc,
        'f1': f1,
        'precision': precision,
        'recall': recall,
        'class_rep': class_rep
    }

def model_trainer_new(model_path, train_dataset, test_dataset):
    model = RobertaForSequenceClassification.from_pretrained('ynie/roberta-large-snli_mnli_fever_anli_R1_R2_R3-nli', num_labels =3, return_dict=True)#ynie/roberta-large-snli_mnli_fever_anli_R1_R2_R3-nli

    training_args = TrainingArguments(
    output_dir=model_path,          # output directory
    num_train_epochs=1,              # total # of training epochs
    per_device_train_batch_size=16,  # batch size per device during training
    per_device_eval_batch_size=16,   # batch size for evaluation
    # gradient_accumulation_steps=3,
    warmup_steps=0,                # number of warmup steps for learning rate scheduler
    weight_decay=0.01,               # strength of weight decay
    logging_dir= os.path.join(model_path, 'logs'),            # directory for storing logs
    logging_steps=1200,
    save_steps = 5900, #1200,
    learning_rate = 1e-05
    # save_strategy='epoch'
    )

    if test_dataset != None:
        trainer = Trainer(
        model=model,                         # the instantiated 🤗 Transformers model to be trained
        args=training_args,                  # training arguments, defined above
        train_dataset=train_dataset,         # training dataset
        eval_dataset=test_dataset,          # evaluation dataset
        compute_metrics = compute_metrics,
        )
    else:
        trainer = Trainer(
        model=model,                         # the instantiated 🤗 Transformers model to be trained
        args=training_args,                  # training arguments, defined above
        train_dataset=train_dataset,         # training dataset
        compute_metrics = compute_metrics,
        )
    return trainer, model

if __name__ == '__main__':
    #install('transformers==4.1.1')
    #install('torch')
    #install('tqdm')
    BASE_PATH = "/Users/enzoveltri/git/pythia/data/generated/"
    MODEL_PATH = './models/feverous_verdict_predictor_pythia'
    datasetTrain = ['iris', 'basket_full_names', 'soccer']
    datasetTest = ['abalone', 'adult', 'basket_acronyms', 'mushroom']
    text_train = []
    labels_train = []
    text_test = []
    labels_test = []
    for trainCSV in datasetTrain:
        fileNameUniformTrue = BASE_PATH + trainCSV + "_uniform_true" + "_train.tsv"
        fileNameUniformFalse = BASE_PATH + trainCSV + "_uniform_false" + "_train.tsv"
        fileNameContradicting = BASE_PATH + trainCSV + "_contradicting" + "_train.tsv"
        textsTrue, labelsTrue = readTSVFile(fileNameUniformTrue)
        textsFalse, labelsFalse = readTSVFile(fileNameUniformFalse)
        textsContradicting, labelsContradicting = readTSVFile(fileNameContradicting)
        text_train.extend(textsTrue)
        text_train.extend(textsFalse)
        text_train.extend(textsContradicting)
        labels_train.extend(labelsTrue)
        labels_train.extend(labelsFalse)
        labels_train.extend(labelsContradicting)

    for test in datasetTest:
        fileNameUniformTrue = BASE_PATH + test + "_uniform_true" + "_test.tsv"
        fileNameUniformFalse = BASE_PATH + test + "_uniform_false" + "_test.tsv"
        fileNameContradicting = BASE_PATH + test + "_contradicting" + "_test.tsv"
        textsTrue, labelsTrue = readTSVFile(fileNameUniformTrue)
        textsFalse, labelsFalse = readTSVFile(fileNameUniformFalse)
        textsContradicting, labelsContradicting = readTSVFile(fileNameContradicting)
        text_test.extend(textsTrue)
        text_test.extend(textsFalse)
        text_test.extend(textsContradicting)
        labels_test.extend(labelsTrue)
        labels_test.extend(labelsFalse)
        labels_test.extend(labelsContradicting)

    train_data = list(zip(text_train, labels_train))
    random.shuffle(train_data)
    text_train, labels_train = zip(*train_data)

    print("TRAIN:", len(text_train))
    print("TEST:", len(text_test))

    tokenizer = RobertaTokenizer.from_pretrained('ynie/roberta-large-snli_mnli_fever_anli_R1_R2_R3-nli')
    text_train = tokenizer(text_train, padding=True, truncation=True)
    train_dataset = FEVEROUSDataset(text_train, labels_train)
    text_test = tokenizer(text_test, padding=True, truncation=True)
    test_dataset = FEVEROUSDataset(text_test, labels_test)

    trainer, model = model_trainer_new(MODEL_PATH, train_dataset, test_dataset)
    trainer.train()
    scores = trainer.evaluate()
    print(scores)