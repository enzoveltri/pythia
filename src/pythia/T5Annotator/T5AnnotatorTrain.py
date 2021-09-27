# TODO order
from random import sample
from transformers import T5ForConditionalGeneration, T5Tokenizer
import torch
from torch.optim import AdamW
from torch.utils.data import TensorDataset
from torch.utils.data import DataLoader, RandomSampler
from tqdm import tqdm
import time
import json
import datetime
import argparse


# Create the parser
my_parser = argparse.ArgumentParser()

# Add the arguments
my_parser.add_argument('--train_file',
                       metavar='fpath',
                       type=str,
                       help='the path to the train file',
                       required=True)
my_parser.add_argument('--model_arch',
                       metavar='fpath',
                       type=str,
                       default='t5-base',
                       help='model architecture')

my_parser.add_argument('--output_path',
                       metavar='op',
                       type=str,
                       default='t5_output',
                       help='output path')

my_parser.add_argument('--max_length',
                       metavar='ml',
                       type=int,
                       default=100,
                       help='the max length for tokenization')

my_parser.add_argument('--epochs',
                       metavar='epochs',
                       type=int,
                       default=10,
                       help='number of epochs')
my_parser.add_argument('--batch_size',
                       metavar='bs',
                       type=int,
                       default=16,
                       help='Training Batch Size')
my_parser.add_argument('--learning_rate',
                       metavar='lr',
                       type=float,
                       default=1e-4,
                       help='Learning Rate')

my_parser.add_argument('--epsilon',
                       metavar='eps',
                       type=float,
                       default=1e-6,
                       help='AdamW Epsilon')
my_parser.add_argument('--verbose',
                       action='store_true',
                       help='Verbose')
my_parser.add_argument('--time_step',
                       metavar='ts',
                       type=int,
                       default=100,
                       help='Time Step for Verbose')


args = my_parser.parse_args()
train_file = args.train_file
arch = args.model_arch
MAX_LENGTH = args.max_length
output_path = args.output_path
epochs = args.epochs
TRAIN_BATCH_SIZE = args.batch_size
LEARNING_RATE = args.learning_rate
eps = args.epsilon
verbose = args.verbose
time_step_size = args.time_step


def set_order(a):
    return sorted(set(a), key=a.index)

def format_time(elapsed):
    elapsed_rounded = int(round((elapsed)))
    return str(datetime.timedelta(seconds=elapsed_rounded))



# Load file
data = json.load(open(train_file,"r"))
# Heuristic: extract lists with length more than three, this encourages no substrings
data = [x for x in data if len(x[1])>3]
data = [[data_sample[0],",".join(set_order([yy.strip() for y in [x.split(",") for x in data_sample[1]] for yy in y]))] for data_sample in data]
data = [[x[0].lower(), ",".join([xx.strip()for xx in x[1].lower().split(',')])] for x in data[:]]

# datapoint example: [['ambiguous label: appeared', 'appeared,appearance,appear,film']]

input = [x[0] for x in data]

# Load models
model = T5ForConditionalGeneration.from_pretrained(arch)
tokenizer = T5Tokenizer.from_pretrained(arch)
device = torch.device("cuda") if torch.cuda.is_available() else torch.device('cpu')
model = model.to(device)

train_input_ids_ = []
for i in tqdm(input):
    encoded = tokenizer.encode_plus(i,
                                    max_length=MAX_LENGTH,
                                    truncation=True,
                                    return_tensors='pt',
                                    padding='max_length')
    train_input_ids_.append(encoded['input_ids'])
train_input_ids = torch.cat(train_input_ids_, dim=0)


labels = [x[1] for  x in data]
train_labels=[]
for i in tqdm(labels):
    encoded = tokenizer.encode_plus(i,
                                    max_length=MAX_LENGTH,
                                    truncation=True,
                                    return_tensors='pt',
                                    padding='max_length')
    train_labels.append(encoded['input_ids'])
train_labels = torch.cat(train_labels, dim = 0)

train_dataset = TensorDataset(train_input_ids,train_labels)
train_dataloader = DataLoader(dataset=train_dataset,
                              sampler=RandomSampler(train_dataset),
                              batch_size=TRAIN_BATCH_SIZE,
                              )


optimizer = AdamW(model.parameters(),
                  lr=LEARNING_RATE,
                  eps=eps)

total_steps = len(train_dataloader) * epochs

training_stats = []

total_t0 = time.time()

for epoch_i in range(epochs):
    # ========================================
    #               Training
    # ========================================
    print("")
    print('======== Epoch {:} / {:} ========'.format(epoch_i + 1, epochs))
    print('Training...')

    t0 = time.time()
    total_train_loss = 0.0

    model.train()

    for step, batch in enumerate(train_dataloader):
        if step % time_step_size == 0 and not step == 0:
            elapsed = format_time(time.time() - t0)
            if verbose:
                print('  Batch {:>5,}  of  {:>5,}.    Elapsed: {:}.'.format(step, len(train_dataloader), elapsed))

        b_input_ids = batch[0].to(device)
        b_labels = batch[1].to(device)
        o = model(input_ids=b_input_ids, labels=b_labels)
        loss = o.loss
        total_train_loss += loss.item()

        optimizer.zero_grad()
        loss.backward()
        optimizer.step()

    avg_train_loss = total_train_loss / len(train_dataloader)

    training_time = format_time(time.time() - t0)
    if verbose:
        print("")
        print("  Average training loss: {0:.2f}".format(avg_train_loss))
        print("  Training epcoh took: {:}".format(training_time))
    training_stats.append(
        {
            'epoch': epoch_i + 1,
            'Training Loss': avg_train_loss,
            'Training Time': training_time,
        }
    )

total_train_time = format_time(time.time() - total_t0)
training_stats.append({'total_train_time': total_train_time})
print("")
print("Training complete!")
print("Total training took {:} (h:mm:ss)".format(total_train_time))

model.save_pretrained(output_path)
tokenizer.save_pretrained(output_path)
json.dump(training_stats,open('training_stats.json','w'),indent=5)