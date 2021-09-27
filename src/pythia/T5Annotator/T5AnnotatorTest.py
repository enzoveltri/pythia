from transformers import T5ForConditionalGeneration, T5Tokenizer
import torch
import pandas as pd
import re
from tqdm import tqdm
import json
import argparse



# Create the parser
my_parser = argparse.ArgumentParser()

# Add the arguments
my_parser.add_argument('--input_file',
                       metavar='fpath',
                       type=str,
                       help='the path to the input file',
                       required=True)

my_parser.add_argument('--model_path',
                       metavar='mpath',
                       type=str,
                       help='the path to the model and tokenizer',
                       required=True)

my_parser.add_argument('--cache_file',
                       metavar='cfile',
                       type=str,
                       help='the path to the cache')

my_parser.add_argument('--gen_steps',
                       metavar='num',
                       type=int,
                       default=10,
                       help='the number of decoder generation steps')

my_parser.add_argument('--max_length',
                       metavar='ml',
                       type=int,
                       default=100,
                       help='the max length for tokenization')


args = my_parser.parse_args()


input_file = args.input_file
model_path = args.model_path
cache_file = args.cache_file
num = args.gen_steps
MAX_LENGTH = args.max_length



# Load model and tokenizer
model = T5ForConditionalGeneration.from_pretrained(model_path)
tokenizer = T5Tokenizer.from_pretrained(model_path)
device = torch.device("cuda") if torch.cuda.is_available() else torch.device('cpu')
model = model.to(device)

# Load file
df = pd.read_csv(input_file, delimiter='\t', header = None)
df.columns = columns=['Input','Output']

# Load cache
if cache_file:
    cachedAttributes = json.load(open(cache_file,"r"))
else:
    cachedAttributes = {}


# Generation Function
def pred(x):
  model.train()
  input_ids = tokenizer.encode_plus(f'ambiguous label: {x}',
                                      max_length=MAX_LENGTH,
                                      truncation=True,
                                      return_tensors='pt',
                                      padding='max_length').input_ids

  input_ids = input_ids.to(device)
  return tokenizer.decode(model.generate(input_ids=input_ids, temperature=1)[0])

# Extract Attributes
all_attrbs=[]
F = []
for i in tqdm(range(len(df))):
  x = df.iloc[i]
  M = re.findall(".* attr1: (.*) attr2: (.*)",x['Input'])
  if M:
    attr1, attr2 = M[0]
    all_attrbs.extend([attr1, attr2])
  else:
    F.append(i)

all_attrbs = list(set(all_attrbs))

# Generate Labels
for attr in tqdm(all_attrbs[:]):
  if attr.lower() not in cachedAttributes:
    a = [pred(attr.lower()) for _ in range (num)]
    if '-' in attr or '_' in attr:
      modified_attr = attr.replace('-',"").replace('_',"")
      b = [pred(modified_attr.lower()) for _ in range (num)]
      a = a+b
    filtered_a = list(set([xx for xx in ",".join([x.replace("<pad> ","").replace("</s>","") for x in a]).split(",") if xx]))
    cachedAttributes[attr] = filtered_a

cachedAttributes = {k.lower():v for (k,v) in cachedAttributes.items()}
if not cache_file:
    cache_file = 'T5_cache.json'
json.dump(cachedAttributes, open(cache_file,"w"))




# Generate T5 Annotations
added_labels=[]
for i in tqdm(range(len(df))):
  if i not in F:
    x = df.iloc[i]
    attr1, attr2 = re.findall(".* attr1: (.*) attr2: (.*)",x['Input'])[0]
    joined = [x for x in cachedAttributes[attr1.lower()]  if x in cachedAttributes[attr2.lower()]]
    joined = [j for j in joined if j not in [str(x['Output']).lower(), attr1.lower(), attr2.lower()]]
    joined = list(set(joined))
    added_labels.append(joined)

df = df.drop(F,axis = 0)

df['New Labels'] = added_labels

new_df = []
for i in tqdm(range(len(df))):
  x = df.iloc[i]
  X = x['Input']
  Y = [x['Output']]  if  x['Output'] == 'None' else [x['Output']] + x['New Labels'] 
  separated = [[X,y] for y in Y]
  new_df.extend(separated)

final_df = pd.DataFrame(new_df)
final_df = final_df[~final_df.apply(lambda x: True if type(x[1])==str and len((x[1])) == 1 else False,axis=1)]

output_file = input_file.split('.tsv')[0]+'_T5Annotations.tsv'
final_df.to_csv(output_file,sep='\t',index=False)

