# T5 Annotator

## Installation

```bash
pip install -r requirements.txt
```

## Training
For training, only the training file is required. The file should contain the ambiguous label and a list of possible alternatives. Check ```train_sample.json``` for examples. The script fine-tunes a model and saves it with its tokenizer. A json files containing some statistics is also outputed.

```bash
!python T5AnnotatorTrain.py --train_file train_sample.json \
                            --verbose \

```
More aspects of the training process can be configured:
```bash
!python T5AnnotatorTrain.py --train_file train_sample.json \
                            --model_arch t5-base \
                            --output_path t5_output \
                            --max_length 100\
                            --epochs 10\
                            --batch_size 16\
                            --learning_rate 1e-4\
                            --epsilon 1e-6\
                            --verbose \
                            --time_step 100

```
* __--train_file__: JSON file containing training data.
* __--model_arch__: T5 Architecture used
* __--output_path__: Path to save the model and tokenizer
* __--max_length__: Man length for tokenization
* __--epochs__: number of epochs
* __--batch_size__: training batch size
* __--learning_rate__: Learning Rate
* __--epsilon__: AdamW epsilon value
* __--verbose__: prints training progress
* __--time_step__: sets the number of steps before printing 

## Testing
For testing, an input file and model path are required. Check ```sample_input.tsv``` for examples. The script produces a new TSV file with the added annotations from the fine-tuned model. The script caches the output for later use.
```bash
!python T5AnnotatorTest.py --input_file sample_input.tsv \
                           --model_path t5_output/ \
```
A cache file can be used:
```bash
!python T5AnnotatorTest.py --input_file sample_input.tsv \
                           --model_path t5_output/ \
                           --cache_file cache.json \
                           --gen_steps 10 \
                           --max_length 100 
```
* __--input_file__: TSV file containing test data.
* __--model_path__: Model Path
* __--cache_file__: Cache File in JSON format
* __--gen_steps__: Number of decoding steps
* __--max_length__: maximum length for tokenization
## License
[MIT](https://choosealicense.com/licenses/mit/)