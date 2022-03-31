from nltk import ngrams

def _entailment_probability(tableList, token):
    n = len(tableList)
    count = 0
    for value in tableList:
        if value == token: count+=1
    return count / n

def _count(ngrams, ngram):
    count = 0
    for ng in ngrams:
        if ng == ngram: count+= 1
    return count

def _entailment_precision(order, sentenceList, tableList):
    norderNgrams = ngrams(sentenceList, order)
    for ngram in norderNgrams:
        countNGram = _count(norderNgrams, ngram)
        wg = _entailment_probability(tableList, ngram)
