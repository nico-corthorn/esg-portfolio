
import os
import json
import pandas as pd
import numpy as np
import contractions
import unicodedata
import re
import en_core_web_lg
import nltk
from nltk.tokenize.toktok import ToktokTokenizer
from tensorflow.keras.preprocessing.text import Tokenizer
from tensorflow.keras.preprocessing.sequence import pad_sequences


nlp = en_core_web_lg.load()
tokenizer = ToktokTokenizer()
stopword_list = nltk.corpus.stopwords.words('english')
stopword_list.remove('no')
stopword_list.remove('not')


def remove_accented_chars(text):
    text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('utf-8', 'ignore')
    return text


def remove_special_characters(text, remove_digits=False):
    pattern = r'[^a-zA-z0-9\s]' if not remove_digits else r'[^a-zA-z\s]'
    text = re.sub(pattern, '', text)
    return text


def lemmatize_text(text):
    text = nlp(text)
    text = ' '.join([word.lemma_ if word.lemma_ != '-PRON-' else word.text for word in text])
    return text


def remove_stopwords(text, is_lower_case=False):
    tokens = tokenizer.tokenize(text)
    tokens = [token.strip() for token in tokens]
    if is_lower_case:
        filtered_tokens = [token for token in tokens if token not in stopword_list]
    else:
        filtered_tokens = [token for token in tokens if token.lower() not in stopword_list]
    filtered_text = ' '.join(filtered_tokens)
    return filtered_text


def encode_text_into_sequence(texts, num_words, max_len=None, oov_token='<UNK>', pad_type='post', trunc_type='post'):
    """
        Encodes every text in texts into a sequence of integers.

        Inputs:
            texts: sequence of texts. Tested on pandas Series.
            ...

        Outputs:
            train_padded: pandas DataFrame with encoded texts.
            word_index: dictionary mapping each word to an integer.
    """

    # Tokenize our training data
    tokenizer = Tokenizer(num_words=num_words, oov_token=oov_token)
    tokenizer.fit_on_texts(texts)

    # Get our training data word index
    word_index = tokenizer.word_index

    # Encode training data sentences into sequences
    train_sequences = tokenizer.texts_to_sequences(texts)

    # Get max training sequence length
    if max_len is None:
        maxlen = max([len(x) for x in train_sequences])

    # Pad the training sequences
    train_padded = pad_sequences(train_sequences, padding=pad_type, truncating=trunc_type, maxlen=max_len)

    return train_padded, word_index


