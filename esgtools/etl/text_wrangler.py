
import contractions
import unicodedata
import re
import en_core_web_lg
import nltk
from nltk.tokenize.toktok import ToktokTokenizer
from tensorflow.keras.preprocessing.text import Tokenizer
from tensorflow.keras.preprocessing.sequence import pad_sequences


def remove_accented_chars(text):
    text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('utf-8', 'ignore')
    return text


def remove_special_characters(text, remove_digits=False):
    pattern = r'[^a-zA-z0-9\s]' if not remove_digits else r'[^a-zA-z\s]'
    text = re.sub(pattern, '', text)
    return text


def lemmatize_text(text, nlp):
    text = nlp(text)
    text = ' '.join([word.lemma_ if word.lemma_ != '-PRON-' else word.text for word in text])
    return text


def remove_stopwords(text, tokenizer, stopword_list, is_lower_case=False):
    tokens = tokenizer.tokenize(text)
    tokens = [token.strip() for token in tokens]
    if is_lower_case:
        filtered_tokens = [token for token in tokens if token not in stopword_list]
    else:
        filtered_tokens = [token for token in tokens if token.lower() not in stopword_list]
    filtered_text = ' '.join(filtered_tokens)
    return filtered_text


def preprocess_texts(texts,
                     bool_expand_contractions=True,
                     bool_remove_accents=True,
                     bool_remove_special_characters=True,
                     bool_lemmatize=True,
                     bool_remove_stop_words=True,
                     stopword_list=None):

    """
        Input:
            texts: pandas Series with strings.

        Returns
            texts_clean: pandas Series with preprocessed strings according to booleans received.
    """

    texts_clean = texts.copy()

    if bool_expand_contractions:
        texts_clean = texts_clean.map(lambda x: contractions.fix(x))

    if bool_remove_accents:
        texts_clean = texts_clean.map(lambda x: remove_accented_chars(x))

    if bool_remove_special_characters:
        texts_clean = texts_clean.map(lambda x: remove_special_characters(x, remove_digits=True))

    if bool_lemmatize:
        nlp = en_core_web_lg.load()
        texts_clean = texts_clean.map(lambda x: lemmatize_text(x, nlp))

    if bool_remove_stop_words:
        tokenizer = ToktokTokenizer()

        if stopword_list is None:
            stopword_list = nltk.corpus.stopwords.words('english')
            stopword_list.remove('no')
            stopword_list.remove('not')

        texts_clean = texts_clean.map(lambda x: remove_stopwords(x, tokenizer, stopword_list))

    return texts_clean


def encode_text_into_sequence(texts, num_words, max_len=None, oov_token='<UNK>', pad_type='post', trunc_type='post'):
    """
        Encodes every text in texts into a sequence of integers.

        Inputs:
            texts: sequence of texts. Tested on pandas Series.
            ...

        Outputs:
            train_padded: numpy array with encoded texts. Each row represents a text. Each column represents a word.
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
        max_len = max([len(x) for x in train_sequences])

    # Pad the training sequences
    train_padded = pad_sequences(train_sequences, padding=pad_type, truncating=trunc_type, maxlen=max_len)

    return train_padded, word_index

