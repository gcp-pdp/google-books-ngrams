import json
import pandas as pd
import tensorflow as tf
import tensorflow_hub as hub
import tensorflow_text
from tensorflow.keras import layers
import bert
from bert import tokenization
import sys
BertTokenizer = bert.bert_tokenization.FullTokenizer
encoder = hub.KerasLayer("https://tfhub.dev/tensorflow/small_bert/bert_en_uncased_L-12_H-128_A-2/2",trainable=True)
preprocessor = hub.load("https://tfhub.dev/tensorflow/bert_en_uncased_preprocess/2")
i = 1
while 1:
    line = sys.stdin.readline()
    if not line:
        break
    line = line.rstrip()
    text_input = tf.constant([line])
    encoder_inputs = preprocessor(text_input)
    encoder_outputs = encoder(encoder_inputs)
    embeddings = encoder_outputs["default"][0]
    print(json.dumps({"term":line,"embeddings":embeddings.numpy().tolist()}))
    if i % 1000 == 0:
        sys.stderr.write("{x:d}\n".format(x=i))
    i = i + 1
