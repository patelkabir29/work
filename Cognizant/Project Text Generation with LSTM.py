import keras
from tokenizers import Tokenizer
from keras.src.utils.sequence_utils import pad_sequences
from keras.src.models import Sequential
from keras.src.layers import LSTM, Dense, Embedding, Dropout
import numpy as np

# Load and preprocess text dataset
def load_text_dataset(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        text = f.read().lower()
    return text

# Tokenize and prepare sequences
def prepare_sequences(text, seq_length=50):
    tokenizer = Tokenizer()
    tokenizer.fit_on_texts([text])
    sequences = []
    for i in range(seq_length, len(text)):
        seq = text[i-seq_length:i+1]
        sequences.append(seq)
    
    tokenized_sequences = tokenizer.texts_to_sequences(sequences)
    X, y = np.array(tokenized_sequences)[:, :-1], np.array(tokenized_sequences)[:, -1]
    vocab_size = len(tokenizer.word_index) + 1
    X = pad_sequences(X, maxlen=seq_length, padding='pre')
    return X, y, vocab_size, tokenizer

# Build LSTM model
def build_lstm_model(vocab_size, seq_length):
    model = Sequential([
        Embedding(vocab_size, 50, input_length=seq_length),
        LSTM(100, return_sequences=True),
        LSTM(100),
        Dense(100, activation='relu'),
        Dropout(0.2),
        Dense(vocab_size, activation='softmax')
    ])
    model.compile(loss='sparse_categorical_crossentropy', optimizer='adam', metrics=['accuracy'])
    return model

# Generate text from a trained model
def generate_text(model, tokenizer, seed_text, seq_length, num_words=50):
    result = seed_text.split()
    for _ in range(num_words):
        tokenized = tokenizer.texts_to_sequences([seed_text])[0]
        tokenized = pad_sequences([tokenized], maxlen=seq_length, padding='pre')
        predicted = np.argmax(model.predict(tokenized), axis=-1)
        word = tokenizer.index_word.get(predicted[0], '')
        seed_text += ' ' + word
        result.append(word)
    return ' '.join(result)

# Main execution (example usage)
if __name__ == "__main__":
    text_data = load_text_dataset("dataset.txt")
    X, y, vocab_size, tokenizer = prepare_sequences(text_data)
    model = build_lstm_model(vocab_size, X.shape[1])
    model.fit(X, y, epochs=20, batch_size=64, validation_split=0.2)
    model.save("lstm_text_generator.h5")
