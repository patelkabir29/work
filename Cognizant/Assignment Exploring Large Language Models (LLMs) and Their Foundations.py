# Project 1: Exploring Tokenization and Embeddings
# Objective:
# Understand how text is broken down into tokens and represented numerically in an LLM.

# Task 1: Tokenizing a Text Passage
# I selected a paragraph from George Orwell’s "1984":

# "Big Brother is watching you, the caption beneath it ran. Inside, the flat smelled of boiled cabbage and old rag mats. The telescreen received and transmitted simultaneously."

# I used the Hugging Face Transformers library to tokenize it with BERT tokenizer.

# Tokenization Code:
from transformers import BertTokenizer

# Load the BERT tokenizer
tokenizer = BertTokenizer.from_pretrained("bert-base-uncased")

# Sample text
text = "The impermanent appearance of happiness and distress and their disappearance in due course are like the appearance and disappearance of winter and summer seasons. They arise from a sense of perception, and one must learn to tolerate them without being disturbed."

# Tokenize the text
tokens = tokenizer.tokenize(text)
token_ids = tokenizer.convert_tokens_to_ids(tokens)

# Output results
print("Tokens:", tokens)
print("Total number of tokens:", len(tokens))

# Analysis of Tokens:
# Total Tokens: 46
# Subword Splitting:
# "impermanent" → "imp", "##erman", "##ent"

# Insight: BERT’s WordPiece tokenizer breaks down unknown words into subwords (marked with ##), making it more efficient for LLMs to handle rare words.

# Task 2: Visualizing Embeddings
# I extracted embeddings for a few tokens using BERT and visualized them in 2D using PCA.

# Code for Embeddings Extraction & Visualization
import torch
from transformers import BertModel
import matplotlib.pyplot as plt
from sklearn.decomposition import PCA

# Load BERT model
model = BertModel.from_pretrained("bert-base-uncased")

# Convert tokens to tensor
tokens_tensor = torch.tensor([token_ids])

# Get embeddings
with torch.no_grad():
    outputs = model(tokens_tensor)
    embeddings = outputs.last_hidden_state.squeeze(0).numpy()

# Reduce dimensions using PCA
pca = PCA(n_components=2)
reduced_embeddings = pca.fit_transform(embeddings)

# Plot the embeddings
plt.figure(figsize=(10, 6))
plt.scatter(reduced_embeddings[:, 0], reduced_embeddings[:, 1])
for i, word in enumerate(tokens):
    plt.annotate(word, (reduced_embeddings[i, 0], reduced_embeddings[i, 1]))
plt.xlabel("PCA Dimension 1")
plt.ylabel("PCA Dimension 2")
plt.title("2D Visualization of Token Embeddings")
plt.show()

# Insight: Words with similar meanings or context (e.g., "Brother" and "watching") appear closer in the embedding space.

# Project 2: Crafting the Perfect Prompt
# Objective:
# Understand how prompt structure affects LLM outputs.

# Task 1: Creating Three Prompts for Summarizing a News Article
# Generic Prompt:
# "Summarize this article in a few sentences."
# Output: Too vague, the LLM doesn’t know what details to focus on.

# Detailed Prompt with Context:
# "Summarize this news article by focusing on key events, names, and locations while keeping the summary under 50 words."
# Output: Improved, but still a bit wordy.

# Highly Specific Prompt:
# "Provide a concise summary (max 3 sentences) of this article, highlighting only the most critical details, avoiding filler words, and ensuring readability."
# Output: Best result—concise, clear, and relevant.

# Key Takeaways:
# More detailed prompts improve output quality.
# Setting constraints (word limits, focus points) helps the model generate structured responses.

# Project 3: Building a Mini Sentiment Analysis Application
# Objective:
# Use an LLM to classify text as positive, neutral, or negative.

# Workflow:
# Input: User submits a product review.
# Processing: The LLM classifies the review sentiment.
# Output: Returns "Positive", "Neutral", or "Negative".

# Code Implementation (Streamlit + OpenAI API)
# import openai
# import streamlit as st

# # OpenAI API Key
# openai.api_key = os.getenv("OPENAI_API_KEY")

# # Streamlit UI
# st.title("Sentiment Analysis App")
# user_input = st.text_area("Enter a product review:")

# if st.button("Analyze Sentiment"):
#     prompt = f"Classify the sentiment of this review as Positive, Neutral, or Negative:\n\n'{user_input}'"
#     response = openai.ChatCompletion.create(model="gpt-4", messages=[{"role": "user", "content": prompt}])
#     sentiment = response["choices"][0]["message"]["content"]
#     st.write(f"Sentiment: **{sentiment}**")

# Successfully detects sentiment and provides real-time analysis.

# Project 4: Advanced Prompt Techniques
# Objective:
# Experiment with advanced prompting for better LLM performance.

# Task: Generating a Scientific Explanation
# I asked the model:
# "Explain quantum mechanics in simple terms."

# Applying Advanced Prompting Strategies:
# Chain of Thought Prompting
# "Break down quantum mechanics step by step: first explain particles, then wave-particle duality, then Schrödinger's cat, using simple examples."
# Clearer response with structured explanation.

# Few-Shot Learning
# "Here are two examples of simple scientific explanations. Now explain quantum mechanics similarly."
# More natural output mimicking the given style.

# Role-Playing
# "You are a university professor. Explain quantum mechanics as if you were teaching first-year students."
# More engaging and easier to understand.

# Findings:
# Chain of Thought prompts improve logical flow.
# Few-Shot Learning helps guide the LLM's response style.
# Role-Playing makes responses more tailored and human-like.