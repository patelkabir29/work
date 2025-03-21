import torch
from transformers import T5Tokenizer, T5ForConditionalGeneration, Trainer, TrainingArguments
from datasets import load_dataset

# Step 1: Define NLP Task - Text Summarization
# Objective: Condense lengthy documents into concise summaries.
# Real-world Application: News article summarization for quick insights.

dataset = load_dataset("ccdv/cnn_dailymail", "3.0.0")  # Using CNN/DailyMail dataset

# Step 2: Dataset Preparation
# Preprocessing function to tokenize text
tokenizer = T5Tokenizer.from_pretrained("t5-small")

def preprocess_function(examples):
    inputs = ["summarize: " + doc for doc in examples["article"]]
    model_inputs = tokenizer(inputs, max_length=512, truncation=True, padding="max_length")
    labels = tokenizer(examples["highlights"], max_length=150, truncation=True, padding="max_length")
    model_inputs["labels"] = labels["input_ids"]
    return model_inputs

# Tokenizing the dataset
tokenized_datasets = dataset.map(preprocess_function, batched=True)

# Step 3: Fine-Tune a Transformer Model
model = T5ForConditionalGeneration.from_pretrained("t5-small")

# Splitting data into train/test
train_dataset = tokenized_datasets["train"].shuffle(seed=42).select(range(200))
test_dataset = tokenized_datasets["validation"].shuffle(seed=42).select(range(50))

# Defining training arguments
training_args = TrainingArguments(
    output_dir="./results",
    evaluation_strategy="epoch",
    learning_rate=2e-5,
    num_train_epochs=3,
    weight_decay=0.01,
    per_device_train_batch_size=4,
    per_device_eval_batch_size=4,
)

trainer = Trainer(
    model=model,
    args=training_args,
    train_dataset=train_dataset,
    eval_dataset=test_dataset,
)

# Step 4: Train the Model
trainer.train()

# Step 5: Evaluate the Model using ROUGE scores
from datasets import load_metric

metric = load_metric("rouge")

# Generating summaries for evaluation
def evaluate_model(test_samples):
    inputs = tokenizer(["summarize: " + doc for doc in test_samples["article"]], return_tensors="pt", truncation=True, padding=True, max_length=512)
    summaries = model.generate(inputs.input_ids)
    decoded_summaries = tokenizer.batch_decode(summaries, skip_special_tokens=True)
    return decoded_summaries

generated_summaries = evaluate_model(test_dataset.select(range(10)))
reference_summaries = [summary for summary in test_dataset.select(range(10))["highlights"]]

# Compute ROUGE Score
results = metric.compute(predictions=generated_summaries, references=reference_summaries)
print("ROUGE Score:", results)
