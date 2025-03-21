import os
from openai import OpenAI

# Initialize OpenAI client
client = OpenAI()

def generate_text(prompt, max_tokens=100, temperature=0.7):
    """Generates text using OpenAI's API based on the given prompt."""
    response = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=max_tokens,
        temperature=temperature
    )
    return response["choices"][0]["message"]["content"]

# Retrieve API key from environment variable
api_key = os.getenv("OPENAI_API_KEY")

# Interactive loop
print("AI-Powered Text Completion. Type 'exit' to stop.")
while True:
    user_input = input("Enter a prompt: ")
    if user_input.lower() == "exit":
        print("Goodbye!")
        break
    # Check for valid input
    if not user_input:
        print("Prompt cannot be empty. Please try again.")
        continue
    response = generate_text(user_input)
    print("AI Response:", response)
