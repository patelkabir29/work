import streamlit as st
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.cluster import KMeans
import tensorflow as tf
from tensorflow import keras
from keras import layers
import torch
import torch.nn as nn
import torch.optim as optim
from torchvision import datasets, transforms
from torchvision.utils import save_image, make_grid
import os

# Streamlit UI
st.title("AI-Powered Student and Artwork Analysis")
st.sidebar.title("Navigation")
page = st.sidebar.radio("Choose a Feature", ["Exam Prediction", "Student Grouping", "AI Artwork Generation"])

# User Story 1: Exam Prediction
if page == "Exam Prediction":
    st.header("Predict Exam Performance")
    
    # Sample Data
    data = pd.DataFrame({
        "hours_studied": np.random.randint(1, 10, 100),
        "past_scores": np.random.randint(40, 100, 100),
        "passed": np.random.choice([0, 1], 100)
    })
    
    # Train Model
    X = data[["hours_studied", "past_scores"]]
    y = data["passed"]
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    scaler = StandardScaler()
    X_train = scaler.fit_transform(X_train)
    X_test = scaler.transform(X_test)
    model = LogisticRegression()
    model.fit(X_train, y_train)
    
    # User Input
    hours = st.number_input("Hours Studied", 1, 10, 2)
    past_score = st.number_input("Past Score", 40, 100, 70)
    prediction = model.predict(scaler.transform([[hours, past_score]]))[0]
    st.write("Prediction:", "Pass" if prediction == 1 else "Fail")

# User Story 2: Student Grouping
if page == "Student Grouping":
    st.header("Group Students Based on Learning Style")
    
    # Sample Data
    student_data = pd.DataFrame({
        "study_hours": np.random.randint(1, 10, 50),
        "exam_scores": np.random.randint(40, 100, 50)
    })
    
    # K-Means Clustering
    kmeans = KMeans(n_clusters=3, random_state=42)  # 3 groups: fast learners, average, struggling
    student_data["cluster"] = kmeans.fit_predict(student_data)
    
    # Visualization
    fig, ax = plt.subplots()
    sns.scatterplot(x="study_hours", y="exam_scores", hue=student_data["cluster"], palette="viridis", data=student_data, ax=ax)
    st.pyplot(fig)

# User Story 3: GAN for Artwork Generation
if page == "AI Artwork Generation":
    st.header("Generate AI Artwork with GANs")
    
    # Load dataset (Fashion-MNIST) as the training data
    transform = transforms.Compose([
        transforms.ToTensor(),
        transforms.Normalize((0.5,), (0.5,))
    ])
    dataset = datasets.FashionMNIST(root="data", train=True, transform=transform, download=True)
    
    # Define DCGAN (Deep Convolutional GAN)
    class Generator(nn.Module):
        def __init__(self):
            super(Generator, self).__init__()
            self.model = nn.Sequential(
                nn.Linear(100, 256),
                nn.ReLU(),
                nn.Linear(256, 512),
                nn.ReLU(),
                nn.Linear(512, 1024),
                nn.ReLU(),
                nn.Linear(1024, 28*28),
                nn.Tanh()
            )
        
        def forward(self, x):
            return self.model(x).view(-1, 1, 28, 28)
    
    generator = Generator()
    
    # Load pre-trained model if exists, otherwise generate new images
    if os.path.exists("generator.pth"):
        generator.load_state_dict(torch.load("generator.pth", map_location=torch.device('cpu')))
        generator.eval()
    else:
        st.write("No pre-trained model found. Please train the model first.")
    
    # Generate and Show Image
    noise = torch.randn(1, 100)
    with torch.no_grad():
        generated_image = generator(noise)
    
    fig, ax = plt.subplots()
    ax.imshow(generated_image.squeeze(), cmap="gray")
    ax.axis("off")
    st.pyplot(fig)
    
    st.write("AI-generated artwork using GANs trained on Fashion-MNIST dataset.")
