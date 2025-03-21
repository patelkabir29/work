# Task: Build a decision tree model to predict weather conditions.

import pandas as pd

# Creating a sample dataset
data = {
    'Temperature': [30, 22, 25, 28, 35, 18, 20, 33, 21, 27],
    'Humidity': [70, 85, 80, 65, 60, 90, 75, 55, 85, 72],
    'WindSpeed': [15, 10, 20, 10, 5, 10, 15, 5, 10, 12],
    'Precipitation': [0, 1, 0, 0, 0, 1, 0, 0, 1, 0],
    'Weather': ['Sunny', 'Rainy', 'Sunny', 'Sunny', 'Sunny', 'Rainy', 'Sunny', 'Sunny', 'Rainy', 'Sunny']
}

df = pd.DataFrame(data)
print(df.head())


from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder

# Encode labels
label_encoder = LabelEncoder()
df['Weather'] = label_encoder.fit_transform(df['Weather'])  # 0 = Rainy, 1 = Sunny

# Split data
X = df.drop('Weather', axis=1)
y = df['Weather']
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)


from sklearn.tree import DecisionTreeClassifier
from sklearn import tree
import matplotlib.pyplot as plt

# Train model
model = DecisionTreeClassifier(max_depth=3, random_state=42)
model.fit(X_train, y_train)

# Visualize the tree
plt.figure(figsize=(12,8))
tree.plot_tree(model, feature_names=X.columns, class_names=label_encoder.classes_, filled=True)
plt.show()


# Training and Testing Accuracy
train_acc = model.score(X_train, y_train)
test_acc = model.score(X_test, y_test)

print(f"Training Accuracy: {train_acc}")
print(f"Testing Accuracy: {test_acc}")

# Tune hyperparameters
model = DecisionTreeClassifier(max_depth=4, min_samples_split=3, random_state=42)
model.fit(X_train, y_train)


from sklearn.metrics import accuracy_score, precision_score, recall_score, confusion_matrix

# Predictions
y_pred = model.predict(X_test)

# Metrics
accuracy = accuracy_score(y_test, y_pred)
precision = precision_score(y_test, y_pred)
recall = recall_score(y_test, y_pred)
conf_matrix = confusion_matrix(y_test, y_pred)

print(f"Accuracy: {accuracy}")
print(f"Precision: {precision}")
print(f"Recall: {recall}")
print("Confusion Matrix:")
print(conf_matrix)
