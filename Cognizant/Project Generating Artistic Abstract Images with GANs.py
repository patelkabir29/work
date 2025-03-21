import tensorflow as tf
from tensorflow.python.keras.layers import Dense, Flatten, Reshape, Conv2D, Conv2DTranspose, LeakyReLU
from keras.src.layers import BatchNormalization
from tensorflow.python.keras.models import Sequential, Model
import numpy as np
import matplotlib.pyplot as plt
import os
from keras.src.datasets import cifar10
from keras.src.optimizers import Adam
from keras.src import Input

# Step 1: Load and preprocess dataset (Use abstract art dataset if available)
def load_data():
    (x_train, _), (_, _) = cifar10.load_data()  # Placeholder dataset
    x_train = x_train / 255.0  # Normalize
    x_train = x_train.astype('float32')
    return x_train

# Step 2: Define the Generator
def build_generator():
    model = Sequential([
        Dense(8*8*256, input_shape=(100,)),  # Fully connected layer
        Reshape((8, 8, 256)),  # Reshape into 8x8 feature maps
        BatchNormalization(),  # Normalize activations
        LeakyReLU(alpha=0.2),  # Use Leaky ReLU activation
        
        Conv2DTranspose(128, kernel_size=4, strides=2, padding='same'),
        BatchNormalization(),  # Normalize before activation
        LeakyReLU(alpha=0.2),
        
        Conv2DTranspose(64, kernel_size=4, strides=2, padding='same'),
        BatchNormalization(),
        LeakyReLU(alpha=0.2),
        
        Conv2DTranspose(3, kernel_size=4, strides=2, padding='same', activation='tanh')  # Output layer
    ])
    return model

# Test if the model builds correctly
generator = build_generator()
generator.summary()

# Step 3: Define the Discriminator
def build_discriminator():
    model = Sequential([
        Conv2D(64, kernel_size=4, strides=2, padding='same', input_shape=(32, 32, 3)),
        LeakyReLU(alpha=0.2),

        Conv2D(128, kernel_size=4, strides=2, padding='same'),
        BatchNormalization(),
        LeakyReLU(alpha=0.2),

        Flatten(),
        Dense(1, activation='sigmoid')
    ])
    return model

# Step 4: Compile GAN
def compile_gan(generator, discriminator):
    discriminator.compile(optimizer=Adam(0.0002, 0.5), loss='binary_crossentropy', metrics=['accuracy'])
    discriminator.trainable = False
    gan_input = Input(shape=(100,))
    fake_image = generator(gan_input)
    validity = discriminator(fake_image)
    gan = Model(gan_input, validity)
    gan.compile(optimizer=Adam(0.0002, 0.5), loss='binary_crossentropy')
    return gan

# Step 5: Train the GAN
def train_gan(generator, discriminator, gan, data, epochs=5000, batch_size=64):
    half_batch = batch_size // 2

    for epoch in range(epochs):
        # Train Discriminator
        idx = np.random.randint(0, data.shape[0], half_batch)
        real_images = data[idx]
        fake_images = generator.predict(np.random.randn(half_batch, 100))
        
        d_loss_real = discriminator.train_on_batch(real_images, np.ones((half_batch, 1)))
        d_loss_fake = discriminator.train_on_batch(fake_images, np.zeros((half_batch, 1)))
        d_loss = 0.5 * np.add(d_loss_real, d_loss_fake)

        # Train Generator
        noise = np.random.randn(batch_size, 100)
        g_loss = gan.train_on_batch(noise, np.ones((batch_size, 1)))

        if epoch % 500 == 0:
            print(f"Epoch {epoch}, D Loss: {d_loss[0]}, G Loss: {g_loss}")
            generate_and_save_images(generator, epoch)

# Step 6: Generate and Save Images
def generate_and_save_images(generator, epoch, num_images=5):
    noise = np.random.randn(num_images, 100)
    generated_images = generator.predict(noise)
    generated_images = (generated_images + 1) / 2.0  # Normalize for display

    fig, axes = plt.subplots(1, num_images, figsize=(10, 2))
    for i in range(num_images):
        axes[i].imshow(generated_images[i])
        axes[i].axis('off')
    plt.savefig(f"generated_epoch_{epoch}.png")
    plt.show()

# Run the training process
data = load_data()
generator = build_generator()
discriminator = build_discriminator()
gan = compile_gan(generator, discriminator)
train_gan(generator, discriminator, gan, data)
