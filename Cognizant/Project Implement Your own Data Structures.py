inventory = {
    "apple": (10, 2.5),
    "banana": (20, 1.2)
}

print("Welcome to the Inventory Manager!\nCurrent Inventory:")

# Print the current inventory
for item in inventory:
    print(f"Item: {item}, Quantity: {inventory[item][0]}, Price: ${inventory[item][1]:.2f}")

# Add a new item to the inventory
inventory["mango"] = (15, 3.0)
print("Added Mango to the inventory.\nUpdated Inventory:")

# Print the updated inventory
for item in inventory:
    print(f"Item: {item}, Quantity: {inventory[item][0]}, Price: ${inventory[item][1]:.2f}")

total = 0

# Calculate the total value of the inventory
for item in inventory:
    total += inventory[item][0] * inventory[item][1]

# Print the total value of the inventory
print(f"Total value of the inventory: ${total:.2f}")