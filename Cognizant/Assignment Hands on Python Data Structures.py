fruits = ['orange', 'apple', 'pear', 'banana', 'kiwi', 'apple']

print(f"Original list: {fruits}")
fruits.append('grape')
print(f"After adding a new fruit: {fruits}")
fruits.remove('apple')
print(f"After removing a fruit: {fruits}")
print(f"Reversed list: {fruits[::-1]}")

# Dictionaries

info = {"name": "Kabir", "age": 23, "city": "Toronto"}

info["favourite color"] = "Black"
info["city"] = "Mississauga"

print("Keys: ", end="")
for i, key in enumerate(info.keys()):
    print(key, end=", ") if i < len(info.keys()) - 1 else print(key)

print("Values: ", end="")
for i, value in enumerate(info.values()):
    print(value, end=", ") if i < len(info.values()) - 1 else print(value)

# Tuples

Favourite_things = ('Inception', 'Bohemian Rhapsody', '1984')
# Favourite_things[0] = 'Interstellar' # This will throw an error as tuples are immutable
print(f"Length of the tuple: {len(Favourite_things)}")