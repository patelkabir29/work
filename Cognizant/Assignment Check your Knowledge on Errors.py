n = int(input("Enter a number: "))

try:
    print("100 divided by ", n, " is ", 100 / n)

except ZeroDivisionError:
    print("Cannot divide by zero")
except ValueError:
    print("You must enter a number")


# Task 2
# it'll pop an error because there is no index 5!
l = [1, 2, 3, 4, 5]
try:
    print(l[5])
except IndexError:
    print("Index out of range")

# key-value pair doesn't exist for key "city"
d = {"name": "John", "age": 30}
try:
    print(d["city"])
except KeyError:
    print("Key not found in dictionary.")

# can't add an int with an alphabetic string
try:
    n = 50 + "abc"
except TypeError:
    print("Unsupported operation")
