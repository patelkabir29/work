# Task 1
def greet_user(name = "user"):
    print(f"Hello, {name}! Welcome aboard. The sum of 5 and 10 is {5 + 10}.")

# Task 2
def describe_pet(pet_name, animal_type = "dog"):
    print(f"I have a {animal_type} named {pet_name}.")

# Task 3
def make_sandwich(*items):
    print("Making a sandwich with the following ingredients:")
    for item in items:
        print(f"- {item}")
    
# Task 4
def factorial(n):
    if n == 0:
        return 1
    else:
        return n * factorial(n - 1)
    
def fibonacci(n):
    if n == 0:
        return 0
    elif n == 1:
        return 1
    else:
        return fibonacci(n - 1) + fibonacci(n - 2)
    
print(f"Factorial of 5 is {factorial(5)}")
print(f"The 6th Fibonacci number is {fibonacci(6)}")