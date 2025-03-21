#Task 1 - Counting Down with Loops

#asking user for a number to start the sequence
while True:
    try:
        num = int(input("Enter the starting number: "))
    except ValueError:
        print("Please enter a valid number.")
        continue

    if num < 0:
        print("Please enter a positive number.")
        continue
    else:
        break

#printing the sequence
while num >= 0:
    if num == 0:
        print("Blast off!", end = " ")
    else:
        print(num, end = " ")
    num -= 1


#Task 2 - Multiplication Table with for Loops

#asking user for a number to start the sequence
while True:
    try:
        num = int(input("Enter the starting number: "))
    except ValueError:
        print("Please enter a valid number.")
        continue
    break
#printing the multiplication table
for i in range(1, 11):
    print(f"{num} x {i} = {num*i}")


#Task 3 - Find the Factorial
while True:
    try:
        num = int(input("Enter the starting number: "))
    except ValueError:
        print("Please enter a valid number.")
        continue

    if num < 0:
        print("Please enter a non-negative number.")
        continue
    else:
        break

new_num = num
#printing the factorial
for i in range(1, num):
    new_num *= i

print(f"The factorial of {num} is {new_num}.")