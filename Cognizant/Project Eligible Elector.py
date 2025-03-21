#asking for user input
while True:
    try:
        age = int(input("How old are you? "))
    except ValueError:
        print("Oops! You have entered an invalid age. Please enter a valid age. Ages cannot be in text.")
        continue

    #checking if the user has entered a valid age
    if age < 0:
        print("Oops! You have entered an invalid age. Please enter a valid age. Ages cannot be negative.")
        continue
    else:
        break


#checking the age of the user
if age >= 18:
    print("Congratulations! You are eligible to vote. Go make a difference!")
else:
    print(f"Oops! You are not eligible to vote. But hey, only {18-age} more years to go!")