while True:
    try:
        f = int(input("Select a function:\n1. Addition\n2. Subtraction\n3. Multiplication\n4. Division\n5. Exit\n"))
    except ValueError:
        print("Invalid input. The input should be a number.")
        continue
    
    if f == 5:
        print("Goodbye!")
        break
    elif f < 1 or f > 5:
        print("Invalid input. The input should be between 1 and 5.")
        continue
    elif f == 1:
        try:
            a = float(input("Enter the first number: "))
            b = float(input("Enter the second number: "))
        except ValueError:
            print("Invalid input. The input should be a number.")
            continue
        print(f"The result is: {a + b}")
        break
    elif f == 2:
        try:
            a = float(input("Enter the first number: "))
            b = float(input("Enter the second number: "))
        except ValueError:
            print("Invalid input. The input should be a number.")
            continue
        print(f"The result is: {a - b}")
        break
    elif f == 3:
        try:
            a = float(input("Enter the first number: "))
            b = float(input("Enter the second number: "))
        except ValueError:
            print("Invalid input. The input should be a number.")
            continue
        print(f"The result is: {a * b}")
        break
    elif f == 4:
        try:
            a = float(input("Enter the first number: "))
            b = float(input("Enter the second number: "))
        except ValueError:
            print("Invalid input. The input should be a number.")
            continue
        if b == 0:
            raise ZeroDivisionError("Division by zero is not allowed.")
        print(f"The result is: {a / b}")
        break


