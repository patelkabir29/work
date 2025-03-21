import turtle


def factorial(n):
    if n == 0:
        return 1
    else:
        return n * factorial(n-1)
    
def fibonacci(n):
    if n <= 1 and n >= 0:
        return n
    else:
        return fibonacci(n-1) + fibonacci(n-2)

def draw_fractal(order, size):
    if order == 0:
        turtle.forward(size)
    else:
        for angle in [60, -120, 60, -120, 60, -120, 60, -120, 60, 0]:
            draw_fractal(order-1, size/3)
            turtle.left(angle)

def fractal(n):
    turtle.speed(0)
    turtle.hideturtle()
    turtle.penup()
    turtle.goto(-100, 100)
    turtle.pendown()
    draw_fractal(n, 500)
    turtle.done()

while True:
    inp = int(input("Welcome to the Recursive Artistry Program! Choose an option: 1. Calculate Factorial 2. Find Fibonacci 3. Draw a Recursive Fractal 4. Exit \n"))

    if inp == 1:
        num = int(input("Enter a number to find its factorial: "))
        print("The factorial of", num, "is", factorial(num))
    elif inp == 2:
        num = int(input("Which term in a Fibonacci sequence are you looking for: "))
        print("The", num, "th number in the Fibonacci sequence is", fibonacci(num))
    elif inp == 3:
        num = int(input("Enter a number to print its fractal pattern: "))
        fractal(num)
    elif inp == 4:
        break

