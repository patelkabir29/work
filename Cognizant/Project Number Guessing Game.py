import random

number_to_guess = random.randint(1, 100)

guess = int(input("Guess the number: "))
count = 1
while guess != number_to_guess and count < 10:
    count += 1
    if guess < number_to_guess:
        print("Too low! Try again.")
    else:
        print("Too high! Try again.")
    guess = int(input("Guess the number: "))

if guess == number_to_guess:
    print("Congratulations! You guessed it in ", count, " tries!")
else:
    print("Game over! Better luck next time!")