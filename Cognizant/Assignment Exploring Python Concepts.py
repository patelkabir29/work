name = "Kabir"
age = 23
height = 5.9

#printing my name, age and height
print("Hey there, my name is ", name, "! I'm ", age, " years old and ", height, "feet tall.")

num1 = 3
num2 = 8

#addition
print("Addition of ", num1, " and ", num2, " is ", num1 + num2)

#subtraction
print("Subtraction of ", num1, " and ", num2, " is ", num1 - num2)

#multiplication
print("Multiplication of ", num1, " and ", num2, " is ", num1 * num2)

#division
print("Division of ", num1, " and ", num2, " is ", num1 / num2)


#asking for user input
num = input("Enter a number: ")

#checking if the number is positive, negative or zero
if int(num) > 0:
    print("The number is positive!")
elif int(num) < 0:
    print("The number is negative!")
else:
    print("The number is zero! Right in the middle of everything!")