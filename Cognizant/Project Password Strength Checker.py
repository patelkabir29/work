# ask the user for a password
pwd = input("Enter your password: ")
# check if the password is at least 8 characters long
uppercase_check = False
lowercase_check = False
digit_check = False
special_check = False
if len(pwd) < 8:
    print("Password is too short")
else:
    for a in pwd:
        uppercase_check = uppercase_check or a.isupper()
        lowercase_check = lowercase_check or a.islower()
        digit_check = digit_check or a.isdigit()
        special_check = special_check or not a.isalnum()
    
    if uppercase_check and lowercase_check and digit_check and special_check:
        print("Your password is strong! 💪")
    else:
        print("Your password is weak! 😟")
        if not uppercase_check:
            print("Password must contain at least one uppercase letter")
        if not lowercase_check:
            print("Password must contain at least one lowercase letter")
        if not digit_check:
            print("Password must contain at least one digit")
        if not special_check:
            print("Password must contain at least one special character")