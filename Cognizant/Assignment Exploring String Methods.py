s = "Python is amazing!"

print(f"First word: {s[:6]}")
print(f"Amazing part: {s[10:17]}")
print("Reversed string: ", s[::-1])

s2 = " hello, python world! "
print(f"Stripped string: {s2.strip()}")
print(f"Capitalized string: {s2.strip().capitalize()}")
print(f"Replaced string: {s2.strip().replace('world', 'universe')}")
print(f"Upper case string: {s2.strip().upper()}")

# Palindrome
s3 = input("Enter a word: ")

if s3 == s3[::-1]:
    print(f"Yes, '{s3}' is a palindrome!")
else:
    print(f"No, '{s3}' is not a palindrome.")