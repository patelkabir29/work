
import os


current_date = ''
current_dir = os.path.expanduser(r"C:\Users\patel\Downloads\STR-Anand")
print(f"Searching in: {current_dir}\n")

# List to store found files
_files = []

# Walk through all subdirectories and files
for root, _, files in os.walk(current_dir):
    for file in files:
        if current_date in file:
            old_path = os.path.join(root, file)
            new_path = old_path
            new_filename = file
            if "Holiday_Inn_Express_&_Suites_Warwick_RI" in file:
                new_filename = file.replace("Holiday_Inn_Express_&_Suites_Warwick_RI_", "")
                new_path = os.path.join(root, new_filename)
            
            try:
                os.rename(old_path, new_path)
                _files.append(new_path)
            except Exception as e:
                print(f"ERROR: Could not rename {file} - {e}")
