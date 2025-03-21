from datetime import date
import re
# ask user for tasks and deadlines, then store them out in a nice format

d = {}

while True:
    task_deadline = input("To EXIT, enter 0\nEnter a task and deadline(YYYY-MM-DD) separated by colon: ")
    if task_deadline == "0":
        break
    task, deadline = task_deadline.split(":")
    task = task.strip()
    deadline = deadline.strip()
    deadline_regex = r"^(\d{4})-(\d{2})-(\d{2})$"
    matcha = re.match(deadline_regex, deadline)
    if task == "" or deadline == "" or ":" not in task_deadline:
        print("Invalid input format. Please enter a task and deadline separated by colon. Like, 'Task: YYYY-MM-DD'")
        continue
    if not matcha or not (int(matcha.group(2)) <= 12 and int(matcha.group(3)) <= 31):
        print("Invalid deadline format. Please use YYYY-MM-DD.")
        continue
    d[task] = date.fromisoformat(deadline)

# sort by deadline
for task, deadline in sorted(d.items(), key=lambda x: x[1]):
    print(f"{task} -> {deadline}")

# ask user for scores by subjects, then store them out in a nice format

s = {}

while True:
    subject_score = input("To EXIT, enter 0\nEnter a subject and score separated by colon: ")
    if subject_score == "0":
        break
    subject, score = subject_score.split(":")
    subject = subject.strip()
    score = score.strip()
    score_regex = r"^\d{1,3}$"
    matchb = re.match(score_regex, score)
    if subject == "" or score == "" or ":" not in subject_score:
        print("Invalid input format. Please enter a subject and score separated by colon. Like, 'Subject: Score'")
        continue
    if not matchb or not (0 <= int(score) <= 100):
        print("Invalid score format. Please enter a number between 0 and 100.")
        continue
    s[subject] = int(score)

# score tracking
total = 0
for subject, score in s.items():
    print(f"{subject} -> {score}")
    total += score
print(f"Total: {total}")
avg = total / len(s)
print(f"Average: {avg}")

# list of subjects that need improvement
print("Subjects that need improvement:")
for subject, score in s.items():
    if score < avg:
        print(subject)