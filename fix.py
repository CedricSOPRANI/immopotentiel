with open('pipeline.py', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace("l\\'annonce", "l annonce")

with open('pipeline.py', 'w', encoding='utf-8') as f:
    f.write(content)

print("Corrections appliquees")