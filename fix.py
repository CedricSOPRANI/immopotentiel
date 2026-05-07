with open('pipeline.py', 'r', encoding='utf-8', errors='ignore') as f:
    content = f.read()

# Remplacer la ligne problematique par une version simple
old = '''{("<a href='" + a.get("url","#") + "' style='display:inline-block;margin-top:8px;font-size:11px;color:#2cb4f5;'>Voir annonce</a>") if a.get("url") else ""}'''

new = '''{url_link}'''

# Ajouter la variable avant la f-string
old2 = '''    rows = ""
    for i, a in enumerate(annonces[:CONFIG["max_digest"]]):'''

new2 = '''    rows = ""
    for i, a in enumerate(annonces[:CONFIG["max_digest"]]):
        url_link = ('<a href="' + (a.get("url") or "#") + '" style="display:inline-block;margin-top:8px;font-size:11px;color:#2cb4f5;">Voir annonce</a>') if a.get("url") else ""'''

content = content.replace(old, new)
content = content.replace(old2, new2)

with open('pipeline.py', 'w', encoding='utf-8') as f:
    f.write(content)

print("Done - verif ligne 791:")
lines = content.split('\n')
print(lines[789])
print(lines[790])
print(lines[791])