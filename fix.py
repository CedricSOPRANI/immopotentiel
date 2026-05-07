with open('pipeline.py', 'r', encoding='utf-8', errors='ignore') as f:
    content = f.read()

old = '''    {('<a href="' + a.get("url","#") + '" style="display:inline-block;margin-top:8px;font-size:11px;color:#2cb4f5;">Voir annonce</a>') if a.get("url") else ""}'''

new = '''    {("<a href='" + a.get("url","#") + "' style='display:inline-block;margin-top:8px;font-size:11px;color:#2cb4f5;'>Voir annonce</a>") if a.get("url") else ""}'''

if old in content:
    content = content.replace(old, new)
    print("Remplacement effectue")
else:
    print("Texte non trouve - affichage ligne 791:")
    lines = content.split('\n')
    print(repr(lines[790]))

with open('pipeline.py', 'w', encoding='utf-8') as f:
    f.write(content)