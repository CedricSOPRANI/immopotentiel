#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════╗
║          ImmoPotentiel — Pipeline d'extraction automatique        ║ 
║  Scraping réel · Analyse Claude IA · Push mobile · Email digest  ║
╚══════════════════════════════════════════════════════════════════╝

USAGE :
  python pipeline.py              → Lancer maintenant
  python pipeline.py --test       → Tester sans envoyer notifications
  python pipeline.py --schedule   → Lancer en mode daemon (tous les jours à 7h)

PRÉREQUIS :
  pip install playwright requests anthropic schedule python-dotenv
  playwright install chromium

VARIABLES D'ENVIRONNEMENT (.env) :
  ANTHROPIC_API_KEY=sk-ant-...
  ONESIGNAL_APP_ID=...           (push mobile - onesignal.com gratuit)
  ONESIGNAL_API_KEY=...
  SMTP_HOST=smtp.gmail.com
  SMTP_PORT=587
  SMTP_USER=vous@gmail.com
  SMTP_PASS=votre_app_password    (Gmail App Password)
  EMAIL_DEST=vous@email.com
"""

import os, json, time, re, math, asyncio, smtplib, schedule
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from dotenv import load_dotenv
import requests
import anthropic
from playwright.async_api import async_playwright

load_dotenv()

# ═══════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════

CONFIG = {
    # Heure d'extraction quotidienne
    "heure_extraction": "07:00",

    # Régions et zones (rayon 40km)
        "zones": {
        "idf": [
            # Paris (0-10km)
            "Paris 1er","Paris 2e","Paris 3e","Paris 4e","Paris 5e","Paris 6e",
            "Paris 7e","Paris 8e","Paris 9e","Paris 10e","Paris 11e","Paris 12e",
            "Paris 13e","Paris 14e","Paris 15e","Paris 16e","Paris 17e",
            "Paris 18e","Paris 19e","Paris 20e",
            "Bagnolet","Gentilly","Montrouge","Kremlin-Bicêtre","Ivry-sur-Seine",
            "Pantin","Malakoff","Arcueil","Charenton-le-Pont","Clichy","Vincennes",
            "Levallois-Perret","Aubervilliers","Neuilly-sur-Seine","Cachan",
            "Issy-les-Moulineaux","Montreuil","Villejuif","Bagneux","Châtillon",
            "Asnières-sur-Seine","Vitry-sur-Seine","Alfortville",
            "Boulogne-Billancourt","Maisons-Alfort","Courbevoie","Bobigny",
            "Noisy-le-Sec","Puteaux","L'Haÿ-les-Roses","Saint-Denis","Clamart",
            "Fontenay-sous-Bois","Meudon","Suresnes",
            # 10-20km
            "Colombes","Saint-Cloud","Thiais","Créteil","Sèvres","Stains",
            "Choisy-le-Roi","Drancy","Épinay-sur-Seine","Nanterre",
            "Châtenay-Malabry","Rungis","Antony","Pierrefitte-sur-Seine",
            "Rueil-Malmaison","Champigny-sur-Marne","Argenteuil","Orly",
            "Enghien-les-Bains","Deuil-la-Barre","Villeneuve-le-Roi",
            "Soisy-sous-Montmorency","Noisy-le-Grand","Montmorency","Ermont",
            "Sarcelles","Massy","Vélizy-Villacoublay","Sucy-en-Brie",
            "Ormesson-sur-Marne","Sartrouville","Le Chesnay","Boissy-Saint-Léger",
            "Franconville","Versailles","Palaiseau","Chelles","Longjumeau",
            "Le Pecq","Domont","Juvisy-sur-Orge","Marly-le-Roi",
            "Saint-Germain-en-Laye","Villecresnes","Ecouen",
            # 20-30km
            "Saint-Leu-la-Forêt","Roissy-en-France","Villeparisis","Taverny",
            "Viry-Châtillon","Goussainville","Santeny","Torcy","Ris-Orangis",
            "Sainte-Geneviève-des-Bois","Poissy","Conflans-Sainte-Honorine",
            "Évry-Courcouronnes","Lagny-sur-Marne","Pontoise","Cergy",
            "Corbeil-Essonnes","Luzarches",
            # 30-40km
            "Arpajon","Chantilly","Meaux","Mantes-la-Jolie","Melun",
            "Dammarie-les-Lys","Fontainebleau","Montereau-Fault-Yonne",
            "Coulommiers","Provins","Étampes","Dourdan","Nemours",
        ],
        "lyon": [
            # Lyon (0-10km)
            "Lyon 1er","Lyon 2e","Lyon 3e","Lyon 4e","Lyon 5e",
            "Lyon 6e","Lyon 7e","Lyon 8e","Lyon 9e",
            "Villeurbanne","Caluire-et-Cuire","Écully","Tassin-la-Demi-Lune",
            "Francheville","Oullins","Saint-Fons","Bron","Vaulx-en-Velin",
            "Pierre-Bénite","Marcy-l'Étoile","Dardilly","Sathonay-Camp",
            "Rillieux-la-Pape","Vénissieux","Saint-Genis-Laval","Craponne",
            "Collonges-au-Mont-d'Or","Limonest","Saint-Cyr-au-Mont-d'Or",
            "Saint-Genis-les-Ollières",
            # 10-20km
            "Feyzin","Chassieu","Saint-Priest","Brignais","Neuville-sur-Saône",
            "Poleymieux-au-Mont-d'Or","Corbas","Solaize","Vaugneray",
            "Saint-Symphorien-d'Ozon","Mions","Albigny-sur-Saône","Communay",
            "Miribel","Lentilly","Décines-Charpieu","Thurins",
            "Fleurieu-sur-Saône","Orliénas","Saint-Pierre-de-Chandieu",
            "Montagny","Chaponnay","Grigny","Ternay","Meyzieu",
            "Saint-Laurent-de-Mure","Chasse-sur-Rhône","L'Arbresle",
            "Montluel","Givors","Mornant",
            # 20-30km — SUD-EST LYONNAIS (Diémoz, Heyrieux, etc.)
            "Trévoux","Jonage","Bully","Péage-de-Roussillon","Beynost",
            "Satolas-et-Bonce","Diémoz","Pusignan","Roussillon","Heyrieux",
            "Valencin","Grenay","Janneyrias","Saint-Quentin-Fallavier",
            "Seyssuel","Reventin-Vaugris","Pont-de-Chéruy","Anthon",
            "Saint-Clair-du-Rhône","Tignieu-Jameyzieu",
            "Villefranche-sur-Saône","Vienne","Auberives-sur-Varèze",
            "Villars-les-Dombes","Villefontaine",
            # 30-40km
            "Loyettes","Anjou","Crémieu","L'Isle-d'Abeau","Tarare",
            "Bourgoin-Jallieu",
        ],
        "avignon": [
            # Avignon (0-10km)
            "Avignon","Villeneuve-lès-Avignon","Le Pontet","Montfavet",
            "Rognonas","Barbentane","Sorgues","Vedène","Chateaurenard",
            # 10-20km
            "Graveson","Entraigues-sur-la-Sorgue","Châteauneuf-du-Pape",
            "Noves","Eyragues","Maillane","Althen-des-Paluds","Cabannes",
            "Monteux","Saint-Rémy-de-Provence","Saint-Andiol","Sarrians",
            "Remoulins","Tarascon",
            # 20-30km
            "L'Isle-sur-la-Sorgue","Beaucaire","Orange","Pernes-les-Fontaines",
            "Mollèges","Carpentras","Cavaillon","Les Baux-de-Provence",
            "Plan-d'Orgon","Vacqueyras","Maussane-les-Alpilles","Gigondas",
            "Fontvieille","Beaumes-de-Venise","Orgon","Bagnols-sur-Cèze",
            "Mouriès",
            # 30-40km
            "Mormoiron","Uzès","Gordes","Sénas","Arles","Eyguières",
            "Saint-Martin-de-Crau","Menerbes","Lamanon","Malaucène",
            "Pont-Saint-Esprit","Bollène","Nîmes","Vaison-la-Romaine",
            "Alleins","Roussillon","Apt","Lourmarin","Bonnieux","Lacoste",
            "Pertuis","Cadenet","Ansouis","Cucuron","Salon-de-Provence",
            "Nyons","Valréas","Charleval","Lambesc","Villelaure",
        ],
    },
    "score_decoupe_min":     70,
    "score_parcellaire_min": 65,
    "score_total_min":       70,
    "prix_max":              3_000_000,
    "surface_min_m2":        80,
    "terrain_min_m2":        0,     # 0 = pas de filtre terrain
    "anciennete_max_jours":  180,
    "taux_marge_min_pct":    10,

    # Types acceptés
    "types_biens": ["Immeuble", "Maison", "Mixte", "Rural", "Appartement", "Terrain"],

    # Nb max d'annonces dans le digest
    "max_digest": 15,
}

# ═══════════════════════════════════════════════════════════════════
# CALCULS MDB (mêmes formules que l'app React)
# ═══════════════════════════════════════════════════════════════════

def calc_frais_notaire_mdb(prix: float) -> dict:
    """Frais de notaire régime MDB — art. 1594F quinquies CGI"""
    droits = round(prix * 0.00715)
    tranches = [(0, 6500, 0.03945), (6500, 17000, 0.01627),
                (17000, 60000, 0.01085), (60000, float("inf"), 0.00814)]
    emol_ht = 0
    for lo, hi, tx in tranches:
        if prix <= lo:
            break
        emol_ht += (min(prix, hi) - lo) * tx
    emol_ht  = round(emol_ht)
    tva      = round(emol_ht * 0.20)
    csi      = round(prix * 0.001)
    debours  = min(1200, round(400 + prix * 0.0005))
    total    = droits + emol_ht + tva + csi + debours
    economie = round(prix * (0.058 - 0.00715))
    return {"total": total, "economie": economie, "taux_pct": (total / prix) * 100}

def calc_travaux(surface_m2: float, type_bien: str, niveau: str = "complet") -> int:
    """Travaux estimés — base 1 000 €/m²"""
    coefs_niveau = {"leger": 0.40, "moyen": 0.70, "complet": 1.00, "lourd": 1.35}
    coefs_type   = {"Immeuble": 1.10, "Mixte": 1.15, "Maison": 1.00,
                    "Appartement": 0.95, "Rural": 1.20, "Autre": 1.00}
    c_niv  = coefs_niveau.get(niveau, 1.00)
    c_type = coefs_type.get(type_bien, 1.00)
    return round(surface_m2 * 1000 * c_niv * c_type)

def calc_offre(prix: float, revente: float, travaux: int,
               anciennete: int, retour_vente: bool) -> dict:
    """Offre d'achat min / cible / max pour MDB"""
    fn     = calc_frais_notaire_mdb(prix)
    denom  = 1 + fn["taux_pct"] / 100 + 0.15
    max_p  = round(((revente - travaux) / denom) / 1000) * 1000
    dc, dm = 0.93, 0.86
    if anciennete > 60:   dc -= 0.03; dm -= 0.03
    if anciennete > 100:  dc -= 0.02; dm -= 0.02
    if retour_vente:       dc -= 0.03; dm -= 0.04
    cible = round(max_p * dc / 1000) * 1000
    mini  = round(max_p * dm / 1000) * 1000
    fn_cible = calc_frais_notaire_mdb(cible)
    marge    = round(revente - cible - fn_cible["total"] - travaux)
    taux_m   = round((marge / (cible + fn_cible["total"] + travaux)) * 100)
    return {"min": mini, "cible": cible, "max": max_p,
            "marge_nette": marge, "taux_marge_pct": taux_m}

# ═══════════════════════════════════════════════════════════════════
# SCRAPING — SELOGER
# ═══════════════════════════════════════════════════════════════════

async def scrape_seloger(ville: str, page) -> list[dict]:
    """Scrape les annonces SeLoger pour une ville donnée"""
    annonces = []
    try:
        # URL de recherche SeLoger (immeuble + maison + terrain)
        url = (
            f"https://www.seloger.com/list.htm?"
            f"types=2,1,13&projects=2&enterprise=0&price=0/3000000"
            f"&surface=80/NaN&places=[{{'label':'{ville}'}}]"
            f"&sort=d_dt_crea&districts=&bedrooms=&rooms="
        )
        await page.goto(url, wait_until="networkidle", timeout=30000)
        await page.wait_for_timeout(2000)

        cards = await page.query_selector_all("[data-testid='sl.explore.resultsItem']")
        if not cards:
            cards = await page.query_selector_all(".c-pa-list__item, .listing-item")

        for card in cards[:10]:  # max 10 par ville
            try:
                titre  = await card.query_selector("h2, .c-pa-info__title")
                prix_el = await card.query_selector("[data-testid='price'], .c-pa-price")
                surf_el = await card.query_selector("[data-testid='surface'], .c-pa-criterion__item")
                addr_el = await card.query_selector("[data-testid='city'], .c-pa-info__city")
                desc_el = await card.query_selector("[data-testid='description'], .c-pa-description")
                link_el = await card.query_selector("a[href*='/annonces/']")

                texte_titre = await titre.inner_text() if titre else ""
                texte_prix  = await prix_el.inner_text() if prix_el else "0"
                texte_surf  = await surf_el.inner_text() if surf_el else "0"
                texte_addr  = await addr_el.inner_text() if addr_el else ville
                texte_desc  = await desc_el.inner_text() if desc_el else ""
                lien        = await link_el.get_attribute("href") if link_el else ""

                # Nettoyage prix
                prix_num = int(re.sub(r"[^\d]", "", texte_prix)) if re.search(r"\d", texte_prix) else 0
                surf_num = int(re.sub(r"[^\d]", "", texte_surf.split("m")[0])) if re.search(r"\d", texte_surf) else 0

                if prix_num > 0 and surf_num >= CONFIG["surface_min_m2"]:
                    annonces.append({
                        "source":    "SeLoger",
                        "url":       f"https://www.seloger.com{lien}" if lien else url,
                        "titre":     texte_titre.strip(),
                        "adresse":   f"{texte_addr.strip()}, {ville}",
                        "prix":      prix_num,
                        "surface":   surf_num,
                        "description": f"{texte_titre}\n{texte_desc}",
                        "ville":     ville,
                    })
            except Exception:
                continue

    except Exception as e:
        print(f"  ⚠️  SeLoger {ville}: {e}")

    return annonces


# ═══════════════════════════════════════════════════════════════════
# SCRAPING — LEBONCOIN
# ═══════════════════════════════════════════════════════════════════

async def scrape_leboncoin(ville: str, page) -> list[dict]:
    """Scrape LeBonCoin immobilier pour une ville"""
    annonces = []
    try:
        url = (
            f"https://www.leboncoin.fr/recherche?"
            f"category=9&locations={ville}&real_estate_type=1,2,3,4"
            f"&price=min-3000000&square=80-max&sort=time&order=desc"
        )
        await page.goto(url, wait_until="networkidle", timeout=30000)
        await page.wait_for_timeout(2000)

        cards = await page.query_selector_all("[data-test-id='ad'], article[data-qa-id='aditem_container']")

        for card  in cards[:10]:
            try:
                titre_el = await card.query_selector("h2, [data-qa-id='aditem_title']")
                prix_el  = await card.query_selector("[data-qa-id='aditem_price'], [data-test-id='price']")
                surf_el  = await card.query_selector("[data-test-id='attribute-surface']")
                desc_el  = await card.query_selector("[data-qa-id='aditem_description']")
                link_el  = await card.query_selector("a")

                texte_titre = await titre_el.inner_text() if titre_el else ""
                texte_prix  = await prix_el.inner_text() if prix_el else "0"
                texte_surf  = await surf_el.inner_text() if surf_el else "0"
                texte_desc  = await desc_el.inner_text() if desc_el else ""
                lien        = await link_el.get_attribute("href") if link_el else ""

                prix_num = int(re.sub(r"[^\d]", "", texte_prix)) if re.search(r"\d", texte_prix) else 0
                surf_num = int(re.sub(r"[^\d]", "", texte_surf.split("m")[0])) if re.search(r"\d", texte_surf) else 0

                if prix_num > 0 and surf_num >= CONFIG["surface_min_m2"]:
                    annonces.append({
                        "source":    "LeBonCoin",
                        "url":       lien if lien.startswith("http") else f"https://www.leboncoin.fr{lien}",
                        "titre":     texte_titre.strip(),
                        "adresse":   ville,
                        "prix":      prix_num,
                        "surface":   surf_num,
                        "description": f"{texte_titre}\n{texte_desc}",
                        "ville":     ville,
                    })
            except Exception:
                continue

    except Exception as e:
        print(f"  ⚠️  LeBonCoin {ville}: {e}")

    return annonces


# ═══════════════════════════════════════════════════════════════════
# SCRAPING — PAP
# ═══════════════════════════════════════════════════════════════════

async def scrape_pap(ville: str, page) -> list[dict]:
    """Scrape PAP (particulier à particulier) pour une ville"""
    annonces = []
    try:
        ville_slug = ville.lower().replace(" ", "-").replace("'", "-").replace("é", "e").replace("è", "e")
        url = f"https://www.pap.fr/annonce/ventes-immobilieres-{ville_slug}-g{ville_slug}?surface=80&prix=&nb-pieces=&nb-chambres="

        await page.goto(url, wait_until="networkidle", timeout=30000)
        await page.wait_for_timeout(2000)

        cards = await page.query_selector_all(".search-list-item, article.item-list")

        for card in cards[:8]:
            try:
                titre_el = await card.query_selector("h2, .item-title")
                prix_el  = await card.query_selector(".price, .item-price")
                desc_el  = await card.query_selector(".item-description, .description")
                link_el  = await card.query_selector("a")

                texte_titre = await titre_el.inner_text() if titre_el else ""
                texte_prix  = await prix_el.inner_text() if prix_el else "0"
                texte_desc  = await desc_el.inner_text() if desc_el else ""
                lien        = await link_el.get_attribute("href") if link_el else ""

                prix_num = int(re.sub(r"[^\d]", "", texte_prix)) if re.search(r"\d", texte_prix) else 0

                # Extraire surface depuis description
                surf_match = re.search(r"(\d+)\s*m[²2]", texte_desc + texte_titre)
                surf_num   = int(surf_match.group(1)) if surf_match else 0

                if prix_num > 0:
                    annonces.append({
                        "source":    "PAP",
                        "url":       f"https://www.pap.fr{lien}" if lien and not lien.startswith("http") else lien,
                        "titre":     texte_titre.strip(),
                        "adresse":   ville,
                        "prix":      prix_num,
                        "surface":   surf_num,
                        "description": f"{texte_titre}\nParticulier — {ville}\n{texte_desc}",
                        "ville":     ville,
                    })
            except Exception:
                continue

    except Exception as e:
        print(f"  ⚠️  PAP {ville}: {e}")

    return annonces


# ═══════════════════════════════════════════════════════════════════
# ANALYSE IA — CLAUDE API
# ═══════════════════════════════════════════════════════════════════

def analyser_avec_claude(annonce: dict, region: str) -> dict | None:
    """
    Analyse une annonce avec Claude et retourne le scoring MDB complet.
    Inclut division parcellaire et découpe immeuble.
    """
    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

    prompt = f"""Tu es un expert marchand de biens en France couvrant 3 régions dans un rayon de 40km :
- Île-de-France étendue (Paris, Versailles, Melun, Meaux, Évry, Cergy, Mantes, Dammarie-les-Lys…)
- Grand Lyon (Lyon, Villeurbanne, Bron, Caluire, Vienne, Bourgoin-Jallieu, L'Isle-d'Abeau…)
- Grand Avignon (Avignon, Carpentras, Orange, Apt, Cavaillon, L'Isle-sur-la-Sorgue, Uzès, Gordes, Lourmarin…)

OBJECTIF PRINCIPAL : évaluer le potentiel de DIVISION PARCELLAIRE (terrain divisible en lots constructibles) ET de division immobilière (découpe d'immeuble en lots).

Analyse cette annonce et retourne UNIQUEMENT un JSON valide, sans markdown :
{{
  "type": "<Immeuble|Maison|Appartement|Mixte|Rural|Terrain>",
  "region": "<idf|lyon|avignon>",
  "adresse": "<adresse complète extraite>",
  "prix": <prix annoncé en euros>,
  "surface": <surface habitable en m² ou 0>,
  "surface_terrain": <surface terrain en m² ou 0>,
  "annee": <année construction ou 0>,
  "anciennete": <estimation jours en ligne ou 30>,
  "dpe": "<A|B|C|D|E|F|G|NC>",
  "etat": "<excellent|bon|moyen|mauvais>",
  "retour_vente": <true si compromis caduc ou retour sur marché>,
  "agence": "<nom agence ou Particulier>",

  "score_decoupe": <0-100 potentiel division immeuble en lots>,
  "score_parcellaire": <0-100 potentiel division terrain en parcelles>,
  "score_marge": <0-100 rentabilité de l'opération>,
  "score_nego": <0-100 potentiel de négociation>,
  "score_total": <0-100 score global pondéré>,

  "decoupes_possibles": <0-8 nombre de lots immobiliers>,
  "parcelles_possibles": <0-6 nombre de parcelles terrain>,
  "surface_parcelle_min": <m² minimum par parcelle selon PLU estimé ou 0>,
  "type_division": "<immeuble|parcellaire|les_deux|aucun>",

  "plu": "<zonage PLU estimé et règles clés>",
  "revente": <prix de revente global estimé après optimisation>,
  "revente_par_parcelle": <prix moyen par parcelle ou 0>,

  "urgence": "<faible|moyenne|haute|critique>",
  "recommandation": "<2-3 phrases d'action concrète pour un MDB>",
  "signaux": ["<signal1>", "<signal2>", "<signal3>"],
  "risques": ["<risque1>", "<risque2>"],
  "opportunites": ["<opp1>", "<opp2>"],
  "specificite_region": "<particularité marché local : prix m², tendance DVF>"
}}

Critères division parcellaire :
- Terrain > 800m² → potentiel
- Zone U ou AU → constructible
- Zone A ou N → division interdite
- Avignon/Gard : détecter zones AOC (Côtes du Rhône, Luberon, Ventoux) → division viticole interdite
- Grand Lyon : noter zones naturelles Rhône (N) non constructibles

Annonce à analyser (région: {region}) :
Source : {annonce['source']}
URL : {annonce.get('url', '—')}
Ville : {annonce['ville']}
Description : {annonce['description'][:2000]}
Prix observé : {annonce['prix']:,} €
Surface observée : {annonce['surface']} m²
"""

    try:
        # Ajout instruction JSON pur dans le prompt
        prompt_json = prompt + "\n\nIMPORTANT: Reponds UNIQUEMENT avec un objet JSON valide, sans markdown, sans texte avant ou apres. Commence directement par { et termine par }."
        message = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1500,
            messages=[{"role": "user", "content": prompt_json}]
        )
        txt = message.content[0].text.strip()
        txt = re.sub(r"```json|```", "", txt).strip()
        # Extrait le JSON entre la premiere { et la derniere }
        first_brace = txt.find("{")
        last_brace = txt.rfind("}")
        if first_brace >= 0 and last_brace > first_brace:
            txt = txt[first_brace:last_brace+1]
        try:
            result = json.loads(txt)
        except json.JSONDecodeError as je:
            print(f"  Debug JSON brut: {txt[:300]}")
            raise je

        # Enrichir avec les calculs MDB
        prix    = result.get("prix", annonce["prix"]) or annonce["prix"]
        surface = result.get("surface", annonce["surface"]) or annonce["surface"]
        revente = result.get("revente", 0) or prix * 1.4
        travaux = calc_travaux(surface, result.get("type", "Immeuble"))
        offre   = calc_offre(prix, revente, travaux,
                             result.get("anciennete", 30),
                             result.get("retour_vente", False))
        fn      = calc_frais_notaire_mdb(prix)

        result.update({
            "id":              f"auto_{annonce['source'].lower()}_{hash(annonce['url']) % 99999}",
            "source":          annonce["source"],
            "url":             annonce.get("url", ""),
            "travaux":         travaux,
            "frais_notaire":   fn["total"],
            "economie_notaire":fn["economie"],
            "offre_min":       offre["min"],
            "offre_cible":     offre["cible"],
            "offre_max":       offre["max"],
            "marge_nette":     offre["marge_nette"],
            "taux_marge":      offre["taux_marge_pct"],
            "timestamp":       datetime.now().isoformat(),
        })

        return result

    except (json.JSONDecodeError, KeyError, anthropic.APIError) as e:
        print(f"  ⚠️  Analyse Claude échouée pour {annonce.get('url','?')}: {e}")
        return None


# ═══════════════════════════════════════════════════════════════════
# FILTRAGE — CRITÈRES MDB
# ═══════════════════════════════════════════════════════════════════

def filtrer_annonces(annonces: list[dict]) -> list[dict]:
    """Garde uniquement les annonces qui passent les critères MDB"""
    retenues = []
    for a in annonces:
        if not a:
            continue
        # Filtre score
        score_ok = (
            a.get("score_decoupe", 0)     >= CONFIG["score_decoupe_min"] or
            a.get("score_parcellaire", 0) >= CONFIG["score_parcellaire_min"]
        ) and a.get("score_total", 0) >= CONFIG["score_total_min"]

        # Filtre prix
        prix_ok = 0 < a.get("prix", 0) <= CONFIG["prix_max"]

        # Filtre surface
        surf_ok = a.get("surface", 0) >= CONFIG["surface_min_m2"]

        # Filtre marge
        marge_ok = a.get("taux_marge", 0) >= CONFIG["taux_marge_min_pct"]

        # Filtre type
        type_ok = a.get("type", "Autre") in CONFIG["types_biens"]

        if score_ok and prix_ok and surf_ok and type_ok:
            retenues.append(a)

    # Trier par score total décroissant
    retenues.sort(key=lambda x: x.get("score_total", 0), reverse=True)
    return retenues[:CONFIG["max_digest"]]


# ═══════════════════════════════════════════════════════════════════
# PIPELINE PRINCIPAL
# ═══════════════════════════════════════════════════════════════════

async def lancer_pipeline(test_mode: bool = False) -> list[dict]:
    """Pipeline complet : scraping → analyse IA → filtrage"""
    debut = datetime.now()
    print(f"\n{'='*60}")
    print(f"  ImmoPotentiel — Pipeline démarré à {debut.strftime('%H:%M:%S')}")
    print(f"  Régions : IDF + Grand Lyon + Grand Avignon (rayon 40km)")
    print(f"{'='*60}\n")

    toutes_annonces_brutes = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"]
        )
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 720},
        )

        for region, villes in CONFIG["zones"].items():
            print(f"📍 Région : {region.upper()} ({len(villes)} zones)")

            # En mode test, limiter à 2 villes par région
            villes_scan = villes[:2] if test_mode else villes

            for ville in villes_scan:
                print(f"  🔍 {ville}...")
                page = await context.new_page()

                try:
                    # Scraper les 3 sources
                    sl  = await scrape_seloger(ville, page)
                    lbc = await scrape_leboncoin(ville, page)
                    pap = await scrape_pap(ville, page)

                    brutes = sl + lbc + pap
                    print(f"      → {len(sl)} SeLoger · {len(lbc)} LBC · {len(pap)} PAP")

                    for a in brutes:
                        a["region"] = region
                        a["ville"]  = ville
                    toutes_annonces_brutes.extend(brutes)

                except Exception as e:
                    print(f"      ⚠️  Erreur scraping {ville}: {e}")
                finally:
                    await page.close()

                # Pause entre villes pour éviter le ban
                await asyncio.sleep(2 if test_mode else 4)

        await browser.close()

    print(f"\n📊 Total brut : {len(toutes_annonces_brutes)} annonces collectées")
    print(f"   Filtre prix/surface → analyse IA en cours...\n")

    # Déduplication par URL
    seen_urls = set()
    brutes_uniques = []
    for a in toutes_annonces_brutes:
        url = a.get("url", "")
        if url and url not in seen_urls:
            seen_urls.add(url)
            brutes_uniques.append(a)

    print(f"   Après déduplication : {len(brutes_uniques)} annonces uniques")

    # Analyse IA (avec limite en mode test)
    a_analyser = brutes_uniques[:10] if test_mode else brutes_uniques
    resultats  = []
    for i, annonce in enumerate(a_analyser):
        print(f"  🤖 [{i+1}/{len(a_analyser)}] Analyse : {annonce.get('adresse','?')} ({annonce['source']})")
        result = analyser_avec_claude(annonce, annonce["region"])
        if result:
            resultats.append(result)
        time.sleep(1)  # Rate limiting Claude API

    # Filtrage final
    retenues = filtrer_annonces(resultats)

    duree = (datetime.now() - debut).seconds
    print(f"\n✅ Pipeline terminé en {duree}s")
    print(f"   {len(brutes_uniques)} annonces analysées → {len(retenues)} retenues")

    return retenues


# ═══════════════════════════════════════════════════════════════════
# NOTIFICATIONS PUSH — ONESIGNAL
# ═══════════════════════════════════════════════════════════════════

def envoyer_push(annonces: list[dict]) -> None:
    """Envoie une notification push via OneSignal"""
    app_id  = os.getenv("ONESIGNAL_APP_ID")
    api_key = os.getenv("ONESIGNAL_API_KEY")

    if not app_id or not api_key:
        print("⚠️  Push non configuré (ONESIGNAL_APP_ID / ONESIGNAL_API_KEY manquants)")
        return

    alertes  = [a for a in annonces if a.get("retour_vente") or a.get("score_total", 0) >= 90]
    nb_total = len(annonces)

    # Push prioritaire si alerte critique
    if alertes:
        a = alertes[0]
        titre   = f"⚡ ALERTE MDB — Score {a['score_total']}/100"
        message = (f"{a.get('adresse','Bien')} · "
                   f"Offre cible {a.get('offre_cible',0):,}€ · "
                   f"Marge +{a.get('marge_nette',0):,}€")
    else:
        titre   = f"🛰️ ImmoPotentiel — {nb_total} opportunités ce matin"
        a0      = annonces[0] if annonces else {}
        message = (f"Meilleur score : {a0.get('score_total',0)}/100 · "
                   f"Offre cible {a0.get('offre_cible',0):,}€")

    payload = {
        "app_id":            app_id,
        "included_segments": ["All"],
        "headings":          {"fr": titre,   "en": titre},
        "contents":          {"fr": message, "en": message},
        "data":              {"nb_annonces": nb_total, "nb_alertes": len(alertes)},
        "priority":          10 if alertes else 5,
    }

    try:
        r = requests.post(
            "https://onesignal.com/api/v1/notifications",
            headers={"Authorization": f"Basic {api_key}",
                     "Content-Type":  "application/json"},
            json=payload, timeout=10,
        )
        if r.status_code == 200:
            print(f"📱 Push envoyé : {titre}")
        else:
            print(f"⚠️  Push échoué ({r.status_code}): {r.text[:200]}")
    except requests.RequestException as e:
        print(f"⚠️  Push erreur réseau: {e}")


# ═══════════════════════════════════════════════════════════════════
# EMAIL DIGEST — HTML
# ═══════════════════════════════════════════════════════════════════

def generer_html_email(annonces: list[dict]) -> str:
    """Génère le HTML du digest email"""
    date_str = datetime.now().strftime("%A %d %B %Y à %H:%M")

    def badge_region(r):
        return {"idf":"🗼 Paris","lyon":"🦁 Lyon","avignon":"🌻 Avignon"}.get(r,"📍")

    def couleur_score(s):
        return "#00d49a" if s >= 85 else "#f0a820" if s >= 70 else "#ff4560"

    rows = ""
    for i, a in enumerate(annonces[:CONFIG["max_digest"]]):
        url_link = ('<a href="' + (a.get("url") or "#") + '" style="display:inline-block;margin-top:8px;font-size:11px;color:#2cb4f5;">Voir annonce</a>') if a.get("url") else ""
        c_score    = couleur_score(a.get("score_total", 0))
        c_parcell  = couleur_score(a.get("score_parcellaire", 0))
        retour_tag = (
            '<span style="background:#ff4560;color:white;padding:2px 8px;'
            'border-radius:12px;font-size:11px;font-weight:bold;">⚡ RETOUR VENTE</span> '
            if a.get("retour_vente") else ""
        )
        parcell_tag = (
            f'<span style="background:#ff8c42;color:white;padding:2px 8px;'
            f'border-radius:12px;font-size:11px;font-weight:bold;">'
            f'🏗️ {a.get("parcelles_possibles",0)} PARCELLES</span> '
            if a.get("parcelles_possibles", 0) > 0 else ""
        )
        sep = ' - '
        surface_terrain = a.get('surface_terrain', 0)
        terrain_str = f' + terrain {surface_terrain:,}m²' if surface_terrain > 0 else ''
        signaux_str = sep.join(a.get('signaux', [])[:3])

        rows += f"""
<tr style="border-bottom:1px solid #1e2d40;">
  <td style="padding:18px 12px;vertical-align:top;">
    <div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:10px;">
      <div>
        <div style="margin-bottom:6px;">{retour_tag}{parcell_tag}</div>
        <div style="font-size:13px;font-weight:700;color:#dce8f5;margin-bottom:3px;">
          {badge_region(a.get('region',''))} {a.get('adresse','—')}
        </div>
        <div style="font-size:11px;color:#3d5470;">
          {a.get('type','—')} · {a.get('surface',0)}m²
          {terrain_str}
          · {a.get('source','—')} · {a.get('anciennete',0)}j en ligne
        </div>
      </div>
      <div style="text-align:center;flex-shrink:0;margin-left:12px;">
        <div style="font-size:24px;font-weight:800;color:{c_score};">{a.get('score_total',0)}</div>
        <div style="font-size:9px;color:#3d5470;">/100</div>
      </div>
    </div>

    <!-- KPIs -->
    <table width="100%" cellpadding="0" cellspacing="4" style="margin-bottom:10px;">
      <tr>
        <td style="background:#0c1220;border-radius:8px;padding:8px;text-align:center;">
          <div style="font-size:13px;font-weight:700;color:#dce8f5;">{a.get('prix',0):,}€</div>
          <div style="font-size:9px;color:#3d5470;">Prix affiché</div>
        </td>
        <td style="background:#0c1220;border-radius:8px;padding:8px;text-align:center;">
          <div style="font-size:14px;font-weight:700;color:#00d49a;">{a.get('offre_cible',0):,}€</div>
          <div style="font-size:9px;color:#3d5470;">🎯 Offre cible</div>
        </td>
        <td style="background:#0c1220;border-radius:8px;padding:8px;text-align:center;">
          <div style="font-size:13px;font-weight:700;color:#1adb7a;">+{a.get('marge_nette',0):,}€</div>
          <div style="font-size:9px;color:#3d5470;">Marge nette</div>
        </td>
        <td style="background:#0c1220;border-radius:8px;padding:8px;text-align:center;">
          <div style="font-size:13px;font-weight:700;color:#00d49a;">{a.get('frais_notaire',0):,}€</div>
          <div style="font-size:9px;color:#3d5470;">Frais notaire MDB</div>
        </td>
      </tr>
    </table>

    <!-- Scores -->
    <table width="100%" cellpadding="0" cellspacing="4" style="margin-bottom:10px;">
      <tr>
        <td>
          <div style="font-size:9px;color:#3d5470;margin-bottom:3px;">
            ✂️ Découpe : <b style="color:#00d49a">{a.get('score_decoupe',0)}</b>
          </div>
          <div style="background:#0e1826;border-radius:2px;height:4px;">
            <div style="width:{a.get('score_decoupe',0)}%;height:100%;background:#00d49a;border-radius:2px;"></div>
          </div>
        </td>
        <td width="8"></td>
        <td>
          <div style="font-size:9px;color:#3d5470;margin-bottom:3px;">
            🏗️ Parcellaire : <b style="color:{c_parcell}">{a.get('score_parcellaire',0)}</b>
          </div>
          <div style="background:#0e1826;border-radius:2px;height:4px;">
            <div style="width:{a.get('score_parcellaire',0)}%;height:100%;background:{c_parcell};border-radius:2px;"></div>
          </div>
        </td>
        <td width="8"></td>
        <td>
          <div style="font-size:9px;color:#3d5470;margin-bottom:3px;">
            💰 Marge : <b style="color:#f0a820">{a.get('score_marge',0)}</b>
          </div>
          <div style="background:#0e1826;border-radius:2px;height:4px;">
            <div style="width:{a.get('score_marge',0)}%;height:100%;background:#f0a820;border-radius:2px;"></div>
          </div>
        </td>
      </tr>
    </table>

    <!-- Recommandation IA -->
    <div style="background:#0a1f18;border-left:3px solid #00d49a;padding:8px 12px;
                border-radius:0 8px 8px 0;font-size:11px;color:#b0cce0;line-height:1.7;margin-bottom:8px;">
      🤖 {a.get('recommandation','—')}
    </div>

    <!-- Signaux -->
    <div style="font-size:10px;color:#3d5470;">
      {sep.join(a.get('signaux',[])[:3])}
    </div>

    <!-- Lien -->
    {url_link}
  </td>
</tr>"""

    html = f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#04060c;font-family:'DM Sans',Arial,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0">
    <tr><td align="center" style="padding:20px 10px;">
      <table width="600" cellpadding="0" cellspacing="0" style="max-width:600px;width:100%;">

        <!-- HEADER -->
        <tr><td style="background:linear-gradient(135deg,#0c1220,#080c16);
                        border:1px solid #152032;border-radius:16px 16px 0 0;padding:24px;">
          <div style="font-size:22px;font-weight:800;color:#dce8f5;margin-bottom:4px;">
            🛰️ Immo<span style="color:#00d49a;">Potentiel</span>
          </div>
          <div style="font-size:11px;color:#3d5470;letter-spacing:2px;text-transform:uppercase;">
            Digest automatique — {date_str}
          </div>
          <div style="margin-top:12px;display:flex;gap:10px;">
            <span style="background:#00d49a18;border:1px solid #00d49a40;color:#00d49a;
                         padding:3px 10px;border-radius:20px;font-size:11px;font-weight:700;">
              {len(annonces)} opportunités retenues
            </span>
            <span style="background:#3d547018;border:1px solid #3d547040;color:#3d5470;
                         padding:3px 10px;border-radius:20px;font-size:11px;">
              🗼 Paris · 🦁 Lyon · 🌻 Avignon · rayon 40km
            </span>
          </div>
        </td></tr>

        <!-- FRAIS NOTAIRE RAPPEL -->
        <tr><td style="background:#0a1f18;border:1px solid #00d49a30;border-top:none;padding:10px 24px;">
          <div style="font-size:11px;color:#00d49a;">
            🏛️ Tous les frais de notaire calculés en régime MDB (art. 1594F quinquies CGI — 0,715% au lieu de 5,80%)
          </div>
        </td></tr>

        <!-- ANNONCES -->
        <tr><td style="background:#080c16;border:1px solid #152032;border-top:none;border-bottom:none;">
          <table width="100%" cellpadding="0" cellspacing="0">
            {rows}
          </table>
        </td></tr>

        <!-- FOOTER -->
        <tr><td style="background:#080c16;border:1px solid #152032;
                        border-radius:0 0 16px 16px;padding:16px 24px;text-align:center;">
          <div style="font-size:10px;color:#3d5470;line-height:1.7;">
            ImmoPotentiel — Pipeline automatique quotidien<br>
            Prochaine extraction demain à {CONFIG['heure_extraction']}<br>
            <a href="#" style="color:#3d5470;">Se désabonner</a>
          </div>
        </td></tr>

      </table>
    </td></tr>
  </table>
</body>
</html>"""

    return html


def envoyer_email(annonces: list[dict]) -> None:
    """Envoie le digest par email"""
    smtp_host = os.getenv("SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_user = os.getenv("SMTP_USER", "")
    smtp_pass = os.getenv("SMTP_PASS", "")
    email_dest = os.getenv("EMAIL_DEST", smtp_user)

    if not smtp_user or not smtp_pass:
        print("⚠️  Email non configuré (SMTP_USER / SMTP_PASS manquants)")
        return

    date_str = datetime.now().strftime("%d/%m/%Y")
    nb_alertes = len([a for a in annonces if a.get("retour_vente") or a.get("score_total", 0) >= 90])

    msg = MIMEMultipart("alternative")
    msg["Subject"] = (
        f"{'⚡ ALERTE — ' if nb_alertes else ''}ImmoPotentiel · "
        f"{len(annonces)} opportunités MDB · {date_str}"
    )
    msg["From"] = smtp_user
    msg["To"]   = email_dest

    html = generer_html_email(annonces)
    msg.attach(MIMEText(html, "html", "utf-8"))

    try:
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.sendmail(smtp_user, email_dest, msg.as_string())
        print(f"📧 Email envoyé à {email_dest} ({len(annonces)} annonces)")
    except smtplib.SMTPException as e:
        print(f"⚠️  Email erreur: {e}")


# ═══════════════════════════════════════════════════════════════════
# SAUVEGARDE JSON — pour l'app React
# ═══════════════════════════════════════════════════════════════════

def sauvegarder_digest(annonces: list[dict]) -> None:
    """
    Sauvegarde le digest en JSON localement ET l'uploade vers JSONBin.io
    pour que l'app React puisse le lire en temps réel.

    Variable d'environnement requise :
      JSONBIN_KEY=votre-master-key-jsonbin
      JSONBIN_BIN_ID=votre-bin-id (créé automatiquement au 1er run)
    """
    digest = {
        "date":       datetime.now().isoformat(),
        "nb_total":   len(annonces),
        "nb_alertes": len([a for a in annonces if a.get("retour_vente")]),
        "regions":    {
            "idf":     len([a for a in annonces if a.get("region") == "idf"]),
            "lyon":    len([a for a in annonces if a.get("region") == "lyon"]),
            "avignon": len([a for a in annonces if a.get("region") == "avignon"]),
        },
        "annonces": annonces,
    }

    # 1. Sauvegarde locale
    path = "digest_latest.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(digest, f, ensure_ascii=False, indent=2)
    print(f"💾 Digest sauvegardé localement → {path}")

    # 2. Upload vers JSONBin.io
    jsonbin_key    = os.getenv("JSONBIN_KEY", "")
    jsonbin_bin_id = os.getenv("JSONBIN_BIN_ID", "")

    if not jsonbin_key:
        print("⚠️  JSONBIN_KEY manquant — digest non uploadé en ligne")
        return

    headers = {
        "Content-Type":  "application/json",
        "X-Master-Key":  jsonbin_key,
        "X-Bin-Private": "false",   # public = lisible par l'app sans auth
    }

    try:
        if jsonbin_bin_id:
            # Mettre à jour le bin existant
            r = requests.put(
                f"https://api.jsonbin.io/v3/b/{jsonbin_bin_id}",
                headers=headers,
                json=digest,
                timeout=15,
            )
            if r.status_code == 200:
                url = f"https://api.jsonbin.io/v3/b/{jsonbin_bin_id}/latest"
                print(f"✅ Digest uploadé → {url}")
            else:
                print(f"⚠️  JSONBin update échoué ({r.status_code}): {r.text[:200]}")
        else:
            # Créer un nouveau bin au 1er run
            headers["X-Bin-Name"] = "immopotentiel-digest"
            r = requests.post(
                "https://api.jsonbin.io/v3/b",
                headers=headers,
                json=digest,
                timeout=15,
            )
            if r.status_code == 200:
                data       = r.json()
                bin_id     = data["metadata"]["id"]
                url        = f"https://api.jsonbin.io/v3/b/{bin_id}/latest"
                print(f"✅ Nouveau bin créé → {url}")
                print(f"   ⚠️  Ajoutez JSONBIN_BIN_ID={bin_id} dans vos variables Railway !")
                # Sauvegarder le bin_id localement pour info
                with open("jsonbin_id.txt", "w") as f:
                    f.write(bin_id)
            else:
                print(f"⚠️  JSONBin création échouée ({r.status_code}): {r.text[:200]}")

    except requests.RequestException as e:
        print(f"⚠️  JSONBin erreur réseau: {e}")


# ═══════════════════════════════════════════════════════════════════
# EXÉCUTION COMPLÈTE
# ═══════════════════════════════════════════════════════════════════

def run(test_mode: bool = False):
    """Lance le pipeline complet"""
    print(f"\n🚀 Démarrage ImmoPotentiel Pipeline — {'MODE TEST' if test_mode else 'PRODUCTION'}")

    # 1. Pipeline scraping + IA
    annonces = asyncio.run(lancer_pipeline(test_mode=test_mode))

    if not annonces:
        print("⚠️  Aucune annonce retenue. Vérifier les critères ou les scrapers.")
        return

    print(f"\n📊 {len(annonces)} opportunités sélectionnées :")
    for a in annonces:
        region = {"idf":"🗼","lyon":"🦁","avignon":"🌻"}.get(a.get("region",""),"📍")
        print(f"  {region} [{a.get('score_total',0)}/100] {a.get('adresse','?')} "
              f"— Offre {a.get('offre_cible',0):,}€ — Marge +{a.get('marge_nette',0):,}€")

    # 2. Notifications (sauf en mode test)
    if not test_mode:
        envoyer_push(annonces)
        envoyer_email(annonces)
    else:
        print("\n[TEST] Push et email non envoyés en mode test")
        print("[TEST] Exemple email HTML généré dans email_preview.html")
        with open("email_preview.html", "w") as f:
            f.write(generer_html_email(annonces))

    # 3. Sauvegarde JSON
    sauvegarder_digest(annonces)

    print(f"\n✅ Pipeline terminé — Prochaine extraction à {CONFIG['heure_extraction']}\n")


# ═══════════════════════════════════════════════════════════════════
# SCHEDULER QUOTIDIEN
# ═══════════════════════════════════════════════════════════════════

def lancer_scheduler():
    """Lance le pipeline tous les jours à l'heure configurée"""
    heure = CONFIG["heure_extraction"]
    print(f"⏰ Scheduler démarré — Pipeline quotidien à {heure}")
    print(f"   Régions : Paris + Lyon + Avignon · rayon 40km")
    print(f"   Critères : Score découpe ≥ {CONFIG['score_decoupe_min']} "
          f"| Taux marge ≥ {CONFIG['taux_marge_min_pct']}%")
    print(f"   Ctrl+C pour arrêter\n")

    schedule.every().day.at(heure).do(run)

    # Lancer immédiatement une première fois au démarrage
    print("🔄 Premier run immédiat au démarrage...")
    run()

    while True:
        schedule.run_pending()
        time.sleep(60)


# ═══════════════════════════════════════════════════════════════════
# POINT D'ENTRÉE
# ═══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys

    args = sys.argv[1:]

    if "--schedule" in args:
        lancer_scheduler()
    elif "--test" in args:
        run(test_mode=True)
    else:
        run(test_mode=False)
