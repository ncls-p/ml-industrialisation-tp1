# Machine Learning Industrialization

## TD1: Industrialization de la pipeline data

Nous allons créer un data pipeline industrialisée. <br/>
Nous travaillons pour un vendeur de légumes, qui récupère les ventes hebdomadaires par une source externe. Nous nous intéressons, pour notre modèle, aux ventes mensuelles.

Nous allons créer une API avec les entry points:
- /post_sales/: reçoit une request POST avec data: une liste de dictionnaire {"date": ..., "vegetable": ...., "kilo_sold": ....} <br/>
Cette entrée est idempotente. On stockera ces données dans la table bronze
- /get_raw_sales/: renvoie les données brutes que nous avons reçues
- /get_monthly_sales/: renvoie les données cleanées (nom du légume standardisé. Cet entry point a l'option "remove_outliers". Si "remove_outliers=False", entraîne le modèle sur toutes les données. Si "remove_outliers=True", entraîne sur le modèle sur les données safe, qui n'ont pas été tagguées comme "unsafe" par nos algorithmes d'outlier detection.

Je fournis le code app.py, une app Flask basique avec les entry points /post_sales/, /get_raw_sales/ et /get_monthly_sales/. <br/>
Je fournis aussi le code client.py, qui va ping chacun des entry points. <br/>
Enfin, je fournis [un CSV sur ce lien](https://drive.google.com/file/d/1WJPZQEijYsfTga6il8Ls3pgjdsGhCrq0/view?usp=sharing) avec des ventes weekly de 2020 à 2023. Les noms des légumes peuvent être en anglais, français ou espagnol. Il peut y avoir une faute d'orthographe. Dans la base cleaned, ils doivent être en anglais, sans faute d'orthographe.

- Créer le code pour "post_sales" qui va stocker les données dans un CSV, est idempotent.
- Créer le code pour "get_raw_sales" qui retourne les données ingérées.
- Créer le code pour "compute_monthly_sales" qui prend des ventes weekly et les transforme en vente monthly. <br/>
Pour une semaine avec n jours sur un mois et 7-n jours sur le mois suivant, on considère que (n / 7)% des ventes étaient sur le mois précédent et ((7 - n) / 7)% sont sur le mois suivant.<br/>
Il est **fortement** suggéré de créer le test unitaire avec les cas problématiques (donnée en entrée, ce qu'on attend en sortie).
- Créer le code "tag_outlier" qui ajoute is_outlier=True si la vente est supérieure à la moyenne plus 5 fois l'écart-type. <br/>
Faut-il prendre moyenne globale ou légume par légume ? Pourquoi ?
- Créer la pipeline qui reçoit les données dans "post_sales", les écrit dans la table bronze, traduit les "vegetable" en anglais clean (table silver) et les transforme en monthly sales, avec le tag is_outlier True ou False, les écrit dans la table gold.
- Créer le code "/get_monthly_sales/" qui retourne les données gold, avec ou sans outliers
- Changer le modèle pour ne plus écrire dans un CSV, mais dans une table SQL Lite.<br/>
Si vous avez bien travaillé, le changement isolé et n'impacte pas les entry points ni la pipeline.<br/>
Si non, qu'auriez-vous dû changer dans votre implémentation pour pouvoir facilement changer de base de données ?
- Ajouter un entry point "/init_database" pour créer la database ou la vider. <br/>
- Utiliser le package "locust" pour faire un test de charge sur votre API.

Je testerai votre code en le faisant tourner dans un container, en appelant post_sales et les différents get_sales sur mes données. <br/>
Si vous êtes allés jusqu'au tables SQL, j'utiliserai "/init_database" pour ré-initialiser la base quand j'en aurais besoin.

Je testerai votre pipeline en la faisant tourner dans un Docker. Votre pipeline doit supporter:
- Ne pas inscrire des données où des champs sont faux (pas de brand_quality)
- post_data est bien idempotent pour une (year_month,vegetable)
- Inscrire toutes les données valables d'une liste. Si la liste contient des données "A: valide", "B: valide", "C: non valide", "D: valide", je dois retrouver dans la base les données A, B, et D.
- Tagger comme "outlier" les ventes à 5 écart-types de leur référence.
- Votre API doit supporter 1000 requêtes par seconde (500 post, 250 get_raw_data, 250 get_monthly_data).
- Vos tests d'intégration doivent couvrir les cas mentionnés ici.

**A rendre**: le code, avec un fichier app_csv.py où l'app fonctionne en enregistrant les données sur des CSV et app_sql.py où l'app enregistre sur une base de données sql lite<br/>
**Code à rendre à la fin du cour** (à 13h pour les IABD2, 17h15 pour les IABD1)
Envoyer à foucheta@gmail.com avec l'objet "[ESGI][ML_INDUS]TP1"