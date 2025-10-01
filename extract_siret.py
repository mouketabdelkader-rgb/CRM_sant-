import csv

csv_file = 'nouveaux_pros_2025-09.csv'
sirets = []
with open(csv_file, 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        if row['Libellé mode exercice'] == 'Lib,indép,artis,com' and row['Identifiant technique de la structure'] and row['Identifiant technique de la structure'].strip():
            sirets.append(row['Identifiant technique de la structure'])
            if len(sirets) == 5:
                break

print("5 SIRET/FINESS libéraux (nouveaux pros) :")
for s in sirets:
    print(s)
