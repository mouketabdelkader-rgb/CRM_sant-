import pandas as pd

df = pd.read_csv('test_liberaux.csv')
dpc_ready = df[(df['Identifiant technique de la structure'].notna()) & (df['Adresse e-mail (coord. structure)'].notna())]
print('Libéraux DPC-ready (SIRET + Email):', len(dpc_ready))
print(dpc_ready[['Nom d\'exercice', 'Libellé profession', 'Adresse e-mail (coord. structure)', 'Identifiant technique de la structure']].to_string(index=False))
dpc_ready.to_csv('liberaux_dpc_ready.csv', index=False)
print('Exporté: liberaux_dpc_ready.csv')
