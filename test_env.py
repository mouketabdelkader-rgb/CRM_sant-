import requests
import pandas as pd
import sqlite3

print("Python version:", __import__('sys').version)
print("Requests OK:", requests.get('https://httpbin.org/status/200').status_code)
print("Pandas version:", pd.__version__)
print("SQLite OK: Connexion test")
conn = sqlite3.connect(':memory:')
conn.close()
print("Tout est prêt !")
