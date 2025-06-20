import pandas as pd
import csv

'''
Esse script filtra o CSV de Global Indicators, mantendo apenas os países que estão na lista de códigos de países do CSV de Country.'''
# abrir o csv de global indicators ja filtrado:
df = pd.read_csv("processing/global_indicators/Global_Indicators_filtered.csv")

# abrir csv de country:
df_country = pd.read_csv("processing/poverty_inequality/entities_csv/Country.csv")
# obter os codigos dos paises selecionados:
df_country = df_country[['country_code']]
# extrarir em uma lista:
country_codes = df_country['country_code'].tolist()
# excluir linhas/paises que não estão na lista de codigos de paises:
df_country = df[df['country_code'].isin(country_codes)]

# Salvar o resultado em Global_Indicators_filtered.csv
df_country.to_csv("processing/global_indicators/Global_Indicators_filtered2.csv", index=False, quoting=csv.QUOTE_ALL)
