import pandas as pd

# Lê o CSV
df = pd.read_csv('data_sets/Poverty_Inequality.csv')

# Converte a coluna 'reporting_pop' para inteiro
df['reporting_pop'] = df['reporting_pop'].astype(int)

# Converte a coluna 'survey_year' para inteiro
df['survey_year'] = df['survey_year'].astype(int)

# Excluindo colunas específicas
colunas_para_excluir = ['watts', 'mld', 'polarization', 'cpi', 'ppp', 'reporting_gdp',
                         'is_interpolated', 'estimation_type','pg', 'spr', 'estimate_type', 'reporting_year']

df_filtrado = df.drop(columns=colunas_para_excluir)

# Mostra o resultado
print(df_filtrado)

# Se quiser salvar:
df_filtrado.to_csv('processing/poverty_inequality/Poverty_Inequality_filtered.csv', index=False)
