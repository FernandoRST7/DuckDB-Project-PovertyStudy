import pandas as pd

# Carregar o CSV
df = pd.read_csv('processing/poverty_inequality/Poverty_Inequality_filtered.csv')


## REGION
# Manter apenas as colunas 'region_code' e 'country_name'
df_region = df[['region_code', 'region_name']]
df_region.drop_duplicates(subset=['region_code'], inplace=True)  # Remover duplicatas por 'country_code'
df_region.to_csv('processing/poverty_inequality/entities_csv/Region.csv', index=False)


## COUNTRY
df_country = df[['region_code', 'country_name', 'country_code']]  # Para a tabela 'Country'
df_country.drop_duplicates(subset=['country_code'], inplace=True)  # Remover duplicatas por 'country_code'
with open('processing/poverty_inequality/entities_csv/Country.csv', 'w') as f1:
    df_country.to_csv(f1, index=False)


## SURVEY
df_survey = df[['country_code', 'welfare_type', 'survey_acronym', 'survey_comparability', 
                 'comparable_spell', 'poverty_line', 'headcount', 'poverty_gap', 
                 'poverty_severity', 'gini', 'reporting_pop', 'reporting_pce', 
                 'distribution_type', 'spl', 'survey_year', 'survey_coverage', 'reporting_level']]  # Para a tabela 'Survey'
with open('processing/poverty_inequality/entities_csv/Survey.csv', 'w') as f2:
    df_survey.to_csv(f2, index=False)

## DECILE
df_decile = df[['country_code', 'survey_year', 'survey_acronym', 'survey_coverage', 'reporting_level', 'decile1', 'decile2', 'decile3', 'decile4', 
               'decile5', 'decile6', 'decile7', 'decile8', 'decile9', 
               'decile10']].copy()  # Usar .copy() para evitar SettingWithCopyWarning


# Transformar para formato long usando melt()
df_long = df_decile.melt(
    id_vars=["country_code", "survey_year", "survey_acronym", "survey_coverage", "reporting_level"],  # Colunas que NÃO serão transformadas
    var_name="name",                     # Nome da coluna que armazenará os deciles
    value_name="value"                   # Nome da coluna que armazenará os valores
)

# Extrair o número do decile (usando a coluna "name")
df_long["decile_num"] = df_long["name"].str.extract(r'(\d+)').astype(int)

# Ordenar pelo número do decile
df_long = df_long.sort_values(by="decile_num")

# Selecionar e renomear as colunas para o formato final
df_long = df_long[["country_code", "survey_year", "survey_acronym", "survey_coverage", "reporting_level", "name", "value"]]

# Salvar o novo CSV (formato long)
with open('processing/poverty_inequality/entities_csv/Decile.csv', 'w') as f2:
    df_long.to_csv(f2, index=False)