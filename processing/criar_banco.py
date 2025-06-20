import duckdb

# Conecta-se ao arquivo do banco de dados (será criado se não existir)
con = duckdb.connect('poverty_study_denormalized.duckdb')

print("Criando tabela 'country_indicators' a partir do CSV processado...")
con.execute("""
    CREATE OR REPLACE TABLE country_indicators AS 
    SELECT * FROM read_csv_auto('duckdb_ready_data/country_indicators.csv');
""")

print("Criando tabela 'poverty_surveys' a partir do CSV processado...")
con.execute("""
    CREATE OR REPLACE TABLE poverty_surveys AS 
    SELECT * FROM read_csv_auto('duckdb_ready_data/poverty_surveys.csv');
""")

print("\nBanco de dados criado com sucesso! Verificando tabelas:")
print(con.table('country_indicators').df().head())
print(con.table('poverty_surveys').df().head())

con.close()