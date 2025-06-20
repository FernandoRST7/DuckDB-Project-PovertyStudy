import pandas as pd
import os

print("Iniciando o script de pré-processamento para o DuckDB...")

# --- 1. Definir os caminhos dos arquivos ---
# É uma boa prática definir os caminhos em variáveis para facilitar a manutenção.
base_path_global = 'global_indicators/entities_csv/'
base_path_poverty = 'poverty_inequality/entities_csv/'

# Criar a pasta de destino se não existir
output_dir = 'duckdb_ready_data'
os.makedirs(output_dir, exist_ok=True)
print(f"Arquivos de saída serão salvos em: '{output_dir}/'")


# --- 2. Processamento para 'country_indicators.csv' ---
print("\n--- Iniciando a criação de 'country_indicators.csv' ---")

try:
    # --- Carregar todos os CSVs necessários ---
    print("Carregando arquivos CSV de indicadores...")
    df_demo = pd.read_csv(base_path_global + 'Demography.csv')
    df_econ = pd.read_csv(base_path_global + 'Economy.csv')
    df_edu = pd.read_csv(base_path_global + 'Education.csv')
    df_emp = pd.read_csv(base_path_global + 'Employment.csv')
    df_health = pd.read_csv(base_path_global + 'Health.csv')
    df_life = pd.read_csv(base_path_global + 'Life_expectancy.csv')
    df_pop = pd.read_csv(base_path_global + 'Population.csv')
    df_country = pd.read_csv(base_path_poverty + 'Country.csv')
    df_region = pd.read_csv(base_path_poverty + 'Region.csv')
    print("Arquivos carregados com sucesso.")

    # --- Pré-processamento de tabelas "longas" (Life Expectancy e Population) ---
    # Precisamos "pivotar" essas tabelas para que cada linha continue sendo única por país/ano.

    print("Pivotando tabela de Expectativa de Vida...")
    df_life_pivoted = df_life.pivot_table(
        index=['year', 'country_code'],
        columns='gender',
        values='value'
    ).reset_index().rename(columns={
        'female': 'life_expectancy_female',
        'male': 'life_expectancy_male'
    })

    print("Pivotando tabela de População...")
    # Criamos uma coluna de categoria combinada para pivotar
    df_pop['pop_category'] = 'pop_' + df_pop['pop_ages'] + '_' + df_pop['gender']
    df_pop_pivoted = df_pop.pivot_table(
        index=['year', 'country_code'],
        columns='pop_category',
        values='number'
    ).reset_index()


    # --- Unir (merge) todas as tabelas de indicadores ---
    print("Unindo as tabelas de indicadores ano a ano...")
    # Começamos com a tabela de demografia como base
    df_indicators = df_demo

    # Lista das outras tabelas para unir
    tables_to_merge = {
        "econ": df_econ,
        "edu": df_edu,
        "emp": df_emp,
        "health": df_health,
        "life": df_life_pivoted,
        "pop": df_pop_pivoted
    }

    # Loop para unir todas as tabelas usando um outer join para não perder dados
    for name, df_to_merge in tables_to_merge.items():
        print(f"Unindo dados de: {name}")
        df_indicators = pd.merge(
            df_indicators,
            df_to_merge,
            on=['year', 'country_code'],
            how='outer',
            # Adiciona sufixos se houver colunas com o mesmo nome (ex: 'expenditure')
            suffixes=(None, f'_{name}') 
        )

    # --- Unir com informações de país e região ---
    print("Unindo com informações de país e região...")
    # Primeiro, une as informações do país (nome)
    df_final_indicators = pd.merge(
        df_indicators,
        df_country[['country_code', 'country_name', 'region_code']],
        on='country_code',
        how='left'
    )

    # Depois, une as informações da região (nome da região)
    df_final_indicators = pd.merge(
        df_final_indicators,
        df_region,
        on='region_code',
        how='left'
    )
    
        # --- Reordenar colunas ---
    # Extrai as colunas que não são as 3 desejadas
    other_columns = [col for col in df_final_indicators.columns 
                    if col not in ['country_name', 'region_code', 'region_name']]
    
    # Define a nova ordem: colunas base + colunas deslocadas + demais colunas
    base_columns = ['year', 'country_code']
    new_columns = base_columns + ['country_name', 'region_code', 'region_name'] 
    new_columns += [col for col in other_columns if col not in base_columns]
    
    # Aplica a nova ordem
    df_final_indicators = df_final_indicators[new_columns]

    # --- Salvar o arquivo final ---
    output_path = os.path.join(output_dir, 'country_indicators.csv')
    df_final_indicators.to_csv(output_path, index=False)
    print(f"\nSUCESSO! Arquivo 'country_indicators.csv' criado em '{output_path}'")
    print(f"Total de Linhas: {len(df_final_indicators)}, Total de Colunas: {len(df_final_indicators.columns)}")

except FileNotFoundError as e:
    print(f"\nERRO: Arquivo não encontrado! Verifique se o caminho está correto: {e}")
except Exception as e:
    print(f"\nERRO inesperado ao processar 'country_indicators': {e}")


# --- 3. Processamento para 'poverty_surveys.csv' ---
print("\n--- Iniciando a criação de 'poverty_surveys.csv' ---")

try:
    # --- Carregar os CSVs necessários ---
    # Nota: Precisamos do Survey.csv também, para os dados principais da pesquisa.
    print("Carregando arquivos CSV de Pobreza/Desigualdade...")
    df_decile = pd.read_csv(base_path_poverty + 'Decile.csv')
    df_survey = pd.read_csv(base_path_poverty + 'Survey.csv')
    # df_country e df_region já foram carregados, vamos reutilizá-los.
    print("Arquivos carregados com sucesso.")
    
    # --- Pivotar a tabela de decís ---
    # O objetivo é transformar as linhas de decís em colunas.
    # A chave que identifica unicamente uma pesquisa nos seus dados é a combinação de várias colunas.
    survey_key_cols = ['country_code', 'survey_year', 'survey_acronym', 'survey_coverage', 'reporting_level']
    
    print("Pivotando a tabela de Decís...")
    df_decile_pivoted = df_decile.pivot_table(
        index=survey_key_cols,
        columns='name',
        values='value'
    ).reset_index()

    # --- Unir dados da pesquisa com os decís pivotados ---
    print("Unindo dados da pesquisa com os decís...")
    df_final_surveys = pd.merge(
        df_survey,
        df_decile_pivoted,
        on=survey_key_cols,
        how='left' # Usamos left para manter todas as pesquisas, mesmo que não tenham dados de decís.
    )

    # --- Unir com informações de país e região (similar ao passo anterior) ---
    print("Unindo com informações de país e região...")
    df_final_surveys = pd.merge(
        df_final_surveys,
        df_country[['country_code', 'country_name', 'region_code']],
        on='country_code',
        how='left'
    )
    df_final_surveys = pd.merge(
        df_final_surveys,
        df_region,
        on='region_code',
        how='left'
    )
    
        # --- Reordenar colunas ---
    # Extrai as colunas que não são as 3 desejadas
    other_columns = [col for col in df_final_surveys.columns 
                    if col not in ['country_name', 'region_code', 'region_name']]
    
    # Define a nova ordem: colunas base + colunas deslocadas + demais colunas
    base_columns = ['country_code', 'survey_year']
    new_columns = base_columns + ['country_name', 'region_code', 'region_name'] 
    new_columns += [col for col in other_columns if col not in base_columns]
    
    # Aplica a nova ordem
    df_final_surveys = df_final_surveys[new_columns]

    # --- Salvar o arquivo final ---
    output_path = os.path.join(output_dir, 'poverty_surveys.csv')
    df_final_surveys.to_csv(output_path, index=False)
    print(f"\nSUCESSO! Arquivo 'poverty_surveys.csv' criado em '{output_path}'")
    print(f"Total de Linhas: {len(df_final_surveys)}, Total de Colunas: {len(df_final_surveys.columns)}")

except FileNotFoundError as e:
    print(f"\nERRO: Arquivo não encontrado! Verifique se o caminho está correto: {e}")
except Exception as e:
    print(f"\nERRO inesperado ao processar 'poverty_surveys': {e}")

print("\nScript de pré-processamento concluído.")