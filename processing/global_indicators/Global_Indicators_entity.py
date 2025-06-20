import pandas as pd

## Carregar o CSV
df = pd.read_csv("processing/global_indicators/Global_Indicators_filtered2.csv")


# CRIA CSV DE DEMOGRAPHY
# Lista correta dos indicadores de demography
indicadores_demography = [
    'EN.POP.DNST', 'SP.URB.TOTL', 'SP.RUR.TOTL', 'SM.POP.NETM',
    'SP.DYN.CDRT.IN', 'SP.DYN.CBRT.IN'
]

# Filtrar diretamente pela coluna 'Indicator Code'
df_filtrado_demography = df[df['Indicator Code'].isin(indicadores_demography)]

# Reorganiza as colunas, tendo uma coluna para ano e uma para country_code, de forma que cada indicador fique em uma coluna e seu valor na linha correspondente
df_demography = df_filtrado_demography.pivot_table(index=['Year', 'country_code'], columns='Indicator Code', values='Value').reset_index()

# Renomear as colunas conforme solicitado
df_demography.rename(columns={
    'Year': 'year',
    'EN.POP.DNST': 'pop_density',
    'SP.URB.TOTL': 'urban_pop',
    'SP.RUR.TOTL': 'rural_pop',
    'SM.POP.NETM': 'net_migration',
    'SP.DYN.CDRT.IN': 'death_rate',
    'SP.DYN.CBRT.IN': 'birth_rate'
}, inplace=True)

## Salvar o resultado
df_demography.to_csv("processing/global_indicators/entities_csv/Demography.csv", index=False)

print("Arquivo Demography.csv gerado com sucesso!")



#CRIA CSV DE LIFE EXPECTANCY
# Filtrar os dados de life expectancy
indicadores_life_expectancy = [
    'SP.DYN.LE00.FE.IN', 'SP.DYN.LE00.MA.IN'
]

df_filtrado_life_expectancy = df[df['Indicator Code'].isin(indicadores_life_expectancy)].copy()

# Criar coluna gender com base no código do indicador
df_filtrado_life_expectancy['gender'] = df_filtrado_life_expectancy['Indicator Code'].map({
    'SP.DYN.LE00.FE.IN': 'female',
    'SP.DYN.LE00.MA.IN': 'male'
})

# Selecionar as colunas necessárias
df_life_expectancy = df_filtrado_life_expectancy[['Year', 'country_code', 'gender', 'Value']].rename(columns={
    'Year': 'year',
    'Value': 'value'
})

# Salvar o CSV
df_life_expectancy.to_csv("processing/global_indicators/entities_csv/Life_expectancy.csv", index=False)

print("Arquivo Life_expectancy.csv gerado com sucesso!")



# Filtrar os dados de Population
indicadores_population = [
    'SP.POP.0014.FE.IN', 'SP.POP.0014.MA.IN',
    'SP.POP.1564.FE.IN', 'SP.POP.1564.MA.IN',
    'SP.POP.65UP.FE.IN', 'SP.POP.65UP.MA.IN'
]

df_filtrado_population = df[df['Indicator Code'].isin(indicadores_population)].copy()

# Criar colunas pop_ages e gender com base no código do indicador
df_filtrado_population['pop_ages'] = df_filtrado_population['Indicator Code'].map({
    'SP.POP.0014.FE.IN': '0-14',
    'SP.POP.0014.MA.IN': '0-14',
    'SP.POP.1564.FE.IN': '15-64',
    'SP.POP.1564.MA.IN': '15-64',
    'SP.POP.65UP.FE.IN': '65+',
    'SP.POP.65UP.MA.IN': '65+'
})

df_filtrado_population['gender'] = df_filtrado_population['Indicator Code'].map({
    'SP.POP.0014.FE.IN': 'female',
    'SP.POP.0014.MA.IN': 'male',
    'SP.POP.1564.FE.IN': 'female',
    'SP.POP.1564.MA.IN': 'male',
    'SP.POP.65UP.FE.IN': 'female',
    'SP.POP.65UP.MA.IN': 'male'
})

# Selecionar as colunas necessárias
df_population = df_filtrado_population[['Year', 'country_code', 'pop_ages', 'gender', 'Value']].rename(columns={
    'Year': 'year',
    'Value': 'number'
})

# Salvar o CSV
df_population.to_csv("processing/global_indicators/entities_csv/Population.csv", index=False)

print("Arquivo Population.csv gerado com sucesso!")


# CRIA CSV DE EMPLOYMENT
# Lista correta dos indicadores de employment
indicadores_employment = [
    'SL.TLF.0714.ZS', 'SL.UEM.TOTL.NE.ZS', 'SL.EMP.VULN.ZS',   
    'SL.TLF.PART.ZS', 'SL.EMP.MPYR.ZS', 'SL.TLF.TOTL.IN', 'SL.TLF.TOTL.FE.ZS'
]

# Filtrar diretamente pela coluna 'Indicator Code'
df_filtrado_employment = df[df['Indicator Code'].isin(indicadores_employment)]

# Reorganiza as colunas, tendo uma coluna para ano e uma para country_code, de forma que cada indicador fique em uma coluna e seu valor na linha correspondente
df_employment = df_filtrado_employment.pivot_table(index=['Year', 'country_code'], columns='Indicator Code', values='Value').reset_index()

# Renomear as colunas conforme solicitado
df_employment.rename(columns={
    'Year': 'year',
    'SL.TLF.0714.ZS': 'child_emp',
    'SL.UEM.TOTL.NE.ZS': 'unemp',
    'SL.EMP.VULN.ZS': 'vulnerable_emp',
    'SL.TLF.PART.ZS': 'part_time',
    'SL.EMP.MPYR.ZS': 'employers',
    'SL.TLF.TOTL.IN': 'labor_force_total',
    'SL.TLF.TOTL.FE.ZS': 'labor_force_fem'
}, inplace=True)

## Salvar o resultado
df_employment.to_csv("processing/global_indicators/entities_csv/Employment.csv", index=False)

print("Arquivo Employment.csv gerado com sucesso!")



# CRIA CSV DE EDUCATION
# Lista correta dos indicadores de education
indicadores_education = [
    'SE.PRM.UNER', 'SE.SEC.PROG.ZS', 'SE.XPD.TOTL.GD.ZS', 'SE.PRE.ENRR',      
    'SE.PRM.ENRR', 'SE.SEC.ENRR', 'SE.TER.ENRR'
]

# Filtrar diretamente pela coluna 'Indicator Code'
df_filtrado_education = df[df['Indicator Code'].isin(indicadores_education)]

# Reorganiza as colunas, tendo uma coluna para ano e uma para country_code, de forma que cada indicador fique em uma coluna e seu valor na linha correspondente
df_education = df_filtrado_education.pivot_table(index=['Year', 'country_code'], columns='Indicator Code', values='Value').reset_index()

# Renomear as colunas conforme solicitado
df_education.rename(columns={
    'Year': 'year',
    'SE.PRM.UNER': 'child_out_of_school',
    'SE.SEC.PROG.ZS': 'progression_to_sec',
    'SE.XPD.TOTL.GD.ZS': 'expenditure',
    'SE.PRE.ENRR': 'preprim_enrol',
    'SE.PRM.ENRR': 'prim_enrol',
    'SE.SEC.ENRR': 'sec_enrol',
    'SE.TER.ENRR': 'terti_enrol'
}, inplace=True)


## Salvar o resultado
df_education.to_csv("processing/global_indicators/entities_csv/Education.csv", index=False)

print("Arquivo Education.csv gerado com sucesso!")



# CRIA CSV DE HEALTH
# Lista correta dos indicadores de health
indicadores_health = [
    'SH.MED.BEDS.ZS', 'SH.MED.PHYS.ZS', 'SH.XPD.CHEX.GD.ZS'
]

# Filtrar diretamente pela coluna 'Indicator Code'
df_filtrado_health = df[df['Indicator Code'].isin(indicadores_health)]

# Reorganiza as colunas, tendo uma coluna para ano e uma para country_code, de forma que cada indicador fique em uma coluna e seu valor na linha correspondente
df_health = df_filtrado_health.pivot_table(index=['Year', 'country_code'], columns='Indicator Code', values='Value').reset_index()

# Renomear as colunas conforme solicitado
df_health.rename(columns={
    'Year': 'year',
    'SH.MED.BEDS.ZS': 'hospital_beds',
    'SH.MED.PHYS.ZS': 'physicians',
    'SH.XPD.CHEX.GD.ZS': 'expenditure'
}, inplace=True)

## Salvar o resultado
df_health.to_csv("processing/global_indicators/entities_csv/Health.csv", index=False)

print("Arquivo Health.csv gerado com sucesso!")


# CRIA CSV DE ECONOMY
# Lista correta dos indicadores de economy
indicadores_economy = [
    'NY.GDP.MKTP.CD', 'NY.GDP.DEFL.KD.ZG', 'GC.TAX.TOTL.GD.ZS'
]

# Filtrar diretamente pela coluna 'Indicator Code'
df_filtrado_economy = df[df['Indicator Code'].isin(indicadores_economy)]

# Reorganiza as colunas, tendo uma coluna para ano e uma para country_code, de forma que cada indicador fique em uma coluna e seu valor na linha correspondente
df_economy = df_filtrado_economy.pivot_table(index=['Year', 'country_code'], columns='Indicator Code', values='Value').reset_index()

# Renomear as colunas conforme solicitado
df_economy.rename(columns={
    'Year': 'year',
    'NY.GDP.MKTP.CD': 'gdp',
    'NY.GDP.DEFL.KD.ZG': 'inflation',
    'GC.TAX.TOTL.GD.ZS': 'tax_revenue'
}, inplace=True)


## Salvar o resultado
df_economy.to_csv("processing/global_indicators/entities_csv/Economy.csv", index=False)

print("Arquivo Economy.csv gerado com sucesso!")