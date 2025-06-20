import pandas as pd
import csv

# Carregar o CSV
df = pd.read_csv('data_sets/Global_Indicators.csv')

# Lista correta dos Indicator Codes a filtrar
indicadores = [
    'SP.POP.0014.FE.IN', 'SP.POP.0014.MA.IN', 'SP.POP.1564.FE.IN', 'SP.POP.1564.MA.IN',
    'SP.POP.65UP.FE.IN', 'SP.POP.65UP.MA.IN', 'EN.POP.DNST', 'SP.URB.TOTL', 'SP.RUR.TOTL', 'SM.POP.NETM',
    'SP.DYN.CDRT.IN', 'SP.DYN.CBRT.IN', 'SP.DYN.LE00.FE.IN', 'SP.DYN.LE00.MA.IN', 'SL.TLF.0714.ZS',
    'SL.UEM.TOTL.NE.ZS', 'SL.EMP.VULN.ZS', 'SL.TLF.PART.ZS', 'SL.EMP.MPYR.ZS', 'SL.TLF.TOTL.IN',
    'SL.TLF.TOTL.FE.ZS', 'SE.PRM.UNER', 'SE.SEC.PROG.ZS', 'SE.XPD.TOTL.GD.ZS',
    'SE.PRE.ENRR', 'SE.PRM.ENRR', 'SE.SEC.ENRR', 'SE.TER.ENRR', 'SH.MED.BEDS.ZS', 'SH.MED.PHYS.ZS',
    'SH.XPD.CHEX.GD.ZS', 'NY.GDP.MKTP.CD', 'NY.GDP.DEFL.KD.ZG', 'GC.TAX.TOTL.GD.ZS'
]

# Filtrar diretamente pela coluna 'Indicator Code'
df_filtrado = df[df['Indicator Code'].isin(indicadores)]

df_filtrado_transformado = df_filtrado.melt(
    id_vars=['Country Name', 'Country Code', 'Indicator Name', 'Indicator Code'],
    var_name='Year',
    value_name='Value'
)

# Renomear a coluna
df_filtrado_transformado.rename(columns={'Country Name': 'country_name', 'Country Code': 'country_code'}, inplace=True)



# Salvar o resultado em Global_Indicators_filtered.csv
df_filtrado_transformado.to_csv("processing/global_indicators/Global_Indicators_filtered.csv", index=False, quoting=csv.QUOTE_ALL)

print("Arquivos filtrados com sucesso!")
