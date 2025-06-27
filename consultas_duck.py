import duckdb

con = duckdb.connect("poverty_study_denormalized.duckdb")

# 1. Análise de Desemprego, Desigualdade e Educação por País
# Consulta que relaciona a taxa de desemprego, o coeficiente de Gini e a taxa de matrícula no ensino
# secundário para avaliar como a educação pode influenciar a desigualdade e o desemprego em diferentes países.
print("\n--- Consulta 1: GDP vs Desemprego na América Latina ---")
result1 = con.execute("""
SELECT 
    ci.country_name,
    ROUND(AVG(ci.unemp), 2) AS avg_unemployment_rate,
    ROUND(AVG(ps.gini), 2) AS avg_gini_coefficient,
    ROUND(AVG(ci.sec_enrol), 2) AS avg_secondary_enrollment_rate,
    CASE 
        WHEN AVG(ci.sec_enrol) > 80 THEN 'High Enrollment'
        WHEN AVG(ci.sec_enrol) BETWEEN 50 AND 80 THEN 'Moderate Enrollment'
        ELSE 'Low Enrollment'
    END AS enrollment_category
FROM 
    country_indicators ci
JOIN 
    poverty_surveys ps 
    ON ci.country_code = ps.country_code 
    AND ci.year = ps.survey_year
WHERE 
    ci.sec_enrol IS NOT NULL
    AND ps.gini IS NOT NULL
GROUP BY 
    ci.country_name
ORDER BY 
    ci.country_name;
""").fetchdf() # fetchdf() retorna um DataFrame do Pandas

# Exportando para CSV
result1.to_csv('results_duck/consulta1_resultado.csv', index=False)

print(result1)

# 2. Relação entre Saúde e Educação
# Consulta que analisa a relação entre saúde e educação usando os dados mais recentes por país. Revela como
# investimentos e resultados nestas áreas se correlacionam, oferecendo insights sobre desenvolvimento humano.
# Ideal para comparações internacionais e identificação de prioridades de políticas públicas.
print("\n--- Consulta 2: Média do Gini e Linha de Pobreza por Região ---")
result2 = con.execute("""
SELECT
    country_name,
    year AS reference_year,
    expenditure AS education_expenditure,
    sec_enrol AS secondary_enrollment_rate,
    child_out_of_school,
    expenditure_health,
                      
    ROUND(expenditure_health::numeric / NULLIF(expenditure, 0)::numeric, 2) AS health_education_exp_ratio,
    ROUND((sec_enrol * life_expectancy_male / 100)::numeric, 2) AS education_health_index_male,
    ROUND((sec_enrol * life_expectancy_female / 100)::numeric, 2) AS education_health_index_female
FROM (
    SELECT *,
    ROW_NUMBER() OVER (PARTITION BY country_code ORDER BY year DESC) AS rn
    FROM country_indicators
    WHERE 
        expenditure > 0
        AND expenditure_health > 0
        AND sec_enrol BETWEEN 0 AND 100
) ranked
WHERE rn = 1
ORDER BY
    country_name;
""").fetchdf()

# Exportando para CSV
result2.to_csv('results_duck/consulta2_resultado.csv', index=False)

print(result2)


# 3. População Urbana, PIB e Migração Líquida
# Consulta que mostra, para cada país, o dado mais recente disponível sobre população urbana, PIB e taxa de migração
# líquida, destacando a taxa de urbanização e a tendência migratória (entrada, saída ou estabilidade populacional).
print("\n--- Consulta 3: Aumento do Gasto com Saúde ---")
result3 = con.execute("""
WITH latest_data AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY country_code ORDER BY year DESC) AS rn
    FROM country_indicators
    WHERE 
        urban_pop IS NOT NULL
        AND rural_pop IS NOT NULL
        AND gdp IS NOT NULL
)
SELECT 
    country_name,
    urban_pop AS urban_population,
    ROUND(gdp, 5) AS gross_domestic_product,
    ROUND(net_migration, 3) AS net_migration,
    ROUND((urban_pop / NULLIF(urban_pop + rural_pop, 0)) * 100, 5) AS urbanization_rate,
    CASE 
        WHEN net_migration > 0 THEN 'Net Influx'
        WHEN net_migration < 0 THEN 'Net Outflux'
        ELSE 'Stable Migration'
    END AS migration_trend
FROM latest_data
WHERE rn = 1
ORDER BY 
    urbanization_rate DESC, 
    gross_domestic_product DESC;
""").fetchdf()

# Exportando para CSV
result3.to_csv('results_duck/consulta3_resultado.csv', index=False)

print(result3)

# 4. Impacto da Educação e Saúde na Pobreza
# Consulta que avalia como a taxa média de matrícula no ensino primário e os gastos médios com saúde se relacionam com a
# taxa média de pobreza, criando um índice combinado de educação e saúde para analisar seu impacto na redução da pobreza.
print("\n--- Consulta 4: Razão de Desigualdade (Decil 10 vs 1) ---")
result4 = con.execute("""
SELECT 
    ci.country_name,
    ROUND(AVG(ci.prim_enrol)::numeric, 10) AS avg_primary_enrollment_rate,
    ROUND(AVG(ci.expenditure_health)::numeric, 10) AS avg_health_expenditure,
    ROUND(AVG(ps.headcount)::numeric, 10) AS avg_poverty_rate,
    ROUND((AVG(ci.prim_enrol) * AVG(ci.expenditure_health))::numeric, 10) AS education_health_index,
    CASE 
        WHEN AVG(ps.headcount) * 100 < 10 THEN 'Low Poverty'
        WHEN AVG(ps.headcount) * 100 BETWEEN 10 AND 30 THEN 'Moderate Poverty'
        ELSE 'High Poverty'
    END AS poverty_category
FROM country_indicators ci
JOIN poverty_surveys ps
    ON ci.country_code = ps.country_code
    AND ci.year = ps.survey_year
WHERE 
    ci.prim_enrol IS NOT NULL
    AND ci.expenditure_health IS NOT NULL
    AND ps.headcount IS NOT NULL
GROUP BY ci.country_name
ORDER BY 
    education_health_index DESC,
    avg_poverty_rate ASC;
""").fetchdf()

# Exportando para CSV
result4.to_csv('results_duck/consulta4_resultado.csv', index=False)

print(result4)


# 5. Desigualdade, Pobreza e Distribuição de Renda por Decil
# Consulta que, para a pesquisa mais recente de cada país, apresenta o coeficiente de Gini, a taxa de pobreza e 
# a diferença entre a participação dos 10% mais ricos e dos 10% mais pobres na renda,classificando os países
# conforme o grau de desigualdade na distribuição de renda.
print("\n--- Consulta 5: Resumo Estatístico por Região ---")
result5 = con.execute("""
SELECT 
    country_name,
    survey_year,
    ROUND(gini::numeric, 3) AS gini_coefficient,
    ROUND((headcount * 100)::numeric, 2) AS poverty_rate_percent,
    decile1 AS decile_1_income_share,
    decile10 AS decile_10_income_share,
    CASE
        WHEN decile1 IS NULL OR decile10 IS NULL THEN NULL
        WHEN (decile10 - decile1) < 0.18 THEN 'Low Decile Gap'
        WHEN (decile10 - decile1) < 0.25 THEN 'Moderate Decile Gap'
        WHEN (decile10 - decile1) < 0.35 THEN 'High Decile Gap'
        ELSE 'Very High Decile Gap'
    END AS decile_inequality_category
FROM (
    SELECT DISTINCT ON (country_code)
        *
    FROM poverty_surveys
    ORDER BY country_code, survey_year DESC
) 
ORDER BY 
    gini_coefficient DESC, 
    poverty_rate_percent DESC;
""").fetchdf()

# Exportando para CSV
result5.to_csv('results_duck/consulta5_resultado.csv', index=False)

print(result5)


con.close()