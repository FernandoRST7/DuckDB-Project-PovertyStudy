import duckdb

con = duckdb.connect("poverty_study_denormalized.duckdb")

# --- Consulta 1: Correlação entre PIB e taxa de desemprego por região na América Latina ---
# Esta consulta seria complexa no modelo original, exigindo múltiplos JOINs. Agora é trivial.
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

# --- Consulta 2: Média do Gini e da linha de pobreza por região ---
# Agregação simples na tabela desnormalizada de surveys.
print("\n--- Consulta 2: Média do Gini e Linha de Pobreza por Região ---")
result2 = con.execute("""
SELECT 
    country_name,
    gender,
    ROUND(AVG(life_expectancy)::numeric, 2) AS avg_life_expectancy,
    ROUND(AVG(expenditure_health)::numeric, 2) AS avg_health_expenditure,
    ROUND(AVG(death_rate)::numeric, 2) AS avg_crude_death_rate,
    NTILE(4) OVER (
        PARTITION BY gender 
        ORDER BY AVG(expenditure_health) DESC
    ) AS health_expenditure_quartile,
    ROUND((AVG(life_expectancy) / NULLIF(AVG(expenditure_health), 0))::numeric, 2) 
        AS avg_life_expectancy_per_expenditure
FROM (
    SELECT 
        country_name,
        death_rate,
        expenditure_health,
        'female' AS gender,
        life_expectancy_female AS life_expectancy
    FROM country_indicators
    WHERE life_expectancy_female IS NOT NULL
    
    UNION ALL
    
    SELECT 
        country_name,
        death_rate,
        expenditure_health,
        'male' AS gender,
        life_expectancy_male AS life_expectancy
    FROM country_indicators
    WHERE life_expectancy_male IS NOT NULL
) AS unpivoted_data
WHERE 
    life_expectancy IS NOT NULL
    AND expenditure_health IS NOT NULL
    AND death_rate IS NOT NULL
    AND expenditure_health > 0
GROUP BY 
    country_name,
    gender
ORDER BY 
    country_name,
    gender,
    health_expenditure_quartile, 
    avg_life_expectancy_per_expenditure DESC, 
    avg_crude_death_rate ASC;
""").fetchdf()

# Exportando para CSV
result2.to_csv('results_duck/consulta2_resultado.csv', index=False)

print(result2)


# --- Consulta 3: Países com maior aumento no gasto com saúde em 5 anos ---
# Usa uma função de janela (LAG) para comparar um ano com o anterior.
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


# --- Consulta 4: Análise da desigualdade: razão entre o decil mais rico e o mais pobre ---
# Demonstra o poder da tabela pivotada. O cálculo é direto.
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

# --- Consulta 5: Usando a cláusula SUMMARIZE para obter múltiplas agregações ---
# SUMMARIZE é uma sintaxe conveniente do DuckDB para múltiplas agregações.
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