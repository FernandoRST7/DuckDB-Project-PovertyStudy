import duckdb

con = duckdb.connect("poverty_analysis.duckdb")

# --- Consulta 1: Correlação entre PIB e taxa de desemprego por região na América Latina ---
# Esta consulta seria complexa no modelo original, exigindo múltiplos JOINs. Agora é trivial.
print("\n--- Consulta 1: GDP vs Desemprego na América Latina ---")
result1 = con.execute("""
SELECT
    region_name,
    year,
    AVG(gdp) AS average_gdp,
    AVG(unemp) AS average_unemployment
FROM country_indicators
WHERE region_name = 'Latin America & Caribbean' AND year > 2010
GROUP BY region_name, year
ORDER BY year;
""").fetchdf() # fetchdf() retorna um DataFrame do Pandas
print(result1)

# --- Consulta 2: Média do Gini e da linha de pobreza por região ---
# Agregação simples na tabela desnormalizada de surveys.
print("\n--- Consulta 2: Média do Gini e Linha de Pobreza por Região ---")
result2 = con.execute("""
SELECT
    r.region_name,
    AVG(ps.gini) AS average_gini,
    AVG(ps.poverty_line) AS average_poverty_line
FROM poverty_surveys ps
JOIN country_indicators ci ON ps.country_code = ci.country_code
JOIN region r ON ci.region_code = r.region_code
GROUP BY r.region_name
ORDER BY average_gini DESC;
""").fetchdf()
print(result2)


# --- Consulta 3: Países com maior aumento no gasto com saúde em 5 anos ---
# Usa uma função de janela (LAG) para comparar um ano com o anterior.
print("\n--- Consulta 3: Aumento do Gasto com Saúde ---")
result3 = con.execute("""
WITH yearly_expenditure AS (
    SELECT
        country_name,
        year,
        health_expenditure,
        LAG(health_expenditure, 1) OVER (PARTITION BY country_name ORDER BY year) as prev_year_expenditure
    FROM country_indicators
    WHERE health_expenditure IS NOT NULL
)
SELECT
    country_name,
    year,
    health_expenditure,
    prev_year_expenditure,
    (health_expenditure - prev_year_expenditure) AS expenditure_increase
FROM yearly_expenditure
WHERE expenditure_increase IS NOT NULL
ORDER BY expenditure_increase DESC
LIMIT 10;
""").fetchdf()
print(result3)


# --- Consulta 4: Análise da desigualdade: razão entre o decil mais rico e o mais pobre ---
# Demonstra o poder da tabela pivotada. O cálculo é direto.
print("\n--- Consulta 4: Razão de Desigualdade (Decil 10 vs 1) ---")
result4 = con.execute("""
SELECT
    country_name,
    survey_year,
    poverty_line,
    (decile_10_value / decile_1_value) AS top_to_bottom_ratio
FROM poverty_surveys
WHERE decile_1_value > 0 AND top_to_bottom_ratio IS NOT NULL
ORDER BY top_to_bottom_ratio DESC
LIMIT 15;
""").fetchdf()
print(result4)

# --- Consulta 5: Usando a cláusula SUMMARIZE para obter múltiplas agregações ---
# SUMMARIZE é uma sintaxe conveniente do DuckDB para múltiplas agregações.
print("\n--- Consulta 5: Resumo Estatístico por Região ---")
result5 = con.execute("""
SUMMARIZE SELECT
    region_name,
    count(*) AS num_countries,
    AVG(gdp) AS avg_gdp,
    MEDIAN(gdp) AS median_gdp,
    MIN(labor_force_fem) AS min_female_labor_force,
    MAX(labor_force_fem) AS max_female_labor_force
FROM country_indicators
WHERE year = 2015
GROUP BY region_name;
""").fetchdf()
print(result5)


con.close()