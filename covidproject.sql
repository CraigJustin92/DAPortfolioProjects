-- Total cases vs total deaths in the U.S.: How many deaths (%) per cases.
-- Shows the likelihood of death if you contract COVID in your country
SELECT
  location,
  CAST(date AS DATE),
  CAST(total_cases AS BIGINT),
  CAST(total_deaths AS BIGINT),
  CASE
    WHEN total_cases = 0 THEN NULL  -- Handle division by zero
    ELSE ROUND((total_deaths / NULLIF(total_cases, 0))::NUMERIC, 3) * 100  -- Explicit cast to numeric as dt is REAL
  END AS death_percentage
FROM
  coviddeaths
WHERE 
  location = 'United States'
ORDER BY
  DATE;

-- Total cases vs population - The percentage of the population that got COVID (with better data types)
SELECT
  location,
  CAST(date AS DATE),
  total_cases,
  population,
  ROUND((CAST(total_cases AS DECIMAL) / CAST(population AS DECIMAL)), 5) * 100 AS contraction_percentage
FROM
  coviddeaths
WHERE 
  location = 'United States' AND 
  total_cases IS NOT NULL
  AND continent IS NOT NULL
ORDER BY
  DATE;

-- What countries have the highest infection rate, considering their population
SELECT
  location,
  MAX(total_cases) AS highest_infect_count,
  population,
  ROUND((CAST(MAX(total_cases) AS DECIMAL) / CAST(population AS DECIMAL)), 5) * 100 AS infect_percentage
FROM
  coviddeaths
WHERE 
  total_cases IS NOT NULL
GROUP BY 
  location, population
ORDER BY 
  infect_percentage DESC;

-- Countries with the highest death count per population (country)
SELECT
  location,
  MAX(total_deaths) AS max_deaths
FROM
  coviddeaths
WHERE 
  total_deaths IS NOT NULL
  AND continent IS NOT NULL
GROUP BY 
  location
ORDER BY 
  max_deaths DESC;

-- Countries with the highest death count per population (continents)
SELECT
  Location,
  MAX(total_deaths) AS max_deaths
FROM
  coviddeaths
WHERE 
  total_deaths IS NOT NULL
  AND continent IS NULL
  AND location NOT IN ('High income', 'Upper middle income', 'Lower middle income', 'Low income','World','European Union')
GROUP BY 
  location
ORDER BY 
  max_deaths DESC;

-- Global numbers
SELECT
  -- CAST(date AS DATE),
  SUM(new_deaths) AS total_deaths,
  SUM(new_cases) AS total_cases,
  CASE
    WHEN SUM(new_cases) = 0 THEN NULL
    ELSE ROUND((CAST(SUM(new_deaths) AS DECIMAL) / CAST(SUM(new_cases) AS DECIMAL)), 5) * 100
  END AS death_percentage
FROM
  coviddeaths
WHERE 
  continent IS NOT NULL
-- GROUP BY date
-- ORDER BY DATE DESC
;

-- Joining of coviddeaths and covidvaccinations with location and date
SELECT d.continent, d.location, CAST(d.date AS DATE), d.population, total_cases, new_cases,
  total_deaths, new_deaths, v.total_tests, v.new_tests, v.total_vaccinations
FROM coviddeaths AS d
JOIN covidvaccinations AS v
ON v.location = d.location AND v.date = d.date
WHERE v.continent IS NOT NULL
ORDER BY location, DATE;

-- Amount of total vaccinations per date per location by rolling the new vaccinations
SELECT
  d.continent,
  d.location,
  CAST(d.date AS DATE) AS formatted_date,
  d.population,
  v.new_vaccinations,
  SUM(v.new_vaccinations) OVER (PARTITION BY d.location ORDER BY d.location, CAST(d.date AS DATE)) AS ppl_vacs_rolling
FROM
  coviddeaths AS d
JOIN
  covidvaccinations AS v
ON
  v.location = d.location AND CAST(v.date AS DATE) = CAST(d.date AS DATE)
WHERE
  d.continent IS NOT NULL
ORDER BY
  d.location, CAST(d.date AS DATE);
  
-- Putting above table into CTE (common table expression) to find the max of the aggregated column (ppl_vacs_rolling)
WITH rolling_vacs (Continent, Location, Date, Population, New_Vacs, People_Vac_Rolling) AS
(
  SELECT
    d.continent,
    d.location,
    CAST(d.date AS DATE) AS formatted_date,
    d.population,
    v.new_vaccinations,
    SUM(v.new_vaccinations) OVER (PARTITION BY d.location ORDER BY d.location, CAST(d.date AS DATE)) AS ppl_vacs_rolling
  FROM
    coviddeaths AS d
  JOIN
    covidvaccinations AS v
  ON
    v.location = d.location AND CAST(v.date AS DATE) = CAST(d.date AS DATE)
  WHERE
    d.continent IS NOT NULL
  ORDER BY
    d.location, CAST(d.date AS DATE)
)

SELECT 
  location, MAX(People_Vac_Rolling)
FROM 
  rolling_vacs 
GROUP BY 
  location;
  
-- Percentage of ppl vaccinated using CTE from previous table
WITH rolling_vacs (Continent, Location, Date, Population, New_Vacs, People_Vac_Rolling) AS
(
  SELECT
    d.continent,
    d.location,
    CAST(d.date AS DATE) AS formatted_date,
    d.population,
    v.new_vaccinations,
    SUM(v.new_vaccinations) OVER (PARTITION BY d.location ORDER BY d.location, CAST(d.date AS DATE)) AS ppl_vacs_rolling
  FROM
    coviddeaths AS d
  JOIN
    covidvaccinations AS v
  ON
    v.location = d.location AND CAST(v.date AS DATE) = CAST(d.date AS DATE)
  WHERE
    d.continent IS NOT NULL
  ORDER BY
    d.location, CAST(d.date AS DATE)
)

SELECT 
  *, round(cast(People_Vac_Rolling as decimal) / cast(population as decimal),5) * 100
FROM 
  rolling_vacs
WHERE
  new_vacs IS NOT NULL;

--SAME INFO BUT USING TEMP TABLE INSTEAD OF CTE

DROP TABLE IF EXISTS rolling_vacs_temp;
CREATE TEMPORARY TABLE rolling_vacs_temp AS
(
  SELECT
    d.continent,
    d.location,
    CAST(d.date AS DATE) AS formatted_date,
    d.population,
    v.new_vaccinations,
    SUM(v.new_vaccinations) OVER (PARTITION BY d.location ORDER BY d.location, CAST(d.date AS DATE)) AS ppl_vacs_rolling
  FROM
    coviddeaths AS d
  JOIN
    covidvaccinations AS v
  ON
    v.location = d.location AND CAST(v.date AS DATE) = CAST(d.date AS DATE)
  WHERE
    d.continent IS NOT NULL
  ORDER BY
    d.location, CAST(d.date AS DATE)
);

SELECT 
  *, round(cast(ppl_vacs_rolling as decimal) / cast(population as decimal),5) * 100
FROM 
  rolling_vacs_temp
WHERE
  new_vaccinations IS NOT NULL;
  
-- CREATING VIEW TO STORE DATA IN LATER VIZ
CREATE VIEW totaldeathspercases as

SELECT
  -- CAST(date AS DATE),
  SUM(new_deaths) AS total_deaths,
  SUM(new_cases) AS total_cases,
  CASE
    WHEN SUM(new_cases) = 0 THEN NULL
    ELSE ROUND((CAST(SUM(new_deaths) AS DECIMAL) / CAST(SUM(new_cases) AS DECIMAL)), 5) * 100
  END AS death_percentage
FROM
  coviddeaths
WHERE 
  continent IS NOT NULL
-- GROUP BY date
-- ORDER BY DATE DESC
;