-- Requêtes SQL de mise en forme des tables Cassandra
-- table_ab
SELECT event_id, mention_id, country, language, event_day, event_month, event_year, COUNT(*) AS total
FROM event, mentions
WHERE event.event_id = mentions.event_id
GROUP BY event_id, mention_id, country, language, event_day, event_month, event_year

-- table_c
SELECT source, theme, person, location, day, month, year, COUNT(*) as total, SUM(tone) as sum_tone
FROM gkg
GROUP BY source, theme, person, location, day, month, year

-- table_d
SELECT location, language, day, month, year, COUNT(*) as total, SUM(tone) as sum_tone
FROM gkg
GROUP BY location, language, day, month, year

-- Requêtes CQL de creation des tables
CREATE KEYSPACE reponses WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};

-- table_ab
CREATE TABLE table_ab (
    event_id int,
    mention_id int,
    country text,
    language text,
    event_day int,
    event_month int,
    event_year int,
    total int,
    PRIMARY KEY ((country), event_day, event_month, event_year)
);

-- table_c
CREATE TABLE table_cd (
    source text,
    theme text,
    person text,
    location text,
    total int,
    sum_tone float,
    day int,
    month int,
    year int,
    PRIMARY KEY ((source), theme, person, location, day, month, year)
);

-- table_d
CREATE TABLE table_d (
    location text,
    language text,
    total int,
    sum_tone float,
    day int,
    month int,
    year int,
    PRIMARY KEY ((location, language), day, month, year)
);

-- Requêtes CQL de réponse aux questions
-- a
SELECT * FROM table_ab

-- b
SELECT event_id, SUM(total) as cnt, event_day, event_month, event_year
FROM table_ab
WHERE country = "input"
GROUP BY day/month/year
ORDER BY cnt DESC

-- c
SELECT theme, person, lieu, SUM(total), SUM(sum_tone)/SUM(total), day, month, year
FROM table_c
WHERE source = "input"
GROUP BY theme, person, lieu, day, month, year

-- d
SELECT language, location, SUM(sum_tone)/SUM(total), day
FROM table_d
WHERE language = "input1", location = "input2"
GROUP BY day, month, year
