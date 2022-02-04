-- Requêtes SQL de mise en forme des tables Cassandra
-- table_ab
SELECT event.event_id, pays, langue, jour, mois, annee, COUNT(*) AS total
FROM event, mentions
WHERE event.event_id = mentions.event_id
GROUP BY event_id, pays, langue, jour, mois, annee

-- table_c
SELECT source, theme, personne, lieu, jour, mois, annee, COUNT(*) as total, SUM(ton) as somme_ton
FROM gkg
GROUP BY source, theme, personne, lieu, jour, mois, annee

-- table_d
SELECT lieu, langue, jour, mois, annee, COUNT(*) as total, SUM(ton) as somme_ton
FROM gkg
GROUP BY lieu, langue, jour, mois, annee

-- Requêtes CQL de creation des tables
CREATE KEYSPACE reponses WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};

-- table_ab
CREATE TABLE table_ab (
    event_id bigint,
    mention_id bigint,
    pays text,
    langue text,
    jour bigint,
    mois int,
    annee int,
    total int,
    PRIMARY KEY ((pays), jour, mois, annee)
);

-- table_c
CREATE TABLE table_cd (
    source text,
    theme text,
    personne text,
    lieu text,
    total int,
    somme_ton float,
    jour bigint,
    mois int,
    annee int,
    PRIMARY KEY ((source), theme, personne, lieu, jour, mois, annee)
);

-- table_d
CREATE TABLE table_d (
    lieu text,
    langue text,
    total int,
    somme_ton float,
    jour bigint,
    mois int,
    annee int,
    PRIMARY KEY ((lieu, langue), jour, mois, annee)
);

-- Requêtes CQL de réponse aux questions
-- a
SELECT * FROM table_ab;

-- b
SELECT event_id, SUM(total) as compte, jour, mois, annee
FROM table_ab
WHERE pays = "input"
GROUP BY jour/mois/annee
ORDER BY compte DESC;

-- c
SELECT theme, personne, lieu, SUM(total), SUM(somme_ton)/SUM(total), jour, mois, annee
FROM table_c
WHERE source = "input"
GROUP BY theme, personne, lieu, jour, mois, annee;

-- d
SELECT langue, lieu, SUM(somme_ton)/SUM(total), jour
FROM table_d
WHERE langue = "input1", lieu = "input2"
GROUP BY jour, mois, annee;
