-- a
SELECT event.date, mentions.language, event.actiongeocountrycode, COUNT(*) as count FROM event, mentions WHERE event.globaleventid = mentions.globaleventid GROUP BY event.date, mentions.language, event.actiongeocountrycode ORDER BY count DESC;

-- b
SELECT date, globaleventid, c FROM event, (SELECT DISTINCT actiongeocountrycode, COUNT(*) as c FROM mentions,event WHERE event.globaleventid = mentions.globaleventid GROUP BY actiongeocountrycode) as mentions_count WHERE event.actiongeocountrycode = 'FR' GROUP BY event.date, event.globaleventid, event.actiongeocountrycode, mentions_count.c ORDER BY mentions_count.c DESC;

-- c
SELECT theme, persons, location, COUNT(*), AVG(score) FROM kb  WHERE kb.source = 'lemonde.fr' GROUP BY theme, persons, location

-- d

