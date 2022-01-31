-- a
SELECT event.date, mentions.language, event.actiongeocountrycode, COUNT(*) as count FROM event, mentions WHERE event.globaleventid = mentions.globaleventid GROUP BY event.date, mentions.language, event.actiongeocountrycode ORDER BY count DESC;

-- b
SELECT * FROM event, (SELECT UNIQUE actioncountrycode, COUNT(*) as count FROM mentions,events WHERE events.globaleventid = mentions.globaleventid GROUP BY actioncountrycode) as mentions_count WHERE event.actioncountrycode = param GROUP BY event.date ORDER BY mentions_count.count DESC

-- c
SELECT theme, persons, location, count, AVG(tone) FROM kb, (SELECT COUNT(*) FROM mentions WHERE events.globaleventid = mentions.globaleventid) as count WHERE kb.source = param GROUP BY theme, persons, location, count

-- d
