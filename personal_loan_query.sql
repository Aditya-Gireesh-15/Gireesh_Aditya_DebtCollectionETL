
SELECT "Language Preference", COUNT(*) AS count
FROM borrowers 
WHERE "Loan Type" = 'Personal Loan'
GROUP BY "Language Preference"
ORDER BY count
DESC
