
SELECT "Language Preference", COUNT(*) AS count
FROM borrowers 
WHERE "Loan Type" = 'Home Loan'
GROUP BY "Language Preference"
ORDER BY count
DESC
