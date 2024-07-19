
SELECT "Language Preference", COUNT(*) AS count
FROM borrowers 
WHERE "Loan Type" = 'Auto Loan'
GROUP BY "Language Preference"
ORDER BY count
DESC
