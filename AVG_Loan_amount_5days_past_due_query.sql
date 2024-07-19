
SELECT AVG("Loan Amount") AS average_loan_amount
FROM borrowers
WHERE "Days Left to Pay Current EMI" <= -5
