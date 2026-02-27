SELECT s.ticker, COUNT(*) AS sep_days
FROM sep s
LEFT JOIN (SELECT DISTINCT ticker FROM sf1) f ON f.ticker = s.ticker
WHERE f.ticker IS NULL
GROUP BY s.ticker
ORDER BY sep_days DESC
LIMIT 20