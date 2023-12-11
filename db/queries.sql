-- local connection psql -h algo-01.postgres.database.azure.com -p 5432 -U postgres 

SELECT  order_id, symbol, pair, pair_order, status, spread, orig_qty, side, position_side, update_time
FROM orders
ORDER BY update_time ASC;


-- Trading pairs table
SELECT tp.*, a1.symbol AS symbol1, a2.symbol AS symbol2
FROM trading_pairs AS tp
INNER JOIN asset AS a1 ON tp.symbol_1_id = a1.id
INNER JOIN asset AS a2 ON tp.symbol_2_id = a2.id
WHERE trainset_end = '2023-11-25'; -- Latest date

-- something
SELECT pair, adf_test_stat, p_value, stationary, symbol_1, symbol_2, trainset_start, trainset_end
FROM adf_test_results
WHERE trainset_end =  '2023-11-25'
ORDER BY adf_test_stat ASC
LIMIT 25;