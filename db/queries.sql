SELECT  order_id, symbol, pair, pair_order, status, spread, orig_qty, side, position_side, update_time
FROM orders
ORDER BY update_time ASC;