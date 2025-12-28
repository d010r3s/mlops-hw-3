SELECT
    us_state,
    argMax(cat_id, amount) AS category_with_max_transaction,
    max(amount) AS max_amount
FROM demo.transactions
GROUP BY us_state
ORDER BY us_state;
