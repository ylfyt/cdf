SELECT
  s.id,
  s.name,
  p.name AS product_name,
  p.id,
  p.price
FROM
  store s
  LEFT JOIN product p ON s.id = p.store_id
WHERE
  s.id = 1
  AND p.price < 100