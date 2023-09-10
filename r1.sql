SELECT
  p.*,
  c.name as category_name
FROM
  product p
  JOIN product_category pc ON p.id = pc.product_id
  JOIN category c ON c.id = pc.category_id
WHERE
  p.id = 2