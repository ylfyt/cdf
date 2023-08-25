INSERT INTO
  orders (user_id, items, payment, created_at, updated_at)
VALUES
  (
    2,
    CONVERT(
      '[{"product_sku": 9101, "quantity": 10}, {"product_sku": 9102, "quantity": 20}]',
      JSON
    ),
    CONVERT(
      '{"currency": "IDR", "amount": 20000, "type": "BANK-VA", "status": "PENDING"}',
      JSON
    ),
    NOW(),
    NULL
  );

2. 
INSERT INTO
  store (id, name, user_id, created_at, updated_at)
VALUES
  (1, 'Toko Jaya Bangunan', 2, NOW(), NULL);

INSERT INTO
  store_detail (store_id, address, phone_number)
VALUES
  (1, 'Jl. Jaya, Kota Jakarta', '08123456789');

3. 
INSERT INTO
  product (id, name, description, price, store_id, category_id, created_at)
VALUES
  (9101, 'TELUR GORENG', 'INI TELUR DIGORENG', 9000, 1, 1, NOW());

INSERT INTO
  product_inventory (id, product_id, stock, created_at)
VALUES
  (1, 9101, 20, NOW());

4. 
INSERT INTO
  users (id, first_name, last_name, username, email, password, created_at)
VALUES
  (2, 'Budi', 'Andi', 'budi123', 'budi@gmail.com', '11895525418021973894889048206274', NOW());

INSERT INTO
  user_detail (user_id, address, phone_number, created_at)
VALUES
  (2, 'Jl. Jakarta, Kota Jakarta', '08123456789', NOW())


5.
UPDATE orders 
SET 
  updated_at = NOW(),
  payment = CONVERT(
    '{"currency": "IDR", "amount": 20000, "type": "BANK-VA", "status": "PAID"}',
    JSON
  )
WHERE 
  _id = '64e553fdd104247cdf7ce800'

6. 
UPDATE users
SET 
  email = 'budi123@yahoo.com',
  updated_at = NOW()
WHERE 
  id = 2

7. 
UPDATE store
SET
  name = 'Toko Baru 123',
  updated_at = NOW()
WHERE 
  id = 1;

8.
UPDATE product
SET
  price = 10000,
  updated_at = NOW()
WHERE 
  id = 9101

9.
DELETE FROM orders
WHERE 
  _id = '64e553fdd104247cdf7ce800'

10. 
DELETE FROM user_detail
WHERE
  user_id = 2

11.
DELETE FROM store_detail
WHERE
  store_id = 1

12.
DELETE FROM 
  product_inventory
WHERE
  product_id = 9101


13.
SELECT
  id, name, user_id, 
  created_at, updated_at
FROM
  store
WHERE
  id = 1

14.
SELECT
  *
FROM 
  orders

15. 
SELECT
  id, first_name, last_name, 
  username, email, created_at,
  updated_at
FROM users
WHERE
  name = 'Budi'

16. 
SELECT
  id, name, description, 
  price, store_id, 
  category_id, created_at,
  updated_at
FROM 
  product

17.
SELECT 
  * 
FROM 
  store s 
  JOIN store_detail sd 
    ON s.id = sd.store_id 
WHERE s.id = 1

SELECT
  *
FROM 
  users u
  JOIN user_detail ud 
    ON u.id = ud.user_id
  LEFT JOIN orders o
    ON u.id = o.user_id

  
18.
UPDATE karyawan
SET
  salary = 900000,
  updated_at = NOW()
WHERE
  name = 'Budi'