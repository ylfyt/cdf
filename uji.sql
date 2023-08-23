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