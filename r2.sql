SELECT p.*, s.* FROM product p JOIN store s ON p.store_id = s.id WHERE p.id < 10 AND p.created_at > '2023-01-01'
