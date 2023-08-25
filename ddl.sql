-- CASSANDRA
CREATE TABLE IF NOT EXISTS db_users.users (
  id int primary key,
  first_name text,
  last_name text,
  username text,
  email text,
  password text,
  created_at timestamp,
  updated_at timestamp
);

CREATE TABLE IF NOT EXISTS db_users.user_detail (
  user_id int primary key,
  first_name text,
  address text,
  phone_number text,
  created_at timestamp,
  updated_at timestamp
);