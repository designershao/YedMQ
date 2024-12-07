CREATE TABLE users (
  id SERIAL PRIMARY KEY,
  username VARCHAR(100) NOT NULL,
  password VARCHAR(500) NOT NULL,
  tenant VARCHAR(100) NOT NULL
);

CREATE INDEX idx_users_username_tenant ON users (username, tenant);

CREATE TABLE acls (
  id SERIAL PRIMARY KEY,
  username VARCHAR(100) NOT NULL,
  tenant VARCHAR(100) NOT NULL,
  topic VARCHAR(255) NOT NULL,
  action VARCHAR(100) NOT NULL,
  result VARCHAR(100) NOT NULL
);

CREATE INDEX idx_acls_username ON acls (username);
CREATE INDEX idx_acls_topic ON acls (topic);