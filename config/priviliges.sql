-- Create remote user for all teammates
CREATE USER IF NOT EXISTS 'TeamETL'@'%' IDENTIFIED BY 'TeamETL@123';

-- Grant privileges
GRANT ALL PRIVILEGES ON stagging.* TO 'TeamETL'@'%';
GRANT ALL PRIVILEGES ON transformed.* TO 'TeamETL'@'%';

-- Apply changes
FLUSH PRIVILEGES;

-- Verify user exists
SELECT user, host FROM mysql.user WHERE user='TeamETL';

SELECT user, host, plugin FROM mysql.user WHERE user='TeamETL';

SELECT user, host, authentication_string FROM mysql.user WHERE user = 'TeamETL';

-- Check connection limits
SHOW VARIABLES LIKE 'max_connections';
SHOW VARIABLES LIKE 'max_user_connections';

-- Check current connections
SHOW PROCESSLIST;

-- Grant specific privileges (run if needed)
GRANT ALL PRIVILEGES ON stagging.* TO 'TeamETL'@'%';
GRANT ALL PRIVILEGES ON transformed.* TO 'TeamETL'@'%';
FLUSH PRIVILEGES;