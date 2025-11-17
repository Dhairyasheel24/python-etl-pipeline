-- Create remote user for all teammates
CREATE USER IF NOT EXISTS 'TeamETL'@'%' IDENTIFIED BY 'TeamETL@123';

-- Grant privileges
GRANT ALL PRIVILEGES ON stagging.* TO 'TeamETL'@'%';
GRANT ALL PRIVILEGES ON transformed.* TO 'TeamETL'@'%';

-- Apply changes
FLUSH PRIVILEGES;

-- Verify user exists
SELECT user, host FROM mysql.user WHERE user='TeamETL';