USE MASTER;
CREATE DATABASE DataWarehouse;



USE DataWarehouse;

CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;

SELECT name
FROM sys.schemas
ORDER BY name;



