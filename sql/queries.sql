#retornar distrito e concelho conforme o código-postal
USE NOS_ZIPCODE_SCHEMA;

SELECT County, District
FROM NOS_ZIPCODE
WHERE Zipcode = '7750-104'
