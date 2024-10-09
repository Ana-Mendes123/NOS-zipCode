create table NOS_ZIPCODE
(
    ZipCode  CHAR(8)     NOT NULL
        PRIMARY KEY,
    County   VARCHAR(30),
    District VARCHAR(30)
);
