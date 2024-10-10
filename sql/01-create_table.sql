create table NOS_ZIPCODE
(
    ZipCode  CHAR(8)     NOT NULL
        PRIMARY KEY,
    County   VARCHAR(30) NOT NULL,
    District VARCHAR(30) NOT NULL
);
