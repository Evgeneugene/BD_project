\c project;

CREATE TABLE train(
    id SERIAL PRIMARY KEY,
    timestamp integer,
    Asset_id integer,
    Count NUMERIC(10, 1),
    Open numeric,
    High numeric,
    Low numeric,
    Close numeric,
    Volume numeric,
    VWAP varchar(50),
    Target varchar(50)
);

CREATE TABLE assets(
    Asset_id integer PRIMARY KEY,
    Weight numeric,
    Asset_name varchar(50)
);


ALTER TABLE train ADD CONSTRAINT fk_train_assetid_assetid FOREIGN KEY(Asset_id) REFERENCES assets (Asset_id);

\COPY assets FROM 'data/asset_details.csv' DELIMITER ',' CSV HEADER NULL AS 'null';

\COPY train (timestamp, Asset_id, Count, Open, High, Low, Close, Volume, VWAP, Target) FROM 'data/train.csv' WITH (FORMAT CSV, HEADER true, DELIMITER ',');


SELECT * FROM assets;
SELECT * FROM train LIMIT 10; 
