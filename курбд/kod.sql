drop schema if exists kurs cascade;
Create schema kurs;

-------------------------CREATE--TYPES------
create sequence kurs.sq_types_id;
CREATE TABLE IF NOT EXISTS kurs.types
(
    id integer NOT NULL DEFAULT nextval('kurs.sq_types_id'::regclass),
    name varchar(500) COLLATE pg_catalog."default" NOT NULL,
    unit varchar(500) COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT pk_type_id PRIMARY KEY (id),
    CONSTRAINT chk_name_length CHECK (length(name) >= 3),
    CONSTRAINT chk_unit_length CHECK (length(unit) >= 2)
)

TABLESPACE pg_default;
ALTER TABLE IF EXISTS kurs.types
    OWNER to postgres;


-------------------------CREATE--TYPES------

-------------------------CREATE--RATES------
create sequence kurs.sq_rates_id;
CREATE TABLE IF NOT EXISTS kurs.rates
(
    id integer NOT NULL DEFAULT nextval('kurs.sq_rates_id'::regclass),
    since timestamp NOT NULL,
    cost float NOT NULL,
    CONSTRAINT pk_rates_id PRIMARY KEY (id),
    CONSTRAINT chk_rates_cost_positive_or_zero CHECK (cost >= 0.0::double precision),
    CONSTRAINT chk_rates_since_after_date CHECK (since > '2021-01-01 00:00:00'::timestamp)
)

TABLESPACE pg_default;
ALTER TABLE IF EXISTS kurs.rates
    OWNER to postgres;

-------------------------CREATE--RATES------

-------------------------CREATE--LOCATIONS------

create sequence kurs.sq_locations_id;
CREATE TABLE IF NOT EXISTS kurs.locations
(
    id integer NOT NULL DEFAULT nextval('kurs.sq_locations_id'::regclass),
    city varchar(500) COLLATE pg_catalog."default" NOT NULL,
    street varchar(500) COLLATE pg_catalog."default" NOT NULL,
    house integer NOT NULL,
    flat integer NOT NULL,
    CONSTRAINT pk_locations_id PRIMARY KEY (id),
    CONSTRAINT chk_locations_city_length CHECK (length(city) >= 2),
    CONSTRAINT chk_locations_flat_positive CHECK (flat > 0),
    CONSTRAINT chk_locations_house_positive CHECK (house > 0),
    CONSTRAINT chk_locations_street_length CHECK (length(street) >= 2)
)

TABLESPACE pg_default;
ALTER TABLE IF EXISTS kurs.locations
    OWNER to postgres;

-------------------------CREATE--LOCATIONS------

-------------------------CREATE--COUNTERS------

CREATE SEQUENCE kurs.sq_counters_id;
CREATE TABLE IF NOT EXISTS kurs.counters
(
    id integer NOT NULL DEFAULT nextval('kurs.sq_counters_id'::regclass),
    location_id integer NOT NULL,
    type_id integer NOT NULL,
    rate_id integer NOT NULL,
    CONSTRAINT pk_counters_id PRIMARY KEY (id),
    CONSTRAINT fk_counters_locations FOREIGN KEY (location_id)
        REFERENCES kurs.locations (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE CASCADE,
    CONSTRAINT fk_counters_rates FOREIGN KEY (rate_id)
        REFERENCES kurs.rates (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE CASCADE,
    CONSTRAINT fk_counters_types FOREIGN KEY (type_id)
        REFERENCES kurs.types (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE CASCADE
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS kurs.counters
    OWNER to postgres;

-------------------------CREATE--COUNTERS------

-------------------------CREATE--MEASUREMENTS------

create sequence kurs.sq_measurements_id;
CREATE TABLE IF NOT EXISTS kurs.measurements
(
    id integer NOT NULL DEFAULT nextval('kurs.sq_measurements_id'),
    counter_id integer NOT NULL,
    mark timestamp NOT NULL,
    value double precision NOT NULL,
    photo varchar(500) COLLATE pg_catalog."default",
    CONSTRAINT pk_measurements_id PRIMARY KEY (id),
    CONSTRAINT fk_measurements_counters FOREIGN KEY (counter_id)
        REFERENCES kurs.counters (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE CASCADE,
    CONSTRAINT chk_measurements_mark_after_date CHECK (mark > '2021-01-01 00:00:00'::timestamp),
    CONSTRAINT chk_measurements_mark_not_in_future CHECK (mark < now()),

    CONSTRAINT chk_measurements_value_positive_or_zero CHECK (value >= 0.0::double precision)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS kurs.measurements
    OWNER to postgres;


set search_path = 'kurs';

--LOCATIONS

insert into locations (city, street, house, flat) values 
	('Moscow', 'Lenina', random() * (500-1) + 1, random() * (250-1) + 1),
	('Pskov', 'Lenina', random() * (500-1) + 1, random() * (250-1) + 1),
	('Luga', 'Lenina', random() * (500-1) + 1, random() * (250-1) + 1),
	('Kaluga', 'Lenina', random() * (500-1) + 1, random() * (250-1) + 1),
	('Kazan', 'Lenina', random() * (500-1) + 1, random() * (250-1) + 1),
	('Podporozhye', 'Lenina', random() * (500-1) + 1, random() * (250-1) + 1),
	('Ufa', 'Lenina', random() * (500-1) + 1, random() * (250-1) + 1);
	
--LOCATIONS	
--TYPES

insert into types ( name, unit) values
	('power', 'kW'),
	('power_night', 'kW'),
	('power_day', 'kW'),
	('cold_water', 'm^3'),
	('hot_water', 'm^3'),
	('gas', 'm^3'),
	('heat', 'gigacal');

--TYPES
COPY rates(since, cost) FROM 'C:/CourseFile/Ratess.csv' DELIMITER '	' CSV;
--COUNTERS_ID

insert into counters (location_id, type_id, rate_id) values 
	(1, 1, 1),
	(2, 2, 2),
	(3, 3, 3),
	(4, 4, 4),
	(5, 5, 5), 
	(6, 6, 6),
	(7, 7, 7);

--COUNTERS_ID
--------------------------------------------------
--------------------------------------------------

set search_path = 'kurs';
CREATE EXTENSION if not exists file_fdw;
CREATE SERVER if not exists file_server FOREIGN DATA WRAPPER file_fdw;
CREATE OR REPLACE PROCEDURE insert_measurements() AS 
$$
DECLARE
i integer := 1;
BEGIN
	LOOP
    CREATE TEMPORARY TABLE temp_counter_table (value float, mark timestamp);
	if i = 1 then
   		COPY temp_counter_table FROM 'C:/CourseFile/counter1.csv' DELIMITER '	' CSV;
	elsif  i = 2 then
	   	COPY temp_counter_table FROM 'C:/CourseFile/counter2.csv' DELIMITER '	' CSV;
	elsif i = 3 then
	   	COPY temp_counter_table FROM 'C:/CourseFile/counter3.csv' DELIMITER ';' CSV;
	elsif i = 4 then
	   	COPY temp_counter_table FROM 'C:/CourseFile/counter4.csv' DELIMITER ';' CSV;
	elsif i = 5 then
	   	COPY temp_counter_table FROM 'C:/CourseFile/counter5.csv' DELIMITER ';' CSV;
	elsif i = 6 then
	   	COPY temp_counter_table FROM 'C:/CourseFile/counter6.csv' DELIMITER ';' CSV;
	elsif i = 7 then
	   	COPY temp_counter_table FROM 'C:/CourseFile/counter7.csv' DELIMITER ';' CSV;
		END IF;
    INSERT INTO measurements (counter_id, value, mark)
    SELECT i, value, mark FROM temp_counter_table;
    DROP TABLE IF EXISTS temp_counter_table;
	i := i + 1;
		EXIT WHEN i > 7;
	END LOOP;	
END;
$$ LANGUAGE plpgsql;
CALL insert_measurements();


------------------------VIEWS

create view counters_view as
    select id, location_id, type_id, rate_id from counters;

create view locations_view as
    select id, city, street, house, flat from locations;

create view measurements_view as
    select id, counter_id, mark, value, photo from measurements;

create view rates_view as
    select id, since, cost from rates;

create view types_view as
    select id, name, unit from types;



