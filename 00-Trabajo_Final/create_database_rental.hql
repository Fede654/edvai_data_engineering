-- Create the database
create database car_rental_db;
use car_rental_db;

-- create tables with appropiate schema
create table
    car_rental_analytics (
        fuelType string,
        rating int,
        renterTripsTaken int,
        reviewCount int,
        city string,
        state_name string,
        owner_id int,
        rate_daily int,
        make string,
        model string,
        year int,
        -- Geo_Point string,
        -- Geo_Shape string,
        year_georef string,
        Code_State int,
        Name_State string,
        Area_Code string,
        Type string,
        state_abbreviation string,
        GNIS_Code int
        )
    row format delimited
    fields terminated by ',';
