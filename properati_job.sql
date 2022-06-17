#The goal of this project is to preprocess the data downloaded from the Properati web. Through data transformation, data imputation and joinings the final table will be ready to perform models over it. Some selections are made to display metrics from the final table

use properati;

drop table if exists properati_data ;

create table properati_data(
	id INT default null,
    ad_type  VARCHAR(10) default null,
    start_date DATE default null,
	lat float default null,
	lon float default null,
    l1 CHAR(55) default null ,
    l2 CHAR(55) default null,
    l3 CHAR(55) default null,
    l4 CHAR(55) default null ,
    l5 CHAR(55) default null,
	l6 CHAR(55) default null,
	rooms char default null,
	bedrooms char default null,
	bathrooms char default null,
	surface_total float default null,
	surface_covered float default null,
	price INT default null,
    currency VARCHAR(3) default null,
    price_period VARCHAR(10) default null,
    title VARCHAR(2000) default null,
    descriptions VARCHAR(12000) default null,
	property_type CHAR(12) default null,
    operation_type CHAR(10) default null);

SET GLOBAL local_infile=1;


LOAD DATA LOCAL INFILE '/Users/user/Desktop/Data Scientis R Python SQL/Datasets/Precio inmuebles/properati_python.csv' 
INTO TABLE properati_data	
FIELDS TERMINATED BY ',' 
OPTIONALLY enclosed by '"'
IGNORE 1 LINES;

#Properati_data:
select * from properati_data;

#Number of columns
 SELECT count(*) 
 FROM information_schema.columns 
 WHERE table_name='properati_data';
 
#Number of rows 
select count(*) from properati_data;

#Unnecessary columns are removed
alter table properati_data drop id, drop ad_type, drop start_date, drop l1, drop l2, drop l5, drop l6, drop title, drop currency, drop price_period, drop operation_type;

#Description 
describe properati_data;

#Detection of empty or zero values in the dataframe. Columns 'lat' , 'lon' , 'rooms' , 'bedrooms' , 'bathroos' , 'surface_total' and 'surface_covered' have NA values and will be imputed
 SELECT sum(bathrooms=0 or bathrooms = ' ') as bathrooms,
 sum(bedrooms=0 or bedrooms = ' ') as bedrooms,
sum(l3 = ' ') as l3,
 sum(l4 = ' ') as l4,
 sum(lon=0 ) as lon,
 sum(price=0 ) as price,
 sum(property_type = ' ') as property_type,
 sum(rooms=0) as rooms,
 sum(surface_covered=0 ) as surface_covered,
 sum(surface_total=0) as surface_total
 FROM properati_data ;


#Pre-processing data: On Properati dataframe the data will be precessed in order to get the final one which will be used to create the models. 

##Before start to clear the data let's to explain the columns in the dataframe:
#Bathrooms: Number of bathrooms in the property
#Room: Number of rooms in the property
#Bedroom: Number of bedrooms in the property
#l3 and l4 are the suburb where the property is located where l4 shows a more precise zone
#property_type: Is the type of the property. House, appartment or PH
#lat and lon: Coordinates where the property is located
#surface_total: Total surface in the house
#surface_covered: Surface covered in the house
#Price: The price of the property

#Delete nulls
delete from properati_data where lat is null;

#'l4' column is no longer needed and 'l3' will be called 'suburb' and then passed as factor
update properati_data set l3 = l4 where l4 != "";

#'l4' column is no longer needed and 'l3' will be called 'suburb' and then passed as factor 
alter table properati_data change l3 suburb  VARCHAR(25);

#Records without suburb are removed 
delete from properati_data where suburb = "";

#'l4' column is no longer needed
alter table properati_data drop l4;

#The dataframe only will contain records of houses and appartments, so any other factor in 'property_type' will be removed
delete from properati_data where property_type !='Departamento' and property_type !='Casa' and property_type !='PH';

#Let's transform first string to lower case
update properati_data set descriptions = lower(descriptions);

#The column 'description' is the description made by the seller in the Properati Web. There is valuable information that could be interesting to get, so if the publication has a garage, backyard, balcony, if it's new, if it's furnished or if it has amenities will be added as boolean columns in the dataframe 

#garage
alter table properati_data add  garage smallint;
UPDATE properati_data SET garage = case 
	when (descriptions like '%cochera%' or descriptions like '%estacioneminto%' or descriptions like '%garage%' or descriptions like  '%parking%' ) then 1
	else 0 
 end;


#backyard
alter table properati_data add  backyard smallint;
update properati_data set backyard = case
when descriptions like '%patio%' or descriptions like '%jardin%' then 1
else 0
end;

#balcony
alter table properati_data add  balcony smallint;
update properati_data set balcony = case
when (descriptions like '%balcon%' or descriptions like '%balcón%') then 1
else 0
end;


#new
alter table properati_data add  new_ smallint;
update properati_data set new_  = case
when (descriptions like '%nuevo%' or descriptions like '%estrenar%') then 1
else 0
end;


#furnished
alter table properati_data add  furnished smallint;
update properati_data set furnished  = case
when (descriptions like '%amoblado%' or descriptions like '%mueble%' or descriptions like '%muebles%') then 1
else 0
end;

#amenities
alter table properati_data add  amenities smallint;
update properati_data set amenities  = case
when (descriptions like '%sum%' or descriptions like '%s.u.m%' or descriptions like '%parrilla%' or descriptions like  '%jacuzzi%') then 1
else 0
end;

#Column 'description' is no longer needed
alter table properati_data drop descriptions;

#There are incomplete records that most of their values ara NA and hard to imputed, so will be removed

#'rooms' , 'bedrooms' and 'bathrooms' are converted to integer
update properati_data  set rooms = null where rooms ='';
alter table properati_data modify rooms int;

update properati_data  set bedrooms = null where bedrooms ='' or bedrooms ='-';
alter table properati_data modify bedrooms int;

update properati_data  set bathrooms = null where  bathrooms ='';
alter table properati_data modify bathrooms  int;

delete from properati_data where rooms is null and bedrooms is null and bathrooms is null;

delete from properati_data  where rooms is null and bedrooms is null and surface_total = 0 and surface_covered = 0;

delete from properati_data where ((bedrooms =0 and rooms = 1) or bedrooms >0  or bedrooms is null)=False;

#Before start to impute values the outliers are removed in order to don't distort the values
delete from properati_data  where (rooms <= 7 or rooms is null) = False; 
delete from properati_data  where (bedrooms <= 6 or bedrooms is null) = False; 
delete from properati_data  where (bedrooms >= 0 or bedrooms is null) = False; 
delete from properati_data  where (bathrooms <=3 or bathrooms is null) = False; 
delete from properati_data  where (price > 0 or price <= 437200) = False; 
delete from properati_data  where (surface_total <= 150 or surface_total is null) = False; 
delete from properati_data  where (surface_covered <= 119 or surface_covered is null) = False; 

drop table if exists lat_mean_bySuburb;
#Imputation of latitude and longitude: There are records where the coordinates are inconsistent with the coordinates of Capital Federal and also, NA values. So they will be imputed by the average of the coordinates of the suburb to which they belong
create table lat_mean_bySuburb(select  suburb, AVG(lat) as mean from properati_data group by suburb);
update properati_data left join lat_mean_bySuburb on properati_data.suburb = lat_mean_bySuburb.suburb set properati_data.lat= lat_mean_bySuburb.mean where properati_data.lat = 0;

drop table if exists lon_mean_bySuburb;
create table lon_mean_bySuburb(select  suburb, AVG(lon) as mean from properati_data group by suburb);
update properati_data left join lon_mean_bySuburb on properati_data.suburb = lon_mean_bySuburb.suburb set properati_data.lon= lon_mean_bySuburb.mean where properati_data.lon = 0;

#'Bedroom' column imputation: Houses and apartments with NA values in 'Bedroom' column are assumed to have one less unit than the 'Room' column
update properati_data set bedrooms = (rooms-1) where bedrooms is null and rooms is not null;

update properati_data set bedrooms = 1 where bathrooms = 1;
update properati_data set bedrooms = 2 where bathrooms = 2;
update properati_data set bedrooms = 3 where bathrooms = 3;

#In the same way, records with NA values in 'Room' column are assumed to have one more unit than the 'Bedroom' column 
update properati_data set rooms = (bedrooms+1) where rooms is null and bedrooms is not null;

#'Bathroom' column imputation: By majority rule is assumed that bethrooms with at least two bedrooms have one bathroom and that ones with more than 2 bedrooms have 2 bathrooms. So NA values in 'Bathroom' column are imputed using that rule.
update properati_data set bathrooms = 1 where bathrooms is null and bedrooms <=2;
update properati_data set bathrooms = 2 where bathrooms is null and bedrooms >2;

#Surface columns imputation: There are records when don't provide values in Total Surface column but it is in the Covered Surface' column and vice versa. Then, for the missing value, the value of the other column will be taken.
update properati_data set surface_covered = surface_total where surface_covered = 0 and surface_total != 0 ;

update properati_data set surface_total = surface_covered where surface_total = 0 and surface_covered != 0;

#There are records where the total surface is lower than the covered one, so those ones will be assumed that the total surface is the same than the covered one.
update properati_data set surface_total = surface_covered where surface_total < surface_covered;

#For records without values on the surface, the average will be imputed depending on the number of rooms, bedrooms and bathrooms

drop table if exists surface_mean_byRooms;
create table surface_mean_byRooms(select rooms,bedrooms,bathrooms,AVG(surface_total) as mean from properati_data where surface_total !=0 group by rooms,bedrooms,bathrooms);

update properati_data as t1 left join surface_mean_byRooms as t2 
on t1.rooms = t2.rooms and t1.bedrooms = t2.bedrooms and t1.bathrooms = t2.bathrooms 
set surface_total = mean where surface_total = 0; 

#As assumption the same value imputed to 
update properati_data set surface_covered = surface_total where surface_covered = 0;

#Addition of the meter squared price to the table
alter table properati_data add m2 int unsigned;
update properati_data set m2 = price/surface_total;

#A new table is added that indicate the average price of the properties given the quantity of rooms, bedrooms and bathrooms and the type of the property.

drop table if exists price_mean_bySuburb;

create table price_mean_bySuburb(select rooms,bedrooms,bathrooms,property_type, round(AVG(price)) as mean from properati_data group by rooms,bedrooms,bathrooms,property_type);
alter table properati_data add price_mean int;

#Selection of the proportion of properties  by suburb where their prices are above the mean price

#Create a table with the number of properties by suburb
drop table if exists total_suburb;
create table total_suburb(select count(suburb) as total ,suburb from properati_data group by suburb order by suburb asc);

#Create a table with the number of properties by suburb above the mean price
drop table if exists above_mean_price;
create table above_mean_price(select a.suburb, count(a.suburb) as numbers
from properati_data as a, price_mean_bySuburb as b
where a.rooms = b.rooms and a.bedrooms = b.bedrooms and a.bathrooms = b.bathrooms and a.property_type = b.property_type and a.price > b.mean group by suburb);


#Selection of the proportions by suburb above the 50%
set @proportion = 0.5;

select a.suburb, a.numbers/b.total as prop from above_mean_price as a, total_suburb as b 
where a.suburb = b.suburb  having prop > (SELECT @proportion) order by prop desc ;


#Tabla temporal: Se usa para hacer pruebas
with
	cte1 as (select rooms,bedrooms,bathrooms,price from properati_data)
select * from cte1;

#Se puede usar como grid de R
with
	cte1 as (select rooms,bedrooms,price from properati_data),
    cte2 as (select bathrooms,mean from price_mean_bySuburb )
select count(bathrooms)from cte1,cte2;


#Stored procedures
delimiter $$
drop procedure if exists dummy;
create procedure dummy(in charac varchar(20))
	begin
		select descriptions in (charac) from properati_data;
	end $$ 
delimiter ;

#Pivot table: Average squared meter price given the suburb and the number of rooms

select suburb,
round(AVG(case when rooms=1 then price/surface_total end)) as '1',
round(AVG(case when rooms=2 then price/surface_total end)) as '2',
round(AVG(case when rooms=3 then price/surface_total end)) as '3',
round(AVG(case when rooms=4 then price/surface_total end)) as '4',
round(AVG(case when rooms=5 then price/surface_total end)) as '5',
round(AVG(case when rooms=6 then price/surface_total end)) as '6',
round(AVG(case when rooms=7 then price/surface_total end)) as '7'
from properati_data 
group by suburb
order by suburb;

#Stored procedure to find the meter squared price for a property given the suburb and the number of rooms
delimiter //
drop procedure if exists m2_price;
create procedure m2_price(in suburb_name varchar(20), rooms_number int)
	begin
		select round(AVG(price/surface_total))  as 'm2 price'
        from properati_data 
        where suburb = suburb_name and rooms = rooms_number;
	end //
delimiter ;

call m2_price('Belgrano',1);



