DROP DATABASE IF EXISTS metro;
CREATE DATABASE metro;
DROP TABLE IF EXISTS metro.CUSTOMER;
CREATE TABLE metro.CUSTOMER(
	Customer_id varchar(10) PRIMARY KEY,
    Customer_name varchar(30) NOT NULL
);
DROP TABLE IF EXISTS Metro.PRODUCT;
CREATE TABLE Metro.PRODUCT(
	Product_id varchar(10) PRIMARY KEY,
    Product_name varchar(30) NOT NULL,
    Price decimal(5,2) not null
);


DROP TABLE IF EXISTS Metro.SUPPLIER;
CREATE TABLE Metro.SUPPLIER(
	Supplier_id varchar(10) PRIMARY KEY,
    Supplier_name varchar(30) NOT NULL
);

DROP TABLE IF EXISTS Metro.STORE;
CREATE TABLE Metro.STORE(
	Store_id varchar(10) PRIMARY KEY,
    Store_name varchar(30) NOT NULL
);


DROP TABLE IF EXISTS Metro.DATE;
CREATE TABLE Metro.DATE(
	T_date date Primary Key,
	T_day varchar (2) NOT NULL,
	T_dayname varchar(10) NOT NULL,
    T_year varchar(4) NOT NULL,
    T_month varchar(20) NOT NULL,
	T_quarter varchar(5) NOT NULL
);

DROP TABLE IF EXISTS Metro.FACT_TABLE;
CREATE TABLE Metro.FACT_TABLE(
	Customer_id varchar(4) NOT NULL,
    Store_id varchar(4) NOT NULL,
    Supplier_id varchar(5) NOT NULL,
	T_date date NOT NULL,
    Quantity smallint NOT NULL,
    Product_id varchar(6) NOT NULL,	
    Sales decimal(10,2) NOT NULL,
    FOREIGN KEY (Customer_id) REFERENCES Metro.CUSTOMER(Customer_id),
	FOREIGN KEY (Product_id) REFERENCES Metro.PRODUCT(Product_id),
    FOREIGN KEY (T_date) REFERENCES Metro.DATE(T_date),
    FOREIGN KEY (Store_id) REFERENCES Metro.STORE(Store_id),
    FOREIGN KEY (Supplier_id) REFERENCES Metro.SUPPLIER(Supplier_id)
   
);