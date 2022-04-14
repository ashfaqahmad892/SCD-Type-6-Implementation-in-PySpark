# SCD-Type-6-Implementation-in-PySpark

Slowly Changing Dimensions (SCD) - dimensions that change slowly over time, rather than changing on regular schedule, time-base. In Data Warehouse there is a need to track changes in dimension attributes in order to report historical data. In other words, implementing one of the SCD types should enable users assigning proper dimension's attribute value for given date.Example of such dimensions could be: customer, geography, employee.

There are many approaches how to deal with SCD. The most popular are:

Type 0 - The passive method

Type 1 - Overwriting the old value

Type 2 - Creating a new additional record

Type 3 - Adding a new column

Type 4 - Using historical table

Type 6 - Combine approaches of types 1,2,3 (1+2+3=6)

In this Notebook we will implement the SCD Type 6 in Pyspark and will explain some of the Characteristics of SCD Type 6.

#### Type 6 - Combine approaches of types 1,2,3 (1+2+3=6). 

In this type we have in dimension table such additional columns as:

##### current_type - 
for keeping current value of the attribute. All history records for given item of attribute have the same current value.

##### historical_type - 
for keeping historical value of the attribute. All history records for given item of attribute could have different values.

##### start_date - 
for keeping start date of 'effective date' of attribute's history.

##### end_date - 
for keeping end date of 'effective date' of attribute's history.

##### current_flag - 
for keeping information about the most recent record.


In this method to capture attribute change we add a new record as in type 2. The current_type information is overwritten with the new one as in type 1. We store the history in a historical_column as in type 3.

![image](https://user-images.githubusercontent.com/16519037/163346549-e567a4c8-cbfa-4bd9-b5b8-82f55896d234.png)
