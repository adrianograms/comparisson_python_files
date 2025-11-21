# Comparison python
This project is a comparison software to find the difference between 2 datasets using key columns and comparison columns to find the differences and explicit those differences in a way you can validate and track down the origin of the differences.


## How it works
This application have 2 engines for the comparison, pandas or pyspark. Pandas for lighter datasets, while pypspark for bigger ones. There's 2 different files for each engine, but the parameters they receive are the same with the exception of the type that pyspark don't support excel, but besides that is all the same.

The application will create 3 csv files with the differences between them, one where the record exists on both datasets (using the column keys to join them), but there's a difference greater than the threshold between one or more comparison columns. Other 2 csv files with the records only find on one of the datasets left been for the primary file and right for the secondary file.

The name of the columns is standardized using the name of the columns on the primary dataset, so even tho the columns in the secondary dataset are named differently, it will be renamed for the same name as in the primary dataset. That's why the order of the columns inputted need to be the same for this renaming. 
 
