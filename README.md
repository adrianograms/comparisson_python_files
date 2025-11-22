# Comparison python
This project is a comparison software to find the difference between 2 datasets using key columns and comparison columns to find the differences and explicit those differences in a way you can validate and track down the origin of the differences.


## How it works
This application have 2 engines for the comparison, pandas or pyspark. Pandas for lighter datasets, while pypspark for bigger ones. There's 2 different files for each engine, but the parameters they receive are the same with the exception of the type that pyspark don't support excel, but besides that is all the same.

The application will create 3 csv files with the differences between them, one where the record exists on both datasets (using the column keys to join them), but there's a difference greater than the threshold between one or more comparison columns. Other 2 csv files with the records only find on one of the datasets left been for the primary file and right for the secondary file.

The name of the columns is standardized using the name of the columns on the primary dataset, so even tho the columns in the secondary dataset are named differently, it will be renamed for the same name as in the primary dataset. That's why the order of the columns inputted need to be the same for this renaming. 

At the end of the process there's a summary of values for both datasets with all the comparison columns side by side for a more general view of the total value and differences between the origins.

## Parameters
- Primary File Path: 
    - **flags**: -p, --primary
    - **Description**: Path to the primary file of the comparison
- Secondary File Path: 
    - **flags**: -s, --secondary
    - **Description**: Path to the secondary file of the comparison
- Join Columns (Same for both datasets):
    - **flags**: -j, --join
    - **Description**: Join Columns if they have the same name on both files
- Join Columns Primary Dataset:
    - **flags**: -jp, --join_primary
    - **Description**: Join Columns of the primary file (in order)
- Join Columns Secondary Dataset:
    - **flags**: -js, --join_secondary
    - **Description**: Join Columns of the secondary file (in order)
- Comparison Columns (Same for both datasets):
    - **flags**: -c, --columns
    - **Description**: Columns that will be compared between the files if the name is the same between the files.
- Comparison Columns Primary Dataset:
    - **flags**: -cp, --columns_primary
    - **Description**: Columns that will be compared of the primary file (in order)
- Comparison Columns Secondary Dataset:
    - **flags**: -cs, --columns_secondary
    - **Description**: Columns that will be compared of the secondary file (in order)
- Difference Threshold: 
    - **flags**: -d, --diff
    - **Description**: Threshold of the different between the values.
    - **Default**: 0.0001
    - **type**: Numeric
- File Type: 
    - **flags**: -t, --type
    - **Description**: Type of the file, can be csv, excel, parquet or tsv.
    - **Default**: csv
    - **choices**: csv, tsv, parquet, excel (only for pandas)

## What do you need
To run this application you only need `python 3`, with the libraries `pandas` or `spark` depending on the engine you're using, and `tabulate` for the end summary. 

## Examples

```bash 
python compare_files.py -p AustraliaDataScienceJob2.csv -s AustraliaDataScienceJob3.csv -j company -c low_estimate high_estimate estimate_base_salary -t csv 
``` 
Comparison using pandas of 2 datasets in csv

```bash 
python compare_files_pyspark.py -p title.parquet -s title_2.parquet -j tconst -c seasonNumber -t parquet
```
Comparison using pypsark of 2 datasets in parquet

## Extra
As an Extra, there's a simple converter file type to parquet for better performance in cases of big datasets you have in less performative types like csv, tsv or excel. For the conversion you can use pandas or pyspark as the main engine.

#### How it works
This converter works converting a file of types csv, tsv, parquet or excel (only for pandas engine) to parquet. You can pass the entire path of the original file and it will create a file of same name on the program folder .parquet.

#### Parameters
- File Path (Positional Argument): 
    - **Description**: Path for the file to be converted.
- File Type: 
    - **flags**: -t, --type
    - **Description**: Type of the file, can be csv, excel (only for pandas as engine) or tsv.
    - **Default**: csv
    - **choices**: csv, tsv, excel (only for pandas)
- File Type: 
    - **flags**: -e, --engine
    - **Description**: Engine to make the conversion, pandas or pyspark.
    - **Default**: pandas
    - **choices**: pandas, pyspark

#### Example
```sh
python convert_parquet.py /home/adriano/Downloads/title_2.tsv -t tsv -e pyspark
```
