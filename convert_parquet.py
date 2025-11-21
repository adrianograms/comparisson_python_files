import argparse
import pandas as pd
import os
from pyspark.sql import SparkSession

def create_map():
    """Function to create the dict with the parameters passed on the execution.

    Returns:
        dict: Dictionary with all the parameters passed.
    """
    parser = argparse.ArgumentParser(description="Comparison program for file.")
    parser.add_argument('filename')           # positional argument
    parser.add_argument('-t' ,"--type", help="Type of the file, can be csv, excel, parquet or tsv", choices = ['csv', 'excel', 'tsv'], default='csv', metavar='')
    parser.add_argument('-e', "--engine", help="Engine to make the conversion, pandas or pyspark", choices = ['pandas', 'pyspark'], default='pandas', metavar='')

    return parser.parse_args()

def read_files_pandas(path_file, type='csv'):
    """Function to read file and convert to parquet using pandas for that.

    Args:
        path_file (string): Path of the file
        type (str, optional): Type of the file. Defaults to 'csv'.

    Raises:
        TypeError: Invalid type.
    """
    if type == 'csv':
        df = pd.read_csv(path_file)
    elif type == 'excel':
        df = pd.read_excel(path_file)
    elif type == 'tsv':
        df = pd.read_csv(path_file, sep='\t')
    else:
        raise TypeError("Type invalid")
    
    file_name = os.path.basename(path_file).split('.')[0] + '.parquet'
    
    df.to_parquet(file_name, index=False)

def read_files_pyspark(path_file, type='csv'):
    """Function to read file and convert to parquet using pyspark for that.

    Args:
        path_file (string): Path of the file
        type (str, optional): Type of the file. Defaults to 'csv'.

    Raises:
        TypeError: Invalid type.
    """
    spark = SparkSession.builder.appName("SelectColumns").getOrCreate()
    if type == 'csv':
        df = spark.read.option("inferSchema", True).csv(path_file, header=True)
    elif type == 'tsv':
        df = spark.read.option("sep", "\t").option("header", "true").csv(path_file)
    elif type == 'parquet':
        df = spark.read.parquet(path_file)
    else:
        raise TypeError("Type invalid")
    
    file_name = os.path.basename(path_file).split('.')[0] + '.parquet'
    
    df.write.parquet(file_name)

    return 1

def convert_parquet():
    args = create_map()
    if args.engine == 'pandas':
        read_files_pandas(args.filename, args.type)
    elif args.engine == 'pyspark':
        read_files_pyspark(args.filename, args.type)
    else:
        raise TypeError("Engine Invalid.")

convert_parquet()