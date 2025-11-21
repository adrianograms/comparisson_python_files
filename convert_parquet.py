import argparse
import pandas as pd
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum, lit
from pyspark.sql.types import FloatType

def create_map():
    parser = argparse.ArgumentParser(description="Comparison program for file.")
    parser.add_argument('filename')           # positional argument
    parser.add_argument('-t' ,"--type", help="Type of the file, can be csv, excel, parquet or tsv", choices = ['csv', 'excel', 'tsv'], default='csv', metavar='')

    return parser.parse_args()

def read_files(path_file, type='csv'):
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
    read_files_pyspark(args.filename, args.type)

convert_parquet()