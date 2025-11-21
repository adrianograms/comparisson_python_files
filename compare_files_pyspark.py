import sys
import argparse
from tabulate import tabulate
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum, lit
from pyspark.sql.types import FloatType

def adjust_column_names(name_columns):
    """Function to adjust the name of the columns on a dataset, lowering case everything and replacing all spaces,
        and creating a dictionary with the old and new names.

    Args:
        name_columns (list): List of columns to be adjusted 

    Returns:
        dict: dictionary with the old name as key and new name as value for the conversion function.
    """
    return dict([(column, column.strip().lower().replace(' ', '_')) for column in name_columns ])

def adjust_column_names_list(name_columns):
    """Function to adjust the name of the columns on a dataset, lowering case everything and replacing all spaces. 

    Args:
        name_columns (list): List of columns to be adjusted 

    Returns:
        list: list with columns with their name adjusted.
    """
    return [column.strip().lower().replace(' ', '_') for column in name_columns ]

def rename_columns(df_1, df_2, key_columns_1, key_columns_2, compare_column_1, compare_column_2):
    """Function to rename the columns of df_2 to the same names as in df_1.

    Args:
        df_1 (Dataframe): Dataframe of the primary dataset
        df_2 (Dataframe): Dataframe of the secondary dataset
        key_columns_1 (list): name of the key columns for the primary dataset.
        key_columns_2 (list): name of the key columns for the secondary dataset.
        compare_column_1 (list): name of the comparison columns for the primary dataset.
        compare_column_2 (list): name of the comparison columns for the secondary dataset.

    Returns:
        tuple: Tuple with both dataframes updated.
    """
    all_columns_1 = key_columns_1 + compare_column_1
    all_columns_2 = key_columns_2 + compare_column_2

    print(f'Original names for the columns of the secondary file {all_columns_2}.\n')

    df_1 = df_1.select(all_columns_1)
    df_2 = df_2.select(all_columns_2)

    all_columns = dict(zip(all_columns_2, all_columns_1))

    df_2 = df_2.withColumnsRenamed(all_columns)

    print('Rename of columns completed!')

    return df_1, df_2

def read_files(path_file_1, path_file_2, type='csv'):
    """Function to the read the dataset and store them in dataframes of pyspark.

    Args:
        path_file_1 (string): path for the primary dataset
        path_file_2 (string): path for the secondary dataset
        type (str, optional): type of the dataset files. Defaults to 'csv'.

    Raises:
        TypeError: Invalid type

    Returns:
        tuple: Tuple with both dataframes from the primary and secondary files.
    """
    spark = SparkSession.builder.appName("SelectColumns").getOrCreate()
    if type == 'csv':
        df_1 = spark.read.option("inferSchema", True).csv(path_file_1, header=True)
        df_2 = spark.read.option("inferSchema", True).csv(path_file_2, header=True)
    elif type == 'tsv':
        df_1 = spark.read.option("sep", "\t").option("header", "true").csv(path_file_1)
        df_2 = spark.read.option("sep", "\t").option("header", "true").csv(path_file_2)
    elif type == 'parquet':
        df_1 = spark.read.parquet(path_file_1)
        df_2 = spark.read.parquet(path_file_2)
    else:
        raise TypeError("Type invalid")
    
    df_1 = df_1.withColumnsRenamed(adjust_column_names(df_1.columns))
    df_2 = df_2.withColumnsRenamed(adjust_column_names(df_2.columns))

    print('Read Complete!')

    return df_1, df_2

def group_datasets(df_1, df_2, key_columns, compare_columns):
    """Function to group the dataset using the key columns as the group columns and the compare columns as the aggregate columns.

    Args:
        df_1 (Dataframe): Dataframe of the primary dataset
        df_2 (Dataframe): Dataframe of the secondary dataset
        key_columns (list): List of the key columns
        compare_columns (list): List of the comparison columns

    Returns:
        tuple: Tuple with the aggregated Dataframes generated.
    """
    numeric_pattern = r"^-?\d+(\.\d+)?$"

    for column_name in compare_columns:
        df_1 = df_1.withColumn(
            column_name,
            when(
                col(column_name).cast("string").rlike(numeric_pattern), # Check if string representation matches numeric pattern
                col(column_name).cast(FloatType()) # Keep original value if numeric
            ).otherwise(0).cast(FloatType()) # Replace with None (null) if not numeric
        )
        df_2 = df_2.withColumn(
            column_name,
            when(
                col(column_name).cast("string").rlike(numeric_pattern), # Check if string representation matches numeric pattern
                col(column_name).cast(FloatType()) # Keep original value if numeric
            ).otherwise(0).cast(FloatType()) # Replace with None (null) if not numeric
        )

    aggregation_expressions = [sum(col(c)).alias(c) for c in compare_columns]
    return df_1.groupBy(key_columns).agg(*aggregation_expressions), df_2.groupBy(key_columns).agg(*aggregation_expressions)

def compare_datasets(df_1, df_2, key_columns, compare_columns, max_diff):
    """Function to compare the Dataframes and generate the files with the difference between them.

    Args:
        df_1 (Dataframe): Dataframe of the primary dataset
        df_2 (Dataframe): Dataframe of the secondary dataset
        key_columns (list): List of the key columns
        compare_columns (list): List of the comparison columns
        max_diff (float): Threshold of difference between the comparison columns.
    """

    columns_primary = [column + '_primary' for column in compare_columns]
    columns_secondary = [column + '_secondary' for column in compare_columns]
    columns = list(zip(columns_primary, columns_secondary))

    compare_columns_primary_dict = dict(zip(compare_columns, columns_primary))
    df_1 = df_1.withColumnsRenamed(compare_columns_primary_dict)

    compare_columns_secondary_dict = dict(zip(compare_columns, columns_secondary))
    df_2 = df_2.withColumnsRenamed(compare_columns_secondary_dict)

    df_1 = df_1.withColumn('primary', lit(1))
    df_2 = df_2.withColumn('secondary', lit(1))

    query_parts = [f"(abs({column[0]} - {column[1]}) >= {max_diff})" for column in columns]
    query_string = '(' + " or ".join(query_parts) + ')' + " and (primary == 1 and secondary == 1)"
    query_primary = 'primary == 1 and secondary is NULL'
    query_secondary = 'primary is NULL and secondary == 1'

    print(f'The comparison query between both dataset is {query_string}.')
    print(f'The comparison query for records exclusive to the primary file is {query_primary}.')
    print(f'The comparison query for records exclusive to the secondary file is {query_secondary}.\n')

    df_join = df_1.join(df_2, how='outer',on=key_columns)

    all_columns = columns_primary + columns_secondary

    aggregation_expressions = [sum(col(c)).alias(c) for c in all_columns]
    df_sum = df_join.groupBy().agg(*aggregation_expressions)

    tabulate_list = []
    for c in compare_columns:
        tabulate_list.append([c,df_sum.first()[c+'_primary'],df_sum.first()[c+'_secondary'],abs(df_sum.first()[c+'_primary'] - df_sum.first()[c+'_secondary'])])

    print(tabulate(tabulate_list, headers=['Column', 'Primary Value', 'Secondary Value', 'Difference'], floatfmt=".2f"), '\n')

    df_join.filter(query_string).repartition(1).write.csv("diff_files_pyspark", header=True, mode="overwrite")
    df_join.filter(query_primary).repartition(1).write.csv("left_only_files_pyspark", header=True, mode="overwrite")
    df_join.filter(query_secondary).repartition(1).write.csv("right_only_files_pyspark", header=True, mode="overwrite")

    print('Join completed and difference files generated!')



def compare_files(path_file_1, path_file_2, key_columns_1, key_columns_2, compare_column_1, compare_column_2, type, max_diff):
    """Function aggregating all the steps of the comparison between the datasets.

    Args:
        path_file_1 (string): Path for the primary dataset.
        path_file_2 (string): Path for the secondary dataset.
        key_columns_1 (list): List of the key columns for the primary dataset.
        key_columns_2 (list): List of the key columns for the secondary dataset.
        compare_column_1 (list): List of the compare columns for the primary dataset.
        compare_column_2 (list): List of the compare columns for the secondary dataset.
        type (string): Type for the datasets.
        max_diff (float8): threshold of difference between the comparison columns.
    """
    key_columns = key_columns_1
    compare_columns = compare_column_1
    df_1, df_2 = read_files(path_file_1, path_file_2, type=type)
    df_1, df_2 = rename_columns(df_1, df_2,key_columns_1, key_columns_2, compare_column_1, compare_column_2)
    df_1, df_2 = group_datasets(df_1, df_2, key_columns, compare_columns)
    compare_datasets(df_1, df_2, key_columns, compare_columns, max_diff)

def create_map():
    """Function to create the dict with the parameters passed on the execution.

    Returns:
        dict: Dictionary with all the parameters passed.
    """
    parser = argparse.ArgumentParser(description="Comparison program for file.")
    parser.add_argument('-p' ,'--primary', help="Path to the primary file of the comparison", metavar='', required=True)
    parser.add_argument('-s' ,"--secondary",  help="Path to the secondary file of the comparison", metavar='', required=True)
    parser.add_argument('-j' ,"--join", help="Join Columns if they have the same name on both files", nargs='*', metavar='')
    parser.add_argument('-jp' ,"--join_primary", help="Join Columns of the primary file (in order)", nargs='*', metavar='')
    parser.add_argument('-js' ,"--join_secondary", help="Join Columns of the secondary file (in order)", nargs='*', metavar='')
    parser.add_argument('-c' ,"--columns", help="Columns that will be compared between the files if the name is the same between the files", nargs='*', metavar='')
    parser.add_argument('-cp' ,"--columns_primary", help="Columns that will be compared of the primary file (in order)", nargs='*', metavar='')
    parser.add_argument('-cs' ,"--columns_secondary", help="Columns that will be compared of the secondary file (in order)", nargs='*', metavar='')
    parser.add_argument('-d' ,"--diff", help="Threshold of the different between the values", default=0.0001, type=float, metavar='')
    parser.add_argument('-t' ,"--type", help="Type of the file, can be csv, excel, parquet or tsv", choices = ['csv', 'excel', 'tsv', 'parquet'], default='csv', metavar='')

    return parser.parse_args()

def get_arguments():
    """Function that compiles the entire application.
    """
    args = create_map()

    path_file_1 = args.primary
    path_file_2 = args.secondary
    print(f'Primary filename is {path_file_1}.')
    print(f'Secondary filename is {path_file_2}.\n')

    type_file = args.type
    print(f'The type of the files are {type_file}.\n')

    max_diff = args.diff
    print(f'The threshold define for the comparison columns is {max_diff}.\n')

    key_columns_1 = []
    key_columns_2 = []

    compare_column_1 = []
    compare_column_2 = [] 

    if not(args.join is None):
        args.join = adjust_column_names_list(args.join)
        key_columns_1 = args.join
        key_columns_2 = args.join

        print(f'Join columns will be {args.join} for both datasets.\n')
    else:
        args.join_primary = adjust_column_names_list(args.join_primary)
        args.join_secondary = adjust_column_names_list(args.join_secondary)
        key_columns_1 =args.join_primary
        key_columns_2 = args.join_secondary

        print(f'Join columns for the primary dataset will be {args.join_primary}.')
        print(f'Join columns for the secondary dataset will be {args.join_secondary}.\n')

    if not(args.columns is None):
        args.columns = adjust_column_names_list(args.columns)
        compare_column_1 = args.columns
        compare_column_2 = args.columns

        print(f'Comparisons columns will be {args.columns} for both datasets.\n')
    else:
        args.columns_primary = adjust_column_names_list(args.columns_primary)
        args.columns_secondary = adjust_column_names_list(args.columns_secondary)
        compare_column_1 =args.columns_primary
        compare_column_2 = args.columns_secondary

        print(f'Comparisons columns for the primary dataset will be {args.columns_primary}.')
        print(f'Comparisons columns for the secondary dataset will be {args.columns_secondary}.\n')

    compare_files(path_file_1, path_file_2, key_columns_1, key_columns_2, compare_column_1, compare_column_2, type_file, max_diff)


get_arguments()
