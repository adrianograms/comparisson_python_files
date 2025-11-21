import sys
import argparse
import pandas as pd
from tabulate import tabulate

def adjust_column_names(name_column):
    """Function to adjust the name of a columns for lower case and replace space with '_'.

    Args:
        name_column (string): name of the columns

    Returns:
        string: adjusted name of the column
    """
    return name_column.strip().lower().replace(' ', '_')

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

    df_1 = df_1[all_columns_1]
    df_2 = df_2[all_columns_2]

    all_columns = dict(zip(all_columns_2, all_columns_1))

    df_2 = df_2.rename(columns=all_columns)

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
    if type == 'csv':
        df_1 = pd.read_csv(path_file_1).rename(adjust_column_names, axis='columns')
        df_2 = pd.read_csv(path_file_2).rename(adjust_column_names, axis='columns')
    elif type == 'excel':
        df_1 = pd.read_excel(path_file_1).rename(adjust_column_names, axis='columns')
        df_2 = pd.read_excel(path_file_2).rename(adjust_column_names, axis='columns')
    elif type == 'tsv':
        df_1 = pd.read_csv(path_file_1, sep='\t').rename(adjust_column_names, axis='columns')
        df_2 = pd.read_csv(path_file_2, sep='\t').rename(adjust_column_names, axis='columns')
    elif type == 'parquet':
        df_1 = pd.read_parquet(path_file_1).rename(adjust_column_names, axis='columns')
        df_2 = pd.read_parquet(path_file_2).rename(adjust_column_names, axis='columns')
    else:
        raise TypeError("Type invalid")
    
    print('Read Complete!')

    return df_1, df_2

def group_datasets(df_1, df_2, key_columns):
    """Function to group the dataset using the key columns as the group columns and the compare columns as the aggregate columns.

    Args:
        df_1 (Dataframe): Dataframe of the primary dataset
        df_2 (Dataframe): Dataframe of the secondary dataset
        key_columns (list): List of the key columns

    Returns:
        tuple: Tuple with the aggregated Dataframes generated.
    """
    return df_1.groupby(key_columns).sum(), df_2.groupby(key_columns).sum()

def compare_datasets(df_1, df_2, key_columns, compare_columns, max_diff):
    """Function to compare the Dataframes and generate the files with the difference between them.

    Args:
        df_1 (Dataframe): Dataframe of the primary dataset
        df_2 (Dataframe): Dataframe of the secondary dataset
        key_columns (list): List of the key columns
        compare_columns (list): List of the comparison columns
        max_diff (float): Threshold of difference between the comparison columns.
    """
    df_join = df_1.merge(df_2, suffixes=('_primary','_secondary'), how='outer',on=key_columns, indicator=True)
    print('Join Complete!')

    columns_primary = [col + '_primary' for col in compare_columns]
    columns_secondary = [col + '_secondary' for col in compare_columns]
    columns = list(zip(columns_primary, columns_secondary))

    query_parts = [f"(abs({col[0]} - {col[1]}) >= {max_diff})" for col in columns]
    query_string = '(' + " or ".join(query_parts) + ')' + "and _merge == 'both'"
    query_primary = "_merge == 'left_only'"
    query_secondary = "_merge == 'right_only'"


    print(f'The comparison query between both dataset is {query_string}.')
    print(f'The comparison query for records exclusive to the primary file is {query_primary}.')
    print(f'The comparison query for records exclusive to the secondary file is {query_secondary}.\n')

    primary_sum = df_join[columns_primary].sum()
    secondary_sum = df_join[columns_secondary].sum()

    tabulate_list = []
    for c in compare_columns:
        tabulate_list.append([c,primary_sum[c + '_primary'],secondary_sum[c + '_secondary'],abs(primary_sum[c + '_primary'] - secondary_sum[c + '_secondary'])])
    print(tabulate(tabulate_list, headers=['Column', 'Primary Value', 'Secondary Value', 'Difference'], floatfmt=".2f"))
    
    df_join.query(query_string).to_csv('diff_files.csv')
    df_join.query(query_primary).to_csv('left_only_files.csv')
    df_join.query(query_secondary).to_csv('right_only_files.csv')

    print('Join completed and difference files generated!')

    return df_join.query(query_string), df_join.query(query_string), df_join.query(query_secondary)


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
    df_1, df_2 = group_datasets(df_1, df_2, key_columns)
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
        args.join = [adjust_column_names(i) for i in args.join]
        key_columns_1 = args.join
        key_columns_2 = args.join

        print(f'Join columns will be {args.join} for both datasets.\n')
    else:
        args.join_primary = [adjust_column_names(i) for i in args.join_primary]
        args.join_secondary = [adjust_column_names(i) for i in args.join_secondary]
        key_columns_1 =args.join_primary
        key_columns_2 = args.join_secondary

        print(f'Join columns for the primary dataset will be {args.join_primary}.')
        print(f'Join columns for the secondary dataset will be {args.join_secondary}.\n')

    if not(args.columns is None):
        args.columns = [adjust_column_names(i) for i in args.columns]
        compare_column_1 = args.columns
        compare_column_2 = args.columns

        print(f'Comparisons columns will be {args.columns} for both datasets.\n')
    else:
        args.columns_primary = [adjust_column_names(i) for i in args.columns_primary]
        args.columns_secondary = [adjust_column_names(i) for i in args.columns_secondary]
        compare_column_1 =args.columns_primary
        compare_column_2 = args.columns_secondary

        print(f'Comparisons columns for the primary dataset will be {args.columns_primary}.')
        print(f'Comparisons columns for the secondary dataset will be {args.columns_secondary}.\n')

    compare_files(path_file_1, path_file_2, key_columns_1, key_columns_2, compare_column_1, compare_column_2, type_file, max_diff)

get_arguments()


