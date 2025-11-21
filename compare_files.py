import sys
import argparse
import pandas as pd
from tabulate import tabulate

def adjust_column_names(name_column):
    return name_column.strip().lower().replace(' ', '_')

def rename_columns(df_1, df_2, key_columns_1, key_columns_2, compare_column_1, compare_column_2):
    all_columns_1 = key_columns_1 + compare_column_1
    all_columns_2 = key_columns_2 + compare_column_2

    df_1 = df_1[all_columns_1]
    df_2 = df_2[all_columns_2]

    all_columns = dict(zip(all_columns_2, all_columns_1))

    df_2 = df_2.rename(columns=all_columns)

    return df_1, df_2

def read_files(path_file_1, path_file_2, type='csv'):
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
    return df_1.groupby(key_columns).sum(), df_2.groupby(key_columns).sum()

def compare_datasets(df_1, df_2, key_columns, compare_columns, max_diff):
    df_join = df_1.merge(df_2, suffixes=('_primary','_secondary'), how='outer',on=key_columns, indicator=True)
    print('Join Complete!')

    columns_primary = [col + '_primary' for col in compare_columns]
    columns_secondary = [col + '_secondary' for col in compare_columns]
    columns = list(zip(columns_primary, columns_secondary))

    query_parts = [f"(abs({col[0]} - {col[1]}) >= {max_diff})" for col in columns]
    query_string = '(' + " or ".join(query_parts) + ')' + "and _merge == 'both'"

    primary_sum = df_join[columns_primary].sum()
    secondary_sum = df_join[columns_secondary].sum()

    tabulate_list = []
    #print('Column\t Primary Value\t Secondary Value\t Difference' )
    for c in compare_columns:
        tabulate_list.append([c,primary_sum[c + '_primary'],secondary_sum[c + '_secondary'],abs(primary_sum[c + '_primary'] - secondary_sum[c + '_secondary'])])
        #print(f'{c}\t {primary_sum[c + '_primary']}\t {secondary_sum[c + '_secondary']}\t {abs(primary_sum[c + '_primary'] - secondary_sum[c + '_secondary'])}' )
    print(tabulate(tabulate_list, headers=['Column', 'Primary Value', 'Secondary Value', 'Difference'], floatfmt=".2f"))
    

    df_join.query(query_string).to_csv('diff_files.csv')
    df_join.query("_merge == 'left_only'").to_csv('left_only_files.csv')
    df_join.query("_merge == 'right_only'").to_csv('right_only_files.csv')

    return df_join.query(query_string), df_join.query("_merge == 'left_only'"), df_join.query("_merge == 'right_only'")


def compare_files(path_file_1, path_file_2, key_columns_1, key_columns_2, compare_column_1, compare_column_2, type, max_diff):
    key_columns = key_columns_1
    compare_columns = compare_column_1
    df_1, df_2 = read_files(path_file_1, path_file_2, type=type)
    df_1, df_2 = rename_columns(df_1, df_2,key_columns_1, key_columns_2, compare_column_1, compare_column_2)
    df_1, df_2 = group_datasets(df_1, df_2, key_columns)
    compare_datasets(df_1, df_2, key_columns, compare_columns, max_diff)

def create_map():
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
    args = create_map()

    path_file_1 = args.primary
    path_file_2 = args.secondary

    type_file = args.type

    max_diff = args.diff

    key_columns_1 = []
    key_columns_2 = []

    compare_column_1 = []
    compare_column_2 = [] 

    if not(args.join is None):
        args.join = [adjust_column_names(i) for i in args.join]
        key_columns_1 = args.join
        key_columns_2 = args.join
    else:
        args.join_primary = [adjust_column_names(i) for i in args.join_primary]
        args.join_secondary = [adjust_column_names(i) for i in args.join_secondary]
        key_columns_1 =args.join_primary
        key_columns_2 = args.join_secondary

    if not(args.columns is None):
        args.columns = [adjust_column_names(i) for i in args.columns]
        compare_column_1 = args.columns
        compare_column_2 = args.columns
    else:
        args.columns_primary = [adjust_column_names(i) for i in args.columns_primary]
        args.columns_secondary = [adjust_column_names(i) for i in args.columns_secondary]
        compare_column_1 =args.columns_primary
        compare_column_2 = args.columns_secondary

    compare_files(path_file_1, path_file_2, key_columns_1, key_columns_2, compare_column_1, compare_column_2, type_file, max_diff)

get_arguments()


