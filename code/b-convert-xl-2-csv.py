# About: Convert excel files to csv format
# Original data: https://open.canada.ca/data/en/dataset/9b34e712-513f-44e9-babf-9df4f7256550
# Author: Ferdous Khan
# Created: 2024

import pandas as pd
import os
import re

# Location of data
DATA_BASE_DIR = os.path.join(os.getcwd(), 'data')
DATA_INPUT_DIR = os.path.join(DATA_BASE_DIR, 'original')
DATA_OUTPUT_DIR = os.path.join(DATA_BASE_DIR, 'bronze')

# Start and end year/month of data
DATA_START_YEAR = 2021
DATA_END_YEAR = 2024
DATA_END_MONTH = 3

# Output filename pattern to remove special character from input file
output_file_pattern = r'[^A-Za-z0-9.]+'

# Get all files/folders from input folder
input_files = os.scandir(DATA_INPUT_DIR)

# Iterate each input file 
for input_file in input_files: 
    if input_file.is_file() and input_file.name.endswith('.xlsx'):
        # Prepare the new CSV file name
        input_filename = input_file.name
        input_filename_tmp = re.sub(output_file_pattern, '', input_filename)
        output_filename = input_filename_tmp.replace('.xlsx', '.csv') 
        output_file = os.path.join(DATA_OUTPUT_DIR, output_filename)

        print(f"Converting {input_filename} to {output_filename}...\n")
        
        # Read input file
        dfi1 = pd.read_excel(input_file.path, sheet_name=0)

        # Drop all rows that don't have value in all columns
        dfi1.dropna(how='all', inplace = True)

        # Assign 0 to cell that doesn't have any value
        dfi1.fillna('Missing', inplace = True)

        # Expand year's merged cell
        dfi1.ffill(axis=1, inplace=True)
        
        row_count = len(dfi1.index)

        irow = 0
        i = 0
        month = 1
        year  = DATA_START_YEAR
        row_value = []

        # Find the start of year
        while irow < row_count:
            # Get all values of a row to a list    
            row_value = dfi1.iloc[irow].values

            # Find the row where it has year value 
            year_flag = False

            for v in row_value:
                # Non-numeric row
                if isinstance(v, int) is False:
                    continue
                # Start of year
                if v >= DATA_START_YEAR:
                    DATA_START_YEAR = v
                    year_flag = True
                    break
            
            # We have found the start of year
            if year_flag is True:
                break
            # Otherwise continue
            irow = irow + 1

        # Prepare new column headers with Year-Month value
        output_column_header = []          

        year = DATA_START_YEAR
        month = 1
        i = 1 
        input_column_len = len(row_value)

        output_column_header.append('Country')

        while i < ( input_column_len - 1):
            output_column_header.append(f'{year}-{month:02}')

            i = i + 1
            month = month + 1

            if month > 12:
                # Total column comes after each year end
                output_column_header.append('Total')
                year = year + 1
                month = 1
                i = i + 1                 

        # Total column comes after end's year
        output_column_header.append('Total')

        # Re-assign the column header
        dfi1.columns = output_column_header

        # Drop all total columns as we can calculate later
        dfi1.drop('Total', axis=1, inplace=True)
        
        # Drop all rows that don't have actual data
        dfi1.drop( dfi1[ dfi1['Country'].str.startswith("Missing") ].index, inplace=True)

        # Drop row that has Total
        dfi1.drop( dfi1[ dfi1['Country'].str.startswith("Total") ].index, inplace=True)

        # Drop rows that contain no data (commment etc.)
        dfi1.reset_index(drop=True, inplace=True)

        row_count = dfi1.shape[0]
        col_count = dfi1.shape[1] - 1
        
        irow = 0
        row_value = []
        row_missing = []

        # Find the start of year
        while irow < row_count:
            # Get all values of a row to a list    
            row_value = dfi1.iloc[irow].values
            lst_not_missing = list(filter(lambda x: x == 'Missing', row_value))
            
            if len(lst_not_missing) == col_count:
                row_missing.append(irow)

            irow = irow + 1

        print(f'Dropping rows that are missing data: {row_missing}')
        dfi1.drop(index=row_missing, inplace=True)            
        

        # Save output to file
        dfi1.to_csv(output_file, index=False)