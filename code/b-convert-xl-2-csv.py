# About: Convert excel files to csv format
# Original data: https://open.canada.ca/data/en/dataset/9b34e712-513f-44e9-babf-9df4f7256550
# Author: Ferdous Khan
# Created: 2024

import pandas as pd
import os
import re

# Data classification:  
#   Original: Unformatted source data
#   Bronze: Formatted data suitable to feed to next stage
#   Silver: Aggregated data from one or multi-stages 
#   Gold: Presentable data

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

# Iterate each file 
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
        dfi1.fillna(0, inplace = True)

        # Expand year's merged cell
        dfi1.ffill(axis=1, inplace=True)
        
        row_count = len(dfi1.index)

        row = 0
        i = 0
        month = 1
        year  = DATA_START_YEAR
        row_value = []

        # Find the start of year
        while row < row_count:
            # Get all values of a row to a list    
            row_value = dfi1.iloc[row].values

            # Find the row where it has year value 
            year_flag = False

            for v in row_value:
                # Non-numeric row
                if isinstance(v, int) is False:
                    continue
                # Start of year
                if v >= DATA_START_YEAR:
                    DATA_START_YEAR = year
                    year_flag = True
                    break
            
            # We have found the start of year
            if year_flag is True:
                break
            # Otherwise continue
            row = row + 1

        # Prepare new column headers with Year-Month value
        output_column_header = []          

        year = DATA_START_YEAR
        month = 1
        i = 1 
        input_column_len = len(row_value)

        output_column_header.append('Country')

        while i < input_column_len:
            output_column_header.append(f'{year}-{month:02}')

            i = i + 1
            month = month + 1

            if month > 12:
                # Total column comes after each year end
                output_column_header.append('Total')
                year = year + 1
                month = 1
                i = i + 1                 

        # Re-assign the column header
        dfi1.columns = output_column_header

        # Drop all total columns as we can calculate later
        dfi1.drop('Total', axis=1, inplace=True)
        
        # Drop all rows that don't have actual data
        dfi1.drop(dfi1[ dfi1['Country'] == 0 ].index, inplace=True)

        # Drop row that begins with Total
        dfi1.drop( dfi1[ dfi1['Country'].str.contains("Total") ].index, inplace=True)

        # Drop row that begins with Source countries
        dfi1.drop( dfi1[ dfi1['Country'].str.contains("Source") ].index, inplace=True)

        # Drop row that begins with Data source
        dfi1.drop( dfi1[ dfi1['Country'].str.contains("Data") ].index, inplace=True)
        
        # Save output to file
        dfi1.to_csv(output_file, index=False)