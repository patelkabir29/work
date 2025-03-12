# Reference: AWS Textract Documentation

import webbrowser, os
import json
import boto3
import io
from io import BytesIO
import sys
from pprint import pprint
import csv


def get_rows_columns_map(table_result, blocks_map):
    rows = {}
    scores = []
    for relationship in table_result['Relationships']:
        if relationship['Type'] == 'CHILD':
            for child_id in relationship['Ids']:
                cell = blocks_map[child_id]
                if cell['BlockType'] == 'CELL':
                    row_index = cell['RowIndex']
                    col_index = cell['ColumnIndex']
                    if row_index not in rows:
                        # create new row
                        rows[row_index] = {}
                    
                    # get confidence score
                    scores.append(str(cell['Confidence']))
                        
                    # get the text value
                    rows[row_index][col_index] = get_text(cell, blocks_map)
    return rows, scores


def get_text(result, blocks_map):
    text = ''
    if 'Relationships' in result:
        for relationship in result['Relationships']:
            if relationship['Type'] == 'CHILD':
                for child_id in relationship['Ids']:
                    word = blocks_map[child_id]
                    if word['BlockType'] == 'WORD':
                        if "," in word['Text'] and word['Text'].replace(",", "").isnumeric():
                            text += '"' + word['Text'] + '"' + ' '
                        else:
                            text += word['Text'] + ' '
                    if word['BlockType'] == 'SELECTION_ELEMENT':
                        if word['SelectionStatus'] =='SELECTED':
                            text +=  'X '
    return text


def get_table_csv_results(file_name, output_file):

    with open(file_name, 'rb') as file:
        img_test = file.read()
        bytes_test = bytearray(img_test)
        print('Image loaded', file_name)

    # process using image bytes
    # get the results
    session = boto3.Session(profile_name='my-profile')
    client = session.client('textract', region_name='us-east-1')
    response = client.analyze_document(Document={'Bytes': bytes_test}, FeatureTypes=['TABLES'])

    # Get the text blocks
    blocks=response['Blocks']
    

    blocks_map = {}
    table_blocks = []
    for block in blocks:
        blocks_map[block['Id']] = block
        if block['BlockType'] == "TABLE":
            table_blocks.append(block)

    if len(table_blocks) <= 0:
        return "<b> NO Table FOUND </b>"

    # ASSUMING ONLY ONE TABLE IS PRESENT
    table = table_blocks[0]

    rows, _ = get_rows_columns_map(table, blocks_map)

    dict_to_csv(rows, output_file)


# Function to convert dictionary to CSV
def dict_to_csv(data, output_file):
    # Extract headers from the first row of the dictionary
    headers = [data[1][i].strip() for i in sorted(data[1].keys())]

    # Open the CSV file for writing
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        csvwriter = csv.writer(csvfile, quoting=csv.QUOTE_MINIMAL)

        # Write the headers
        csvwriter.writerow(headers)

        # Write the rows
        for row_index in sorted(data.keys()):
            if row_index == 1:
                continue  # Skip header row
            row = [data[row_index][i].strip() for i in sorted(data[row_index].keys())]
            csvwriter.writerow(row)


def main(file_name, output_file):
    get_table_csv_results(file_name, output_file)


if __name__ == "__main__":
    file_name = sys.argv[1]
    output_file = sys.argv[2]
    main(file_name, output_file)
