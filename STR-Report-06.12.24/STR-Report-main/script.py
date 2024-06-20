import pandas as pd
import snowflake.connector
import numpy as np
import json
import re
import os
from datetime import datetime, timedelta

# Función para cargar la configuración desde el archivo JSON
def load_config(archivo_config):
    with open(archivo_config, 'r') as f:
        configuracion = json.load(f)
    return configuracion
    
def read_dinamic_data(hoja, configuracion, table_name, sheet_name, create_table, filename):
    parser_config = configuracion["__parser_config__"]
    total_rows = parser_config["number_of_rows"]
    row_start = parser_config["row_start"]
    include_condition = parser_config["include_condition"]
    
    null_condition_row = configuracion["__null_condition_row__"] # 19
    y_axis = configuracion["__y_axis__"] # B
    x_axis = configuracion["__x_axis__"] # 20
    static_columns = configuracion["__static_columns__"]
    column_start = get_column_number(y_axis) # 
    
    result = {}
    records_result = []
    first = True;
    query_creation_str = ""

    records = []
    
    for i in range(total_rows):
        row = row_start + i
        
        count = 0
        add_row = True
        
        x_term = get_value("name", "VARCHAR", x_axis - 1, column_start - 1, True)  # C 19
        condition_value = get_value("name", "VARCHAR", null_condition_row - 1, column_start - 1, True)
        record = get_base_record(filename, condition_value, row, static_columns, include_condition)
        
        while (isinstance(x_term, str) or (not pd.isnull(x_term) and not np.isnan(x_term))):
            count += 1
            next_column = column_start + count
            
            value = get_value("name", "DOUBLE", row - 1, next_column - 2, True)
            condition_value = get_value("name", "VARCHAR", null_condition_row - 1, next_column - 1, True) 
            if isinstance(x_term, float) and x_term.is_integer():
                x_term = int(x_term)
                print("x_term es un número entero:", int(x_term))
            else:
                print("x_term es un número decimal o no es un float:", x_term)
            record[x_term] = value           

            if condition_value is None:
                add_row = False
                records.append(record)
                record = get_base_record(filename, condition_value, row, static_columns, include_condition)
            elif isinstance(condition_value, str) or not np.isnan(condition_value):
                records.append(record)
                record = get_base_record(filename, condition_value, row, static_columns, include_condition)
            
            x_term = get_value("name", "VARCHAR", x_axis - 1, next_column - 1, True)  # C 19
        
        if add_row:
            records.append(record)
    
    result[table_name] = {"__upload_schema__": "list", "__is_merge__": True, "values": records }
    print("result records: ", result)
    return result;

#If the titles are vertical then the parser will read horizonatlly 
#If the titles are horizontal then the parser will read vertically 
def read_full_dynamic_data(hoja, configuracion, table_name, sheet_name, create_table, filename):
    result = {}
    records = []

    parser_config = configuracion["__parser_config__"]
    titles = parser_config["__titles__"] # Object that represents the configuration to get the <Titles>
    record_processor = parser_config["record_processor"] # Object that represents the record/title processor to be used 

    outer_loop_index = get_outer_loop_index(titles) # Positional index for the first record.
    inner_loop_index = get_inner_loop_reference(titles) # Positional index for the first title.
    total_records = count_records(titles)
    total_titles = count_titles(titles)

    if record_processor == "skip_line_processor":
        records = skip_line_processor(titles, inner_loop_index, outer_loop_index, total_titles, total_records, filename)
    elif record_processor == "record_type_subtype_category_processor":
        records = record_multiple_catergory_processor(titles, inner_loop_index, outer_loop_index, total_titles, total_records, filename)
    elif record_processor == "record_static_category_processor":
        records = record_static_category_processor(titles, inner_loop_index, outer_loop_index, total_titles, total_records, filename)
    elif record_processor == "occunpancy_dow_processor":
        records = occunpancy_dow_processor(titles, inner_loop_index, outer_loop_index, total_titles, total_records, filename)
    elif record_processor == "occunpancy_delta_dow_processor":
        records = occunpancy_delta_dow_processor(titles, inner_loop_index, outer_loop_index, total_titles, total_records, filename)
    elif record_processor == "adr_dow_processor":
        records = adr_dow_processor(titles, inner_loop_index, outer_loop_index, total_titles, total_records, filename)
    elif record_processor == "adr_delta_dow_processor":
        records = adr_delta_dow_processor(titles, inner_loop_index, outer_loop_index, total_titles, total_records, filename)
    elif record_processor == "revpar_dow_processor":
        records = revpar_dow_processor(titles, inner_loop_index, outer_loop_index, total_titles, total_records, filename)
    elif record_processor == "revpar_delta_dow_processor":
        records = revpar_delta_dow_processor(titles, inner_loop_index, outer_loop_index, total_titles, total_records, filename)
    elif record_processor == "horizontal_static_category":
        records = horizontal_static_category(titles, inner_loop_index, outer_loop_index, total_titles, total_records, filename)
    elif record_processor == "horizontal_static_skipline_category":
        records = horizontal_static_skipline_category(titles, inner_loop_index, outer_loop_index, total_titles, total_records, filename)
    elif record_processor == "horizontal_census_static_skipline_category":
        records = horizontal_census_static_skipline_category(titles, inner_loop_index, outer_loop_index, total_titles, total_records, filename)
    elif record_processor == "segmentation_glance_static_category_reference_processor":
        records = segmentation_glance_static_category_reference_processor(titles, inner_loop_index, outer_loop_index, total_titles, total_records, filename)
    elif record_processor == "segmentation_glance_delta_category_reference_processor":
        records = segmentation_glance_delta_category_reference_processor(titles, inner_loop_index, outer_loop_index, total_titles, total_records, filename)
    elif record_processor == "glance_static_category_reference_processor":
        records = glance_static_category_reference_processor(titles, inner_loop_index, outer_loop_index, total_titles, total_records, filename)
    else:
        records = outter_loop(titles, inner_loop_index, outer_loop_index, total_titles, total_records, filename)

    result[table_name] = {"__upload_schema__": "list", "__is_merge__": True, "values": records }
    #print("RESULT", result)
    return result

def glance_static_category_reference_processor(titles, inner_loop_index, outer_loop_index, total_titles, total_records, filename):
    records = []
    static_type = ""
    for record_index in range(outer_loop_index + 1, outer_loop_index + 8, 2):
        type_str = get_directional_value(titles, 4, record_index)
        print("TYPESTR ----------------", type_str)
        if static_type != type_str and value_exists(type_str):
            static_type = type_str

        result = segmentation_glance_delta_inner_loop(titles, record_index, inner_loop_index, outer_loop_index, static_type, total_titles, filename)
        records.append(result)
    print("RECORDS", records)
    return records


def segmentation_glance_delta_category_reference_processor(titles, inner_loop_index, outer_loop_index, total_titles, total_records, filename):
    records = []
    static_type = ""
    for record_index in range(outer_loop_index, outer_loop_index + 11):
        if record_index not in [12,16,29,33]:
            type_str = get_directional_value(titles, 2, record_index)
            
            if static_type != type_str and value_exists(type_str):
                static_type = type_str

            result = segmentation_glance_delta_inner_loop(titles, record_index, inner_loop_index, outer_loop_index, static_type, total_titles, filename)
            records.append(result)
    return records

def segmentation_glance_delta_inner_loop(titles, record_index, inner_loop_index, outer_loop_index, type_str, total_titles, filename):
    aux = {}
    property_id = get_value("property_id", "VARCHAR", 2, 1)
    property_name = get_value("property_name", "VARCHAR", 1, 1)
    period = get_value("period", "VARCHAR", 3, 1)

    aux = {
        "SOURCE" : filename,
        "created_date": datetime.now(),
        "PROPERTY_ID": property_id,
        "PROPERTY_NAME": property_name,
        "PERIOD": period
    }

    static_subtype_str = ""
    for title_index in range(inner_loop_index, inner_loop_index + 13, 1):
        if title_index not in [10, 11, 15, 16]:
            title = get_directional_title(titles, outer_loop_index, title_index)
            subtype_str = get_directional_title(titles, outer_loop_index - 2, title_index - 1)
            value = get_directional_value(titles, title_index, record_index)
            
            if static_subtype_str != subtype_str and value_exists(subtype_str):
                static_subtype_str = subtype_str.replace(' (%)', '')

            if not value_exists(value):
                value = 0

            formatted_title = title.replace('(', '').replace(')', '').replace(' ', '_')

            aux[formatted_title + "_" + static_subtype_str] = value
        
    aux["TYPE"] = type_str
    return aux

def segmentation_glance_static_category_reference_processor(titles, inner_loop_index, outer_loop_index, total_titles, total_records, filename):
    records = []
    static_type = ""
    for record_index in range(outer_loop_index, outer_loop_index + 11):
        if record_index not in [12,16,29,33]:
            type_str = get_directional_value(titles, 2, record_index)
            
            if static_type != type_str and value_exists(type_str):
                static_type = type_str

            result = segmentation_glance_inner_loop(titles, record_index, inner_loop_index, outer_loop_index, static_type, total_titles, filename)
            records.append(result)
    return records

def segmentation_glance_inner_loop(titles, record_index, inner_loop_index, outer_loop_index, type_str, total_titles, filename):
    aux = {}
    property_id = get_value("property_id", "VARCHAR", 2, 1)
    property_name = get_value("property_name", "VARCHAR", 1, 1)
    period = get_value("period", "VARCHAR", 3, 1)

    aux = {
        "SOURCE" : filename,
        "created_date": datetime.now(),
        "PROPERTY_ID": property_id,
        "PROPERTY_NAME": property_name,
        "PERIOD": period
    }

    subtype_str = ""
    for title_index in range(inner_loop_index, inner_loop_index + 14, 4):
        title = get_directional_title(titles, outer_loop_index - 1, title_index - 1)
        value = get_directional_value(titles, title_index, record_index)
        subtype_str = get_directional_value(titles, 4, record_index)
        
        if not value_exists(value):
            value = 0

        aux[title] = value
    
    aux["TYPE"] = type_str
    aux["SUBTYPE"] = subtype_str
    
    #print("AUX", aux)
    return aux

def record_multiple_catergory_processor(titles, inner_loop_index, outer_loop_index, total_titles, total_records, filename):
    records = []
    static_subtype = ""
    for record_index in range(outer_loop_index, outer_loop_index + total_records):
        type_str = get_record_type(titles, inner_loop_index, record_index)
        subtype_str = get_record_subtype(titles, inner_loop_index, record_index)
        if subtype_str != static_subtype and value_exists(subtype_str):
            static_subtype = subtype_str
        
        result = record_multiple_catergory_processor_inner(titles, record_index, inner_loop_index, outer_loop_index, total_titles, type_str, static_subtype, filename)
        records.extend(result)
    return records

def horizontal_census_static_skipline_category(titles, inner_loop_index, outer_loop_index, total_titles, total_records, filename):
    records = []
    for record_index in range(outer_loop_index, outer_loop_index + total_records):
        type_str = get_record_type(titles, 3, record_index)
        records.append(horizontal_census_static_skipline_category_inner_loop(titles, record_index, inner_loop_index, outer_loop_index, 5, filename, type_str))
    return records

def horizontal_census_static_skipline_category_inner_loop(titles, record_index, inner_loop_index, outer_loop_index, total_titles, filename, type_str):
    aux = {}
    property_id = get_value("property_id", "VARCHAR", 2, 1)
    property_name = get_value("property_name", "VARCHAR", 1, 1)
    period = get_value("period", "VARCHAR", 3, 1)
    last_category_str = ""

    aux = {
        "SOURCE" : filename,
        "created_date": datetime.now(),
        "PROPERTY_ID": property_id,
        "PROPERTY_NAME": property_name,
        "PERIOD": period
    }

    current_index = inner_loop_index
    odd_counter = 0
    pair_counter = 0

    for i in range(total_titles):
        
        title = get_directional_title(titles, outer_loop_index, current_index)
        value = get_directional_value(titles, current_index, record_index)
        category_str = get_record_subtype(titles, current_index, outer_loop_index)

        if category_str != last_category_str and value_exists(category_str):
            last_category_str = category_str
        
        if current_index % 2 != 0:
            if odd_counter < 1:
                odd_counter = odd_counter + 1
                current_index = current_index + 2
            else:
                odd_counter = 0
                current_index = current_index + 1
        elif current_index % 2 == 0:
            if pair_counter < 1:
                pair_counter = pair_counter + 1
                current_index = current_index + 2
            else:
                pair_counter = 0
                current_index = current_index + 1
        

        aux[title + last_category_str] = value
    aux["TYPE"] = type_str 
    return aux

def horizontal_static_skipline_category(titles, inner_loop_index, outer_loop_index, total_titles, total_records, filename):
    records = []
    for record_index in range(outer_loop_index, outer_loop_index + total_records):
        type_str = get_record_type(titles, 3, record_index)
        records.append(horizontal_static_skipline_category_inner_loop(titles, record_index, inner_loop_index, outer_loop_index, 4, filename, type_str))
    return records

def horizontal_static_skipline_category_inner_loop(titles, record_index, inner_loop_index, outer_loop_index, total_titles, filename, type_str):
    aux = {}
    property_id = get_value("property_id", "VARCHAR", 2, 1)
    property_name = get_value("property_name", "VARCHAR", 1, 1)
    period = get_value("period", "VARCHAR", 3, 1)
    first = True

    aux = {
        "SOURCE" : filename,
        "created_date": datetime.now(),
        "PROPERTY_ID": property_id,
        "PROPERTY_NAME": property_name,
        "PERIOD": period
    }
    for title_index in range(inner_loop_index, inner_loop_index + (total_titles * 2), 2):
            title = ""
            value = ""

            if first: 
                title = get_directional_title(titles, outer_loop_index - 1, title_index)
                value = get_directional_value(titles, title_index, record_index)
                first = False
            else:
                title = get_directional_title(titles, outer_loop_index - 1, title_index - 1)
                value = get_directional_value(titles, title_index, record_index)

            aux[title] = value
    aux["TYPE"] = type_str 
    return aux

def horizontal_static_category(titles, inner_loop_index, outer_loop_index, total_titles, total_records, filename):
    records = []
    for record_index in range(outer_loop_index, outer_loop_index + total_records):
        type_str = get_record_type(titles, 3, record_index)
        records.append(horizontal_static_category_inner_loop(titles, record_index, inner_loop_index, outer_loop_index, total_titles, filename, type_str))
    return records

def horizontal_static_category_inner_loop(titles, record_index, inner_loop_index, outer_loop_index, total_titles, filename, type_str):
    aux = {}
    property_id = get_value("property_id", "VARCHAR", 2, 1)
    property_name = get_value("property_name", "VARCHAR", 1, 1)
    period = get_value("period", "VARCHAR", 3, 1)

    aux = {
        "SOURCE" : filename,
        "created_date": datetime.now(),
        "PROPERTY_ID": property_id,
        "PROPERTY_NAME": property_name,
        "PERIOD": period
    }

    static_title = ""
    for title_index in range(inner_loop_index, inner_loop_index + total_titles):
            title = get_directional_title(titles, outer_loop_index - 1, title_index)
            value = get_directional_value(titles, title_index, record_index)
            
            if title == "% Chg":
                title = static_title + " " + title
            else:
                static_title = title

            aux[title] = value
    aux["TYPE"] = type_str 
    return aux

def record_static_category_processor(titles, inner_loop_index, outer_loop_index, total_titles, total_records, filename):
    records = []
    static_subtype = ""
    for record_index in range(outer_loop_index, outer_loop_index + total_records):
        type_str = get_record_type(titles, inner_loop_index, record_index)
        subtype_str = get_record_subtype(titles, inner_loop_index, record_index)
        
        if subtype_str != static_subtype and value_exists(subtype_str):
            static_subtype = subtype_str
        
        result = record_multiple_catergory_processor_inner(titles, record_index + 13, inner_loop_index, outer_loop_index, total_titles, type_str, static_subtype, filename)
        records.extend(result)
    return records

def occunpancy_dow_processor(titles, inner_loop_index, outer_loop_index, total_titles, total_records, filename):
    records = []
    for record_index in range(outer_loop_index, outer_loop_index + 6, 2):
        type_str = get_record_subtype(titles, inner_loop_index, record_index)
        
        result = dow_processor_inner(titles, record_index, inner_loop_index, outer_loop_index, total_titles, type_str, filename)
        records.extend(result)
    return records

def occunpancy_delta_dow_processor(titles, inner_loop_index, outer_loop_index, total_titles, total_records, filename):
    records = []
    for record_index in range(outer_loop_index + 1, outer_loop_index + 7, 2):
        type_str = get_record_delay_subtype(titles, inner_loop_index, record_index)
        
        result = dow_processor_inner(titles, record_index, inner_loop_index, outer_loop_index, total_titles, type_str, filename)
        records.extend(result)
    return records

def adr_dow_processor(titles, inner_loop_index, outer_loop_index, total_titles, total_records, filename):
    records = []
    for record_index in range(outer_loop_index, outer_loop_index + 6, 2):
        modified_index = record_index + 6
        type_str = get_record_subtype(titles, inner_loop_index, modified_index)
        
        result = dow_processor_inner(titles, modified_index, inner_loop_index, outer_loop_index, total_titles, type_str, filename)
        records.extend(result)
    return records

def adr_delta_dow_processor(titles, inner_loop_index, outer_loop_index, total_titles, total_records, filename):
    records = []
    for record_index in range(outer_loop_index + 1, outer_loop_index + 7, 2):
        modified_index = record_index + 6
        type_str = get_record_delay_subtype(titles, inner_loop_index, modified_index)
        
        result = dow_processor_inner(titles, modified_index, inner_loop_index, outer_loop_index, total_titles, type_str, filename)
        records.extend(result)
    return records

def revpar_dow_processor(titles, inner_loop_index, outer_loop_index, total_titles, total_records, filename):
    records = []
    for record_index in range(outer_loop_index, outer_loop_index + 6, 2):
        modified_index = record_index + 12
        type_str = get_record_subtype(titles, inner_loop_index, modified_index)
        
        result = dow_processor_inner(titles, modified_index, inner_loop_index, outer_loop_index, total_titles, type_str, filename)
        records.extend(result)
    return records

def revpar_delta_dow_processor(titles, inner_loop_index, outer_loop_index, total_titles, total_records, filename):
    records = []
    for record_index in range(outer_loop_index + 1, outer_loop_index + 7, 2):
        modified_index = record_index + 12
        type_str = get_record_delay_subtype(titles, inner_loop_index, modified_index)
        
        result = dow_processor_inner(titles, modified_index, inner_loop_index, outer_loop_index, total_titles, type_str, filename)
        records.extend(result)
    return records

#Logic to create a different record base on the upper category of the titles.
def dow_processor_inner(titles, record_index, inner_loop_index, outer_loop_index, total_titles, type_str, filename):
    records_aux = []
    property_id = get_value("property_id", "VARCHAR", 2, 1)
    property_name = get_value("property_name", "VARCHAR", 1, 1)
    period = get_value("period", "VARCHAR", 3, 1)

    aux = {
        "TYPE": type_str,
        "SOURCE" : filename,
        "created_date": datetime.now(),
        "PROPERTY_ID": property_id,
        "PROPERTY_NAME": property_name,
        "PERIOD": period
    }
    
    static_ctg = ""

    for day_of_week in range(9):
        for title_index in range(inner_loop_index, inner_loop_index + total_titles):
                new_title_index = 0
                if day_of_week == 0:
                    new_title_index = title_index + (total_titles * day_of_week)
                else:
                    new_title_index = title_index + (total_titles * day_of_week) + (2 * day_of_week) 

                print("NEW TITLE INDEX", new_title_index)
                title = get_directional_title(titles, outer_loop_index, new_title_index)
                category = get_directional_ctg(titles, outer_loop_index, new_title_index)
                category_exist = value_exists(category)
                if category != static_ctg and category_exist:
                    if static_ctg == "":
                        static_ctg = category
                        aux["DAY_OF_WEEK"] = category
                    else:
                        records_aux.append(aux)
                        aux = {
                            "TYPE": type_str,
                            "SOURCE" : filename,
                            "created_date": datetime.now(),
                            "PROPERTY_ID": property_id,
                            "PROPERTY_NAME": property_name,
                            "PERIOD": period,
                            "DAY_OF_WEEK": category
                        }
                        static_ctg = category

                value = get_directional_value(titles, new_title_index, record_index)
                
                if value_exists(value):
                    aux[title] = value
    
    records_aux.append(aux)
    return records_aux

#Logic to create a different record base on the upper category of the titles.
def record_multiple_catergory_processor_inner(titles, record_index, inner_loop_index, outer_loop_index, total_titles, type_str, subtype_str, filename):
    records_aux = []
    property_id = get_value("property_id", "VARCHAR", 2, 1)
    property_name = get_value("property_name", "VARCHAR", 1, 1)
    period = get_value("period", "VARCHAR", 3, 1)

    aux = {
        "SUBTYPE": type_str,
        "TYPE": subtype_str,
        "SOURCE" : filename,
        "created_date": datetime.now(),
        "PROPERTY_ID": property_id,
        "PROPERTY_NAME": property_name,
        "PERIOD": period
    }
    
    static_ctg = ""

    for title_index in range(inner_loop_index, inner_loop_index + total_titles):
            title = get_directional_title(titles, outer_loop_index, title_index)
            category = get_directional_ctg(titles, outer_loop_index, title_index)
            category_exist = value_exists(category)
            if category != static_ctg and category_exist:
                if static_ctg == "":
                    static_ctg = category
                    aux["YEAR"] = category
                else:
                    records_aux.append(aux)
                    aux = {
                        "SUBTYPE": type_str,
                        "TYPE": subtype_str,
                        "SOURCE" : filename,
                        "created_date": datetime.now(),
                        "PROPERTY_ID": property_id,
                        "PROPERTY_NAME": property_name,
                        "PERIOD": period,
                        "YEAR": category
                    }
                    static_ctg = category

            value = get_directional_value(titles, title_index, record_index)
            
            if value_exists(value):
                aux[title] = value
    
    records_aux.append(aux)
    return records_aux

def skip_line_processor(titles, inner_loop_index, outer_loop_index, total_titles, total_records, filename):
    records = []
    for record_index in range(outer_loop_index, outer_loop_index + total_records, 2):
        result = inner_loop(titles, record_index, inner_loop_index, outer_loop_index, total_titles)
        records.append(result)
    return records

def outter_loop(titles, inner_loop_index, outer_loop_index, total_titles, total_records, filename):
    records = []
    for record_index in range(outer_loop_index, outer_loop_index + total_records):
        records.append(inner_loop(titles, record_index, inner_loop_index, outer_loop_index, total_titles))
    return records

def inner_loop(titles, record_index, inner_loop_index, outer_loop_index, total_titles):
    aux = {}
    for title_index in range(inner_loop_index, inner_loop_index + total_titles):
            title = get_directional_title(titles, outer_loop_index, title_index)
            value = get_directional_value(titles, title_index, record_index)
            aux[title] = value
    return aux

def value_exists(value):
    if (isinstance(value, str) or (not pd.isnull(value) and not np.isnan(value))):
        return True
    return False
    
def get_inner_loop_reference(titles):
    if(titles["direction"] == "vertical"):
        return titles["inner_loop_index"]
    else:
        return get_column_number(titles["inner_loop_index"])

def get_outer_loop_index(titles):
    if(titles["direction"] == "vertical"):
        return get_column_number(titles["outer_loop_index"])
    else:
        return titles["outer_loop_index"]

#Function used to get the next category to control the inner while loop 
def get_directional_ctg(titles, outer_loop_index, title_index):
    if(titles["direction"] == "vertical"):
        return get_value("name", "VARCHAR", title_index - 1, outer_loop_index - 2, True)
    else:
        return get_value("name", "VARCHAR", outer_loop_index - 3, title_index, True)
    
#Function used to get the next value on the inner while loop 
def get_record_type(titles, title_index, record_index):
    if(titles["direction"] == "vertical"):
        return get_value("name", "VARCHAR", title_index - 2, record_index, True)
    else:
        return get_value("name", "VARCHAR", record_index, title_index - 2, True)

#Function used to get the next value on the inner while loop 
def get_record_delay_subtype(titles, title_index, record_index):
    if(titles["direction"] == "vertical"):
        return get_value("name", "VARCHAR", title_index - 3, record_index - 1, True)
    else:
        return get_value("name", "VARCHAR", record_index - 2, title_index - 2, True)

#Function used to get the next value on the inner while loop 
def get_record_subtype(titles, title_index, record_index):
    if(titles["direction"] == "vertical"):
        return get_value("name", "VARCHAR", title_index - 3, record_index, True)
    else:
        return get_value("name", "VARCHAR", record_index - 2, title_index - 1, True)

#Function used to get the next title to control the inner while loop 
def get_directional_title(titles, outer_loop_index, title_index):
    if(titles["direction"] == "vertical"):
        return get_value("name", "VARCHAR", title_index - 1, outer_loop_index - 1, True)
    else:
        return get_value("name", "VARCHAR", outer_loop_index - 1, title_index - 1, True)

#Function used to get the next value on the inner while loop 
def get_directional_value(titles, title_index, record_index):
    if(titles["direction"] == "vertical"):
        return get_value("name", "VARCHAR", title_index - 1, record_index, True)
    else:
        return get_value("name", "VARCHAR", record_index, title_index - 1, True)

#Function used to count the records if the matrix has a continious set of records
def count_records(titles):
    if titles["direction"] == "vertical":
        column_start = get_column_number(titles["outer_loop_index"]) + 1
        row_check = titles["inner_loop_index"]
        return count_columns(row_check, column_start)
    else:
        columm_check = get_column_number(titles["inner_loop_index"])
        row_start_dynamic = titles["outer_loop_index"] + 1
        return count_rows(columm_check - 1, row_start_dynamic)    

#Function used to count the records if the matrix has a continious set of titles
def count_titles(titles):
    if titles["direction"] == "vertical":
        columm_check = get_column_number(titles["outer_loop_index"]) + 1
        row_start_dynamic = titles["inner_loop_index"]
        return count_rows(columm_check, row_start_dynamic)
    else:
        column_start = get_column_number(titles["inner_loop_index"])
        row_check = titles["outer_loop_index"]
        return count_columns(row_check, column_start)    
        
#Function used to get the base record on some of the models created
def get_base_record(filename, year, row, static_columns, include_condition):
    record = {}
    record["PROPERTY_NAME"] = get_value("property_name", "VARCHAR", 1, 1)
    record["PROPERTY_ID"] = get_value("property_id", "VARCHAR", 2, 1)
    record["PERIOD"] = get_value("period", "VARCHAR", 3, 1)
    
    if include_condition:
        record["YEAR"] = year
        
    
    for item in static_columns:
        column = get_column_number(item["column"])
        name = item["name"]
        var_type = item["type"]
        value = get_value(name, var_type, row - 1, column - 1, True)
        record[name] = value
    
    record["created_date"] = datetime.now()
    record["source"] = filename;
    return record

#Function used to get the row number base on a static start row to count from
def get_row_number(parser_config, row_start):
    if "number_of_rows" in parser_config and parser_config["number_of_rows"]:
        return parser_config["number_of_rows"]
    else:
        columm_check = get_column_number(parser_config["row_check"])
        return count_rows(columm_check, row_start)

#Function used to get the records for a matrix of information. to use this one the information need to be set on a perferct square matrix with no gaps on the info
def read_range_data(hoja, configuracion, table_name, sheet_name, create_table, filename):
    parser_config = configuracion["__parser_config__"]
    row_start = parser_config["row_start"]
    total_rows = get_row_number(parser_config, row_start)
    print("TOTAL ROWS", total_rows)

    result = {}
    records_result = []
    first = True;
    query_creation_str = ""
    


    for i in range(total_rows):
        record = {}
        
        for variable, ubicacion in configuracion.items():
            if "__" not in variable:
                valor = ""
                var_type = ubicacion['type']
                if 'value' in ubicacion:
                   valor = ubicacion['value'] 
                else:
                    column = ubicacion['column']
                    var_type = ubicacion['type']
                    columna_numero = get_column_number(column) - 1 
                    row = row_start + i
                    valor = get_value(variable, var_type, row - 1, columna_numero, True)

                record[variable] = valor
                var_name = get_var_name(variable)
                if i == 0:
                    if first:
                        query_creation_str += f' {var_name} {var_type}'
                        first = False
                    else: 
                        query_creation_str += f', {var_name} {var_type}'

        if i < 1 and create_table:
            create_tables(query_creation_str, sheet_name, table_name)


        record["PROPERTY_NAME"] = get_value("property_name", "VARCHAR", 1, 1)
        record["PROPERTY_ID"] = get_value("property_id", "VARCHAR", 2, 1)
        record["PERIOD"] = get_value("period", "VARCHAR", 3, 1)
        record["created_date"] = datetime.now();
        record["SOURCE"] = filename;
                
        records_result.append(record)
    
    result[table_name] = {"__upload_schema__": "list", "__is_merge__": False, "values": records_result }
    return result

#Function used to read static data with the configuration saying each column/row position values
def read_static_data(hoja, configuracion, table_name, sheet_name, create_table, filename):
    datos_hoja = {}
    result = {}
    first = True
    query_creation_str = ""
    for variable, ubicacion in configuracion.items():
        if "__" not in variable:
            valor = ""
            var_type = ubicacion['type']
            
            if 'value' in ubicacion:
               valor = ubicacion['value'] 
            else:
                fila = ubicacion['row'] - 1
                columna = ubicacion['column']
                columna_numero = ord(columna) - ord('A')
                valor = get_value(variable, var_type, fila, columna_numero) 

            var_name = get_var_name(variable)
            
            if first:
                query_creation_str += f'{var_name} {var_type}'
                first = False
            else: 
                query_creation_str += f', {var_name} {var_type}'
            
            datos_hoja[variable] = valor
            
    datos_hoja["property_name"] = get_value("property_name", "VARCHAR", 1, 1)
    datos_hoja["property_id"] = get_value("property_id", "VARCHAR", 2, 1)
    datos_hoja["period"] = get_value("period", "VARCHAR", 3, 1)
    datos_hoja["month"] = get_value("month", "VARCHAR", 3, 1)
    datos_hoja["created_date"] = datetime.now();
    datos_hoja["source"] = filename;
    
 
    if create_table:
        create_tables(query_creation_str, sheet_name, table_name);
    result = {}

    result[table_name] = {"__upload_schema__": "record", "values": datos_hoja };
    return result
    
def get_var_name(var):
    if var == "2022" or var == "2023" or var == "2024":
        return f'\"{var}\"' 
    return f'{var}'

def count_rows(columna, fila_inicio):
    fila_actual = fila_inicio - 1
    count = 0
    valor = hoja.iloc[fila_actual, columna]
    while not (isinstance(valor, str) and valor == "") and not (isinstance(valor, float) and (np.isnan(valor))):
        try:
            valor = hoja.iloc[fila_actual, columna]
        except Exception as e:
            if "is out of bounds for axis" in str(e):
                valor = ""
            else:
                valor = None
        count += 1
        fila_actual += 1
    return count

def count_columns(fila, columna_inicio):
    current_col = columna_inicio
    count = 0
    valor = hoja.iloc[fila, current_col]
    
    while not (isinstance(valor, str) and valor == "") and not (isinstance(valor, float) and (np.isnan(valor))):
        try:
            valor = hoja.iloc[fila - 1, current_col]
        except Exception as e:
            valor = None
        
        count += 1
        current_col += 1
    return count

def get_value(variable, value_type, fila, columna, static=False):
    valor = None
    try:
        dato = hoja.iloc[int(fila) - 1, columna]
        if static:
            valor = dato
        elif isinstance(dato, str):
            index_prop_name = dato.index("  ")
            pattern_prop_id = r"ChainID:\s+(\d{5})"
            pattern_prop_period = r'For the Month of: (\w+\s+\d{4})'
            
            match_period_str = re.search(pattern_prop_period, dato)
            match_prop_id = re.search(pattern_prop_id, dato)

            if variable == 'property_id':
                valor = match_prop_id.group(1)
            elif variable == 'property_name':
                valor = dato[:index_prop_name]
            elif variable == 'period':
                valor = datetime.strptime(match_period_str.group(1), '%B %Y')
            elif variable == 'month':
                now_date = datetime.now()
                last_month = now_date - timedelta(days=30)
                valor = last_month
            else:
                valor = dato
        else:
            valor = dato

    except KeyError:
        print(f"Error: column '{columna}' not found on the sheet.")
    except Exception as e:
        print(f"Couldn't find the value on the report based on directions", variable, value_type, fila, columna, static)
        return None

    return clean_value_output(valor, value_type)

def clean_value_output(value, value_type):
    if value_type in ['DOUBLE', 'INTEGER'] and (pd.isnull(value) or value == "" or np.isnan(value)):
        return 0
    return value

# def clean_value_output(value, value_type):
#     try:
#         if value_type in ['DOUBLE', 'INTEGER']:
#             if pd.isnull(float(value)) or float(value) == "" or np.isnan(float(value)):
#                 return 0
#     except (ValueError, TypeError):
#         return None

#     return value

def create_tables(sql_script, sheet_name, table_name):
    conn = snowflake.connector.connect(
        user='kabir',
        password='Score@1000',
        account='nzb10951.us-east-1',
        warehouse='COMPUTE_WH',
        database ='STR'
    )

    fnal_sql =  f"create or replace TABLE STR.{sheet_name}.{table_name} ( property_name VARCHAR,property_id INTEGER,period DATE,created_date DATE,source Varchar,"+sql_script +")"

    cur = conn.cursor()
    cur.execute(fnal_sql)
    conn.commit()

    cur.close()
    conn.close()
    
def get_column_number(columna):
    numero = 0
    for letra in columna:
        numero = numero * 26 + (ord(letra.upper()) - ord('A')) + 1
    return numero
    
def get_columns(json_data):
    columnas = []
    for columna in json_data.keys():
        col = str(columna)
        if "2021" in col or "2022" in col or "2023" in col or "2024" in col or "Group" == col or col.isdigit() or " " in col:
            columnas.append('"' + col.upper() + '"')
        else:
            columnas.append(col)
    return columnas

def get_query(json_data, sheet_name, table_name, condition_columns, isMerge):
    
    columns = get_columns(json_data)
    columns_comma = ', '.join(get_columns(json_data))
    placeholders = ', '.join(['%s'] * len(json_data))
    if isMerge:
         
        on_clause = " AND ".join([f"t.{key} = s.{key}" for key in condition_columns])
        select_clause = ", ".join([f"%s as {key}" for key in columns])
        update_clause = ", ".join([f"t.{key} = s.{key}" for key in columns])
        
        new_data_columns = ", ".join([f"s.{key}" for key in columns])
        # Construir la consulta MERGE
        return f"""
            MERGE INTO STR."{sheet_name.upper()}"."{table_name.upper()}" t
            USING (SELECT {select_clause}) s
            ON {on_clause}
            WHEN MATCHED THEN
                UPDATE SET
                    {update_clause}
            WHEN NOT MATCHED THEN
                INSERT ({columns_comma})
                VALUES ({new_data_columns});
        """ 
    else:
        return f"""INSERT INTO STR."{sheet_name.upper()}"."{table_name.upper()}" ({columns_comma}) VALUES ({placeholders})"""
        
       
    
def insert_data(sheet_name, table_name, json_data, isMerge):
    conn = snowflake.connector.connect(
        user='kabir',
        password='Score@1000',
        account='nzb10951.us-east-1',
        warehouse='COMPUTE_WH',
        database ='STR'
    )

    if json_data["PERIOD"]:
        # print(json_data["PERIOD"])
        # json_data["PERIOD"] = datetime.strftime(str(json_data["PERIOD"]), "%B %Y").replace(day=1, hour=0, minute=0, second=0)
        json_data["PERIOD"] = json_data["PERIOD"].strftime('%Y-%m-%d %H:%M:%S')

    if json_data["created_date"]:
        json_data["created_date"] = json_data["created_date"].strftime('%Y-%m-%d %H:%M:%S')


    cur = conn.cursor()
    on_keys = getOnKeys(table_name, sheet_name)
    print(on_keys)
    
    query = get_query(json_data, sheet_name, table_name, on_keys, isMerge)
    #print(query, on_keys)
    print(json_data)
    cur.execute(query, list(json_data.values()))
    conn.commit()

    cur.close()
    conn.close()

def getOnKeys(table_name, sheet_name):
    year_table_name = ['Occupancy', 'OCCUPANCY_DELTA', 'ADR', 'ADR_DELTA', 'REVPAR', 'REVPAR_DELTA']
    sheet_names = ['Industry', 'Comp']
    
    multiple_category_name = ['OCCUPANCY', 'OCCUPANCY_DELTA', 'ADR', 'ADR_DELTA', 'REVPAR', 'REVPAR_DELTA', 'INDEXES', 'INDEXES_DELTA', 'RANKING', 'RANKING_DELTA', 'REVENUE_PER_ROOM', 'REVENUE_PER_ROOM_DELTA']
    multiple_category_sheets = ['Segmentation Occ', 'Segmentation ADR', 'Segmentation RevPAR', 'Segmentation Indexes', 'Segmentation Ranking', 'Add Rev ADR', 'Add Rev REVPAR']

    day_of_week_name = ['OCCUPANCY', 'OCCUPANCY_DELTA', 'ADR', 'ADR_DELTA', 'REVPAR', 'REVPAR_DELTA']
    day_of_week_sheets = ['Day of Week', 'Day of Week Industry']

    seg_glance_name = ['GLANCE', 'GLANCE_DELTA', 'GLANCE_YEAR_TO_DATE', 'GLANCE_YEAR_TO_DATE_DELTA']
    seg_glance_sheets = ['Segmentation Glance']

    if (sheet_name in sheet_names) and (table_name in year_table_name):
        return  ["PROPERTY_ID", "TYPE", "YEAR"]
    if (sheet_name in multiple_category_sheets) and (table_name in multiple_category_name):
        return  ["PROPERTY_ID", "TYPE", "YEAR", "SUBTYPE"]
    if (sheet_name in day_of_week_sheets) and (table_name in day_of_week_name):
        return  ["PROPERTY_ID", "TYPE", "PERIOD", "DAY_OF_WEEK"]
    if (sheet_name in seg_glance_sheets) and (table_name in seg_glance_name):
        return  ["PROPERTY_ID", "PERIOD", "TYPE", "SUBTYPE"]
    else:
        return ["PROPERTY_ID", "TYPE", "PERIOD"]


# REPLACE WITH:  datetime.now().strftime('%Y%m') + '00'
# For Current date
current_date = '202301' + '00'

# Get current dir file list
current_dir = os.getcwd()

# search for files with name format
_files = []
for _file in os.listdir(current_dir):
    if current_date in _file:
        _files.append(_file)

# Imprimir los archivos encontrados
if _files:
    print(f"""{len(_files)} files were found for the specified date""")
else:
    print("ERROR: Couldn't find any file with the current date")


crear_tablas = True
total = 0

for archivo_excel in _files:
    # Ruta al archivo de configuración JSON
    print("Loadin from file: ", archivo_excel)
    archivo_configuracion = 'configuracion.json'

    # Load the STR report
    try:
        datos_excel = pd.ExcelFile(archivo_excel)
    except FileNotFoundError:
        print(f"Error: Couldn't find the STR report file specified '{archivo_excel}'.")
        exit()

    # Load the configuration from the file
    try:
        config = load_config(archivo_configuracion)
    except FileNotFoundError:
        print(f"Error: Couldn't find the configuration file '{archivo_configuracion}'.")
        exit()
    except json.decoder.JSONDecodeError:
        print(f"Error: Config file '{archivo_configuracion}' is not a valid object.")
        exit()

    # Read the data from the files based on the configuration specified for each sheet
    datos = {}
    for sheet_name, tablas in config.items():
        for table_name, sheet_config in tablas.items():
            print(sheet_config["__data_schema__"])
            if sheet_name not in datos_excel.sheet_names:
                print(f"Error: sheet '{sheet_name}' specified on the config doesn't exist on the data file")
                continue
            hoja = pd.read_excel(datos_excel, sheet_name)
            target_table = table_name
            create_table = crear_tablas
            sheet_result = None

            if "__parser_config__" in sheet_config:
                if 'table' in sheet_config["__parser_config__"]:
                    target_table = sheet_config["__parser_config__"]['table']
                    if crear_tablas:
                        create_table = False

            if(sheet_config["__data_schema__"] == 'static'):
                sheet_result = read_static_data(hoja, sheet_config, table_name, sheet_name, create_table, archivo_excel)
            elif(sheet_config["__data_schema__"] == 'range'):
                sheet_result = read_range_data(hoja, sheet_config, target_table, sheet_name, create_table, archivo_excel)
            elif(sheet_config["__data_schema__"] == 'dynamic'):
                sheet_result = read_dinamic_data(hoja, sheet_config, target_table, sheet_name, create_table, archivo_excel)
            elif(sheet_config["__data_schema__"] == 'full_dynamic'):
                sheet_result = read_full_dynamic_data(hoja, sheet_config, target_table, sheet_name, create_table, archivo_excel)
                print("RECORD PROCESSOR LOGIC FINISHED")

            # Suponiendo que result es un diccionario
            if sheet_result:
                if sheet_name in datos:
                    if target_table in datos[sheet_name]:
                        if 'values' in datos[sheet_name][target_table]:
                            datos[sheet_name][target_table]['values'].extend(sheet_result[target_table]['values'])
                    else:
                        datos[sheet_name].update(sheet_result)
                else:
                    datos[sheet_name] = sheet_result

    # Suponiendo que "result" es tu JSON
    for sheet_name, sheet_content in datos.items():
        for table_name, table_values in sheet_content.items():
            print(f"UPLOADING CONTENT FOR SHEET {sheet_name} ON TABLE {table_name}")
            upload_schema = table_values["__upload_schema__"]
            isMerge = table_values["__is_merge__"]
            records = table_values["values"]
            if upload_schema == "record":
                print("VALUES GENERATED AS SINGLE RECORDS")
                insert_data(sheet_name, table_name, records, False)
            elif upload_schema == "list":
                print("VALUES GENERATED AS LIST RECORDS", isMerge)
                count = 1
                for item in records:
                    print(f"PUSHING RECORD #{count} OF TABLE {table_name}, {sheet_name}")
                    count+=1
                    insert_data(sheet_name, table_name, item, isMerge)
                    print('\n\n' +table_name+'\n\n')
                    
    crear_tablas=False
