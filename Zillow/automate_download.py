import os
import re
import requests

file_name = ""
# URL of the file
url_list = ["https://files.zillowstatic.com/research/public_csvs/zhvi/County_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1738346498",
"https://files.zillowstatic.com/research/public_csvs/zhvi/Neighborhood_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1738346498",
"https://files.zillowstatic.com/research/public_csvs/zhvi/Zip_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1738346498",
"https://files.zillowstatic.com/research/public_csvs/zhvi/City_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1738346498",
"https://files.zillowstatic.com/research/public_csvs/zhvi/City_zhvi_uc_sfrcondo_tier_0.0_0.33_sm_sa_month.csv?t=1738346498",
"https://files.zillowstatic.com/research/public_csvs/zhvi/County_zhvi_uc_sfrcondo_tier_0.0_0.33_sm_sa_month.csv?t=1738346498",
"https://files.zillowstatic.com/research/public_csvs/zhvi/City_zhvi_uc_sfrcondo_tier_0.67_1.0_sm_sa_month.csv?t=1738346498",
"https://files.zillowstatic.com/research/public_csvs/zhvi/County_zhvi_uc_sfrcondo_tier_0.67_1.0_sm_sa_month.csv?t=1738346498",
"https://files.zillowstatic.com/research/public_csvs/zhvi/Neighborhood_zhvi_uc_sfr_sm_sa_month.csv?t=1738346498",
"https://files.zillowstatic.com/research/public_csvs/zhvi/Zip_zhvi_uc_sfr_tier_0.33_0.67_sm_sa_month.csv?t=1738346498",
"https://files.zillowstatic.com/research/public_csvs/zhvi/City_zhvi_uc_sfr_tier_0.33_0.67_sm_sa_month.csv?t=1738346498",
"https://files.zillowstatic.com/research/public_csvs/zhvi/County_zhvi_uc_sfr_tier_0.33_0.67_sm_sa_month.csv?t=1738346498",
"https://files.zillowstatic.com/research/public_csvs/zhvi/Neighborhood_zhvi_uc_condo_tier_0.33_0.67_sm_sa_month.csv?t=1738346498",
"https://files.zillowstatic.com/research/public_csvs/zhvi/Zip_zhvi_uc_condo_tier_0.33_0.67_sm_sa_month.csv?t=1738346498",
"https://files.zillowstatic.com/research/public_csvs/zhvi/City_zhvi_uc_condo_tier_0.33_0.67_sm_sa_month.csv?t=1738346498",
"https://files.zillowstatic.com/research/public_csvs/zhvi/County_zhvi_uc_condo_tier_0.33_0.67_sm_sa_month.csv?t=1738346498",
"https://files.zillowstatic.com/research/public_csvs/zhvi/Neighborhood_zhvi_bdrmcnt_1_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1738346498",
"https://files.zillowstatic.com/research/public_csvs/zhvi/Zip_zhvi_bdrmcnt_1_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1738346498",
"https://files.zillowstatic.com/research/public_csvs/zhvi/City_zhvi_bdrmcnt_1_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1738346498",
"https://files.zillowstatic.com/research/public_csvs/zhvi/County_zhvi_bdrmcnt_1_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1738346498",
"https://files.zillowstatic.com/research/public_csvs/zhvi/Neighborhood_zhvi_bdrmcnt_2_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1738346498",
"https://files.zillowstatic.com/research/public_csvs/zhvi/Zip_zhvi_bdrmcnt_2_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1738346498",
"https://files.zillowstatic.com/research/public_csvs/zhvi/City_zhvi_bdrmcnt_2_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1738346498",
"https://files.zillowstatic.com/research/public_csvs/zhvi/County_zhvi_bdrmcnt_2_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1738346498",
"https://files.zillowstatic.com/research/public_csvs/zhvi/Neighborhood_zhvi_bdrmcnt_3_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1738346498",
"https://files.zillowstatic.com/research/public_csvs/zhvi/Zip_zhvi_bdrmcnt_3_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1738346498",
"https://files.zillowstatic.com/research/public_csvs/zhvi/City_zhvi_bdrmcnt_3_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1738346498",
"https://files.zillowstatic.com/research/public_csvs/zhvi/County_zhvi_bdrmcnt_3_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1738346498",
"https://files.zillowstatic.com/research/public_csvs/zhvi/Neighborhood_zhvi_bdrmcnt_4_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1738346498",
"https://files.zillowstatic.com/research/public_csvs/zhvi/Zip_zhvi_bdrmcnt_4_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1738346498",
"https://files.zillowstatic.com/research/public_csvs/zhvi/City_zhvi_bdrmcnt_4_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1738346498",
"https://files.zillowstatic.com/research/public_csvs/zhvi/County_zhvi_bdrmcnt_4_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1738346498",
"https://files.zillowstatic.com/research/public_csvs/zhvi/Neighborhood_zhvi_bdrmcnt_5_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1738346498",
"https://files.zillowstatic.com/research/public_csvs/zhvi/Zip_zhvi_bdrmcnt_5_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1738346498",
"https://files.zillowstatic.com/research/public_csvs/zhvi/City_zhvi_bdrmcnt_5_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1738346498",
"https://files.zillowstatic.com/research/public_csvs/zhvi/County_zhvi_bdrmcnt_5_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1738346498",
"https://files.zillowstatic.com/research/public_csvs/zhvf_growth/Zip_zhvf_growth_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1738346498",
"https://files.zillowstatic.com/research/public_csvs/zori/Zip_zori_uc_sfrcondomfr_sm_month.csv?t=1738346498",
"https://files.zillowstatic.com/research/public_csvs/zori/City_zori_uc_sfrcondomfr_sm_month.csv?t=1738346498",
"https://files.zillowstatic.com/research/public_csvs/zori/County_zori_uc_sfrcondomfr_sm_month.csv?t=1738346498"]



for url in url_list:
    pattern = r"[^/]+(?=\?)"
    match = re.search(pattern, url)

    if match:
        print("Extracted file name:", match.group())
        file_name = match.group()
    else:
        print("No match found")

    # Output path
    output_path = f"C:\\Users\\patel\\Downloads\\zillow\\{file_name}"

    # Download the file
    response = requests.get(url)
    if response.status_code == 200 and not os.path.exists(output_path):
        print(f"{file_name} is NOT in the downloads folder. Downloading...")
        with open(output_path, 'wb') as file:
            file.write(response.content)
        print("File downloaded successfully!")
    elif os.path.exists(output_path):
        print(f"{file_name} is already in the downloads folder.")
    else:
        print(f"Failed to download the file. Status code: {response.status_code}")



# https://files.zillowstatic.com/research/public_csvs/zorf_growth/National_zorf_growth_uc_sfr_sm_month.csv?t=1734926461
# https://files.zillowstatic.com/research/public_csvs/invt_fs/Metro_invt_fs_uc_sfrcondo_sm_month.csv?t=1734926461
# https://files.zillowstatic.com/research/public_csvs/zori/Zip_zori_uc_sfrcondomfr_sm_month.csv?t=1734926461
# https://files.zillowstatic.com/research/public_csvs/zhvf_growth/Zip_zhvf_growth_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1734926461
# https://files.zillowstatic.com/research/public_csvs/zhvi/Zip_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1734926461
# https://files.zillowstatic.com/research/public_csvs/zordi/Metro_zordi_uc_sfrcondomfr_month.csv?t=1734926461

# ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# ZHVI 
# https://files.zillowstatic.com/research/public_csvs/zhvi/County_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1738346498
# https://files.zillowstatic.com/research/public_csvs/zhvi/Neighborhood_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1738346498
# https://files.zillowstatic.com/research/public_csvs/zhvi/Zip_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1738346498
# https://files.zillowstatic.com/research/public_csvs/zhvi/City_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1738346498

# https://files.zillowstatic.com/research/public_csvs/zhvi/City_zhvi_uc_sfrcondo_tier_0.0_0.33_sm_sa_month.csv?t=1738346498
# https://files.zillowstatic.com/research/public_csvs/zhvi/County_zhvi_uc_sfrcondo_tier_0.0_0.33_sm_sa_month.csv?t=1738346498

# https://files.zillowstatic.com/research/public_csvs/zhvi/City_zhvi_uc_sfrcondo_tier_0.67_1.0_sm_sa_month.csv?t=1738346498
# https://files.zillowstatic.com/research/public_csvs/zhvi/County_zhvi_uc_sfrcondo_tier_0.67_1.0_sm_sa_month.csv?t=1738346498

# https://files.zillowstatic.com/research/public_csvs/zhvi/Neighborhood_zhvi_uc_sfr_sm_sa_month.csv?t=1738346498
# https://files.zillowstatic.com/research/public_csvs/zhvi/Zip_zhvi_uc_sfr_tier_0.33_0.67_sm_sa_month.csv?t=1738346498
# https://files.zillowstatic.com/research/public_csvs/zhvi/City_zhvi_uc_sfr_tier_0.33_0.67_sm_sa_month.csv?t=1738346498
# https://files.zillowstatic.com/research/public_csvs/zhvi/County_zhvi_uc_sfr_tier_0.33_0.67_sm_sa_month.csv?t=1738346498

# https://files.zillowstatic.com/research/public_csvs/zhvi/Neighborhood_zhvi_uc_condo_tier_0.33_0.67_sm_sa_month.csv?t=1738346498
# https://files.zillowstatic.com/research/public_csvs/zhvi/Zip_zhvi_uc_condo_tier_0.33_0.67_sm_sa_month.csv?t=1738346498
# https://files.zillowstatic.com/research/public_csvs/zhvi/City_zhvi_uc_condo_tier_0.33_0.67_sm_sa_month.csv?t=1738346498
# https://files.zillowstatic.com/research/public_csvs/zhvi/County_zhvi_uc_condo_tier_0.33_0.67_sm_sa_month.csv?t=1738346498

# https://files.zillowstatic.com/research/public_csvs/zhvi/Neighborhood_zhvi_bdrmcnt_1_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1738346498
# https://files.zillowstatic.com/research/public_csvs/zhvi/Zip_zhvi_bdrmcnt_1_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1738346498
# https://files.zillowstatic.com/research/public_csvs/zhvi/City_zhvi_bdrmcnt_1_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1738346498
# https://files.zillowstatic.com/research/public_csvs/zhvi/County_zhvi_bdrmcnt_1_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1738346498

# https://files.zillowstatic.com/research/public_csvs/zhvi/Neighborhood_zhvi_bdrmcnt_2_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1738346498
# https://files.zillowstatic.com/research/public_csvs/zhvi/Zip_zhvi_bdrmcnt_2_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1738346498
# https://files.zillowstatic.com/research/public_csvs/zhvi/City_zhvi_bdrmcnt_2_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1738346498
# https://files.zillowstatic.com/research/public_csvs/zhvi/County_zhvi_bdrmcnt_2_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1738346498

# https://files.zillowstatic.com/research/public_csvs/zhvi/Neighborhood_zhvi_bdrmcnt_3_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1738346498
# https://files.zillowstatic.com/research/public_csvs/zhvi/Zip_zhvi_bdrmcnt_3_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1738346498
# https://files.zillowstatic.com/research/public_csvs/zhvi/City_zhvi_bdrmcnt_3_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1738346498
# https://files.zillowstatic.com/research/public_csvs/zhvi/County_zhvi_bdrmcnt_3_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1738346498

# https://files.zillowstatic.com/research/public_csvs/zhvi/Neighborhood_zhvi_bdrmcnt_4_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1738346498
# https://files.zillowstatic.com/research/public_csvs/zhvi/Zip_zhvi_bdrmcnt_4_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1738346498
# https://files.zillowstatic.com/research/public_csvs/zhvi/City_zhvi_bdrmcnt_4_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1738346498
# https://files.zillowstatic.com/research/public_csvs/zhvi/County_zhvi_bdrmcnt_4_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1738346498

# https://files.zillowstatic.com/research/public_csvs/zhvi/Neighborhood_zhvi_bdrmcnt_5_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1738346498
# https://files.zillowstatic.com/research/public_csvs/zhvi/Zip_zhvi_bdrmcnt_5_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1738346498
# https://files.zillowstatic.com/research/public_csvs/zhvi/City_zhvi_bdrmcnt_5_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1738346498
# https://files.zillowstatic.com/research/public_csvs/zhvi/County_zhvi_bdrmcnt_5_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1738346498

# ZHVF
# https://files.zillowstatic.com/research/public_csvs/zhvf_growth/Zip_zhvf_growth_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1738346498

# ZORI
# https://files.zillowstatic.com/research/public_csvs/zori/Zip_zori_uc_sfrcondomfr_sm_month.csv?t=1738346498
# https://files.zillowstatic.com/research/public_csvs/zori/City_zori_uc_sfrcondomfr_sm_month.csv?t=1738346498
# https://files.zillowstatic.com/research/public_csvs/zori/County_zori_uc_sfrcondomfr_sm_month.csv?t=1738346498