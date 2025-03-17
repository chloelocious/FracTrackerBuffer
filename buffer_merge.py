import os
import glob
import shutil
import pandas as pd
import geopandas as gpd
from simpledbf import Dbf5

dbf_folder = "/Users/chloelocious/Documents/GitHub/FracTrackerBuffer/Demographic Queries"
shapefile_folder = "/Users/chloelocious/Documents/GitHub/FracTrackerBuffer/Petrochem Union Pieces 2025"
output_folder = "/Users/chloelocious/Documents/GitHub/FracTrackerBuffer/Processed_Results"

if os.path.exists(output_folder):
    shutil.rmtree(output_folder)
os.makedirs(output_folder)

state_fips = {
    "AL": "01", "AK": "02", "AZ": "04", "AR": "05", "CA": "06",
    "CO": "08", "CT": "09", "DE": "10", "DC": "11", "FL": "12",
    "GA": "13", "HI": "15", "ID": "16", "IL": "17", "IN": "18",
    "IA": "19", "KS": "20", "KY": "21", "LA": "22", "ME": "23",
    "MD": "24", "MA": "25", "MI": "26", "MN": "27", "MS": "28",
    "MO": "29", "MT": "30", "NE": "31", "NV": "32", "NH": "33",
    "NJ": "34", "NM": "35", "NY": "36", "NC": "37", "ND": "38",
    "OH": "39", "OK": "40", "OR": "41", "PA": "42", "RI": "44",
    "SC": "45", "SD": "46", "TN": "47", "TX": "48", "UT": "49",
    "VT": "50", "VA": "51", "WA": "53", "WV": "54", "WI": "55",
    "WY": "56", "GU": "66", "PR": "72", "VI": "78"
}

deq_files = glob.glob(os.path.join(dbf_folder, "DEQ_*.DBF"))
petrochem_files = glob.glob(os.path.join(shapefile_folder, "Petrochem_Union_By_State_*.shp"))

print("\n Matching DEQ State Codes to Correct Petrochem Files:\n")

# Process each DEQ file
for deq_file in deq_files:
    state_abbr = os.path.basename(deq_file).split("_")[1][:2]  #  state code
    fips_code = state_fips.get(state_abbr, "Unknown")  # convert to FIPS code

    if fips_code == "Unknown":
        print(f"No FIPS code found for {state_abbr}")
        continue

    matching_petrochem = [shp for shp in petrochem_files if f"_{fips_code}." in shp]

    if not matching_petrochem:
        print(f" No match found for {deq_file} (State: {state_abbr}, FIPS: {fips_code})")
        continue

    petrochem_shapefile = matching_petrochem[0] 

    print(f" {deq_file} (State: {state_abbr}, FIPS: {fips_code}) → {os.path.basename(petrochem_shapefile)}")

    dbf = Dbf5(deq_file)
    deq_df = dbf.to_dataframe()

    if "GEOCODE" in deq_df.columns:
        deq_df["GEOCODE"] = deq_df["GEOCODE"].astype(str).str.replace("7500000US", "", regex=False).str.strip()
    else:
        print(f"Warning: 'GEOCODE' column missing in {deq_file}. Skipping merge.")
        continue

    petrochem_gdf = gpd.read_file(petrochem_shapefile)

    if "GEOID" in petrochem_gdf.columns:
        petrochem_gdf.rename(columns={"GEOID": "GEOID_x"}, inplace=True)

    petrochem_gdf["GEOID_x"] = petrochem_gdf["GEOID_x"].astype(str).str.strip()
    deq_df["GEOCODE"] = deq_df["GEOCODE"].astype(str).str.strip()

    # merge DBF data onto Shapefile using cleaned GEOID_x and GEOCODE
    merged_gdf = petrochem_gdf.merge(deq_df, left_on="GEOID_x", right_on="GEOCODE", how="left")

    # ensure missing population data doesn't crash script
    if "P0010001" in merged_gdf.columns:
        merged_gdf["TotalPop"] = merged_gdf["P0010001"].fillna(0).astype(int)  # total population per block
    else:
        print(f"Warning: Missing 'P0010001' column in DBF for {deq_file}. Skipping population assignment.")

    # cap Population Percentage at 100%**
    if "TotalPop" in merged_gdf.columns and "Clp_SqKm" in merged_gdf.columns and "Blk_SqKm" in merged_gdf.columns:
        merged_gdf["Pop_Percent"] = merged_gdf.apply(
            lambda row: min(100, (row["TotalPop"] * row["Clp_SqKm"] / row["Blk_SqKm"]) * 100)
            if pd.notna(row["Clp_SqKm"]) and pd.notna(row["Blk_SqKm"]) and row["Blk_SqKm"] > 0
            else 0, 
            axis=1
        )
        print("\nPopulation percentage correctly calculated using 'Clp_SqKm / Blk_SqKm' (capped at 100%).")
    else:
        print(f"Warning: Missing 'Clp_SqKm' or 'Blk_SqKm' in merged dataset. Skipping population percentage calculation.")

    joined_output = os.path.join(output_folder, f"joined_{fips_code}.shp")
    merged_gdf.to_file(joined_output)
    print(f"Merged shapefile saved: {joined_output}\n")

print("All DEQ files processed successfully!")
