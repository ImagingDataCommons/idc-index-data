cmake_minimum_required(VERSION 3.15...3.26)
project(${SKBUILD_PROJECT_NAME} LANGUAGES NONE)

# Use the csv.zip and parquet files generated from the root folder
set(idc_index_csv_zip "${CMAKE_SOURCE_DIR}/idc_index.csv.zip")
set(idc_index_parquet "${CMAKE_SOURCE_DIR}/idc_index.parquet")

# Install the csv.zip and parquet files
install(FILES "${idc_index_csv_zip}" DESTINATION "idc_index_data")
install(FILES "${idc_index_parquet}" DESTINATION "idc_index_data")
