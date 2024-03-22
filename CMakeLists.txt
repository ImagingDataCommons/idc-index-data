cmake_minimum_required(VERSION 3.15...3.26)
project(${SKBUILD_PROJECT_NAME} LANGUAGES NONE)

find_package(
  Python
  COMPONENTS Interpreter
  REQUIRED)

if(NOT DEFINED ENV{GCP_PROJECT})
  message(FATAL_ERROR "GCP_PROJECT env. variable is not set")
endif()

option(IDC_INDEX_DATA_GENERATE_PARQUET "Generate idc_index.parquet file" OFF)

set(download_dir "${PROJECT_BINARY_DIR}")

add_custom_command(
  OUTPUT
    ${download_dir}/idc_index.csv.zip
    $<$<BOOL:IDC_INDEX_DATA_GENERATE_PARQUET>:${download_dir}/idc_index.parquet>
  COMMAND python ${CMAKE_CURRENT_SOURCE_DIR}/scripts/python/idc_index_data_manager.py
    --generate-csv-archive
    $<$<BOOL:IDC_INDEX_DATA_GENERATE_PARQUET>:--generate-parquet>
)

add_custom_target(run_idc_index_data_manager ALL
  DEPENDS
    ${download_dir}/idc_index.csv.zip
    $<$<BOOL:IDC_INDEX_DATA_GENERATE_PARQUET>:${download_dir}/idc_index.parquet>
)

install(
  FILES
    ${download_dir}/idc_index.csv.zip
    $<$<BOOL:IDC_INDEX_DATA_GENERATE_PARQUET>:${download_dir}/idc_index.parquet>
  DESTINATION "idc_index_data")
