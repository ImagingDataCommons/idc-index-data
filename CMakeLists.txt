cmake_minimum_required(VERSION 3.15...3.26)
project(${SKBUILD_PROJECT_NAME} LANGUAGES NONE)

find_package(
  Python
  COMPONENTS Interpreter
  REQUIRED)

if(NOT DEFINED ENV{GCP_PROJECT})
  message(FATAL_ERROR "GCP_PROJECT env. variable is not set")
endif()

set(download_dir "${PROJECT_BINARY_DIR}")

add_custom_command(
   OUTPUT
     ${download_dir}/idc_index.csv.zip
     #${download_dir}/idc_index.parquet
   COMMAND python ${CMAKE_CURRENT_SOURCE_DIR}/scripts/python/idc_index_data_manager.py
)

add_custom_target(run_idc_index_data_manager ALL
   DEPENDS ${download_dir}/idc_index.csv.zip
   #${download_dir}/idc_index.parquet
)

install(FILES ${download_dir}/idc_index.csv.zip DESTINATION "idc_index_data")
#install(FILES ${download_dir}/idc_index.parquet DESTINATION "idc_index_data")
