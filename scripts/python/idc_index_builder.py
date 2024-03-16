import os
from idc_index_data_manager import IDCIndexDataManager

project_id = os.environ["GCP_PROJECT_ID"]
manager = IDCIndexDataManager(project_id)
latest_idc_release_version = manager.retrieve_latest_idc_release_version()
manager.update_sql_queries_folder(
        "scripts/sql/", latest_idc_release_version
    )
manager.run_queries_folder("scripts/sql/")
manager.set_multiline_output("latest_idc_release_version", int(latest_idc_release_version))
