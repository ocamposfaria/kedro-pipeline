"""
This is a boilerplate pipeline 'data_processing'
generated using Kedro 0.18.4
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import download_and_unzip, aggregate_files, one_hot_encoding_asset_types, prepare_to_postgresql

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=download_and_unzip,
                inputs="parameters",
                outputs=["cda_fi_BLC_1", "cda_fi_BLC_2", "cda_fi_BLC_3", "cda_fi_BLC_4", "cda_fi_BLC_5", "cda_fi_BLC_6", "cda_fi_BLC_7", "cda_fi_BLC_8", "cda_fi_CONFID", "cda_fi_PL"], 
                name="download_and_unzip_CDA_files"
            ),
            node(
                func=aggregate_files,
                inputs=["cda_fi_BLC_1", "cda_fi_BLC_2", "cda_fi_BLC_3", "cda_fi_BLC_4", "cda_fi_BLC_5", "cda_fi_BLC_6", "cda_fi_BLC_7", "cda_fi_BLC_8"],
                outputs="aggregated_file", 
                name="aggregate_files"
            ),
            node(
                func=one_hot_encoding_asset_types,
                inputs="aggregated_file",
                outputs="one_hot_encoded_asset_types", 
                name="one_hot_encoding_asset_types"
            ),
            node(
                func=prepare_to_postgresql,
                inputs="aggregated_file",
                outputs="postgres_table", 
                name="export_to_postgresql"
            )
        ]
    )