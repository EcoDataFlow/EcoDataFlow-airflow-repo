import tempfile, logging
from os import path

from airflow.providers.google.cloud.hooks.gcs import GCSHook


class GCSUploader:
    def __init__(self, gcp_conn_id, bucket_name) -> None:
        self.gcp_conn_id = gcp_conn_id
        self.bucket_name = bucket_name

    def upload_dataframe_to_gcs(self, dataframe, object_name, file_name):
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = path.join(tmp_dir, file_name)
            dataframe.to_csv(tmp_path, index=False)
            gcs_hook = GCSHook(self.gcp_conn_id)
            gcs_hook.upload(bucket_name=self.bucket_name, object_name=object_name, filename=tmp_path)
            logging.info(gcs_hook.client_info)


if __name__ == '__main__':
    import pandas as pd

    data = {'column1': [1, 2, 3], 'column2': ['A', 'B', 'C']}
    df = pd.DataFrame(data)
    csv_data = df.to_csv('wk-test.csv', index=False)

    gcs_uploader = GCSUploader("google_cloud_conn_id", 'data-lake-storage')
    gcs_uploader.upload_dataframe_to_gcs(df, 'wk-test.csv', 'wk-test.csv')
