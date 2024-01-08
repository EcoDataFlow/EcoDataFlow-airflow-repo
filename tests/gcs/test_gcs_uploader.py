# from google.cloud import storage
# from google.cloud.exceptions import NotFound
#
# # Replace 'your_gcs_conn_id' with the connection ID you set up in Airflow for GCS
# GCS_CONN_ID = 'your_gcs_conn_id'
# BUCKET_NAME = 'your_destination_bucket'
#
#
# def is_file_in_gcs(bucket_name, file_path):
#     try:
#         bucket = storage.Client().bucket(bucket_name)
#         blob = bucket.blob(file_path)
#         blob.reload()  # This will raise an exception if the blob doesn't exist
#         return True
#     except NotFound:
#         return False
#
#
# if __name__ == '__main__':
#     assert is_file_in_gcs(''), True
