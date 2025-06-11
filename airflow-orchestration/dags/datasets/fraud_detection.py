from airflow import Dataset

fraud_detection_dataset = Dataset(uri="smb://fraud-detection-dataset/raw")
