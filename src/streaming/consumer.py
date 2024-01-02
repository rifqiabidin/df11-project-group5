from confluent_kafka.avro import AvroConsumer
from google.cloud import bigquery
import os
from datetime import datetime
from google.cloud import storage
import csv
from io import StringIO
from datetime import datetime

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/ivandwinugraha45/streaming/df11-group5-678871138dac.json"

def read_messages():
    consumer_config = {
        "bootstrap.servers": "localhost:9092",
        "schema.registry.url": "http://localhost:8081",
        "group.id": "bankmarketing.avro.consumer.2",
        "auto.offset.reset": "earliest",
    }

    consumer = AvroConsumer(consumer_config)
    consumer.subscribe(["data_bank_marketing"])

    gcs_client = storage.Client()
    bucket_name = "datalake_bank"
    bucket = gcs_client.bucket(bucket_name)
    gcs_path = "marketing_bank"

    timestamp_data = {}  # Kamus untuk menyimpan pesan dengan timestamp yang sama
    current_date = None

    include_header = True  # Variabel untuk mengontrol apakah header harus disertakan

    while True:
        try:
            message = consumer.poll(5)
            print("Polling for messages...")
        except Exception as e:
            print(f"Exception while trying to poll messages - {e}")
        else:
            if message:
                print(
                    f"Successfully poll a record from "
                    f"Kafka topic: {message.topic()}, partition: {message.partition()}, offset: {message.offset()}\n"
                    f"message key: {message.key()} || message value: {message.value()}"
                )

                # Konversi data ke dalam format CSV
                csv_data = convert_to_csv(message.value(), include_header)

                # Dapatkan timestamp dari data (format timestamp dalam 'DD/MM/YYYY HH:MM:SS')
                timestamp_str = message.value().get("timestamp", "")
                timestamp = datetime.strptime(timestamp_str, "%d/%m/%Y %H:%M:%S").date()

                # Cek apakah tanggal berubah
                if current_date and current_date != timestamp:
                    # Simpan data untuk hari yang sama ke file CSV
                    combined_csv_data = timestamp_data.get(current_date, '')
                    blob = bucket.blob(
                        f"{gcs_path}/{current_date}/combined_data_{current_date}.csv"
                    )
                    blob.upload_from_string(combined_csv_data, content_type="text/csv")

                    # Kosongkan kamus setelah mengunggah data
                    timestamp_data[current_date] = ''

                # Tambahkan data ke kamus berdasarkan timestamp
                timestamp_data.setdefault(timestamp, '')
                timestamp_data[timestamp] += csv_data

                current_date = timestamp
                consumer.commit()
                include_header = False  # Set include_header ke False setelah header dicetak
            else:
                print("No new messages at this point. Try again later.")

    # Tutup consumer di luar loop
    consumer.close()

def convert_to_csv(data, include_header=True):
    # Fungsi untuk mengonversi data menjadi format CSV
    # Misalnya, Anda memiliki data dalam bentuk dictionary
    # dan ingin mengonversinya menjadi baris CSV
    csv_columns = ["timestamp", "email", "first_name", "last_name", "birth_date", "job", "marital", "education", "default", "housing", "loan"]
    
    csv_data = StringIO()
    csv_writer = csv.DictWriter(csv_data, fieldnames=csv_columns)
    
    # Cetak header hanya jika include_header bernilai True
    if include_header:
        # Periksa apakah CSV kosong (berukuran nol)
        csv_data.seek(0, os.SEEK_END)
        is_empty = csv_data.tell() == 0

        if is_empty:
            csv_writer.writeheader()

    # Tulis data ke dalam CSV
    csv_writer.writerow(data)
    
    return csv_data.getvalue()

if __name__ == "__main__":
    read_messages()