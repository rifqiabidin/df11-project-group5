import gspread
from oauth2client.service_account import ServiceAccountCredentials
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
from time import sleep

# Konfigurasi kunci API dan akses
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name('/home/ivandwinugraha45/streaming/df11-group5-678871138dac.json')

gc = gspread.authorize(credentials)

# Tentukan ID spreadsheet
spreadsheet_id = '1DLIiR135fsZOb6Kt-AI_QnajUzjmgl_rk2xo097554w'


def load_avro_schema_from_file():
    key_schema = avro.load("avro_key.avsc")
    value_schema = avro.load("avro_value.avsc")

    return key_schema, value_schema


def send_record():
    key_schema, value_schema = load_avro_schema_from_file()

    producer_config = {
        "bootstrap.servers": "localhost:9092",
        "schema.registry.url": "http://localhost:8081",
        'compression.type': 'gzip',  # atau 'gzip', 'lz4'
        'acks': 'all',
        'batch.size': 15240,  # Ukuran batch dalam byte
        # 'linger.ms': 1,
        # 'num.partitions': 4,  # Jumlah partisi
    }

    producer = AvroProducer(producer_config, default_key_schema=key_schema, default_value_schema=value_schema)

    # Buka spreadsheet berdasarkan ID
    worksheet = gc.open_by_key(spreadsheet_id).sheet1

    # Get all values from each column
    all_values = worksheet.get_all_values()

    header = all_values[0]
    for row in all_values[1:]:  # Skip the header row
        key = {"email": row[1]}  # Assuming the email is in the first column
        value = {
            "timestamp": row[0],  # Adjust the index based on your actual data
            "email": row[1],
            "first_name": row[2],
            "last_name": row[3],
            "birth_date": row[4],
            "job": row[5],
            "marital": row[6],
            "education": row[7],
            "default": row[8],
            "housing": row[9],
            "loan": row[10]
        }

        try:
            producer.produce(topic='data_bank_marketing', key=key, value=value)
        except Exception as e:
            print(f"Exception while producing record value - {value}: {e}")
        else:
            print(f"Successfully producing record value - {value}")

        producer.flush()
        sleep(1)

if __name__ == "__main__":
    send_record()
