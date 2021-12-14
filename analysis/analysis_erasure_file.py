import csv
import json
import time

import requests

base_url = 'http://localhost:9000'
iterations = 50
files = ['10KB', '100KB', '1MB', '10MB', '100MB']
result_file = "results_erasure.csv"
fields = ['id',
          'file_size',
          'write_time',
          'write_status_code',
          'read_time',
          'read_status_code',
          'storage_mode',
          'max_erasures']

# Open csv file
with open(f'./results/{result_file}', 'w') as csvfile:
    csv_writer = csv.writer(csvfile)

    # Write first row of csv
    csv_writer.writerow(fields)

    for storage_mode in ['erasure_coding_rs', 'erasure_coding_rs_random_worker']:
        for max_erasure in [1, 2]:
            # For each file size (10kB, 100kB, 1MB, 10MB and 100MB)
            for file in files:
                with open(f'./test_files/{file}.txt', 'r') as f:
                    # Perform iterations
                    for i in range(iterations):
                        multipart_form_data = {
                            'file': f,
                            'storage': (None, storage_mode),
                            'max_erasures': (None, max_erasure),
                        }

                        # Upload file
                        start_time = time.time()
                        write_response = requests.post(f'{base_url}/files_mp', files=multipart_form_data)
                        end_time = time.time()
                        write_total_time = end_time - start_time
                        id = json.loads(write_response.text)['id']

                        # Download file
                        start_time = time.time()
                        read_response = requests.get(f'{base_url}/files/{id}')
                        end_time = time.time()
                        read_total_time = end_time - start_time

                        # Save entry in CSV
                        csv_writer.writerow([id,
                                             file,
                                             write_total_time,
                                             write_response.status_code,
                                             read_total_time,
                                             read_response.status_code,
                                             storage_mode,
                                             max_erasure
                                             ])
                        f.seek(0)

