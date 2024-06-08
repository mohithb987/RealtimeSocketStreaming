import json
import socket
import time
import pandas as pd

local_host = '127.0.0.1'

def send_data_over_socket(file_path, host=local_host, port=9999, chunk_size=2): # pull 2 records at once from source
    s = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM) # address family = IPv4, type = stream oriented TCP socket
    s.bind((host, port))
    s.listen(1) # listen to one connection at a time
    print(f'Listening for connections on host:{host}, port:{port}')
    
    conn, addr = s.accept()
    print(f'Accepted connection request from client: {addr}')

    try:
        last_sent_index = 0
        with open(file_path, 'r') as file:
            # skip the lines that were sent to client already
            for _ in range(last_sent_index):
                next(file)
            
            records = []
            for i, line in enumerate(file):
                records.append(json.loads(line))
                if(len(records) == chunk_size):
                    chunk = pd.DataFrame(records)
                    print(f'Chunk {i//chunk_size} has been sent.')
                    print(chunk)
                    for record in chunk.to_dict(orient='records'):
                        serialized_data = json.dumps(record).encode('utf-8')
                        conn.send(serialized_data + b'\n') # socket sends data only when the line ends in serialized record
                        time.sleep(5)
                        last_sent_index = i
                    
                    records = [] # reset records after sending a chunk

    except (BrokenPipeError, ConnectionResetError):
        print('Client has disconnected')
    
    finally:
        conn.close()
        print('Connection closed.')

if __name__ == "__main__":
    send_data_over_socket('src/datasets/yelp_academic_dataset_review.json', local_host, 9999, 2)