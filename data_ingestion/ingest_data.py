import os
import requests

def download_genomic_data(url, output_path):
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(output_path, 'wb') as file:
            for chunk in response.iter_content(chunk_size=1024):
                file.write(chunk)
    else:
        print(f"Failed to download data from {url}")

if __name__ == "__main__":
    url = "http://example.com/genomic_data.gz"
    output_path = "data/genomic_data.gz"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    download_genomic_data(url, output_path)
