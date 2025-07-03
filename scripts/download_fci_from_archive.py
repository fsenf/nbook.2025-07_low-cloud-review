# Copyright (C) 2025 EUMETSAT
#
# This program is free software: you can redistribute it and/or modify it under the terms of the
# GNU General Public License as published by the Free Software Foundation, either version 3 of the License,
# or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
# See the GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along with this program.
# If not, see <https://www.gnu.org/licenses/>.

import datetime
import fnmatch
import os
import shutil
from datetime import datetime, timedelta

import eumdac
import requests

from get_fci_chunks_for_area import get_chunks_for_lon_lat_bbox

from multiprocessing import Pool

N_PARALLEL_DOWNLOADS = 4

# This function checks if a product entry is part of the requested coverage
def get_coverage(coverage, filenames):
    chunks = []
    for pattern in coverage:
        for file in filenames:
            if fnmatch.fnmatch(file, pattern):
                chunks.append(file)
    return chunks


def filter_chunks(chunks_list, entries):
    filtered_entries = []
    chunk_patterns = [f"*_????_00{str(chunk).zfill(2)}.nc" for chunk in chunks_list]
    for entry in entries:
        for chunk_pattern in chunk_patterns:
            if fnmatch.fnmatch(entry, chunk_pattern):
                filtered_entries.append(entry)
    return filtered_entries


def download_file(args):
    product, file, output_folder = args
    try:
        with product.open(entry=file) as fsrc, \
                open(os.path.join(output_folder, fsrc.name), mode='wb') as fdst:
            shutil.copyfileobj(fsrc, fdst)
            print(f'Download of file {os.path.join(output_folder, fsrc.name)} finished.')
    except eumdac.product.ProductError as error:
        print(f"Error related to the product '{product}' while trying to download it: '{error}'")
    except requests.exceptions.ConnectionError as error:
        print(f"Error related to the connection: '{error}'")
    except requests.exceptions.RequestException as error:
        print(f"Unexpected error: {error}")

def main_download_fci_from_archive(start_time, end_time, collection_id, lonlat_bbox, output_folder, run_name, eumdac_key, eumdac_secret):
    print("Downloading collection_id ", collection_id, " and time range ", start_time, " to ", end_time, ".")
    
    # Feed the token object with your credentials
    credentials_eumdac = (eumdac_key, eumdac_secret)
    token = eumdac.AccessToken(credentials_eumdac)
    
    # Create datastore object with your token
    datastore = eumdac.DataStore(token)
    
    # Select an FCI collection, eg "FCI Level 1c High Resolution Image Data - MTG - 0 degree" - "EO:EUM:DAT:0665"
    selected_collection = datastore.get_collection(collection_id)
    
    isl2 = 'L2' in selected_collection.product_type
    
    # add and remove one second to avoid retrieval of too many L2 products
    start_time = datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%S")
    end_time = datetime.strptime(end_time, "%Y-%m-%dT%H:%M:%S")
    
    if isl2:
        start_time += timedelta(seconds=1)
        end_time -= timedelta(seconds=1)
    
    # Retrieve datasets that match the filter
    products = selected_collection.search(
        dtstart=start_time,
        dtend=end_time)
    
    print("Found products:")
    for product in products:
        print(product)
    
    if len(products) == 0:
        print("WARNING: No products found for collection_id ", collection_id, " and time range ", start_time, " to ", end_time, ". Exiting.")
        return
    
    output_folder = os.path.join(output_folder, run_name, 'fci_l1c_input_data')
    os.makedirs(output_folder, exist_ok=True)
    
    if not isl2:
        chunks_list = get_chunks_for_lon_lat_bbox(lonlat_bbox)
        print(f"Will be retrieving chunks: {chunks_list}")
    
        download_tasks = []
        for product in products:
            for file in filter_chunks(chunks_list, product.entries):
                download_tasks.append((product, file, output_folder))
                
    else:
        download_tasks = []
        for product in products:
            for file in product.entries:
                download_tasks.append((product, file, output_folder))
    
    # start with older data
    download_tasks = reversed(download_tasks)
    with Pool(N_PARALLEL_DOWNLOADS) as pool:
        pool.map(download_file, download_tasks)


if __name__ == "__main__":

    #########

    # FCI L1c data produced on VAL after the post-COM anomaly reactivation from 23/05/2024 to 23/07/2024 is stored permanently under the Special scan collections
    # FCI-1C-RRAD-FDHSI-xx (EO:EUM:DAT:0664:COM)
    # FCI-1C-RRAD-HRFI-xx  (EO:EUM:DAT:0667:COM)
    #
    # All the other L1c commissioning data from 24/10/2023 until the 23/09/2024 is stored under
    # FDHSI: EO:EUM:DAT:0662:COM
    # HRFI: EO:EUM:DAT:0665:COM
    #
    # L1c data since the (pre-)op dissemination start from 24/09/2024 on is stored under
    # FDHSI: EO:EUM:DAT:0662
    # HRFI: EO:EUM:DAT:0665
    
    # FCI cloud mask: EO:EUM:DAT:0678
    # FCI OCA: EO:EUM:DAT:0684

    ########
    
    eumdac_key = os.environ.get('EUMDAC_KEY')
    eumdac_secret = os.environ.get('EUMDAC_SECRET')


    # geographical bounds of search area
    # minLon minLat maxLon maxLat
    lonlat_bbox = [ 6, 50, 12, 55] #N Germany scene
    # lonlat_bbox = None

    output_folder = "/work/bb1376/user/fabian/data/"
    run_name = "fci"
    
    # date/times for the N Germany case. 
    # you can adjust the start/end time depending how many frames you need. Data are every 10min
    start_time = "2025-06-19T06:30:00"
    end_time = "2025-06-19T06:50:00"
    
    collection_ids = [
        "EO:EUM:DAT:0678",  # CLM,
        "EO:EUM:DAT:0684",  # OCA
        "EO:EUM:DAT:0662",  # FDHSI
        "EO:EUM:DAT:0665",  # HRFI
    ]
    
    for collection_id in collection_ids:
        main_download_fci_from_archive(start_time, end_time, collection_id, lonlat_bbox, output_folder, run_name, eumdac_key, eumdac_secret)
