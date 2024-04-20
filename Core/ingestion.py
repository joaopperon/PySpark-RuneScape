import os
import requests

from json import dump
from Core.constants import *

def get_runescape_data(num_of_beasts):
    
    if not os.path.exists(JSON_DATA_PATH):
        os.makedirs(JSON_DATA_PATH)
    
    for id in range(1, num_of_beasts + 1):
        
        beast_data = requests.get(RUNESCAPE_BASE_URL + str(id))
        
        if beast_data.status_code == 200:
            if beast_data.text != '':
                with open(f"{JSON_DATA_PATH}/{id}.json", 'w') as file:
                    dump(beast_data.json(), fp=file)
            
            else:
                print(f"Beast data for ID {id} returned empty. Status code: {beast_data.status_code}")
        else:
            print(f"Failed to fetch data for beast with ID {id}. Status code: {beast_data.status_code}")

    
