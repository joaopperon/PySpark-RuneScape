import os
from Core.constants import *

def erase_input_files():
    for file in os.listdir(JSON_DATA_PATH):
        os.remove(os.path.join(JSON_DATA_PATH, file))
