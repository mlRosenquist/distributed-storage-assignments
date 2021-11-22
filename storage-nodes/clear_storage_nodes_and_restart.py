import os
import shutil

folders = ['1', '2', '3', '4']

for folder in folders:
    if(os.path.isdir(folder)):
        shutil.rmtree(folder)