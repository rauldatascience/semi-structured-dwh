import glob
import json
from datetime import date

current_date = date.today()

file_name = str(current_date)


list_path_file = glob.glob("/Users/macbookpro/Documents/qoala/movies/movies/*.json")

read_files = list_path_file[0:10000]

with open('merged_file_movies_'+file_name+'.json', "w") as outfile:
     for f in read_files:
          with open(f, "r") as infile:
               outfile.write(infile.read())
               outfile.write('\n')

list_path_file = glob.glob("/Users/macbookpro/Documents/qoala/series/series/*.json")

read_files = list_path_file[0:10000]

with open('merged_file_series_'+file_name+'.json', "w") as outfile:
     for f in read_files:
          with open(f, "r") as infile:
               outfile.write(infile.read())
               outfile.write('\n')


