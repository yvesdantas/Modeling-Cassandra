import re
import os
import glob
import json
import csv

class Data_processing:
    
    def __init__(self):
        
        self.filepath = os.getcwd() + '/event_data/'
        self.file_path_list = glob.glob(self.filepath + '*.csv')
        
    def csv_reader(self):
        
        full_data_rows_list = [] 
     
        for f in self.file_path_list: 
            with open(f, 'r', encoding = 'utf8', newline='') as csvfile:  
                csvreader = csv.reader(csvfile) 
                next(csvreader)

                for line in csvreader:
                    full_data_rows_list.append(line)
                    
        return full_data_rows_list
    
    def csv_generator(self, full_data_rows_list):

        csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

        with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
            writer = csv.writer(f, dialect='myDialect')
            writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',\
                        'level','location','sessionId','song','userId'])
            for row in full_data_rows_list:
                if (row[0] == ''):
                    continue
                writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))
        
        with open('event_datafile_new.csv', 'r', encoding = 'utf8') as f:
            print(sum(1 for line in f), "lines created")

        
def main():
        
    data = Data_processing()
    data.csv_generator(data.csv_reader())
    
if __name__ == '__main__':
    main()
    
    