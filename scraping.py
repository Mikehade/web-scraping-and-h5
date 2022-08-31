#pip install beautifulsoup4
#pip install html-table-parser-python3
from bs4 import BeautifulSoup
import requests
import pandas as pd 
import numpy as np
from selenium import webdriver
from selenium.webdriver.support.ui import Select
import time
import calendar
from datetime import date
#from datetime import time
from datetime import datetime
import schedule
import matplotlib.pyplot as plt
#import pytables

from os.path import exists

import urllib.request
 
# pretty-print python data structures
from pprint import pprint
 
from html_table_parser.parser import HTMLTableParser
#from html_table_parser import HTMLTableParser

import re

import h5py


link1 = "https://www.x-rates.com/table/?from=EUR&amount=1"
link2 = "https://www.bankofcanada.ca/rates/exchange/daily-exchange-rates/"
link3 = "https://bankofindia.co.in/Dynamic/ForexCardRate"

links_dict = {"link1": link1, "link2": link2, "link3": link3}

def chexist(datapath):
    if exists("links.csv") is False:
        return False
    else:
        return True


if chexist("links.csv") is False:
    file = open("links.csv", "a")
    keys_list = links_dict.keys()
    for keys in keys_list:
        #print(links_dict[keys])
        file.write(f"{links_dict[keys]} \n")
    file.close()

else:
    pass
    
link_list = []
file = open("links.csv", "r")
for rows in file:
    link_list.append(rows)
file.close



def main():
    
    
        
    
    option = input("What would you like to do? write, read or analyse:  ")
    
    if option.lower() == "write":
        print("writing to file...")
        
        #loop_func()
        

            
        
        while True:
            try:
                #scheduler
                schedule.every().day.at("21:00").do(loop_func)  #scrape data everyday at 9pm
                schedule.run_pending()
            except EOFError:
                break
            except KeyboardInterrupt:
                break
        
        
        
    elif option.lower() == "read" or option.lower() == "analyse":
        
        gro = int(input(f"Which data group would you like to {option.lower()}? 1, 2, or 3: "))
        
        if gro in [1,2,3]:
           
            data_df = read_data(gro)  #call read data function
            count = 0
            for data in data_df.values():   #for loop to concatenate dataframes from specific group
                if count == 0:
                    df = data
                else:
                    df = pd.concat([df, data], ignore_index=True)
                count += 1
                
            if option.lower() == "read":
                print("reading from file...")
                print(df)
            else:
                df = df.set_index("Timestamp")
                print("Date Start: ", df.index[0])
                print("Date End: ", df.index[-1])
                print("Amount of Days: ", df.index[-1] - df.index[0])
                increase_per_day = pd.Series(np.ones(len(df.index)), index = df.index).resample('d').sum()
                increase_per_day.plot(title="Increase per day")  #
                #plt.savefig("third series.png")
                
        else:
            print("Invalid Input")
        
        
    #elif option.lower() == "analysis":
        #print("analyzing data...")
        
        #df = df.set_index("Timestamp")
    
    else:
        print("Incorrect option please type write, read or analyse")
        return
    


def input_data(group, df, day):  #group (first, second..), df = dataframe, day = 1,2....
    """
    function to input data into hdf file
    """
    with pd.HDFStore("data.h5", "a") as hdf:
        if day < 365:
            pass
        else:
            return
        print(f"{group}/day_{day}")
        hdf.put(f"/{group}/day_{day}", df, format = 'table', data_columns=True)
        
def url_get_contents(url):
    """
    Function to get data from websites"""
    #to make request from the website
    req = urllib.request.Request(url=url)
    f = urllib.request.urlopen(req)
    #to read contents of the website
    return f.read()

def scraping(day):
    time.sleep(60)
    for i in range(len(link_list)):

        xhtml = url_get_contents(link_list[i]).decode('utf-8')   #link_list[i]
        p = HTMLTableParser()

        # feeding the html contents to the HTMLTableParser object
        p.feed(xhtml)
        #print(p)

        table = p.tables[-1]
        df = pd.DataFrame(table)
        df.columns = df.iloc[0]   #change name of name to first row in the columns
        df = df.iloc[1:, :]  #remove first row
        df["Timestamp"] = datetime.now()  #.date()

        group_dict = {0:"first", 1:"second", 2:"third"}  #, 3:"forth"
        
        group = str(group_dict[i])
        print(group)
        print(df)
        print("I: ",i)
        input_data(group, df, day)


        
def loop_func():
    start_date = date(2022,8,21)
    #t = datetime.now()
    t = datetime.now().date()
    days = t - start_date
    day_str =  str(days)
    d = int(day_str.split(" ")[0])
    
    
    print(t)
    print(type(days))
    print(d)
    scraping(d)  #call scraping function daily

def read_data(gr):
    
    """
    function to read data from hdf5 file"""
    with pd.HDFStore("data.h5", "r") as hdf:
        #condtion to verify input for group selection
        if gr == 1:
            group = "first"
        elif gr == 2:
            group = "second"
        elif gr == 3:
            group = "third"
        else:
            print("Invalid group")
            return
        datasets = list(hdf.items()) #get keys of group in file in form of list
        count = 0
        data_dict = {}  #initialize dictionary to store dataframes
        #for loop to iterate through datasets
        for keys in datasets:
            key = keys[0]  #get data path e.g. day_1, day_2
            subkey = key.split("/")[1]    #get current group
            data = hdf.get(f"/{key}")
            #if current group is same as selected group, load dataset
            if subkey == group:
                count += 1
                
                data_dict[count] = data
                
        return data_dict  
    
  
    
    
    
main()