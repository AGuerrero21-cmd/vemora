#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 17 13:08:21 2022

@author: aleja
"""
import json
import csv

#Reads the Smithsonian DB of eruptions and returns year, and magnitude arrays
def read_events (filename):
    try:
        # Opening JSON file
        f = open(filename,encoding='utf-8')
        print(f)
        # returns JSON object as
        # a dictionary
        data = json.load(f)
        year=[]
        vei=[]
        for i in range (len(data['features'])-1):
            year.append(int(data['features'][i]['properties']['StartDateYear']))
            vei.append((data['features'][i]['properties']['ExplosivityIndexMax']))
     
        return([year,vei])
    except Exception as e:
        print(e)
   
def analogues_to_csv(dict_volca):
    with open('dct.csv', 'w') as f:  
    
        headers=['ID','Name','General_Type', 'Tectonic_Settings','Rock_Type','Total_eruptions','VEI_None','VEI_0','VEI_1','VEI_2','VEI_3','VEI_4','VEI_5','VEI_6']
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for volcano, info in dict_volca.items():
            print(volcano,info)
