# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
from owslib.wfs import WebFeatureService
from owslib.etree import etree
from owslib.fes import *
import json
import io

#*********************** VARIABLES ASSIGNATION**********************


url="https://webservices.volcano.si.edu/geoserver/GVP-VOTW/ows?service=WFS" #DB URL
path_data="data/"

#*****************FUNCTIONS******************


#Eruption filter by volcano ID
def filter_data_byid(wfs11,volcanoid):
    filter = PropertyIsLike(propertyname='Volcano_Number', literal=str(volcanoid), wildCard='*')
    filterxml = etree.tostring(filter.toXML()).decode("utf-8")
    response = wfs11.getfeature(typename='GVP-VOTW:Smithsonian_VOTW_Holocene_Eruptions', filter=filterxml, outputFormat='json')
    return (response)

#Eruption filter by box coordinates
def filter_bybbox(wfs11,list_coords):
    try:
        response = wfs11.getfeature(typename='GVP-VOTW:Smithsonian_VOTW_Holocene_Eruptions', bbox=(list_coords[3],list_coords[1],list_coords[2],list_coords[0]), srsname='urn:x-ogc:def:crs:EPSG:4326')
        return response
    except Exception as e:
        print (e)
    
#Smithsonian Server Connection
def connect_wfs(volcanoid):
    try:
        wfs11 = WebFeatureService(url, version='1.1.0')
        #operations and contents
        #print([operation.name for operation in wfs11.operations])
        #print(list(wfs11.contents))
        #filter only volcano eruptions
        response=filter_data_byid(wfs11,volcanoid)
        responsev=volcano_data(wfs11,volcanoid)
        #response=filter_bybbox(wfs11, list_coords)
        
        volcano_json=file_define(volcanoid,response)
        volcano_eruptions_json=volcano_data_file(volcanoid,responsev)
        return(volcano_json,volcano_eruptions_json)
    except Exception as e:
        print("....")
        print(e)
        print("Connection lost. Please check internet and Server values")
        return 0
#Json File Creation for eruption   
def file_define(volcanoid,response):
    try:
       lista=[]
       json_response=json.load(response)
       for element in json_response['features']:
           eruption_year=element['properties']['StartDateYear']
           #Checks the year if the event, if the event has not a year assigned this will be discarded
           if(eruption_year!=None):
               eruption_id=element['properties']['Eruption_Number']
               
               eruption_year_error=element['properties']['StartDateYearUncertainty']
               eruption_VEI=element['properties']['ExplosivityIndexMax']
               eruption_month=element['properties']["StartDateMonth"] 
               if(eruption_month==None):
                   eruption_month=0
               entry={
                       "_id":eruption_id,
                       "volcano":volcanoid,
                       "year":int(eruption_year),
                       "error":eruption_year_error,
                       "month":(int(eruption_month)),
                       "VEI":eruption_VEI,
                       "volume":None,
                       "mass":None,
                       "magnitude_mastin":None,
                       "energy":None,
                       "magnitude":None,
                       "biblio":["https://doi.org/10.5479/si.GVP.VOTW5-2023.5.1"]
                   }
               lista.append(entry)
        
       
       return(lista)
   
           
       
    except Exception as e:
        print(e)
        print("Error: Response without content, please check the input data")

def volcano_data(wfs11,volcanoid):
    filter = PropertyIsLike(propertyname='Volcano_Number', literal=str(volcanoid), wildCard='*')
    filterxml = etree.tostring(filter.toXML()).decode("utf-8")
    response = wfs11.getfeature(typename='GVP-VOTW:Smithsonian_VOTW_Holocene_Volcanoes', filter=filterxml, outputFormat='json')
    return (response)
        
 #Define json volcano to add to DB       
def volcano_data_file(volcanoid,response):
        try:
            json_response=json.load(response)
            volcano_name=json_response['features'][0]['properties']['Volcano_Name']
            volcano_location=json_response['features'][0]['geometry']['coordinates']
            general_volcano_type=json_response['features'][0]['properties']['Primary_Volcano_Type']
            volcano_elevation=json_response['features'][0]['properties']['Elevation']
            volcano_country=json_response['features'][0]['properties']['Country']
            volcano_tectonics=json_response['features'][0]['properties']['Tectonic_Setting']
            volcano_rock_type=json_response['features'][0]['properties']['Major_Rock_Type']
            dicti={
                "volcano_id":volcanoid,
                "name":volcano_name,
                "country":volcano_country,
                "latitude":volcano_location[0],
                "longitude":volcano_location[1],
                "elevation":volcano_elevation,
                "general_type":general_volcano_type,
                "tectonic_setting":volcano_tectonics,
                "rock_type":volcano_rock_type,
                "classification":"None"
                
                }
            return(dicti)
        except Exception as e:
            print(e)
            print("Error: Response without content, please check the input data")
        
        
def filter_data_by_parameter(wfs11,volcanoid):
    filter = PropertyIsLike(propertyname='Volcano_Number', literal=str(volcanoid), wildCard='*')
    filterxml = etree.tostring(filter.toXML()).decode("utf-8")
    response = wfs11.getfeature(typename='GVP-VOTW:Smithsonian_VOTW_Holocene_Eruptions', filter=filterxml, outputFormat='json')
    return (response)
