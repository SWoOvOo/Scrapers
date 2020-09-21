# -*- coding: utf-8 -*-
"""
Foursquare super scraper for all venues of all categories

Created on Mon Sep 21 21:05:25 2020

@author: Siwei Luo
"""

#load packages needed for the functions
import pandas as pd
import math
import requests
from pandas.io.json import json_normalize

#Function extracting the category of the venue, used for the processing within the scraper functions
def get_category_type(row):
    try:
        categories_list = row['categories']
    except:
        categories_list = row['venue.categories']
        
    if len(categories_list) == 0:
        return None
    else:
        return categories_list[0]['name']

#Recursive downloading function for searching venues of certain category
#Returns an organized pandas dataframe of venue ID, name, category, latitude and longitude
def RD(latitude,longitude,SearchRadius,categoryID,LIMIT=50,CLIENT_ID,CLIENT_SECRET,VERSION,f_t=0):
    lat = latitude
    lng = longitude
    radius = SearchRadius
    #define URL
    url = 'https://api.foursquare.com/v2/venues/search?client_id={}&client_secret={}&ll={},{}&radius={}&v={}&limit={}&categoryId={}'.format(CLIENT_ID,CLIENT_SECRET,lat,lng,radius,VERSION,LIMIT,categoryID)
    #download results
    try:
        results = requests.get(url).json()
        response = results['response']
        #If there is at least a required venue
        if len(response)>=1:
            venues = results['response']['venues']
            if len(venues)>=1:
                #transform to dataframe
                df_venues = json_normalize(venues)
                #data frame wraggling
                df_venues['categories'] = df_venues.apply(get_category_type, axis=1)
                df_venues = df_venues.loc[:,['id','name','categories','location.lat','location.lng']]
                df_venues = df_venues[df_venues['categories']!='Moving Target']
                df_venues.columns = ['ID','name','categories','latitude','longitude']
            else:
                df_venues = pd.DataFrame(columns = ['ID','name','categories','latitude','longitude'])
        else:
            df_venues = pd.DataFrame(columns = ['ID','name','categories','latitude','longitude'])
        return df_venues
    except:
        f_t = f_t + 1
        if f_t <= 30:
            print('failed!...Retry:',f_t)
            return RD(lat,lng,radius,categoryID,LIMIT,CLIENT_ID,CLIENT_SECRET,VERSION,f_t)
        else:
            print('failed more than 30 times! Please check and restart the downloading process from the returned dataframe!')
            df_venues = pd.DataFrame(['Failed!...radius=',radius,categoryID,lat,lng],columns = ['ID','name','categories','latitude','longitude'])

#Function for progressively creating dynamic map grids and downloading
#Need the downloading function "RD" to be defined first
#Please define west longitude > east longitude, do not support bbox cross meridian
#Returns a pandas dataframe of all venues of a user-defined bounding box
def MGD(NorthBoundary,EastBoundary,SouthBoundary,WestBoundary,categoryID,LIMIT,CLIENT_ID,CLIENT_SECRET,VERSION):
    n = NorthBoundary
    e = EastBoundary
    s = SouthBoundary
    w = WestBoundary
    venues_summary = pd.DataFrame(columns = ['ID','name','categories','latitude','longitude'])
    lat = s + 0.5*(n-s)
    lng = w + 0.5*(e-w)
    radius = ((max(n-s,e-w)/360)*(2*math.pi*6379000))/(2**0.5) #use maximum earth radius
    df_venues = RD(lat,lng,radius,categoryID,LIMIT,CLIENT_ID,CLIENT_SECRET,VERSION)
    df_maxlen = len(df_venues)
    d = 1
    while df_maxlen==LIMIT and d<=4:
        dt = 2**d
        sep1 = (n-s)/dt
        sep2 = (e-w)/dt
        radius1 = radius/dt
        print('dividing d=',d,'partsNo dt=',dt)
        list_df = []
        for i in range(dt): #north and south
            for j in range(dt): #east and west
                s1 = s + sep1*i
                n1 = s + sep1*(i+1)
                w1 = w + sep2*j
                e1 = w + sep2*(j+1)
                lat1 = s1 + 0.5*(n1-s1)
                lng1 = w1 + 0.5*(e1-w1)
                df_venues = RD(lat1,lng1,radius1,categoryID,LIMIT,CLIENT_ID,CLIENT_SECRET,VERSION)
                list_df.append(df_venues)
        df_maxlen = 0
        for df in list_df:
            df_maxlen = max(len(df),df_maxlen)
        d = d + 1
    if d > 1:
        for df in list_df:
            venues_summary = venues_summary.append(df)
    else:
        venues_summary = df_venues
    return venues_summary

#Function to get all categories of foursquare venues
#Returns a pandas dataframe of category hierarchy including all categories and their super categories
def CatD(CLIENT_ID, CLIENT_SECRET, VERSION):
    url = 'https://api.foursquare.com/v2/venues/categories?client_id={}&client_secret={}&v={}'.format(CLIENT_ID, CLIENT_SECRET, VERSION)
    results = requests.get(url).json()
    categories = results['response']['categories']
    #get the hierarchical category ID
    df_hierarchical_cat = pd.DataFrame(columns = ['categories','c5','c4','c3','c2','c1'])
    for c1 in categories:
        c1_id = c1['id']
        h_row = pd.DataFrame([[c1_id,c1_id,c1_id,c1_id,c1_id,c1_id]],columns = ['categories','c5','c4','c3','c2','c1'])
        df_hierarchical_cat = df_hierarchical_cat.append(h_row)
        for c2 in c1['categories']:
            c2_id = c2['id']
            h_row = pd.DataFrame([[c2_id,c2_id,c2_id,c2_id,c2_id,c1_id]],columns = ['categories','c5','c4','c3','c2','c1'])
            df_hierarchical_cat = df_hierarchical_cat.append(h_row)
            if len(c2['categories'])>0:
                for c3 in c2['categories']:
                    c3_id = c3['id']
                    h_row = pd.DataFrame([[c3_id,c3_id,c3_id,c3_id,c2_id,c1_id]],columns = ['categories','c5','c4','c3','c2','c1'])
                    df_hierarchical_cat = df_hierarchical_cat.append(h_row)
                    if len(c3['categories'])>0:
                        for c4 in c3['categories']:
                            c4_id = c4['id']
                            h_row = pd.DataFrame([[c4_id,c4_id,c4_id,c3_id,c2_id,c1_id]],columns = ['categories','c5','c4','c3','c2','c1'])
                            df_hierarchical_cat = df_hierarchical_cat.append(h_row)
                            if len(c4['categories'])>0:
                                for c5 in c4['categories']:
                                    c5_id = c5['id']
                                    h_row = pd.DataFrame([[c5_id,c5_id,c4_id,c3_id,c2_id,c1_id]],columns = ['categories','c5','c4','c3','c2','c1'])
                                    df_hierarchical_cat = df_hierarchical_cat.append(h_row)
                                    if len(c5['categories'])>0:
                                        categories_id = c5['id']
                                        h_row = pd.DataFrame([[categories_id,c5_id,c4_id,c3_id,c2_id,c1_id]],columns = ['categories','c5','c4','c3','c2','c1'])
                                        df_hierarchical_cat = df_hierarchical_cat.append(h_row)
                                    else:
                                        categories_id = c5_id
                                        h_row = pd.DataFrame([[categories_id,c5_id,c4_id,c3_id,c2_id,c1_id]],columns = ['categories','c5','c4','c3','c2','c1'])
                                        df_hierarchical_cat = df_hierarchical_cat.append(h_row)
                            else:
                                categories_id = c4_id
                                c5_id = c4_id
                                h_row = pd.DataFrame([[categories_id,c5_id,c4_id,c3_id,c2_id,c1_id]],columns = ['categories','c5','c4','c3','c2','c1'])
                                df_hierarchical_cat = df_hierarchical_cat.append(h_row)
                    else:
                        categories_id = c3_id
                        c5_id = c3_id
                        c4_id = c3_id
                        h_row = pd.DataFrame([[categories_id,c5_id,c4_id,c3_id,c2_id,c1_id]],columns = ['categories','c5','c4','c3','c2','c1'])
                        df_hierarchical_cat = df_hierarchical_cat.append(h_row)
            else:
                categories_id = c2_id
                c5_id = c2_id
                c4_id = c2_id
                c3_id = c2_id
                h_row = pd.DataFrame([[categories_id,c5_id,c4_id,c3_id,c2_id,c1_id]],columns = ['categories','c5','c4','c3','c2','c1'])
                df_hierarchical_cat = df_hierarchical_cat.append(h_row)
    df_hierarchical_cat.reset_index(inplace = True, drop = True)
    return df_hierarchical_cat