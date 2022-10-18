import os
from bs4 import BeautifulSoup
from datetime import date
import numpy as np
import requests
import prettify
import re
import time
import pandas as pd


class ZillowScraper:
    def __init__(self):
        self.PAGECOUNT = 2
        self.headerNumber = -1
        self.addressDF = []
        self.priceDF = []
        self.linkDF = []
        self.realtorDF = []
        self.bedDF = []
        self.bathDF = []
        self.sqDF = []
        self.zestimateDF = []
        self.df = pd.DataFrame()
    
    def HeaderRefresh(self):
    #Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36
    #Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.131 Safari/537.36
    #Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.157 Safari/537.36
        #The purpose of this method is to rotate through headers to not get blocked by Zillow
        self.headerNumber += 1
        if self.headerNumber == 0:
            header0 = {
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                'accept-encoding': 'gzip, deflate, br',
                'accept-language': 'en-US,en;q=0.8',
                'upgrade-insecure-requests': '1',
                'user-agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36',
                'referer': 'http://facebook.com'
            }
            return (header0)
        elif self.headerNumber == 1:
            header1 = {
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                'accept-encoding': 'gzip, deflate, br',
                'accept-language': 'en-US,en;q=0.8',
                'upgrade-insecure-requests': '1',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36',
                'referer': 'http://twitter.com'
            }
            return (header1)
        elif self.headerNumber == 2:
            header2 = {
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                'accept-encoding': 'gzip, deflate, br',
                'accept-language': 'en-US,en;q=0.8',
                'upgrade-insecure-requests': '1',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',
                'referer': 'http://youtube.com'
            }
            return (header2)
        elif self.headerNumber > 2:
            self.headerNumber = -1
            return (zs.HeaderRefresh())                

    def ZillowOverviewSite(self, city):
    #The purpose of this method is to load and pull 5 pages of the city in the loop
        #linkDictionary dictionary set up as Url[i] : url link
        self.linkDictionary = {}
        #soupDictionary dictionary set up as Soup[i] : content of page
        self.soupDictionary = {}
        for i in range(1, self.PAGECOUNT):
            #Create a new session for each url
            with requests.Session() as request:            
                url = 'https://www.zillow.com/homes/for_sale/' + city + '/' + str(i) +'_p'
                r = request.get(url, headers=zs.HeaderRefresh())
                self.linkDictionary["url{0}".format(i)] = url
                soup = BeautifulSoup(r.content, 'html.parser')
                self.soupDictionary["soup{0}".format(i)] = soup
            print (f"{city} --- Zillow Site --- Page {i} out of {(self.PAGECOUNT-1)}")
            #Prevent timeout by waiting 6 seconds
            time.sleep(6)
            print (f"{city} --- Page {i}/{(self.PAGECOUNT-1)} Completed")


    def PullData(self):
        for soupValue in self.soupDictionary.values():
            self.housing_options = soupValue.find_all('li', 
                                                        class_ ='ListItem-c11n-8-73-8__sc-10e22w8-0 srp__hpnp3q-0 enEXBq with_constellation', limit=8)
            for house in self.housing_options:
                address = house.find('address', attrs={"data-test":"property-card-addr"}).text
                rawPrice = house.find('span', attrs={"data-test":"property-card-price"}).text
                price = int(zs.StringReplacement(rawPrice))
                if ((address not in self.addressDF) and price != 0):
                    for allLinks in house.find_all('a'):
                        link = allLinks.get('href')
                    zestimate = zs.ZestimateCalc(link)
                    self.zestimateDF.append(zestimate)
                    realtor = house.find(class_ = 'StyledPropertyCardDataArea-c11n-8-73-8__sc-yipmu-0 hTcpwx').text
                    rawbbsf = house.find('span', class_="StyledPropertyCardHomeDetails-c11n-8-73-8__sc-1mlc4v9-0 jlVIIO").text
                    bbsf = zs.StringReplacement(rawbbsf)
                    bbsfSplit = re.split(' ', bbsf)
                    try:
                        bedCount = (int(bbsfSplit[0]))
                        bathCount = (int(bbsfSplit[1]))
                        sqCount = (int(bbsfSplit[2]))
                        self.bedDF.append(bedCount)
                        self.bathDF.append(bathCount)
                        self.sqDF.append(sqCount)
                    except ValueError:
                        self.bedDF.append(1)
                        self.bathDF.append(1)
                        try: 
                            self.sqDF.append(int(bbsfSplit[2]))
                        except ValueError:
                            self.sqDF.append(0)
                    self.addressDF.append(address)
                    self.priceDF.append(price)
                    self.linkDF.append(link)
                    self.realtorDF.append(realtor)
                else:
                    continue


    def StringReplacement(self, value):
    #The purpose of this method is to replace characters in strings such as beds/baths/sqft/$$$/,
        try:
            FormattedValue = re.sub(r'[^0-9\s]', '', value)
        #If there isn't characters to replace, throw ValueError to return 0.
        except ValueError:
            FormattedValue = 0
        return FormattedValue


    def DataFrame(self):
    #The purpose of the method is to create the output Dataframe
        self.df['Address: '] = self.addressDF
        self.df['prices: '] = self.priceDF
        self.df['zestimate: '] = self.zestimateDF
        #The % difference between Posting Price vs. Zillow Estimate
        zestimateDeltaDF =  ((self.df['prices: ']-
                                    self.df['zestimate: ']) * 
                                    100/(self.df['prices: '])).round(2)
        self.df['Zest Delta %: '] = zestimateDeltaDF.astype(str) + '%'
        self.df['beds: '] = self.bedDF
        self.df['baths: '] = self.bathDF
        self.df['sqft: '] = self.sqDF
        self.df['link: '] = self.linkDF
        #The Average cost/sqft
        self.df['price/sqft: '] = self.df['prices: ']/self.df['sqft: ']
        self.df['realtor: '] = self.realtorDF
        self.df['zestimate: '] = self.zestimateDF
        print (self.df)

    def ZestimateCalc(self, link):
        #Booleans to determine if Zestimate has been found
        #Zillow Estimate on the Top of Page
        topZillowFound = False
        #Zillow Estimate on the Bot of Page
        botZillowFound = False
        #Sends the URL request for individual homes
        with requests.Session() as zestRequest:
            zestPage = zestRequest.get(link, headers = zs.HeaderRefresh())
            zestSoup = BeautifulSoup(zestPage.content, 'html.parser')
            #Pulls all the items with the same class, this includes random things
            #Filter for the topZillow
        zestimateGroup = zestSoup.find_all('span', class_ = 
                                                'Text-c11n-8-73-0__sc-aiai24-0 xGfxD')
        for zestimateSample in zestimateGroup:
            if topZillowFound == False:
                #zestimateSample is the zestimate
                if zestimateSample.text.startswith('$'):
                    topZillowFound = True
                    zestimate = zestimateSample.text
                    break
                #zestimateSample is NOT the zestimate
                else:
                    continue
            #Found Zillow Estimate in the Top
            elif topZillowFound:
                break
        #Looking for Zillow Estimate in the Bottom, Not in the TOP
        if not topZillowFound:
            zestimateGroup = zestSoup.find_all('span', class_ = 
                                                    'Text-c11n-8-65-2__sc-aiai24-0 eUxM')
            for zestimateSample in zestimateGroup:
                #Looking for Zillow Estimate in the Bottom
                if not botZillowFound:
                    if zestimateSample.text.startswith('$'):
                        botZillowFound = True
                        zestimate = zestimateSample.text
                        break
                    else:
                        continue
                #Found Zillow Estimate in the Bottom
                elif botZillowFound:
                    break
        if not topZillowFound and not botZillowFound:
            zestimate = '0'    
        zestimate = int(zs.StringReplacement(zestimate))
        time.sleep(1.5)
        return zestimate


    def OutputCSV(self):    
        #The purpose of this method is to output reviewable data as a text file and csv

            #TXT OUTPUT
        #with open(city + '_Soup.txt', 'w', encoding="utf-8") as f:
        #    f.write(str(self.soupDictionary))

            #CSV OUTPUT
        folderPath = os.getcwd() + '\\Output Files\\'
        self.df.to_csv(folderPath + (str(date.today())).replace('-','') + '_' + city + '_DF.csv', index=True)


    def main(self, city):
        #This bypasses Captcha requests using HeaderRefresh()
        zs.ZillowOverviewSite(city)
        #This method is used to 
        print ("Pulling Data on " + city)
        zs.PullData()
        #This method will create the output DataFrame
        print ("Compiling Data on " + city)
        zs.DataFrame()
        #This method will write to ouput files
        print ("Writing Data on " + city)
        zs.OutputCSV()

if __name__ == "__main__":
    # Run through script for each City within the list
    listofCities = ["Houston", "Los Angeles", "Boston", "NYC"]
    for city in listofCities:
        zs = ZillowScraper()
        zs.main(city.upper())
    
    