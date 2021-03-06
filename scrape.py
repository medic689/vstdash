#!/usr/bin/python3

from requests import Session
from lxml import html
from bs4 import BeautifulSoup
import csv
import os.path
from os import path
import mysql.connector
from datetime import datetime
import sys

# login information
loginUrl = "https://emresource.juvare.com/login"
payload = {
    "loginName": "amrarlington",
    "password": "Password2*"
}


mydb = mysql.connector.connect(
    host="arlingtonems.org",
    user="emApp",
    password="passwordISpassword",
    database="emdata"
)
mycursor = mydb.cursor(buffered=True)

# List of EMResource websites to pull data from
hospitalWebsites = [
    "https://emresource.juvare.com/EMSystem?uc=VIEWSTATUS&currentStep&nextStep=VIEW_RSD&nextStepDetail=4434",  # MCA
    "https://emresource.juvare.com/EMSystem?uc=VIEWSTATUS&currentStep&nextStep=VIEW_RSD&nextStepDetail=4424",  # AMH
    "https://emresource.juvare.com/EMSystem?uc=VIEWSTATUS&currentStep&nextStep=VIEW_RSD&nextStepDetail=8445",  # USMD
    "https://emresource.juvare.com/EMSystem?uc=VIEWSTATUS&currentStep&nextStep=VIEW_RSD&nextStepDetail=14573",  # MMMC
    "https://emresource.juvare.com/EMSystem?uc=VIEWSTATUS&currentStep&nextStep=VIEW_RSD&nextStepDetail=4423",  # JPS
    "https://emresource.juvare.com/EMSystem?uc=VIEWSTATUS&currentStep&nextStep=VIEW_RSD&nextStepDetail=4421",  # Cooks
    "https://emresource.juvare.com/EMSystem?uc=VIEWSTATUS&currentStep&nextStep=VIEW_RSD&nextStepDetail=4422",  # Harris
    "https://emresource.juvare.com/EMSystem?uc=VIEWSTATUS&currentStep&nextStep=VIEW_RSD&nextStepDetail=4443",  # HEB
]
datalocations = [
    {  # Med Surg data Location
        "row": 6,
        "name": 1,
        "count": 2,
        "time": 4
    },
    {  # ICU data Location
        "row": 4,
        "name": 1,
        "count": 2,
        "time": 4
    },
    {  # ED data Location
        "row": 12,
        "name": 1,
        "count": 2,
        "time": 4
    },
    {  # NegPres data Location
        "row": 11,
        "name": 1,
        "count": 2,
        "time": 4
    }
]

# used to convert the str formatted months to numericals
monthDict = {
    "Jan": "01",
    "Feb": "02",
    "Mar": "03",
    "Apr": "04",
    "May": "05",
    "Jun": "06",
    "Jul": "07",
    "Aug": "08",
    "Sep": "09",
    "Oct": "10",
    "Nov": "11",
    "Dec": "12",
}

# declaring an empty array to hold returned data
# no need to reset back to zero as the script closes and runs new each time.
dataReturned = []


def fetchData():
    with Session() as s:
        s.post(loginUrl, data=payload)  # log session into website
        for site in hospitalWebsites:  # go through list of webpages to pull data from
            siteDataObject = s.get(site)  # pull the html in
            parsedData = BeautifulSoup(
                siteDataObject.text, 'html.parser')  # parse the html
            # pull out the HHS table
            hhsTable = parsedData.find('table', id='stGroup7139')
            # pull out the NEDOC table
            nedTable = parsedData.find('table', id='stGroup6122')

            # get hospital name
            # using the full html, find the hospital name
            hospitalName = getHospitalName(parsedData)
            # using the data locations, pull out the wanted info and use the supplied hospital name to later add it into the db
            getData(hhsTable, datalocations, hospitalName)
            # same as above but for the NEDOC info
            getNedoc(nedTable, hospitalName)
        addDataToDB(dataReturned)


def getHospitalName(data):
    return data.find('h1', id='r_name').text


def getData(table, locations, hospitalName):
    # loop to pull data from the main table
    for bedType in locations:
        rows = table.find_all("tr")
        cols = rows[bedType.get("row")].find_all("td")

        bedTypeName = cols[bedType.get("name")].text
        countOfBeds = cols[bedType.get("count")].text
        timeLastUpdated = formatDateTime(cols[bedType.get("time")].text)

        if countOfBeds == "--":
            countOfBeds = ""
        dataReturned.extend(
            [(hospitalName, countOfBeds, bedTypeName, timeLastUpdated)])


def getNedoc(table, hospitalName):
    # Manual data pull for NEDOC
    rows = table.find_all("tr")
    cols = rows[2].find_all("td")
    name = cols[1].text
    countOfBeds = cols[2].text.split(" ")[0]
    timeLastUpdated = formatDateTime(cols[4].text)
    # missing entries show up as "--". Below swaps that out for a blank string.
    if countOfBeds == "--":
        countOfBeds = ""
    # add on to list of data to update
    dataReturned.extend(
        [(hospitalName, countOfBeds, name, timeLastUpdated)])

# takes in the text string and is combined into a parseable format


def formatDateTime(time):
    # the data scraped uses a Latin-1 or char(160) space, idk.
    time = time.replace(u'\xa0', u' ').split(" ")
    # check if the argument is blank to prevent lower errors
    if time[0] == "":
        return " "
    # set the hour:min spot
    hour = time[2]
    # break apart the hour:min based off the :
    hoursplit = hour.split(":")[0]
    minutesplit = hour.split(":")[1]
    # concatenate everything back together in the correct date time format
    return str("2021-" + monthDict.get(time[1]) + "-" + time[0] + " " + hoursplit + ":" + minutesplit + ":00")

# takes in a tuple with the correct ordering


def addDataToDB(dataLine):
    data = ()  # blank tuple to hold returned data
    # mysql string to retrieve all of the previously submitted data
    mycursor.execute("SELECT * FROM hospitalData")
    data = mycursor.fetchall()
    # to keep an eye on the size of the data
    print("Size of returned db data " + str(sys.getsizeof(data)) + " Bytes")

    for line in dataLine:  # for each line of data scraped from emResources,
        if line not in data:  # if the scraped data is not in the database,
            print("{hospital: <40} {count: >4} - {type: >41} not in data".format(
                count=line[1], hospital=line[0], type=line[2]))
            sql = "INSERT INTO hospitalData (name, bedsAvailable, bedType, lastUpdated) VALUES (%s, %s, %s, %s)"
            val = (line[0], line[1], line[2], line[3])
            try:
                mycursor.execute(sql, val)
                mydb.commit()
                print(mycursor.rowcount, "record inserted.")
            except:
                print("Error inserting new record")

        else:
            print("{hospital: <40} {count: >4} - {type: <41} already in data".format(
                count=line[1], hospital=line[0], type=line[2]))


# used to show when the program runs in the log file
print("\n********* Commencing Scrape on ",  datetime.now(), " ***********")
fetchData()  # how we kick things off
