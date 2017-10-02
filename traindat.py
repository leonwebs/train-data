import zipfile
from io import TextIOWrapper
import os
import re 
##from time import strftime, strptime, mktime
##from datetime import date
import datetime
import sqlite3

'''This code is to create the database for train schedule data.
The data for a given year is in a zipped folder named by the year,
'2016.zip' for example.  Within the folder are zipped folders of
each schedule '1.zip' for example.  Each of these contain a text
file for each day the schedule ran '20160102.txt' for example.
The text file has lines including the train name and other info,
followed by a list of headers of the columns of data, followed
by a row of 'V's indicating the beginning of the columns of data,
with a row thereafter for each station, the scheduled and actual
departure times and a comment.  '''

junk='* +|\t\n-'

def cleantime(thestring):
    cleanout = thestring.strip()
    if re.match('\d*[AaPp]$', cleanout) != None:
        cleanout=strftime('%H:%M',strptime(thestring+'m', '%I%M%p'))
    elif thestring=='*' or thestring=='':
        cleanout = None
    elif re.match('\d{8}$', thestring) != None:
        b=(0,4,6,None)
        cleanout = date( *(int(thestring[b[i]:b[i+1]]) for i in range(3)))
    elif re.match('\d*$', thestring) != None: 
    #elif: str.isdecimal(thestring):  #works only for python 3
        cleanout=int(thestring)        
    return(cleanout)

def checkheadings(datahand, dbcurs, numanddate):
## Check that the data still requires collection and if so that the
## headings and train name match.  Add any missing heading data to the
## database if possible.
    ## Don't bother with reading if the data are already present.
    dbcurs.execute('''SELECT SchedNum FROM "TrainInfo"
                        WHERE SchedNum = ? AND RunDate = ?''', (numanddate[0], numanddate[1]) )
    dbval=dbcurs.fetchall()
    if len(dbval) != 0 :
##        print('Already have data for train ' + str(numanddate[0]) + ' on')
##        print(numanddate[1])
        return ## Continue getting the header only if the data aren't available already.
    ## Get the header as text
    datahand=TextIOWrapper(datahand)
    #start reading lines and putting them into headlines
    #as long as the last line read isn't the 'V's keep reading
    headlines=[datahand.readline().strip(junk)]
    while headlines[-1].strip(' V')!='':
        headlines.append(datahand.readline().strip(junk))
    #take the line of 'V's and find the list of places to break 
    #the remaining lines
    breaks = [m.start() for m in re.finditer('V', headlines.pop() )]
    n=len(breaks)
    breaks.extend([None])
    colnames=headlines[-n:]
    traininfo = headlines[0:-n]
    ## Headings should match those in the database
    dbcurs.execute('''PRAGMA table_info("Timing")''')
    dbval = dbcurs.fetchall()
    dbval = [el[1] for el in dbval[-8:]]
    if [st.lower() for st in dbval ] != [st.lower() for st in colnames]:
        print('Problem with column headings '+str(numanddate))
        print( dbval)
        print(colnames)
        return 
    ##Get the name of the train from the database if it exists
    ## make sure there's a match and if so remove it from the traininfo
    ## If it doesn't exist test to see whether the name can be inferred
    ## given only one train info datum.
    dbcurs.execute('''SELECT SchedNum, SchedName FROM "ScheduleNames"
                        WHERE SchedNum = ?''', (numanddate[0],) )
    namespresent = False
    for row in dbcurs:
        if row[1] in traininfo:
            namespresent = True
            traininfo.remove(row[1])
    if not namespresent and len(traininfo)==1:
        dbcurs.execute('''INSERT INTO "ScheduleNames"
                        ("SchedNum",
                         "SchedName")
                         VALUES (?,?)''', (numanddate[0],traininfo.pop()))
    ## Condense any remaining train info to one line
    traininfo = '; '.join(traininfo)
    return(breaks, traininfo,datahand)

def scheduledatatodatabase(zippedfold, dbcurs):
    """Extract and insert data from a folder named zippedfold
     containing one schedule's data for the year."""
    try:
        with zipfile.ZipFile(zippedfold) as zipref:
         #zippedfold eg '1.zip'
            print(zippedfold)
            for eachfile in zipref.namelist():
                with zipref.open(eachfile) as data:
                    #eachfile eg '1_20160101.txt'
                    numanddate = [cleantime(el) for el in (\
                        os.path.basename(\
                            os.path.splitext(eachfile)[0]).split('_'))]
                    headcheck = checkheadings(data, dbcurs, numanddate)
                    if headcheck == None:
                        pass
                    else:
                        (breaks, traininfo, dh) = headcheck
                        dbcurs.execute("""INSERT INTO "TrainInfo"
                                ("SchedNum", "RunDate", "Info")
                                 VALUES (?,?,?)""",
                                       (numanddate[0],numanddate[1],traininfo))
                        #For each line for that schedule insert the timing data
                        #print('Parsing ' + str(eachfile))
                        for lines in dh:
                            lines=lines.strip(junk)
                            #Break given locations of 'V's
                            insertion = [lines[breaks[i]:breaks[i+1]].strip() for i in range(len(breaks)-1)]
                            #and clean
                            insertion = numanddate+[cleantime(i) for i in insertion]
                            (a,b,c,d,e,f,g,h,i,j)=insertion
                            dbcurs.execute("""INSERT INTO "Timing" (
                                    "SchedNum" ,
                                    "RunDate" ,
                                    "Station Code" ,
                                    "Schedule Arrival Day" , 
                                    "Schedule Arrival Time" ,
                                    "Schedule Departure Day" ,
                                    "Schedule Departure Time" ,
                                    "Actual Arrival Time" ,
                                    "Actual Departure Time" ,
                                    "Comments" )
                                 VALUES (?,?,?,?,?, ?,?,?,?,?)""", (a,b,c,d,e,f,g,h,i,j))
    except ValueError as err:
         print(str(err))

###########################################################
##Test the definitions
###########################################################



import matplotlib.pyplot as plt
import numpy as np
      


import plotly as py
py.tools.set_credentials_file(username='lwebster', api_key='SplfOxbJix3lKqAtiPcb')
# Learn about API authentication here: https://plot.ly/python/getting-started
# Find your api_key here: https://plot.ly/settings/api



    

conn=sqlite3.connect('TrainData.sqlite')
curs=conn.cursor()

##try:
curs.execute('''SELECT "Schedule Arrival Time", "Actual Arrival Time" FROM timing 
WHERE "Station Code" = "DEN" AND "SchedNum" = 5 ''')

rows = curs.fetchall()
rows = [(datetime.datetime.strptime(r[1],'%H:%M')-datetime.datetime.strptime(r[0],'%H:%M')).total_seconds() for r in rows]


x = np.array(rows)
plt.hist(x/60,50)
plt.title("2016 Train 5 arrivals in DEN")
plt.xlabel("Lateness (min)")
plt.ylabel("Frequency")

fig = plt.gcf()

plot_url = py.plotly.plot_mpl(fig, filename='mpl-basic-histogram')



try:
    pass
##    namelist =  os.listdir('2016')
##    namelist.pop()
##    for scheds in namelist:
##        testout = scheduledatatodatabase('2016\\'+scheds, curs)
##    conn.commit()
except sqlite3.IntegrityError as err:
    print('An error: '+str(err))
finally:
    curs.close()
    conn.close()




###########################################################
##Create Tables
###########################################################



##curs.execute(
##    ''' CREATE TABLE "ScheduleNames" (
##    "SchedNum" INTEGER,
##    "SchedName" TEXT
##    ) ''')
##
##curs.execute(
##    ''' CREATE TABLE "TrainInfo" (
##    "SchedNum" INTEGER,
##    "RunDate" DATE,
##    "Info" TEXT,
##    PRIMARY KEY (SchedNum, RunDate)  
##    ) ''')
##
##curs.execute( ''' CREATE TABLE "Timing" (
##    "SchedNum" INTEGER,
##    "RunDate" DATE,
##    "Station Code" TEXT NOT NULL,
##    "Schedule Arrival Day" INTEGER, 
##    "Schedule Arrival Time" TIME,
##    "Schedule Departure Day" INTEGER,
##    "Schedule Departure Time" TIME,
##    "Actual Arrival Time" TIME,
##    "Actual Departure Time" TIME,
##    "Comments" TEXT
##    )
##    ''')
##
####curs.execute( ''' CREATE TABLE "Timing" (
####    "SchedNum" INTEGER,
####    "RunDate" DATE,
####    "StnCode" TEXT NOT NULL,
####    "SchedArrDay" INTEGER, 
####    "SchedArrTime" TIME,
####    "SchedDepDay" INTEGER,
####    "SchedDepTime" TIME,
####    "ActArrTime" TIME,
####    "ActDepTime" TIME,
####    "Comments" TEXT
####    )
####    ''')
##
##
##curs.close()
##conn.close()
##
##
##
#########################################################
##Code graveyard
#########################################################

##    
##yearstoadd = ['2016.zip']
##for everyyear in yearstoadd:
##    for everysched in sched:
##        for everyday in day:
##            take the data, clean it and insert into the database
##
#out = gettraindata('./2016/1.zip')

##testout=[]
##
##try:
##    with zipfile.ZipFile(".\\2016\\1.zip") as zipref, \
##         zipref.open(zipref.namelist()[0]) as data:
##        data = TextIOWrapper(data)
##        for lines in data:
##            testout.append(lines)
##            print(testout[-1],end='')
##except IOError as err:
##    print(str(err))
##
##try:
##    with zipfile.ZipFile(".\\2016\\1.zip") as zipref, \
##         zipref.open(zipref.namelist()[0]) as data:
##        data = TextIOWrapper(data)
####            testout.append(data.readline())
####            for lines in range(10):
##        line=data.readline().strip(junk)
##        while not line.strip(' V')=='':
##            testout.append(line)
##            line = data.readline().strip(junk)
##            print(testout[-1])
##except IOError as err:
##    print(str(err))



##def gettimedata(folder):
##    tmpcont=os.listdir(folder)
##    junk='* +|\t\n-'
##    for eachfile in tmpcont:
##        try:
##            with open(eachfile) as filehandle:
##                traindict={'Route': filehandle.readline().strip(junk)}
##                headerlist=[]
##                for eachhead in range(8)
##                    headerlist.append(filehandle.readline().strip(junk))
##                    traindict[headerlist[eachhead]]=[]
##                filehandle.readline()
##                
##                    'Name': tempdat.pop(0),
##                    'DOB': tempdat.pop(0),
##                    'Times': sorted([sanitize(times) for times in tempdat])
##
##        except IOError as err:
##            print('An I/O error has occurred: ' + str(err))
##            return(None)

##select "Actual Departure Time" as at, "Schedule Departure time" as st, round((julianday(datetime("actual departure time"))-julianday(datetime("Schedule departure time")))*24*60) from timing

# def scheduledatatodatabase(zippedfold, dbcurs):
#     """Extract and insert data from a folder named zippedfold
#         containing one schedule's data for the year."""
#     try:
#         with zipfile.ZipFile(zippedfold) as zipref:
#             #zippedfold eg '1.zip'
#             for eachfile in zipref.namelist():
#                 with zipref.open(eachfile) as data: 
#                           #eachfile eg '1_20160101.txt'
#                     numanddate= os.path.basename(os.path.splitext(eachfile)[0]).split('_')
#                     data = TextIOWrapper(data)
#                     #Get the headings up to the row of 'V's
#                     traininfo = []
#                     for lines in data:
#                         lines=lines.strip(junk)
#                         if lines.strip(' V')=='':
#                             brks = [m.start() for m in re.finditer('V', lines)]+[None]
#                             break
#                         elif lines not in headings:
#                             traininfo.append(lines)
#                     #Sometimes there will be two additions to train info
#                             # a service disruption warning and the schedule's
#                             #name.  If there's only one (as usual) insert a
#                             #None to fill it out.  
#                     if len(traininfo)==1:
#                         traininfo.insert(0,None)
#                     (a, b, c, d) = (i for i in numanddate+traininfo)
#                     #Now enter the schedule data into the schedule table
#                     dbcurs.execute("""INSERT INTO "Schedule"
#                             ("Schednum",
#                             "RunDate",
#                             "Info",
#                             "SchedName")
#                             VALUES (?,?,?,?)""", (a,b,c,d))
#                     #For each line for that schedule insert the timing data 
#                     for lines in data:
#                         lines=lines.strip(junk)
# ##                        #Break the line into headnum pieces at whitespace
# ##this used to work?!                        insertion = lines.strip(junk).split(None, 8-1)
#                         #Break given locations of 'V's
#                         try:
#                             insertion = [lines[brks[i]:brks[i+1]] for i in range(len(brks)-1)]
#                             #and clean
#                             insertion = [cleantime(i) for i in numanddate+insertion]
#                             (a,b,c,d,e,f,g,h,i,j)=insertion
                        
#                             dbcurs.execute("""INSERT INTO "Timing" (
#                                 "SchedNum" ,
#                                 "RunDate" ,
#                                 "StanCode" ,
#                                 "SchedArrDay" , 
#                                 "SchedArrTime" ,
#                                 "SchedDepDay" ,
#                                 "SchedDepTime" ,
#                                 "ActArrTime" ,
#                                 "ActDepTime" ,
#                                 "Comments" )
#                                 VALUES (?,?,?,?,?, ?,?,?,?,?)""", (a,b,c,d,e,f,g,h,i,j))
#                         except ValueError as err:
#                             print('Value Error: ' + str(err))
#                             return([insertion,eachfile])
#     except IOError as err:
#         print(str(err))

#year = os.path.basename(os.path.abspath(os.path.dirname(zippedfold)))


##def getheadings(zippedfold):
##    tripq Extracts the headings from zipped folder named zippedfold.
##        The zipped folder should contain the years
##        data for a train schedule.tripq
####
##    headdict={'Headings':[]}
##    try:
##        with zipfile.ZipFile(zippedfold) as zipref, \
##             zipref.open(zipref.namelist()[0]) as data:
##                #Interpret the file as text
##                data = TextIOWrapper(data)
##                #The first line is the name of the schedule
##                headdict.update({'Train_Name': data.readline().strip(junk)})
##                for lines in data:
##                    if lines.strip(junk).strip(' V')=='':
##                        break
##                    headdict['Headings'].append(lines.strip(junk))
####                    
####                #Keep reading lines, stripping them and appending them to the
####                #headings list until a line composed only of "V" or space
####                line = data.readline().strip(junk)
####                while line.strip(' V')!='':
####                    headdict['Headings'].append(line)
####                    line =data.readline().strip(junk)
####                #The locations of the "V"s indicate where to break the string
####                headdict.update({'Databreaks':
####                                 [m.start() for m in finditer('V',line)]
####                                 })
##    except IOError as err:
##        print(str(err))
##    return headdict
