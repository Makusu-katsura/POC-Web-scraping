# This program are develop to make web scraping for educational purpose only.
# The idea is to send a request in multiple processs and scraping asyncronously to get data from website.

from bs4 import BeautifulSoup
import pyodbc 
import re
import time
import aiohttp 
import asyncio
import random
from collections import OrderedDict
from ordered_set import OrderedSet
count = 0
from datetime import datetime
asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
from threading import Thread
import multiprocessing
# EnableCookies = False
EnableCookies = True

def randomCookies():
    cookies_list = [
        {'Cookie':'XXXX'},
        {'Cookie':'XXXX'},
        {'Cookie':'XXXX'},
        {'Cookie':'XXXX'},
        {'Cookie':'XXXX'},
        {'Cookie':'XXXX'},
        {'Cookie':'XXXX'},
        ]
    # return cookies_list
    ordered_cookies_list = []
    for cookie in cookies_list:
        h = OrderedDict()
        for cookie,value in cookie.items():
            h[cookie]=value
        ordered_cookies_list.append(h)
    return ordered_cookies_list

def randomHeaders():
    headers_list = [
    # Firefox 77 Mac
     {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:77.0) Gecko/20100101 Firefox/77.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Referer": "https://www.XXX.com/",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1"
    },
    # Firefox 77 Windows
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:77.0) Gecko/20100101 Firefox/77.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://www.XXX.com/",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1"
    },
    # Chrome 83 Mac
    {
        "Connection": "keep-alive",
        "DNT": "1",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Dest": "document",
        "Referer": "https://www.XXX.com/",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8"
    },
    # Chrome 83 Windows 
    {
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-User": "?1",
        "Sec-Fetch-Dest": "document",
        "Referer": "https://www.XXX.com/",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.9"
    }
    ]
    # Create ordered dict from Headers above
    cookies_list = [
        {'Cookie':'XXXX'},
        {'Cookie':'XXXX'},
        {'Cookie':'XXXX'},
        {'Cookie':'XXXX'},
        {'Cookie':'XXXX'},
        {'Cookie':'XXXX'},
        {'Cookie':'XXXX'},
        ]
    ordered_headers_list = []
    cookie= cookies_list[random.randint(0,5)]
    for headers in headers_list:
        h = OrderedDict()
        for header,value in headers.items():
            h[header]=value
        #for random set manual cookie into header 
        # for header,value in cookie.items():
        #     h[header] = value
        ordered_headers_list.append(h)
    
    return ordered_headers_list

async def Scraping(session,ProductID):
    #set url request
    url = 'https://www.XXX.com/'+ProductID
    headers = ''
    for i in range(1,4):
    #Pick a random browser headers to prevent bot detect
        headers = random.choice(randomHeaders())
    # show number of request on process
    global count
    count+=1
    print(count)
    print(url)
    #before send request delay process of each request to prevent flooding
    time.sleep(5*random.randint(1,3))
    async with session.get(url,headers=headers) as resp:
        Noted = ''
        # if response is not 200 note the problem
        if resp.status == 404:
            Noted = "404 Not Found"
            print(ProductID+" :"+ Noted)
        #get all text from website
        text = await resp.text()
        #use regular expression to detect bot and automated warning
        if re.search(r'Bot',text) !=None and Noted == '' :
            Noted = 'Web Detected Bot'
            print(ProductID+ " : "+Noted)
            
        if re.search(r'automated',text) !=None and Noted == '' :
            Noted = 'Web Detected automated'
            print(ProductID+ " : "+Noted)

        # ob = await Filtering(ProductID,text,Noted)
        ob = await Filtering(ProductID,text,Noted)
        #try to close seesion after get data finished
        await asyncio.sleep(0.250)
        return ob
    
async def Filtering(ProductID,text='',Noted=''):
        try:
            #parse text to htl format
            soup = BeautifulSoup(text, 'html.parser')
            #set tuple to insert data indatabase
            ob = ()
            #find all html tag <span>
            for title in soup.find_all('span'):
                result = title.get_text().strip()

                #find pattern of what we want (#64 in Door Levers)
                find = re.findall(r'#[\s\S]+',str(result))

                #filter only find is not empty and text length is not longer than 150 and pattern of text are like 
                # '#700 in XXXX (XXXX)' 
                # or '#50,123 in XXXX' which length is more than 0
                if(find!=[] and len(find[0])<150 and len(re.findall(r'#\d+ in |#\d+,\d+ in ',find[0]))>0):
                    # print("-",find)
                    #find pattern that has word 'in' and end with any pattern
                    findCategories = re.findall(r'in [\s\S]+',str(find[0]))
                    # remove word 'in ' in front of categories we want
                    removeIn = str(findCategories[0]).removeprefix('in ')
                    # remove anything inside () that come after categories ex. (XXXX)
                    removeIn =re.sub(r' \([\s\S]+','',removeIn)
                    # filter only word that doesn't have '#' inside 
                    if(re.search(r'#',removeIn)==None):
                        # append to tuple
                        ob+= (removeIn,)
            #filter tuple to not duplicate same word
            res = tuple(OrderedSet(ob))
            request_already_done = 0
            if(len(res)<1 and Noted == ''):
                Noted = "Can't Find Categories"
                request_already_done = 1
            if(len(res)>1 and Noted != ''):
                Noted = ''
                request_already_done = 1
            # if length still less than 5 append more empty string for insert to database
            if(len(res)<5):
                while len(res)<5:
                    res+=('',)
            # add noted to mark if this record has a problem and add ProductID to update in database
            res+=(Noted,request_already_done,ProductID,)
            return res
        except Exception as e:
            print(ProductID+" error : "+str(e))


def excuteTableThread(cursor,data):
        try:
            #update data we got from scraping by ProductID
            cursor.executemany('''UPDATE dbo.XXX_Scraping
            SET  Sub_Categories_1= ?, Sub_Categories_2=?, Sub_Categories_3=? ,Sub_Categories_4=? , Sub_Categories_5=?,Noted=?,Done =?
            WHERE ProductID =?;''',data)
            cursor.commit()
            # cursor.close()
            print("finish write DB")

        except Exception as e:
            print("excuteTableThread err: "+str(e))
async def SessionRequest(ProductIDList,EnableCookies,results):
    try:
        #setting session
        #prevent session timeout before get data success
        session_timeout =   aiohttp.ClientTimeout(total=None)
        async with aiohttp.ClientSession(timeout=session_timeout) as session:
            tasks = []
            #loop 10 data we got from main
            for ProductID in ProductIDList:
                cookie =''
                print('Check EnableCookies: ',EnableCookies)
                if(EnableCookies == True):
                    for i in range(1,6):
                        #random set cookie header to make prevent bot detect
                        cookie = random.choice(randomCookies())
                    session._cookie_jar.update_cookies(cookies=cookie)

                #create task to make each request working asyncronous(seperately) to prevent code error in any request and speed up process
                task = asyncio.create_task(Scraping(session,ProductID[0]))
                tasks.append(task)
            
            #get all task result      
            res = await asyncio.gather(*tasks)
            #add result into queue
            results.put(res)
    except Exception as e:
        print('err '+ProductID+' SessionRequest: ' + str(e))

def runloop(url,EnableCookies,results):
    #run loop to create session
    loop = asyncio.get_event_loop()
    loop.run_until_complete(SessionRequest(url,EnableCookies,results))

def setGlobalCookie():
    #toggle enable web cookie
    global EnableCookies
    EnableCookies = not EnableCookies
    print(EnableCookies)

def main():
    try:
        # connect database
        conn  = pyodbc.connect('DRIVER={SQL Server};'
                        'Server=localhost;'
                        'Database=test;'
                        'Trusted_Connection=yes;')
        cursor = conn.cursor()

        # set program to run 10 times
        for i in range(10):
        # sql command for get data
            # cursor.execute("SELECT DISTINCT top 100 ProductID from XXX_Scraping ")
            # cursor.execute("SELECT DISTINCT top 100  ProductID from XXX_Scraping  WHERE Noted ='Web Detected Bot'")
            # cursor.execute("SELECT DISTINCT ProductID from XXX_Scraping  WHERE Noted ='Web Detected automated'")


            #setting time and thread
            start_time = time.time()
            start_date = datetime.now()
            threads  = []
            num_threads = 10
            print(f"start :{str(start_date)}")

            #get value from database and make it to [10][10] array = 100 query
            values =cursor.fetchall()
            urls = [values[j:j+10] for j in range(0, len(values), 10)]

            #split data to thread each thread got 10 data
            while len(urls) > 0:
                if len(urls) > num_threads:
                    url_sub_li = urls[:num_threads]
                    urls = urls[num_threads:]
                else:
                    url_sub_li = urls
                    urls = []
            #get boolean if this loop need to enable web cookie
            global EnableCookies
            #set variable to send in each thread after each thread get data
            q = multiprocessing.Queue()
            #create a thread for each url to scrape
            for url in url_sub_li:
            
                t = multiprocessing.Process(target= runloop,args=(url,EnableCookies,q) )
                threads.append(t)
                i+=1
            #start each thread
            for t in threads :
                t.start()
            #wait for each thread to finish
            for t in threads :
                t.join()

            results = []
            #get data from every thread
            for t in threads :
                results += q.get()
            #count how much bot detected we get
            countBot =0
        

            for i in results:
                for j in i:
                    if (j == 'Web Detected Bot'):
                        countBot+= 1
            print(results)
            print("count Bot : ",countBot)
            
            #if detect bot > half of all record toggle global boolean cookie for change api request pattern
            if(countBot>len(results)/2):
                print('find bot > 50')
                setGlobalCookie()
                print("change EnableCookies: " , EnableCookies)
            
            print(f"finish :{(time.time() - start_time):.2f} seconds "+str(datetime.now()) )

            #create new thread to save to database/backup file, prevent code error in some thread gonna make program stop

            ExcuteDBThread = Thread(target= excuteTableThread(cursor,results))

            ExcuteDBThread.start()
            #close all thread
            for t in threads :
                t.close()
            #delay process to prevent web flooding
            time.sleep(random.randint(1,3)*60)
    except Exception as e:
        print("error: "+str(e))

#run main function
if __name__ == '__main__':
        main()


    


