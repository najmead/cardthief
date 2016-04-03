import requests
import json
import sqlite3
import time
import logging
import bs4

from time import mktime
from datetime import datetime, timedelta
from random import shuffle
from bs4 import BeautifulSoup

def dbInit(conn):
    
    c = conn.cursor()
    
    c.execute('''
                create table card
                    (
                        code text primary key,
                        cyclenumber numeric,
                        number numeric,
                        
                        setname text,
                        side text,
                        faction text,
                        
                        title text,
                        type text,
                        subtype text,
                        text text,
                        flavor text,
                        quantity numeric,
                        limited text,
                        minimumdecksize numeric,
                        uniqueness text,
                        imagesrc text,
                        url text,
                        added_date,
                        last_modified_date text
                        );
                ''')
                
    c.execute('''
                create table deck
                    (
                        id int primary key,
                        name text,
                        create_date text,
                        description text,
                        username text,
                        votes int,
                        favourites int,
                        comments int
                    );
                ''')
    c.execute('''
                create table decklist
                    (
                        deckid int,
                        cardid text,
                        quantity int,
                        primary key (deckid, cardid)
                    );
                ''')
    
    conn.commit()

def getCards(conn, fromDate):
        
    ## Get JSON Data from website
    url = 'http://netrunnerdb.com/api/cards/'
    logging.info("Getting list of cards.")
    response = requests.get(url=url)
    results = json.loads(response.text)

    ## Open Cursor
    c = conn.cursor()

    ## Iterate over JSON results and enter into database
    logging.info("Dumping cards into database.")
    for card in results:
        lastMod=datetime.strptime(card['last-modified'], "%Y-%m-%dT%H:%M:%S+00:00")
        
        if lastMod >fromDate:
            
            if 'flavor' in card: 
                flavor = card['flavor'] 
            else: 
                flavor = None
            
            if 'minimumdecksize' in card: 
                mindeck = card['minimumdecksize'] 
            else: 
                mindeck = None
            
            if 'subtype' in card:
                subtype = card['subtype']
            else:
                subtype = None
            
            if 'text' in card:
                text = card['text']
            else:
                text = None
            
            row = ( card['code'],
                    card['cyclenumber'],
                    card['number'],
                    card['setname'],
                    card['side'],
                    card['faction'],
                    card['title'],
                    card['type'],
                    subtype,
                    text,
                    flavor,
                    card['quantity'],
                    card['limited'],
                    mindeck,
                    card['uniqueness'],
                    card['imagesrc'],
                    card['url'],
                    datetime.strftime(datetime.today(), "%Y-%m-%d %H:%M:%S"),
                    datetime.strftime(lastMod, "%Y-%m-%d %H:%M:%S")
                    )
            
            c.execute('''
                        insert or replace into card
                        (code, cyclenumber, number, setname, side, 
                        faction, title,type, subtype, text, 
                        flavor, quantity, limited, minimumdecksize, uniqueness,
                        imagesrc, url, added_date,last_modified_date) 
                        values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
                    ''', row)
                    
    conn.commit()
    c.close()

def getDecks(conn, wayBack):
    
    ## Create connection to db and query it
    conn = sqlite3.connect('netrunner.db')
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    c.execute('''
                select distinct date(create_date) 
                from deck 
                order by date(create_date);
            ''')
    
    dates = c.fetchall()
    existingDates = set(datetime.strptime(dates[x][0],"%Y-%m-%d") for x in range(0, len(dates)))
    
    ## Get the current date (rounded down).  
    today = datetime.today()-timedelta(days=1)
    today = today.replace(hour=0, minute=0, second=0, microsecond=0)

    ## How far back do we want to go?
    #   Use the wayBack variable as passed to this function.
    #   Alternatively, uncomment the line below if you want to test
    #   with a specific date
    #wayBack = datetime.strptime('2016-03-27', "%Y-%m-%d")
  
    ## Create a set with our current dates, plus whatever is already in db
    logging.info('''
                    Running a query on dates from '''+str(wayBack)+''' 
                    to '''+str(today)+'''.
                ''')
                
    d = [wayBack, today]
    d.extend(existingDates)
    d = sorted(d)
    
    ## Generate a complete list of dates.  Anything not in the list, is missing
    #   Shuffle the missing dates, so we randomly pull them
    allDates = set(d[0] + timedelta(x) for x in range((d[-1] - d[0]).days))
    
    missingDates = sorted(allDates - set(d))
    shuffle(missingDates)
    
    ## Iterate over the sequence of days, and query API
    for date in missingDates:
        queryDate = datetime.strftime(date, '%Y-%m-%d')
        url = 'http://netrunnerdb.com/api/decklists/by_date/'+queryDate        
        logging.info("Querying "+url)
        
        response = requests.get(url=url)
        decks = json.loads(response.text)
                
        for deck in decks:
            
            ## Try and get the social tags from the website
            logging.info('Scraping social data for deck '+deck['name']+'('+str(deck['id'])+')')

            url = 'http://netrunnerdb.com/en/decklist/'+str(deck['id'])
            response = requests.get(url=url)

            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                
                section = soup.find(id="social-icon-like")
                votes=section.find("span", class_="num").next
                
                section = soup.find(id="social-icon-favorite")
                favourites=section.find("span", class_="num").next
                
                section = soup.find(id="social-icon-comment")
                comments=section.find("span", class_="num").next
            else:
                logging.warning("Couldn't find a page for deck "+str(deck['id']))
                votes=None
                favourites=None
                comments=None
    
            row = (deck['id'],
                    deck['name'],
                    deck['creation'],
                    deck['description'],
                    deck['username'],
                    votes,
                    favourites,
                    comments)
            
            c.execute('''
                        insert or ignore into deck (id, name, create_date, description, 
                                            username, votes, favourites,
                                            comments)
                        values (?,?,?,?,?,?,?,?)''', row)
            
            for cardid, qty in deck['cards'].items():
                row = (deck['id'],
                        cardid,
                        qty
                        )
                c.execute('''
                            insert or ignore into decklist(deckid, cardid, quantity)
                            values (?,?,?);
                        ''', row)
            
            conn.commit()
            time.sleep(2)
            
    conn.commit()
    c.close()
                        
            
            
    
def main():

    ## Set up basics
    logging.basicConfig(level=logging.DEBUG)    
    dbFile='netrunner.db'
   
    ## Establish database for storing data
    try:
        logging.info('Trying to find existing database.')        
        d = open(dbFile)
    
    except IOError:
        logging.warning('No database found. Creating one.')    
        firstRun = True    
        conn = sqlite3.connect(dbFile)
        dbInit(conn=conn)
    
    else:    
        firstRun = False    
        conn = sqlite3.connect(dbFile)
        d.close()    
        logging.info('Found one. I hope it''s correct.')
    

    ## How far back do you want to go?
    wayBack = datetime.strptime('2015-03-01', "%Y-%m-%d")
    
    c = conn.cursor()
    c.execute("select max(last_modified_date) from card")
    
    if firstRun is True:
        lastUpdate = wayBack
    else:
        lastUpdate = datetime.strptime(c.fetchone()[0], "%Y-%m-%d %H:%M:%S")
    
    getCards(conn=conn, fromDate=lastUpdate)

    getDecks(conn=conn, wayBack=wayBack)
    
    conn.close()
    
if __name__ == "__main__":
    main()
