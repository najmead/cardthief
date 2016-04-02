import requests
import json
import sqlite3
import time
import datetime
import logging
import random
import ast

from time import mktime
from datetime import datetime, timedelta

def dbInit(conn):
    
    c = conn.cursor()
    
    c.execute('''
                create table card
                    (
                        code text,
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
                        id int,
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
                        quantity int
                    );
                ''')
    
    conn.commit()

def getCards(conn, fromDate):
        
    ## Get JSON Data from website
    url = 'http://netrunnerdb.com/api/cards/'
    response = requests.get(url=url)
    results = json.loads(response.text)

    ## Open Cursor
    c = conn.cursor()

    ## Iterate over JSON results and enter into database
    for card in results:
        lastMod=time.strptime(card['last-modified'], "%Y-%m-%dT%H:%M:%S+00:00")
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
                    currentDate,
                    card['last-modified']
                    )
            
            c.execute('''   insert into card(code, cyclenumber, number, setname, side, 
                                            faction, title,type, subtype, text, 
                                            flavor, quantity, limited, minimumdecksize,
                                            uniqueness, imagesrc, url, added_date,
                                            last_modified_date) 
                            values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);''', row)
                    
    conn.commit()
    c.close()

def getDecks():
    
    ## Get the current date (rounded down)
    currentDate = datetime.today()
    currentDate = currentDate.replace(hour=0, minute=0, second=0, microsecond=0)
    print(currentDate)

    ## Get the latest date, according to the database
    dbMaxDate = datetime.strptime('2016-03-27', "%Y-%m-%d")
    print(dbMaxDate)

    ## Calculate how many days of querying, and generate random number sequence
    queryDays = (dbMaxDate - currentDate).days  * -1
    print(queryDays)
    
    random.seed(123)
    days = random.sample(range(0, queryDays), queryDays)
 
    conn = sqlite3.connect('netrunner.db')
    c = conn.cursor()
    
    ## Iterate over the sequence of days, and query API
    for day in days:
        queryDate = (dbMaxDate+timedelta(days=day)).strftime('%Y-%m-%d')
        url = 'http://netrunnerdb.com/api/decklists/by_date/'+queryDate        
        
        response = requests.get(url=url)
        decks = json.loads(response.text)
        
        
        for deck in decks:
            row = (deck['id'],
                    deck['name'],
                    deck['creation'],
                    deck['description'],
                    deck['username'])
            
            c.execute('''
                        insert into deck (id, name, create_date, 
                                            description, username)
                        values (?,?,?,?,?)''', row)
            
            for cardid, qty in deck['cards'].items():
                row = (deck['id'],
                        cardid,
                        qty
                        )
                c.execute('''
                            insert into decklist(deckid, cardid, quantity)
                            values (?,?,?);
                        ''', row)

            
    conn.commit()
    c.close()
                        
            
            
    
def main():
    
    logging.basicConfig(level=logging.DEBUG)
    
    dbFile='netrunner.db'
     
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
        d.close()    
        logging.info('Found one. I hope it''s correct.')
    
   
    conn = sqlite3.connect(dbFile)

    scanDate = '2016-04-01'
    
    scanDateTime = time.strptime(scanDate, "%Y-%m-%d")
    
    #getCards(conn=conn, fromDate=scanDateTime)

    getDecks()
    #getDecks(conn=conn, fromDate=scanDate)
    
    conn.close()
    
if __name__ == "__main__":
    main()
