import requests
import json
import logging
import sqlite3

def dbInit(conn):
    
    c = conn.cursor()
    
    ## Create set table structure
    c.execute('''
                create table [cardSet]
                (
                    setId text,
                    setName text,
                    setType text,
                    cycleNumber numeric,
                    cycleName text,
                    isAvailable boolean,
                    availableDate text,
                    PRIMARY KEY (setId)
                );''')
    
    ## Create trigger to auto populate the cycleName (which isn't in the API)
    c.execute('''
                CREATE TRIGGER populateCycleName 
                AFTER INSERT ON cardSet 
                BEGIN 
                    UPDATE cardSet
                    SET cycleName = CASE 
                                        WHEN cycleNumber = 0 then 'Draft'
                                        WHEN cycleNumber = 1 THEN 'Core' 
                                        when cycleNumber = 2 then 'Genesis'
                                        when cycleNumber = 3 then 'Creation and Control'
                                        when cycleNumber = 4 then 'Spin'
                                        when cycleNumber = 5 then 'Honor and Profit'
                                        when cycleNumber = 6 then 'Lunar'
                                        when cycleNumber = 7 then 'Order and Chaos'
                                        when cycleNumber = 8 then 'SanSan'
                                        when cycleNumber = 9 then 'Data and Destiny'
                                        when cycleNumber = 10 then 'Mumbad'
                                        ELSE 'Unknown' END;
                END;
                ''')
    c.execute('''
                CREATE TRIGGER populateSetType
                AFTER INSERT ON cardSet
                BEGIN
                    UPDATE  cardSet
                    set     setType = CASE 
                                        WHEN cycleNumber = 0 then 'Draft'
                                        WHEN cycleNumber = 1 then 'Core Set'
                                        WHEN cycleNumber in (3,5,7,9) then 'Expansion'
                                        ELSE 'Data Pack'
                                    END;
                END;
                ''')
    
    c.execute('''
                create table [card]
                (
                    cardId text,
                    setId text,
                    setQuantity int,
                    
                    cardName text,
                    cardSide text,
                    cardFaction text,
                    cardType text,
                    cardSubtype text,
                    cardText text,
                    cardFlavour text,
                    cardInfluence int,
                    
                    -- Identity card details 
                    baseLink int,
                    influenceLimit int,
                    minimumDeckSize int,    
                
                    -- General card details    
                    isUnique boolean,
                    cost int,
                    
                    -- ICE / ICEbreaker / Program details
                    strength int,
                    memoryUnits int,
                    
                    dateAdded text,
                    dateModified text,
                    PRIMARY KEY (cardId),
                    FOREIGN KEY (setID) REFERENCES cardSet (setId)
                );
                ''')

def getSets(conn):
    url = 'http://netrunnerdb.com/api/sets'
    
    logging.info('Querying website '+url)
    response = requests.get(url=url)
    
    if response.status_code == 200:
        logging.info('Found sets data.  Loading.')
        
        results = json.loads(response.text)
        
        c = conn.cursor()
        
        for set in results:

            if set['available'] == '':
                available = False
            else:
                available = True
            
            if set['available'] != '':
                availableDate = set['available']
            else:
                availableDate = None
            
            row = ( set['code'],
                    set['name'],
                    set['cyclenumber'],
                    None,
                    available,
                    availableDate
                )
            
            c.execute('''
                        insert or replace into cardSet 
                        (setId, setName, cycleNumber, cycleName,
                        isAvailable, availableDate)
                        values (?,?,?,?,?,?)''', row)
            conn.commit()
        c.close()
        return 1
    else:
        logging.warning('No response from API.  Is there a network issue?')
        return 0

def getCards(conn):        
        
    ## Get JSON Data from website
    url = 'http://netrunnerdb.com/api/cards/'
    logging.info("Querying website "+url)
    
    response = requests.get(url=url)
    results = json.loads(response.text)
    
    if response.status_code == 200:
        logging.info("Found card data.  Loading.")

        ## Open Cursor
        c = conn.cursor()
    
        ## Iterate over JSON results and enter into database
        for card in results:
                
            if 'flavor' in card: 
            #    flavor = h.handle(card['flavor'])
                flavor=card['flavor']
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
                text=card['text']
            else:
                text = None
            
            if 'baselink' in card:
                baselink=card['baselink']
            else:
                baselink = None
            
            if 'influencelimit' in card:
                inflimit=card['influencelimit']
            else:
                inflimit = None
    
            if 'minimumdecksize' in card:
                mindeck=card['minimumdecksize']
            else:
                mindeck = None
            
            if 'uniqueness' in card:
                unique=card['uniqueness']
            else:
                unique = None
            
            if 'factioncost' in card:
                influence=card['factioncost']
            else:
                influence=None
                
            if 'cost' in card:
                cost=card['cost']
            else:
                cost = None
            
            if 'strength' in card:
                str=card['strength']
            else:
                str = None
                
            if 'memoryunits' in card:
                mu=card['memoryunits']
            else:
                mu = None
                
            row = ( card['code'],
                    card['set_code'],
                    card['quantity'],
                    card['title'],
                    card['side'],
                    card['faction'],
                    card['type'],
                    subtype,
                    text,
                    flavor,
                    influence,
                    baselink,
                    inflimit,
                    mindeck,
                    unique,
                    cost,
                    str,
                    mu,
                    datetime.strftime(datetime.today(), "%Y-%m-%d %H:%M:%S"),
                    datetime.strptime(card['last-modified'], "%Y-%m-%dT%H:%M:%S+00:00"))
                        
            c.execute('''   
                        insert or replace
                        into card(cardId, setId, setQuantity, cardName, 
                                    cardSide, cardFaction, cardType,
                                    cardSubtype, cardText, cardFlavour,
                                    cardInfluence, baseLink, influenceLimit, 
                                    minimumDeckSize, isUnique, cost, 
                                    strength, memoryUnits, dateAdded, 
                                    dateModified)
                        values (?,?,?,?,?,?,?,?,?,?,
                                ?,?,?,?,?,?,?,?,?,?)
                    ''', row)
                    
        conn.commit()
        c.close()
        
    else:
        logging.warning("No valid response.  Is there a network issue?")

            
    
def main():
    
    logging.basicConfig(level=logging.DEBUG)
    
    dbFile='card.db'
     
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

    if getSets(conn=conn) == 1:
        getCards(conn=conn)
    else:
        logging.warning('Failed to get card sets.  Did not proceed to get cards')
    
    conn.close()
    
if __name__ == "__main__":
    main()
