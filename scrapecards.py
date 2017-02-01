import requests
import logging
import sqlite3
import json
import configparser
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

config = configparser.ConfigParser()
config.read('cardthief.conf')


def getTopDecks(conn):

    logging.info("Getting top decks.")
    refreshCycle = 5
    refreshDate = datetime.strftime(datetime.today() - timedelta(days=refreshCycle), "%Y-%m-%d")
    logging.debug('Updating existing decks with timestamp older than '+refreshDate)

    c = conn.cursor()
    factions = list(filter(None, config.get("CardThief", "FactionsToScrape").split('\n')))       
    logging.info("Scraping factions "+"-".join(factions))

    ## Iterate through factions, and pages
    maxIterations = int(config.get("CardThief", "DeckScrapeDepth"))

    for faction in factions:
        for page in range(1,maxIterations):
            logging.info("Getting "+faction+" decks, page "+str(page))
 
            url = "http://netrunnerdb.com/en/decklists/find/"+str(page)+"?faction="+faction+"&sort=likes"
            logging.info('Scrapping '+url)
            
            ## Request data, and soupify it
            result = requests.get(url=url)

            soup = BeautifulSoup(result.text, "html.parser")
            
            ## Search for each deck, and it's corresponding social tags
            decks = soup.find_all("div", attrs={'class' : 'col-sm-9'})
            social = soup.find_all("div", attrs={'class' : 'col-sm-3 small social'})
            logging.debug('Found '+str(len(decks))+' decks')

            for i in range(0, len(decks)):
                logging.debug('Found link '+decks[i].a['href'])

                text = social[i].get_text().split()
                logging.debug('Found Likes: '+text[0]+', Favourites: '+text[1]+', Comments: '+text[2])

                deckId=decks[i].a['href'].split("/")[3]
                logging.info("Found deck "+deckId)

                recentlyUpdated = c.execute('''
                                            select exists 
                                            (
                                                select  1
                                                from    deck
                                                where   dateUpdated > '''+refreshDate+'''
                                                        and deckId = '''+deckId+'''
                                            );''').fetchone()[0]

                logging.info('Deck '+deckId+' updated status since '+refreshDate+': '+str(recentlyUpdated))

                if recentlyUpdated != 1:
                    logging.debug('Getting info for deck '+deckId)
                    url = url = 'https://netrunnerdb.com/api/2.0/public/decklist/'+deckId
                    result = requests.get(url=url)
                    
                    if result.status_code == 200:
                        logging.info("Got a response when querying for deck "+deckId)
                    
                        deck = json.loads(result.text)
                        
                        row = (deck['data'][0]['id'],
                                deck['data'][0]['name'],
                                deck['data'][0]['description'],
                                deck['data'][0]['user_name'],
                                text[0],
                                text[1],
                                text[2],
                                deck['data'][0]['date_creation'],
                                datetime.strftime(datetime.today(), "%Y-%m-%d %H:%M:%S")
                            )
                        
                        c.execute('''
                                insert or ignore
                                into deck(deckId, deckName, deckDescription,
                                                createdBy, likes, favourites,
                                                comments, dateCreated, dateUpdated)
                                values (?,?,?,?,?,?,?,?,?)
                                ''', row)
                        
                        for cardid, qty in deck['data'][0]['cards'].items():
                            row = (deck['data'][0]['id'],
                                    cardid,
                                    qty
                                    )
                            c.execute('''
                                    insert or ignore
                                    into deckList(deckId, cardId, cardQty)
                                    values (?,?,?)
                                    ''', row)
                    else:
                        logging.warning("Could get a response when querying for deck "+deckId)
                else:
                    logging.info('Deck '+deckId+' has recently been added, so I won''t update it again.')
                    
                conn.commit()
    c.close()
                
def getSets(conn):
    myPacks = list(filter(None, config.get("CardThief", "MySets").split('\n')))
    logging.info("Adding "+", ".join(myPacks)+" to my list of owned sets.")

    ## Get list of sets
    url = 'https://netrunnerdb.com/api/2.0/public/packs'
    
    logging.info('Querying website '+url)
    response = requests.get(url=url)
    
    if response.status_code == 200:
        logging.info('Found sets data. Loading.')
        
        results = json.loads(response.content.decode('utf-8'))
        
        c = conn.cursor()
        
        for set in results['data']:
            if set['date_release'] == '':
                logging.info('Found a release date')
                available = False
            else:
                available = True
            
            if set['date_release'] != '':
                availableDate = set['date_release']
            else:
                availableDate = None
            
            row = ( set['code'],
                    set['name'],
                    set['cycle_code'],
                    available,
                    availableDate
                )
            
            c.execute('''
                        insert or replace into cardSet 
                        (setId, setName, cycleCode,
                        isAvailable, dateAvailable)
                        values (?,?,?,?,?)''', row)
            conn.commit()
            
        ## Update with my owned packs
        for pack in myPacks:
            logging.debug('Adding '+pack+' to list of owned sets')
            c.execute('''
                    update  cardSet
                    set     isOwned =  1
                    where   upper(setName) = ?;
                    ''', (pack.upper(),)) 
        conn.commit()
        
        
        c.close()
        
        return 1
    else:
        logging.warning('No response from API.  Is there a network issue?')
        return 0


def getCards(conn):        
        
    ## Get JSON Data from website
    url = 'https://netrunnerdb.com/api/2.0/public/cards'
    logging.info("Querying website "+url+".")
    
    response = requests.get(url=url)
    results = json.loads(response.content.decode('utf-8'))
    
    if response.status_code == 200:
        logging.info("Found card data. Loading.")

        ## Open Cursor
        c = conn.cursor()
    
        ## Iterate over JSON results and enter into database
        for card in results['data']:
                
            if 'flavor' in card: 
                flavor=card['flavor']
            else: 
                flavor = None
            
            if 'text' in card:
                text=card['text']
            else:
                text = None
            
            if 'base_link' in card:
                baselink=card['base_link']
            else:
                baselink = None
            
            if 'influence_limit' in card:
                inflimit=card['influence_limit']
            else:
                inflimit = None
    
            if 'minimum_deck_size' in card:
                mindeck=card['minimum_deck_size']
            else:
                mindeck = None
            
            if 'uniqueness' in card:
                unique=card['uniqueness']
            else:
                unique = None
            
            if 'faction_cost' in card:
                influence=card['faction_cost']
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
                
            if 'memory_cost' in card:
                mu=card['memory_cost']
            else:
                mu = None
                
            if 'advancement_cost' in card:
                advancement_cost=card['advancement_cost']
            else:
                advancement_cost=None

            if 'agenda_points' in card:
                agenda_points=card['agenda_points']
            else:
                agenda_points=None

            row = ( card['code'],
                    card['pack_code'],
                    card['quantity'],
                    card['title'],
                    card['side_code'],
                    card['faction_code'],
                    card['type_code'],
                    text,
                    flavor,
                    influence,
                    baselink,
                    inflimit,
                    mindeck,
                    advancement_cost,
                    agenda_points,
                    unique,
                    cost,
                    str,
                    mu,
                    datetime.strftime(datetime.today(), "%Y-%m-%d %H:%M:%S"),
                    datetime.strptime(results['last_updated'], "%Y-%m-%dT%H:%M:%S+00:00"))

            ##logging.info(row)
                        
            c.execute('''   
                        insert or replace
                        into card(cardId, setId, setQuantity, cardName, 
                                    cardSide, cardFaction, cardType,
                                    cardText, cardFlavour,
                                    cardInfluence, baseLink, influenceLimit, 
                                    minimumDeckSize, advancementCost, agendaPoints,
                                    isUnique, cost, strength, memoryUnits, dateAdded, 
                                    dateModified)
                        values (?,?,?,?,?,?,?,?,?,?,?,
                                ?,?,?,?,?,?,?,?,?,?)
                    ''', row)
            conn.commit()

            if 'keywords' in card:
                keywords = card['keywords'].split(' - ')
                for word in keywords:
                    row = (card['code'], word)
                    c.execute('''insert or ignore into cardSubType(cardId, cardSubType) values(?,?);''', row)
                    conn.commit()

        c.close()
        return 1
        
    else:
        logging.warning("No valid response.  Is there a network issue?")
        return 0



def dbInit(conn):
    
    ## Open Cursor
    c = conn.cursor()
    c.execute('PRAGMA foreign_keys = ON;')
    
    ## Create table for decks
    e = c.execute('''select exists (select * from sqlite_master where name='deck')''').fetchone()[0]
    if e != 1:
        logging.info('No deck table, creating one.')
        c.execute('''
                create table deck
                (
                    deckId int,
                    deckName text,
                    deckDescription text,
                    createdBy text,	
                    likes int,	
                    favourites int,
                    comments int,
                    dateCreated text,
                    dateUpdated text,
                    PRIMARY KEY (deckId)
                );''')
    else:
        logging.info('Found deck table, no need to create one.')
    
    ## Create table to join decks to cards
    e = c.execute('''select exists (select * from sqlite_master where name='deckList')''').fetchone()[0]
    if e != 1:
        logging.info('No decklist table, creating one.')
        c.execute('''
                create table deckList
                (
                    deckId int,
                    cardId text,
                    cardQty int,
                    PRIMARY KEY (deckId, cardId),
                    FOREIGN KEY (deckId) references deck (deckId),
                    FOREIGN KEY (cardId) references card (cardId)
                );''')
    else:
        logging.info('Found decklist table, no need to create one.')

    ## Create table for storing card subtype
    e = c.execute('''select exists (select * from sqlite_master where name='cardSubType')''').fetchone()[0]
    if e != 1:
        logging.info('No card subtype table, creating one.')
        c.execute('''
                create table cardSubType
                (
                    cardId text,
                    cardSubType text,
                    PRIMARY KEY (cardId, cardSubType),
                    FOREIGN KEY (cardId) references card (cardId)
                );''')
    else:
        logging.info('Found card subtype table, no need to create one.')
    
    ## Create set table structure
    e = c.execute('''select exists (select * from sqlite_master where name='cardSet')''').fetchone()[0]
    if e != 1:
        logging.info('No cardset table, creating one.')
        c.execute('''
                    create table [cardSet]
                    (
                        setId text,
                        setName text,
                        cycleCode text,
                        cycleName text,
                        isOwned boolean DEFAULT (0),
                        isAvailable boolean,
                        dateAvailable text,
                        PRIMARY KEY (setId)
                    );''')
    else:
        logging.info('Found cardset table, no need to create one.')
    
    ## Create card table
    e = c.execute('''select exists (select * from sqlite_master where name='card')''').fetchone()[0]
    if e != 1:
        logging.info('No card table, creating one.')
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
                        cardKeywords text,
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

                        -- Agenda details
                        advancementCost int,
                        agendaPoints int,
                    
                        dateAdded text,
                        dateModified text,
                        PRIMARY KEY (cardId),
                        FOREIGN KEY (setID) REFERENCES cardSet (setId)
                    );
                    ''')
    else:
        logging.info('Found card table, no need to create one.')

    ## Create most wanted
    e = c.execute('''select exists (select * from sqlite_master where name='mostWanted');''').fetchone()[0]
    if e != 1:
        logging.info('No most wanted table, creating one.')
        c.execute('''
            create table [mostWanted] 
                (
                    mostWantedId text,
                    mostWantedName text,
                    isActive boolean,
                    dateActive text,
                    PRIMARY KEY (mostWantedId)
                );
                ''')
    else:
        logging.info('Found most wanted table, no need to create one.')

    ## Create most wanted list
    e = c.execute('''select exists (select * from sqlite_master where name='mostWantedList');''').fetchone()[0]
    if e != 1:
        logging.info('No most wanted list table, creating one.')
        c.execute('''
            create table [mostWantedList]
                (
                    mostWantedId text,
                    cardId text,
                    cardQty int,
                    PRIMARY KEY (mostWantedId, cardId),
                    FOREIGN KEY (mostWantedId) REFERENCES mostWanted (mostWantedId),
                    FOREIGN KEY (cardId) REFERENCES card (cardId)
                 );
                 ''')                
    else:
        logging.info('Found most wanted list table, no need to create one.')

    ## Create deckSummary view
    c.execute('drop view if exists deckSummary;')
    c.execute('''
        create view deckSummary as
        select  d.deckId,
                c.cardName as deckIdentity,
                c.cardSide as side,
                c.cardFaction as deckFaction,
                d.deckName,
                d.likes,
                d.favourites,
                d.comments,
                round(d.likes / (julianday('now')-julianday(d.dateCreated))*365) as weightedLikes,
                round(d.favourites / (julianday('now')-julianday(d.dateCreated))*365) as weightedFavourites,
                round(d.comments / (julianday('now')-julianday(d.dateCreated))*365) as weightedComments,
                dateCreated
        from    deck d
                    left outer join deckList dl on d.deckId = dl.deckId
                        left outer join card c on dl.cardId = c.cardId
        where   c.cardType = 'identity';
        ''')
    
    ## Create deckReport view
    c.execute('drop view if exists deckReport;')
    c.execute('''
        create  view deckReport as
        select  ds.deckId, ds.deckIdentity, ds.side, ds.deckFaction, ds.deckName,
                ds.likes, ds.favourites, ds.comments, ds.weightedLikes, 
                ds.weightedFavourites, ds.weightedComments, ds.dateCreated,
                count(distinct case when isOwned = 0 then c.cardId else null end) missingCards,
                sum(case when isOwned = 0 then dl.cardQty else 0 end) totalMissingCards,
                count(distinct case when isOwned = 0 then cs.setId else null end) missingSets
        from    deckSummary ds
                    left outer join deckList dl on ds.deckId = dl.deckId
                        left outer join card c on dl.cardId = c.cardId
                            left outer join cardSet cs on c.setId = cs.setId
        group   by ds.deckId, ds.deckIdentity, ds.side, ds.deckFaction,
                ds.deckName, ds.likes, ds.favourites, ds.comments, ds.weightedLikes,
                ds.weightedFavourites, ds.weightedComments, ds.dateCreated;
            ''')

def updateCardData(conn):
    errorCount = 1

    if getSets(conn=conn) == 1:
        logging.info('Sucessfully updated list of card sets.')
        if getCards(conn=conn) == 1:
            logging.info('Successfully updated list of cards.')
            if updateMWL(conn=conn) == 1:
                logging.info('Successfully updated most wanted lists.')
            else:
                logging.warning('Failed to update most wanted lists.')
                errorCount+=1
        else:
            logging.warning('Failed to update list of cards.')
            errorCount+=1
    else:
        logging.warning('Failed to update list of card sets.')
        errorCount+=1

    return errorCount


def updateMWL(conn):
    logging.info('Updating Most Wanted List')
    c = conn.cursor()

    url='https://netrunnerdb.com/api/2.0/public/mwl'

    response=requests.get(url=url)

    if response.status_code == 200:
        logging.info('Found Most Wanted List')
        results=json.loads(response.content.decode('utf-8'))

        for mwl in results['data']:
            logging.info('Found '+mwl['name']+' with '+str(len(mwl['cards']))+' entries')
            row = ( mwl['id'],
                    mwl['name'],
                    mwl['active'],
                    mwl['date_start']
                  )
            c.execute('''
                        insert or ignore into mostWanted
                        (mostWantedId, mostWantedName, isActive, dateActive)                                                                                                 
                        values (?,?,?,?)''', row)                                                                                                                            
            conn.commit()
                                                                                                                                                                             
            for card, value in mwl['cards'].items():                                                                                                                         
                logging.debug('Found card '+card+' ('+str(value)+')')                                                                                                         
                row = ( mwl['id'],                                                                                                                                           
                        card,                                                                                                                                                
                        value                                                                                                                                                
                      )                                                                                                                                                      
                c.execute('''                                                                                                                                                
                            insert or ignore into mostWantedList
                            (mostWantedId, cardId, cardQty)                                                                                                                  
                            values (?,?,?)''', row)                                                                                                                          
                conn.commit()                                                                                                                                                
        return 1

    else:
        logging.warning('No response from API.')
        return 0

def main():

    ## Set up basics
    logging.basicConfig(level=logging.INFO)    
    dbFile='data.db'
   
    ## Establish database for storing data
    try:
        logging.info('Trying to find existing database.')        
        d = open(dbFile)
    
    except IOError:
        logging.warning('No database found. Creating one.')    
        firstRun = True    
        conn = sqlite3.connect(dbFile)
    
    else:    
        firstRun = False    
        conn = sqlite3.connect(dbFile)
        d.close()    
        logging.info('Found one. I hope it''s correct.')

    ## Initialise database
    dbInit(conn=conn)
    
    ## Update card data
    if updateCardData(conn=conn) == 1:
        logging.info('Successfully updated card information.')
        if config.get("CardThief", "UpdateTopDecks").upper() == "TRUE":
            logging.info("Scrapping top decks")
            getTopDecks(conn)
        else:
            logging.info("No scrape of top decks requested.")
    else:
        logging.warning('Failed to update card information.  Without up to date information, I can''t scrape decks.')

    conn.close()

if __name__ == "__main__":
    main()
