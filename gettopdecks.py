import requests
import logging
import sqlite3
import json
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

myPacks = ( "Core Set", 
            "Data and Destiny",
            "Creation and Control",
            "Honor and Profit",
            "Order and Chaos",
            "Opening Moves",
            "What Lies Ahead",
            "Future Proof",
            "True Colors",
            "All That Remains",
            "Breaker Bay",
            "First Contact",
            "Old Hollywood",
            "Upstalk",
            "Cyber Exodus",
            "Trace Amount",
            "Double Time",
            "Humanity's Shadow")



def getTopDecks(conn):

    logging.info("Getting top decks.")
    refreshCycle = 5
    refreshDate = datetime.strftime(datetime.today() - timedelta(days=refreshCycle), "%Y-%m-%d")
    logging.debug('Updating existing decks with timestamp older than '+refreshDate)

    c = conn.cursor()
    
    #factions = ('haas-bioroid','jinteki', 'nbn', 'weyland-consortium', 'corp', 
    #           'anarch', 'criminal', 'shaper', 'runner',
    #           'adam', 'apex', 'sunny-lebeau')

    factions = ('haas-bioroid',)
    
    ## Iterate through factions, and pages
    for faction in factions:
        for page in range(1,3):
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
            logging.info('Adding '+pack+' to list of owned sets')
            c.execute('''
                    update  cardSet
                    set     owned =  1
                    where   setName = ?;
                    ''', (pack,)) 
        conn.commit()
        
        
        c.close()
        
        return 1
    else:
        logging.warning('No response from API.  Is there a network issue?')
        return 0


def getCards(conn):        
        
    ## Get JSON Data from website
    url = 'https://netrunnerdb.com/api/2.0/public/cards'
    logging.info("Querying website "+url)
    
    response = requests.get(url=url)
    results = json.loads(response.content.decode('utf-8'))
    
    if response.status_code == 200:
        logging.info("Found card data.  Loading.")

        ## Open Cursor
        c = conn.cursor()
    
        ## Iterate over JSON results and enter into database
        for card in results['data']:
                
            if 'flavor' in card: 
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
                    card['pack_code'],
                    card['quantity'],
                    card['title'],
                    card['side_code'],
                    card['faction_code'],
                    card['type_code'],
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
                    datetime.strptime(results['last_updated'], "%Y-%m-%dT%H:%M:%S+00:00"))
                        
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
        return 1
        
    else:
        logging.warning("No valid response.  Is there a network issue?")
        return 0



def dbInit(conn):
    
    ## Open Cursor
    c = conn.cursor()
    
    ## Create table for decks
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
    
    ## Create table to join decks to cards
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

    
    ## Create set table structure
    c.execute('''
                create table [cardSet]
                (
                    setId text,
                    setName text,
                    cycleCode text,
                    cycleName text,
                    owned INTEGER DEFAULT (0),
                    isAvailable boolean,
                    dateAvailable text,
                    PRIMARY KEY (setId)
                );''')
    
    
    ## Create card table
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
                
    ## Create deckSummary view
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
    c.execute('''
        create  view deckReport as
        select  ds.deckId, ds.deckIdentity, ds.side, ds.deckFaction, ds.deckName,
                ds.likes, ds.favourites, ds.comments, ds.weightedLikes, 
                ds.weightedFavourites, ds.weightedComments, ds.dateCreated,
                count(distinct case when owned = 0 then c.cardId else null end) missingCards,
                sum(case when owned = 0 then dl.cardQty else 0 end) totalMissingCards,
                count(distinct case when owned = 0 then cs.setId else null end) missingSets
        from    deckSummary ds
                    left outer join deckList dl on ds.deckId = dl.deckId
                        left outer join card c on dl.cardId = c.cardId
                            left outer join cardSet cs on c.setId = cs.setId
        group   by ds.deckId, ds.deckIdentity, ds.side, ds.deckFaction,
                ds.deckName, ds.likes, ds.favourites, ds.comments, ds.weightedLikes,
                ds.weightedFavourites, ds.weightedComments, ds.dateCreated;
            ''')

def main():

    ## Set up basics
    logging.basicConfig(level=logging.DEBUG)    
    dbFile='data.db'
   
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
    
    if getSets(conn=conn) == 1:
        if getCards(conn=conn) == 1:
            getTopDecks(conn)
        else:
            logging.warning('Failed to get cards.  Did not proceed to get decks')
    else:
        logging.warning('Failed to get card sets.  Did not proceed to get cards')
    conn.close()

if __name__ == "__main__":
    main()
