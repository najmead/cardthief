import requests
import html2text

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
                
def getCards(conn):        
        
    ## Get JSON Data from website
    url = 'http://netrunnerdb.com/api/cards/'
    response = requests.get(url=url)
    results = json.loads(response.text)

    #h = html2text.HTML2Text()
    
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
        #    text = h.handle(card['text'])
            text=card['text']
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

    c = conn.cursor()
    
    getCards(conn=conn)

    conn.close()
    
if __name__ == "__main__":
    main()
