import requests
import logging
import sqlite3
import json

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
                        insert or replace into mostWanted
                        (mostWantedId, mostWantedName, isActive, dateActive)
                        values (?,?,?,?)''', row)
            conn.commit()

            for card, value in mwl['cards'].items():
                logging.info('Found card '+card+' ('+str(value)+')')
                row = ( mwl['id'],
                        card,
                        value
                      )
                c.execute('''
                            insert or replace into mostWantedList
                            (mostWantedId, cardId, cardQty)
                            values (?,?,?)''', row)
                conn.commit()

    else:
        logging.warning('No response from API')

def main():
    logging.basicConfig(level=logging.DEBUG)
    dbFile='data.db'
    conn=sqlite3.connect(dbFile)
    
    updateMWL(conn=conn)

if __name__ == "__main__":
     main()
