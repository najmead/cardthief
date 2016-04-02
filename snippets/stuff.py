import requests
from bs4 import BeautifulSoup

url = 'http://netrunnerdb.com/en/decklist/33347/turntableela-gang-sign-undefeated-sc-1st-before-top-4-'

response = requests.get(url=url)

if response.status_code == 200:
    soup = BeautifulSoup(response.text, 'html.parser')
    
    section = soup.find(id="social-icon-like")
    
    print(section.find("span", class_="num").next)
    
    print(section)
    
    print(soup.find_all("span", class_="num"))
    bit=soup.find(id="social-icon-like")
    print(bit.find(id="num"))
    print(soup.prettify())