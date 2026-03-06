import requests
from bs4 import BeautifulSoup

url = 'https://quotes.toscrape.com/'
response = requests.get(url)
soup = BeautifulSoup(response.text, 'html.parser')

# Try to find quote divs
quotes = soup.find_all('div', class_='quote')
print('Found', len(quotes), 'quote divs')

if quotes:
    q = quotes[0]
    print('\nFirst quote structure:')
    print(q.prettify()[:500])
    
    print('\nElements inside:')
    for elem in q.find_all(['p', 'span', 'a', 'small']):
        text = elem.get_text(strip=True)[:60]
        tag = elem.name
        cls = elem.get('class')
        print('  <' + tag + '> class=' + str(cls) + ': ' + text)
