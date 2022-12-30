import requests
from bs4 import BeautifulSoup

URL = "https://www.elbruk.se/timpriser-se3-stockholm"
page = requests.get(URL)

soup = BeautifulSoup(page.content, "html.parser")
elbruk_dagspris = soup.find_all("span", class_="info-box-number")[0].text
elbruk_dagspris = float(elbruk_dagspris.replace(',','.'))


print(elbruk_dagspris)