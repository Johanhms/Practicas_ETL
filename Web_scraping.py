import requests
from bs4 import BeautifulSoup
import pandas as pd

url = 'https://quotes.toscrape.com/'
response = requests.get(url)

if response.status_code != 200:
    print(f"error al acceder a la pag': {response.status_code}")
    exit()

soup = BeautifulSoup(response.text, "html.parser")

citas = soup.find_all("div", class_ = "quote")

citas[0]

resultados = []

for cita in citas:
  texto = cita.find("span", class_ = "text").get_text(strip = True)
  autor = cita.find("small", class_ = "author").get_text(strip = True)
  resultados.append({"cita": texto, "autor": autor})
  
resultados