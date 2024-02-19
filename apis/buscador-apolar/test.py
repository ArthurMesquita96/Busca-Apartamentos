import os
from google.cloud import storage
import pandas as pd
import numpy as np
from flask import Flask
import bs4
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
import time

def get_vitrine(LINK):
    chrome_options = Options()
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--disable-dev-shm-usage')

    try:
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options = chrome_options)
        driver.get(LINK)
    except:
        raise('Erro ao iniciar navegador')

    # LINK =  "https://www.apolar.com.br/alugar/apartamento/curitiba?mensal"
    # driver.maximize_window()

    SCROLL_PAUSE_TIME = 5

    # Get scroll height
    last_height = driver.execute_script("return document.body.scrollHeight")

    # while True:
    for i in range(2):
        # Scroll down to bottom
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

        try:
            load_more = driver.find_element(By.CLASS_NAME, 'load-more')
            load_more.click()
        except:
            pass

        # Wait to load page
        time.sleep(SCROLL_PAUSE_TIME)

        
        # Calculate new scroll height and compare with last scroll height
        new_height = driver.execute_script("return document.body.scrollHeight")


        if new_height == last_height:
            break
        last_height = new_height
        print('scroll')

    soup = bs4.BeautifulSoup(driver.page_source, 'html.parser')

    driver.quit()

    return soup

def get_anuncio_link(anuncio):
    anuncio_info = {}
    anuncio_info['link'] = anuncio.findAll('a')[-1]['href']

    return anuncio_info

def get_anuncios_links(lista_de_anuncios):
    anuncio_list = []
    for anuncio in lista_de_anuncios:

        anuncio_aux = get_anuncio_link(anuncio)

        anuncio_list.append(anuncio_aux)

    return pd.DataFrame(anuncio_list)

LINK = "https://www.apolar.com.br/alugar/apartamento/curitiba?mensal"

vitrine = get_vitrine(LINK)

lista_de_anuncios = vitrine.findAll('div',{'class':'property-component'})

links_anuncios = get_anuncios_links(lista_de_anuncios)

BUCKET_NAME = 'busca-apartamentos-bucket'
FILE_NAME = 'test.csv'
TEMP_FILE = 'local.csv'
RAW_PATH = '/tmp/{}'.format(TEMP_FILE)

storage_client = storage.Client()
bucket = storage_client.get_bucket(BUCKET_NAME)

d = {'col1': [1, 2], 'col2': [3, 4]}
df = pd.DataFrame(data=d)

# links_anuncios = pd.DataFrame(links_anuncios)
links_anuncios.to_csv(RAW_PATH, index=False)
blob = bucket.blob(FILE_NAME)
blob.upload_from_filename(RAW_PATH)