from flask import Flask
import os
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import bs4
import pandas as pd
import datetime
from google.cloud import storage
import time 
import numpy as np

# app = Flask(__name__)

# @app.route("/")
def hello_world():

    def get_last_page(LINK):
        print('Coletando número de páginas')
        chrome_options = Options()
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--disable-dev-shm-usage')
        
        driver = webdriver.Chrome(options = chrome_options)
        driver.get(LINK)

        time.sleep(3)

        soup = bs4.BeautifulSoup(driver.page_source, 'html.parser')
        list_aux = [i.text for i in soup.findAll('li',{'class': 'paginate_button'})]

        pages = []
        for i in list_aux:
            try:
                pages.append(int(i))
            except:
                pass
        
        last_page = pages[-1]

        driver.quit()

        return last_page
    
    def get_anuncio_link(anuncio):
        anuncio_link = {}
        anuncio_link['link'] = 'https://cilar.com.br' + anuncio.findAll('a')[0]['href']

        return anuncio_link

    def get_anuncios_links(last_page):

        anuncios_links = []
        print('Número total de páginas: {}'.format(last_page))

        for page in range(1,last_page + 1):

            LINK = f'https://cilar.com.br/alugar/apartamento/curitiba-pr?valueType=price&valueMin=100.00&valueMax=30000.00&areaType=area_total&areaMin=0&areaMax=10000&order=1&page={page}'

            print('page: {}'.format(page))
            chrome_options = Options()
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--disable-dev-shm-usage')
            
            driver = webdriver.Chrome(options = chrome_options)
            driver.get(LINK)

            time.sleep(3)

            soup = bs4.BeautifulSoup(driver.page_source, 'html.parser')
            anuncios = soup.findAll('div', {'class':'box'})
            for anuncio in anuncios:
                anuncio_aux = get_anuncio_link(anuncio)
                anuncios_links.append(anuncio_aux)

        print('Todos os links coletados!')

        return pd.DataFrame(anuncios_links)
                
    def get_info_anuncio(driver, link):
        
        # print(link)
        driver.get(link)
        soup = bs4.BeautifulSoup(driver.page_source, 'html.parser')

        anuncio_info_aux = {}

        try:
            # titulo
            anuncio_info_aux['titulo'] = soup.findAll('h1', {'class':'title title-default'})[0].text
        except:
            anuncio_info_aux['titulo'] = np.nan

        anuncio_info_aux['data_coleta'] = datetime.datetime.today().strftime("%Y-%m-%d")

        try:
        # titulo
            anuncio_info_aux['link'] = link
        except:
            anuncio_info_aux['link'] = np.nan
        
        try:
            # titulo
            anuncio_info_aux['endereco'] = soup.findAll('a', {'class': 'anchor'})[0].find('p').text
        except:
            anuncio_info_aux['endereco'] = np.nan

        try:
            #detalhes
            anuncio_info_aux['detalhes'] = soup.findAll('div',{'class': 'list'})[0].text.split()
        except:
            anuncio_info_aux['detalhes'] = np.nan

        try:
            # aluguel
            anuncio_info_aux['aluguel'] = soup.findAll('div',{'class': 'rental'})[0].find('h3').text
        except:
            anuncio_info_aux['aluguel'] = np.nan

        try:
            # condominio
            anuncio_info_aux['condominio'] = soup.findAll('div',{'class': 'condominium'})[0].findAll('dl')[0].text
        except:
            anuncio_info_aux['condominio'] = np.nan

        try:
            # iptu
            anuncio_info_aux['iptu'] = soup.findAll('div',{'class': 'condominium'})[0].findAll('dl')[1].text
        except:
            anuncio_info_aux['iptu'] = np.nan

        try:
            # características do imóvel
            anuncio_info_aux['catacteristicas_imovel'] = ' '.join([i.text for i in soup.findAll('article',{'class':'col-md-7 col-lg-8 details-property'})[0].findAll('p')[0]])
        except:
            anuncio_info_aux['catacteristicas_imovel'] = np.nan
        
        try:
            # detalhes do condominio
            anuncio_info_aux['detalhes_condominio'] = ', '.join([i.text for i in soup.findAll('article',{'class':'col-md-7 col-lg-8 details-property'})[0].findAll('p')[1::]])
        except:
            anuncio_info_aux['detalhes_condominio'] = np.nan

        try:
            # mais detalhes do imóvel
            anuncio_info_aux['mais_detalhes_imovel'] = ', '.join([i.text.replace('\n', '').replace('  ', '') for i in soup.findAll('ul',{'class':'list-arrow'})[0]])
        except:
            anuncio_info_aux['mais_detalhes_imovel'] = np.nan
            

        return anuncio_info_aux

    def get_infos_anuncios(anuncios_links):
        print('Coletando informações dos anuncios')
        anuncios_infos = []
        for link in anuncios_links['link'].tolist():

            chrome_options = Options()
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--disable-dev-shm-usage')

            driver = webdriver.Chrome(options=chrome_options)
            anuncio_info_aux = get_info_anuncio(driver, link)

            anuncios_infos.append(anuncio_info_aux)

        return pd.DataFrame(anuncios_infos)    

    LINK = 'https://cilar.com.br/alugar/apartamento/curitiba-pr?valueType=price&valueMin=100.00&valueMax=30000.00&areaType=area_total&areaMin=0&areaMax=10000&order=1&page=1'

    last_page = get_last_page(LINK)

    anuncios_links = get_anuncios_links(last_page)

    anuncios = get_infos_anuncios(anuncios_links)

    BUCKET_NAME = 'busca-apartamentos-bucket'
    FILE_NAME = f'{datetime.datetime.today().strftime("%Y-%m-%d")} - apartamentos - cilar.csv'
    TEMP_FILE = 'local.csv'  
    RAW_PATH = '/tmp/{}'.format(TEMP_FILE)

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(BUCKET_NAME)

    anuncios.to_csv(RAW_PATH, index=False)
    blob = bucket.blob(FILE_NAME)
    blob.upload_from_filename(RAW_PATH)

    return 'Done!'


if __name__ == "__main__":
    # app.run(debug=True, host='0.0.0.0', port=int(os.environ.get("PORT", 8080)))
    hello_world()