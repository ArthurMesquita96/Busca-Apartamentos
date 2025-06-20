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
import re
from unidecode import unidecode

# app = Flask(__name__)

# @app.route("/")
def coleta_dados():

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

        anuncio_info_aux['site'] = 'Cilar'
        anuncio_info_aux['data_coleta'] = datetime.datetime.today().strftime("%Y-%m-%d")

        try:
            # titulo
            anuncio_info_aux['titulo'] = soup.findAll('h1', {'class':'title title-default'})[0].text
        except:
            anuncio_info_aux['titulo'] = np.nan

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
            anuncio_info_aux['caracteristicas_imovel'] = ' '.join([i.text for i in soup.findAll('article',{'class':'col-md-7 col-lg-8 details-property'})[0].findAll('p')[0]])
        except:
            anuncio_info_aux['caracteristicas_imovel'] = np.nan
        
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

    return anuncios

def feature_engineering(df):

    ##### TRATAMENTO INICIAL DOS DADOS #####

    df['detalhes'] = df['detalhes'].apply(lambda x: x if isinstance(x,float) else ' '.join(x).replace('Características do imóvel ', ''))

    df['iptu'] = df[['condominio','iptu']].apply(lambda x: 
                                                x['iptu'] if (isinstance(x['condominio'],float)) else 
                                                x['condominio'] if ('IPTU' in x['condominio']) else x['iptu'], axis = 1)

    df['condominio'] = df[['condominio','iptu']].apply(lambda x: 
                                                x['condominio'] if (isinstance(x['condominio'],float)) else 
                                                np.nan if ('IPTU' in x['condominio']) else x['condominio'], axis = 1)

    df['aluguel'] = df['aluguel'].apply(lambda x: x if isinstance(x,float) else int(x.replace('Aluguel','').replace('R$ ','').replace(',00','').replace('.','').split(',')[0]))
    df['condominio'] = df['condominio'].apply(lambda x: x if isinstance(x, float) else float(x.replace('Condominio  ','').replace('R$ ', '').replace('.','').replace(',','.')))
    df['iptu'] = df['iptu'].apply(lambda x: x if isinstance(x, float) else float(x.replace('IPTU  ','').replace('R$ ','').replace('.',',').replace(',','.')))
    df['seguro_incendio'] = 0

    def extrai_valores_string(string,substring):

        # Padronizar a expressão regular para encontrar a área total
        padrao = f'{substring} (\d+)'

        # Encontrar a área total usando regex
        area_total = re.search(padrao, string)

        if area_total:
            # Extrair o valor numérico da área total
            valor_area = area_total.group(1)
            
            # Remover vírgulas e converter para float
            valor_area = int(valor_area.replace(',', '.'))
            
        else:
            valor_area = 0
        
        return valor_area
    
    #### FEATURES #####

    ## Detalhes do imóvel
    df['area'] = df['detalhes'].apply(lambda x: x if isinstance(x,float) else extrai_valores_string(x,'Área Total'))
    df['quartos'] = df['detalhes'].apply(lambda x: x if isinstance(x,float) else extrai_valores_string(x,'Quartos'))
    df['suites'] = df['detalhes'].apply(lambda x: x if isinstance(x,float) else extrai_valores_string(x,'Suítes'))
    df['banheiros'] = df['detalhes'].apply(lambda x: x if isinstance(x,float) else extrai_valores_string(x,'Banheiros'))
    df['andar'] = df['detalhes'].apply(lambda x: x if isinstance(x,float) else extrai_valores_string(x,'Andar'))
    df['vagas_garagem'] = df['mais_detalhes_imovel'].apply(lambda x: 0 if pd.isnull(x) else extrai_valores_string(x,'Vagas de garagem:'))

    # Localidade
    df['bairro'] = df['endereco'].apply(lambda x: x if isinstance(x,float) else x.split(' - ')[-2])
    df['cidade'] = df['endereco'].apply(lambda x: x if isinstance(x,float) else x.split(' - ')[-1])

    # Atributos do imóvel e condomínio
    df['mobiliado'] = df['caracteristicas_imovel'].apply(lambda x: np.nan if isinstance(x,float) else 'Sim' if 'mobiliado' in unidecode(x.lower()) else 'Não')
    df['piscina'] = df['detalhes_condominio'].apply(lambda x: np.nan if isinstance(x,float) else 'Sim' if 'piscina' in unidecode(x.lower()) else 'Não')
    df['academia'] = df['detalhes_condominio'].apply(lambda x: np.nan if isinstance(x,float) else 'Sim' if 'academia' in unidecode(x.lower()) else 'Não')
    df['sacada'] = df['caracteristicas_imovel'].apply(lambda x: np.nan if isinstance(x,float) else 'Sim' if 'sacada' in unidecode(x.lower()) else 'Não')
    df['churrasqueira'] = df['detalhes_condominio'].apply(lambda x: np.nan if isinstance(x,float) else 'Sim' if 'churrasqueira' in unidecode(x.lower()) else 'Não')
    df['salao_de_festas'] = df['detalhes_condominio'].apply(lambda x: np.nan if isinstance(x,float) else 'Sim' if 'salao de festa' in unidecode(x.lower()) else 'Não')

    #### TRATAMENTO FINAL DE DADOS ####

    ## Preenchendo valores nulos
    # df[['aluguel','condominio','seguro_incendio','iptu']] = df[['aluguel','condominio','seguro_incendio','iptu']].fillna(0)
    # df['cidade'] = df['cidade'].fillna('Curitiba')
    # df[['area','quartos','suites','banheiros','vagas_garagem']] = df[['area','quartos','suites','banheiros','vagas_garagem']].fillna(0)

    ## Transformando dtypes
    df['data_coleta'] = pd.to_datetime(df['data_coleta'])
    df['cidade'] = df['cidade'].apply(lambda x: x if isinstance(x,float) else unidecode(x.capitalize())).astype('category')
    df['bairro'] = df['bairro'].apply(lambda x: x if isinstance(x,float) else unidecode(x.capitalize())).astype('category')
    df[['aluguel','condominio','seguro_incendio','iptu']] = df[['aluguel','condominio','seguro_incendio','iptu']].astype('float64')

    #### ULTIMAS FEATURES

    # df['valor_total'] = df['aluguel'] + df['condominio'] + df['seguro_incendio'] + df['iptu']

    columns_selected = [
    'site',
    'titulo',
    'link',
    'data_coleta',
    'endereco',
    'caracteristicas_imovel',
    'detalhes_condominio',
    'mais_detalhes_imovel',
    'bairro',
    'cidade',
    'aluguel',
    'condominio',
    'seguro_incendio',
    'iptu',
    'area',
    'quartos',
    'suites',
    'banheiros',
    'vagas_garagem',
    'mobiliado',
    'piscina',
    'academia',
    'sacada',
    'churrasqueira',
    'salao_de_festas'
    ]

    BUCKET_NAME = 'busca-apartamentos-trusted'
    FILE_NAME = f'{datetime.datetime.today().strftime("%Y-%m-%d")} - apartamentos - cilar.csv'
    TEMP_FILE = 'local.csv'  
    RAW_PATH = '/tmp/{}'.format(TEMP_FILE)

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(BUCKET_NAME)

    df[columns_selected].to_csv(RAW_PATH, index=False)
    blob = bucket.blob(FILE_NAME)
    blob.upload_from_filename(RAW_PATH)

    return 'Done!'


if __name__ == "__main__":
    anuncios = coleta_dados()
    # feature_engineering(anuncios)
