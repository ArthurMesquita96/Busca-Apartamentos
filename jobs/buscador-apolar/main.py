import os
from google.cloud import storage
import pandas as pd
import numpy as np
from flask import Flask
import bs4
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
import time
import datetime
import re
from unidecode import unidecode


def coleta_dados():

    def get_vitrine(LINK):
        chrome_options = Options()
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--disable-dev-shm-usage')
        
        driver = webdriver.Chrome(options = chrome_options)
        driver.get(LINK)
       
        # LINK =  "https://www.apolar.com.br/alugar/apartamento/curitiba?mensal"
        # driver.maximize_window()

        SCROLL_PAUSE_TIME = 5

        # Get scroll height
        last_height = driver.execute_script("return document.body.scrollHeight")

        while True:
        # for i in range(5):
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

    def get_anuncios_infos(links_anuncios):
        anuncios_infos = []

        for link_anuncio in links_anuncios['link'].tolist():

            # time.sleep(2)

            anuncio_info_aux = get_anuncio_infos(link_anuncio)

            anuncios_infos.append(anuncio_info_aux)
                
        return pd.DataFrame(anuncios_infos)

    def get_anuncio_infos(link_anuncio):
        
        chrome_options = Options()
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument("--disable-notifications")
        driver = webdriver.Chrome(options=chrome_options)

        driver.get(link_anuncio)
        # print(link_anuncio)
        # time.sleep(2)
        page = bs4.BeautifulSoup(driver.page_source, 'html.parser')
        anuncio_infos = {}

        anuncio_infos['site'] = 'Apolar'
        anuncio_infos['data_coleta'] = datetime.datetime.today().strftime('%Y-%m-%d')
        try:
            anuncio_infos['titulo'] = page.findAll('h1', {'class':'property-title'})[0].text
        except:
            anuncio_infos['titulo'] = np.nan
        try:
            anuncio_infos['link'] = link_anuncio
        except:
            anuncio_infos['link'] = np.nan
        try:
            anuncio_infos['endereco'] = page.findAll('a', {'class':'property-address'})[0].text
        except:
            anuncio_infos['endereco'] = np.nan
        try:
            anuncio_infos['valores'] = page.findAll('table', {'class':'sohtectable-style'})[0].text
        except:
            anuncio_infos['valores'] = np.nan
        try:
            anuncio_infos['atributos'] = ', '.join([i.text for i in page.findAll('li',{'class':'highlight'})]).replace('\n','').replace('                        ',' ').replace('                      ', ' ').replace('(','').replace(')','').strip().replace('  ',', ')
        except:
            anuncio_infos['atributos'] = np.nan
        try:
            anuncio_infos['descricao'] = page.findAll('div', {'class':'description'})[0].text
        except:
            anuncio_infos['descricao'] = np.nan

        driver.quit()

        return anuncio_infos


    LINK = "https://www.apolar.com.br/alugar/apartamento/curitiba?mensal"

    vitrine = get_vitrine(LINK)

    lista_de_anuncios = vitrine.findAll('div',{'class':'property-component'})

    links_anuncios = get_anuncios_links(lista_de_anuncios)

    anuncios = get_anuncios_infos(links_anuncios)

    BUCKET_NAME = 'busca-apartamentos-bucket'
    FILE_NAME = f'{datetime.datetime.today().strftime("%Y-%m-%d")} - apartamentos - apolar.csv'
    TEMP_FILE = 'local.csv'
    RAW_PATH = '/tmp/{}'.format(TEMP_FILE)

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(BUCKET_NAME)

    anuncios.to_csv(RAW_PATH, index=False)
    blob = bucket.blob(FILE_NAME)
    blob.upload_from_filename(RAW_PATH)

    return anuncios

def feature_engineering(df):

    df['titulo'] = df['titulo'].str.strip()
    df['endereco'] = df['endereco'].str.strip()
    df['descricao'] = df['descricao'].str.strip().str.replace('  ', '').str.replace('\n',' ')

    def busca_substring(substring, string_list):
        result = np.nan
        for s in string_list:
            if substring in s:
                try:
                    result = re.findall(r'\s(\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?)', s)[0]
                except:
                    result = s
                break
                
        return result

    def separa_valores_imovel(string):

        # Padrao regex para encontrar nome e valor monetário
        padrao = r'(\w+)\sR\$\s(\d{1,3}(?:\.\d{3})*(?:,\d{2})?)'

        # Encontrar todas as correspondências na string
        correspondencias = re.findall(padrao, string)

        # Imprimir os resultados
        list_values = []
        for correspondencia in correspondencias:
            nome, valor = correspondencia
            list_values.append(f'{nome}: {valor}')
        
        return list_values

    # Localidade
    df['bairro'] = df['endereco'].apply(lambda x: x if isinstance(x,float) else x.split(', ')[2].split(' - ')[0])
    df['cidade'] = df['endereco'].apply(lambda x: x if isinstance(x,float) else x.split(', ')[2].split(' -')[1])

    # Atributos do imóvel
    df['area'] = df['atributos'].apply(lambda x: x if isinstance(x,float) else busca_substring('m²', x.split(', ')))
    df['banheiros'] = df['atributos'].apply(lambda x: x if isinstance(x,float) else busca_substring('banheiro', x.split(', ')))
    df['vagas_garagem'] = df['atributos'].apply(lambda x: x if isinstance(x,float) else busca_substring('vaga', x.split(', ')))
    df['quartos'] = df['atributos'].apply(lambda x: x if isinstance(x,float) else busca_substring('quarto', x.split(', ')))
    df['suites'] = df['atributos'].apply(lambda x: x if isinstance(x,float) else busca_substring('suite', x.split(', ')))

    df['area'] = df['area'].apply(lambda x: np.nan if isinstance(x, float) else x.split(' ')[0]).astype('float64')
    df['banheiros'] = df['banheiros'].apply(lambda x: np.nan if isinstance(x, float) else x.split(' ')[0]).astype('float64')
    df['vagas_garagem'] = df['vagas_garagem'].apply(lambda x: np.nan if isinstance(x, float) else x.split(' ')[0]).astype('float64')
    df['quartos'] = df['quartos'].apply(lambda x: np.nan if isinstance(x, float) else x.split(' ')[0]).astype('float64')
    df['suites'] = df['suites'].apply(lambda x: np.nan if isinstance(x, float) else x.split(' ')[0]).astype('float64')

    # Valores
    df['aluguel'] = df['valores'].apply(lambda x: x if isinstance(x,float) else busca_substring('Aluguel',separa_valores_imovel(x)))
    df['condominio'] = df['valores'].apply(lambda x: x if isinstance(x,float) else busca_substring('Condomínio',separa_valores_imovel(x)))
    df['seguro_incendio'] = df['valores'].apply(lambda x: x if isinstance(x,float) else busca_substring('Incêndio',separa_valores_imovel(x)))
    df['iptu'] = df['valores'].apply(lambda x: x if isinstance(x,float) else busca_substring('IPTU',separa_valores_imovel(x)))

    df['aluguel'] = df['aluguel'].apply(lambda x: x if isinstance(x,float) else x.replace(',00', '').replace('.','').replace(',','.')).astype('float64')
    df['condominio'] = df['condominio'].apply(lambda x: x if isinstance(x,float) else x.replace(',00', '').replace('.','').replace(',','.')).astype('float64')
    df['seguro_incendio'] = df['seguro_incendio'].apply(lambda x: x if isinstance(x,float) else x.replace(',00', '').replace('.','').replace(',','.')).astype('float64')
    df['iptu'] = df['iptu'].apply(lambda x: x if isinstance(x,float) else x.replace(',00', '').replace('.','').replace(',','.')).astype('float64')
    df['valor_total'] = df['aluguel'] + df['condominio'] + df['seguro_incendio'] + df['iptu']

    # Detalhes do imóvel/condomínio
    df['mobiliado'] = df['descricao'].apply(lambda x: np.nan if isinstance(x,float) else 'Sim' if 'mobiliado' in unidecode(x.lower()) else 'Não')
    df['piscina'] = df['descricao'].apply(lambda x: np.nan if isinstance(x,float) else 'Sim' if 'piscina' in unidecode(x.lower()) else 'Não')
    df['academia'] = df['descricao'].apply(lambda x: np.nan if isinstance(x,float) else 'Sim' if 'academia' in unidecode(x.lower()) else 'Não')
    df['sacada'] = df['descricao'].apply(lambda x: np.nan if isinstance(x,float) else 'Sim' if 'sacada' in unidecode(x.lower()) else 'Não')
    df['churrasqueira'] = df['descricao'].apply(lambda x: np.nan if isinstance(x,float) else 'Sim' if 'churrasqueira' in unidecode(x.lower()) else 'Não')
    df['salao_de_festas'] = df['descricao'].apply(lambda x: np.nan if isinstance(x,float) else 'Sim' if 'salao de festas' in unidecode(x.lower()) else 'Não')
    
    columns_selected = [
    'site',
    'titulo',
    'link',
    'data_coleta',
    'endereco',
    'atributos',
    'descricao',
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
    FILE_NAME = f'{datetime.datetime.today().strftime("%Y-%m-%d")} - apartamentos - apolar.csv'
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
    feature_engineering(anuncios)