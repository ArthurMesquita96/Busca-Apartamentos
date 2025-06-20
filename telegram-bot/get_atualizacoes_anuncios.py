import numpy as np
import pandas as pd
import math 
from google.cloud import storage
import os
from unidecode import unidecode
from datetime import datetime, timedelta
import requests

def get_bucket_files():
    BUCKET_NAME = 'busca-apartamentos-trusted'

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(BUCKET_NAME)

    # lista objetos no bucket
    files = [i.name for i in bucket.list_blobs()]

    # Criando DataFrame
    files = pd.DataFrame(files, columns=['name'])

    # criando coluna de data
    files['date'] = pd.to_datetime(files['name'].apply(lambda f: f.split(' - ')[0]))

    return files

def get_anuncios(df_files, data_diff):

    dates = df_files['date'].sort_values(ascending=False).drop_duplicates().reset_index(drop=True)[:data_diff].tolist()

    df_full = pd.DataFrame()

    for file_name in df_files.loc[df_files['date'].isin(dates),'name']:
        try:
            df_aux = pd.read_csv(f'gs://busca-apartamentos-trusted/{file_name}')
            df_full = pd.concat([df_full, df_aux], axis = 0)
        except:
            pass

    return df_full

def build_portfolio(df_full):
    # removendo imóveis com falha na busca
    df_full = df_full.loc[~df_full['titulo'].isna()]

    # calculando data máxima de mínima de cada anuncio
    df_grouped = df_full[['endereco','data_coleta']].groupby('endereco').agg(data_entrada=('data_coleta','min'), data_saida=('data_coleta','max')).reset_index()
    portfolio = pd.merge(df_full, df_grouped, on = 'endereco', how='left').sort_values('data_coleta', ascending=False).drop_duplicates('endereco', keep='first')

    portfolio['aluguel'] = portfolio['aluguel'].apply(lambda x: int(x) if not math.isnan(x) else np.nan )
    portfolio['condominio'] = portfolio['condominio'].apply(lambda x: int(x) if not math.isnan(x) else int(0))

    # print(portfolio['aluguel'])

    # criando coluna de status anuncio
    portfolio['status'] = portfolio['data_saida'].apply(lambda d: 'ativo' if pd.to_datetime(d) == max(pd.to_datetime(portfolio['data_saida'])) else 'inativo')

    # criando coluna de entrada do anuncio em dias
    portfolio['entrada_em_dias'] = portfolio[['data_entrada','data_saida']].apply(lambda d: (pd.to_datetime(d['data_saida']) - pd.to_datetime(d['data_entrada'])).days, axis=1)

    return portfolio

def build_apartamentos_entrantes(portfolio):
    apartamentos_entrantes = portfolio.loc[
    (portfolio['status'] == 'ativo') & 
    (portfolio['entrada_em_dias'] == 0)
    ].reset_index().sort_values('aluguel', ascending=True)

    apartamentos_entrantes.loc[:,'texto'] = apartamentos_entrantes[['bairro','endereco','link','area','aluguel', 'condominio']].apply(lambda x:
    f'''
Bairro: {x['bairro']}
Endereco: {x['endereco']}
Maps: https://www.google.com.br/maps/place/{x['endereco'].replace(' -', ',').replace(' ','-')}
Area: {x['area']}
Valor aproximado: {x['aluguel'] + x['condominio']}
Link: {x['link']}
    ''', 
    axis=1)

    return apartamentos_entrantes

def send_message_telegram(apartamentos_entrantes):

    apartamentos_entrantes = apartamentos_entrantes.loc[(apartamentos_entrantes['aluguel'] + apartamentos_entrantes['condominio']) <= 2000]

    TOKEN = '6807526969:AAHh2qyButkfg8ofvYWRHa0XUJXQVP3a4yM'
    chat_id = 620603429
    text = f"Relatório de Apartamentos Novos de {datetime.today().strftime('%Y-%m-%d')}"

    url = f'https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={chat_id}&text={text}'
    response = requests.post(url)

    for infos in apartamentos_entrantes['texto'].tolist():
        url = f'https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={chat_id}&text={infos}'
        response = requests.post(url)

    return None
 
if __name__ == '__main__':
    data_diff = 2

    df_files = get_bucket_files()
    df_full = get_anuncios(df_files, data_diff)
    portfolio = build_portfolio(df_full)
    ap_entrantes = build_apartamentos_entrantes(portfolio)
    send_message_telegram(ap_entrantes)