import numpy as np
import pandas as pd
import math 
from google.cloud import storage
import os
from unidecode import unidecode
from datetime import datetime, timedelta
import requests

def get_bucket_files():
        BUCKET_NAME = 'busca-apartamentos-bucket'

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
            df_aux = pd.read_csv(f'gs://busca-apartamentos-bucket/{file_name}')
            df_full = pd.concat([df_full, df_aux], axis = 0)
        except:
            pass

    return df_full

def tratamento_valor_aluguel(string):
    if (not pd.isna(string)):
        string = int(string.split('$ ')[-1].split(',')[0].replace('.',''))
    else:
        string = 0
    return string

def tratamento_valor_condominio(string):

    if (not pd.isna(string)) and (not 'consulta' in string):
        string = string.split('$ ')[-1].split(',')[0]
        if (len(string.split('.')[-1]) == 2):
            string = int(string.split('.')[0])
        else:
            string = int(string.replace('.',''))
    else:
        string = 0

    return string

def busca_substring(substring, string_list):
    result = np.nan
    for s in string_list:
        if substring in s:
            try:
                result = re.findall(r'\s(\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?)', s)[0]
            except:
                result = s
            break

    if pd.isna(result):
        return '0'
    else:    
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
    
def formata_valores(valores):
    return valores.str.replace('.','').apply(lambda x: x if pd.isna(x) else x.split(',')[0]).astype('float64')

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
        valor_area = np.nan
    
    return valor_area

def build_portfolio(df_full):
    # removendo imóveis com falha na busca
    df_full = df_full.loc[~df_full['titulo'].isna()]

    # calculando data máxima de mínima de cada anuncio
    df_grouped = df_full[['endereco','data_coleta']].groupby('endereco').agg(data_entrada=('data_coleta','min'), data_saida=('data_coleta','max')).reset_index()
    portfolio = pd.merge(df_full, df_grouped, on = 'endereco', how='left').sort_values('data_coleta', ascending=False).drop_duplicates('endereco', keep='last')

    portfolio['aluguel'] = portfolio[['site','aluguel','valores']].apply(lambda x: x['valores'].split(',')[0].split('$ ')[-1].replace('.','') if x['site'] == 'Apolar' else tratamento_valor_aluguel(x['aluguel']), axis=1).astype('float64')
    portfolio['condominio'] = portfolio[['site','condominio','valores']].apply(lambda x: busca_substring('Condomínio',separa_valores_imovel(x['valores'])).split(',')[0] if x['site'] == 'Apolar' else tratamento_valor_condominio(x['condominio']), axis=1).astype('float64')
    portfolio['valor_total'] = portfolio['aluguel'] + portfolio['condominio']

    # Apolar
    portfolio.loc[portfolio['site'] == 'Apolar', 'area'] = portfolio.loc[portfolio['site'] == 'Apolar', 'atributos'].apply(lambda x: x if pd.isna(x)  else busca_substring('m²', x.split(', '))).str.replace('m²','')
    portfolio.loc[portfolio['site'] == 'Apolar', 'banheiros'] = portfolio.loc[portfolio['site'] == 'Apolar', 'atributos'].apply(lambda x: x if pd.isna(x)  else busca_substring('banheiro', x.split(', '))).str.replace('banheiro','').str.replace('s','').fillna(0)
    portfolio.loc[portfolio['site'] == 'Apolar', 'quartos'] = portfolio.loc[portfolio['site'] == 'Apolar', 'atributos'].apply(lambda x: x if pd.isna(x)  else busca_substring('quarto', x.split(', '))).str.replace('quarto','').str.replace('s','').fillna(0)
    portfolio.loc[portfolio['site'] == 'Apolar', 'suites'] = portfolio.loc[portfolio['site'] == 'Apolar', 'atributos'].apply(lambda x: x if pd.isna(x)  else busca_substring('suite', x.split(', '))).str.replace('suite','').str.replace('s','').fillna(0)
    portfolio.loc[portfolio['site'] == 'Apolar', 'vagas_garagem'] = portfolio.loc[portfolio['site'] == 'Apolar', 'atributos'].apply(lambda x: x if pd.isna(x) else busca_substring('vagas', x.split(', '))).str.replace('vaga','').str.replace('s','').fillna(0)

    # Cilar
    portfolio['detalhes'] = portfolio['detalhes'].apply(lambda x: x if pd.isna(x) else ast.literal_eval(x))
    portfolio['detalhes'] = portfolio['detalhes'].apply(lambda x: ' '.join(x).replace('Características do imóvel ','').strip() if isinstance(x,list) else x)

    portfolio.loc[portfolio['site'] == 'Cilar', 'area'] = portfolio.loc[portfolio['site'] == 'Cilar', 'detalhes'].apply(lambda x: 0 if pd.isna(x) else extrai_valores_string(x,'Área Total')).fillna(0).astype('int64')
    portfolio.loc[portfolio['site'] == 'Cilar', 'quartos'] = portfolio.loc[portfolio['site'] == 'Cilar', 'detalhes'].apply(lambda x: 0 if  pd.isna(x) else extrai_valores_string(x,'Quartos')).fillna(0).astype('int64')
    portfolio.loc[portfolio['site'] == 'Cilar', 'suites'] = portfolio.loc[portfolio['site'] == 'Cilar', 'detalhes'].apply(lambda x: 0 if  pd.isna(x) else extrai_valores_string(x,'Suítes')).fillna(0).astype('int64')
    portfolio.loc[portfolio['site'] == 'Cilar', 'banheiros'] = portfolio.loc[portfolio['site'] == 'Cilar', 'detalhes'].apply(lambda x: 0 if  pd.isna(x) else extrai_valores_string(x,'Banheiros')).fillna(0).astype('int64')
    portfolio.loc[portfolio['site'] == 'Cilar', 'andar'] = portfolio.loc[portfolio['site'] == 'Cilar', 'detalhes'].apply(lambda x: 0 if  pd.isna(x) else extrai_valores_string(x,'Andar')).fillna(0).astype('int64')
    portfolio.loc[portfolio['site'] == 'Cilar', 'vagas_garagem'] = portfolio.loc[portfolio['site'] == 'Cilar', 'mais_detalhes_imovel'].apply(lambda x: 0 if  pd.isna(x) else extrai_valores_string(x,'Vagas de garagem:')).fillna(0).astype('int64')

    portfolio.loc[portfolio['site'] == 'razao', 'atributos'] = portfolio.loc[portfolio['site'] == 'razao', 'atributos'].apply(lambda x: x if pd.isna(x) else ast.literal_eval(x))
    # formatando valores
    portfolio.loc[portfolio['site'] == 'razao', 'area'] = portfolio.loc[portfolio['site'] == 'razao', 'atributos'].apply(lambda x: x[4].split(' ')[0]).replace('(--)',0)
    portfolio.loc[portfolio['site'] == 'razao', 'quartos'] = portfolio.loc[portfolio['site'] == 'razao', 'atributos'].apply(lambda x: x[0]).replace('(--)',0)
    portfolio.loc[portfolio['site'] == 'razao', 'suites'] = portfolio.loc[portfolio['site'] == 'razao', 'atributos'].apply(lambda x: x[1]).replace('(--)',0)
    portfolio.loc[portfolio['site'] == 'razao', 'banheiros'] = portfolio.loc[portfolio['site'] == 'razao', 'atributos'].apply(lambda x: x[2]).replace('(--)',0)
    portfolio.loc[portfolio['site'] == 'razao', 'vagas_garagem'] = portfolio.loc[portfolio['site'] == 'razao', 'atributos'].apply(lambda x: x[3]).replace('(--)',0)

    portfolio['endereco'] = portfolio['endereco'].apply(lambda x: x if pd.isna(x) else x.replace('\n','').replace('  ',''))

    # criando coluna de status anuncio
    portfolio['status'] = portfolio['data_saida'].apply(lambda d: 'ativo' if pd.to_datetime(d) == max(pd.to_datetime(portfolio['data_saida'])) else 'inativo')

    # criando coluna de entrada do anuncio em dias
    portfolio['entrada_em_dias'] = portfolio[['data_entrada','data_saida']].apply(lambda d: (pd.to_datetime(d['data_saida']) - pd.to_datetime(d['data_entrada'])).days, axis=1)

    return portfolio 

def build_apartamentos_entrantes(portfolio):
    apartamentos_entrantes = portfolio.loc[
    (portfolio['status'] == 'ativo') & 
    (portfolio['entrada_em_dias'] == 0) & 
    (portfolio['quartos'] != '1')
    ].reset_index().sort_values('aluguel', ascending=True)

    apartamentos_entrantes.loc[:,'texto'] = apartamentos_entrantes[['quartos','endereco','link','area','valor_total']].apply(lambda x:
    f'''
📍 Endereco: {x['endereco']}
📏 Area: {x['area']}
🛏️ Quartos: {x['quartos']}
💵 Valor aproximado: {x['valor_total']}
🗺️ Maps: https://www.google.com.br/maps/place/{x['endereco'].replace(' -', ',').replace(' ','-')}
🔗 Link: {x['link']}
    ''', 
    axis=1)

    return apartamentos_entrantes

def send_message_telegram(apartamentos_entrantes):
    TOKEN = '6807526969:AAHh2qyButkfg8ofvYWRHa0XUJXQVP3a4yM'
    chat_id = 620603429
    text = f"Relatório de Apartamentos Novos de {datetime.today().strftime('%Y-%m-%d')}"

    url = f'https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={chat_id}&text={text}'
    response = requests.post(url)

    for infos in apartamentos_entrantes['texto'].tolist():
        url = f'https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={chat_id}&text={infos}'
        response = requests.post(url)

    return None

data_diff = 2

df_files = get_bucket_files()
df_full = get_anuncios(df_files, data_diff)
portfolio = build_portfolio(df_full)
ap_entrantes = build_apartamentos_entrantes(portfolio)
send_message_telegram(ap_entrantes)