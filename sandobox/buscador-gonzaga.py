import numpy as np
import pandas as pd
import bs4
import requests
import datetime

def coleta_dados_apartamentos():
    # link da imobiliaria
    LINK = f'https://gonzagaimoveis.com.br/aluguel/apartamento/curitiba'

    # requisicao
    response = requests.get(LINK)

    # declarando beautifulsoup
    site = bs4.BeautifulSoup(response.text, "html.parser")

    # selecionando classes dos imóveis
    imoveis = site.find_all('div', {'class': 'col-xs-12 imovel-box-single'})

    # armazena dados dos apartamentos em um dataframe
    df_imoveis = pd.DataFrame()

    for imovel in imoveis:
        
        # criando dataframe auxiliar para armazenar dados do imóvel
        df_imoveis_aux = pd.DataFrame()

        # link
        df_imoveis_aux.loc[0, 'link'] = imovel.find_all('a')[0]['href']
        # titulo
        df_imoveis_aux.loc[0, 'titulo'] = imovel.find_all('h3', {'class': 'titulo-grid'})[0].getText()
        # endereco
        df_imoveis_aux.loc[0, 'endereco'] = imovel.find_all('p', {'itemprop': 'streetAddress'})[0].getText()
        # tipo_locacao
        df_imoveis_aux.loc[0, 'tipo_locacao'] = imovel.find_all('span', {'class': 'thumb-status'})[0].getText()
        # preco_aluguel
        df_imoveis_aux.loc[0, 'preco_aluguel'] = imovel.find_all('span', {'class': 'thumb-price'})[0].getText() 
        # preco_condominio
        try:
            df_imoveis_aux.loc[0, 'preco_condominio'] = imovel.find_all('span', {'class': 'item-price-condominio'})[0].getText()
        except:
            df_imoveis_aux.loc[0, 'preco_condominio'] = 'Condomínio R$ 0,00'
        # preco_iptu
        try:
            df_imoveis_aux.loc[0, 'preco_iptu'] = imovel.find_all('span', {'class': 'item-price-iptu'})[0].getText()
        except:
            df_imoveis_aux.loc[0, 'preco_iptu'] = 'IPTU R$ 0,00'
        # propriedades do imóvel
        propriedades_imovel = imovel.find_all('div', {'class': 'property-amenities amenities-main'})
        propriedade = [i['class'][1] for i in propriedades_imovel[0].find_all('i')]
        valor = [i.getText() for i in propriedades_imovel[0].find_all('span')]
        for c in zip(['quartos', 'vagas_garagem', 'area', 'suites'], ['fa-bed', 'fa-car', 'fa-compress-arrows-alt', 'fa-bath']):
            try:
                df_imoveis_aux.loc[0, c[0]] = dict(zip(propriedade, valor))[c[1]]
            except:
                df_imoveis_aux.loc[0, c[0]] = 0

        # armazenando no dataframe principal
        df_imoveis = pd.concat([df_imoveis, df_imoveis_aux], axis = 0)

    print(df_imoveis.shape)

    # resetando index
    df_imoveis = df_imoveis.reset_index(drop=True)

    return df_imoveis

def data_cleaning(df):
    ### TRATAMENTO DE DADOS
    df['preco_aluguel'] = df['preco_aluguel'].astype('int64')

    df['preco_condominio'] = df['preco_condominio'].replace('', 0)
    df['preco_condominio'] = df['preco_condominio'].astype('int64')

    df['preco_iptu'] = df['preco_iptu'].replace('', 0)
    df['preco_iptu'] = df['preco_iptu'].astype('int64')

    df['quartos'] = df['quartos'].astype('int64')

    df['vagas_garagem'] = df['vagas_garagem'].astype('int64')

    df['suites'] = df['suites'].astype('int64')

    return df

def feature_engineering(df):

    ### FEATURES BÁSICAS
    # imobiliaria
    df['imobiliaria'] = 'gonzaga_imoveis'

    # data de coleta
    df['data_coleta'] = datetime.datetime.today().strftime('%Y-%m-%d')

    return df

def feature_creation(df):
    # criando feature de bairro
    df['bairro'] = df['endereco'].apply(lambda x: x.split(',')[-1].split('-')[0].strip())

    # preco_aluguel
    df['preco_aluguel'] = df['preco_aluguel'].apply(lambda x: x.replace('.','')[3:-3])

    # preco_condominio
    df['preco_condominio'] = df['preco_condominio'].apply(lambda x: x.replace('Condomínio ', '').replace( '.', '')[3:-3] if not pd.isna(x) else 0)

    # preco_iptu
    df['preco_iptu'] = df['preco_iptu'].apply(lambda x: x.replace('IPTU ', '').replace( '.', '')[3:-3] if not pd.isna(x) else 0)

    # reordenando colunas
    df = df[['imobiliaria','link', 'data_coleta','titulo','endereco','bairro','tipo_locacao','preco_aluguel', 'preco_condominio','preco_iptu','quartos','vagas_garagem','area','suites']]

    return df

def buscador_gonzaga(bairros):
    df = pd.DataFrame()
    for b in bairros:
        df_aux = coleta_dados_apartamentos(b)
        df = pd.concat([df, df_aux], axis = 0, ignore_index=True )

    ### FEATURES BÁSICAS
    # imobiliaria
    df['imobiliaria'] = 'gonzaga_imoveis'

    # data de coleta
    df['data_coleta'] = datetime.datetime.today().strftime('%Y-%m-%d')

    return df

def movimentacoes_anuncios(df):

    df_grouped = df[['endereco', 'data_coleta']].groupby('endereco').agg(data_entrada=('data_coleta', 'min'), data_saida = ('data_coleta','max')).reset_index()
    df_com_datas = pd.merge(df, df_grouped, left_on = ['endereco', 'data_coleta'], right_on= ['endereco', 'data_saida'], how= 'left')
    df_infos_mais_recentes = df_com_datas.loc[~df_com_datas['data_saida'].isna()].reset_index(drop=True)

    # criando coluna com status de atividade
    df_infos_mais_recentes['anuncio'] = df_infos_mais_recentes['data_saida'].apply(lambda x: 'Ativo' if x == datetime.datetime.today().strftime('%Y-%m-%d') else 'Inativo')

    # zerando a data de saida dos imóveis que estão ativos
    df_infos_mais_recentes.loc[df_infos_mais_recentes['anuncio'] == 'Ativo', ['data_saida']] = np.nan

    return df_infos_mais_recentes


if __name__ == '__main__':
    
    bairros = [
    'ganchinho',
    'sitio-cercado',
    'umbara',
    'abranches',
    'atuba',
    'bacacheri',
    'bairro-alto',
    'barreirinha',
    'boa-vista',
    'cachoeira',
    'pilarzinho',
    'santa-candida',
    'sao-lourenco',
    'taboao',
    'taruma',
    'tingui',
    'alto-boqueirao',
    'boqueirao',
    'hauer',
    'xaxim',
    'cajuru',
    'capao-da-imbuia',
    'guabirotuba',
    'jardim-das-americas',
    'uberaba',
    'augusta',
    'cidade-industrial',
    'riviera',
    'sao-miguel',
    'agua-verde',
    'campo-comprido',
    'fanny',
    'fazendinha',
    'guaira',
    'lindoia',
    'novo-mundo',
    'parolin',
    'portao',
    'santa-quiteria',
    'vila-izabel',
    'ahu',
    'alto-da-gloria',
    'alto-da-rua-xv',
    'batel',
    'bigorrilho',
    'bom-retiro',
    'cabral',
    'centro',
    'centro-civico',
    'cristo-rei',
    'hugo-lange',
    'jardim-botanico',
    'jardim-social',
    'juveve',
    'merces',
    'prado-velho',
    'reboucas',
    'sao-francisco',
    'campo-de-santana',
    'capao-raso',
    'caximba',
    'pinheirinho',
    'tatuquara',
    'butiatuvinha',
    'campina-do-siqueira',
    'campo-comprido',
    'cascatinha',
    'lamenha-pequena',
    'mossungue',
    'orleans',
    'santa-felicidade',
    'santo-inacio',
    'sao-braz',
    'sao-joao',
    'seminario',
    'vista-alegre',
    'jardim-social'
    ]

    # busca imóveis no bairros desejados
    df_imoveis = coleta_dados_apartamentos()

    ## CAMADA RAW
    # concatenando histórico de imóveis em csv com coleta atual
    csv_imoveis = pd.read_csv('historico_imoveis_raw.csv')
    csv_imoveis = pd.concat([csv_imoveis, df_imoveis], axis = 0)
    # salvando camada raw
    csv_imoveis.to_csv('historico_imoveis_raw.csv', index=False)

    ## Camada TRUSTED
    # criando novas colunas
    csv_imoveis = feature_creation(csv_imoveis)
    # ajustando tipos de dados
    csv_imoveis = data_cleaning(csv_imoveis)
    # salvando camada trusted
    csv_imoveis.to_csv('historico_imoveis_trusted.csv', index=False)

    # ## PORTFOLIO
    # # salvando pdf de com todos os imóveis únicos e suas movimentações
    # portfolio_imoveis = movimentacoes_anuncios(csv_imoveis)
    # portfolio_imoveis.to_csv('portfolio_imoveis.csv', index=False)