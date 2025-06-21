# 🔍 Monitoramento Diário de Novos Imóveis para Locação
Este projeto tem como objetivo auxiliar pessoas na busca por imóveis para alugar, automatizando a identificação de novos imóveis disponíveis para locação. 
- Através de web scraping diário, o sistema identifica quais imóveis foram recentemente disponibilizados para locação e notifica o usuário sobre esseas oportunidades, permitindo que ele veja, em primeira mão, os mais novos anúncios.

## Motivação
Encontrar um bom imóvel para alugar exige velocidade e acompanhamento constante.
- Muitas das melhores oportunidades são alugadas em poucas horas após a publicação. Além disso, realizar buscas manuais todos os dias em diversos sites de anuncios de imóveis é trabalhoso e ineficiente.
O objetivo deste projeto é automatizar o monitoramento desses imóveis.
- Dessa forma, o usuário é informado rapidamente sobre os anúncios mais recentes, tendo um acompanhamento mais eficiente dos imóveis no processo de locação

## Benefícios
- Ganho de tempo nas buscas diárias por imóveis.
- Acesso rápido às oportunidades mais recentes.
- Eliminação de processos manuais repetitivos.

## Metodologia

- **Coleta de dados**: Realiza web scraping diário em sites de imobiliárias e armazena os dados dos imóveis coletados em uma base local.
- **Identificação dos novos anúncios**: Compara os dados do dia atual com os dados dos dias anteriores e identificar os novos anúncios que surgiram nos sites.
- **Notificação para o usuário**: Com a lista de novos anuncios, notifica-se o usuário pelo Telegram, apresentando informações e link para a consulta dos novos imóveis idenfifiados

## Tecnologias Utilizadas
- Python
- BeautifulSoup e Selenium: para scraping
- Pandas: para manipulação de dados
- GCP (Google Coud Storge e Google Cloud Run): para armazenação dos dados e agendamento de scripts
- API do Telegram: para notificação ao usuário

## Exemplo de uso

<br>

<div align="center">
<img src="images/exemplo-pratico.gif" width="250px">
</div>
</br>
