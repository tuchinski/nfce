from bs4 import BeautifulSoup
import json
import requests
from pymongo import MongoClient

# Recebe como parâmetro a página e retorna os itens comprados na nota
def obtem_dados_produtos(soup):
    tabela_itens = soup.findAll('tr')
    produtos = []
    for item in tabela_itens:
        produto = {}
        nomeProduto = item.find(class_='txtTit2').string

        # Retira somente o número do código
        codigoProduto = item.find(class_='RCod').string.split(':')[1]
        codigoProduto = codigoProduto[1:-1]
         
        # Pega a str obtida ex:Qtde.:0,072, divide ela no caractere ':' e pega somente o valor numérico
        qtdeProduto = item.find(class_='Rqtd').get_text()
        qtdeProduto = qtdeProduto.split(':')[1].strip()

        unidadeProduto = item.find(class_='RUN').get_text()
        unidadeProduto = unidadeProduto.split(':')[1].strip()

        valorUnitarioProduto = item.find(class_='RvlUnit').get_text()
        valorUnitarioProduto = valorUnitarioProduto.split(':')[1].strip()

        valorTotalProduto = item.find(class_='valor').string
        
        produto['nome'] = nomeProduto
        produto['codigo'] = codigoProduto
        produto['qtde'] = qtdeProduto
        produto['un'] = unidadeProduto
        produto['valorUN'] = valorUnitarioProduto
        produto['ValorTotal'] = valorTotalProduto

        produtos.append(produto)
        
    return produtos


def remove_escape_chars(string,removeN = True, removeT=True):
    if removeN and removeT:
        return string.replace('\t','').replace('\n','')
    elif removeN == True and removeT == False:
        return string.replace('\n','')
    elif removeN == False and removeT == True:
        return string.replace('\t','')
    else:
        return string

# Recebe como parâmetro a página e retorna as informações da nota
def extrair_infos_nota(soup):

    html_dados_nota = soup.find('li')

    # Retira os '\t' mas deixa os '\n', que vão servir para usar com o split, para pegar cada dado
    texto_infos_nota = remove_escape_chars(html_dados_nota.get_text(), removeN=False)

    # Retira os espaços em branco e divide a string usando o '\n
    infos_notas_split = texto_infos_nota.strip().split('\n')

    numero_nota = infos_notas_split[2].split(' ')[1]
    serie_nota = infos_notas_split[3].strip().split(' ')[1]
    data_emissao_nota = infos_notas_split[4].strip().split(' ')[1]
    hora_emissao_nota = infos_notas_split[4].strip().split(' ')[2]
    hora_emissao_nota = infos_notas_split[4].strip().split(' ')[2]
    num_protocolo_autoriz_nota = infos_notas_split[7].strip().split(':',1)[1].strip().split(' ')[0]
    data_protocolo_autoriz_nota = infos_notas_split[7].strip().split(':',1)[1].strip().split(' ')[-2]
    hora_protocolo_autoriz_nota = infos_notas_split[7].strip().split(':',1)[1].strip().split(' ')[-1]

    # print('numero_nota',numero_nota)
    # print('serie_nota',serie_nota)
    # print('data_emissao_nota',data_emissao_nota)
    # print('hora_emissao_nota',hora_emissao_nota)
    # print('num_protocolo_autoriz_nota',num_protocolo_autoriz_nota)
    # print('data_protocolo_autoriz_nota',data_protocolo_autoriz_nota)
    # print('hora_protocolo_autoriz_nota',hora_protocolo_autoriz_nota)

    dados_nota = {
        'numero_nota': numero_nota,
        'serie_nota': serie_nota,
        'data_emissao_nota': data_emissao_nota,
        'hora_emissao_nota': hora_emissao_nota,
        'num_protocolo_autoriz_nota': num_protocolo_autoriz_nota,
        'data_protocolo_autoriz_nota': data_protocolo_autoriz_nota,
        'hora_protocolo_autoriz_nota': hora_protocolo_autoriz_nota,
    }
    return dados_nota


def obtem_infos_empresa(soup):
    dados_local_compra = soup.find(class_='txtCenter').findAll('div')
    nome_empresa = dados_local_compra[0].string
    cnpj_empresa = dados_local_compra[1].string.split(':')[1].strip()
    endereco_empresa = remove_escape_chars(dados_local_compra[2].string)
    infos_adicionais = soup.findAll('li')[-1].string

    infos_empresa = {
        'nome_empresa': nome_empresa,
        'cnpj_empresa': cnpj_empresa,
        'endereco_empresa': endereco_empresa,
        'infos_adicionais': infos_adicionais,

    }
    return infos_empresa

# Recebe como parâmetro a página e retorna os dados do consumidor
def extrai_dados_consumidor(soup):
    html_dados_consumidor = soup.find(id='infos')
    html_dados_consumidor = html_dados_consumidor.findAll('div')[2].findAll('li')
    dados_consumidor = {}
    for i in range(0,len(html_dados_consumidor)):
        dado_atual = html_dados_consumidor[i].text.strip()
        try:
            dado_atual = dado_atual.split(':')
            dados_consumidor[dado_atual[0].strip().lower()] = dado_atual[1].strip()
        except:
            dados_consumidor['nome'] = dado_atual[0]

    return dados_consumidor


def conecta_mongo(link):
    url = open(link, 'r').read()
    cliente = MongoClient(url)
    return cliente


def envia_nota_db(cliente):
    banco = cliente['nfce']

    notas = banco['notas']

    notas.insert_one(nota)

if __name__ == "__main__":
    
    # url = 'http://www.fazenda.pr.gov.br/nfce/qrcode/?p=41200476189406004113651080000588291061915005|2|1|1|FB408E05CECD0D6A9BA23214DD7F588C900BD3D3'
    url = 'http://www.fazenda.pr.gov.br/nfce/qrcode/?p=41200776189406004113651170000114001111234247|2|1|1|1448DC4A54266EBBE74D638B65E7314A225E721F'
    # url = 'http://www.fazenda.pr.gov.br/nfce/qrcode/?p=41200675878686000117651280003023911003023920|2|1|1|6D6D0EEF927C1B39BA93B42BBBB56CB68A169517'
    page = requests.get(url)

    # Pega o html da página e joga pro BS
    html_doc = page.content
    soup = BeautifulSoup(html_doc, 'html.parser')

    # Extrai os dados do consumidor, passando como parâmetro a página
    json_dados_consumidor = extrai_dados_consumidor(soup)

    # Obtem a lista com os dados da nota
    json_dados_nota = extrair_infos_nota(soup)

    # obtem os itens comprados
    json_produtos = obtem_dados_produtos(soup)

    # Obtem os dados do local da compra(Nome, CNPJ e endereco)
    # dados_local_compra = 
    json_dados_empresa = obtem_infos_empresa(soup)

    # Dados extra da nota
    qtde_itens = soup.find(class_='totalNumb').string
    valor_final = soup.find(class_='totalNumb txtMax').string
    valor_tributos = soup.find(class_='totalNumb txtObs').string
    
    # Add dados extra na nota
    json_dados_nota['qtdeItens'] = qtde_itens
    json_dados_nota['valor_final'] = valor_final
    json_dados_nota['valor_tributos'] = valor_tributos

    # Criando a nota
    nota = {
            'dados_nota' : json_dados_nota,
            'produtos' : json_produtos,
            'dados_empresa': json_dados_empresa,
            'dados_consumidor': json_dados_consumidor,
    }
    
    print(json.dumps(nota))
    
    cliente = conecta_mongo('dbaddress')

    envia_nota_db(cliente)
