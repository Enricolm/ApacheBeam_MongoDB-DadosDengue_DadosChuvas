import re
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions
from pymongo import MongoClient

# Conectar ao MongoDB
client = MongoClient('localhost', 27017)
db = client.dados_chuvas
collection = db.DadosChuva

# Consultar os dados
dados = collection.find()

pipeline_options = PipelineOptions(argv=None)
pipeline = beam.Pipeline(options=pipeline_options)

def extract_from_mongodb(nome_db,nome_collection):

    client = MongoClient('localhost', 27017)

    db = client[nome_db]
    collection = db[nome_collection]

    dados = collection.find()

    return list(dados)


dados_chuva = pipeline | "Lendo os dados do MongoDB Chuvas" >> beam.Create(extract_from_mongodb('dados_chuvas',"DadosChuva"))
dados_dengue = pipeline | "Lendo os dados do MongoDB Dengue" >> beam.Create(extract_from_mongodb('dados_dengue','DadosDengue'))


pipeline_options = PipelineOptions (argv = None)
pipeline = beam.Pipeline(options=pipeline_options)


nome_colunas = [
    'id','data_iniSE' ,'casos' ,'ibge_code' ,'cidade' ,'uf' ,'cep' ,'latitude' ,'longitude'
]


class ContadorElementos(beam.DoFn):
    def __init__(self):
        self.contador = 0
    
    def process(self, elemento):
        self.contador += 1
        if self.contador % 100000 == 0:
            print(f"Elemento {self.contador}: {elemento}")
            return [elemento]
    

def transforma_dados (elemento, identificador = "|"):
    "Transformando os dados do texto em uma lista, para que possa ser transoformado em dataset"
    return elemento.split(identificador)


def dados_Dict(elemento, colunas):
    "Recebe uma lista e transforma em um Dict"
    return dict(zip(colunas,elemento))


def transforma_Data(elemento):
    #Criando um novo campo onde vai ter o Ano-mes para realizar divisoes
    valor = elemento['data_iniSE'].split('-')[:2]
    elemento['Ano-mes'] = "-".join(valor)
    return elemento

def Unindo_por_estado(elemento):
    chaveUF = elemento['uf']
    return (chaveUF,elemento)

def Unindo_por_data(elemento):
    uf,valor = elemento
    for i in valor:
        if bool(re.search(r'\d', i['casos'])) == True:
            yield (f"{uf}-{i['Ano-mes']}",float(i['casos']))
        else:
            yield (f"{uf}-{i['Ano-mes']}", 0.0)

dados_dengue = (
    dados_dengue
    | "Tratando os dados" >> beam.Map(transforma_dados)
    #| "Contando os dados" >> beam.ParDo(ContadorElementos())
    | "Transformando os dados em Dict" >> beam.Map(dados_Dict,nome_colunas)
    | "Criando Data-mes" >> beam.Map(transforma_Data)
    | "Classificando pelo uf" >> beam.Map(Unindo_por_estado)
    |  "Agrupando pelo uf"  >> beam.GroupByKey()
    | "Agrupando pelo Ano-mes" >> beam.FlatMap(Unindo_por_data)
    | "Somando os casos" >> beam.CombinePerKey(sum)
    #| "Mostrando elementos" >> beam.Map(print)
)


def arrumando_valores (elemento):
    data = "-".join(elemento[0].split('-')[:2])
    valor_final = f"{elemento[2]}-{data}"
    if float(elemento[1]) >= 0.0:
        return (valor_final,float(elemento[1]))
    else: return (valor_final,0.0)



def arredonda_valores(elemento):
    return (elemento[0], round(elemento[1],2))

dados_chuvas =(
    dados_chuva
    | "Separando os elementos" >> beam.Map(transforma_dados, identificador = ',')
    | "Separando valores" >> beam.Map(arrumando_valores)
    | "Juntando os valores da mesma data" >> beam.CombinePerKey(sum)
    #| "Contando os dados" >> beam.ParDo(process)
    | "Arredondando os valores" >> beam.Map(arredonda_valores)
    # | "Mostrando os dados" >> beam.Map(print)

)


def criando_filtro(elemento):
    chave, dados = elemento
    if all([
        dados['dados_chuvas'],
        dados['dados_dengue']
        ]):
        return True
    return False



Dados_dengue_chuvas = (
    # (dados_chuvas, dados_dengue)
    # | "Juntando as duas Pcollections" >> beam.Flatten()
    # | "Juntando pelo nome" >> beam.GroupByKey()
    ({"dados_chuvas": dados_chuvas, "dados_dengue": dados_dengue})
    | "Juntar as pcollection" >> beam.CoGroupByKey()
    | "Criando um filtro para dados nulos" >> beam.Filter(criando_filtro)
    | "Mostrando os dados" >> beam.Map(print)
)





pipeline.run()