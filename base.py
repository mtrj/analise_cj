# -*- coding: utf-8 -*-
import getpass
from pricer import bmf
from datetime import date
import pandas as pd
from interpolador import interpolador as intp
import time
from tqdm import trange
import os.path
import numpy as np
import plotly.graph_objects as go
import plotly.io as pio
from plotly.subplots import make_subplots

start_time = time.time()
path = f'C:\\Users\\{getpass.getuser()}\\Desktop\\base_curvas\\'
leitura = 'datas_curva_pre.txt'
leitura_erros = 'datas_erros.txt'

class base():
    def __init__(self, arquivo_datas=leitura,
                 arquivo_erros=leitura_erros,
                 path=path,
                 export_name='consolidado_1y_to_10y.csv'):
        
        self.arquivo_datas = arquivo_datas
        self.arquivo_erros = arquivo_erros
        self.path = path
        self.lista = [252*i for i in range(1,11)]
        self.lista_nomes_colunas = [str(i)+'y' for i in range(1,11)]
        self.export_name = export_name
        self.erros = []
        self.__inicializar_vetores()
        self.__datas()
        self.__erros()
        
    def __datas(self):
        with open(self.arquivo_datas, 'r') as f:
            datas = [line.replace('\n','') for line in f.readlines()]
        self.datas = [date(int(datas[i].split('/')[2]),int(datas[i].split('/')[1])
              ,int(datas[i].split('/')[0])) for i in range(len(datas))]

    def __erros(self):
        with open(self.arquivo_erros) as p:
            datas_erros = [line.replace('\n','') for line in p.readlines()]
        self.datas_erros = [date(int(datas_erros[i].split('/')[2]),int(datas_erros[i].split('/')[1])
              ,int(datas_erros[i].split('/')[0])) for i in range(len(datas_erros))]
        
    def __inicializar_vetores(self):
        self.datas_corretas=[]
        self.erros=[]
        self.taxas_1y=[]
        self.taxas_2y=[]
        self.taxas_3y=[]
        self.taxas_4y=[]
        self.taxas_5y=[]
        self.taxas_6y=[]
        self.taxas_7y=[]
        self.taxas_8y=[]
        self.taxas_9y=[]
        self.taxas_10y=[]
        self.taxas_geral = [self.taxas_1y, self.taxas_2y, self.taxas_3y,
                            self.taxas_4y, self.taxas_5y, self.taxas_6y,
                            self.taxas_7y, self.taxas_8y, self.taxas_9y,
                            self.taxas_10y]
        
    def __inserir_vetores(self, arr=[]):
        for i in range(len(arr)):
            self.taxas_geral[i].append(arr[i])
        
    def __multi_intp(self, arr=[], df=pd.DataFrame()):
        interpolados=[]
        obj_intp = intp(252,df,'taxas252')
        for x in range(len(arr)):
            obj_intp.p = arr[x]
            obj_intp._config(arr[x])
            interpolados.append(obj_intp.exp())
        return interpolados
            
    def gera_df_geral(self):
        for i in trange(len(self.datas)):
            nome_arquivo = f'{self.path}PRE-DI {self.datas[i]}.csv'
            if self.datas[i] not in self.datas_erros:
                if os.path.isfile(nome_arquivo)==True:
                    df = pd.read_csv(nome_arquivo, sep=',', index_col=0)
                    intps = self.__multi_intp(self.lista, df)
                    self.__inserir_vetores(intps)
                    self.datas_corretas.append(self.datas[i])
                else:
                    try:
                        df = bmf(val_date=self.datas[i])._baixa_pre()
                        if df.empty == False:
                            df.to_csv(nome_arquivo)
                            intps = self.__multi_intp(self.lista, df)
                            self.__inserir_vetores(intps)
                            self.datas_corretas.append(self.datas[i])
                        elif df.empty == True:
                            self.erros.append(self.datas[i])
                    except:
                        self.erros.append(self.datas[i])
            else:
                self.erros.append(self.datas[i])
                
        dados = {self.lista_nomes_colunas[i]:self.taxas_geral[i] 
                 for i in range(len(self.lista_nomes_colunas))}
        dados_gerais = pd.DataFrame(dados, index=self.datas_corretas)        
        dados_gerais.to_csv(f'C:\\Users\\{getpass.getuser()}\\Desktop\\{self.export_name}')
        return dados_gerais
