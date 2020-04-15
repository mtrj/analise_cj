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
        self.taxas_1y,self.taxas_2y,self.taxas_3y,self.taxas_4y=[],[],[],[]
        self.taxas_5y,self.taxas_6y,self.taxas_7y,self.taxas_8y=[],[],[],[]
        self.taxas_9y,self.taxas_10y=[],[]
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
        self.dados_gerais = pd.DataFrame(dados, index=self.datas_corretas)        
        self.dados_gerais.to_csv(f'C:\\Users\\{getpass.getuser()}\\Desktop\\{self.export_name}')
        self.inclinacoes()
        self.volatilidades()
        return self.dados_gerais
    
    def inclinacoes(self):
        # CP
        self.dados_gerais['1y5y'] = (self.dados_gerais['5y']-self.dados_gerais['1y'])*10000
        # MP
        self.dados_gerais['2y7y']=(self.dados_gerais['7y']-self.dados_gerais['2y'])*10000
        self.dados_gerais['2y5y']=(self.dados_gerais['5y']-self.dados_gerais['2y'])*10000
        # LP
        self.dados_gerais['5y10y'] = (self.dados_gerais['10y']-self.dados_gerais['5y'])*10000
        self.dados_gerais['2y10y'] = (self.dados_gerais['10y']-self.dados_gerais['2y'])*10000
        
    def volatilidades(self):
        n_periodos = len(self.lista)+1
        for i in range(1,n_periodos):
            self.dados_gerais[f'ret_{i}y'] = self.dados_gerais[f'{i}y']/self.dados_gerais[f'{i}y'].shift(1)-1
        # 3mo vol
        for i in range(1,n_periodos):
            self.dados_gerais[f'vol_ret_3m_{i}y'] = self.dados_gerais[f'ret_{i}y'].rolling(window=66).std()*np.sqrt(252)
        # 6mo vol
        for i in range(1,n_periodos):
            self.dados_gerais[f'vol_ret_6m_{i}y'] = self.dados_gerais[f'ret_{i}y'].rolling(window=132).std()*np.sqrt(252)
        # 9mo vol
        for i in range(1,n_periodos):
            self.dados_gerais[f'vol_ret_9m_{i}y'] = self.dados_gerais[f'ret_{i}y'].rolling(window=198).std()*np.sqrt(252)
        # 1y vol
        for i in range(1,n_periodos):
           self. dados_gerais[f'vol_ret_1y_{i}y'] = self.dados_gerais[f'ret_{i}y'].rolling(window=252).std()*np.sqrt(252)

def adicionar_cinza(figura, df, colunas, inicio, fim, reflect=True):
    if reflect==True:y0=-max(df[colunas].max())*1.1
    else: y0=0
    figura.add_shape(
        # Rectangle reference to the plot
            type="rect",
            xref="paper",
            yref="paper",
            x0=inicio,
            y0=y0,
            x1=fim,
            y1=max(df[colunas].max())*1.1,
            line=dict(
                color="lightgray",
                width=3,
            ),
            fillcolor="lightgray",
        )
    figura.update_shapes(dict(
            xref="x",
            yref="y",
            opacity=0.3,
            line=dict(
                color="lightgray",
                width=3,
                ),
            layer='below'))

def fundo_transparente(figura):
    figura.update_layout({'paper_bgcolor':'rgba(0,0,0,0)',
                          'plot_bgcolor':'rgba(0,0,0,0)'})

def exporta_html(figura, nome): 
    pio.write_html(figura, file=nome+'.html', auto_open=False)

objdg = base()
dg = base().gera_df_geral()

#=========================================================
# Gera as figuras de inclinação
inc5y10y = go.Scatter(x=dg['5y10y'].index,
                      y=dg['5y10y'],name='5Y10Y')
inc1y5y = go.Scatter(x=dg['1y5y'].index,
                     y=dg['1y5y'],name='1Y5Y')
inc2y5y = go.Scatter(x=dg['2y5y'].index,
                     y=dg['2y5y'],name='2Y5Y')
inc2y10y = go.Scatter(x=dg['2y10y'].index,
                      y=dg['2y10y'],name='2Y10Y')
#=========================================================
# Gera a matriz de inclinação
inclinacoes = [inc1y5y, inc2y5y, inc2y10y]
#=========================================================
# Gera as figuras de vol 3m
vol3m_3y = go.Scatter(x=dg['vol_ret_3m_3y'].index,
                      y=dg['vol_ret_3m_3y'], name='3Y')
vol3m_5y = go.Scatter(x=dg['vol_ret_3m_5y'].index,
                      y=dg['vol_ret_3m_5y'], name='5Y')
vol3m_10y = go.Scatter(x=dg['vol_ret_3m_10y'].index,
                       y=dg['vol_ret_3m_10y'], name='10Y')
#=========================================================
# Gera as figuras de vol 6m
vol6m_3y = go.Scatter(x=dg['vol_ret_6m_3y'].index,
                      y=dg['vol_ret_6m_3y'], name='3Y')
vol6m_5y = go.Scatter(x=dg['vol_ret_6m_5y'].index,
                      y=dg['vol_ret_6m_5y'], name='5Y')
vol6m_10y = go.Scatter(x=dg['vol_ret_6m_10y'].index,
                       y=dg['vol_ret_6m_10y'], name='10Y')
#=========================================================
# Gera as figuras de vol 9m
vol9m_3y = go.Scatter(x=dg['vol_ret_9m_3y'].index,
                      y=dg['vol_ret_9m_3y'], name='3Y')
vol9m_5y = go.Scatter(x=dg['vol_ret_9m_5y'].index,
                      y=dg['vol_ret_9m_5y'], name='5Y')
vol9m_10y = go.Scatter(x=dg['vol_ret_9m_10y'].index,
                       y=dg['vol_ret_9m_10y'], name='10Y')
#=========================================================
# Gera as figuras de vol 1y
vol1y_3y = go.Scatter(x=dg['vol_ret_1y_3y'].index,
                      y=dg['vol_ret_1y_3y'], name='3Y')
vol1y_5y = go.Scatter(x=dg['vol_ret_1y_5y'].index,
                      y=dg['vol_ret_1y_5y'], name='5Y')
vol1y_10y = go.Scatter(x=dg['vol_ret_1y_10y'].index,
                       y=dg['vol_ret_1y_10y'], name='10Y')
#=========================================================
# Gera as matrizes de vol
vols3m = [vol3m_3y, vol3m_5y, vol3m_10y]
vols6m = [vol6m_3y, vol6m_5y, vol6m_10y]
vols9m = [vol9m_3y, vol9m_5y, vol9m_10y]
vols1y = [vol1y_3y, vol1y_5y, vol9m_10y]
#=========================================================
# Inicios e fins para cada um dos períodos de crise
datas_crise = [date(2007,1,4), date(2009,10,22)]
datas_dilma = [date(2011,7,11), date(2016,8,30)]
datas_corona = [date(2019,12,30), date(2020,4,3)]
#=========================================================
# Gera figura inclinações
fig = go.Figure(inclinacoes)
fig.update_xaxes(title='Data')
fig.update_yaxes(title='bps')
fig.update_layout(title_text='<b>Inclinações</b>',title_x=0.5)
cols = {'1y5y', '2y5y', '2y10y'}
adicionar_cinza(fig, dg, cols, datas_crise[0], datas_crise[1])
adicionar_cinza(fig, dg, cols, datas_dilma[0], datas_dilma[1])
adicionar_cinza(fig, dg, cols, datas_corona[0], datas_corona[1])
fundo_transparente(fig)
exporta_html(fig, 'Inclinações')
fig = go.Figure()
#=========================================================
fig = make_subplots(rows=2, cols=2,specs=[[{"colspan": 2}, None],[{}, {}]],
                     subplot_titles=("<b>Inclinações</b>",
                                     "<b>Vol Histórica 9M</b>",
                                     "<b>Vol Histórica 1Y</b>"))
fig.add_trace(inc1y5y, row=1, col=1), fig.append_trace(inc2y5y, row=1, col=1)
fig.append_trace(inc2y10y, row=1, col=1)
fig.add_trace(vol9m_3y, row=2, col=1), fig.append_trace(vol9m_5y, row=2, col=1)
fig.append_trace(vol9m_10y, row=2, col=1)
fig.add_trace(vol1y_3y, row=2, col=2), fig.append_trace(vol1y_5y, row=2, col=2)
fig.append_trace(vol1y_10y, row=2, col=2)
fig.update_layout(showlegend=True, title_text="<b>Análise curva de juros</b>")
fundo_transparente(fig)
exporta_html(fig, 'Análise CJ')
fig = go.Figure()
#=========================================================
fig = go.Figure(data=[go.Surface(x=objdg.lista,
                                 y=dg.index,
                                 z=dg.loc[:, '1y':'10y'],
                                 colorscale='Viridis')])
fig.update_layout(title='<b>Superfície da Curva de Juros (2004-2020)</b>',title_x=0.5)
fig.update_layout(scene = dict(xaxis_title='DU',
                                yaxis_title='Datas',
                                zaxis_title='Taxas'))
exporta_html(fig, 'Surface CJ')
fig = go.Figure()
#=========================================================
fig = go.Figure(vols3m)
fig.update_xaxes(title='Data')
fig.update_yaxes(title='Volatilidade')
fig.update_layout(title_text='<b>Volatilidade 3M</b>',title_x=0.5)
cols = {'vol_ret_3m_3y','vol_ret_3m_5y','vol_ret_3m_10y'}
adicionar_cinza(fig, dg, cols, datas_crise[0], datas_crise[1], False)
adicionar_cinza(fig, dg, cols, datas_dilma[0], datas_dilma[1], False)
adicionar_cinza(fig, dg, cols, datas_corona[0], datas_corona[1], False)
fundo_transparente(fig)
exporta_html(fig, 'Volatilidade 3M')
fig = go.Figure()
#=========================================================
fig = go.Figure(vols6m)
fig.update_xaxes(title='Data')
fig.update_yaxes(title='Volatilidade')
fig.update_layout(title_text='<b>Volatilidade 6M</b>',title_x=0.5)
cols = {'vol_ret_6m_3y','vol_ret_6m_5y','vol_ret_6m_10y'}
adicionar_cinza(fig, dg, cols, datas_crise[0], datas_crise[1], False)
adicionar_cinza(fig, dg, cols, datas_dilma[0], datas_dilma[1], False)
adicionar_cinza(fig, dg, cols, datas_corona[0], datas_corona[1], False)
fundo_transparente(fig)
exporta_html(fig, 'Volatilidade 6M')
fig = go.Figure()
#=========================================================
fig = go.Figure(vols9m)
fig.update_xaxes(title='Data')
fig.update_yaxes(title='Volatilidade')
fig.update_layout(title_text='<b>Volatilidade 9M</b>',title_x=0.5)
cols = {'vol_ret_9m_3y','vol_ret_9m_5y','vol_ret_9m_10y'}
adicionar_cinza(fig, dg, cols, datas_crise[0], datas_crise[1], False)
adicionar_cinza(fig, dg, cols, datas_dilma[0], datas_dilma[1], False)
adicionar_cinza(fig, dg, cols, datas_corona[0], datas_corona[1], False)
fundo_transparente(fig)
exporta_html(fig, 'Volatilidade 9M')
fig = go.Figure()
#=========================================================
fig = go.Figure(vols1y)
fig.update_xaxes(title='Data')
fig.update_yaxes(title='Volatilidade')
fig.update_layout(title_text='<b>Volatilidade 1Y</b>',title_x=0.5)
cols = {'vol_ret_1y_3y','vol_ret_1y_5y','vol_ret_1y_10y'}
adicionar_cinza(fig, dg, cols, datas_crise[0], datas_crise[1], False)
adicionar_cinza(fig, dg, cols, datas_dilma[0], datas_dilma[1], False)
adicionar_cinza(fig, dg, cols, datas_corona[0], datas_corona[1], False)
fundo_transparente(fig)
exporta_html(fig, 'Volatilidade 1Y')
fig = go.Figure()
#=========================================================
# Roda as estruturas de volatilidade para 5Y
lista_estrutura = [66, 132, 198, 252]
datas_crises = [date(2007,1,4), date(2011,7,11), date(2019,12,30)]
df_vol = dg[{'vol_ret_3m_3y','vol_ret_6m_5y','vol_ret_3m_10y','vol_ret_6m_3y','vol_ret_6m_5y','vol_ret_6m_10y', 'vol_ret_9m_3y','vol_ret_9m_5y','vol_ret_9m_10y', 'vol_ret_1y_3y','vol_ret_1y_5y','vol_ret_1y_10y'}]
df_vol_f = df_vol.loc[datas_crises]
lista3m = list(df_vol_f['vol_ret_3m_3y'])
lista6m = list(df_vol_f['vol_ret_6m_3y'])
lista9m = list(df_vol_f['vol_ret_9m_3y'])
lista1y = list(df_vol_f['vol_ret_1y_3y'])
estrutura = []
for i in range(len(lista3m)):
    estrutura.append([lista3m[i], lista6m[i], lista9m[i], lista1y[i]])
graf1 = go.Scatter(x=lista_estrutura,y=estrutura[0],name='Crise de 2008')
graf2 = go.Scatter(x=lista_estrutura,y=estrutura[1],name='Governo Dilma')
graf3 = go.Scatter(x=lista_estrutura,y=estrutura[2],name='Coronavírus')
grafs = [graf1, graf2, graf3]
fig = go.Figure(grafs)
fig.update_layout(scene = dict(xaxis_title='Data',
                                yaxis_title='Volatilidade'))
fig.update_layout(title_text='<b>Estrutura a termo de volatilidade PRÉ</b>',title_x=0.5)
fundo_transparente(fig)
exporta_html(fig, 'Estrutura PRE')
fig = go.Figure()
#=========================================================
df_vol = dg[{'vol_ret_3m_3y','vol_ret_3m_5y','vol_ret_3m_10y','vol_ret_6m_3y','vol_ret_6m_5y','vol_ret_6m_10y', 'vol_ret_9m_3y','vol_ret_9m_5y','vol_ret_9m_10y', 'vol_ret_1y_3y','vol_ret_1y_5y','vol_ret_1y_10y'}]
datas_crises = [date(2009,10,22), date(2016,8,30), date(2020,4,3)]
df_vol_f = df_vol.loc[datas_crises]
lista3m = list(df_vol_f['vol_ret_3m_5y'])
lista6m = list(df_vol_f['vol_ret_6m_5y'])
lista9m = list(df_vol_f['vol_ret_9m_5y'])
lista1y = list(df_vol_f['vol_ret_1y_5y'])
estrutura = []
for i in range(len(lista3m)):
    estrutura.append([lista3m[i], lista6m[i], lista9m[i], lista1y[i]])
graf1 = go.Scatter(x=lista_estrutura,y=estrutura[0],name='Crise de 2008')
graf2 = go.Scatter(x=lista_estrutura,y=estrutura[1],name='Governo Dilma')
graf3 = go.Scatter(x=lista_estrutura,y=estrutura[2],name='Coronavírus')
grafs = [graf1, graf2, graf3]
fig = go.Figure(grafs)
fig.update_layout(scene = dict(xaxis_title='Data',
                                yaxis_title='Volatilidade'))
fig.update_layout(title_text='<b>Estrutura a termo de volatilidade PÓS</b>',title_x=0.5)
fundo_transparente(fig)
exporta_html(fig, 'Estrutura POS')
fig = go.Figure()
df_vol_f = pd.DataFrame()
elapsed_time = time.time() - start_time
print('Tempo de execução: ' + str(round(elapsed_time,4)) + 's')
