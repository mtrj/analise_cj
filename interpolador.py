# -*- coding: utf-8 -*-

class interpolador:
    def __init__(self, p, df, column, dc=252):
        self.df = df
        self.column = column
        self.p = p
        self.dc = dc
        self.x_axis = list(df.index)
        self.y_axis = list(df[column])
        self._config(self.p)
    
    def _config(self, p):
        """
        Função que configura o objeto para definir o ponto anterior e posterior para interpolar
        """
        self.x1 = min(self.df.index, key=lambda a:abs(a-p))
        if p-self.x1>0: self.x2 = self.x_axis[self.x_axis.index(self.x1)+1]
        elif p-self.x1==0: self.x2 = self.x1
        elif p-self.x1<0:
            self.x2 = self.x1
            self.x1 = self.x_axis[self.x_axis.index(self.x1)-1]
        self.y1 = self.y_axis[self.x_axis.index(self.x1)]
        self.y2 = self.y_axis[self.x_axis.index(self.x2)]
    
    def exp(self):
        """
        Função de interpolação exponencial flat forward
        """
        x1,x2,y1,y2,p,dc = self.x1, self.x2, self.y1, self.y2, self.p, self.dc
        if x1-x2!=0:
            return ((((1+y1)**(x1/dc))*(((1+y2)**(x2/dc))/((1+y1)**(x1/dc)))**((p-x1)/(x2-x1)))**(dc/p))-1
        else:
            return y1
        
    def lin(self):
        """
        Função de interpolação linear nas taxas
        """
        x1,x2,y1,y2,p = self.x1, self.x2, self.y1, self.y2, self.p
        if x1-x2!=0:
            return y1+(p-x1)*(y2-y1)/(x2-x1)
        else:
            return y1
    
    def multi_intp(self, arr=[], t='exp'):
        interpolados=[]
        obj_intp = interpolador(arr[0],self.df,self.column,dc=self.dc)
        for x in range(len(arr)):
            obj_intp.p = arr[x]
            obj_intp._config(arr[x])
            if t.lower()=='exp': interpolados.append(obj_intp.exp())
            elif t.lower()=='lin': interpolados.append(obj_intp.lin())
        return interpolados
