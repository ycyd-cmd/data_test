import numpy as np
import pandas as pd

def ASI_judge(df):
    if (df['AA']>df['BB']) & (df['AA']>df['CC']):
        return df['AA'] + df['BB']/2 + df['DD']/4
    elif (df['BB']>df['CC']) & (df['BB']>df['AA']):
        return df['BB'] + df['AA']/2 + df['DD']/4
    else:
        return df['CC'] + df['DD']/4


def get_alpha_ma(df):
    stock = alpha_ma(df)
    df['ma', 'AMV_3'] = stock.AMV_3()
    df['ma', 'AMV_5'] = stock.AMV_5()
    df['ma', 'AMV_10'] = stock.AMV_10()
    df['ma', 'AMV_20'] = stock.AMV_20()
    df['ma', 'AMV_30'] = stock.AMV_30()
    df['ma', 'AMV_55'] = stock.AMV_55()
    df['ma', 'AMV_60'] = stock.AMV_60()
    df['ma', 'AMV_120'] = stock.AMV_120()
    # df['ma', 'AMV_250'] = stock.AMV_250()
    df['ma', 'MA_3'] = stock.MA_3()
    df['ma', 'MA_5'] = stock.MA_5()
    df['ma', 'MA_10'] = stock.MA_10()
    df['ma', 'MA_20'] = stock.MA_20()
    df['ma', 'MA_30'] = stock.MA_30()
    df['ma', 'MA_55'] = stock.MA_55()
    df['ma', 'MA_60'] = stock.MA_60()
    df['ma', 'MA_120'] = stock.MA_120()
    # df['ma', 'MA_250'] = stock.MA_250()
    df['ma', 'EMA_3'] = stock.EMA_3()
    df['ma', 'EMA_5'] = stock.EMA_5()
    df['ma', 'EMA_10'] = stock.EMA_10()
    df['ma', 'EMA_20'] = stock.EMA_20()
    df['ma', 'EMA_30'] = stock.EMA_30()
    df['ma', 'EMA_55'] = stock.EMA_55()
    df['ma', 'EMA_60'] = stock.EMA_60()
    df['ma', 'EMA_120'] = stock.EMA_120()
    # df['ma', 'EMA_250'] = stock.EMA_250()
    df['ma', 'HMA_3'] = stock.HMA_3()
    df['ma', 'HMA_5'] = stock.HMA_5()
    df['ma', 'HMA_10'] = stock.HMA_10()
    df['ma', 'HMA_20'] = stock.HMA_20()
    df['ma', 'HMA_30'] = stock.HMA_30()
    df['ma', 'HMA_55'] = stock.HMA_55()
    df['ma', 'HMA_60'] = stock.HMA_60()
    df['ma', 'HMA_120'] = stock.HMA_120()
    # df['ma', 'HMA_250'] = stock.HMA_250()
    df['ma', 'LMA_3'] = stock.LMA_3()
    df['ma', 'LMA_5'] = stock.LMA_5()
    df['ma', 'LMA_10'] = stock.LMA_10()
    df['ma', 'LMA_20'] = stock.LMA_20()
    df['ma', 'LMA_30'] = stock.LMA_30()
    df['ma', 'LMA_55'] = stock.LMA_55()
    df['ma', 'LMA_60'] = stock.LMA_60()
    df['ma', 'LMA_120'] = stock.LMA_120()
    # df['ma', 'LMA_250'] = stock.LMA_250()
    df['ma', 'VMA_3'] = stock.VMA_3()
    df['ma', 'VMA_5'] = stock.VMA_5()
    df['ma', 'VMA_10'] = stock.VMA_10()
    df['ma', 'VMA_20'] = stock.VMA_20()  # 注意，这里原本是 VNA_20，确保没有拼写错误
    df['ma', 'VMA_30'] = stock.VMA_30()
    df['ma', 'VMA_55'] = stock.VMA_55()
    df['ma', 'VMA_60'] = stock.VMA_60()
    df['ma', 'VMA_120'] = stock.VMA_120()
    # df['ma', 'VMA_250'] = stock.VMA_250()
    df['ma', 'BBI'] = stock.BBI()
    df['ma', 'BBIBOLL_UP'] = stock.BBIBOLL_UP()
    df['ma', 'BBIBOLL_DOWN'] = stock.BBIBOLL_DOWN()
    df['ma', 'DPO'] = stock.DPO()
    df['ma', 'MADPO'] = stock.MADPO()
    df['ma', 'BOLL'] = stock.BOLL()
    df['ma', 'BOLL_UP'] = stock.BOLL_UP()
    df['ma', 'BOLL_DOWN'] = stock.BOLL_DOWN()
    df['ma', 'MACD_IFF'] = stock.MACD_IFF()
    df['ma', 'MACD_DEA'] = stock.MACD_DEA()
    df['ma', 'MACD_HIST'] = stock.MACD_HIST()
    df['ma', 'TRIX'] = stock.TRIX()
    df['ma', 'MATRIX'] = stock.MATRIX()
    

class alpha_ma(object):
    def __init__(self,df_data):
        
        self.open = df_data['S_DQ_OPEN'] 
        self.high = df_data['S_DQ_HIGH'] 
        self.low = df_data['S_DQ_LOW']   
        self.close = df_data['S_DQ_CLOSE'] 
        self.volume = df_data['S_DQ_VOLUME']*100
        self.length = len(df_data) 

    def AMV(self,N):
        temp = pd.DataFrame(self.volume)
        temp.columns = ['volume']
        temp['AMOV'] = self.volume*(self.open+self.close)/2
        temp_sum = temp.rolling(window = N).sum()
        return temp_sum['AMOV']/temp_sum['volume']
    
    def AMV_3(self):
        return self.AMV(N = 3)
    
    def AMV_5(self):
        return self.AMV(N = 5)
    
    def AMV_10(self):
        return self.AMV(N = 10)
    
    def AMV_20(self):
        return self.AMV(N = 20)
    
    def AMV_30(self):
        return self.AMV(N = 30)
    
    def AMV_55(self):
        return self.AMV(N = 55)
    
    def AMV_60(self):
        return self.AMV(N = 60)
    
    def AMV_120(self):
        return self.AMV(N = 120)
    
    def AMV_250(self):
        return self.AMV(N = 250)

    def MA(self,N):
        temp = pd.DataFrame(self.close)
        return temp.rolling(window = N).mean()
    
    def MA_3(self):
        return self.MA(N = 3)
    
    def MA_5(self):
        return self.MA(N = 5)
    
    def MA_10(self):
        return self.MA(N = 10)
    
    def MA_20(self):
        return self.MA(N = 20)
    
    def MA_30(self):
        return self.MA(N = 30)
    
    def MA_55(self):
        return self.MA(N = 55)
    
    def MA_60(self):
        return self.MA(N = 60)
    
    def MA_120(self):
        return self.MA(N = 120)
    
    def MA_250(self):
        return self.MA(N = 250)
    
    def EMA(self,N):
        temp = pd.DataFrame(self.close)
        return temp.ewm(span = N,adjust = False).mean()
    
    def EMA_3(self):
        return self.EMA(N = 3)
    
    def EMA_5(self):
        return self.EMA(N = 5)
    
    def EMA_10(self):
        return self.EMA(N = 10)
    
    def EMA_20(self):
        return self.EMA(N = 20)
    
    def EMA_30(self):
        return self.EMA(N = 30)
    
    def EMA_55(self):
        return self.EMA(N = 55)
    
    def EMA_60(self):
        return self.EMA(N = 60)
    
    def EMA_120(self):
        return self.EMA(N = 120)
    
    def EMA_250(self):
        return self.EMA(N = 250)
    
    def HMA(self,N):
        temp = pd.DataFrame(self.high)
        return temp.rolling(window = N).mean()
    
    def HMA_3(self):
        return self.HMA(N = 3)
    
    def HMA_5(self):
        return self.HMA(N = 5)
    
    def HMA_10(self):
        return self.HMA(N = 10)
    
    def HMA_20(self):
        return self.HMA(N = 20)
    
    def HMA_30(self):
        return self.HMA(N = 30)
    
    def HMA_55(self):
        return self.HMA(N = 55)
    
    def HMA_60(self):
        return self.HMA(N = 60)
    
    def HMA_120(self):
        return self.HMA(N = 120)
    
    def HMA_250(self):
        return self.HMA(N = 250)
    
    def LMA(self,N):
        temp = pd.DataFrame(self.low)
        return temp.rolling(window = N).mean()
    
    def LMA_3(self):
        return self.LMA(N = 3)
    
    def LMA_5(self):
        return self.LMA(N = 5)
    
    def LMA_10(self):
        return self.LMA(N = 10)
    
    def LMA_20(self):
        return self.LMA(N = 20)
    
    def LMA_30(self):
        return self.LMA(N = 30)
    
    def LMA_55(self):
        return self.LMA(N = 55)
    
    def LMA_60(self):
        return self.LMA(N = 60)
    
    def LMA_120(self):
        return self.LMA(N = 120)
    
    def LMA_250(self):
        return self.LMA(N = 250)
    
    def VMA(self,N):
        temp = pd.DataFrame((self.high+self.open+self.low+self.close)/4)
        return temp.rolling(window = N).mean()
    
    def VMA_3(self):
        return self.VMA(N = 3)
    
    def VMA_5(self):
        return self.VMA(N = 5)
    
    def VMA_10(self):
        return self.VMA(N = 10)
    
    def VMA_20(self):
        return self.VMA(N = 20)
    
    def VMA_30(self):
        return self.VMA(N = 30)
    
    def VMA_55(self):
        return self.VMA(N = 55)
    
    def VMA_60(self):
        return self.VMA(N = 60)
    
    def VMA_120(self):
        return self.VMA(N = 120)
    
    def VMA_250(self):
        return self.VMA(N = 250)
    
    def BBI(self,M1 = 3,M2 = 6,M3 = 12,M4 = 24):
        return (self.MA(N = M1)+self.MA(N = M2)+self.MA(N= M3)+self.MA(N = M4))/4
    
    def BBIBOLL_UP(self,M = 6,N = 11):
        return self.BBI()+M*self.BBI().rolling(window = N).std()
    
    def BBIBOLL_DOWN(self,M = 6,N = 11):
        return self.BBI()-M*self.BBI().rolling(window = N).std()
    
    def DPO(self,M1 = 20,M2 = 10):
        temp1 = pd.DataFrame(self.close)
        temp2 = self.MA(N = M1)
        return temp1-temp2.shift(M2)
    
    def MADPO(self,M3 = 6):
        temp = pd.DataFrame(self.DPO())
        return temp.rolling(window = M3).mean()
    
    def BOLL(self,m = 20):
        return self.MA(N = m)

    def BOLL_UP(self,p = 2, N = 20):
        temp = pd.DataFrame(self.close)
        return self.BOLL()+p*temp.rolling(window = N).std()
    
    def BOLL_DOWN(self,p = 2,N = 20):
        temp = pd.DataFrame(self.close)
        return self.BOLL()-p*temp.rolling(window = N).std()
    
    def MACD_IFF(self,SHORT = 12,LONG = 26):
        DIFF = self.EMA(N = SHORT)-self.EMA(N = LONG)
        return DIFF
    
    def MACD_DEA(self,M = 9):
        return self.MACD_IFF().ewm(span = M,adjust = False).mean()
    
    def MACD_HIST(self):
        DIFF = self.MACD_IFF()
        DEA = self.MACD_DEA()
        return (DIFF-DEA)*2
    
    def TRIX(self,M1 = 12):
        TR = (self.EMA(N = M1).ewm(span = M1,adjust=False).mean()).ewm(span = M1,
            adjust = False).mean()
        TRIX = (TR-TR.shift(1))/TR.shift(1)*100
        return TRIX
    
    def MATRIX(self,M2 = 20):
        return (self.TRIX()).rolling(window = M2).mean()
        
    def ASI(self,M1= 26):
        LC = self.close.shift(1)
        AA = abs(self.high-LC)
        BB = abs(self.low-LC)
        CC = abs(self.high-self.low.shift(1))
        DD = abs(LC-self.open.shift(1))
        data = pd.DataFrame({'LC':LC,'AA':AA,'BB':BB,'CC':CC,'DD':DD})
        data['R'] = data.apply(lambda p: ASI_judge(p),axis = 1)
        data['AA_BB'] = data.apply(lambda p: max(p['AA'],p['BB']),axis = 1)
        X = (self.close - LC + (self.close-self.open)/2 + LC - self.open.shift(1))
        SI = X*16/data['R']*data['AA_BB']
        ASI = SI.rolling(M1).sum()
        return ASI
    
    def ASIT(self,M2=10):
        return self.ASI().rolling(window = M2).mean()
    
