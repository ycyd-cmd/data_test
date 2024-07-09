import numpy as np
import pandas as pd
import copy

def sgn(x):
    if x>0:
        return 1
    elif x<0:
        return -1
    else:
        return 0
    
def get_alpha_energy(df):
    stock = alpha_energy(df)
    df['AR'] = stock.AR()
    df['BR'] = stock.BR()
    df['VR'] = stock.VR()
    df['MAVR'] = stock.MAVR()
    df['CR'] = stock.CR()
    df['MACR1'] = stock.MACR1()
    df['MACR2'] = stock.MACR2()
    df['MACR3'] = stock.MACR3()
    df['MACR4'] = stock.MACR4()
    df['MASS'] = stock.MASS()
    df['MAMASS'] = stock.MAMASS()
    df['SY'] = stock.SY()
    df['PCNT'] = stock.PCNT()
    df['AMP1'] = stock.AMP1()
    df['AMP3'] = stock.AMP3()
    df['AMP5'] = stock.AMP5()
    df['AMP10'] = stock.AMP10()
    df['AMP20'] = stock.AMP20()
    df['AMP60'] = stock.AMP60()
    df['WMA3'] = stock.WMA3()
    df['WMA5'] = stock.WMA5()
    df['WMA10'] = stock.WMA10()
    df['WMA20'] = stock.WMA20()
    df['WMA60'] = stock.WMA60()
    df['WMA120'] = stock.WMA120()
    #df['WMA250'] = stock.WMA250()
    df['VOLT20'] = stock.VOLT20()
    df['VOLT60'] = stock.VOLT60()
    df['MDD20'] = stock.MDD20()
    df['MDD60'] = stock.MDD60()
    df['AROON_UP'] = stock.AROON_UP()
    df['AROON_DOWN'] = stock.AROON_DOWN()
    df['QTYR_5_20'] = stock.QTYR_5_20()
    df['OBV'] = stock.OBV()



class alpha_energy(object):
    def __init__(self,df_data):

        self.open = df_data['S_DQ_OPEN'] 
        self.high = df_data['S_DQ_HIGH'] 
        self.low = df_data['S_DQ_LOW']   
        self.close = df_data['S_DQ_CLOSE'] 
        self.volume = df_data['S_DQ_VOLUME']*100
        self.length = len(df_data) 
    
    def AR(self,M1 = 26):
        temp1 = self.high-self.open
        temp2 = self.open-self.low
        return temp1.rolling(window = M1).sum()/temp2.rolling(window = M1).sum()*100

    def BR(self,M1 = 26):
        c = len(self.high)
        temp1 = (self.high-self.close.shift(1)).apply(lambda p:max(p,0))
        temp2 = (self.close.shift(1)-self.low).apply(lambda p:max(p,0))
        return temp1.rolling(window = M1).sum()/temp2.rolling(window = M1).sum()*100
    
    def VR(self,M1 = 26):
        LC = self.close.shift(1)
        temp1 = self.volume.copy()
        temp1.loc[self.close<=LC] = 0
        temp2 = self.volume.copy()
        temp2.loc[self.close>LC] = 0
        return temp1.rolling(window = M1).sum()/temp2.rolling(window = M1).sum()*100
    
    def MAVR(self,M = 6):
        return self.VR().rolling(window = M).mean()
    
    def CR(self,N = 26):
        MID = (self.high+self.low).shift(1)/2
        temp1 = (self.high-MID).apply(lambda p:max(p,0))
        temp2 = (MID-self.low).apply(lambda p:max(p,0))
        return temp1.rolling(window = N).sum()/temp2.rolling(window = N).sum()*100
    
    def MACR(self,M):
        c = int(M/2.5)
        temp = (self.CR().rolling(window = M).mean()).shift(1+c)
        return temp
    
    def MACR1(self):
        return self.MACR(M = 10)
    
    def MACR2(self):
        return self.MACR(M = 20)
    
    def MACR3(self):
        return self.MACR(M = 40)
    
    def MACR4(self):
        return self.MACR(M = 60)
    
    def MASS(self,N1 = 9,N2 = 25):
        temp1 = (self.high-self.low).rolling(window = N1).mean()
        temp2 = temp1.rolling(window = N1).mean()
        temp3 = temp1/temp2
        return temp3.rolling(window = N2).sum()
    
    def MAMASS(self,M = 6):
        return self.MASS().rolling(window = M).mean()
    
    def SY(self,N = 9):
        temp1 = self.close-self.close.shift(1)
        temp2 = temp1.apply(lambda p:1 if p>0 else 0)
        return temp2.rolling(window = N).sum()/N*100
    
    def PCNT(self):
        return (self.close-self.close.shift(1))/self.close*100
    
    def AMP(self,N):
        temp1 = self.high.rolling(window = N).max()
        temp2 = self.low.rolling(window = N).min()
        return (temp1-temp2)/self.close.shift(N)
    
    def AMP1(self):
        return self.AMP(N = 1)
    
    def AMP3(self):
        return self.AMP(N = 3)
    
    def AMP5(self):
        return self.AMP(N = 5)
    
    def AMP10(self):
        return self.AMP(N = 10)
    
    def AMP20(self):
        return self.AMP(N = 20)
    
    def AMP60(self):
        return self.AMP(N = 60)
    
    def WMA(self,N):
        temp = self.close*N
        for i in range(1,N):
            temp = temp + self.close.shift(i)*(N-i)

        return temp*2/(N*(N+1))
    
    def WMA3(self):
        return self.WMA(N = 3)
    
    def WMA5(self):
        return self.WMA(N = 5)
    
    def WMA10(self):
        return self.WMA(N = 10)
    
    def WMA20(self):
        return self.WMA(N = 20)
    
    def WMA60(self):
        return self.WMA(N = 60)
    
    def WMA120(self):
        return self.WMA(N = 120)
    
    def WMA250(self):
        return self.WMA(N = 250)
    
    def VOLT(self,N):
        return self.close.rolling(window = N).std()
    
    def VOLT20(self):
        return self.VOLT(N = 20)
    
    def VOLT60(self):
        return self.VOLT(N = 60)
    
    def MDD(self,N):
        return self.close.rolling(window = N).max()-self.close.rolling(window = N).min()
    
    def MDD20(self):
        return self.MDD(N = 20)
    
    def MDD60(self):
        return self.MDD(N = 60)
    
    def AROON_UP(self,N = 14):
        temp = self.close.rolling(window  = N).apply(np.argmax)+1
        return temp/N*100
    
    def AROON_DOWN(self,N = 14):
        temp = self.close.rolling(window  = N).apply(np.argmin)+1
        return temp/N*100
    
    def QTYR_5_20(self,N=5,M=20):
        return self.volume.rolling(window = N).mean()/self.volume.rolling(window = M).mean()
    
    def OBV(self):
        temp = self.close-self.close.shift(1)
        temp1 = temp.apply(lambda p:sgn(p))
        temp2 = temp1*self.volume
        result = temp2.cumsum()
        return result