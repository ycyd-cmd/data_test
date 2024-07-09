import numpy as np
import pandas as pd

def get_alpha_obos(df):
    stock = alpha_obos(df)
    df['KDJ_K'] = stock.KDJ_K()
    df['KDJ_D'] = stock.KDJ_D()
    df['KDJ_J'] = stock.KDJ_J()
    df['RSI6'] = stock.RSI6()
    df['RSI10'] = stock.RSI10()
    df['WR'] = stock.WR()
    df['LWR1'] = stock.LWR1()
    df['LWR2'] = stock.LWR2()
    df['BIAS5'] = stock.BIAS5()
    df['BIAS10'] = stock.BIAS10()
    df['BIAS20'] = stock.BIAS20()
    df['BIAS36'] = stock.BIAS36()
    df['BIAS612'] = stock.BIAS612()
    df['MABIAS'] = stock.MABIAS()
    #df['ACCER'] = stock.ACCER()
    df['ADTM'] = stock.ADTM()
    df['MAADTM'] = stock.MAADTM()
    df['TR'] = stock.TR()
    df['ATR'] = stock.ATR()
    df['DKX'] = stock.DKX()
    df['MADKX'] = stock.MADKX()
    df['OSC'] = stock.OSC()
    df['CCI'] = stock.CCI()
    df['ROC'] = stock.ROC()
    df['MFI'] = stock.MFI()
    df['MTM'] = stock.MTM()
    df['MAMTM'] = stock.MAMTM()
    df['MARSI6'] = stock.MARSI6()
    df['MARSI10'] = stock.MARSI10()
    df['SKD_K'] = stock.SKD_K()
    df['SKD_D'] = stock.SKD_D()
    df['UDL'] = stock.UDL()
    df['MAUDL'] = stock.MAUDL()
    df['DI1'] = stock.DI1()
    df['DI2'] = stock.DI2()
    df['ADX'] = stock.ADX()
    df['ADXR'] = stock.ADXR()
    



class alpha_obos(object):
    def __init__(self,df_data):
        
        self.open = df_data['S_DQ_OPEN'] 
        self.high = df_data['S_DQ_HIGH'] 
        self.low = df_data['S_DQ_LOW']   
        self.close = df_data['S_DQ_CLOSE'] 
        self.volume = df_data['S_DQ_VOLUME']*100
        self.length = len(df_data) 

    def KDJ_K(self,N = 9,M1 = 3):
        RSV = (self.close-self.low.rolling(window = N).min())/(self.high.rolling(window = N).max()-self.low.rolling(window = N).min())
        K = RSV.ewm(span = M1*2-1,adjust = False).mean()
        return K
    
    def KDJ_D(self,N = 9,M2 = 3):
        D = self.KDJ_K().ewm(span = M2*2-1,adjust = False).mean()
        return D
    
    def KDJ_J(self):
        return self.KDJ_K()*3-self.KDJ_D()*2
    
    def RSI(self,N1):
        LC = self.close.shift(1)
        temp1 = (self.close-LC).apply(lambda p:max(p,0)).rolling(window = N1).mean()
        temp2 = abs(self.close-LC).rolling(window = N1).mean()
        return temp1/temp2*100
    
    def RSI6(self):
        return self.RSI(N1 = 6)
    
    def RSI10(self):
        return self.RSI(N1 = 10)
    
    def WR(self,N = 10): #L1 = 6
        temp1 = (self.high.rolling(window = N).max()-self.close)/(self.high.rolling(window = N).max()-self.low.rolling(window = N).min())*100
        return temp1
    
    def LWR(self,m):
        RSV = self.WR(N = 9)
        return RSV.rolling(window = m).mean()
    
    def LWR1(self):
        return self.LWR(m = 3)
    
    def LWR2(self):
        return self.LWR1().rolling(window = 3).mean()
    
    def BIAS(self,L1):
        return (self.close-self.close.rolling(window = L1).mean())/self.close.rolling(window = L1).mean()*100
    
    def BIAS5(self):
        return self.BIAS(L1 = 5)
    
    def BIAS10(self):
        return self.BIAS(L1 = 10)
    
    def BIAS20(self):
        return self.BIAS(L1 = 20)
    
    def BIAS_n_m(self,n,m):
        return self.close.rolling(window = n).mean()-self.close.rolling(window = m).mean()
    
    def BIAS36(self):
        return self.BIAS_n_m(n = 3,m = 6)
    
    def BIAS612(self):
        return self.BIAS_n_m(n = 6,m = 12)
    
    def MABIAS(self,M = 30):
        return self.BIAS36().rolling(window = M).mean()
    
    def ADTM(self,N = 23):
        open_bias = self.open-self.open.shift(1)
        HIGH_OPEN = self.high-self.open
        OPEN_LOW = self.open-self.low
        data = pd.DataFrame({'open-bias':open_bias,'HIGH_OPEN':HIGH_OPEN,'OPEN_LOW':OPEN_LOW})
        data['DTM'] = data.apply(lambda p:max(p['open-bias'],p['HIGH_OPEN']),axis = 1) #apply axis参数必须加！
        data['DBM'] = data.apply(lambda p:max(p['open-bias'],p['OPEN_LOW']),axis = 1)
        data.loc[(data['open-bias']<=0),'DTM'] = 0
        data.loc[(data['open-bias']>=0),'DBM'] = 0
        STM = data['DTM'].rolling(window = N).sum()
        SBM = data['DBM'].rolling(window = N).sum()
        ADTM = (STM-SBM)/STM
        ADTM.loc[(STM == SBM)] = 0 #如果是单列，loc条件里后面,:就不加了
        ADTM.loc[(STM<SBM)] = (STM-SBM)/SBM
        return ADTM

    def MAADTM(self,M = 8):
        return self.ADTM().rolling(window = M).mean()
    
    def TR(self,M1 = 9):
        HIGH_LOW = self.high-self.low
        HIGH_REFCLOSE = abs(self.high-self.close.shift(1))
        LOW_REFCLOSE = abs(self.low-self.close.shift(1))
        data = pd.DataFrame({'HIGH_LOW':HIGH_LOW,'HIGH_REFCLOSE':HIGH_REFCLOSE,'LOW_REFCLOSE':LOW_REFCLOSE})
        data['TR'] = data.apply(lambda p: max(p['HIGH_LOW'],p['HIGH_REFCLOSE'],p['LOW_REFCLOSE']),axis = 1)
        TR = data['TR'].rolling(window = M1).sum()
        return TR
    
    def ATR(self,N = 14):
        return self.TR().rolling(window = N).mean()
    
    def DKX(self):
        MID = (self.close*3 + self.low + self.high + self.open)/6
        DKX = 20*MID
        for i in range(1,19):
            DKX = DKX + (20-i)*(MID.shift(i))
        DKX = DKX + MID.shift(20)
        return DKX/210
    
    def MADKX(self,M = 10):
        return self.DKX().rolling(window = M).mean()
    
    def OSC(self,N = 10):
        return 100*(self.close-self.close.rolling(window = N).mean())
    
    def CCI(self,N = 14):
        TYP = (self.high + self.low + self.close)/3
        MA = TYP.rolling(window = N).mean()
        AVEDEV_TYP = TYP.rolling(window = N).apply(lambda p: abs(p-p.mean()).mean(),raw = False)
        return (TYP-MA)/(0.015*AVEDEV_TYP)
    
    def ROC(self,N = 12):
        return 100*(self.close-self.close.shift(N))/self.close.shift(N)
    
    def MFI(self,N = 14):
        TYP = (self.high + self.low + self.close)/3
        data = pd.DataFrame({'TYP':TYP,'TYP_shift1':TYP.shift(1),'volume':self.volume})
        data['up'] = data.apply(lambda p: p['TYP']*p['volume'] if p['TYP']>p['TYP_shift1'] else 0,axis = 1)
        data['down'] = data.apply(lambda p: p['TYP']*p['volume'] if p['TYP']<p['TYP_shift1'] else 0,axis = 1)
        V1 = data['up'].rolling(window=N).sum()/data['down'].rolling(window=N).sum()
        return 100-(100/(1+V1))
    
    def MTM(self,N = 14):
        return self.close-self.close.shift(N)
    
    def MAMTM(self,M = 10):
        return self.MTM().rolling(window = M).mean()
    
    def MARSI(self,N):
        return self.RSI(N1 = N).rolling(window = N).mean()
    
    def MARSI6(self):
        return self.MARSI(N = 6)
    
    def MARSI10(self):
        return self.MARSI(N = 10)
    
    def SKD_K(self,N = 9,M = 3):
        RSV = (self.close-self.low.rolling(window = N).min())/(self.high.rolling(window = N).max()-self.low.rolling(window = N).min()).ewm(span = M,adjust = False).mean()
        return RSV.ewm(span = M,adjust = False).mean()
    
    def SKD_D(self,M = 3):
        return self.SKD_K().rolling(window = M).mean()
    
    def UDL(self,N1 = 3,N2 = 5,N3 = 10,N4 = 20):
        temp = self.close
        return (temp.rolling(window = N1).mean()+temp.rolling(window = N2).mean()+temp.rolling(window = N3).mean()+temp.rolling(window = N4).mean())/4
    
    def MAUDL(self,M = 6):
        return self.UDL().rolling(window = M).mean()
    
    def DI1(self,M1 = 14):
        HIGH_LOW = self.high-self.low
        HIGH_REFCLOSE = abs(self.high-self.close.shift(1))
        LOW_REFCLOSE = abs(self.low-self.close.shift(1))
        data = pd.DataFrame({'HIGH_LOW':HIGH_LOW,'HIGH_REFCLOSE':HIGH_REFCLOSE,'LOW_REFCLOSE':LOW_REFCLOSE})
        data['TR'] = data.apply(lambda p:max(p['HIGH_LOW'],p['HIGH_REFCLOSE'],p['LOW_REFCLOSE']),axis = 1).rolling(window = M1).sum()
        data['HD'] = self.high-self.high.shift(1)
        data['LD'] = self.low.shift(1)-self.low
        data['DMP'] = data.apply(lambda p: p['HD'] if (p['HD']>0)&(p['HD']>p['LD']) else 0, axis = 1)
        data['DMM'] = data.apply(lambda p: p['LD'] if (p['LD']>0)&(p['LD']>p['HD']) else 0, axis = 1)
        DI1 = data['DMP']*100/data['TR']
        DI2 = data['DMM']*100/data['TR']
        return DI1
    
    def DI2(self,M1 = 14):
        HIGH_LOW = self.high-self.low
        HIGH_REFCLOSE = abs(self.high-self.close.shift(1))
        LOW_REFCLOSE = abs(self.low-self.close.shift(1))
        data = pd.DataFrame({'HIGH_LOW':HIGH_LOW,'HIGH_REFCLOSE':HIGH_REFCLOSE,'LOW_REFCLOSE':LOW_REFCLOSE})
        data['TR'] = data.apply(lambda p:max(p['HIGH_LOW'],p['HIGH_REFCLOSE'],p['LOW_REFCLOSE']),axis = 1).rolling(window = M1).sum()
        data['HD'] = self.high-self.high.shift(1)
        data['LD'] = self.low.shift(1)-self.low
        data['DMP'] = data.apply(lambda p: p['HD'] if (p['HD']>0)&(p['HD']>p['LD']) else 0, axis = 1)
        data['DMM'] = data.apply(lambda p: p['LD'] if (p['LD']>0)&(p['LD']>p['HD']) else 0, axis = 1)
        DI1 = data['DMP']*100/data['TR']
        DI2 = data['DMM']*100/data['TR']
        return DI2
    
    def ADX(self,M2 = 6):
        DI1 = self.DI1()
        DI2 = self.DI2()
        return (abs(DI2-DI1)/(DI1+DI2)*100).rolling(window = M2).mean()
    
    def ADXR(self,M2 = 6):
        return (self.ADX()+self.ADX().shift(M2))/2



    


    

    


    


    



