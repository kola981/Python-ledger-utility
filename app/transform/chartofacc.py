'''
Created on Dec 30, 2020

@author: ak981
'''
from transform import constants
import re
import pandas as pd

'''
    Block variables
'''

sub1 = constants.SUBANALYTIC_1
sub3 = constants.SUBANALYTIC_3
code = 'Код'
counterparties = constants.COUNTERPARTIES

'''
    Block functions
'''

def createDictOfAcc(df):
    d = dict()
    
    subconto1 = df.columns.get_loc(sub1)
    subconto2 = df.columns.get_loc(sub3)
    acc_col = df.columns.get_loc(code)
    #print("s1={}, s2={}, acc={}".format(subconto1, subconto2, acc_col))

    for i in range(0, len(df)):
        temp = []
        acc = str(df.iat[i, acc_col])
        #print("i:{} acc:{}".format(i, df.iat[i, acc_col]))
        if re.match('^[0-9]', acc) != None :
                
            for j in range(subconto1, subconto2 + 1):
                item = df.iat[i, j]
                _processAnalytics(temp, item, acc)
                d[acc] = temp
           
        #stop if 9th class is processed
        if _isOffBalanceSheet(df, i, acc_col):
            break
    
    #logger5.info(str(d))     
    print(str(d))
    
    return d

def _isOffBalanceSheet(df, row, col):
    return str(df.iat[row, col]).startswith('9') and str(df.iat[row+1, col]).startswith('0')


def _processAnalytics(l, val, account):
        suffix = ''
        #print("type:{} {} {}".format(type(val), val, pd.isnull(val)))
        if isinstance(val, float) and pd.isnull(val):
            val = ''
        elif isinstance(val, str):
            val = val.strip()
            
            #save debtors  creditors distinction for counterparties
            if counterparties == val or constants.AGREEMENTS == val:
                suffix = _setSuffix(account)
                
        #print("type:{} {}".format(type(val), val))
        l.append('{}{}'.format(val, suffix))

        
def _setSuffix(account):
    if account.startswith('2'):
        return ''
    elif account.startswith('681'):
        return ' Dt'
    elif account.startswith('371'):
        return ' Kt'
    elif int(account[:1]) < 4:
        return ' Dt'
    else:
        return ' Kt'