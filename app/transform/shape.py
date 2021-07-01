'''
Created on Dec 29, 2020

@author: ak981
'''
import pandas as pd
from abc import ABC, abstractmethod
from itertools import compress
from app.transform import constants
from app.transform.data import Data
from pandas.core.frame import DataFrame

class Reshaper:
    '''
    classdocs
    '''     
    ROWS = 10
    
    
    def __init__(self, journal, chart, reshaper, **kwargs):
        self._journal = journal
        self._chart = chart
        self._reshaper = reshaper
        
        #unique column to scan header
        self.header_col = None
        if 'header_col' in kwargs:
            self.header_col = kwargs['header_col'] #'Рахунок Дт'

        #unique column to scan transactions
        self.transact_col = None
        if 'transact_col' in kwargs:
            self.transact_col = kwargs['transact_col'] #'№' #'Документ'

        #account dt and kt
        self.dt = None  
        if 'dt' in kwargs:
            self.dt = kwargs['dt']
        
        self.kt = None  
        if 'kt' in kwargs:
            self.kt = kwargs['kt']
        
        self.code = None
        if 'code' in kwargs:
            self.code = kwargs['code']
     
    
    def preprocessTable(self, df):
        self._dropNonTableRows(df)
        self._dropNonTableColumns(df)
        df = self._reshaper.flatten(df)
        df = df.fillna('')    
        print("finished preprocessing table")
        return df
    
     
    def setReshaper(self, reshaper):
        self._reshaper = reshaper
     
        
    def _dropNonTableRows(self, df):
    # find header & drop unnecessary rows
        temp_df = df.head(self.ROWS)
        start_row = self._findFirstRow(temp_df)
        if start_row != 0:
            tech_rows = [i for i in range(0, start_row)]
            #print("_preprocessD {}".format(tech_rows))  
            df.drop(tech_rows, axis="index", inplace=True)
            df.reset_index(drop='true', inplace=True)
    
    def _findFirstRow(self, df):
    #find header row   
        rows = set()
        for item in constants.HEADERS:
            rows.update(set(df.loc[df.isin([item]).any(axis=1)].index.tolist()))
            #print("{} -> {}".format(item, rows))
        return  min(rows)
    
    def _dropNonTableColumns(self, df):
        df.dropna(axis='columns', how='all', inplace=True)
        columns = [i for i in range(0, len(df.columns))]
        df.columns = columns
        #print(df.head(1)) 
    
    def dropEmpty(self, df, rows):
        df = df.drop(rows, axis="index")
        df = df.dropna(axis='columns', how='all')
        return df
    
    def findUniqueColumnIndex(self, header_col, df):
    #scan unique records by column (e.g. date, Account etc...)
        for i in range(0, df.columns.size):
            #print("{} {}".format(i, df.iat[0,i]))       
            if  self.adjust_str(df.iat[0,i]) in constants.HEADERS:
                return i
        
    def adjust_str(self, txt):
        if type(txt) is str:
            return  txt.replace(u'\xa0', u' ')
        return str(txt)        
    
    
class TableReshaper(ABC):
    
    @abstractmethod
    def flatten(self, df: DataFrame) -> DataFrame:
        pass
    

class InlineJournalReshaper(TableReshaper):
    
    def __init__(self, **kwargs):
        #unique column to scan header
        self.header_col = None
        if 'header_col' in kwargs:
            self.header_col = kwargs['header_col'] #'Рахунок Дт'

        #unique column to scan transactions
        self.transact_col = None
        if 'transact_col' in kwargs:
            self.transact_col = kwargs['transact_col'] #'№' #'Документ'

        #account dt and kt
        self.dt = None  
        if 'dt' in kwargs:
            self.dt = kwargs['dt']
        
        self.kt = None  
        if 'kt' in kwargs:
            self.kt = kwargs['kt']
        
        self.code = None
        if 'code' in kwargs:
            self.code = kwargs['code']
    
    
    def flatten(self, df):   
        print(df.head(3))  
      
        #find full header
        temp_df = df.head(10)
        start_row = 0
        start_col = self._findColumn(0, temp_df)
        header_rows = self._findHeaderRows(start_row, start_col, temp_df)
        print(header_rows)
    
        #scan headers and add new columns
        col_names = self.createColumnNamesList(header_rows, df)
        col_names = self.cleanUpHeader(col_names)
        col_indexes = self.createColumnIndexesList(header_rows, df)
        
        self.addExtraColumnsFromList(col_indexes, df) 
    
        #update header and delete header lines, correct indexes and dtypes
        df.head(10).to_excel(r'/media/ak981/ITProjects/data/BTU/1C Data/je3.xlsx', sheet_name='db')
        print(df.columns)
        print(df.head(2))
        df2 = self.updateHeader(col_names, df)
        hr2 = [x for x in range(0, start_row)]
        header_rows.extend(hr2)
        df2 = df2.drop(header_rows, axis="index")
        #print(df2.head(20))
        #df2.to_excel(r'/media/ak981/ITProjects/data/BTU/1C Data/je1_.xlsx', sheet_name='db') 
        df2 = df2.reset_index(drop='true')
        df2 = df2.astype('object')
    
       
        
        #find unique records
        temp_arr3 = self.findPrimaryRecords(self.transact_col, df2)
        temp_arr2 = self.isAccountingRecord(self.transact_col, self.dt, self.kt, df2)   
        temp_arr = list(compress(temp_arr3, temp_arr2))
    
        #print('{} = {}'.format(len(temp_arr3), len(temp_arr2)))
        #print("{}\n{}\n{}".format(temp_arr3, temp_arr2, temp_arr))
    
        #transform inline, drop empty cols, drop na
        self.transformRecordsInline(temp_arr, df2)
        df2 = self.deleteUnusedRows(df2, temp_arr)
        return df2

    


    #find header column
    def _findColumn(self, row, df):
        for i in range(0, df.columns.size):
            #print("{}".format(df.iat[row, i]))
            if  df.iat[row, i] in constants.HEADERS:
                #print(i)
                return i    
        

    def _findHeaderRows(self, row, col, df):
    #find header rows   
        arr = list()
        arr.append(row)
    
        df_size = df[col].size
        for x in range(row+1, df_size): 
            #add each record till first number
            #print("{} {}".format(x,  df.iat[x, col]))
            if pd.isnull(df.iat[x, col]):
                arr.append(x)
            #if not isinstance(df.loc[x, col], (int, float, complex)) or pd.isnull(df.loc[x, col]):
            #    arr.append(x)        
            else:
                return arr 



    def createColumnIndexesList(self, arr, df):
    #create list of indexes to insert    
    # print(arr)
        cols = []
        for i in range(0, df.columns.size):
            k = i
            for j in range(0, len(arr)):
                val = df.iat[j, df.columns.get_loc(i)]
                if j == 0:
                    if val in constants.ANALYTIC:
                        cols.append(k+len(cols)+1)
                        cols.append(k+len(cols)+1)
                        #print(val +" -> "+str(cols))
                elif not pd.isnull(val):
                    cols.append(k+len(cols)+1)
                    #print(val +" -> "+str(cols))
    
        return cols


    def createColumnNamesList(self, arr, df):
    #create list of columns names to insert    
        cols = []
        for i in range(0, df.columns.size):
            for j in range(0, len(arr)):
                val = df.iat[arr[j], df.columns.get_loc(i)]
                #print("i:{} c:{} {}".format(j, i, val))
                if not pd.isnull(val):
                    if val in constants.ANALYTIC:
                        cols.append(val)
                        cols.append(val+" "+str(2))
                        cols.append(val+" "+str(3))
                    else:
                        if cols.count(val) > 0:
                            temp_cols = [x for x in cols if x.startswith(val) and len(x) > len(val)]
                            if (temp_cols):
                                #print(temp_cols)
                                for x in range(0, len(temp_cols)):
                                    item = arr[x]
                                    i_num = int(item[item.find(" "):])
                                    #print("{} {}".format(item, i_num))
                                    raise Exception("uncompleted method - indexes should be updated")
                            else:
                                cols.append(val +" "+str(1) )   
                        else:
                            cols.append(val) 
        #print(cols)
        return cols


    def cleanUpHeader(self, cols):
    #clean up non unicode space
        return [c.replace(u'\xa0', u' ') for c in cols]


    def addExtraColumnsFromList(self, list,df):
        #add columns to dataframe        
        for i in range(0, len(list)):
            df.insert(list[i], 'n'+str(list[i]), '') 



    def updateHeader(self, arr, df):
        #update column names        
        dict = {}
        for i in range(0, df.columns.size):
            dict[df.columns[i]] = arr[i]
        #print(dict)
        return df.rename(columns=dict)  



    def findPrimaryRecords(self, col, df):
        #print("{}  -> {}".format(col, df.head(1)) )
    # find unique records     
        arr = []
        ind = []
        
        df_size = df[col].size
        col_ind = df.columns.get_loc(col)
        #print(col_ind)
        for x in range(0, df_size): 
            #add each not nan record
            if not pd.isnull(df.iat[x, col_ind]):
                if x != 0:
                    arr.append(ind)
                    #print(str(x)+" :: "+ str(ind))
                    #logger4.info(str(x)+" :: "+ str(ind))
                ind = []
                ind.append(x)  
            else:
                ind.append(x)
            #add last record
            if x == df_size - 1:
                arr.append(ind)
                #print(str(x)+" :: "+ str(ind))
                #logger4.info(str(x)+" :: "+  str(ind))
        
        return arr



    def isAccountingRecord(self, col, dt, ct, df):
    # check if double entry is correct
        arr = []
        df_size = df[col].size
        col_ind = df.columns.get_loc(col)
        dt_ind = df.columns.get_loc(dt)
        ct_ind = df.columns.get_loc(ct)
    
        for x in range(0, df_size):
            #add each not nan record
            if not pd.isnull(df.iat[x, col_ind]):
                a = not pd.isnull(df.iat[x, dt_ind])
                b = not pd.isnull(df.iat[x, ct_ind])
                res = a and b
                arr.append(res)
                #print(str(x)+"::"+str(a)+" - "+str(b)+" -> " + str(res))
                #logger1.info(str(x)+"::"+str(a)+" - "+str(b)+" -> " + str(res))
            
        return arr



    def transformRecordsInline(self, arr, df):
    #transform columns in line
        #print(df.columns)
        for col in df.columns:
            self.transformColumn(arr, col, df)

        

    def transformColumn(self, arr, col, df):
    #transform column            
        col_ind = df.columns.get_loc(col)
        for i in range(0, len(arr)):
            item = arr[i]
            #print(item)
            if len(item) > 0:
                for j in range (1, len(item)):
                    row_ind = item[j]
                    delta = row_ind - item[0]
                    #print("{} > {}".format(row_ind, col_ind))
                    #print("{}  : [{}, {}) -> ({}, {})".format(df.iat[row_ind, col_ind], row_ind, col_ind, item[0], col_ind + delta))
                    val = df.iat[row_ind, col_ind]
                    
                    if not pd.isnull(val) and not val == '':
                        df.iat[item[0], col_ind + delta] = df.iat[row_ind, col_ind]
                        #s2 = "{}  : [{}, {}) -> ({}, {})".format(df.iat[row_ind, col_ind], row_ind, col_ind, item[0], col_ind + delta)
                        #logger2.info(s2)
                        #print(s2)

    def deleteUnusedRows(self, df, list_of_transactions):   
        #get only actual records and flatten list 
        new_list=[sublist[0] for sublist in list_of_transactions]
        #print(new_list)
        return df[df.index.isin(new_list)]

    
class ChartReshaper(TableReshaper):
    
    def __init__(self):
        pass
    
    def flatten(self, df: DataFrame) -> DataFrame:
        if not self._areColumnNames(df.columns):
            list_ = df.loc[0].values.tolist()
            list_ = self._correctHeader(list_)
            df.columns = list_
            df = df.drop([0], axis="index")
            df = df.reset_index(drop='true')
        return df
    
    def _areColumnNames(self, columns):
        for col in columns:
            #print("{} is int? {}".format(col, isinstance(col, int)))
            if not isinstance(col, int):
                return True
        return False
    
    def _correctHeader(self, list_: list) -> list:
        return list_
    
    
class SimpleJournalReshaper(ChartReshaper):
    
    def _correctHeader(self, list_):
        #check duplicates and correct names
        s = set(list_)
        if len(s)!=len(list_):
            #find duplicates
            for item in s:
                if list_.count(item)>1:
                    for j in range(0, list_.count(item)):
                        list_[list_.index(item)] = "{}_{}".format(item, j)
