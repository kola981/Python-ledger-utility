'''
Created on Dec 29, 2020

@author: ak981
'''
import re
import pandas as pd
from app.transform import constants
from app.transform.data import Data

class Transformer:
    '''
    classdocs
    '''    
    
    sub1 = constants.SUBANALYTIC_1
    sub2 = constants.SUBANALYTIC_2
    sub3 = constants.SUBANALYTIC_3
    counterparties = constants.COUNTERPARTIES
    
    
    
    
    COL_DOC_TYPE = 'Doc_type'
    COL_DOC_NUMBER = 'Doc_number'
    REV_AMOUNT = 'Rev'
    PL_ITEMS = 'PL items'
    
    REV_COLS = ('Договоры Dt', 'Контрагенты Dt', )
    COST_COLS = ('Подразделения', 'Номенклатурные группы', '(об) Подразделения')
    
    INV = 'Номенклатура'
    QUANT_KT = 'Количество Кт'
    
    docs = constants.DOCUMENT_TYPES_1C8
    
    
    def __init__(self, journal, chart, data, **kwargs):
        self._data = data
        self._journal = journal
        self._chart = chart
        
        self.code = None
        if 'code' in kwargs:
            self.code = kwargs['code']

        self.sub_dt = None
        if 'sub_dt' in kwargs:
            self.sub_dt = kwargs['sub_dt']
        
        self.sub_kt = None
        if 'sub_kt' in kwargs:
            self.sub_kt = kwargs['sub_kt']

        if 'doc' in kwargs:
            self.doc = kwargs['doc']
            
        self.amount = None
        if 'amount' in kwargs:
            self.amount = kwargs['amount']
          
        self.dt = None  
        if 'dt' in kwargs:
            self.dt = kwargs['dt']
        
        self.kt = None  
        if 'kt' in kwargs:
            self.kt = kwargs['kt']
        
        self.rev = None  
        if 'rev' in kwargs:
            self.rev = kwargs['rev']
        
        self.cost = None  
        if 'cost' in kwargs:
            self.cost = kwargs['cost']
        
        self.rev_columns = None
        self.cost_columns = None
     
     
     
    def addDimensionalData(self, df):
        #add new columns to journal
        df = self. _createNewColumnsAndUpdateDataframe(df)
        print("new columns one per analytics added to table")        
        
        #update analytics
        is_je_list = self._findAllJournalEntries(df)
        if not is_je_list:
            raise Exception("Empty list of journal entries")
        self._fillInAnalyticalData(self.dt, self.sub_dt, df,  is_je_list)
        self._fillInAnalyticalData(self.kt, self.sub_kt, df,  is_je_list)
        print("Analytics added")
        #df.head(5).to_excel(r'/media/ak981/ITProjects/data/BTU/1C Data/je6.xlsx', sheet_name='db')
        
        return df

    
    def _findAllJournalEntries(self, df):        
        return [Transformer.isJournalEntry(
                                                             df.iat[col, df.columns.get_loc(self.dt)],
                                                             df.iat[col, df.columns.get_loc(self.kt)]
                                                             )
             for col in range(0, len(df))]

    
    def _createNewColumnsAndUpdateDataframe(self, df):
        header = self._data.getHeader()
        if not header:
           raise Exception("Empty dictionary of accounts from chart of accounts") 
        df = df.reindex(columns=header)
        return df.astype('object')
        
    '''
    def _createListOfColumnNames(self,  _dict):
        l = []
        
        for val in _dict.values():
            l.extend(val)
        set_of_columns = set(l)
        set_of_columns.discard('')
        #print(set_of_columns)
        
        return sorted(set_of_columns, key=str.lower, reverse=False) 


    def _updateColumnNames(self, set_of_columns, df):
        #add new columns to ledger dataframe
        list_of_names = df.columns.tolist()
        list_of_names.extend(set_of_columns)
        
        return  df.reindex(columns=list_of_names)
    '''
    
    
    
    def _fillInAnalyticalData(self, column, analytics_column, df, *l):
        dict_coa = self._data.getChartOfAccounts()
        if not dict_coa:
           raise Exception("Empty dictionary of accounts from chart of accounts") 
        column_ind = df.columns.get_loc(column)
        analytical_col_ind = df.columns.get_loc(analytics_column)
        is_je_list = l[0]
        #print(df.columns)
        #for each row
        for i in range(0, len(df)):
            if is_je_list[i]:
            
                #for each document
                item = str(df.iat[i, column_ind])
                #print("{} -> {}".format(item, dict_coa[item]))
                #logger6.info("{} -> {}".format(item, dict_coa[item]))
                analytics = dict_coa[item]
                for k in range(0, len(analytics)):
                    if analytics[k]:                    
                        c = analytical_col_ind + k
                        cc = analytics[k]
                        #logger3.debug(cc+" :: "+str(i)+"::"+str(df.at[i, cc])+" -> "+str(df.iat[i, c]))
                        if df.iat[i, c]:
                            df.iat[i, df.columns.get_loc(cc)] = df.iat[i, c]
     
     
    def postProcessData(self, df):
    #post processing revenue and clean up    
        self.configure(df.columns)
        df[self.amount] = df[self.amount].apply(Postprocessor.toFloat)  
        self.splitDocument(df)
    
        #find transactions & correct rev transactions
        list_of_transactions = self._findTransactions(self.doc, df)
        #print(" List of tr - postprocess {}".format(list_of_transactions))
        self._processTransactions(df, list_of_transactions)
    
        print('Revenue processed : cols {}'.format(len(df.columns)))
    
        df = df.dropna(axis='columns', how='all')    
        print('Empty columns dropped : cols {}'.format(len(df.columns)))

        return df
    
    
    def configure(self, columns):
        if len(set(self.REV_COLS).intersection(set(columns))) == len(self.REV_COLS):
            self.rev_columns = self.REV_COLS
        else:
            raise Exception("Revenue columns {} not in the list:{}".format(self.REV_COLS, columns))
        
        if len(set(self.COST_COLS).intersection(set(columns))) == len(self.COST_COLS):
            self.cost_columns = self.COST_COLS
        else:
            raise Exception("Cost columns {} not in the list{}".format(self.COST_COLS, columns))
    
    def splitDocument(self, df):
        df[self.doc] = df[self.doc].apply(Postprocessor.correctSpace)
        #split string by document type & number
        df[self.COL_DOC_TYPE] = df[self.doc].apply(self._getDocType)
        df[self.COL_DOC_NUMBER] = df[self.doc].apply(self._getDocNumber)
    
        print('Documents parsed')


    def _getDocType(self, doc):
        for item in constants.DOCUMENT_TYPES_1C8:
            if doc.startswith(item):
                #print("{} = {}: {}".format(doc, item, doc.startswith(item)))
                return item
    
        raise Exception("{} not parsed".format(doc))
     
     
    def _getDocNumber(self, doc):
        for item in constants.DOCUMENT_TYPES_1C8:
            if doc.startswith(item):
                index = str(doc).find(item)
                if index != -1:
                    return str(doc[index+len(item)+1:])
                else:
                    return doc

    def _findTransactions(self, col, df):
        # find unique records
        arr = []
        ind = []
    
        df_size = df[col].size
        colInd = df.columns.get_loc(col)
        #print(colInd)
    
        for x in range(0, df_size - 1): 
            #add each equal record
            if df.iat[x, colInd] != df.iat[x+1, colInd]: 
                ind.append(x)
                arr.append(ind)     
                ind = []
                #print(str(x)+" :: "+ str(ind))
                #logger4.info(str(x)+" :: "+ str(ind))          
             
            else:
                ind.append(x)
                #add last record
                if x == df_size - 2:
                    ind.append(x+1)
                    arr.append(ind)
                    #print(str(x)+" :: "+ str(ind))
                    #logger4.info(str(x)+" :: "+  str(ind))
        
        return arr
    



    def _processTransactions(self, df, list_of_transactions):
        col = df.columns.get_loc(self.COL_DOC_TYPE)
        for transaction in list_of_transactions:
            #check doc type
            document =  df.iat[transaction[0], col]
            #print(document)
            self._processRevenue(df, document, transaction)
            self._processCosts(df, document, transaction)
            
            
     
    def _processRevenue(self, df, document, transaction):
        docs = ('Акт об оказании производственных услуг', 'Реализация товаров и услуг' , 'Возврат товаров поставщику',
                'Реалізація товарів і послуг',)   
        existing_cols = self.rev_columns #('Договоры Dt', 'Контрагенты Dt')
        columns_to_expand = [df.columns.get_loc(i) for i in existing_cols]
        
        conditions = { df.columns.get_loc(self.dt):(70, 90), df.columns.get_loc(self.kt):(70, 90)}
        #print("dt  {} col {} kt  {} col {}".format(self.dt, df.columns.get_loc(self.dt), self.kt, df.columns.get_loc(self.kt)))
        #print("conditions {} \t cols {} \t existing cols {}".format(conditions, columns_to_expand, existing_cols))
        seq_condition = { df.columns.get_loc(self.kt):(70,), }
        
        if not isinstance(conditions, dict):
            raise Exception("Wrong conditions")   
        
        if document in docs:
            #print("Processing {}".format(document))   
            self._expandTransaction(df, transaction, columns_to_expand, conditions)   
            self._addNewData(df, transaction, self.REV_AMOUNT, self._addRevenue)
            self._addNewData(df, transaction, self.PL_ITEMS, self._addPlItem)
            self._addSequentialData(df, transaction, self.INV,  seq_condition)
    
    def _processCosts(self, df, document, transaction):
        docs = ('Авансовый отчет', 'Требование-накладная', 'Поступление товаров и услуг',
                        'Авансовий звіт', 'Надходження товарів і послуг')
        existing_cols = self.cost_columns #('Подразделения', 'Номенклатурные группы', '(об) Подразделения')
        columns_to_expand = [df.columns.get_loc(i) for i in existing_cols]
        
        conditions = { df.columns.get_loc(self.dt):(23, 9) }
        
        #print("dt  {} col {} kt  {} col {}".format(self.dt, df.columns.get_loc(self.dt), self.kt, df.columns.get_loc(self.kt)))
        #print("conditions {} \t cols {} \t existing cols {}".format(conditions, columns_to_expand, existing_cols))
        
        if document in docs: 
            self._expandTransaction(df, transaction, columns_to_expand, conditions)
    
    
    def _expandTransaction(self, df, transaction, columns_to_expand, conditions):
        #cols = [df.columns.get_loc(i) for i in columns_to_expand]
        row = self._findRowWithData(df, transaction, conditions)
        self._expandRow(df, transaction, row, columns_to_expand)
            
    
    def _findRowWithData(self, df, transaction, conditions):
        for row in transaction:
            for col, vals in conditions.items():
                list_of_conditions = self._createConditions(self._checkAccountCondition, df.iat[row, col], vals)
                print("Row[{}] {} starts with {}".format(row,df.iat[row, col], vals))
                if any(list_of_conditions):
                    return row    
    
    
    def _createConditions(self, condition, acc, vals):
        return [condition(acc, val) for val in vals]
    
    
    def _checkAccountCondition(self, acc, val):
        #print("{} = {} -> {}".format(acc,val, self._checkAccountCondition(acc, val)))
        if not isinstance(acc, str):
            acc = str(acc)
        if not isinstance(val, str):
            val = str(val)       
        return acc.startswith(val)
 
                    
    def _expandRow(self, df,  transaction, row_with_data, cols):
        for row in transaction:
                if row != row_with_data and row_with_data:
                    for col in cols:
                        #print("row {}, col {}, val ".format(row_with_data, col)) #, df.iat[row_with_data, col]
                        df.iat[row, col] = df.iat[row_with_data, col]
    
    
    def _addNewData(self, df, transaction, col_name,  func):
        if col_name not in df.columns:
            df.insert(len(df.columns), col_name,'')
        for row in transaction:
            df.iat[row, df.columns.get_loc(col_name)] = func(df, row)
    
    
    def _addSequentialData(self, df, transaction, col_name,  conditions):
        l = list()
        q = list()
        
        for row in transaction:
            item = df.iat[row, df.columns.get_loc(col_name)]
            quantity = df.iat[row, df.columns.get_loc(self.QUANT_KT)]
            
            print("Conditions: item -> {}, item not in list -> {} ".format(item, item not in l))
            
            if (not pd.isnull(item)) and item not in l:
                ind = len(l)
                l.append(item)
                
                print("index {} quantity {} list l {}".format(ind, quantity, len(l)))
                q.append(quantity)
                
            elif  item  in l:
                print(" list {} quantity {}".format(len(l), len(q)))
                index = l.index(item, 0)
                q_item = q.pop(index)
                q_item = q_item + quantity
                print("index {} quantity {} quantity to insert {}".format(index, quantity, q_item))
                q.insert(index, q_item)
            
                
                
        row = self._findRowWithData(df, transaction, conditions)       
        print("First row {}, list {}".format(row, l))
        
        if l:
            print("if -> {}".format(not l))
            if len(l) != len(q):
                raise RuntimeError("Length l {} != q {}".format(len(l), len(q)))
            while l:
                i = l.pop(0)
                j = q.pop(0)
                print("{} - {}".format(i, not l))
                df.iat[row, df.columns.get_loc(col_name)] = i
                df.iat[row, df.columns.get_loc(self.QUANT_KT)] = j
                row = row + 1
                print("Item {}, row {}, col {}, ".format(i, row, df.columns.get_loc(col_name)))    
        
        
    
    def _addRevenue(self, df,  row):
        dt = df.iat[row, df.columns.get_loc(self.dt)]
        kt = df.iat[row, df.columns.get_loc(self.kt)]
        amount = df.iat[row, df.columns.get_loc(self.amount)]
        #adjust amounts
        if str(kt).startswith('7'):
            return amount
        elif str(dt).startswith('7') or str(dt).startswith('9'):
            return - amount
        
        
    def _addPlItem(self, df, row):   
        dt = df.iat[row, df.columns.get_loc(self.dt)]
        kt = df.iat[row, df.columns.get_loc(self.kt)]
        rev = df.iat[row, df.columns.get_loc(self.rev)]
        cost = df.iat[row, df.columns.get_loc(self.cost)]
        if str(kt).startswith('7') or str(dt).startswith('7'):
            return rev
        elif str(dt).startswith('9'):
            return cost
        
        
    def addRevenue(self, dt,  kt, amount):
        #adjust amounts
        if str(kt).startswith('7'):
            return amount
        elif str(dt).startswith('7') or str(dt).startswith('9'):
            return - amount           

    def addPlItem(self, dt, kt, rev, cost):
        if str(kt).startswith('7') or str(dt).startswith('7'):
            return rev
        elif str(dt).startswith('9'):
            return cost

    def createDictOfAcc(self, df):
        d = dict()
    
        subconto1 = df.columns.get_loc(self.sub1)
        subconto2 = df.columns.get_loc(self.sub3)
        acc_col = df.columns.get_loc(self.code)
        #print("s1={}, s2={}, acc={}".format(subconto1, subconto2, acc_col))

        for i in range(0, len(df)):
            temp = []
            acc = str(df.iat[i, acc_col])
            #print("i:{} acc:{}".format(i, df.iat[i, acc_col]))
            if re.match('^[0-9]', acc) != None :
                
                for j in range(subconto1, subconto2 + 1):
                    item = df.iat[i, j]
                    self._processAnalytics(temp, item, acc)
                    d[acc] = temp
           
            #stop if 9th class is processed
            if self._isOffBalanceSheet(df, i, acc_col):
                break
    
        #logger5.info(str(d))     
        print(str(d))
    
        return d

    def _isOffBalanceSheet(self, df, row, col):
        return str(df.iat[row, col]).startswith('9') and str(df.iat[row+1, col]).startswith('0')


    def _processAnalytics(self, l, val, account):
            suffix = ''
            #print("type:{} {} {}".format(type(val), val, pd.isnull(val)))
            if isinstance(val, float) and pd.isnull(val):
                val = ''
            elif isinstance(val, str):
                val = val.strip()
            
                #save debtors  creditors distinction for counterparties
                if self.counterparties == val or constants.AGREEMENTS == val:
                    suffix = self._setSuffix(account)
                
            #print("type:{} {}".format(type(val), val))
            l.append('{}{}'.format(val, suffix))

        
    def _setSuffix(self, account):
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
   
     
     
     
     
                            
    @staticmethod
    def isJournalEntry(dt, kt):
        a = re.match('^[0-9]', str(dt)) != None
        b = re.match('^[0-9]', str(kt)) != None
        return a and b
    
    
class Postprocessor:
     
    @staticmethod        
    def toFloat(item):
        #print(item)
        if isinstance(item, str):
            item = item.replace(u'\xa0', u'')
            item = item.replace(u',', u'.')
            if not item:
                item = "0"
        
        return float(item)  

    @staticmethod
    def correctSpace(item):
        if isinstance(item, str):
            return item.replace(u'\xa0', u' ')   
    
    @staticmethod
    def changeDataType(df, *cols):
        lengthOfCols = len(cols)
        begin = df.columns.get_loc(cols[0])
        end = -1
    
        if lengthOfCols == 1:        
            end = len(df.columns)
        elif lengthOfCols == 2:
            end = df.columns.get_loc(cols[1]) + 1
    
        if end != -1:
            for i in range(begin, end):
                df[df.columns[i]].fillna(value = "", inplace=True)
        else:
            for j in range(0, len(cols)):
                df[cols[j]].fillna(value = "", inplace=True)       
                
                
    

