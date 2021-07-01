'''
Created on Dec 29, 2020

@author: ak981
'''
from app.transform.data import Data
from app.transform.shape import Reshaper, ChartReshaper, InlineJournalReshaper,\
    SimpleJournalReshaper
from app.transform.transform import Transformer
from app.transform.chartofacc import createDictOfAcc
from app.transform import constants
from app.transform.utilities import mergeColumnNames
from pandas.core.frame import DataFrame

class Processor:
    
    def __init__(self):
        self._data = Data()
        self._configurator = Configurator(self._data)
    
    def process(self, journal, chart, name):
        self._configurator.configure(journal, chart)
        chart_reshaper = self._configurator.getChartReshaper()
        
        chart = chart_reshaper.preprocessTable(chart)
        dict_of_acc = createDictOfAcc(chart)
        self._data.setChartOfAccounts(dict_of_acc)
        
        journal_reshaper = self._configurator.getJournalReshaper(name)
        journal = journal_reshaper.preprocessTable(journal)
            
        print(">>{}\n{}".format(journal.columns.tolist(),dict_of_acc))
        col_names = mergeColumnNames( journal.columns.tolist(), dict_of_acc)
        self._data.setHeader(col_names)
        
        transformer = self._configurator.getTransformer()
        journal = transformer.addDimensionalData(journal)
        print("dimensional data added")
        #journal.head(100).to_excel(r'/media/ak981/ITProjects/data/BTU/1C Data/je4_.xlsx', sheet_name='db')
        
        journal = transformer.postProcessData(journal)
        print(journal.shape)
        print("Processing finished")
            
        return journal

    
class Configurator:
    ROWS = 15
    
    
    def __init__(self, data):
        self._data = data
        self._journal = None
        self._chart = None
        self._code = None
        self._header_col = None
        self._transact_col = None
        self._dt = None
        self._kt = None
        self._sub_dt = None
        self._sub_kt = None
        self._doc = None
        self._amount = None
        self._rev = None 
        self._cost = None
    
    
    def configure(self, journal, chart):
        if not isinstance(journal, DataFrame) or not isinstance(chart, DataFrame):
            raise Exception("journal {} or chart {} is absent".format(not isinstance(self._journal, DataFrame), not isinstance(self._chart, DataFrame)))
        self._journal = journal
        self._chart = chart
        self._configureVariables()
     
    def _configureVariables(self):
        header = self._findHeader()
        self._code = self._updateVariable(self._code, constants.CODE, header)
        self._header_col = self._updateVariable(self._header_col, constants.HEADER_COLUMN, header)
        self._transact_col = self._updateVariable(self._transact_col, constants.TRANSACTION_COLUMN, header)
        self._dt = self._updateVariable(self._dt, constants.DT, header)
        self._kt = self._updateVariable(self._kt, constants.KT, header)
        self._sub_dt = self._updateVariable(self._sub_dt, constants.ANALYTICS_DT, header)
        self._sub_kt = self._updateVariable(self._sub_kt, constants.ANALYTICS_KT, header)
        self._doc = self._updateVariable(self._doc, constants.DOCUMENT, header)
        self._amount = self._updateVariable(self._amount, constants.AMOUNT, header)
        self._rev = self._updateVariable(self._rev, constants.REVENUE, header)
        self._cost = self._updateVariable(self._cost, constants.COST, header)
         
            
    def _findHeader(self):
        header = self._data.getHeader()
        if not header:
            header = self._findHeaderData(self._journal)
            header.update(self._findHeaderData(self._chart))
        return header
            
    def _findHeaderData(self, df):
        data = df.head(self.ROWS)
        return set ([data.iat[row, col] for row in data.index for col in data.columns])
    
    def _updateVariable(self, variable, values, set_of_data):
        if not variable:
            try:
                return self._findVariable(values, set_of_data)
            except Exception:
                pass
        else:
            return variable
    
    def _findVariable(self, column_names, _set_of_data):
        for name in column_names:
            if name in _set_of_data:
                return name
        raise Exception("No match for column names {}".format(column_names))
    
    
    def getChartReshaper(self):
        return self._getReshaper(ChartReshaper())
    
    
    def getJournalReshaper(self, name):
        if not self._transact_col:
            raise Exception("Variable not set")
        if name == "1C7v2":
           pass 
        elif name == "1C8.3":
            reshaper = SimpleJournalReshaper()
        elif name == "1C8.3inline":
            reshaper = InlineJournalReshaper(
                                    code=self._code,
                                    header_col = self._header_col,
                                    transact_col = self._transact_col,
                                    dt=self._dt,
                                    kt= self._kt,
            )
        return self._getReshaper(reshaper)

    def _getReshaper(self, reshaper_type):
        return Reshaper(self._journal, self._chart, reshaper_type)
    
    
    def getTransformer(self):
        print("Value set to {}".format(self._amount))
        self._configureVariables()
        print("Value set to {}".format(self._amount))
        return Transformer(self._journal, self._chart, self._data,
                                         dt = self._dt,
                                         kt = self._kt,
                                         sub_dt = self._sub_dt,
                                         sub_kt = self._sub_kt,
                                         doc = self._doc,
                                         amount = self._amount,
                                         rev = self._rev,
                                         cost = self._cost,
        )
        