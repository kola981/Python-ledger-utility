'''
Created on Jan 15, 2021

@author: ak981
'''

class Data:
    
    def __init__(self):
        self.chart_of_accounts = None
        self.transactions = None
        self.header = None
        #self.journal = None
        #self.chart = None
    
    def setChartOfAccounts(self, chart_of_accounts):
        self.chart_of_accounts = chart_of_accounts
    
    def setTransactions(self, transactions):
        self.transactions = transactions
    
    def setHeader(self, header):
        self.header = header
        
    def getChartOfAccounts(self):
        return self.chart_of_accounts
    
    def getTransactions(self):
        return self.transactions
    
    def getHeader(self):
        return self.header
