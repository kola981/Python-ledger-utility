'''
Created on Dec 29, 2020

@author: ak981
'''
import os.path as p
import pandas as pd

class FileHandler:
    '''
    classdocs
    '''
    def __init__(self, out_folder="", in_folder="", journal="",  chart_of_acc="", pr_journal = "", jsheet="", coasheet=""):
        self.out_folder = out_folder
        self.in_folder = in_folder
        self.journal = journal
        self.chart_of_acc = chart_of_acc
        self.pr_journal = pr_journal
        self.jsheet = jsheet
        self.coasheet = coasheet
        
    
        
    def readJournal(self, header=None):
        path = p.join(self.in_folder, self.journal)
        df = self._readFile(path, self.jsheet, header)
        #self.writeJournal(df.head(10))
        return df
   
        
    def readChartOfAccounts(self, header=None):
        path = p.join(self.in_folder, self.chart_of_acc)
        df = self._readFile(path, self.coasheet, header)
        #self.writeJournal(df.head(10))
        return df
   
    
    def writeJournal(self, df):
        path = p.join(self.out_folder, self.pr_journal)
        if not p.exists(self.out_folder):
            raise Exception("File or folder {} does not exist".format(path))
        df.to_excel(path, sheet_name='db')

     
    def _readFile(self, path, sheet, head):
        if not p.exists(path):
            raise Exception("File or folder {} does not exist".format(path))
        if head:
            df = pd.read_excel(path, sheet_name=sheet, header=head)
        else:
            df = pd.read_excel(path, sheet_name=sheet, header=None)
            
        return df
            
    
    def setOutDir(self, path):
        if p.exists(path):
            self.out_folder = path
    
    def setInDir(self, path):
        if p.exists(path):
            self.in_folder = path
    
    def setJournal(self, file):
        self.journal = file
            
    def setCoA(self, file):
        self.chart_of_acc = file
            
    def setProcessedJournal(self, file):
        self.pr_journal = file
        
    def setJournalSheet(self, sh_name):
        self.jsheet = sh_name
        
    def setChartSheet(self, sh_name):
        self.coasheet = sh_name