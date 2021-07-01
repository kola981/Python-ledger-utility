'''
Created on Jun 11, 2019

@author: ak981
'''

from transform.io import FileHandler 
from transform.processor import Processor

'''
Variables
'''

#path to directory
mypath = r'/media/data/out'

#name of file
journal = r'Journal_2021.xlsx' # 
chartOfAcc = r'План рахунків бухгалтерського обліку.xls'
processedJournal = 'preprocessed_db_inline_2021_1.xlsx'

#name of sheet
sheetToTransform = "TDSheet" #TDSheet
sheetWithCoA = "TDSheet" #TDSheet

#fh.configure()
fh = FileHandler()
fh.setOutDir(mypath)
fh.setInDir(mypath)
fh.setJournal(journal)
fh.setJournalSheet(sheetToTransform)
fh.setCoA(chartOfAcc)
fh.setChartSheet(sheetWithCoA)
fh.setProcessedJournal(processedJournal)

df = fh.readJournal()
df2 = fh.readChartOfAccounts()

pr = Processor()

#print(df2.head(5))
df3 = pr.process(df, df2, "1C8.3inline") #"1C8.3inline"
fh.writeJournal(df3)
#print("----")
#print(df3.head(5))
