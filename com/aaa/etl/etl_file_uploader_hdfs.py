from com.aaa.etl.fred_hdfs import Fred2Hdfs

title_unemployee = 'Unemployment Rate in '

if __name__ == '__main__':
    fred = Fred2Hdfs()
    fred.clear_input_files('outputs', 'unemployee_annual.csv')
    df_list = fred.getListFredDF('A', title_unemployee)
    
    for i, df in enumerate(df_list):
        if i == 0:
            fred.writeCsv2Hdfs('unemployee_annual.csv', df)
        else:
            fred.appendCsv2Hdfs('unemployee_annual.csv', df)
            
        df.to_csv('outputs/unemployee_annual.csv', mode='a', index_label='date', header=(i==0))
        
    print('============= Unemployee Annual Done!')
            
            