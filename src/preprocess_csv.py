import pandas as pd
#you can use the output to plot a timeline
def callPreprocessData(fn='/home/sscd/Downloads/infostealer_flows.csv'):
    data=pd.read_csv(fn)
    print(data.head(3))
    data['Bytes']=data['Bytes'].fillna(0)
    data['Bytes']=data['Bytes'].astype(str)
    #silly nfdump writes bytes as integer but sometimes it converts it to
    #mb. The following finds those entries and converts them into bytes.
    data.loc[data['Bytes'].str.contains('M'),'Bytes'] =   data[data['Bytes'].str.contains('M')]['Bytes'].apply(lambda x: float(x[1:-2])*1024*1024)
    data['Bytes']=data['Bytes'].astype(int)
    data.dropna(subset=['Datefirstseen'],inplace=True)
    data['Datefirstseen']=pd.to_datetime(data['Datefirstseen'])
    data=data.set_index('Datefirstseen')
    
    data['Duration']=data['Duration'].astype(int)
    data['DstPt']=data['DstPt'].astype(int)
    data['Datefirstseenunix']=data['Datefirstseenunix'].astype(int)

    #remove all white spaces from all columns:
    data['SrcIPAddr']=data['SrcIPAddr'].astype(str).str.strip()
    data['DstIPAddr']=data['DstIPAddr'].astype(str).str.strip()
    data['Proto']=data['Proto'].astype(str).str.strip()

    print('I am now saving the file')
    print(data.head(3))

    data.to_csv('infostealer_flows_processed.csv')
