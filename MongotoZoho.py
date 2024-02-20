from pymongo  import MongoClient
import pandas as pd
from AnalyticsClient import AnalyticsClient
## Sample Design
client=MongoClient('mongodb://BijnisLiveMongoReader:XUerT9JxnWnEstSA@prod-mongo.bijnis.internal:27010/?authSource=admin&readPreference=primary&directConnection=true&ssl=false')
db=client['product_service_production']
collection=db['sample_design']
data=list(collection.find())
df=pd.json_normalize(data)
df=df.rename(columns={'_id':'designId'})
df['designId']=df['designId'].astype(str)
df['designId'] = df['designId'].apply(lambda x: x.strip())
df['designId'] = df['designId'].apply(lambda x: x.replace('\n', ''))


## Design requirements
db=client['product_service_production']
collection=db['design_requirement']
data1=list(collection.find())
df1=pd.json_normalize(data1)
df1 = df1.explode('designs').reset_index(drop=True)
df1.head()
final_df=df1.merge(df,how='left',left_on='designId',right_on='designs',suffixes=('_sample','_requirement'))
final_df=final_df.fillna(0)
final_df['sourcingFactoryId']=final_df['sourcingFactoryId'].astype(int)
final_df['possibleSize'] = final_df['possibleSize'].replace('[A-Za-z]+', 0, regex=True)
final_df.to_csv('Mongo_flatten.csv',index=False)

## Importing Data to MONGO
def import_data(path):
    class Config:
        CLIENTID = "1000.DQ32DWGNGDO7CV0V1S1CB3QFRAI72K"
        CLIENTSECRET = "92dfbbbe8c2743295e9331286d90da900375b2b66c"
        REFRESHTOKEN = "1000.0cd324af15278b51d3fc85ed80ca5c04.7f4492eb09c6ae494a728cd9213b53ce"
        ORGID = "60006357703"
        WORKSPACEID = "174857000004732522"
        VIEWID = "174857000084552645"

    class Sample:
        def __init__(self):
            self.ac = AnalyticsClient(Config.CLIENTID, Config.CLIENTSECRET, Config.REFRESHTOKEN)

        def import_data(self, file_path):
            import_type = "truncateadd"
            file_type = "csv"
            auto_identify = "true"
            bulk = self.ac.get_bulk_instance(Config.ORGID, Config.WORKSPACEID)
            try:
                result = bulk.import_data(Config.VIEWID, import_type, file_type, auto_identify, file_path)
                print(result)
            except Exception as e:
                print(f"Error importing data: {e}")

    try:
        obj = Sample()
        obj.import_data(path)  # Pass the path of the CSV file to the function
    except Exception as e:
        print(f"Error creating Sample instance: {e}")

# Use the path of the CSV file you saved earlier
import_data('Mongo_flatten.csv')
