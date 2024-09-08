from playwright.sync_api import sync_playwright
import pandas as pd
import luigi
from luigi import Task
import engine 

class Extract_Salesdata(Task):
    db_url="postgresql://postgres:password123@localhost:5439/etl_db"
    extract_path=f'C:/Users/istyw/Downloads/Final Project/raw/amazon_sales_data.csv' 
    def requires(self):
        pass
    def output(self):
        return luigi.LocalTarget(self.extract_path)
    def  run(self):
        engine.extract_salesdata(self.db_url,self.output().path) 

class Extract_productdata(Task):
    source=f'C:/Users/istyw/Downloads/ElectronicsProductsPricingData.csv'
    extract_path=f'C:/Users/istyw/Downloads/Final Project/raw/ElectronicsProductsPricingData.csv' 
    def requires(self):
        pass
    def output(self):
        return luigi.LocalTarget(self.extract_path)
    def  run(self):
        df=pd.read_csv(source,sep=',')
        df.to_csv(extract_path, sep=',',index=False)

        

class Scrape(Task):
    keyword='sepatu puma'
    html_path=f'C:/Users/istyw/Downloads/Final Project/raw/{keyword}.html'
    def output(self):
        return luigi.LocalTarget(self.html_path)
    def  run(self):
        engine.scraping(self.keyword,self.output().path) 

class Parse(Task):
    parsed_path= f'C:/Users/istyw/Downloads/Final Project/raw/sepatu puma.csv'
    def requires(self):
        return (Scrape())
    def output(self):
        return luigi.LocalTarget(self.parsed_path)
    def run(self):
        engine.parsing(self.input().path,self.output().path)


class Transform(Task):
    transformed_path_olx= f'C:/Users/istyw/Downloads/Final Project/result/sepatu puma.csv'
    transformed_path_salesdata= f'C:/Users/istyw/Downloads/Final Project/result/amazon_sales_data.csv' 
    transformed_path_productdata= f'C:/Users/istyw/Downloads/Final Project/result/ElectronicsProductsPricingData.csv'
    source=f'C:/Users/istyw/Downloads/Final Project/raw/ElectronicsProductsPricingData.csv'

    def requires(self):
        return [Parse(),
                Extract_Salesdata(),
                Extract_productdata()]
    def output(self):
        return [luigi.LocalTarget(self.transformed_path_olx),
                luigi.LocalTarget(self.transformed_path_salesdata),
                luigi.LocalTarget(self.transformed_path_productdata)]
    def run(self):
        engine.transform_olx(self.input()[0].path,self.output()[0].path)
        engine.transform_salesdata(self.input()[1].path,self.output()[1].path)
        engine.transform_productdata(self.source,self.output()[2].path)

class Load(Task):
    inserted_path_olx=f'C:/Users/istyw/Downloads/Final Project/load/puma.json'
    table_name_olx= 'olx_data'
    inserted_path_salesdata=f'C:/Users/istyw/Downloads/Final Project/load/amazon_sales_data.json'
    table_name_salesdata= 'sales_data'
    inserted_path_productdata=f'C:/Users/istyw/Downloads/Final Project/load/ElectronicsProductsPricingData.json'
    table_name_productdata= 'product_data'
    db_url="postgresql://postgres:pw1234@localhost:5440/datawarehouse"
    def requires(self):
        return(Transform())
    def output(self):
        return [luigi.LocalTarget(self.inserted_path_olx),
                luigi.LocalTarget(self.inserted_path_salesdata),
                luigi.LocalTarget(self.inserted_path_productdata)]
    def run(self):
        engine.load_olx(self.input()[0].path,self.output()[0].path, self.table_name_olx, self.db_url )
        engine.load_salesdata(self.input()[1].path,self.output()[1].path, self.table_name_salesdata, self.db_url )
        engine.load_productdata(self.input()[2].path,self.output()[2].path, self.table_name_productdata, self.db_url )
