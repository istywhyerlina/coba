from playwright.sync_api import sync_playwright
import re
import math
import webbrowser
import os
from bs4 import BeautifulSoup
import pandas as pd
from re import sub
from decimal import Decimal
from sqlalchemy import *
import json
from datetime import datetime
from datetime import timezone
import numpy as np

def extract_salesdata(db_url,extract_path):
    engine = create_engine(db_url, future=True) 
    conn = engine.connect() 
    metadata = MetaData(bind=None)
    table = Table(
    'amazon_sales_data', 
    metadata, 
    autoload=True, 
    autoload_with=engine
    )
    mapped_object = inspect(table)
    mapped_object.columns.items()
    stmt = select([
    table.columns.name,
    table.columns.main_category,
    table.columns.sub_category,
    table.columns.image,
    table.columns.link,
    table.columns.ratings,
    table.columns.no_of_ratings,
    table.columns.discount_price,
    table.columns.actual_price,
    ])

    connection = engine.connect()
    results = connection.execute(stmt).fetchall()
    sales_data=pd.DataFrame(results)
    sales_data.to_csv(extract_path, sep=';',index=False)



# extract_path=f'C:/Users/istyw/Downloads/Final Project/raw/amazon_sales_data.csv' 
# db_url="postgresql://postgres:password123@localhost:5439/etl_db"
# extract_salesdata(db_url,extract_path)




def scraping(keyword,html_path):
    keyword = sub(' ','-', keyword)
    p = sync_playwright().start()
    browser=p.chromium.launch(headless=False)
    page=browser.new_page()
    page.goto(f"https://www.olx.co.id/items/q-{keyword}")
    page.wait_for_timeout(3000)
    page.get_by_placeholder('Cari kota, area, atau lokalitas').fill('Indonesia')
    page.locator('xpath=/html/body/div[1]/div/header/div/div/div[2]/div/div/div[1]/div/div[2]/div/div/div').click()
    page.wait_for_timeout(3000)
    #page.pause()
    #to get number of ads listings
    total_listing= page.locator('xpath=/html/body/div[1]/div/main/div/div/section/div/div/div[4]/div[2]/div/div[1]/div[1]/div/p/span[2]').inner_text()
    total_listing=int(re.findall(r'\d+',total_listing)[0])
    #page.locator('xpath=/html/body/div/div/main/div/div/section/div/div/div[4]/div[2]/div/div[4]/ul/li[42]/div/button').click()
    #page.locator('button:text("muat lainnya")').click()
    for i in range(math.ceil(total_listing/20)-1):
        try:
            page.get_by_role("button", name="muat lainnya").click()
            #page.locator('button:text("muat lainnya")')
            #page.locator('xpath=/html/body/div/div/main/div/div/section/div/div/div[4]/div[2]/div/div[4]/ul/li[42]/div/button').click()
        except:
            break
    page.wait_for_timeout(3000)
    html_content=page.content()
    #Func = open("GFG-1.html","w") 
    #Func.write(page.content())
    #Func.close()
    with open(html_path, "w+", encoding="utf-8") as f:
            full_html_articles = page.content()
            f.write(full_html_articles)
    page.close()




# keyword='sepatu puma'
# buat tes
# scraping(keyword,f'C:/Users/istyw/Downloads/Final Project/{keyword}.html')

def parsing(html_data,parsed_path):
    # to open/create a new html file in the write mode
    with open(html_data, encoding="utf-8") as file:
        soup = BeautifulSoup(file, 'html.parser')
    rawlink=soup.find_all("li",class_="_1DNjI")
    title=[]
    price=[]
    href_link=[]
    location=[]
    installment=[]
    posting_date=[]
    pdl=[]
    for link in rawlink:
        #print(link,"\n","\n")
        try:
            title.append(link.find("span",class_="_2poNJ").get_text())
        except:
            title.append('Title not found')
        
        try:
            price.append(link.find("span",class_="_2Ks63").get_text())
        except:
            price.append('Price not found')
            
        
        try:
            href_link.append(link.find("a",class_="").get("href"))
        except:
            href_link.append('href link not found')
        
        
        posting_date_location=(link.find("div",class_="_3rmDx").get_text(separator=" | "))
        pdl.append(posting_date_location)
        try:
            l,d=posting_date_location.split(' | ',1)
            posting_date.append(d)
            location.append(l)
        except:
            posting_date.append('Hari ini')
            location.append(posting_date_location)
            

        #print(link.find("div",class_="_3VRSm").find("span").get_text())
    # Calling DataFrame constructor after zipping
    # both lists, with columns specified
    df = pd.DataFrame(list(zip(title,price, href_link,location,posting_date)),
    columns =['title','price', 'url_listing','location','posting_date'])
    df.to_csv(parsed_path, sep=';',index=False)
    
# html_data=f'C:/Users/istyw/Downloads/Final Project/raw/{keyword}.html'    

# parsing(html_data,f'C:/Users/istyw/Downloads/Final Project/raw/{keyword}.csv')



def transform_olx(parsed_data,transformed_path):
    df=pd.read_csv(parsed_data,sep=';')

    for i in range(len(df)):
        #extract number price
        df.loc[i,'price']=int((sub(r'[^\d]', '',df['price'][i])))
        df[['kelurahan', 'kabupaten_kota']] = df['location'].str.split(', ', expand=True)
        date_format = '%d %b %Y'

        #change postingdate
        if df['posting_date'][i]=='Hari ini':
            df.loc[i,'posting_date']= str('01 Sep 2024')
        elif df['posting_date'][i]=='Kemarin':
            df.loc[i,'posting_date']= str('31 Aug 2024')
        elif df['posting_date'][i]=='2 hari yang lalu':
            df.loc[i,'posting_date']= str('30 Aug 2024')
        elif df['posting_date'][i]=='3 hari yang lalu':
            df.loc[i,'posting_date']= str('29 Aug 2024')
        elif df['posting_date'][i]=='4 hari yang lalu':
            df.loc[i,'posting_date']= str('28 Aug 2024')
        elif df['posting_date'][i]=='5 hari yang lalu':
            df.loc[i,'posting_date']= str('27 Aug 2024')
        elif df['posting_date'][i]=='6 hari yang lalu':
            df.loc[i,'posting_date']= str('26 Aug 2024')
        elif df['posting_date'][i]=='7 hari yang lalu':
            df.loc[i,'posting_date']= str('25 Aug 2024')
        else:
            df.loc[i,'posting_date']=((sub('Agu', 'Aug 2024',df['posting_date'][i])))
        df.loc[i,'posting_date'] = datetime.strptime(df['posting_date'][i], date_format)


            # df.loc[i,'posting_date']=df['posting_date'][i]
    #enrich the url link
    df['url_listing']='https://olx.co.id'+df['url_listing']



    df=df.drop('location',axis=1) #drop year_mileage


    df.to_csv(transformed_path,index=False,sep=';')

#buat tes
#transform_olx(f'C:/Users/istyw/Downloads/Final Project/raw/{keyword}.csv',f'C:/Users/istyw/Downloads/Final Project/result/{keyword}.csv')


def transform_salesdata(source,result):
    df=pd.read_csv(source , sep=';')
    df=df.fillna(np.nan).replace([np.nan], [None])
    df['discount_price']=df['discount_price'].astype(str)
    df['actual_price']=df['actual_price'].astype(str)

    for i in range(len(df)):
        df.loc[i,'discount_price']=(sub('₹','',(sub(',', '',df['discount_price'][i]))))
        df.loc[i,'actual_price']=(sub('₹','',(sub(',', '',df['actual_price'][i]))))
        #print (df["ratings"][i])


    df.to_csv(result, sep=';',index=False)




# source=f'C:/Users/istyw/Downloads/Final Project/raw/amazon_sales_data.csv'
# result=f'C:/Users/istyw/Downloads/Final Project/result/amazon_sales_data.csv' 
# transform_salesdata(source,result)

def transform_productdata(source,result):
    df=pd.read_csv(source,sep=',')
    df=df.drop(['sourceURLs','imageURLs',	'Unnamed: 26','Unnamed: 27','Unnamed: 28','Unnamed: 29','Unnamed: 30','ean',],axis=1)
    df=df[(df['id'].str.startswith('AV' or 'AW', na=False))]
    # for i in range(len(df)):
    #     df.loc[i,'dateAdded']=datetime.fromisoformat(df['dateAdded'][i][:-1]).astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    #     df.loc[i,'dateUpdated']=datetime.fromisoformat(df['dateUpdated'][i][:-1]).astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    df.to_csv(result , sep=';',index=False)

# source=f'C:/Users/istyw/Downloads/Final Project/raw/ElectronicsProductsPricingData.csv'
# result=f'C:/Users/istyw/Downloads/Final Project/result/ElectronicsProductsPricingData.csv'
# transform_productdata(source,result)




def load_olx(transformed_data,inserted_path,table_name,db_url):
    transformed = pd.read_csv(transformed_data,sep=';')
    transformed = transformed.to_dict(orient = "records")
    engine = create_engine(db_url, future=True) 
    conn = engine.connect() 
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    table = metadata_obj.tables.get(table_name)
    try:
        query = table.insert().values(transformed)
        conn.execute(query)
        conn.commit()
        conn.close()
        engine.dispose()
        with open(inserted_path, 'w') as fp:
            json.dump(transformed, fp)
    except:
        conn.rollback()
        conn.close()
        engine.dispose()
        raise Exception('RuntimeError')

# db_url="postgresql://postgres:pw1234@localhost:5440/datawarehouse"
# transformed_data=f'C:/Users/istyw/Downloads/Final Project/result/{keyword}.csv'
# table_name= 'olx_data'
# inserted_path=f'C:/Users/istyw/Downloads/Final Project/load/{keyword}.json'
# load_olx(transformed_data,inserted_path,table_name,db_url)

def load_productdata(transformed_data,inserted_path,table_name,db_url):
    transformed = pd.read_csv(transformed_data,sep=';')
    transformed = transformed.to_dict(orient = "records")
    engine = create_engine(db_url, future=True) 
    conn = engine.connect() 
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    table = metadata_obj.tables.get(table_name)
    try:
        query = table.insert().values(transformed)
        conn.execute(query)
        conn.commit()
        conn.close()
        engine.dispose()
        with open(inserted_path, 'w') as fp:
            json.dump(transformed, fp)
    except:
        conn.rollback()
        conn.close()
        engine.dispose()
        raise Exception('RuntimeError')

# db_url="postgresql://postgres:pw1234@localhost:5440/datawarehouse"
# transformed_data=f'C:/Users/istyw/Downloads/Final Project/result/ElectronicsProductsPricingData.csv'
# table_name= 'product_data'
# inserted_path=f'C:/Users/istyw/Downloads/Final Project/load/ElectronicsProductsPricingData.json'
# load_productdata(transformed_data,inserted_path,table_name,db_url)

def load_salesdata(transformed_data,inserted_path,table_name,db_url):
    transformed = pd.read_csv(transformed_data,sep=';')
    transformed = transformed.to_dict(orient = "records")
    engine = create_engine(db_url, future=True) 
    conn = engine.connect() 
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    table = metadata_obj.tables.get(table_name)
    try:
        query = table.insert().values(transformed)
        conn.execute(query)
        conn.commit()
        conn.close()
        engine.dispose()
        with open(inserted_path, 'w') as fp:
            json.dump(transformed, fp)
    except:
        conn.rollback()
        conn.close()
        engine.dispose()
        raise Exception('RuntimeError')

# db_url="postgresql://postgres:pw1234@localhost:5440/datawarehouse"
# transformed_data=f'C:/Users/istyw/Downloads/Final Project/result/amazon_sales_data.csv'
# table_name= 'sales_data'
# inserted_path=f'C:/Users/istyw/Downloads/Final Project/load/amazon_sales_data.json'
# load_salesdata(transformed_data,inserted_path,table_name,db_url)