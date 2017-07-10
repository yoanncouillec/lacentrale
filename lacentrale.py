#!/Users/couillec/anaconda/bin/python

import urllib2, json, sys, time, re, datetime
from elasticsearch import Elasticsearch
from bs4 import BeautifulSoup

baseurl="http://lacentrale.fr/"
es = Elasticsearch()

def count_ads(model_href):
    f = urllib2.urlopen (baseurl + model_href)
    html_doc = f.read()
    soup = BeautifulSoup (html_doc, 'html.parser')
    return int(soup.find(class_="numAnn").getText().replace(" ",""))

def get_ad():
    res = es.search(index="lacentrale_model", doc_type="model", body={"query": {"match_all": {}},"from":0,"size":2000},sort='marque_name:1,model_name:1',from_=98)
    for model in res['hits']['hits']:
        model_href = model['_source']['model_href']
        print "model_href="+model_href
        marque_name = model['_source']['marque_name']
        print "marque_name="+marque_name
        model_name = model['_source']['model_name']
        print "model_name="+model_name
        ad_num = count_ads(model_href)
        page_num = 1
        while ad_num > 0:
            if page_num == 1:
                f = urllib2.urlopen (baseurl + model_href)
            else:
                f = urllib2.urlopen (baseurl + model_href.split(".")[0]+"-"+str(page_num)+".html")
            html_doc = f.read ()
            soup = BeautifulSoup (html_doc, 'html.parser')
            lines = soup.find_all(class_="adLineContainer")
            for line in lines:
                for ad in line.find_all(class_="adContainer"):
                    print
                    print "["+marque_name+":"+model_name+"] "+str(ad_num)
                    try:
                        ad_href = ad.find('a').get('href')
                    except:
                        ad_href=None
                    print "ad="+ad_href
                    try:
                        ad_year=int(ad.find(class_="kmYearPrice").find(class_="fieldYear").getText())
                    except:
                        ad_year=None
                    print "ad_year="+str(ad_year)
                    try:
                        ad_km=int(re.sub('[^0-9]','',ad.find(class_="kmYearPrice").find(class_="fieldMileage").getText()).replace(" ",""))
                    except:
                        ad_km=None
                    print "ad_km="+str(ad_km)
                    try:
                        ad_price=int(re.sub('[^0-9]','',ad.find(class_="kmYearPrice").find(class_="fieldPrice").getText()).replace(" ",""))
                    except:
                        ad_price=None
                    print "ad_price="+str(ad_price)
                    try:
                        ad_dpt=ad.find(class_="kmYearPrice").find(class_="dptCont").getText().strip()
                    except:
                        ad_dpt=None
                    print "ad_dpt="+ad_dpt
                    try:
                        ad_version=ad.find(class_="version").getText().strip()
                    except:
                        ad_version=None
                    try:
                        print "ad_version="+ad_version
                    except:
                        pass
                    try:
                        ad_type_seller=ad.find(class_="typeSeller").getText().strip()
                    except:
                        ad_type_seller=None
                    print "ad_type_seller="+ad_type_seller
                    ad_timestamp=datetime.datetime.now()
                    print "ad_timestamp="+ad_timestamp.isoformat()
                    es.index(index='lacentrale_ad', doc_type='ad', body={"marque_name":marque_name, "model_name":model_name,"model_href":model_href,
                                                                         "ad_href":ad_href,"ad_year":int(ad_year),"ad_km":int(ad_km),"ad_price":int(ad_price),
                                                                         "ad_version":ad_version,"ad_type_seller":ad_type_seller,
                                                                         "timestamp":ad_timestamp})
                    ad_num = ad_num - 1
            page_num = page_num+1
            time.sleep(1)

get_ad()

def get_model():
    res = es.search(index="lacentrale_marque", doc_type="marque", body={"query": {"match_all": {}},"from":0,"size":1000})
    for marque in res['hits']['hits']:
        marque_href = marque['_source']['marque_href']
        marque_name = marque['_source']['marque_name']
        f = urllib2.urlopen (baseurl + marque_href)
        html_doc = f.read ()
        soup = BeautifulSoup (html_doc, 'html.parser')
        
        print marque_href
        
        lm = soup.find (class_="listModel")
        wraps = lm.find_all(class_="wrapListModel")
        for wrap in wraps:
            print "WRAP"
            print wrap
            for div in wrap.find_all("div"):
                a = div.find('a')
                if a == None:
                    continue
                print a
                model_href = a.get("href")
                model_name = model_href.split("-")[4].split(".")[0].replace("+"," ")
                print model_name, model_href
                es.index(index='lacentrale_model', doc_type='model', body={"marque_name":marque_name, "model_name":model_name,"model_href":model_href,
                                                                           "timestamp":datetime.datetime.now()})
                time.sleep(0.2)

#get_model()

def get_marque():
    f = urllib2.urlopen (baseurl + 'occasion-voiture.html')
    html_doc = f.read ()
    soup = BeautifulSoup (html_doc, 'html.parser')
    
    mW10 = soup.find (class_="ListeModeleMarque").find(class_="mW10")
    blocs = mW10.find_all(class_="modelBlocContent")
    for bloc in blocs:
        marques = bloc.find("ul")
        for marque in marques.find_all("li"):
            marque_href = marque.find("a").get("href")
            marque_name = marque_href.split("-")[3].split(".")[0].replace("+"," ")
            print marque_name +","+ marque_href
            es.index(index='lacentrale_marque', doc_type='marque', body={"marque_name":marque_name, "marque_href":marque_href,"timestamp":datetime.datetime.now()})

#get_marque()

