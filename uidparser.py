#!/usr/bin/python

import csv,time,couchdb
import _mysql
#from mysql.connector import errorcode
import sys,hashlib,json
import logging
import logging.config
import Geohash #,geohash
from collections import namedtuple

logging.basicConfig(filename='uidparser.log',level=logging.DEBUG)

logger = logging.getLogger(__name__)

logging.config.dictConfig({
    'version': 1,              
    'disable_existing_loggers': False,  # this fixes the problem

    'formatters': {
        'standard': {
            'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
        },
    },
    'handlers': {
        'default': {
            'filename':'uidparser.log',
            'level':'INFO',    
            'class':'logging.FileHandler',
        },  
    },
    'loggers': {
        '': {                  
            'handlers': ['default'],        
            'level': 'INFO',  
            'propagate': True  
        }
    }
})

#Row = namedtuple('Row','import_stamp,time_stamp,customer_id,action_name,action_category,event_name,domain_url,domain_page_full_url,domain_page_relative_url,domain_page_query_string,sub_domain,domain_host,referal_page_url,page_load,context_name,custom_context_value,custom_referal_page,custom_referal_page_host,authtype,domain_user_id,domain_user_email,facebook_user_id,twitter_user_id,session_id,user_ipaddr,lang,user_agent,device_type,mobile_device,browser_type,browser_version,os_type,os_version,geo_dim_id,city_code,latitude,longitude,is_new_visitor,is_new_visit,session_count,campaign_id,campaign_name,campaign_start_date,campaign_end_date,campaign_ad_channel,campaign_budget,campaign_goal_name,campaign_goal_value,campaign_customer_id,campaign_domain_url,product_customer_id,product_customer_name,product_page_relative_url,product_domain_url,product_id,product_sku,product_name,product_category,product_price,product_discount,product_amount,order_id,order_value,order_discount,shortener_group_base35,shortener_item_base35,channel_type,channel_text,process_minute,process_day')
Row = namedtuple('Row','import_stamp,time_stamp,customer_id,action_name,action_category,event_name,domain_url,domain_page_full_url,domain_page_relative_url,domain_page_relative_path,domain_page_query_string,domain_page_fragment,sub_domain,domain_host,referal_page_url,page_load,context_name,custom_context_value,custom_referal_page,custom_referal_page_host,authtype,domain_user_id,domain_user_email,facebook_user_id,twitter_user_id,session_id,user_ipaddr,lang,user_agent,device_type,mobile_device,browser_type,browser_version,os_type,os_version,geo_dim_id,city_code,latitude,longitude,is_new_visitor,session_count,is_new_visit,is_landing,campaign_id,campaign_name,campaign_start_date,campaign_end_date,campaign_ad_channel,campaign_budget,campaign_goal_name,campaign_goal_value,campaign_customer_id,campaign_domain_url,product_customer_id,product_customer_name,product_page_relative_url,product_domain_url,product_id,product_sku,product_name,product_category,product_price,product_discount,product_amount,order_id,order_value,order_discount,shortener_group_base35,shortener_item_base35,channel_type,channel_name,channel_text,process_minute,process_day')
#
# ini
#
#couch = couchdb.Server('http://localhost:5984/')
cnx = _mysql.connect(user='root',passwd='root',db='fractal_v1')
#cursor = cnx.cursor()
#DB_NAME='fractal_v1'


#try:
#cnx.database = DB_NAME    
#except mysql.connector.Error as err:
#    if err.errno == errorcode.ER_BAD_DB_ERROR:
#        #do something
#        print "Err:" + err
#    else:
#        print(err)
#        exit(1)


##db=couch.create('test')  ## if new
#db=couch['fractaluser']

logger.info("Accessessing fractaluser and updating from file:" + sys.argv[1])

numrow=0
#add_row = ("INSERT INTO fractaluser (hash_uid,guid,session,campaign_id) VALUES (%s,%s,%s,%S)" )

with open(sys.argv[1], "r") as f:
    next(f)
    rows = csv.reader(f, delimiter="\t")
    for i in rows:
        #print numrow
        r = Row._make(i[0:])
        uidHashVal=hashlib.sha1(i[26] + i[28] + i[33] + i[34])
        try:
            pattern = '%Y-%m-%d %H:%M:%S'
            epoch = str(int(time.mktime(time.strptime(i[1], pattern))))
        except ValueError:
            logger.error("Failed to parse time_stamp: " + i[1])
            continue
        citycode=i[36]
        if(citycode==""): 
           citycode="0"
        try:
           geohashRef=Geohash.encode(float(i[37]),float(i[39]))
        except ValueError:
           logger.warn("Failed to encode lat/long: " + str(i[37]) + ":" + str(i[39]))
           geohashRef='zzzzz'
        hex_dig = uidHashVal.hexdigest()
        data_row=(hex_dig, i[19], i[23], str(i[40]))
        cnx.query("INSERT INTO fractaluser2 (hash_uid,guid,session,campaign_id) VALUES ('" + hex_dig + "','" + i[21] + "','" + i[25] + "','" + i[43] + "')")




#        doc = { "_id":hex_dig,"fingerprint": i[24] + i[27] + i[31] + i[32] ,"data":{"campaigns":[{"campaign_id": i[40] ,"ts": epoch }],"guids":[{"domain_user_id": i[19],"ts": epoch }] , "sessions": [{"session_id": i[23], "ts": epoch }] ,"geoRef": geohashRef,"segments":[{"segment_0001": citycode, "ts": epoch}]  } }
#        olddoc = {'_rev':''}
#        olddoc = db.get(hex_dig)
#        if olddoc:
#             numg=1
#             nums=1
#             numc=1
#             numseg=1
#             for g in olddoc["data"]["guids"]:
#                  #numg=1
#                  doc["data"]["guids"].insert(numg, g)
#                  logger.debug("Added " + str(numg) + " guids to user: " + doc["_id"])
#                  numg+=1
#             for s in olddoc["data"]["sessions"]:
#                  #nums=1
#                  doc["data"]["sessions"].insert(nums, s)
#                  logger.debug("Added " + str(nums) + " sessions to user: " + doc["_id"])
#                  nums+=1
#             for c in olddoc["data"]["campaigns"]:
#                  #numc=1
#                  doc["data"]["campaigns"].insert(numc, c)
#		  logger.debug("Added " + str(numc) + " campaigns to user: " + doc["_id"])
#                  numc+=1
#             for seg in olddoc["data"]["segments"]:
#                  #numseg=1
#                  doc["data"]["segments"].insert(numseg, seg)
#                  logger.debug("Added " + str(numseg) + " segments to user: " + doc["_id"])
#                  numseg+=1
#             doc["_rev"] = olddoc["_rev"]
#             logger.debug("Updated: " + doc["_id"] + " rev: " + doc["_rev"])
#        db.save(doc)
        numrow+=1
