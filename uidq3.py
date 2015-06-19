#!/usr/bin/python
import csv,time,couchdb
import sys,hashlib,json
import Geohash #,geohash
from collections import namedtuple

#Row = namedtuple('Row','import_stamp,time_stamp,customer_id,action_name,action_category,event_name,domain_url,domain_page_full_url,domain_page_relative_url,domain_page_query_string,sub_domain,domain_host,referal_page_url,page_load,context_name,custom_context_value,custom_referal_page,custom_referal_page_host,authtype,domain_user_id,domain_user_email,facebook_user_id,twitter_user_id,session_id,user_ipaddr,lang,user_agent,device_type,mobile_device,browser_type,browser_version,os_type,os_version,geo_dim_id,city_code,latitude,longitude,is_new_visitor,is_new_visit,session_count,campaign_id,campaign_name,campaign_start_date,campaign_end_date,campaign_ad_channel,campaign_budget,campaign_goal_name,campaign_goal_value,campaign_customer_id,campaign_domain_url,product_customer_id,product_customer_name,product_page_relative_url,product_domain_url,product_id,product_sku,product_name,product_category,product_price,product_discount,product_amount,order_id,order_value,order_discount,shortener_group_base35,shortener_item_base35,channel_type,channel_text,process_minute,process_day')



couch = couchdb.Server('http://localhost:5984/')
db2=couch['fractaluser']
db1=couch['fractalcampaigntarget']

map_fun1='''function(doc) { if ( doc.campaign_id.length>0 )  emit(doc.campaign_id, doc.audience_target_segments); }''' 

#map_fun='''function(doc) { if (doc.fingerprint.length>0)  emit(doc._id, null); }''' #== '74.138.24.64SmartphoneiOS8.0.2')
#map_fun='''function(doc) {  for (var campaign in doc.data.campaigns) { emit(doc._id.concat(":",doc.data.campaigns[campaign].campaign_id,":",doc.data.campaigns[campaign].ts));  } }  '''
#map_fun='''function(doc) {  for (var g in doc.data.guids) { emit(doc._id.concat(":",doc.data.guids[g].domain_user_id,":",doc.data.guids[g].ts));  } }  '''
#map_fun='''function(doc) {  for (var s in doc.data.sessions) { emit(doc._id.concat(":",doc.data.sessions[s].session_id,":",doc.data.guids[s].ts));  } }  '''

for row1 in db1.query(map_fun1):
     print row1.key,row1.value
     #funcstr=
     map_fun2="function(doc) { if ( doc.campaign_id.match(/" + str(row1.key) + "/) )  emit(doc._id,\"" + str(row1.key) + ":" + str(row1.value) + "\"); }" 
     print "Mapfn2: " + map_fun2
     for row2 in db2.query(map_fun2):
          print row2.key,row2.value    
     

 
