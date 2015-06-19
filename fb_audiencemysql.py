#!/usr/bin/python
#-*- coding: utf-8 -*-

import csv,time,couchdb
import _mysql
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

Row = namedtuple('Row','id,creatives_name,creatives_type,creatives_object_id,creatives_body,creatives_image_hash,creatives_image_url,creatives_title,creatives_link_url,creatives_url_tags,creatives_preview_url,creatives_related_fan_page,creatives_follow_redirect,creatives_auto_update,creatives_story_id,creatives_action_specs,creatives_description,object_store_url,actor_name,actor_image_hash,video_id,actor_id,call_to_action_type,link_deep_link_url,placement_type,placement_name,placement_location,ad_set_id,user_campaign_status,creative_id,bid_type,hash_id,ad_group_id,targeting_specs,name,campaign_parent_id,page_types,bid,lifetime_budget,budget_remaining,tracking_specs,optimization_status,start_time,end_time,creatives_video_description,creatives_video_call_to_action,campaign_id,ad_combo_id,ui_targeting_specs,ad_creative_id,creatives_object_url,factor')


#
# ini
#
#couch = couchdb.Server('http://localhost:5984/')
cnx = _mysql.connect(user='root',passwd='root',db='fractal_v1')
##db=couch.create('test')  ## if new
#db=couch['fractalcampaigntarget']

logger.info("Accessessing fractaluser and updating from file:" + sys.argv[1])

numrow=0
with open(sys.argv[1], "r") as f:
    next(f)
    rows = csv.reader(f, delimiter="\t")
    for i in rows:
        r = Row._make(i[0:])  #enforce tuple
        #uistr=i[48].replace('\\','')
        #uistr=uistr.replace("\'","\"")
        #uistr=uistr.replace('"{','{')
        #uistr=uistr.replace('}"','}')
        #uistr=uistr.replace('\xe2\x80\x93','')
        #uistr=uistr.replace('\xe9','')
        #uistr=uistr.replace('\x93','')
        #uistr=uistr.replace('\u2013','')
        #uistr=uistr.decode('utf-8') #u"{}".format(uistr)
        # uistr=i[48].(/[\u0080-\uC350]/g, 
        #               function(a) {
        #                 return '&#'+a.charCodeAt(0)+';';
        #               }
        #replace("\\","")
        #uistr = uistr.replace("'","")      
        #uistr=uistr.replace(/[\u0080-\uC350]/g, 
        #              function(a) {
        #                return '&#'+a.charCodeAt(0)+';';
        #              }
        #uistr=uistr.replace('"{','{')
        #uistr=uistr.replace('}"','}')

#        # fix string has extra escapement slashes and a leading double quote around a hash
        #audiencestr=i[48].replace('\\','')
        #audiencestr=audiencestr.replace('\u2013','')
        #audiencestr=audiencestr.replace('"{','{')
        #audiencestr=audiencestr.replace('}"','}')
        
        cnx.query("INSERT INTO fractalaudience2 (campaign_id,category,ui_target) VALUES ('" + str(i[31]) + "','','" + i[48].replace("'","") + "');")  
        #aud=json.loads(json.loads(json.dumps(audiencestr)))
        #numsubsegment=0
        #for category in aud["audience"]:
        #     #print "category:" + category
        #     numsubsegment=0
        #     for catsegments in aud["audience"][category]:
        #         #audiencedata["audience_target_segments"].insert(numsubsegment,{"segment": category,"data":[{}]})
        #         item={}
        #         for key in aud["audience"][category][numsubsegment]["subSegment"]:
        #             keycnt=0
        #             print "key: " + key
                     #item[key]=str(aud["audience"][category][numsubsegment]["subSegment"][key]).replace('\u2013','').encode('utf-8','replace')
                     #try:
                     #    print "Str: " + "num: " + str(numsubsegment) + " : " +  str(aud["audience"][category][numsubsegment]["subSegment"][key]) 
                     #    #audiencedata["audience_target_segments"][numsubsegment]["data"].insert(keycnt, { key: str(aud["audience"][category][numsubsegment]["subSegment"][key]) } ) #.encode('utf8')}) 
                     #except UnicodeEncodeError:
                     #   print aud["audience"][category][numsubsegment]["subSegment"][key].encode('utf8') 
                     #    
        #             keycnt+=1
                 #cnx.query("INSERT INTO fractalaudience2 (campaign_id,category,keylabel,valuelabel,typelabel,ui_target) VALUES ('" + str(i[31]) + "','" + category + "','null','" + str(item["value"].replace("'","''"))  +"','null','foo');")  
                 #try:
                 #print "hid: " + str(i[31])
                 #print "cat: " + category
                 #print "uistr: " + uistr
        #         cnx.query("INSERT INTO fractalaudience2 (campaign_id,category,ui_target) VALUES ('" + str(i[31]) + "','" + category + "','" +  uistr + "');")  
                 #except UnicodeEncodeError: 
                 #print "Err: " + i[48]
        #         numsubsegment+=1
    
#        db.save(audiencedata)
        
        #olddoc = {'_rev':''}
        #olddoc = db.get(hex_dig)
        #if olddoc:
             # numg=1
             # nums=1
             # numc=1
             # numseg=1
             # for g in olddoc["data"]["guids"]:
             #      #numg=1
             #      doc["data"]["guids"].insert(numg, g)
             #     logger.debug("Added " + str(numg) + " guids to user: " + doc["_id"])
             #     numg+=1
             #for s in olddoc["data"]["sessions"]:
             #     #nums=1
             #     doc["data"]["sessions"].insert(nums, s)
             #     logger.debug("Added " + str(nums) + " sessions to user: " + doc["_id"])
             #     nums+=1
             #for c in olddoc["data"]["campaigns"]:
             #     #numc=1
             #     doc["data"]["campaigns"].insert(numc, c)
             #     logger.debug("Added " + str(numc) + " campaigns to user: " + doc["_id"])
             #     numc+=1
             #for seg in olddoc["data"]["segments"]:
             #     #numseg=1
             #     doc["data"]["segments"].insert(numseg, seg)
             #     logger.debug("Added " + str(numseg) + " segments to user: " + doc["_id"])
             #     numseg+=1
             #doc["_rev"] = olddoc["_rev"]
             #logger.debug("Updated: " + doc["_id"] + " rev: " + doc["_rev"])
        #db.save(audiencedata)
        # numrow+=1
