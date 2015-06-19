import csv,time
import sys
from collections import namedtuple

Row = namedtuple('Row', 'import_stamp,time_stamp,customer_id,action_name,action_category,event_name,domain_url,domain_page_full_url,domain_page_relative_url,domain_page_query_string,sub_domain,domain_host,referal_page_url,page_load,context_name,custom_context_value,custom_referal_page,custom_referal_page_host,authtype,domain_user_id,domain_user_email,facebook_user_id,twitter_user_id,session_id,user_ipaddr,lang,user_agent,device_type,mobile_device,browser_type,browser_version,os_type,os_version,geo_dim_id,city_code,latitude,longitude,is_new_visitor,is_new_visit,session_count,campaign_id,campaign_name,campaign_start_date,campaign_end_date,campaign_ad_channel,campaign_budget,campaign_goal_name,campaign_goal_value,campaign_customer_id,campaign_domain_url,product_customer_id,product_customer_name,product_page_relative_url,product_domain_url,product_id,product_sku,product_name,product_category,product_price,product_discount,product_amount,order_id,order_value,order_discount,shortener_group_base35,shortener_item_base35,channel_type,channel_text,process_minute,process_day')

fout = open(sys.argv[2], "w")

def write_out(row, event, duration):
    # if row.app != "unknown":
    # print "\t".join([str(x) for x in row]), "\t", event, "\t", duration
    fout.write("\t".join([str(x) for x in row[0:-1]]) + "\t" + event + "\t" + str(duration) + "\t" + row[-1] + "\n")

#def close_last():
#    write_out(state['previous_row'], "close", state['previous_row'].ts - state['ts_open'])
#    return

def parse_row(row):
 dd   if state['previous_row'] is None:
        write_out(row, "open", 0)
        state['previous_row'] = row
        state['ts_open'] = row.ts
        return

    if state['previous_row'].user != row.user:
        write_out(state['previous_row'], 'close', state['previous_row'].ts - state['ts_open'])
        write_out(row, 'open', 0)
        state['ts_open'] = row.ts
        state['previous_row'] = row
        return

    if state['previous_row'].app != row.app:
        if (row.ts - state['previous_row'].ts) > SESSION_THRESHOLD_DURATION_SECONDS:
            prev = state['previous_row']
            prepared_row = Row(prev.ts + SESSION_THRESHOLD_DURATION_SECONDS, prev.user, prev.ip, prev.hour, prev.app, prev.host, prev.date)
            write_out(prepared_row, 'close', SESSION_THRESHOLD_DURATION_SECONDS)
        else:
            prev = state['previous_row']
            prepared_row = Row(row.ts, prev.user, prev.ip, prev.hour, prev.app, prev.host, prev.date)
            write_out(prepared_row, 'close', row.ts - state['ts_open'])

        write_out(row, 'open', 0)
        state['ts_open'] = row.ts
        state['previous_row'] = row
        return

    write_out(row, 'running', 0)
    state['previous_row'] = row
    return


with open(sys.argv[1], "r") as f:
    rows = csv.reader(f, delimiter="\t")
    for i in rows:
        r = Row._make([float(time.mktime(time.strptime(i[0], '%Y-%m-%dT%H:%M:%S.%fZ')))] + i[1:])
        parse_row(r)
    close_last()

fout.close()
