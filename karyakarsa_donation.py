import requests
import pandas as pd
import time
from tqdm import tqdm
from datetime import datetime
from google.cloud import bigquery
from google.cloud.bigquery.table import TimePartitioning, TimePartitioningType

def get_all_user_by_category(kateg=None):
    username = []
    all_user = "https://api.karyakarsa.com/api/cat?category="+kateg.replace("\\","")
    page_total = eval(requests.get(all_user).text.replace("null","None"))["last_page"]
    
    for page in range(1,page_total+1):
        user_ = eval(requests.get(all_user+"&page="+str(page)).text.replace("null","None"))["data"]
        for usr in user_:
            u_n = usr["username"]
            username.append(u_n)
    username = list(set(username))
    print(f"total username in {kateg} : "+str(len(username)))
    return username

def get_donation(username,category):
    posts = []    
    for name in tqdm(username):
        null = None
        try:
            if name != "null" or name != "None" or name != None:
                url_posts = "https://api.karyakarsa.com/api/payment/recent/"+name+""
                page_total = eval(requests.get(url_posts).text.replace("null","None").replace("false","False").replace("true","True"))["last_page"]
                for page in range(1,page_total+1):
                    url_ = "https://api.karyakarsa.com/api/payment/recent/"+name+"?page="+str(page)+""
                    res_posts = eval(requests.get(url_).text.replace("null","None").replace("false","False").replace("true","True"))["data"]
                    l_posts = [posts.append(post) for post in res_posts]
        except:
            continue
    print(f"total donation category {category} : {len(posts)}")
    df_posts = pd.DataFrame(posts)
    df_posts["created_at"] = datetime.now()
    
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema_update_options=[
            "ALLOW_FIELD_ADDITION", "ALLOW_FIELD_RELAXATION"
        ],
        time_partitioning=TimePartitioning(
            type_=TimePartitioningType.DAY,
            field="created_at",
            expiration_ms=1209600000  # 14 days
        )
    )
    upload_to_bq(
        df_posts,
        "evos-data-platform-317507.stg_scrape.karyakarsa_donation",
        job_config
    )

def push_log(type_log=None, categ=None):
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema_update_options=[
            "ALLOW_FIELD_ADDITION", "ALLOW_FIELD_RELAXATION"
        ],
        time_partitioning=TimePartitioning(
            type_=TimePartitioningType.DAY,
            field="created_at",
            expiration_ms=1209600000  # 14 days
        )
    )

    df_log = pd.DataFrame()
    df_log["type"] = [type_log]
    df_log["category"] = [categ]
    df_log["created_at"] = [datetime.now()]

    upload_to_bq(
        df_log,
        "evos-data-platform-317507.stg_scrape.karyakarsa_scrape_logs",
        job_config
    )
    
def upload_to_bq(dataframe, table_id, job_config):
    bq_client = bigquery.Client.from_service_account_json('service.json')

    job = bq_client.load_table_from_dataframe(
        dataframe, table_id, job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.

    table = bq_client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )

def main():
    kategori = [ "Penulis%20%2F%20Jurnalis"
    , "Fotografer"
    , "Ilustrator%20%26%20Komik"
    , "Youtubers"
    , "Edukasi%20%26%20tutorial"
    , "Komunitas"
    , "Cosplayer"
    , "Gaming%20%26%20eSport"
    , "Streamer"
    , "Podcasters"
    , "Arsitek"
    , "Audio"
    , "Animator"
    , "Developer"
    , "Desain"
    , "Musisi"
    , "Video"
    , "3D%20Artist"
    , "Non-profit"
    , "Lainnya"
    ]

    for kateg in kategori:
        username = get_all_user_by_category(kateg)
        if username :
            get_donation(username,kateg)
            push_log("donation success",kateg)
            print("success!")
    
if __name__=='main':
    main()

