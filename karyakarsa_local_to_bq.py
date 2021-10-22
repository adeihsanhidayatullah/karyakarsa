import requests
import pandas as pd
import time
from tqdm import tqdm
from datetime import datetime
from google.cloud import bigquery
from google.cloud.bigquery.table import TimePartitioning, TimePartitioningType

def get_all_user(kategori=None):
    username = []
    for kateg in kategori:
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

def get_profile_user(username,category):
    profile = []    
    for name in tqdm(username):
        try:
            url_profile = "https://api.karyakarsa.com/api/profile/"+name+"/username"
            res_profile = eval(requests.get(url_profile).text.replace("null","None"))
            if type(res_profile) == dict:
                profile.append(res_profile)
        except:
            continue
    
    print(f"total user category {category} : {len(profile)}")
    print(profile[:3])

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

    df = pd.DataFrame(profile)
    df["name"] = df["name"].str.encode('utf-16','surrogatepass').str.decode('utf-16','surrogatepass')
    df["username"] = df["username"].str.encode('utf-16','surrogatepass').str.decode('utf-16','surrogatepass')
    df["created_at"] = datetime.now()
    df["profile"]=df["profile"].astype('str')
    df["settings"]=df["settings"].astype('str')

    upload_to_bq(
        df,
        "evos-data-platform-317507.stg_scrape.karyakarsa_profile",
        job_config
    )

def get_posts_user(username,category):
    posts = []    
    for name in tqdm(username):
        null = None
        try:
            if name != "null" or name != "None" or name != None:
                url_posts = "https://api.karyakarsa.com/api/posts/"+name+""
                page_total = eval(requests.get(url_posts).text.replace("null","None").replace("false","False").replace("true","True"))["last_page"]
                for page in range(1,page_total+1):
                    url_ = "https://api.karyakarsa.com/api/posts/"+name+"?page="+str(page)+""
                    res_posts = eval(requests.get(url_).text.replace("null","None").replace("false","False").replace("true","True"))["data"]
                    l_posts = [posts.append(post) for post in res_posts]
        except:
            continue
    print(f"total post category {category} : {len(posts)}")

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

    # flat_list = [item for sublist in posts for item in sublist]
    df_posts = pd.DataFrame(posts)
    df_posts["content"] = df_posts["content"].str.encode('utf-16','surrogatepass')
    df_posts["title"] = df_posts["title"].str.encode('utf-16','surrogatepass')

    df_posts["content"] = df_posts["content"].str.decode('utf-16','surrogatepass')
    df_posts["title"] = df_posts["title"].str.decode('utf-16','surrogatepass')
    df_posts["has_access"] = df_posts["has_access"].astype('str')
    df_posts["user"] = df_posts["user"].astype('str')
    df_posts["tiers"] = df_posts["tiers"].astype('str')
    df_posts["created_at"] = datetime.now()
    
    upload_to_bq(
        df_posts,
        "evos-data-platform-317507.stg_scrape.karyakarsa_posts",
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

def get_karyakarsa():   
    kategori = ["Penulis%20%2F%20Jurnalis"]
    for kateg in kategori:    
        kateg_user = [kateg]
        username = get_all_user(kateg_user) 
        username = list(set(username))
        if len(username)==0:
            print("Get Username Error")
        else:
            # get_profile_user(username, kateg)
            print("Get Profile Success")
            # push_log("insert profile success",kateg)

        if len(username)==0:
            print("Get Username Error : no username detected")
        else:
            print("length username : "+str(len(username)))
            get_posts_user(username, kateg)
            push_log("insert posts success",kateg)

    return "Success!", 200

def upload_to_bq(dataframe, table_id, job_config):
    bq_client = bigquery.Client.from_service_account_json('D:/EVOS/cred/service_account/data-prod.json')

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


get_karyakarsa()