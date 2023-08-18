import requests
import base64
import urllib.parse
import os
from dotenv import load_dotenv

azure_baseurl="https://dev.azure.com/"
# Pat_token=os.getenv("pat")
organisation_name="ChandrakumarRavikumar0787"
project="ChandrakumarRavikumar"
url = azure_baseurl+"/"+organisation_name+"/"+project+"/_apis"
pat = "vkwaedjii7fzezhupjmmmxpurasbdlmhyjs3uzvm7wpbsczel32a"
api_responses = []
page_paths=[]
subpage_dict={}


#encoding the pat token
def encode_token(token):
  
    token_bytes = token.encode('ascii')
    base64_bytes = base64.b64encode(token_bytes)
    base64_token = base64_bytes.decode('ascii')
    return base64_token

id_value="";

#get parentpage
def parentpage(url):
    headers = {
    "Authorization": "Basic " + encode_token(":" + pat)
    }
    payload={}
    response = requests.request("GET", url, headers=headers, data=payload)
    data = response.json()
    # data = json.loads(data)
    parent_path = data['subPages'][0]['path']

    # print(data)
    # print(parent_path)
    return parent_path
#get pagepaths
def pagepathfn(url):
    headers = {
    "Authorization": "Basic " + encode_token(":" + pat)
    }
    payload={}
    response = requests.request("GET", url, headers=headers, data=payload)
    data = response.json()
    print
    subpages = data['subPages']

    # Store the subpage paths in an array
    page_paths = [subpage['path'] for subpage in subpages]
    # print(page_paths)
    return page_paths

#get wikiid
def send_request(url):
    headers = {
    "Authorization": "Basic " + encode_token(":" + pat)
    }
    payload={}
    response = requests.request("GET", url, headers=headers, data=payload)
    data = response.json()
    id_value = data['value'][0]['id']
    return id_value
#get entire page
def get_pages(url,documentname):
    headers = {
    "Authorization": "Basic " + encode_token(":" + pat)
    }
    payload={}
    response = requests.request("GET", url, headers=headers, data=payload)
    data = response.json()
    api_responses.append(data)
    return "sucesss"
#get  sub page
def subpagesfn(url):
    headers = {
    "Authorization": "Basic " + encode_token(":" + pat)
    }
    # print(url)
    payload={}
    response = requests.request("GET", url, headers=headers, data=payload)
    data = response.json()
    count = len(data["subPages"])
    # print(count)
    for i in range(count):
        subpage_dict[data["subPages"][i]["path"]]=data["subPages"][i]["url"]
#to write the file
def write_file():
    with open("documents/azuredevopslinks.txt", "w") as f:
        for response in api_responses:
            for key, value in subpage_dict.items():
                f.write(f'{key}:{value}\n')

wiki_id_fetch= f"{url}/wiki/wikis?api-version=7.1-preview.2"
wiki_id=send_request(wiki_id_fetch)

Parentpage=f"{url}/wiki/wikis/{wiki_id}/pages?recursionLevel=OneLevel&includeContent=true&path=/&api-version=7.1-preview.1"
parentpagepath=parentpage(Parentpage)

encodepath=urllib.parse.quote(parentpagepath)
pagepaths=f"{url}/wiki/wikis/{wiki_id}/pages?recursionLevel=OneLevel&includeContent=true&path={encodepath}&api-version=7.1-preview.1"
subpagespath=""
page_paths=pagepathfn(pagepaths)
page_paths2=subpagesfn(pagepaths)
for document in page_paths:
    file_path = document
    encoded_path = urllib.parse.quote(file_path)
    pages=f"{url}/wiki/wikis/{wiki_id}/pages?includeContent=true&path={encoded_path}&api-version=7.1-preview.1"
    get_pages(pages,document)
    subpagespath=f"{url}/wiki/wikis/{wiki_id}/pages?path={encoded_path}&recursionLevel=OneLevel&includeContent=true&api-version=7.1-preview.1"
    subpages_path=subpagesfn(subpagespath)
# print(subpage_dict)
write_file()




       