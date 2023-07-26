import functions_framework
from google.cloud import storage
import json
from datetime import datetime, timedelta
import os
from googleapiclient.discovery import build
from google.oauth2 import service_account
import time
from bs4 import BeautifulSoup
import traceback
import requests
import re 
from flask import escape, make_response

@functions_framework.http
def hello_http(request):

    print(str(request))
    #google storage setup
    storage_client = storage.Client()
    bucket_name = os.environ.get("bucket_name")
    bucket = storage_client.bucket(bucket_name)
    

    #google sheets
    blob = bucket.blob("pivotal-keep-156022-af60fb230dfd.json")
    with blob.open("r") as f:
        txt = f.read()
    credJson = json.loads(txt)

    # Load the credentials from the service account key
    credentials = service_account.Credentials.from_service_account_info(credJson, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    sheetID = os.environ.get("sheet_ID")

    # Build the Sheets API client
    service = build("sheets", "v4", credentials=credentials)

    #pull the sheets for both Ind and Wet
    try:
        indSheet = service.spreadsheets().values().get(spreadsheetId=sheetID, range = "Industrial").execute().get("values",[])
        wetSheet = service.spreadsheets().values().get(spreadsheetId=sheetID, range = "Wetlands").execute().get("values",[])
        print(f"indsheet: {indSheet}")
        print(f"wetSheet {wetSheet}")
    except Exception as e:
        print("unable to get sheets from google due to" + str(e))
    
    #define functions for gsheets

    #name is the unique identifier per page. name is the third entry. 
    def getIndex(name, sheet):
        i=0
        for row in sheet:
            if row[2] == name:
                return i
            i+=1
        return -1

    #replace a row in the appropriate sheet.
    def replaceRow(data, dType):
        if dType == "Industrial":
            index = getIndex(data["agent_name"],indSheet)
            if(index>0):
                indSheet[index] = [data[ind] for ind in ["title","property_address","agent_name","agent_address","agent_phone","company","last_mailed","num_mailed","ready_to_mail","property_type","first_seen"]]
            else:
                data["first_seen"] = datetime.now().strftime("%Y/%m/%d")
                addRow(data, dType)
        else:
            index = getIndex(data["agent_name"],wetSheet)
            if(index>0):
                wetSheet[index] = [data[ind] for ind in ["title","property_address","agent_name","agent_address","agent_phone","company","last_mailed","num_mailed","ready_to_mail","property_type","first_seen"]]
            else:
                data["first_seen"] = datetime.now().strftime("%Y/%m/%d")
                addRow(data,dType)

    def addRow(data, dType):
        if dType == "Industrial":
            indSheet.append([data[ind] for ind in ["title","property_address","agent_name","agent_address","agent_phone","company","last_mailed","num_mailed","ready_to_mail","property_type","first_seen"]])
        else:
            wetSheet.append([data[ind] for ind in ["title","property_address","agent_name","agent_address","agent_phone","company","last_mailed","num_mailed","ready_to_mail","property_type","first_seen"]])
    
    def getLastMailedDTO(name, sheet):
        index = getIndex(name, sheet)
        out = datetime(2023,1,1)
        if(index>0):
            dstr = str(sheet[index][7])
            try:
                out = datetime(int(dstr[0:4]), int(dstr[5:7]), int(dstr[8:10]))
            except:
                pass
        return out

    def getLastMailed(name, sheet):
        index = getIndex(name, sheet)
        if(index>0):
            return sheet[index][7]
        else:
            return ""
    
    #pull data from request json
    try:
        jsonIn = request.get_json()
        for folder in jsonIn["roots"]["bookmark_bar"]["children"]:
            if folder["name"]=="Industrial":
                industrials = folder["children"]
            if folder["name"]=="Wetlands":
                wetlands = folder["children"]
    except Exception as e:
        print("unable to pull data due to " + str(e))
    #check to see if they exist.
    try:
        print(f"industrials: {industrials}")
        print(f"wetlands: {wetlands}")
    except:
        print("unable to load industrials and/or wetlands.")
    #get json of agents previously seen. [{"Industrial/Wetlands:{"agent_name": agentname, "first_seen": datetime as seconds since 1/1/2023, "last_mailed": datetime as seconds since 1/1/2023, num_mailed: int}]
    try:
        blob = bucket.blob("agents.json")
        with blob.open("r") as f:
            txt = f.read()
    except Exception as e:
        print("unable to open agents.json. Does it exist? error: " + str(e))


    #load ajson
    try:
        ajson = json.loads(txt)

        #load indJson
        try:
            indJson = ajson["Industrial"] #this may need json.loads again.
        except Exception as e:
            print("unable to load json: " + str(e))

        #load wetJson
        try:
            wetJson = ajson["Wetlands"]
        except Exception as e:
            print("unable to load json: " + str(e))
    except:
        print("no data in ajson.")
        indJson = []
        wetJson = []


   
    #industrials
    dataType = "Industrial"
    for obj in industrials:
        #[{title, property_address, agent_name, agent_address, agent_phone, company, last_mailed, num_mailed, ready_to_mail, property_type},$agent2 if 2 agents$]
        data = getData(obj["url"]) 
        for entry in data:
            if len(indJson) > 0:
                if entry["agent_name"] in [seen["agent_name"] for seen in indJson]:
                    entry["property_type"] = "industrial"
                    agent_index = getIndexByKey(indJson, "agent_name", entry["agent_name"])
                    lastMailedSheet = getLastMailedDTO(entry["agent_name"],indSheet)
                    if (lastMailedSheet- datetime(2023,1,1)).total_seconds() > indJson[agent_index]["last_mailed"]:
                        indJson[agent_index]["last_mailed"] = (lastMailedSheet- datetime(2023,1,1)).total_seconds()
                        indJson[agent_index]["num_mailed"] += 1
                    entry["num_mailed"] = indJson[agent_index]["num_mailed"]
                    entry["first_seen"] = (timedelta(seconds=indJson[agent_index]["first_seen"])+datetime(2023,1,1)).strftime("%Y/%m/%d")
                    entry["last_mailed"] = getLastMailed(entry["agent_name"],indSheet)
                    if(now() - indJson[agent_index]["last_mailed"]>90*24*60*60):
                        entry["ready_to_mail"] = "yes"
                    else:
                        entry["ready_to_mail"] = "no"
                    replaceRow(entry, dataType)
                else:
                    indJson.append({"agent_name":entry["agent_name"],"first_seen": now(),"last_mailed": 0, "num_mailed": 0})
                    entry["first_seen"] = datetime.now().strftime("%Y/%m/%d")
                    entry["property_type"] = "industrial"
                    entry["last_mailed"] = ""
                    entry["num_mailed"] = 0
                    entry["ready_to_mail"] = "yes"
                    addRow(entry, dataType)
            else:
                    indJson.append({"agent_name":entry["agent_name"],"first_seen": now(),"last_mailed": 0, "num_mailed": 0})
                    entry["first_seen"] = datetime.now().strftime("%Y/%m/%d")
                    entry["property_type"] = "industrial"
                    entry["last_mailed"] = ""
                    entry["num_mailed"] = 0
                    entry["ready_to_mail"] = "yes"
                    addRow(entry, dataType)
            

    
    #wetlands
    dataType = "Wetlands"
    for obj in wetlands:
        #[{title, property_address, agent_name, agent_address, agent_phone, company, last_mailed, num_mailed, ready_to_mail, property_type},$agent2 if 2 agents$]
        data = getData(obj["url"]) 
        for entry in data:
            if len(wetJson) > 0:
                if entry["agent_name"] in [seen["agent_name"] for seen in wetJson]:
                    entry["property_type"] = "industrial"
                    agent_index = getIndexByKey(wetJson, "agent_name", entry["agent_name"])
                    lastMailedSheet = getLastMailedDTO(entry["agent_name"],wetSheet)
                    if (lastMailedSheet- datetime(2023,1,1)).total_seconds() > wetJson[agent_index]["last_mailed"]:
                        wetJson[agent_index]["last_mailed"] = (lastMailedSheet- datetime(2023,1,1)).total_seconds()
                        wetJson[agent_index]["num_mailed"] += 1
                    entry["last_mailed"] = getLastMailed(entry["agent_name"],wetSheet)
                    entry["num_mailed"] = wetJson[agent_index]["num_mailed"]
                    entry["first_seen"] = (timedelta(seconds=wetJson[agent_index]["first_seen"])+datetime(2023,1,1)).strftime("%Y/%m/%d")
                    if(now() - wetJson[agent_index]["last_mailed"]>90*24*60*60):
                        entry["ready_to_mail"] = "yes"
                    else:
                        entry["ready_to_mail"] = "no"
                    replaceRow(entry, dataType)
                else:
                    wetJson.append({"agent_name":entry["agent_name"],"first_seen": now(),"last_mailed": 0, "num_mailed": 0})
                    entry["first_seen"] = datetime.now().strftime("%Y/%m/%d")
                    entry["property_type"] = "industrial"
                    entry["last_mailed"] = ""
                    entry["num_mailed"] = 0
                    entry["ready_to_mail"] = "yes"
                    addRow(entry, dataType)
            else:
                    wetJson.append({"agent_name":entry["agent_name"],"first_seen": now(),"last_mailed": 0, "num_mailed": 0})
                    entry["first_seen"] = datetime.now().strftime("%Y/%m/%d")
                    entry["property_type"] = "industrial"
                    entry["last_mailed"] = ""
                    entry["num_mailed"] = 0
                    entry["ready_to_mail"] = "yes"
                    addRow(entry, dataType)


    #upload the new jsons.
    jsonOut = {"Industrial": indJson, "Wetlands": wetJson}
    with blob.open("w") as f:
        f.write(json.dumps(jsonOut))    

    #upload the new sheets.
    print(indSheet)
    print(wetSheet)
    # Update the values in the "Industrial" sheet
    service.spreadsheets().values().update(spreadsheetId=sheetID, range="Industrial!A1", valueInputOption="RAW", body={"values": indSheet}).execute()

    # Update the values in the "Wetlands" sheet
    service.spreadsheets().values().update(spreadsheetId=sheetID, range="Wetlands!A1", valueInputOption="RAW", body={"values": wetSheet}).execute()

    return make_response("success!",200)



#[{title, property_address, agent_name, agent_address, agent_phone, company},$agent2 if 2 agents
def getData(url):
    data_out = [{}]
    print(f"getting data for url: {url}")
    headers =  { 
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'
    }

    success= False
    tries = 0
    timeout = 20
    waitTime = 0
    initialWait = 0.7
    maxWait = 10
    while (success == False and waitTime < timeout):
        tries += 1
        print(f"try number {tries}...")
        response = requests.get(url, headers=headers)
        print(f"response code: {response.status_code}")
        if response.status_code == 200:
            success = True
        waitTime = initialWait*(1.65**tries)
        time.sleep(max(maxWait,waitTime))


    html_content = response.content
    soup = BeautifulSoup(html_content, 'html.parser')

    # Extract the title
    title_element = soup.find('h1', class_='profile-hero-title')
    if title_element:
        title_text = title_element.get_text(strip=True)
        data_out[0]["title"] = title_text

    # Extract property address
    address_element = soup.find(['h4','span'], string = "Address: ")
    if address_element:
        data_out[0]["property_address"] = address_element.next_sibling.get_text(strip=True)
    
    # Extract the agent address
    agent_address_element = soup.find('div', class_='cta-address')
    if agent_address_element:
        agent_address_text = agent_address_element.get_text(strip=True)
        data_out[0]["agent_address"] = agent_address_text

    # Extract the agent phone
    agent_phone_element = soup.find('span', class_='phone-number')
    if agent_phone_element:
        agent_phone_text = agent_phone_element.get_text(strip=True)
        data_out[0]["agent_phone"] = agent_phone_text

    # Extract the company
    company_element = soup.find('span', class_=re.compile(r'company-name(-no-image)?'))
    if company_element:
        company_text = company_element.get_text(strip=True)
        data_out[0]["company"] = company_text

    # Extract the agent names
    try:
        agent_names_elements = soup.find_all('span', class_='contact-name')
        agent_names_list = [name.get_text(strip=True).replace("Power Broker","") for name in agent_names_elements]
        if len(agent_names_list)>1:
            data_out.append(data_out[0].copy())
            data_out[1]["agent_name"] = agent_names_list[1]
        data_out[0]["agent_name"] = agent_names_list[0]
    except Exception as e:
        print(f"unable to get agent names due to {e}. agent_names_list = {agent_names_list}")
    
    #fill any empties.
    for str in ["title","property_address","agent_name","agent_address","agent_phone","company"]:
        for data in data_out:
            try:
                data[str]
            except:
                data[str] = ""

    print(f"all together, we found: {data_out}.")
    
    return data_out
    


#returns the index of the correct dictionary.
def getIndexByKey(listIn, key, value):
    i=0
    for dic in listIn:
        if dic[key] == value:
            return i
        i = i+1

def now():
    return (datetime.now()-datetime(2023,1,1)).total_seconds()