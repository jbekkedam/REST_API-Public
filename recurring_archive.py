

import requests
import pandas as pd
import requests
import xml.etree.ElementTree as ET
import pandas as pd
import math
import re

#get Token
def get_token(username, password):

    url = "<server-url>"

    payload = "<tsRequest>\r\n  <credentials name=\""+username+"\""+" password=\""+password+"\" >\r\n    <site contentUrl=\"\" />\r\n  </credentials>\r\n</tsRequest>"
    headers = {
      'Content-Type': 'application/xml'
    }

    r = requests.request("POST", url, headers=headers, data=payload)
    data = r.text
    myroot = ET.fromstring(data)

    return myroot[0].attrib['token'] #token




def get_projects(token, page_number):

    df_list = []
    for page in range(0, page_number):

        url = "<server url>" + "&pageNumber=" + str(page+1)

        payload={}
        headers = {
          'x-tableau-auth': token
        }

        r = requests.request("GET", url, headers=headers, data=payload)
        myroot = ET.fromstring(r.text)

        output = pd.DataFrame()

        #ids of each datasource
        for x in myroot[1]:

        #     #     print("datsource_id:", x.attrib["id"])
        #     #     print("project", x[0].attrib)
            project_id = x.attrib["id"]
            project_name = x.attrib["name"]
            project_dict = {"project_id": project_id, "project_name": project_name }
            output = output.append(project_dict, ignore_index=True)

        project_df = output

        df_list.append(project_df)
        df_main = df_list[0]

        for i ,df in enumerate(df_list):

            if i > 0:
                df_main = df_main.append(df, ignore_index=True)



        df_main = df_main[df_main['project_name'].str.contains(r'archive',flags=re.IGNORECASE)]
        df_main.to_csv("./project_df.csv")
        print("Wrote " + str(len(df_main)) + " projects to projects_df.csv" )
        return df_main





#Get pageSize for datasource
def get_page_number(token, site_id, query, test_mode=False):
    url = "<server-url>"+ site_id +"/" + query

    payload = ""
    headers = {
      'x-tableau-auth': token
    }

    r = requests.request("GET", url, headers=headers, data=payload)
    myroot = ET.fromstring(r.text)
    totalAvailable =  myroot[0].attrib["totalAvailable"]
    page_number = math.ceil(int(totalAvailable)/1000)
    if test_mode:
        print("Found " + str(page_number) + " pages associated with " + query + ".")
    return page_number




def get_workbooks(token, site_id,page_number,test_mode=False):
        df_list = []

        for page in range(0, page_number):

            url = "<server-url>"+ site_id +"/workbooks"   + "?pageSize=1000" + "&pageNumber=" + str(page+1)

            if test_mode:
                print("Performing the following call: workbook url: " , url)
            payload = ""
            headers = {
              'x-tableau-auth': token
            }

            r = requests.request("GET", url, headers=headers, data=payload)

            myroot = ET.fromstring(r.text)
            myroot


            output = pd.DataFrame()


            for x in myroot[1]:
                workbook_id = x.attrib["id"]
                workbook_name = x.attrib["name"]
                workbook_project_name = x[0].attrib["name"]
                workbook_project_id = x[0].attrib["id"]
                workbook_dict = { "workbook_id": workbook_id, "workbook_name": workbook_name, "workbook_project_name": workbook_project_name, "workbook_project_id": workbook_project_id, }


                output = output.append(workbook_dict, ignore_index=True)


            workbook_df = output



#             workbook_df = workbook_df[workbook_df['workbook_project_name'] == project_name]

            df_list.append(workbook_df)
#                 print("df_list", df_list)

        df_main = df_list[0]

        for i ,df in enumerate(df_list):

            if i > 0:
                df_main = df_main.append(df, ignore_index=True)



        df_main = df_main[df_main['workbook_project_name'].str.contains(r'archive',flags=re.IGNORECASE)]
        df_main.to_csv("./workbook_df.csv")
        print("Wrote " + str(len(df_main)) + " workbooks to workbook_df.csv" )
        return df_main




def get_datasources(token, site_id,page_number,project_name_filter, test_mode):
    df_list = []

    for page in range(0, page_number):

        url = "<server-url>"+ site_id +"/datasources" + "?pageSize=" + "1000" + "&pageNumber=" + str(page+1)
        if test_mode:
            print("datasource url: " , url)
        payload = ""
        headers = {
          'x-tableau-auth': token
        }

        r = requests.request("GET", url, headers=headers, data=payload)
        myroot = ET.fromstring(r.text)


        #print(r)
        #print(myroot)


        datasource_arr = []
        output = pd.DataFrame()

        #ids of each datasource
        for x in myroot[1]:
        #     print("datsource_id:", x.attrib["id"])
        #     print("project", x[0].attrib)
            datasource_id = x.attrib["id"]
            datasource_name = x.attrib["name"]
            project_id = x[0].attrib["id"]
            project_name = x[0].attrib["name"]

            datasource_dict = {"datasource_id": datasource_id, "project_id": project_id, "project_name": project_name, "datasource_name":datasource_name }
        #     print(datasource_dict)
            datasource_arr.append(datasource_dict)
            output = output.append(datasource_dict, ignore_index=True)
        datasource_arr
        datasource_df = output
        datasource_df.to_csv("./all_datasources")
        #datasource_df = datasource_df[datasource_df['project_name_filter'] == project_name_filter]
        datasource_df


        #print(datasource_df)

        df_list.append(datasource_df)

    df_main = df_list[0]

    for i ,df in enumerate(df_list):

        if i > 0:
            df_main = df_main.append(df, ignore_index=True)



    df_main = df_main[df_main['project_name'].str.contains(r'archive',flags=re.IGNORECASE)]
    df_main.to_csv("./datasources_df.csv")


    print("Wrote " + str(len(df_main))+ " datasources to datasources_df.csv")



def get_extracts(token,site_id,test_mode):

    url = "<server-url>" + site_id + "/tasks/extractRefreshes"
    if test_mode:
        print("taskrefresh url:", url)
    payload = ""
    headers = {
      'x-tableau-auth': token
    }

    response = requests.request("GET", url, headers=headers, data=payload)




    r = requests.request("GET", url, headers=headers, data=payload)

    myroot = ET.fromstring(r.text)
    myroot[0][0][0][1].attrib
    datasource_output = pd.DataFrame()
    workbook_output = pd.DataFrame()

    for x in myroot[0]:

        extract_id = x[0].attrib["id"]
        schedule_id = x[0][0].attrib["id"]
        workbook_id = None
        datasource_id = None

        if "datasource" in x[0][1].tag:
            datasource_id = x[0][1].attrib["id"]
            task_dict = { "extract_id": extract_id, "schedule_id": schedule_id, "workbook_id": workbook_id, "datasource_id": datasource_id }
            datasource_output = datasource_output.append(task_dict,  ignore_index=True)
        else:
            if "workbook" in x[0][1].tag:
                workbook_id = x[0][1].attrib["id"]
                task_dict = { "extract_id": extract_id, "schedule_id": schedule_id, "workbook_id": workbook_id, "datasource_id": datasource_id }
                workbook_output = workbook_output.append(task_dict,  ignore_index=True)


    datasource_output.to_csv("./datasource_tasks_df.csv")
    workbook_output.to_csv("./workbook_tasks_df.csv")

    print("Wrote " + str(len(datasource_output)) + " datasource tasks to datasource_tasks_df.csv")
    print("Wrote " + str(len(workbook_output)) + " workbook tasks to workbook_tasks_df.csv")





def main():

    project_name_filter = "placeholder"

    test_mode = True


    token = get_token()

    #extracts
    get_extracts(token,site_id, test_mode)

    #projects
    page_number = get_page_number(token, site_id, "projects", test_mode=test_mode)
    get_projects(token, page_number)

    #datasources
    page_number = get_page_number(token, site_id, "datasources",  test_mode=test_mode)
    get_datasources(token, site_id,page_number,project_name_filter,test_mode=test_mode)

    #workbooks
    page_number = get_page_number(token, site_id, "workbooks", test_mode=test_mode)
    get_workbooks(token, site_id,page_number,test_mode=test_mode)

    merge_for_deletion()

    if not test_mode:
        delete_tasks(token, site_id)

















def merge_for_deletion():
    workbook_tasks_df = pd.read_csv('./workbook_tasks_df.csv')
    datasource_tasks_df = pd.read_csv('./datasource_tasks_df.csv')

    datasources_df = pd.read_csv('./datasources_df.csv')
    workbook_df = pd.read_csv('./workbook_df.csv')

    datasource_tasks_to_delete_df = pd.merge( datasources_df,datasource_tasks_df, left_on='datasource_id', right_on='datasource_id')
    #print(len(datasource_tasks_to_delete_df))

    workbook_tasks_to_delete_df = pd.merge( workbook_df,workbook_tasks_df, left_on='workbook_id', right_on='workbook_id')
    #print(len(workbook_tasks_to_delete_df))

    df_delete = datasource_tasks_to_delete_df.append(workbook_tasks_to_delete_df)
    df_delete.to_csv("extract_tasks_to_be_delete.csv")
    print("After looking through projects that contain the word Archive, there are " + str(len(df_delete)) + " extracts to delete, they have been written to extract_tasks_to_be_delete.csv")










def delete_tasks(token, site_id):
    extracts_to_delete_df= pd.read_csv('./extract_tasks_to_be_delete.csv')
    delete_counter = 0
    for extract_id in extracts_to_delete_df["extract_id"]:



        url = "<server-url>" + site_id + "/tasks/extractRefreshes/" + extract_id

        payload = ""
        headers = {
          'x-tableau-auth': token
        }

        response = requests.request("DELETE", url, headers=headers, data=payload)



        if str(response) != "<Response [404]>":


            delete_counter = delete_counter + 1

    if delete_counter > 0:

        print("Successfully deleted " + str(delete_counter) + " extracts.")


    else:

        print("No tasks associated with the projects containing "  + "archive" + " were identified or deleted.")


main()