# Copyright 2021 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from google.cloud import container_v1
from google.api_core.exceptions import PermissionDenied
from google.cloud.exceptions import NotFound
import argparse
import sys
import json
from google.cloud import bigquery
from apiclient.discovery import build
from google.cloud.resourcemanager import ProjectsClient
import logging
from googleapiclient.errors import HttpError

import io
import os

from colorama import Back
from colorama import Style

app_version = "1.0"

out_dir="output/"
projects_output_file=out_dir + "projects/project-ids.json"
gke_out_dir= out_dir + "gke/"

def getProjectsAndGkeCluster():
    """
    Get all projects which this credential have access
    then write all gke clusters info in that project to a file
    """
    with open(projects_output_file, "w") as outfile:
        for project in ProjectsClient().search_projects():
            outfile.write(project.project_id +"\n")
            getGkeClusters(project.project_id, "-")
    

def getGkeClusters(project_id: str, location: str):
    """
    List all the GKE clusters in the given GCP Project and Zone
    https://github.com/googleapis/python-container/blob/c041c28b68b54fc0e5cbae841a4e99a99d4dd1d2/samples/snippets/quickstart.py
    https://developers.google.com/resources/api-libraries/documentation/container/v1/python/latest/container_v1.projects.locations.clusters.html#list

    Required "container.clusters.list" permission(s) for "projects/blah".
    Args:
        project_id : project id to look for GKE clusters
        location: if "-" this will find for all regions but otherwise it could be a region/zone
    Logic:
        Get all gke clusters for a project and writes in a local file

    Returns:
        None.
    """

    service = build('container', 'v1')
    
    try:
        list_resp = service.projects().locations().clusters().list(
        parent=f'projects/{project_id}/locations/{location}').execute()

        json_object = json.dumps(list_resp['clusters'])
        # Writing to file
        with open(gke_out_dir + project_id +".json", "w") as outfile:
            outfile.write(json_object)
    except KeyError:
        # If no GKE cluster, response will be empty
        print(f"No GKE clusters in project={project_id}")
    except PermissionDenied as e:
        print('Permission Denied for project={}{} exception={}'.format(Back.RED, project_id, e))  
        print(Style.RESET_ALL)
    except Exception as e:
        print('Project={} Exception={}{}'.format(project_id,Back.RED, e))  
        print(Style.RESET_ALL)


def format_gke_output() -> None:

    with open("final-output.json", "w") as outfile:
        # Iterate directory
        for path in os.listdir(gke_out_dir):
            # check if current path is a file
            if os.path.isfile(os.path.join(gke_out_dir, path)):
                
                with open(gke_out_dir+path, 'r') as openfile:
                    print("working on project={}".format(gke_out_dir+path))
                    # Reading from json file
                    json_object = json.load(openfile)

                    for cluster in json_object:
                        newJsonStr = '{}'
                        # parsing JSON string:
                        newJson = json.loads(newJsonStr)
                        rewriteBasicAttributes(cluster, newJson)
                        rewriteVersionAttributes(cluster, newJson)
                        rewriteAddonsConfigAttributes(cluster, newJson)
                        rewriteNetworkAttributes(cluster, newJson)
                        rewriteSecurityAttributes(cluster, newJson)
                        rewriteScalingAttributes(cluster, newJson)
                        rewriteNodePoolAttributes(cluster, newJson)

                        outfile.write(f'{json.dumps(newJson)}\n')


def rewriteBasicAttributes(cluster, newJson) -> None:
    basicDict = {}

    basicDict["id"] = cluster["id"]
    basicDict["name"] = cluster["name"]
    basicDict["location"] = cluster["location"]
    basicDict["createTime"] = cluster["createTime"]
    basicDict["regionalCluster"] = (len(cluster["locations"]) > 1)
    basicDict["autopilot"] = getBoolValue(cluster, "autopilot", "enabled")
    basicDict["projectId"] = getProjectId(cluster)

    newJson["basic"] = basicDict


def rewriteVersionAttributes(cluster, newJson) -> None:
    versionDict = {}
    versionDict["initialClusterVersion"] = cluster["initialClusterVersion"]
    versionDict["currentMasterVersion"] = cluster["currentMasterVersion"]
    versionDict["releaseChannel"] = getValue(
        cluster, "releaseChannel", "channel")
    if ("enableKubernetesAlpha" in cluster):
        versionDict["enableKubernetesAlpha"] = True
    else:
        versionDict["enableKubernetesAlpha"] = False

    versionDict["maintenancePolicy"] = isPresent(
        cluster, "maintenancePolicy", "window")
    versionDict["notificationConfigPubSubTopic"] = getTwoNodeLevelValue(
        cluster, "notificationConfig", "pubsub", "topic")

    newJson["version"] = versionDict


def rewriteNetworkAttributes(cluster, newJson) -> None:
    networkDict = {}

    networkDict["networkPolicy"] = isPresent(
        cluster, "networkPolicy", "enabled")
    networkDict["masterAuthorizedNetworksConfig"] = isPresent(
        cluster, "masterAuthorizedNetworksConfig", "enabled")
    networkDict["enablePrivateNodes"] = getBoolValue(
        cluster, "privateClusterConfig", "enablePrivateNodes")
    networkDict["enablePrivateEndpoint"] = getBoolValue(
        cluster, "privateClusterConfig", "enablePrivateEndpoint")
    networkDict["masterIpv4CidrBlock"] = getValue(
        cluster, "privateClusterConfig", "masterIpv4CidrBlock")

    newJson["network"] = networkDict


def rewriteSecurityAttributes(cluster, newJson) -> None:
    secDict = {}
    secDict["binaryAuthorization"] = isPresent(
        cluster, "binaryAuthorization", "enabled")
    secDict["etcdEncryption"] = getValue(
        cluster, "databaseEncryption", "state")
    secDict["shieldedNodes"] = getBoolValue(
        cluster, "shieldedNodes", "enabled")
    secDict["workloadIdentityConfig"] = getValue(
        cluster, "workloadIdentityConfig", "workloadPool")
    secDict["meshCertificates"] = getBoolValue(
        cluster, "meshCertificates", "enableCertificates")
    secDict["confidentialNodes"] = getBoolValue(
        cluster, "confidentialNodes", "enabled")
    secDict["identityServiceConfig"] = getBoolValue(
        cluster, "identityServiceConfig", "enabled")

    newJson["security"] = secDict


def rewriteScalingAttributes(cluster, newJson) -> None:
    scaleDict = {}
    scaleDict["enableNodeAutoprovisioning"] = isPresent(
        cluster, "autoscaling", "enableNodeAutoprovisioning")
    scaleDict["autoscalingProfile"] = getValue(
        cluster, "autoscaling", "autoscalingProfile")
    scaleDict["verticalPodAutoscaling"] = getBoolValue(
        cluster, "verticalPodAutoscaling", "enabled")

    newJson["scaling"] = scaleDict


def rewriteNodePoolAttributes(cluster, newJson) -> None:
    nodePoolsList = []
    newJson["nodePools"] = []
    try:
        for nodePool in cluster["nodePools"]:
            nodePoolDict = {}
            nodePoolDict["name"] = nodePool["name"]
            nodePoolDict["machineType"] = getValue(
                nodePool, "config", "machineType")
            nodePoolDict["initialNodeCount"] = getDirectValue(
                nodePool, "initialNodeCount", "Int")
            nodePoolDict["version"] = getDirectValue(nodePool, "version", "Str")
            if ( nodePoolDict["version"] == cluster["currentMasterVersion"] ):
                nodePoolDict["SameAsMasterVersion"] = True;
            else:
                nodePoolDict["SameAsMasterVersion"] = False;
            nodePoolDict["upgradeStrategy"] = getValue(
                nodePool, "upgradeSettings", "strategy")
            nodePoolDict["autoUpgrade"] = getBoolValue(
                nodePool, "management", "autoUpgrade")
            nodePoolDict["autoRepair"] = getBoolValue(
                nodePool, "management", "autoRepair")
            nodePoolsList.append(nodePoolDict)
    except KeyError:
        print("No Node pool clusters in cluster={}".format(getDirectValue(cluster, "selfLink", "str")));

    newJson["nodePools"] = nodePoolsList


# Some attributes are disabled others are enabled. Don't understand the rationale
# to make it simple output here will be if ENABLED or not
# so if we look for disabled keyword and its present return False
# and if we look for enabled keyword and its present return True
# disable https://cloud.google.com/kubernetes-engine/docs/reference/rest/v1/projects.locations.clusters#httploadbalancing
# enable https://cloud.google.com/kubernetes-engine/docs/reference/rest/v1/projects.locations.clusters#dnscacheconfig


def rewriteAddonsConfigAttributes(cluster, newJson) -> None:
    addonDict = {"httpLoadBalancing": "",
                 "horizontalPodAutoscaling": "false", "kubernetesDashboard": "", }

    addonDict["httpLoadBalancing"] = disabledOrNot(
        cluster, "addonsConfig", "httpLoadBalancing", "disabled")
    addonDict["horizontalPodAutoscaling"] = disabledOrNot(
        cluster, "addonsConfig", "horizontalPodAutoscaling", "disabled")
    addonDict["kubernetesDashboard"] = disabledOrNot(
        cluster, "addonsConfig", "kubernetesDashboard", "disabled")

    # only track master network policy
    addonDict["masterNetworkPolicyConfig"] = disabledOrNot(
        cluster, "addonsConfig", "networkPolicyConfig", "disabled")

    addonDict["dnsCacheConfig"] = enabledOrNot(
        cluster, "addonsConfig", "dnsCacheConfig", "enabled")

    newJson["addonsConfig"] = addonDict


def getProjectId(cluster):
    # https://container.googleapis.com/v1/projects/<projectid>/locations/<region/zone>/clusters/<clustername>
    projectId = getDirectValue(cluster, "selfLink", "str")
    if (len(projectId.split('/')) > 5):
        projectId = projectId.split('/')[5]
    else:
        return "-"

    return projectId


def isPresent(cluster, nodeStr, findStr):
    if(nodeStr in cluster and findStr in cluster[nodeStr]
            and (True == cluster[nodeStr][findStr])):
        return True
    return False


def getDirectValue(cluster, attributeStr, type: str):
    if(attributeStr in cluster):
        return cluster[attributeStr]

    if(type == "str"):
        return "-"
    elif (type == "Int"):
        return 0
    elif (type == "list"):
        return []


def getValue(cluster, nodeStr, attributeStr):
    if(nodeStr in cluster and attributeStr in cluster[nodeStr]):
        return cluster[nodeStr][attributeStr]
    return "-"


def getTwoNodeLevelValue(cluster, nodeStr, node2Str, attributeStr):
    if(nodeStr in cluster and node2Str in cluster[nodeStr] and attributeStr in cluster[nodeStr][node2Str]):
        return cluster[nodeStr][node2Str][attributeStr]
    return "-"


def getBoolValue(cluster, nodeStr, attributeStr):
    if(nodeStr in cluster and attributeStr in cluster[nodeStr]):
        return cluster[nodeStr][attributeStr]
    return False


# Look for disable
# ex: "addonsConfig": {"kubernetesDashboard": {"disabled": true}
def disabledOrNot(cluster, findStr1, findStr2, findStr3):
    if(findStr2 in cluster[findStr1] and findStr3 in cluster[findStr1][findStr2]):
        return False
    else:
        return True


# Look for enable
# ex: "addonsConfig":"gcePersistentDiskCsiDriverConfig": {"enabled": true}
def enabledOrNot(cluster, findStr1, findStr2, findStr3):
    if(findStr2 in cluster[findStr1] and findStr3 in cluster[findStr1][findStr2]):
        return True
    else:
        return False


def writeToBQ():

    table_id = "gke_cluster_ds.gke_data"

    client = bigquery.Client()

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
    )

    with open("final-output.json", "rb") as source_file:
        job = client.load_table_from_file(
            source_file, table_id, job_config=job_config)

    errors = job.result()  # Waits for the job to complete.
    if errors == []:
        print("New rows have been added.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))

    table = client.get_table(table_id)  # Make an API request.

    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )



if __name__ == "__main__":

    #getProjectsAndGkeCluster();
    #format_gke_output()
    writeToBQ()
    # createBQTable();
