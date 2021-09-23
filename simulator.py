from kubernetes import client, config
import random
import time
import math
import pandas as pd
from datetime import datetime
import time


# initialize k8s config
def dosomth():
    try:
        config.load_incluster_config()
    except config.ConfigException:
            try:
                config.load_kube_config()
            except config.ConfigException:
                raise Exception("Could not configure kubernetes python client")

    k8s_api = client.CoreV1Api()


def main():
    
    # get all nodes in the cluster
    #node_list = k8s_api.list_node()
    starttime = time.time()

    print(starttime)

    while True:
        # update annotations with recent energy data
        #for node in node_list.items:
            #annotate_node(node.metadata.name)

        print(datetime.now())
        time.sleep(60.0 - (time.time() % 60.0))

        # repeat after 5 minutes
        #time.sleep(120)


def annotate_node(node_name):
        
    # get energy data for node
    renewable_share_float = random.uniform(0, 1)

    # formatting
    renewable = "{:.1f}".format(renewable_share_float)
    forecast = renewable

    print("Node %s has a renewable energy share of %s" % (node_name, renewable))

    # annotation body
    annotations = {
                "metadata": {
                    "annotations": {
                        "renewable": renewable, 
                        "forecast": forecast 
                    }
                }
            }

    # send to k8s
    response = k8s_api.patch_node(node_name, annotations)
    # print(response)



if __name__ == '__main__':
    main()

