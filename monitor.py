from urllib.parse import MAX_CACHE_SIZE
from kubernetes import client, config
import time
import math
import random
import pandas as pd
from datetime import datetime
import time
import logging

logging.basicConfig(filename='green-k8s-scheduler.log', level=logging.INFO, format='%(asctime)s;%(message)s')

try:
    config.load_incluster_config()
except config.ConfigException:
    try:
        config.load_kube_config()
    except config.ConfigException:
        raise Exception("Could not configure kubernetes python client")

k8s_api = client.CoreV1Api()

MAX_SCORE = 10

nodes_scores = {}

def get_nodes_renewable_shares(nodes):

    nodes_renewable_shares = {}

    for node in nodes.items:
        if node.metadata.annotations["renewable"] is not None:
            nodes_renewable_shares[node.metadata.name] = float(node.metadata.annotations["renewable"])
        else:
            nodes_renewable_shares[node.metadata.name] = 0.0
    
    return nodes_renewable_shares

def calculate_scores(renewable_shares):

    highest = 1.0  
    scores = {}

    for share in renewable_shares.values():
	    highest = max(highest, share)
	
    for node, share in renewable_shares.items():
	    score = int(share * MAX_SCORE / highest)
	    scores[node] = score

    return scores


def main():
    
    logging.info("This is a log message")

    while True:
        
        # get all nodes in the cluster
        nodes = k8s_api.list_node()
    
        renewable_shares = get_nodes_renewable_shares(nodes)
        scores = calculate_scores(renewable_shares)

        # get list of node names in cluster
        for node in nodes.items:
            node_name = node.metadata.name
            field_selector = 'spec.nodeName='+node_name+',metadata.namespace=default'
            pods = k8s_api.list_pod_for_all_namespaces(watch=False, field_selector=field_selector)
            number_of_pods_on_node = len(pods.items)

            # logging node information
            logging.info(node_name + ';' + str(renewable_shares[node_name]) + ';' + str(scores[node_name]) + ';' +  str(number_of_pods_on_node))
        
        #pods = k8s_api.list_pod_for_all_namespaces(watch=False)
        #for pod in pods.items:

            # logging pod information
        #    logging.info(pod.status.pod_ip, pod.metadata.namespace, pod.metadata.name)
        time.sleep(60.0 - (time.time() % 60.0))



if __name__ == '__main__':
    main()

