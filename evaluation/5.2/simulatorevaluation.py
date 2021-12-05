import math
import time
from datetime import datetime

import pandas as pd
from kubernetes import client, config
from numpy.lib.function_base import select


INTERVAL_SECONDS = 10
LOG_MSG = "%s Node %s has a %s share of: %s"


try:
    config.load_incluster_config()
except config.ConfigException:
    try:
        config.load_kube_config()
    except config.ConfigException:
        raise Exception("Could not configure kubernetes python client")

k8s_api = client.CoreV1Api()    

def main():

    while True:
        # get all nodes in the cluster
        nodes = k8s_api.list_node()
        for node in nodes.items:
            ts = node.metadata.annotations["timestamp"]
            eq = node.metadata.annotations["equipment"]
            res = node.metadata.annotations["renewable"]
            print(LOG_MSG % (ts, node.metadata.name, eq, res))
        time.sleep(INTERVAL_SECONDS)
  


if __name__ == '__main__':
    main()

