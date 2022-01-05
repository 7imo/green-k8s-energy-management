#### monitors node data or analysis: ts, production, consumption ####

import time
from datetime import datetime
from kubernetes import client, config


INTERVAL_SECONDS = 5
LOG_MSG = "%s;%s;%s;%s;%s"


try:
    config.load_incluster_config()
except config.ConfigException:
    try:
        config.load_kube_config()
    except config.ConfigException:
        raise Exception("Could not configure kubernetes python client")

k8s_api = client.CoreV1Api()


def main():
    print("ts_now;ts_simulator;node;consumption;production")
    api = client.CustomObjectsApi()
    consumption = {}

    while True:
        api = client.CustomObjectsApi()
        k8s_nodes = api.list_cluster_custom_object(
            "metrics.k8s.io", "v1beta1", "nodes")
        consumption = {}

        for stats in k8s_nodes['items']:

            cpu = float(stats['usage']['cpu'][:-1])
            allocatable = float(10**9)  # 1 Core
            current_consumption = (cpu / allocatable) * 10000

            consumption[stats['metadata']['name']] = str(
                round(current_consumption))

        # get all nodes in the cluster
        nodes = k8s_api.list_node(label_selector='kubernetes.io/role=node')
        for node in nodes.items:
            c = consumption[node.metadata.name]
            tsnow = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            ts = node.metadata.annotations["timestamp"]
            res = node.metadata.annotations["renewables"].split(";", 1)[0]
            print(LOG_MSG % (tsnow, ts, node.metadata.name, c, res))
        time.sleep(INTERVAL_SECONDS)


if __name__ == '__main__':
    main()
