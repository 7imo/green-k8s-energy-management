from kubernetes import client, config
import time
import math
import random
import pandas as pd
from datetime import datetime
import time

# constats 
DATE = '20200501'
HOUR_INTERVAL = 1000
TEN_MINUTES_INTERVAL = 6000
INTERVAL = TEN_MINUTES_INTERVAL
LOG_MSG = "Node %s has a renewable energy share of %s and a forecasted share of %s"

# dataframe columns
rad = 'Radiation'
sc = 'Solar Current'
sf = 'Solar Forecast'
ws = 'Wind Speed'
wc = 'Wind Current'
wf = 'Wind Forecast'



try:
    config.load_incluster_config()
except config.ConfigException:
    try:
        config.load_kube_config()
    except config.ConfigException:
        raise Exception("Could not configure kubernetes python client")

k8s_api = client.CoreV1Api()    



def prepare_solar_data():

    file=open('data/produkt_zehn_min_sd_20200101_20201231_05705.txt',"r")
    rows=file.readlines()
    result={}

    # extract relevant data from source
    for row in rows:
        key = row.split(';')[1]
        value = row.split(';')[4]

        if key.startswith(DATE):
            datetime_key = datetime.strptime(key, '%Y%m%d%H%M')
            result[datetime_key] = value
    file.close()

    # create pandas dataframe
    output = pd.DataFrame.from_dict(result, orient='index', columns=[rad])
    output[rad] =  pd.to_numeric(output[rad])

    # calculate current output
    output[sc] =  output[rad] / 1000 * 2.78 * 10 * INTERVAL
    del output[rad]

    # 'forecast' = mean of the following three values in the dataset
    output[sf] = output.rolling(window=3).mean().shift(-3)

    return output



def prepare_wind_data():

    file=open('data/produkt_zehn_min_ff_20200101_20201231_05705.txt',"r")
    rows=file.readlines()
    result={}

    # extract relevant data from source
    for row in rows:
        key = row.split(';')[1]
        value = row.split(';')[3]

        if key.startswith(DATE):
            datetime_key = datetime.strptime(key, '%Y%m%d%H%M')
            result[datetime_key] = value
    file.close()

    # create pandas dataframe
    output = pd.DataFrame.from_dict(result, orient='index', columns=[ws])
    output[ws] =  pd.to_numeric(output[ws])

    # min required wind speed to produce energy according to manufacturer
    output[ws].values[output[ws] < 3] = 0 

    # calculate current output
    output[wc] =   math.pi / 2 * 5.1**2 * output[ws]**3 * 1.2 * 0.5
    del output[ws]

    # set ceiling for max yield
    output[wc].values[output[wc] > 8999] = 9000

    # 'forecast' = mean of the following three values in the dataset
    output[wf] = output.rolling(window=3).mean().shift(-3)

    return output


def apply_random_losses(renewable):
    # randomizes renewable values with different efficiency factors
    return round(renewable * random.uniform(0.85, 1.0), 1)


def update_annotation(node_name, renewable, forecast):

    renewable = apply_random_losses(renewable)
    print(LOG_MSG % (node_name, renewable, forecast))

    # annotation body
    annotations = {
                "metadata": {
                    "annotations": {
                        "renewable": str(renewable), 
                        "forecast": str(forecast) 
                    }
                }
            }

    # send to k8s
    response = k8s_api.patch_node(node_name, annotations)
    # print(response)



def annotate_nodes(solar_output, solar_forecast, wind_output, wind_forecast):

    print("%s Starting next annotation ..." % (datetime.now()))
        
    # get all nodes in the cluster
    nodes = k8s_api.list_node()
    nodes_list = []

    # get list of node names in cluster
    for node in nodes.items:
        nodes_list.append(node.metadata.name)


    # equipment of nodes with renewable energy
    for node in nodes_list:
        if nodes_list.index(node) == 0:
            # mixed equipment
            combined_output = round(0.4 * wind_output + 0.7 * solar_output, 1)
            combined_forecast = round(0.4 * wind_forecast + 0.7 * solar_forecast, 1)
            update_annotation(node,  combined_output, combined_forecast)
        elif nodes_list.index(node) % 2 == 0:
            # solar equipment
            update_annotation(node, solar_output, solar_forecast)
        else:
            # wind equipment
            update_annotation(node, wind_output, wind_forecast)

    print("Sleeping 180 seconds...")
    time.sleep(180.0 - (time.time() % 180.0))


def merge_outputs(solar_output, wind_output):
    # merge dataframes for easy iteration and set precision of float to 1
    return pd.merge(solar_output, wind_output, how='inner', left_index=True, right_index=True).round(1)



def main():
    
    solar_output = prepare_solar_data()
    wind_output = prepare_wind_data()
    renewables_data = merge_outputs(solar_output, wind_output)

    # iterate over renewable energy timeseries
    annotations = [annotate_nodes(solar_output, solar_forecast, wind_output, wind_forecast) for solar_output, solar_forecast, wind_output, wind_forecast in zip(renewables_data[sc], renewables_data[sf], renewables_data[wc], renewables_data[wf])]

    print("We are done here.")


if __name__ == '__main__':
    main()

