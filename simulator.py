from kubernetes import client, config
import random
import time
import math
import pandas as pd
from datetime import datetime
import time

DATE = '20200501'
HOUR_INTERVAL = 1000
TEN_MINUTES_INTERVAL = 6000
INTERVAL = TEN_MINUTES_INTERVAL


# initialize k8s config
def dont_do_that_without_k8s():
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

    for row in rows:
        key = row.split(';')[1]
        value = row.split(';')[4]

        if key.startswith(DATE):
            datetime_key = datetime.strptime(key, '%Y%m%d%H%M')
            result[datetime_key] = value
    file.close()

    df = pd.DataFrame.from_dict(result, orient='index', columns=['Radiation'])
    df['Radiation'] =  pd.to_numeric(df['Radiation'])

    output = df 

    output['Solar Current'] =  output['Radiation'] / 1000 * 2.78 * 10 * INTERVAL
    del output['Radiation']

    # Mean of the following three values in the dataset
    output['Solar Forecast'] = output.rolling(window=3).mean().shift(-3)

    print(output.head(100))

    return output



def prepare_wind_data():

    file=open('data/produkt_zehn_min_ff_20200101_20201231_05705.txt',"r")
    rows=file.readlines()
    result={}

    for row in rows:
        key = row.split(';')[1]
        value = row.split(';')[3]

        if key.startswith(DATE):
            datetime_key = datetime.strptime(key, '%Y%m%d%H%M')
            result[datetime_key] = value
    file.close()

    df = pd.DataFrame.from_dict(result, orient='index', columns=['Wind Speed'])
    df['Wind Speed'] =  pd.to_numeric(df['Wind Speed'])

    output = df

    # Set Bottom (Min Speed)
    output['Wind Speed'].values[output['Wind Speed'] < 3] = 0 

    output['Wind Current'] =   math.pi / 2 * 5.1**2 * output['Wind Speed']**3 * 1.2 * 0.5
    del output['Wind Speed']

    # Set Ceiling (Max Yield)
    output['Wind Current'].values[output['Wind Current'] > 8999] = 9000

    # Mean of the following three values in the dataset
    output['Wind Forecast'] = output.rolling(window=3).mean().shift(-3)

    return output



def update_annotation(node_name, renewable, forecast):

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



def annotate_nodes(solar_output, wind_output):

    print("%s Starting next annotation ..." % (datetime.now()))
        
    node_list = [1, 2, 3, 4, 5]
    # get all nodes in the cluster
    #node_list = k8s_api.list_node()

    #for node in node_list.items:
    for node in node_list:
        if node_list.index(node) == 0:
            #update_annotation(node.metadata.name, 0.0,0.0)
            print("Node %s has a renewable energy share of %s" % (node, 0.0))
        elif node_list.index(node) % 2 == 0:
            #update_annotation(node.metadata.name, solar_output, 0.0)
            print("Node %s has a renewable energy share of %s" % (node, solar_output))
        else:
            #update_annotation(node.metadata.name, wind_output, 0.0)
            print("Node %s has a renewable energy share of %s" % (node, wind_output))

    print("eat sleep rave")
    time.sleep(60.0 - (time.time() % 60.0))



def merge_outputs(solar_output, wind_output):

    return pd.merge(solar_output, wind_output, how='inner', left_index=True, right_index=True).round(1)



def main():
    
    solar_output = prepare_solar_data()
    wind_output = prepare_wind_data()
    renewables_data = merge_outputs(solar_output, wind_output)

    print(renewables_data.head(12))

    annotations = [annotate_nodes(solar_output, wind_output) for solar_output, wind_output in zip(renewables_data['Solar Current'], renewables_data['Wind Current'])]



if __name__ == '__main__':
    main()

