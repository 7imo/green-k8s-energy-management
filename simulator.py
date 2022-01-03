import math
import time
from datetime import datetime

import pandas as pd
from kubernetes import client, config
from numpy.lib.function_base import select

# constants 
START_DATE = '2020-09-22 21:40:00'
END_DATE = '2020-09-23 20:10:00'
INTERVAL_SECONDS = 60
MIXED_SHARE_SOLAR = 0.6
MIXED_SHARE_WIND = 0.6
LOG_MSG = "%s - %s Updating %s with a consumption of %s and %s renewable data: %s"
NOMINAL_POWER = 10000



try:
    config.load_incluster_config()
except config.ConfigException:
    try:
        config.load_kube_config()
    except config.ConfigException:
        raise Exception("Could not configure kubernetes python client")

k8s_api = client.CoreV1Api()    


def create_forecasts(df):
    cols = ['Watt_10min']

    # Forecast
    for i in range(1, 25):
        col = 'Watt_' + str(i) + 'h_ahead'
        df[col] = df['Watt_10min'].rolling(6*i).mean().shift(-6*i).fillna(0)
        cols.append(col)

    df[cols] = df[cols].apply(lambda x: pd.Series.round(x, 1))

    return df


def filter_dates(df):

    df['MESS_DATUM'] = pd.to_datetime(df['MESS_DATUM'], format='%Y%m%d%H%M')

    # Filter
    mask = (df['MESS_DATUM'] >= START_DATE) & (df['MESS_DATUM'] < END_DATE)
    selected_dates = df.loc[mask]

    selected_dates.set_index('MESS_DATUM', inplace=True)

    return selected_dates


def create_renewables_string(selected_dates):

    return selected_dates.apply(lambda x :';'.join(x.astype(str)),1)


def prepare_solar_data():

   # Extract & Transform
    df = pd.read_csv('data/produkt_zehn_min_sd_20200101_20201231_05705.txt', delimiter = ";")

    # Remove unneccesary columns
    df.drop(['STATIONS_ID', '  QN', 'DS_10', 'SD_10', 'LS_10', 'eor'], axis = 1, inplace = True)

    # Current 10min Output
    df['Watt_10min'] =  df['GS_10'] / 1000 * 2.78 * 50 * 6000 * 0.2

    df = create_forecasts(df)

    selected_dates = filter_dates(df)
    selected_dates.drop('GS_10', axis=1, inplace=True)
    selected_dates['renewables_solar'] = create_renewables_string(selected_dates)

    return selected_dates


def prepare_wind_data():

    df = pd.read_csv('data/produkt_zehn_min_ff_20200101_20201231_05705.txt', delimiter = ";")

    # Remove unneccesary columns
    df.drop(['STATIONS_ID', '  QN', 'DD_10', 'eor'], axis = 1, inplace = True)

    # Current 10min Output
    df['Watt_10min'] =  math.pi / 2 * 5.1**2 * df['FF_10']**3 * 1.2 * 0.5

    # set ceiling
    df['Watt_10min'].values[df['Watt_10min'] > 9999] = NOMINAL_POWER

    df = create_forecasts(df)

    selected_dates = filter_dates(df)
    selected_dates.drop('FF_10', axis=1, inplace=True)
    selected_dates['renewables_wind'] = create_renewables_string(selected_dates)

    return selected_dates


def prepare_mixed_data(solar_data, wind_data):

    solar_share = solar_data.drop('renewables_solar', axis=1)
    wind_share = wind_data.drop('renewables_wind', axis=1)

    solar_share = solar_share.mul(MIXED_SHARE_SOLAR, fill_value=0)
    wind_share = wind_share.mul(MIXED_SHARE_WIND, fill_value=0)

    mixed_data = round(pd.concat([solar_share, wind_share]).groupby(['MESS_DATUM']).sum(), 1)
    mixed_data['renewables_mixed'] = create_renewables_string(mixed_data) 

    return mixed_data


def update_annotation(node_name, ts, consumption, equipment, renewables):

    # annotation body
    annotations = {
                "metadata": {
                    "annotations": {
                        "timestamp": ts,
                        "equipment": equipment,
                        "consumption": consumption,
                        "renewables": renewables
                    }
                }
            }

    # send to k8s
    response = k8s_api.patch_node(node_name, annotations)
    #print(response)


def calculate_consumption():
    api = client.CustomObjectsApi()
    k8s_nodes = api.list_cluster_custom_object("metrics.k8s.io", "v1beta1", "nodes")
    consumption = {}

    for stats in k8s_nodes['items']:

        cpu = float(stats['usage']['cpu'][:-1])
        allocatable = float(10**9) # 1 Core
        current_consumption = (cpu / allocatable) * NOMINAL_POWER

        consumption[stats['metadata']['name']] = str(round(current_consumption))
    
    return consumption


def annotate_nodes(ts, equipped_nodes, data):

    currentConsumption = calculate_consumption()
    now = datetime.now().strftime("%m/%d/%Y %H:%M:%S")

    # equipment of nodes with renewable energy
    for node, eq in equipped_nodes.items():
        cons = currentConsumption[node]
        if eq == 'solar':
            update_annotation(node, ts, cons, eq, data['renewables_solar'])
            print(LOG_MSG % (now, ts, node, cons, eq, data['renewables_solar']))
        elif eq == 'wind':
            update_annotation(node, ts, cons, eq, data['renewables_wind'])
            print(LOG_MSG % (now, ts, node, cons, eq, data['renewables_wind']))
        elif eq == 'mixed':
            update_annotation(node, ts, cons, eq, data['renewables_mixed'])
            print(LOG_MSG % (now, ts, node, cons, eq, data['renewables_mixed']))
        else:
            update_annotation(node, ts, cons, eq, "")
            print(LOG_MSG % (now, ts, node, cons, eq, "0"))


def merge_outputs(solar_output, wind_output, mixed_output):
    # merge dataframes for easy iteration
    return pd.concat([solar_output['renewables_solar'], wind_output['renewables_wind'], mixed_output['renewables_mixed']], axis=1, keys=['renewables_solar', 'renewables_wind', 'renewables_mixed'])


def assign_equipment():

    # get all nodes in the cluster
    worker_nodes = k8s_api.list_node(label_selector='kubernetes.io/role=node')
    nodes_list = []
    equipment = {}

    for node in worker_nodes.items:
        nodes_list.append(node.metadata.name)

    # equipment of nodes with renewable energy
    for node in nodes_list:
        if nodes_list.index(node) == 0:
            # no equipment
            equipment[node] = 'none'
            print("Node %s has no renewable equipment" % node)
        elif nodes_list.index(node) % 3 == 0:
            # wind equipment
            equipment[node] = 'wind'
            print("Node %s has a wind equipment" % node)
        elif nodes_list.index(node) % 2 == 0:
            # solar equipment
            equipment[node] = 'solar'
            print("Node %s has a solar equipment" % node)
        else:
            # mixed equipment
            equipment[node] = 'mixed'
            print("Node %s has a mixed equipment" % node)
            

    return equipment


def main():

    # assign renewables equipment to each node
    equipped_nodes = assign_equipment()

    # generate electricity production and forecast data from weather data
    solar_data = prepare_solar_data()
    wind_data = prepare_wind_data()
    mixed_data = prepare_mixed_data(solar_data, wind_data)

    # prepare for iteration
    renewables_data = merge_outputs(solar_data, wind_data, mixed_data)

    # iterate over renewable energy timeseries
    for index, data in renewables_data.iterrows():
        # print("Next annotation for timestamp %s: %s, %s, %s" % (index, data['renewables_solar'], data['renewables_wind'], data['renewables_mixed']))
        print("Next annotation for timestamp %s:" % (index))
        annotate_nodes(str(index), equipped_nodes, data)
        # wait for next interval
        time.sleep(INTERVAL_SECONDS - (time.time() % INTERVAL_SECONDS))

    print("We are done here.")


if __name__ == '__main__':
    main()

