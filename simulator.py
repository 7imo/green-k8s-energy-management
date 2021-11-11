from kubernetes import client, config
import time
import math
from numpy.lib.function_base import select
import pandas as pd
from datetime import datetime
import time

# constants 
START_DATE = '2020-06-01 00:00:00'
END_DATE = '2020-06-02 00:00:00'
INTERVAL_SECONDS = 60
MIXED_SHARE_SOLAR = 0.6
MIXED_SHARE_WIND = 0.6
LOG_MSG = "Updating %s with %s data: %s"



try:
    config.load_incluster_config()
except config.ConfigException:
    try:
        config.load_kube_config()
    except config.ConfigException:
        raise Exception("Could not configure kubernetes python client")

k8s_api = client.CoreV1Api()    


def create_forecasts(df):

    # Forecast
    df['Watt_1h_ahead'] = df['Watt_10min'].rolling(6).sum().shift(-6).fillna(0).astype(int)
    df['Watt_4h_ahead'] = df['Watt_10min'].rolling(24).sum().shift(-24).fillna(0).astype(int)
    df['Watt_12h_ahead'] = df['Watt_10min'].rolling(72).sum().shift(-72).fillna(0).astype(int)
    df['Watt_24h_ahead'] = df['Watt_10min'].rolling(144).sum().shift(-144).fillna(0).astype(int)

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
    df['Watt_10min'] =  round((df['GS_10'] / 1000 * 2.78 * 50 * 6000 * 0.2)).astype(int)

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
    df['Watt_10min'] =  round((math.pi / 2 * 5.1**2 * df['FF_10']**3 * 1.2 * 0.5).astype(int))

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

    mixed_data = round(pd.concat([solar_data, wind_data]).groupby(['MESS_DATUM']).sum()).astype(int)
    mixed_data['renewables_mixed'] = create_renewables_string(mixed_data) 

    return mixed_data


def update_annotation(node_name, renewables):

    # annotation body
    annotations = {
                "metadata": {
                    "annotations": {
                        "renewable": renewables, 
                    }
                }
            }

    # send to k8s
    response = k8s_api.patch_node(node_name, annotations)
    # print(response)


def annotate_nodes(equipped_nodes, data):

    print("%s Starting next annotation ..." % (datetime.now()))

    # equipment of nodes with renewable energy
    for key, value in equipped_nodes:
        if value == 'solar':
            update_annotation(key, data['renewables_solar'])
            print(LOG_MSG % (key, value, data['renewables_solar']))
        elif value == 'wind':
            update_annotation(key, data['renewables_wind'])
            print(LOG_MSG % (key, value, data['renewables_wind']))
        else:
            update_annotation(key, data['renewables_mixed'])
            print(LOG_MSG % (key, value, data['renewables_mixed']))


def merge_outputs(solar_output, wind_output, mixed_output):
    # merge dataframes for easy iteration
    return pd.concat([solar_output['renewables_solar'], wind_output['renewables_wind'], mixed_output['renewables_mixed']], axis=1, keys=['renewables_solar', 'renewables_wind', 'renewables_mixed'])


def assign_equipment():

    # get all nodes in the cluster
    nodes = k8s_api.list_node()
    nodes_list = []
    equipment = {}

    # get list of node names in cluster
    for node in nodes.items:
        nodes_list.append(node.metadata.name)

    # equipment of nodes with renewable energy
    for node in nodes_list:
        if nodes_list.index(node) == 0 or nodes_list.index(node) % 3 == 0:
            # mixed equipment
            equipment.add(node, 'mixed')
            print("Node %s has a mixed equipment" % node)
        elif nodes_list.index(node) % 2 == 0:
            # solar equipment
            equipment.add(node, 'solar')
            print("Node %s has a solar equipment" % node)
        else:
            # wind equipment
            equipment.add(node, 'wind')
            print("Node %s has a wind equipment" % node)

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
        print('Next annotation for timestamp %s: %s, %s, %s' % (index, data['renewables_solar'], data['renewables_wind'], data['renewables_mixed']))
        annotate_nodes(equipped_nodes, data)
        # wait for next interval
        time.sleep(INTERVAL_SECONDS - (time.time() % INTERVAL_SECONDS))

    print("We are done here.")


if __name__ == '__main__':
    main()

