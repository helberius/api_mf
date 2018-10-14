from elasticsearch import Elasticsearch
from configuration import *
import datetime

class ESQ():
    es = Elasticsearch([{'host': ELASTICSEARCH_SERVER_IP, 'port': ELASTICSEARCH_SERVER_PORT}])

    def __init__(self):
        print('init of class ESQ')



    @staticmethod
    def get_most_recent_info_for_station(numer_sta):
        """ To get the 1000 most recent observation groups of a given station """
        dict_query = {"sort": [{"date_iso": "desc"}], "from": 0, "size": 1000,
                      "query": {"match": {'numer_sta': numer_sta}}}

        result_query = ESQ.es.search(index=MF_SYNOP_INDEX,body=dict_query)

        ls_g_obs=[]
        for x in result_query['hits']['hits']:
            ls_g_obs.append(x['_source'])

        return ls_g_obs #result_query

    @staticmethod
    def get_info_for_station_for_period(numer_sta, period_init, period_end):
        """ To get the group observations for a defined window frame """
        dict_query = {"sort":[{"date_iso":"desc"}],"from":0, "size":1000 ,
                      "query":{
                          "bool":{
                              "must":[
                                  {"match": {'numer_sta': numer_sta}},
                                  {"range": {"timestamp": {"gte": period_init, "lte": period_end}}}
                              ]}},
                      }
        result_query = ESQ.es.search(index=MF_SYNOP_INDEX,body=dict_query)
        ls_g_obs=[]
        for x in result_query['hits']['hits']:
            ls_g_obs.append(x['_source'])

        return ls_g_obs

    @staticmethod
    def get_var_values_for_station_for_period(numer_sta, period_init, period_end, var):
        """ In this case, I will retrieve the values for a single variable for a station for a defined window frame. """
        dict_query = {"sort": [{"date_iso": "desc"}], "from": 0, "size": 1000,
                      "_source":[var, 'timestamp', 'date_iso'],
                      "query": {
                          "bool": {
                              "must": [
                                  {"match": {'numer_sta': numer_sta}},
                                  {"range": {"timestamp": {"gte": period_init, "lte": period_end}}}
                              ]}},
                      }
        result_query = ESQ.es.search(index=MF_SYNOP_INDEX, body=dict_query)
        ls_g_obs=[]
        for x in result_query['hits']['hits']:
            ls_g_obs.append(x['_source'])

        return ls_g_obs


    @staticmethod
    def get_most_recent_var_values_for_station(numer_sta, var):
        """ To get the most recent observation values (1000) for a single variable for a single station."""
        dict_query = {"sort": [{"date_iso": "desc"}], "from": 0, "size": 1000,
                      "_source": [var, 'timestamp', 'date_iso'],
                      "query": {"match": {'numer_sta': numer_sta}}}

        result_query = ESQ.es.search(index=MF_SYNOP_INDEX, body=dict_query)
        ls_g_obs=[]
        for x in result_query['hits']['hits']:
            ls_g_obs.append(x['_source'])

        return ls_g_obs

    @staticmethod
    def get_hist_for_station(numer_sta, varname, timestamp):
        """
            To get the historical data for a single variable for a single timepoint.
            The user selects a timestamp, and a variable name.
            The app will process the timestamp and calculate the point of the year that this timestamp represents.
            Then it will retrieve similar timepoints for all the other years in the database. for instance.
            May 5th. 2018 , May 5th. 2017, May 5th. 2016 ..... May 5th. 1996
            The results will be grouped by year.
        """
        curr_date = datetime.datetime.now()

        init_data_year = 1996
        end_data_year = curr_date.year

        ls_timestamps = []
        ls_timestamps2 = []
        try:
            x_date = datetime.datetime.fromtimestamp(int(timestamp))

            month = x_date.month
            day = x_date.day
            hour = x_date.hour


            for x in range(init_data_year, (end_data_year + 1)):
                dict_time = {}

                x_datetime = datetime.datetime(x, month, day, hour, 0, 0)


                dict_time['iso'] = x_datetime.isoformat()
                dict_time['timestamp'] = x_datetime.timestamp()

                dict_timestamp = {"match": {'timestamp': x_datetime.timestamp()}}
                ls_timestamps2.append(dict_timestamp)

                # ls_timestamps.append(x_date.timestamp())
                ls_timestamps.append(dict_time)


            if varname is not None:
                dict_query = {"sort": [{"date_iso": "desc"}], "from": 0, "size": 1000,
                              "_source": [varname, 'timestamp', 'date'],
                              "query": {
                                  "bool": {
                                      "must": [
                                          {"match": {'numer_sta': numer_sta}},
                                          {"bool": {"should": ls_timestamps2}}

                                      ]
                                  }
                              },
                              }

            else:

                dict_query = {"sort": [{"date_iso": "desc"}], "from": 0, "size": 1000,
                              "query": {
                                  "bool": {
                                      "must": [
                                          {"match": {'numer_sta': numer_sta}},
                                          {"bool": {"should": ls_timestamps2}}
                                      ]
                                  }
                              },
                              }

            result_query = ESQ.es.search(index=MF_SYNOP_INDEX, body=dict_query)

            ls_results_per_year={}
            for x in result_query['hits']['hits']:
                year=x['_source']['date'][0:4]
                print(year)
                if year in ls_results_per_year:
                    ls_results_per_year[year].append(x['_source'])
                else:
                    ls_results_per_year[year] = []
                    ls_results_per_year[year].append(x['_source'])

            return ls_results_per_year

        except Exception as err:
            print(err)
            return 'error parsing the timestamp'

    @staticmethod
    def get_hist_for_station_time_window(numer_sta, timestamp, window_range):
        """
            To get the historical data for a single station for a timestamp and a defined window range.
            The user selects a station, and a timestamp.
            The app will process the timestamp and calculate the point of the year that this timestamp represents.
            Then it will retrieve similar timepoints for all the other years in the database. for instance.
            May 5th. 2018 , May 5th. 2017, May 5th. 2016 ..... May 5th. 1996
            The window range, is a temporal buffer with units defined in days. Then imagine the window_range is 2 then:
            [[May 3th. 2018, May 7th. 2018],[May 3th. 2017, May 7th. 2017],[May 3th. 2016, May 7th. 2016], ....[May 3th. 1996, May 7th. 1996], ]
            The results will be grouped by year.
        """

        curr_date = datetime.datetime.now()

        init_data_year = 1996
        end_data_year = curr_date.year

        ls_timestamps = []
        x_date=None
        seconds_one_day=86400

        window_range_in_seconds=None

        try:
            window_range_in_seconds=seconds_one_day*int(window_range)
        except Exception as err:

            return str(err)

        try:
            x_date = datetime.datetime.fromtimestamp(int(timestamp))

        except Exception as err:
            return str(err)

        ls_time_filters=[]

        if x_date!=None and window_range_in_seconds!=None:
            month = x_date.month
            day = x_date.day
            hour = x_date.hour

            for x in range(init_data_year, (end_data_year + 1)):
                dict_time = {}


                x_datetime = datetime.datetime(x, month, day, hour, 0, 0)
                lower_window_limit=x_datetime.timestamp()-window_range_in_seconds
                upper_window_limit=x_datetime.timestamp()+window_range_in_seconds

                dict_time['iso'] = x_datetime.isoformat()
                dict_time['timestamp'] = x_datetime.timestamp()
                dict_time['timestamp_lower_window_limit'] = lower_window_limit
                dict_time['timestamp_upper_window_limit'] = upper_window_limit
                dict_time['lower_window_limit'] = datetime.datetime.fromtimestamp(lower_window_limit).isoformat()
                dict_time['upper_window_limit'] = datetime.datetime.fromtimestamp(upper_window_limit).isoformat()

                filter_range_time={"range": {"timestamp": {"gte": lower_window_limit, "lte": upper_window_limit}}}
                ls_time_filters.append(filter_range_time)

                ls_timestamps.append(dict_time)

            dict_query = {"sort": [{"date_iso": "asc"}], "from": 0, "size": 1000,
                          "query": {
                              "bool": {
                                  "must": [
                                      {"match": {'numer_sta': numer_sta}},
                                      {"bool": {"should": ls_time_filters}}
                                  ]
                              }
                          },
                          }
            result_query = ESQ.es.search(index=MF_SYNOP_INDEX, body=dict_query)

            ls_results_per_year={}
            for x in result_query['hits']['hits']:
                year=x['_source']['date'][0:4]
                if year in ls_results_per_year:
                    ls_results_per_year[year].append(x['_source'])
                else:
                    ls_results_per_year[year]=[]
                    ls_results_per_year[year].append(x['_source'])

            return ls_results_per_year