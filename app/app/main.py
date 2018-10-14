from flask import Flask
from flask_restful import Resource, Api
from flask import request
from flask import jsonify
from flask import make_response
import time
import datetime
from configuration import *

from flask_cors import CORS
from requests_es import ESQ

app =Flask(__name__)
CORS(app)
api = Api(app)

class GetMfInfo(Resource):
    def get(self,request_type):
        print('request type:',request_type)
        dict_response = {}

        if request_type=='test':
            dict_response['response']='test success'
            return make_response(jsonify(dict_response), 200)
        # possible parameters: numer_sta, period_init, period_end , varname


        if request_type=='get_info_for_station' and 'numer_sta' in request.args :

            numer_sta=request.args.get('numer_sta')

            if 'period_init' in request.args and 'period_end' in request.args and 'varname' not in request.args :
                print('getting info for a station for a defined period')
                period_init=request.args.get('period_init')
                period_end=request.args.get('period.end')
                ls_g_obs=ESQ.get_info_for_station_for_period(numer_sta, period_init, period_end)
                dict_response['g_obs']=ls_g_obs
            elif 'period_init' in request.args and 'period_end' in request.args and 'varname'  in request.args:
                print('getting var values for a period')
                period_init=request.args.get('period_init')
                period_end=request.args.get('period.end')
                varname=request.args.get('varname')
                ls_g_obs=ESQ.get_var_values_for_station_for_period(numer_sta, period_init, period_end, varname)
                dict_response['g_obs']=ls_g_obs


            elif ('period_init' not in request.args or 'period_end' not in request.args) and 'varname'  in request.args:
                print('getting the most recent var values for a specific station')
                varname=request.args.get('varname')
                ls_g_obs=ESQ.get_most_recent_var_values_for_station(numer_sta,  varname)
                dict_response['g_obs']=ls_g_obs


            else:
                print('getting the most recent info for a specific station')
                ls_g_obs = ESQ.get_most_recent_info_for_station(numer_sta)
                dict_response['g_obs'] = ls_g_obs

        elif request_type == 'get_hist' and 'numer_sta' in request.args and 'timestamp' in request.args:

            numer_sta=request.args.get('numer_sta')
            timestamp= request.args.get('timestamp')

            if  'varname' in request.args:
                varname = request.args.get('varname')
                ls_g_obs = ESQ.get_hist_for_station(numer_sta, varname, timestamp)
                dict_response['g_obs'] = ls_g_obs
            elif 'varname' not in request.args and 'window_range' in request.args:
                window_range=request.args.get('window_range')
                ls_g_obs = ESQ.get_hist_for_station_time_window(numer_sta, timestamp, window_range)
                dict_response['g_obs'] = ls_g_obs
            else:
                ls_g_obs = ESQ.get_hist_for_station(numer_sta, None, timestamp)
                dict_response['g_obs'] = ls_g_obs


        else:
            dict_response['response']='request not supported'

        return make_response(jsonify(dict_response), 200)


api.add_resource(GetMfInfo,'/<request_type>')


if __name__ == '__main__':
    app.run(host=APP_HOST, debug = APP_DEBUG, port = APP_PORT)
