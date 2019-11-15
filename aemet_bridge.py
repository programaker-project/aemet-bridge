import os
import logging
import json
from request_cache import DailyRequestCache, DailyTime

REQUEST_CACHE = DailyRequestCache(extra_reset_times=(
    DailyTime(hour=4),
))

from plaza_bridge import (
    PlazaBridge,  # Import bridge functionality
    CallbackBlockArgument,  # Needed for argument definition
    VariableBlockArgument,
    BlockContext,
)

bridge = PlazaBridge(
    name="AEMET",
    endpoint=os.environ['BRIDGE_ENDPOINT'],
    is_public=True,
)

API_KEY=os.environ['API_KEY']  # Can be obtained at https://opendata.aemet.es/centrodedescargas/inicio

# Get location codes
with open(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                       "municipalities_sorted.tsv")) as f:
    MUNICIPALITIES = []
    for line in f.readlines()[1:]: # Ignore header
        line = line.strip()
        if len(line) == 0:
            continue

        code_autonomy, code_province, code_municipality, code_dc, place = line.strip().split("\t")
        full_id = '/'.join(map(str, [code_autonomy, code_province, code_municipality, code_dc]))
        MUNICIPALITIES.append({"id": full_id, "name": place})

def to_aemet_id(full_id):
    _code_autonomy, code_province, code_municipality, _code_dc = full_id.split('/')
    return code_province + code_municipality

@bridge.callback
def get_locations(_extra_data):
    logging.info('DATA locations')
    return MUNICIPALITIES

@bridge.getter(
    id="get_today_max_in_place",
    message="Get today's max temperature for %1",
    arguments=[
        CallbackBlockArgument(str, get_locations),
    ],
    block_result_type=str,
)
def get_max_prediction(place_code, extra_data):
    # Getter logic
    logging.info('GET max-prediction on {}'.format(place_code))
    return get_all_prediction(place_code, extra_data)['prediccion']['dia'][0]['temperatura']['maxima']

@bridge.getter(
    id="get_today_min_in_place",
    message="Get today's min temperature for %1",
    arguments=[
        CallbackBlockArgument(str, get_locations),
    ],
    block_result_type=str,
)
def get_min_prediction(place_code, extra_data):
    logging.info('GET min-prediction on {}'.format(place_code))
    return get_all_prediction(place_code, extra_data)['prediccion']['dia'][0]['temperatura']['minima']

@bridge.getter(
    id="get_prediction_update_time",
    message="Get prediction elaboration time for %1",
    arguments=[
        CallbackBlockArgument(str, get_locations),
    ],
    block_result_type=str,
)
def get_prediction_update_time(place_code, extra_data):
    logging.info('GET prediction-update-time on {}'.format(place_code))
    return get_all_prediction(place_code, extra_data)['elaborado']


@bridge.operation(
    id="get_today_rain_probability_in_place",
    message="Get today's rain probabilities for %1. Save to %2",
    arguments=[
        CallbackBlockArgument(str, get_locations),
        VariableBlockArgument(list),
    ],
    save_to=BlockContext.ARGUMENTS[1],
)
def get_rain_prediction(place_code, extra_data):
    logging.info('GET rain-prediction on {}'.format(place_code))
    return get_all_prediction(place_code, extra_data)['prediccion']['dia'][0]['probPrecipitacion']

# @bridge.operation(
#     id="get_all_data_from_place",
#     message="Get all data for %1. Save to %2",
#     arguments=[
#         CallbackBlockArgument(str, get_locations),
#         VariableBlockArgument(list),
#     ],
#     save_to=BlockContext.ARGUMENTS[1],
# )
def get_all_prediction(place_code, extra_data):
    # Getter logic
    r = REQUEST_CACHE.request(
        "https://opendata.aemet.es/opendata/api/prediccion/especifica/municipio/diaria/{id}?api_key={api_key}"
        .format(id=to_aemet_id(place_code), api_key=API_KEY))
    pointers = json.loads(r)
    data_req = REQUEST_CACHE.request(pointers['datos'])

    data = data_req.decode('latin1')
    return json.loads(data)[0]


if __name__ == '__main__':
   logging.basicConfig(format="%(asctime)s - %(levelname)s [%(filename)s] %(message)s")
   logging.getLogger().setLevel(logging.INFO)

   logging.info('Starting bridge')
   bridge.run()
