# api_call_manager module
# Module holds the class => APICallManager - manages the api interface to fetch provider id for a given study id
# Class responsible for all api related interactions with both the study builder and the odc account service including
# the api call, data fetch, json file dict read and search for data collection
#
import json
import requests
import logging


class APICallManager(object):
    def __init__(self):
        self.key_id = 'id'
        self.key_name = 'name'
        self.key_campaigns = 'campaigns'
        self.start_date = 'startDate'
        self.end_date = 'endDate'
        self.logger = logging.getLogger(__name__)

    # Launch the api call to return a json dictionary to be searched for required data
    #
    def api_call(self, api_url, arg):
        try:
            response = requests.get("{}{}".format(api_url, arg))
        except Exception as e:
            self.logger.error("Failed to create a response object => {}".format(e))
            return None
        else:
            call_dict = response.json()
            return call_dict

    # Returns the parent company id value from api return json dict
    #
    @staticmethod
    def parent_id_fetch(call_dict):
        parent_company_id = call_dict.get('parentCompanyId')
        return parent_company_id

    # Searches for and returns the provider id value from api return json dict
    #
    @staticmethod
    def provider_id_fetch(call_dict):
        for k, v in call_dict.items():
            if k == 'accountReferences':
                for v_dict in v:
                    if v_dict.get('referenceType') == 'provider':
                        provider_id = v_dict.get('referenceId')
                        return provider_id

    # Optional method to load json dictionary from a json file
    #
    @staticmethod
    def json_file_load(json_file="pixel.json"):
        # create a json file
        with open(json_file, 'r') as target:
            data = target.read()
        # load a dictionary from the json file
        pixel_dict = json.loads(data)
        return pixel_dict
