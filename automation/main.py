# Measurement/Retail - Transaction Data Crawler

# Description -
# The Measurement/Retail Transaction Data Crawler is an automation to alert the Retail team when data is ready for use
# in their Measurement studies. The automation will run on a daily schedule. The source of the study id will be a Jira
# ticket that excludes Pinterest and Twitter studies and whose data source excludes Visa studies. The ticket type is
# 'Measurement Strategy', it is in the 'Retail' vertical and it must be in the 'Transactions' status. Found tickets
# that fulfill these criteria are mined for 'Start Date', 'Post-period End Date', Study ID and the Lead Analyst's name
# (if available). The study id is used to start two api calls that conclude with the return of the provider id required
# for the query search.  Any tickets that fail to return a provider id trigger an email to the retail analytics team
# alerting this, since this may indicate the ticket is actually a Visa study. The tickets are then checked to see if
# their post-period end date has passed, if this date is still in the future, no further analysis is done. Finally, any
# ticket that passes all the checks is subject to a qubole/hive query that looks for the existence of data across the
# study period (start date minus one year to the post-period end date plus one day). The query returns a count of all
# unique transaction dates as well as the earliest and last transaction dates. The query results are posted as a comment
# in the ticket and the lead analyst is alerted to the data availability. If no lead analyst has been assigned the alert
# is sent to the default, 'retail analytics'. Finally, the ticket is transitioned to the 'Analytics Processes' status
# which automatically assigns the ticket to the lead analyst (if available).
#
# Application Information -
# Required modules:     main.py,
#                       data_crawler_manager.py,
#                       api_call_manager.py,
#                       jira_manager.py,
#                       qubole_manager.py,
#                       email_manager.py,
#                       provider_transaction_query.py,
#                       config.ini
# Deployed Location:    //prd-use1a-pr-34-ci-operations-01/home/bradley.ruck/Projects/
#                                                                           retail_transaction_data_crawler/
# ActiveBatch Trigger:  //prd-09-abjs-01 (V11)/'Jobs, Folders & Plans'/Operations/Report/Retail_DC/RDC_Once_a_Day
# Source Code:          //gitlab.oracledatacloud.com/odc-operations/Measurement_Retail_Trans_Data_Crawler/
# LogFile Location:     //zfs1/Retail_GTM/Retail_Data_Crawler_Logs/
#
# Contact Information -
# Primary Users:        Measurement/Retail
# Lead Customer(s):     Brian Quinn(brian.quinn@oracle.com), Kristina Merrigan(kristina.merrigan@oracle.com)
# Lead Developer:       Bradley Ruck (bradley.ruck@oracle.com)
# Date Launched:        July, 2018
# Date Updated:

# main module
# Responsible for reading in the basic configurations settings, creating the log file, and creating and launching
# the Retail Data Crawler Manager (RDCM), finally it launches the purge_files method to remove log files that are older
# than a prescribed retention period.
# A console logger option is offered via keyboard input for development purposes when the main.py script is invoked.
# For production, import main as a module and launch the main function as main.main(), which uses 'n' as the default
# input to the the console logger run option.
#
from datetime import datetime, timedelta
import os
import configparser
import logging

from data_crawler_manager import DataCrawlerManager


# Define a console logger for development purposes
#
def console_logger():
    # define Handler that writes DEBUG or higher messages to the sys.stderr
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    # set a simple format for console use
    formatter = logging.Formatter('%(levelname)-7s: %(name)-30s: %(threadName)-12s: %(message)s')
    console.setFormatter(formatter)
    # add the Handler to the root logger
    logging.getLogger('').addHandler(console)


def main(con_opt='n'):
    today_date = (datetime.now() - timedelta(hours=6)).strftime('%Y%m%d-%H%M%S')

    # create a configparser object and open in read mode
    config = configparser.ConfigParser()
    config.read('config.ini')

    # create a dictionary of configuration parameters
    config_params = {
        "jira_url":             config.get('Jira', 'url'),
        "jira_token":           tuple(config.get('Jira', 'authorization').split(',')),
        "jql_type":             config.get('Jira', 'type'),
        "jql_status":           config.get('Jira', 'status'),
        "jql_labels":           config.get('Jira', 'labels'),
        "jql_vertical":         config.get('Jira', 'vertical'),
        "jql_media_partner":    config.get('Jira', 'media_partner'),
        "jql_data_source_hub":  config.get('Jira', 'data_source_hub'),
        "jql_text":             config.get('Jira', 'text'),
        "qubole_token":         config.get('Qubole', 'bradruck-prod-operations-consumer'),
        "cluster_label":        config.get('Qubole', 'cluster-label'),
        "study_url":            config.get('Api', 'study_url', raw=True),
        "account_url":          config.get('Api', 'account_url', raw=True),
        "email_subject":        config.get('Email', 'subject'),
        "email_to":             config.get('Email', 'to'),
        "email_from":           config.get('Email', 'from')
    }

    # logfile path to point to the Operations_limited drive on zfs
    purge_days = config.get('LogFile', 'retention_days')
    log_file_path = config.get('LogFile', 'path')
    logfile_name = '{}{}_{}.log'.format(log_file_path, config.get('Project Details', 'app_name'), today_date)

    # check to see if log file already exits for the day to avoid duplicate execution
    if not os.path.isfile(logfile_name):
        logging.basicConfig(filename=logfile_name,
                            level=logging.INFO,
                            format='%(asctime)s: %(levelname)-7s: %(name)-30s: %(threadName)-12s: %(message)s',
                            datefmt='%m/%d/%Y %H:%M:%S')

        logger = logging.getLogger(__name__)

        # checks for console logger option, default value set to 'n' to not run in production
        if con_opt and con_opt in ['y', 'Y']:
            console_logger()

        logger.info("Process Start - Daily Transaction Data Crawler, Retail - {}\n".format(today_date))

        # create RDCM object and launch the process manager
        retail_trans = DataCrawlerManager(config_params)
        retail_trans.process_manager()

        # search logfile directory for old log files to purge
        retail_trans.purge_files(purge_days, log_file_path)


if __name__ == '__main__':
    # prompt user for use of console logging -> for use in development not production
    ans = input("\nWould you like to enable a console logger for this run?\n Please enter y or n:\t")
    print()
    main(ans)
