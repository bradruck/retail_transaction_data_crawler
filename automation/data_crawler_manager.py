# data_crawler_manager module
# Module holds the class => DataCrawlerManager - manages the Transaction Data Alert Process
# Class responsible for overall program management
#
from datetime import datetime, timedelta
import time
import os
from multiprocessing.dummy import Pool as ThreadPool
from multiprocessing_logging import install_mp_handler
import logging

from jira_manager import JiraManager
from qubole_manager import QuboleManager
from provider_transaction_query import ProviderTransaction
from api_call_manager import APICallManager
from email_manager import EmailManager

today_date = (datetime.now() - timedelta(hours=6)).strftime('%Y-%m-%d')


class DataCrawlerManager(object):
    def __init__(self, config_params):
        self.jira_url = config_params['jira_url']
        self.jira_token = config_params['jira_token']
        self.jira_pars = JiraManager(self.jira_url, self.jira_token)
        self.jql_type = config_params['jql_type']
        self.jql_status = config_params['jql_status']
        self.jql_labels = config_params['jql_labels']
        self.jql_vertical = config_params['jql_vertical']
        self.jql_media_partner = config_params['jql_media_partner']
        self.jql_data_source_hub = config_params['jql_data_source_hub']
        self.jql_text = config_params['jql_text']
        self.qubole_token = config_params['qubole_token']
        self.cluster_label = config_params['cluster_label']
        self.study_url = config_params['study_url']
        self.account_url = config_params['account_url']
        self.email_subject = config_params['email_subject']
        self.email_to = config_params['email_to']
        self.email_from = config_params['email_from']
        self.tickets = []
        self.tickets_iter = []
        self.not_yet_list = []
        self.logger = logging.getLogger(__name__)

    # Manages the overall automation
    #
    def process_manager(self):
        try:
            # Pulls desired tickets via jql
            self.tickets = self.jira_pars.find_tickets(self.jql_type, self.jql_status, self.jql_labels, self.jql_vertical,
                                                       self.jql_media_partner, self.jql_data_source_hub, self.jql_text)
        except Exception as e:
            self.logger.error("Jira ticket search failed => {}".format(e))
        else:
            # check for an empty ticket list, if not empty, process the list
            if self.tickets is not None:
                self.logger.info("{} ticket(s) were found that match the criteria.\n".format(len(self.tickets)))

                # mines the ticket for data, calls apis and creates iterable for concurrency to populate queries
                self.input_collection_manager()

                # launch the queries concurrently on Qubole
                self.retail_concurrency_manager(self.tickets_iter)
            else:
                # writes log error and exits program
                self.logger.info("\n\nThere were no tickets to process today.\n")

    # Validates ticket for processing, collects and organizes data to run queries, sends alert if no pid
    #
    def input_collection_manager(self):
        # iterates through list of found tickets
        for ticket in self.tickets:

            # fetches the relevant ticket information for the api calls and qubole query
            study_number, pp_end_date_adj, start_date, lead_analyst, pp_end_date = \
                self.jira_pars.ticket_information_pull(ticket)

            # check for past post-period end date, if true process to find pid, else log alert and do nothing else
            if pp_end_date < today_date:
                pid = self.api_manager(study_number)

                # create the iterable required for the concurrency processing, excluding those without a pid
                if pid is not None:
                    self.tickets_iter.append([ticket.key, pid, pp_end_date_adj, start_date, study_number,
                                              lead_analyst, pp_end_date])
                    self.logger.info("Ticket {} will have a Qubole query run, see the concurrent processing "
                                     "section below.".format(ticket.key))
                else:
                    # writes missing pid alert to log file
                    self.logger.warning("Ticket Number: {} has no Provider id, Qubole/Hive query will NOT be run."
                                        .format(ticket))
                    self.logger.info("Ticket data -> Study no.: {}\tPost-period end date (plus 1 day): {}"
                                     "\tStart Date (minus 1 yr): {}\tProvider id: {}"
                                     .format(study_number, pp_end_date_adj, start_date, pid))

                    # send an alert email to notify that a ticket has no associated provider id
                    self.emailer(ticket, study_number, start_date, pp_end_date_adj)
            else:
                self.not_yet_list.append(ticket)

        self.logger.info("")
        # log a list of any tickets that have not yet reached their pp date which is required
        self.logger.info("{} tickets that have not yet passed their Post-processing date: {}"
                         .format(len(self.not_yet_list), [ticket.key for ticket in self.not_yet_list]))

    # Manages the api class instance creation and function calls
    #
    def api_manager(self, id_num):
        # create api search object instance
        api_manager = APICallManager()

        # api call to find study data
        study_call_results = api_manager.api_call(self.study_url, id_num)

        # confirm api call returned results, search to find required data
        if study_call_results is not None:
            parent_company_id = api_manager.parent_id_fetch(study_call_results)
        else:
            return None

        # api call to find parent company data
        provider_call_results = api_manager.api_call(self.account_url, parent_company_id)

        # confirm api call returned results, search to find required data
        if provider_call_results is not None:
            provider_id = api_manager.provider_id_fetch(provider_call_results)
            return provider_id
        else:
            return None

    # Creates the Email Manager instance, launches the emailer module
    #
    def emailer(self, ticket, study_number, start_date, pp_end_date_adj):
        # create emailer object instance
        retail_email = EmailManager(ticket, study_number, start_date, pp_end_date_adj, self.email_subject,
                                    self.email_to, self.email_from)

        # launch the emailer
        retail_email.retail_emailer()

    # Run the qubole/hive query for each of the tickets that qualify
    #
    def retail_concurrency_manager(self, tickets_iter):
        self.logger.info("")
        self.logger.info("Beginning the concurrent processing of {} ticket(s).".format(len(tickets_iter)))
        self.logger.info("\n")

        # activate concurrency logging handler
        install_mp_handler(logger=self.logger)
        # set the logging level of urllib3 to "ERROR" to filter out 'warning level' logging message deluge
        logging.getLogger("urllib3").setLevel(logging.ERROR)

        # launches a thread for each of the tickets
        with ThreadPool(processes=len(tickets_iter)) as pixel_pool:
            try:
                pixel_pool.map(self.query_manager, tickets_iter)
                pixel_pool.close()
                pixel_pool.join()
            except Exception as e:
                self.logger.error("Concurrency run failed => {}".format(e))
            else:
                self.logger.info("")
                self.logger.info("Concluded the concurrent processing")

    # Manages the qubole queries, returns and logs results
    #
    def query_manager(self, ticket_iter):
        # checks that the required ticket information exists, else bypasses Qubole
        if ticket_iter:
            # set the logging level of Qubole to "WARNING" to filter out 'info level' logging message deluge
            logging.getLogger("qds_connection").setLevel(logging.WARNING)

            # create an instance of query object
            query = ProviderTransaction()

            # create an instance of qubole object
            qubole = QuboleManager((ticket_iter[0], "".join(str(ticket_iter[1]))), self.qubole_token,
                                   self.cluster_label, query.max_transact_date_query(ticket_iter[1],
                                   ticket_iter[2], ticket_iter[3]))
            # launch query and return results
            query_results = qubole.get_results()

            # log the study parameters
            self.logger.info("Ticket Number: {}".format(ticket_iter[0]))
            self.logger.info("Study no.: {}\tPost-period end date (plus 1 day): {}\tStart Date (minus 1 yr): {}"
                             "\tProvider id: {}".format(ticket_iter[4], ticket_iter[2], ticket_iter[3], ticket_iter[1]))

            # check that all the study data is available by verifying the max-data-date at least equals the pp-end-date
            if query_results and query_results[3] >= ticket_iter[6]:
                # call function to post results to and progress ticket
                self.ticket_manager(ticket_iter, query_results)
            else:
                # call function to log no results and end of thread
                self.ticket_manager(ticket_iter, None)

    # Confirms output of query, posts results to Jira ticket, transitions ticket to 'Analytics Processes' status
    #
    def ticket_manager(self, ticket_iter, results):
        # verify completeness/existence of results
        if results is not None:
            # writes found data to log file, comments data to ticket, finally transitions ticket
            self.logger.info("Qubole results: Total days: {}, Total transaction date count: {}, "
                             "Earliest Transaction Date: {}, Latest Transaction Date: {}"
                             .format(results[0], results[1], results[2], results[3]))
            self.jira_pars.add_transaction_data_comment(ticket_iter[0], ticket_iter[5], results)
            self.logger.info("A ticket alert has been added as a comment to Jira Ticket: {}".format(ticket_iter[0]))
            self.jira_pars.update_field_value(ticket_iter[0])
            #self.jira_pars.progress_ticket(ticket_iter[0])
            self.logger.info("Ticker {} has had its 'labels' field updated to 'data_complete'".format(ticket_iter[0]))
            #self.logger.info("Ticket {} has been transitioned to the 'Analytics Processes' status".format(ticket_iter[0]))
        else:
            # make no ticket changes if none or incomplete results
            self.logger.info("There is either none or incomplete data, Ticket: {}".format(ticket_iter[0]))

        self.logger.info("End of thread\n")

    # Checks the log directory for all files and removes those after a specified number of days
    #
    def purge_files(self, purge_days, purge_dir):
        try:
            self.logger.info("")
            self.logger.info("Remove {} days old files from the {} directory".format(purge_days, purge_dir))
            now = time.time()
            for file_purge in os.listdir(purge_dir):
                f_obs_path = os.path.join(purge_dir, file_purge)
                if os.stat(f_obs_path).st_mtime < now - int(purge_days) * 86400:
                    time_stamp = time.strptime(time.strftime('%Y-%m-%d %H:%M:%S',
                                                             time.localtime(os.stat(f_obs_path).st_mtime)),
                                               '%Y-%m-%d %H:%M:%S')
                    self.logger.info("Removing File [{}] with timestamp [{}]".format(f_obs_path, time_stamp))

                    os.remove(f_obs_path)

        except Exception as e:
            self.logger.error("{}".format(e))
