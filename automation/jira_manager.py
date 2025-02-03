# jira_manager module
# Module holds the class => JiraManager - manages JIRA ticket interface
# Class responsible for all JIRA related interactions including ticket searching, data pull, file attaching, comment
# posting and field updating.
#
from jira import JIRA
from datetime import datetime, timedelta
from urllib.parse import urlparse


class JiraManager(object):
    def __init__(self, url, jira_token):
        self.tickets = []
        self.jira = JIRA(url, basic_auth=jira_token)
        self.date_range = ""
        self.file_name = ""
        self.hub_study_number = ""
        self.pp_end_date = ""
        self.pp_end_date_adj = ""
        self.start_date = ""
        self.lead_analyst = None
        self.today_date = (datetime.now() - timedelta(hours=6)).strftime('%m/%d/%Y')
        self.transaction_data_alert = 'transaction data has been found '
        self.attn_default = 'retailanalytics'
        self.ticket_transitionid = '51'   # id for 'Analytics Processes'

    # Searches Jira for all tickets that match the parent ticket query criteria
    #
    def find_tickets(self, jira_type, jira_status, labels, vertical, media_partner, data_source_hub, text):
        # Query to find corresponding Jira Tickets
        self.tickets = []
        jql_query = "Project in (CAM) AND Type = " + jira_type + " AND Status in " + jira_status + " AND labels not in " + labels + " AND Vertical in " \
                    + vertical + " AND 'Media Partner - HUB' not in " + media_partner + " AND 'Data Source - HUB' ~ " \
                    + data_source_hub + " AND Summary ~ " + text
        self.tickets = self.jira.search_issues(jql_query, maxResults=500)
        if len(self.tickets) > 0:
            return self.tickets
        else:
            return None

    # Retrieves the hub study number from ticket to populate api study call convert to integer type, also returns
    # the post-period end date and start date for hive query
    #
    def ticket_information_pull(self, ticket):
        ticket = self.jira.issue(ticket.key)
        # Converts field value returned link url to tuple via urlparse, selects the item that represents the path [-4],
        # parse this item before selecting the last item [-1] from this string after splitting on '/'
        self.hub_study_number = int(urlparse(ticket.fields.customfield_17018)[-4].split('/')[-1].strip())
        # set the Post-period end date to a plus one day
        self.pp_end_date = datetime.strptime(ticket.fields.customfield_11426, "%Y-%m-%d").strftime("%Y-%m-%d")
        self.pp_end_date_adj = (datetime.strptime(ticket.fields.customfield_11426, "%Y-%m-%d") + timedelta(days=1))\
            .strftime("%Y-%m-%d")
        # set the start date to a minus one year
        self.start_date = (datetime.strptime(ticket.fields.customfield_10431, "%Y-%m-%d") - timedelta(days=365))\
            .strftime("%Y-%m-%d")
        lead_analyst = ticket.fields.customfield_12325
        # check to see if lead analyst field is populated, if not substitute with default
        if lead_analyst is not None:
            self.lead_analyst = '.'.join(str(lead_analyst).split(' '))
        else:
            self.lead_analyst = self.attn_default

        return self.hub_study_number, self.pp_end_date_adj, self.start_date, self.lead_analyst, self.pp_end_date

    # Add a comment to ticket informing lead analyst of data availability and post qubole results
    #
    def add_transaction_data_comment(self, ticket_key, lead_analyst, results):
        cam_ticket = self.jira.issue(ticket_key)
        reporter = cam_ticket.fields.reporter.key
        message = """[~{attention}], {transaction_data_alert} for Ticket =>    *{ticket_id}*
        
                     ||Return Parameter||Result||
                     |Total Days in Period|{days}|
                     |Distinct Days Transaction Count|{trans}|
                     |Earliest Transaction Date|{min}|
                     |Final Transaction Date|{max}|
                     """.format(reporter, attention=lead_analyst,
                                transaction_data_alert=self.transaction_data_alert,
                                ticket_id=ticket_key,
                                days=results[0],
                                trans=results[1],
                                min=results[2],
                                max=results[3]
                                )
        self.jira.add_comment(issue=cam_ticket, body=message)

    # Transition the ticket status field to 'Analytics Processes'
    #
    def progress_ticket(self, ticket_key):
        ticket = self.jira.issue(ticket_key)
        self.jira.transition_issue(ticket, self.ticket_transitionid)

    # Change the field 'labels' in the child ticket to the value 'data_complete' to omit from future search results
    #
    @staticmethod
    def update_field_value(ticket):
        ticket.fields.labels.append(u'data_complete')
        ticket.update(fields={'labels': ticket.fields.labels})

    # Ends the current JIRA session
    #
    def kill_session(self):
        self.jira.kill_session()
