# email_manager module
# Module holds the class => EmailManager - manages the email creation and the smtp interface
# Class responsible for all email related management
#
from smtplib import SMTP
from email.message import EmailMessage
import logging


class EmailManager(object):
    def __init__(self, ticket, study_id, start_date, pp_end_date, subject, to_address, from_address):
        self.ticket = ticket
        self.logger = logging.getLogger(__name__)
        self.msg = ""
        self.subj = subject
        self.to_address = to_address
        self.from_address = from_address
        self.text = "Retail Analytics,\n\n" + \
                    "There appears to be a problem locating the Provider ID. Please find details below:\n\n" + \
                    "Ticket: " + self.ticket.key + "\n\n" + \
                    "Study Number: " + str(study_id) + "\n\n" + \
                    "Study Start Date (minus 1 yr): " + start_date + "\n\n" + \
                    "Study Post-Period End Date (plus 1 day): " + pp_end_date + "\n\n" + \
                    "Thanks,\n" + \
                    "The CI Team - in memoriam"

    # Create the email in a text format then send via smtp
    #
    def retail_emailer(self):
        try:
            # Simple Text Email
            self.msg = EmailMessage()
            self.msg['Subject'] = self.subj
            self.msg['From'] = self.from_address
            self.msg['To'] = self.to_address

            # Message Text
            self.msg.set_content(self.text)

            # Send Email
            with SMTP('mailhost.valkyrie.net') as smtp:
                smtp.send_message(self.msg)

        except Exception as e:
            self.logger.error = ("Email failed for ticket {} => {}".format(self.ticket.key, e))

        else:
            self.logger.warning("An alert email for ticket {} has been sent.".format(self.ticket.key))
            self.logger.info("")
