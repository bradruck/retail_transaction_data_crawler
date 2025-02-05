**Description -**

The Measurement/Retail Transaction Data Crawler is an automation to alert the Retail team when data is ready for use
in their Measurement studies. The automation will run on a daily schedule. The source of the study id will be a Jira
ticket that excludes Pinterest and Twitter studies and whose data source excludes Visa studies. The ticket type is
'Measurement Strategy', it is in the 'Retail' vertical and it must be in the 'Transactions' status. Found tickets
that fulfill these criteria are mined for 'Start Date', 'Post-period End Date', Study ID and the Lead Analyst's name
(if available). The study id is used to start two api calls that conclude with the return of the provider id required
for the query search.  Any tickets that fail to return a provider id trigger an email to the retail analytics team
alerting this, since this may indicate the ticket is actually a Visa study. The tickets are then checked to see if
their post-period end date has passed, if this date is still in the future, no further analysis is done. Finally, any
ticket that passes all the checks is subject to a qubole/hive query that looks for the existence of data across the
study period (start date minus one year to the post-period end date plus one day). The query returns a count of all
unique transaction dates as well as the earliest and last transaction dates. The query results are posted as a comment
in the ticket and the lead analyst is alerted to the data availability. If no lead analyst has been assigned the alert
is sent to the default, 'retail analytics'. Finally, the ticket is transitioned to the 'Analytics Processes' status
which automatically assigns the ticket to the lead analyst (if available).

**Application Information -**

Required modules: <ul>
                  <li>main.py,
                  <li>data_crawler_manager.py,
                  <li>api_call_manager.py,
                  <li>jira_manager.py,
                  <li>qubole_manager.py,
                  <li>email_manager.py,
                  <li>provider_transaction_query.py,
                  <li>config.ini
                  </ul>

Location:         <ul>
                  <li>Deployment -> 
                  <li>Scheduled to run once a day, triggered by ActiveBatch-V11 under File/Plan -> 
                  </ul>

Source Code:      <ul>
                  <li>
                  </ul>

LogFile Location: <ul>
                  <li>
                  </ul>

**Contact Information -**

Primary Users:    <ul>
                  <li>
                  </ul>

Lead Customer(s): <ul>
                  <li>?, ?
                  </ul>

Lead Developer:   <ul>
                  <li>Bradley Ruck (bradley.ruck@oracle.com)
                  </ul>

Date Launched:    <ul>
                  <li>July, 2018
                  </ul>
