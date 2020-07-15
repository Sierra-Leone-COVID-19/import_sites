Import TEIs
===========

`import_teis.py` imports DHIS2 "Quarantine Site" Tracked Entity
Instances as CommCare "quarantine_facility" cases.

Requires Python 3.5 or higher.

### Usage

1. Clone the repository and install Python requirements:

        $ git clone https://github.com/Sierra-Leone-COVID-19/import_sites.git
        $ cd import_sites
        $ pip install -r requirements.txt

2. Set environment variables for your credentials:

        $ export DHIS2_USERNAME='example_username'
        $ export DHIS2_PASSWORD='example.password'
        $ export COMMCARE_USERNAME='user@example.commcarehq.org'
        $ export COMMCARE_PASSWORD='example-password'

3. Run:

        $ python3 import_teis.py
