Xero Accounting
=============

Description
-----------
This component allows you to extract data from Xero Accounting API and load it into Keboola Connection. It supports multiple endpoints and provides both full and incremental loading capabilities.

**Table of contents:**

[TOC]

Functionality notes
===================
- The component uses OAuth 2.0 for authentication
- Supports both full and incremental data loading
- Handles multiple Xero tenants/organizations
- Automatically refreshes OAuth tokens
- Supports date-based filtering of data
- Handles pagination of API responses automatically

Prerequisites
=============
1. A Xero account with API access
2. A registered Xero application with OAuth 2.0 credentials
3. Required OAuth scopes for the endpoints you want to access
4. Keboola Connection account with appropriate permissions

Features
========

| **Feature**             | **Note**                                      |
|-------------------------|-----------------------------------------------|
| Generic UI form         | Dynamic UI                                    |
| Row Based configuration | Allows structuring the configuration in rows  |
| oAuth                   | OAuth 2.0 authentication with automatic token refresh |
| Incremental loading     | Supports both full and incremental data loads |
| Backfill mode           | Support for seamless backfill setup           |
| Date range filter       | Filter data by modification date              |
| Multi-tenant support    | Download data from multiple Xero organizations|

Supported endpoints
===================
The component supports the following Xero Accounting API endpoints:
- Accounts
- BankTransfers
- BatchPayments
- BrandingThemes
- ContactGroups
- Currencies
- Employees
- ExpenseClaims
- InvoiceReminders
- Items
- LinkedTransactions
- Organisations
- Payments
- Quotes
- RepeatingInvoices
- TaxRates
- Users
- Contacts
- Invoices
- BankTransactions
- Journals
- ManualJournals

If you need more endpoints, please submit your request to
[ideas.keboola.com](https://ideas.keboola.com/)

Configuration
=============

Tenant IDs
----------
Comma-separated list of Tenant IDs to download data from. If left empty, data will be downloaded from all available tenants.

Endpoints
---------
List of Xero Accounting API endpoints to download data from. At least one endpoint must be selected.

Modified Since
-------------
Optional date filter in YYYY-MM-DD format or relative string (e.g., "5 days ago") to only fetch records modified after the specified date.

Load Type
---------
- **Full Load**: Overwrites the destination table with fresh data each run
- **Incremental Load**: Updates existing records and appends new ones based on primary keys

Output
======
The component creates CSV files for each endpoint and tenant combination. Each table includes:
- Primary keys where applicable
- All available fields from the Xero API
- Tenant ID for multi-tenant setups
- Timestamps for incremental loading

Development
-----------

If required, change local data folder (the `CUSTOM_FOLDER` placeholder) path to
your custom path in the `docker-compose.yml` file:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    volumes:
      - ./:/code
      - ./CUSTOM_FOLDER:/data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Clone this repository, init the workspace and run the component with following
command:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
git clone git@bitbucket.org:kds_consulting_team/kds-team.ex-xero-accounting-v2.git kds-team.ex-xero-accounting-v2
cd kds-team.ex-xero-accounting-v2
docker-compose build
docker-compose run --rm dev
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Run the test suite and lint check using this command:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
docker-compose run --rm test
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Integration
===========

For information about deployment and integration with KBC, please refer to the
[deployment section of developers
documentation](https://developers.keboola.com/extend/component/deployment/)
