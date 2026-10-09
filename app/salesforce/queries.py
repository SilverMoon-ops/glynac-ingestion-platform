"""
SOQL queries for all 10 Salesforce objects.
Use these when calling the real Bulk API v2.
"""

SALESFORCE_QUERIES = {
    "Accounts": "SELECT Id, Name, Industry, AnnualRevenue, BillingCity, BillingCountry, CreatedDate FROM Account LIMIT 10000",
    "Contacts": "SELECT Id, FirstName, LastName, Email, Phone, AccountId, CreatedDate FROM Contact LIMIT 10000",
    "Opportunities": "SELECT Id, Name, StageName, Amount, Probability, AccountId, CloseDate FROM Opportunity LIMIT 10000",
    "Leads": "SELECT Id, FirstName, LastName, Company, Status, Email, LeadSource FROM Lead LIMIT 10000",
    "Tasks": "SELECT Id, Subject, Status, Priority, WhoId, ActivityDate FROM Task LIMIT 10000",
    "Cases": "SELECT Id, CaseNumber, Subject, Status, Priority, Origin, AccountId FROM Case LIMIT 10000",
    "Products": "SELECT Id, Name, ProductCode, Family, IsActive FROM Product2 LIMIT 10000",
    "PricebookEntries": "SELECT Id, ProductId, PricebookId, UnitPrice, IsActive FROM PricebookEntry LIMIT 10000",
    "Contracts": "SELECT Id, AccountId, Status, ContractTerm, StartDate FROM Contract LIMIT 10000",
    "Assets": "SELECT Id, Name, AccountId, SerialNumber, Status, InstallDate FROM Asset LIMIT 10000",
}

import re

_FROM = re.compile(r"\bFROM\s+(\w+)", re.IGNORECASE)
_SELECT = re.compile(r"SELECT\s+(.*?)\s+FROM\b", re.IGNORECASE | re.DOTALL)


def sobject_of(soql: str) -> str:
    """'SELECT ... FROM Account LIMIT 1' -> 'Account'."""
    m = _FROM.search(soql)
    if not m:
        raise ValueError(f"cannot find FROM clause in SOQL: {soql!r}")
    return m.group(1)


def fields_of(soql: str) -> list[str]:
    m = _SELECT.search(soql)
    if not m:
        raise ValueError(f"cannot find SELECT list in SOQL: {soql!r}")
    return [f.strip() for f in m.group(1).split(",") if f.strip()]


def object_name_for_soql(soql: str) -> str:
    """Map a SOQL query back to our object name ('Account' -> 'Accounts')."""
    target = sobject_of(soql).lower()
    for name, query in SALESFORCE_QUERIES.items():
        if sobject_of(query).lower() == target:
            return name
    raise ValueError(f"no configured object for SOQL sobject {target!r}")
