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