"""One deterministic pool of company names, shared by the Salesforce and HubSpot mock data.

Salesforce Account i and HubSpot Company i get the same name, so the cross-platform
view (v_client_360_deals) has real matches to join on instead of returning nothing.
"""
_PREFIX = ["Apex", "Summit", "Harbor", "Northwind", "Blue Ridge", "Cedar", "Atlas", "Pioneer", "Meridian", "Sterling"]
_SUFFIX = ["Financial", "Capital", "Advisors", "Wealth", "Partners", "Holdings", "Trust", "Group", "Securities", "Asset Management"]


def company_name(i: int) -> str:
    """Unique for i in 0..99."""
    return f"{_PREFIX[i % 10]} {_SUFFIX[(i // 10 + i) % 10]}"
