"""
One-time ingest of Commercial MBS issuers (conduit depositors + CRE-CLO
managers). Runs the standard aup_updater pipeline restricted to the CMBS
issuer keys so existing issuers are not touched.
"""

import aup_updater
from issuers import CMBS_CONDUIT_ISSUERS, CRE_CLO_ISSUERS

aup_updater.ISSUERS = {**CMBS_CONDUIT_ISSUERS, **CRE_CLO_ISSUERS}

if __name__ == "__main__":
    summary = aup_updater.check_for_new_filings()
    print(f"\nIssuers checked: {summary['checked_issuers']}")
    print(f"New filings:     {summary['new_filings']}")
    print(f"Errors:          {summary['errors']}")
    for d in summary["details"]:
        print(f"  {d['issuer_key']:18s} {d['filed_date']}  procs={d['procedures']}  provider={d['aup_provider']}")
