1. add parameter billingDate with date type on SP
2. revise logical cut off

there is 2 rules cut off in day 26 and day 1 (first day every month)
rule cut off for 26 is 26th date month to 25 next month
rule cut off for 1 is 01th date month to month end date

A. in this case is cut off day 25
=======================================

if parameter billingDate for example 2025-09-27 so the cut off is 2025-09-26 00:00:00 to 2025-10-25 23:59:59 

if parameter billingDate for example 2025-10-27 so the cut off is 2025-10-26 00:00:00 to 2025-11-25 23:59:59 

B. in this case is cut off day 1
=======================================

if parameter billingDate for example 2025-09-27 so the cut off is 2025-09-01 00:00:00 to 2025-10-30 23:59:59 

if parameter billingDate for example 2025-10-27 so the cut off is 2025-10-01 00:00:00 to 2025-11-31 23:59:59 