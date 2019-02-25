# AD version 2

This version of AD is to combine all RDA AD, Chronic OOC AD as well as
ORION AD into one set of code.

## AD mode supported:
- daily
- daily_test
- single
- orion

## Key Changes Compared to V1:

1. Merged ORION AD
2. Support configuration change by cmd
3. Use mu_f10ds package for all common operations

## Key features add:

1. Channel level analysis support
2. Write back analysis result
3. Add script to do database/log files/report files cleaning
4. DF process position conversion
5. remove single context value for certain categories.
6. trending check

## Key features removed:
1. remove last ooc check (pending verification)

[Future Roadmap](https://confluence.micron.com/confluence/display/WWDS/Future+Road+Map)

## Example command to trigger ad
### ORION Mode
### ORION Test Mode

### Daily RDA Mode
ingestion
```bash
#!/bin/bash

/anaconda_env/personal/hdfsf10n/py27orion/bin/python /home/hdfsf10n/apps/chronic_ad_v2/src/analysis_main.py \
--fab 10 --mode daily --save_data --min_query_date `date -d "3 day ago" '+%Y-%m-%d'` \
--config /home/hdfsf10n/apps/chronic_ad_v2/configurations/analysis/analysis_daily_rda.yaml
```

analysis
```bash
#!/bin/bash

/anaconda_env/personal/hdfsf10n/py27orion/bin/python /home/hdfsf10n/apps/chronic_ad_v2/src/analysis_main.py \
--fab 10 --mode daily --save_data --min_query_date `date -d "3 day ago" '+%Y-%m-%d'` \
--config /home/hdfsf10n/apps/chronic_ad_v2/configurations/analysis/analysis_daily_rda.yaml
```

### Daily Chronic Mode
ingestion
```bash
#!/bin/bash

/anaconda_env/personal/hdfsf10n/py27orion/bin/python /home/hdfsf10n/apps/chronic_ad_v2/src/ingest_main.py \
--fab 7 --config /home/hdfsf10n/apps/chronic_ad_v2/configurations/ingest/ingest_daily_chronic.yaml
```

analysis
```bash
#!/bin/bash

/anaconda_env/personal/hdfsf10n/py27orion/bin/python /home/hdfsf10n/apps/chronic_ad_v2/src/analysis_main.py \
--fab 10 --mode daily --save_data --min_query_date `date -d "3 day ago" '+%Y-%m-%d'` \
--config /home/hdfsf10n/apps/chronic_ad_v2/configurations/analysis/analysis_daily_chronic.yaml
```

### Daily RDA Test Mode
daily_test mode
```bash
#!/bin/bash

/anaconda_env/personal/hdfsf10n/py27orion/bin/python /home/hdfsf10n/apps/chronic_ad_v2/src/analysis_main.py \
--fab 10 --mode daily_test --save_data --channel_id 123456 --ckc_id 0 --query_session "2018-12-24 05:00:00" \
--config /home/hdfsf10n/apps/chronic_ad_v2/configurations/analysis/analysis_daily_rda.yaml
```

single mode
```bash
#!/bin/bash

/anaconda_env/personal/hdfsf10n/py27orion/bin/python /home/hdfsf10n/apps/chronic_ad_v2/src/analysis_main.py \
--fab 10 --mode single --save_data --channel_id 123456 --ckc_id 0 --query_session "2018-12-24 05:00:00" \
--config /home/hdfsf10n/apps/chronic_ad_v2/configurations/analysis/analysis_daily_rda.yaml
```

### Daily Chronic Test Mode
daily_test mode
```bash
#!/bin/bash

/anaconda_env/personal/hdfsf10n/py27orion/bin/python /home/hdfsf10n/apps/chronic_ad_v2/src/analysis_main.py \
--fab 10 --mode daily_test --save_data --channel_id 123456 --ckc_id 0 --no_tracking --query_session "2018-12-24 05:00:00" \
--config /home/hdfsf10n/apps/chronic_ad_v2/configurations/analysis/analysis_daily_chronic.yaml
```

single mode
```bash
#!/bin/bash

/anaconda_env/personal/hdfsf10n/py27orion/bin/python /home/hdfsf10n/apps/chronic_ad_v2/src/analysis_main.py \
--fab 10 --mode single --save_data --channel_id 123456 --ckc_id 0 --no_tracking --query_session "2018-12-24 05:00:00" \
--config /home/hdfsf10n/apps/chronic_ad_v2/configurations/analysis/analysis_daily_chronic.yaml
```

### Weekly RDA Mode
### Weekly RDA Test Mode
