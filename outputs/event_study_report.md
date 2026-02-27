# Delist & Merger Event Study


## 1. ACTIONS event type overview

| Action | Count | Earliest | Latest |
|--------|-------|----------|--------|
| dividend | 524,814 | 1997-12-31 00:00:00 | 2026-02-23 00:00:00 |
| listed | 22,951 | 1997-12-31 00:00:00 | 2026-02-20 00:00:00 |
| delisted | 19,026 | 1997-12-31 00:00:00 | 2026-02-23 00:00:00 |
| tickerchangefrom | 13,281 | 1998-01-02 00:00:00 | 2026-02-23 00:00:00 |
| tickerchangeto | 13,281 | 1998-01-02 00:00:00 | 2026-02-23 00:00:00 |
| split | 12,408 | 1997-12-31 00:00:00 | 2026-02-24 00:00:00 |
| relation | 9,750 | 1997-12-31 00:00:00 | 2026-02-23 00:00:00 |
| initiated | 8,384 | 1997-12-31 00:00:00 | 1997-12-31 00:00:00 |
| acquisitionby | 8,137 | 1998-01-07 00:00:00 | 2026-02-13 00:00:00 |
| acquisitionof | 8,137 | 1998-01-07 00:00:00 | 2026-02-13 00:00:00 |
| bankruptcyliquidation | 3,326 | 1998-01-06 00:00:00 | 2026-02-06 00:00:00 |
| regulatorydelisting | 834 | 1997-12-31 00:00:00 | 2026-02-23 00:00:00 |
| spunofffrom | 556 | 1998-01-02 00:00:00 | 2026-01-05 00:00:00 |
| spinoff | 556 | 1998-01-02 00:00:00 | 2026-01-05 00:00:00 |
| spinoffdividend | 515 | 1997-12-31 00:00:00 | 2026-01-05 00:00:00 |
| voluntarydelisting | 367 | 1998-03-16 00:00:00 | 2026-01-16 00:00:00 |
| adrratiosplit | 365 | 2000-03-21 00:00:00 | 2026-02-02 00:00:00 |
| mergerfrom | 132 | 1998-06-26 00:00:00 | 2025-12-02 00:00:00 |
| mergerto | 132 | 1998-06-26 00:00:00 | 2025-12-02 00:00:00 |

### Terminal events (cause ticker to stop trading)

| Action | Distinct Tickers | Events | Earliest | Latest |
|--------|-----------------|--------|----------|--------|
| delisted | 19,026 | 19,026 | 1997-12-31 00:00:00 | 2026-02-23 00:00:00 |
| acquisitionby | 7,408 | 8,137 | 1998-01-07 00:00:00 | 2026-02-13 00:00:00 |
| bankruptcyliquidation | 3,326 | 3,326 | 1998-01-06 00:00:00 | 2026-02-06 00:00:00 |
| regulatorydelisting | 834 | 834 | 1997-12-31 00:00:00 | 2026-02-23 00:00:00 |
| voluntarydelisting | 367 | 367 | 1998-03-16 00:00:00 | 2026-01-16 00:00:00 |
| mergerfrom | 113 | 132 | 1998-06-26 00:00:00 | 2025-12-02 00:00:00 |

## 2. ACTIONS fields for terminal events (sample)

Checking: ticker, date, action, value, contraticker, name

### delisted (5 most recent)

| ticker   | date                | action   |   value | contraticker   | name                                          |
|:---------|:--------------------|:---------|--------:|:---------------|:----------------------------------------------|
| MYD      | 2026-02-23 00:00:00 | delisted |     nan |                | BLACKROCK MUNIYIELD FUND INC                  |
| MVT      | 2026-02-23 00:00:00 | delisted |     nan |                | BLACKROCK MUNIVEST FUND II INC                |
| MVF      | 2026-02-23 00:00:00 | delisted |     nan |                | BLACKROCK MUNIVEST FUND INC                   |
| MQT      | 2026-02-23 00:00:00 | delisted |     nan |                | BLACKROCK MUNIYIELD QUALITY FUND II INC       |
| BTA      | 2026-02-23 00:00:00 | delisted |     nan |                | BLACKROCK LONG-TERM MUNICIPAL ADVANTAGE TRUST |

### bankruptcyliquidation (5 most recent)

| ticker   | date                | action                |   value | contraticker   | name                            |
|:---------|:--------------------|:----------------------|--------:|:---------------|:--------------------------------|
| CREVF    | 2026-02-06 00:00:00 | bankruptcyliquidation |     0.8 |                | CARBON REVOLUTION PUBLIC LTD CO |
| TWNPQ    | 2026-02-03 00:00:00 | bankruptcyliquidation |     4.1 |                | TWIN HOSPITALITY GROUP INC      |
| FATAQ    | 2026-02-03 00:00:00 | bankruptcyliquidation |     2.9 |                | FAT BRANDS INC                  |
| AAM      | 2026-02-02 00:00:00 | bankruptcyliquidation |   468.8 |                | AA MISSION ACQUISITION CORP     |
| NINE     | 2026-01-30 00:00:00 | bankruptcyliquidation |    25.4 |                | NINE ENERGY SERVICE INC         |

### acquisitionby (5 most recent)

| ticker   | date                | action        |   value | contraticker   | name                           |
|:---------|:--------------------|:--------------|--------:|:---------------|:-------------------------------|
| SNCR     | 2026-02-13 00:00:00 | acquisitionby |   103.6 |                | SYNCHRONOSS TECHNOLOGIES INC   |
| MOFG     | 2026-02-13 00:00:00 | acquisitionby |  1017.4 | NIC            | MIDWESTONE FINANCIAL GROUP INC |
| ZEUS     | 2026-02-12 00:00:00 | acquisitionby |   535.9 | RYI            | OLYMPIC STEEL INC              |
| SOHO     | 2026-02-12 00:00:00 | acquisitionby |    46.1 |                | SOTHERLY HOTELS INC            |
| SOHO     | 2026-02-12 00:00:00 | acquisitionby |    46.1 |                | SOTHERLY HOTELS INC            |

### mergerfrom (5 most recent)

| ticker   | date                | action     |   value | contraticker   | name                       |
|:---------|:--------------------|:-----------|--------:|:---------------|:---------------------------|
| PURR     | 2025-12-02 00:00:00 | mergerfrom |     8.5 | SONN           | HYPERLIQUID STRATEGIES INC |
| ELVR     | 2025-08-29 00:00:00 | mergerfrom |   159.1 | PLL            | ELEVRA LITHIUM LTD         |
| KG       | 2025-05-28 00:00:00 | mergerfrom |  3289.5 | MHLD           | KESTREL GROUP LTD          |
| LION     | 2025-05-06 00:00:00 | mergerfrom |  1960.1 | LION2          | LIONSGATE STUDIOS CORP     |
| LION     | 2025-05-06 00:00:00 | mergerfrom |  1849.3 | LGF.B          | LIONSGATE STUDIOS CORP     |

### regulatorydelisting (5 most recent)

| ticker   | date                | action              |   value | contraticker   | name                     |
|:---------|:--------------------|:--------------------|--------:|:---------------|:-------------------------|
| ABP      | 2026-02-23 00:00:00 | regulatorydelisting |     1.4 |                | ABPRO HOLDINGS INC       |
| SSKN     | 2026-02-19 00:00:00 | regulatorydelisting |     1   |                | STRATA SKIN SCIENCES INC |
| STAI     | 2026-02-09 00:00:00 | regulatorydelisting |     5.6 |                | SCANTECH AI SYSTEMS INC  |
| VERO     | 2026-02-06 00:00:00 | regulatorydelisting |     2.1 |                | VENUS CONCEPT INC        |
| SYBX     | 2026-01-20 00:00:00 | regulatorydelisting |     6.6 |                | SYNLOGIC INC             |

### voluntarydelisting (5 most recent)

| ticker   | date                | action             |   value | contraticker   | name                                 |
|:---------|:--------------------|:-------------------|--------:|:---------------|:-------------------------------------|
| TELFY    | 2026-01-16 00:00:00 | voluntarydelisting | 21603.3 |                | TELEFONICA S A                       |
| GRP.U    | 2026-01-02 00:00:00 | voluntarydelisting |  3745.3 |                | GRANITE REAL ESTATE INVESTMENT TRUST |
| OMCC     | 2025-12-31 00:00:00 | voluntarydelisting |    35.2 |                | OLD MARKET CAPITAL CORP              |
| TTSH     | 2025-12-26 00:00:00 | voluntarydelisting |   164.6 |                | TILE SHOP HOLDINGS INC               |
| GLBZ     | 2025-12-22 00:00:00 | voluntarydelisting |    12.6 |                | GLEN BURNIE BANCORP                  |

## 3. Case studies: price behavior around terminal events

Found 12 terminal events for study tickers:

| ticker   | event_date          | action                |   value | contraticker   |
|:---------|:--------------------|:----------------------|--------:|:---------------|
| CIT      | 2022-01-03 00:00:00 | delisted              |  5305.6 |                |
| CIT      | 2022-01-03 00:00:00 | acquisitionby         |  5305.6 | FCNCA          |
| DNKN     | 2020-12-14 00:00:00 | delisted              |  8775.8 |                |
| DNKN     | 2020-12-14 00:00:00 | acquisitionby         |  8775.8 |                |
| ETFC     | 2020-10-02 00:00:00 | delisted              | 10891.2 |                |
| ETFC     | 2020-10-02 00:00:00 | acquisitionby         | 10891.2 | MS             |
| MON      | 2022-12-23 00:00:00 | delisted              |   314.7 |                |
| MON      | 2022-12-23 00:00:00 | bankruptcyliquidation |   314.7 |                |
| TIF      | 2021-01-06 00:00:00 | delisted              | 15960.7 |                |
| TIF      | 2021-01-06 00:00:00 | acquisitionby         | 15960.7 | LVMUY          |
| TWX      | 2018-06-15 00:00:00 | acquisitionby         | 77269.7 | T              |
| TWX      | 2018-06-15 00:00:00 | delisted              | 77269.7 |                |

### CIT — delisted on 2022-01-03 00:00:00

  - **Last SEP date**: 2022-01-03 00:00:00
  - **Event date (ACTIONS)**: 2022-01-03 00:00:00
  - **Gap (calendar days)**: 0
  - **Last closeadj**: $53.50
  - **Last closeunadj**: $53.50
  - **ACTIONS value (final mktcap $M)**: 5305.6
  - **Return over last ~60 cal days of trading**: 5.4%

  Last 10 trading days:

| date                | closeadj   | closeunadj   | volume     |
|:--------------------|:-----------|:-------------|:-----------|
| 2021-12-20 00:00:00 | $50.94     | $50.94       | 2,232,083  |
| 2021-12-21 00:00:00 | $52.26     | $52.26       | 861,538    |
| 2021-12-22 00:00:00 | $53.12     | $53.12       | 696,496    |
| 2021-12-23 00:00:00 | $53.61     | $53.61       | 508,941    |
| 2021-12-27 00:00:00 | $53.32     | $53.32       | 356,744    |
| 2021-12-28 00:00:00 | $52.84     | $52.84       | 910,399    |
| 2021-12-29 00:00:00 | $51.57     | $51.57       | 3,335,143  |
| 2021-12-30 00:00:00 | $50.30     | $50.30       | 2,087,364  |
| 2021-12-31 00:00:00 | $51.34     | $51.34       | 1,782,885  |
| 2022-01-03 00:00:00 | $53.50     | $53.50       | 32,809,510 |

### CIT — acquisitionby on 2022-01-03 00:00:00

  - **Last SEP date**: 2022-01-03 00:00:00
  - **Event date (ACTIONS)**: 2022-01-03 00:00:00
  - **Gap (calendar days)**: 0
  - **Last closeadj**: $53.50
  - **Last closeunadj**: $53.50
  - **ACTIONS value (final mktcap $M)**: 5305.6
  - **Return over last ~60 cal days of trading**: 5.4%

  Last 10 trading days:

| date                | closeadj   | closeunadj   | volume     |
|:--------------------|:-----------|:-------------|:-----------|
| 2021-12-20 00:00:00 | $50.94     | $50.94       | 2,232,083  |
| 2021-12-21 00:00:00 | $52.26     | $52.26       | 861,538    |
| 2021-12-22 00:00:00 | $53.12     | $53.12       | 696,496    |
| 2021-12-23 00:00:00 | $53.61     | $53.61       | 508,941    |
| 2021-12-27 00:00:00 | $53.32     | $53.32       | 356,744    |
| 2021-12-28 00:00:00 | $52.84     | $52.84       | 910,399    |
| 2021-12-29 00:00:00 | $51.57     | $51.57       | 3,335,143  |
| 2021-12-30 00:00:00 | $50.30     | $50.30       | 2,087,364  |
| 2021-12-31 00:00:00 | $51.34     | $51.34       | 1,782,885  |
| 2022-01-03 00:00:00 | $53.50     | $53.50       | 32,809,510 |

  Acquirer/survivor: FCNCA
  FCNCA price around event:
| date                |   closeadj |
|:--------------------|-----------:|
| 2021-12-29 00:00:00 |    821.341 |
| 2021-12-30 00:00:00 |    799.861 |
| 2021-12-31 00:00:00 |    818.422 |
| 2022-01-03 00:00:00 |    847.93  |
| 2022-01-04 00:00:00 |    872.547 |
| 2022-01-05 00:00:00 |    827.466 |
| 2022-01-06 00:00:00 |    858.296 |
| 2022-01-07 00:00:00 |    893.613 |

### DNKN — delisted on 2020-12-14 00:00:00

  - **Last SEP date**: 2020-12-14 00:00:00
  - **Event date (ACTIONS)**: 2020-12-14 00:00:00
  - **Gap (calendar days)**: 0
  - **Last closeadj**: $106.48
  - **Last closeunadj**: $106.48
  - **ACTIONS value (final mktcap $M)**: 8775.8
  - **Return over last ~60 cal days of trading**: 22.9%

  Last 10 trading days:

| date                | closeadj   | closeunadj   | volume    |
|:--------------------|:-----------|:-------------|:----------|
| 2020-12-01 00:00:00 | $106.32    | $106.32      | 1,969,307 |
| 2020-12-02 00:00:00 | $106.34    | $106.34      | 1,113,207 |
| 2020-12-03 00:00:00 | $106.39    | $106.39      | 1,244,156 |
| 2020-12-04 00:00:00 | $106.34    | $106.34      | 1,070,723 |
| 2020-12-07 00:00:00 | $106.42    | $106.42      | 813,056   |
| 2020-12-08 00:00:00 | $106.41    | $106.41      | 633,934   |
| 2020-12-09 00:00:00 | $106.45    | $106.45      | 1,067,200 |
| 2020-12-10 00:00:00 | $106.45    | $106.45      | 579,665   |
| 2020-12-11 00:00:00 | $106.42    | $106.42      | 1,080,569 |
| 2020-12-14 00:00:00 | $106.48    | $106.48      | 1,826,521 |

### DNKN — acquisitionby on 2020-12-14 00:00:00

  - **Last SEP date**: 2020-12-14 00:00:00
  - **Event date (ACTIONS)**: 2020-12-14 00:00:00
  - **Gap (calendar days)**: 0
  - **Last closeadj**: $106.48
  - **Last closeunadj**: $106.48
  - **ACTIONS value (final mktcap $M)**: 8775.8
  - **Return over last ~60 cal days of trading**: 22.9%

  Last 10 trading days:

| date                | closeadj   | closeunadj   | volume    |
|:--------------------|:-----------|:-------------|:----------|
| 2020-12-01 00:00:00 | $106.32    | $106.32      | 1,969,307 |
| 2020-12-02 00:00:00 | $106.34    | $106.34      | 1,113,207 |
| 2020-12-03 00:00:00 | $106.39    | $106.39      | 1,244,156 |
| 2020-12-04 00:00:00 | $106.34    | $106.34      | 1,070,723 |
| 2020-12-07 00:00:00 | $106.42    | $106.42      | 813,056   |
| 2020-12-08 00:00:00 | $106.41    | $106.41      | 633,934   |
| 2020-12-09 00:00:00 | $106.45    | $106.45      | 1,067,200 |
| 2020-12-10 00:00:00 | $106.45    | $106.45      | 579,665   |
| 2020-12-11 00:00:00 | $106.42    | $106.42      | 1,080,569 |
| 2020-12-14 00:00:00 | $106.48    | $106.48      | 1,826,521 |

### ETFC — delisted on 2020-10-02 00:00:00

  - **Last SEP date**: 2020-10-02 00:00:00
  - **Event date (ACTIONS)**: 2020-10-02 00:00:00
  - **Gap (calendar days)**: 0
  - **Last closeadj**: $49.26
  - **Last closeunadj**: $49.26
  - **ACTIONS value (final mktcap $M)**: 10891.2
  - **Return over last ~60 cal days of trading**: -4.0%

  Last 10 trading days:

| date                | closeadj   | closeunadj   | volume     |
|:--------------------|:-----------|:-------------|:-----------|
| 2020-09-21 00:00:00 | $50.02     | $50.02       | 3,685,330  |
| 2020-09-22 00:00:00 | $49.30     | $49.30       | 1,663,909  |
| 2020-09-23 00:00:00 | $48.02     | $48.02       | 1,151,519  |
| 2020-09-24 00:00:00 | $48.21     | $48.21       | 1,808,900  |
| 2020-09-25 00:00:00 | $48.63     | $48.63       | 1,869,632  |
| 2020-09-28 00:00:00 | $50.10     | $50.10       | 1,385,858  |
| 2020-09-29 00:00:00 | $48.93     | $48.93       | 1,642,761  |
| 2020-09-30 00:00:00 | $50.05     | $50.05       | 2,850,135  |
| 2020-10-01 00:00:00 | $49.26     | $49.26       | 20,692,447 |
| 2020-10-02 00:00:00 | $49.26     | $49.26       | 1          |

### ETFC — acquisitionby on 2020-10-02 00:00:00

  - **Last SEP date**: 2020-10-02 00:00:00
  - **Event date (ACTIONS)**: 2020-10-02 00:00:00
  - **Gap (calendar days)**: 0
  - **Last closeadj**: $49.26
  - **Last closeunadj**: $49.26
  - **ACTIONS value (final mktcap $M)**: 10891.2
  - **Return over last ~60 cal days of trading**: -4.0%

  Last 10 trading days:

| date                | closeadj   | closeunadj   | volume     |
|:--------------------|:-----------|:-------------|:-----------|
| 2020-09-21 00:00:00 | $50.02     | $50.02       | 3,685,330  |
| 2020-09-22 00:00:00 | $49.30     | $49.30       | 1,663,909  |
| 2020-09-23 00:00:00 | $48.02     | $48.02       | 1,151,519  |
| 2020-09-24 00:00:00 | $48.21     | $48.21       | 1,808,900  |
| 2020-09-25 00:00:00 | $48.63     | $48.63       | 1,869,632  |
| 2020-09-28 00:00:00 | $50.10     | $50.10       | 1,385,858  |
| 2020-09-29 00:00:00 | $48.93     | $48.93       | 1,642,761  |
| 2020-09-30 00:00:00 | $50.05     | $50.05       | 2,850,135  |
| 2020-10-01 00:00:00 | $49.26     | $49.26       | 20,692,447 |
| 2020-10-02 00:00:00 | $49.26     | $49.26       | 1          |

  Acquirer/survivor: MS
  MS price around event:
| date                |   closeadj |
|:--------------------|-----------:|
| 2020-09-28 00:00:00 |     40.771 |
| 2020-09-29 00:00:00 |     39.81  |
| 2020-09-30 00:00:00 |     40.745 |
| 2020-10-01 00:00:00 |     39.827 |
| 2020-10-02 00:00:00 |     40.425 |
| 2020-10-05 00:00:00 |     40.956 |
| 2020-10-06 00:00:00 |     40.231 |
| 2020-10-07 00:00:00 |     41.049 |

### MON — delisted on 2022-12-23 00:00:00

  - **Last SEP date**: 2022-12-23 00:00:00
  - **Event date (ACTIONS)**: 2022-12-23 00:00:00
  - **Gap (calendar days)**: 0
  - **Last closeadj**: $10.07
  - **Last closeunadj**: $10.07
  - **ACTIONS value (final mktcap $M)**: 314.7
  - **Return over last ~60 cal days of trading**: 1.2%

  Last 10 trading days:

| date                | closeadj   | closeunadj   | volume   |
|:--------------------|:-----------|:-------------|:---------|
| 2022-12-12 00:00:00 | $10.04     | $10.04       | 31,768   |
| 2022-12-13 00:00:00 | $10.02     | $10.02       | 20,188   |
| 2022-12-14 00:00:00 | $10.04     | $10.04       | 37,184   |
| 2022-12-15 00:00:00 | $10.16     | $10.16       | 45,346   |
| 2022-12-16 00:00:00 | $10.18     | $10.18       | 37,378   |
| 2022-12-19 00:00:00 | $10.57     | $10.57       | 45,006   |
| 2022-12-20 00:00:00 | $10.20     | $10.20       | 21,380   |
| 2022-12-21 00:00:00 | $10.20     | $10.20       | 11,575   |
| 2022-12-22 00:00:00 | $10.05     | $10.05       | 69,372   |
| 2022-12-23 00:00:00 | $10.07     | $10.07       | 26,349   |

### MON — bankruptcyliquidation on 2022-12-23 00:00:00

  - **Last SEP date**: 2022-12-23 00:00:00
  - **Event date (ACTIONS)**: 2022-12-23 00:00:00
  - **Gap (calendar days)**: 0
  - **Last closeadj**: $10.07
  - **Last closeunadj**: $10.07
  - **ACTIONS value (final mktcap $M)**: 314.7
  - **Return over last ~60 cal days of trading**: 1.2%

  Last 10 trading days:

| date                | closeadj   | closeunadj   | volume   |
|:--------------------|:-----------|:-------------|:---------|
| 2022-12-12 00:00:00 | $10.04     | $10.04       | 31,768   |
| 2022-12-13 00:00:00 | $10.02     | $10.02       | 20,188   |
| 2022-12-14 00:00:00 | $10.04     | $10.04       | 37,184   |
| 2022-12-15 00:00:00 | $10.16     | $10.16       | 45,346   |
| 2022-12-16 00:00:00 | $10.18     | $10.18       | 37,378   |
| 2022-12-19 00:00:00 | $10.57     | $10.57       | 45,006   |
| 2022-12-20 00:00:00 | $10.20     | $10.20       | 21,380   |
| 2022-12-21 00:00:00 | $10.20     | $10.20       | 11,575   |
| 2022-12-22 00:00:00 | $10.05     | $10.05       | 69,372   |
| 2022-12-23 00:00:00 | $10.07     | $10.07       | 26,349   |

### TIF — delisted on 2021-01-06 00:00:00

  - **Last SEP date**: 2021-01-06 00:00:00
  - **Event date (ACTIONS)**: 2021-01-06 00:00:00
  - **Gap (calendar days)**: 0
  - **Last closeadj**: $131.46
  - **Last closeunadj**: $131.46
  - **ACTIONS value (final mktcap $M)**: 15960.7
  - **Return over last ~60 cal days of trading**: 0.7%

  Last 10 trading days:

| date                | closeadj   | closeunadj   | volume    |
|:--------------------|:-----------|:-------------|:----------|
| 2020-12-22 00:00:00 | $131.15    | $131.15      | 1,098,454 |
| 2020-12-23 00:00:00 | $131.15    | $131.15      | 1,117,716 |
| 2020-12-24 00:00:00 | $131.16    | $131.16      | 433,142   |
| 2020-12-28 00:00:00 | $131.31    | $131.31      | 864,414   |
| 2020-12-29 00:00:00 | $131.33    | $131.33      | 1,533,917 |
| 2020-12-30 00:00:00 | $131.35    | $131.35      | 2,794,205 |
| 2020-12-31 00:00:00 | $131.45    | $131.45      | 1,385,750 |
| 2021-01-04 00:00:00 | $131.44    | $131.44      | 2,840,203 |
| 2021-01-05 00:00:00 | $131.43    | $131.43      | 2,377,944 |
| 2021-01-06 00:00:00 | $131.46    | $131.46      | 6,340,608 |

### TIF — acquisitionby on 2021-01-06 00:00:00

  - **Last SEP date**: 2021-01-06 00:00:00
  - **Event date (ACTIONS)**: 2021-01-06 00:00:00
  - **Gap (calendar days)**: 0
  - **Last closeadj**: $131.46
  - **Last closeunadj**: $131.46
  - **ACTIONS value (final mktcap $M)**: 15960.7
  - **Return over last ~60 cal days of trading**: 0.7%

  Last 10 trading days:

| date                | closeadj   | closeunadj   | volume    |
|:--------------------|:-----------|:-------------|:----------|
| 2020-12-22 00:00:00 | $131.15    | $131.15      | 1,098,454 |
| 2020-12-23 00:00:00 | $131.15    | $131.15      | 1,117,716 |
| 2020-12-24 00:00:00 | $131.16    | $131.16      | 433,142   |
| 2020-12-28 00:00:00 | $131.31    | $131.31      | 864,414   |
| 2020-12-29 00:00:00 | $131.33    | $131.33      | 1,533,917 |
| 2020-12-30 00:00:00 | $131.35    | $131.35      | 2,794,205 |
| 2020-12-31 00:00:00 | $131.45    | $131.45      | 1,385,750 |
| 2021-01-04 00:00:00 | $131.44    | $131.44      | 2,840,203 |
| 2021-01-05 00:00:00 | $131.43    | $131.43      | 2,377,944 |
| 2021-01-06 00:00:00 | $131.46    | $131.46      | 6,340,608 |

  Acquirer/survivor: LVMUY
  No SEP data for LVMUY around event date

### TWX — acquisitionby on 2018-06-15 00:00:00

  - **Last SEP date**: 2018-06-15 00:00:00
  - **Event date (ACTIONS)**: 2018-06-15 00:00:00
  - **Gap (calendar days)**: 0
  - **Last closeadj**: $98.77
  - **Last closeunadj**: $98.77
  - **ACTIONS value (final mktcap $M)**: 77269.7
  - **Return over last ~60 cal days of trading**: 1.5%

  Last 10 trading days:

| date                | closeadj   | closeunadj   | volume     |
|:--------------------|:-----------|:-------------|:-----------|
| 2018-06-04 00:00:00 | $93.46     | $93.46       | 3,460,972  |
| 2018-06-05 00:00:00 | $93.46     | $93.46       | 3,847,752  |
| 2018-06-06 00:00:00 | $94.92     | $94.92       | 7,783,309  |
| 2018-06-07 00:00:00 | $95.37     | $95.37       | 5,593,905  |
| 2018-06-08 00:00:00 | $95.34     | $95.34       | 8,902,660  |
| 2018-06-11 00:00:00 | $96.17     | $96.17       | 10,895,082 |
| 2018-06-12 00:00:00 | $96.22     | $96.22       | 22,939,516 |
| 2018-06-13 00:00:00 | $97.95     | $97.95       | 61,986,356 |
| 2018-06-14 00:00:00 | $98.77     | $98.77       | 29,391,076 |
| 2018-06-15 00:00:00 | $98.77     | $98.77       | 8,475      |

  Acquirer/survivor: T
  T price around event:
| date                |   closeadj |
|:--------------------|-----------:|
| 2018-06-11 00:00:00 |     16.321 |
| 2018-06-12 00:00:00 |     16.402 |
| 2018-06-13 00:00:00 |     15.385 |
| 2018-06-14 00:00:00 |     15.528 |
| 2018-06-15 00:00:00 |     15.829 |
| 2018-06-18 00:00:00 |     15.371 |
| 2018-06-19 00:00:00 |     15.466 |
| 2018-06-20 00:00:00 |     15.28  |

### TWX — delisted on 2018-06-15 00:00:00

  - **Last SEP date**: 2018-06-15 00:00:00
  - **Event date (ACTIONS)**: 2018-06-15 00:00:00
  - **Gap (calendar days)**: 0
  - **Last closeadj**: $98.77
  - **Last closeunadj**: $98.77
  - **ACTIONS value (final mktcap $M)**: 77269.7
  - **Return over last ~60 cal days of trading**: 1.5%

  Last 10 trading days:

| date                | closeadj   | closeunadj   | volume     |
|:--------------------|:-----------|:-------------|:-----------|
| 2018-06-04 00:00:00 | $93.46     | $93.46       | 3,460,972  |
| 2018-06-05 00:00:00 | $93.46     | $93.46       | 3,847,752  |
| 2018-06-06 00:00:00 | $94.92     | $94.92       | 7,783,309  |
| 2018-06-07 00:00:00 | $95.37     | $95.37       | 5,593,905  |
| 2018-06-08 00:00:00 | $95.34     | $95.34       | 8,902,660  |
| 2018-06-11 00:00:00 | $96.17     | $96.17       | 10,895,082 |
| 2018-06-12 00:00:00 | $96.22     | $96.22       | 22,939,516 |
| 2018-06-13 00:00:00 | $97.95     | $97.95       | 61,986,356 |
| 2018-06-14 00:00:00 | $98.77     | $98.77       | 29,391,076 |
| 2018-06-15 00:00:00 | $98.77     | $98.77       | 8,475      |

## 4. Systematic: gap between last SEP price and terminal event date

| action                |     n |    avg_gap_days |   median_gap_days |   min_gap_days |   max_gap_days |   gap_gt_5d |   event_before_last_price |
|:----------------------|------:|----------------:|------------------:|---------------:|---------------:|------------:|--------------------------:|
| delisted              | 15626 |    -0.00243184  |                 0 |            -18 |              0 |           0 |                         6 |
| acquisitionby         |  8137 |    -0.000122895 |                 0 |             -1 |              0 |           0 |                         1 |
| bankruptcyliquidation |  3326 |    -0.00571257  |                 0 |            -12 |              0 |           0 |                         4 |
| regulatorydelisting   |   833 |     0           |                 0 |              0 |              0 |           0 |                         0 |
| voluntarydelisting    |   367 |     0           |                 0 |              0 |              0 |           0 |                         0 |
| mergerfrom            |   129 | -3042.7         |             -2170 |         -10105 |              0 |           0 |                       127 |

## 5. Terminal returns: last 30/60/90 trading day returns before delist

| action                |     n |   avg_ret_21d |   med_ret_21d |   avg_ret_63d |   med_ret_63d |   avg_ret_252d |   med_ret_252d |
|:----------------------|------:|--------------:|--------------:|--------------:|--------------:|---------------:|---------------:|
| delisted              | 15626 |    0.00667809 |    0.00200803 |    0.0214235  |     0.0122462 |       0.138116 |     0.0441478  |
| acquisitionby         |  7408 |    0.0230386  |    0.00781053 |    0.214254   |     0.0687327 |       0.48602  |     0.333195   |
| bankruptcyliquidation |  3326 |    0.0959881  |   -0.189189   |   -0.340413   |    -0.489796  |      -0.560263 |    -0.858757   |
| regulatorydelisting   |   833 |   -0.119094   |   -0.140845   |    0.0437061  |    -0.3       |      -0.364471 |    -0.657534   |
| voluntarydelisting    |   367 |   -0.0215371  |   -0.0173603  |    0.0137768  |    -0.0109056 |       0.104507 |     0.00903614 |
| mergerfrom            |   110 |   -0.0455379  |    0.00242609 |   -0.00966087 |     0.0297858 |       0.237016 |     0.124734   |

Interpretation: bankruptcies should show large negative returns; acquisitions near 0 or positive (premium).

## 6. ACTIONS value vs last SEP price (acquisitions/mergers)

ACTIONS.value = final market cap in $M. Compare to last closeadj * sharesbas if available.

| ticker   | action        | event_date          |   actions_mktcap_m | last_date           |   last_closeadj |
|:---------|:--------------|:--------------------|-------------------:|:--------------------|----------------:|
| SNCR     | acquisitionby | 2026-02-13 00:00:00 |              103.6 | 2026-02-13 00:00:00 |           9     |
| MOFG     | acquisitionby | 2026-02-13 00:00:00 |             1017.4 | 2026-02-13 00:00:00 |          49.31  |
| SOHO     | acquisitionby | 2026-02-12 00:00:00 |               46.1 | 2026-02-12 00:00:00 |           2.25  |
| ZEUS     | acquisitionby | 2026-02-12 00:00:00 |              535.9 | 2026-02-12 00:00:00 |          47.86  |
| SOHO     | acquisitionby | 2026-02-12 00:00:00 |               46.1 | 2026-02-12 00:00:00 |           2.25  |
| AVDL     | acquisitionby | 2026-02-11 00:00:00 |             2113.3 | 2026-02-11 00:00:00 |          21.64  |
| THS      | acquisitionby | 2026-02-10 00:00:00 |             1221.5 | 2026-02-10 00:00:00 |          24.43  |
| CYBR     | acquisitionby | 2026-02-10 00:00:00 |            20686.2 | 2026-02-10 00:00:00 |         408.85  |
| HI       | acquisitionby | 2026-02-09 00:00:00 |             2259.6 | 2026-02-09 00:00:00 |          31.98  |
| GBIO     | acquisitionby | 2026-02-09 00:00:00 |               36   | 2026-02-09 00:00:00 |           5.34  |
| DVAX     | acquisitionby | 2026-02-09 00:00:00 |             1820.1 | 2026-02-09 00:00:00 |          15.5   |
| ISPO     | acquisitionby | 2026-02-03 00:00:00 |               53.8 | 2026-02-03 00:00:00 |           4.26  |
| DAY      | acquisitionby | 2026-02-03 00:00:00 |            11180   | 2026-02-03 00:00:00 |          69.86  |
| APLT     | acquisitionby | 2026-02-03 00:00:00 |               14.9 | 2026-02-03 00:00:00 |           0.103 |
| PCH      | acquisitionby | 2026-01-30 00:00:00 |             3225.4 | 2026-01-30 00:00:00 |          41.73  |
| FSFG     | acquisitionby | 2026-01-30 00:00:00 |              238.4 | 2026-01-30 00:00:00 |          33.98  |
| CADE     | acquisitionby | 2026-01-30 00:00:00 |             3803.8 | 2026-01-30 00:00:00 |          42.11  |
| REVG     | acquisitionby | 2026-01-30 00:00:00 |             3118.7 | 2026-01-30 00:00:00 |          63.9   |
| CMA      | acquisitionby | 2026-01-30 00:00:00 |            11326.9 | 2026-01-30 00:00:00 |          88.67  |
| JAMF     | acquisitionby | 2026-01-29 00:00:00 |             1738.6 | 2026-01-29 00:00:00 |          13.05  |

## 7. Universe delist flags vs ACTIONS dates

Check: does fwd_delisted_30d flip to 1 at the right time?

## 8. Recommendations for price adjustment

Based on the data above, consider:

1. **Add `delist_type` to universe** from ACTIONS (bankruptcyliquidation, acquisitionby, etc.)
2. **Bankruptcy terminal price**: If last SEP closeadj > $1 but action = bankruptcyliquidation,
   append a synthetic row with closeadj = 0 on the event date (or a recovery fraction).
3. **Acquisition terminal price**: Last SEP price usually reflects the deal price.
   Verify the gap is small; if > 5 days, interpolate or use ACTIONS value / sharesbas.
4. **Forward return labels**: Split fwd_delisted into fwd_bankruptcy and fwd_acquired.
   Models should treat these very differently.
5. **Gap handling**: For tickers where last SEP date is > 5 days before event date,
   decide whether to forward-fill the last price or mark those days as missing.

