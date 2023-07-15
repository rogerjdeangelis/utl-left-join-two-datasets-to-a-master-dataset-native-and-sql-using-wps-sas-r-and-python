# utl-left-join-two-datasets-to-a-master-dataset-native-and-sql-using-wps-sas-r-and-python
Left join two datasets to a master dataset native and sql using wps sas r and python
    %let pgm=utl-left-join-two-datasets-to-a-master-dataset-native-and-sql-using-wps-sas-r-and-python;

    Left join two datasets to a master dataset native and sql using wps sas r and python

    Stackoverflow
    https://tinyurl.com/3p6hamek
    https://stackoverflow.com/questions/76661847/i-would-like-to-merge-three-separate-datasets-to-form-one-large-dataset

        SOLUTIONS

           1. wps/sas transpose the sql
           2. wps/sas geneate sql code then pure sql
           3. r native no sql
           4. r just sql
              https://stackoverflow.com/users/12109788/jpsmith
           5. python just sql

    For solutions 3-5, I paste the code cenerated by array and do over macro

    /*----  SAME CODE WILL RUN IN SAS. NOTE THREE LEVEL QUOTES               ----*/
    %utl_submit_wps64x('
    options sasautos=(sasautos "c:/otowps");
    libname sd1 "d:/sd1";
    %array(_yr,values=%utl_varlist(sd1.insurance,keep=_2:));
    data _null_;
      put  %do_over(_yr,phrase=%str("select names, input(substr(`?`,2) ,best.) as year, ? as insurance from sd1.insurance union all" /));
    run;quit;
    ');

    Copied from log. This is a sql pivot. Samt compiler would do the four in parallel?

        select names, input(substr('_2014',2) ,best.) as year, _2014 as insurance from sd1.insurance union all
        select names, input(substr('_2015',2) ,best.) as year, _2015 as insurance from sd1.insurance union all
        select names, input(substr('_2016',2) ,best.) as year, _2016 as insurance from sd1.insurance union all
        select names, input(substr('_2017',2) ,best.) as year, _2017 as insurance from sd1.insurance union all
        select names, input(substr('_2018',2) ,best.) as year, _2018 as insurance from sd1.insurance union all

    /*               _     _
     _ __  _ __ ___ | |__ | | ___ _ __ ___
    | `_ \| `__/ _ \| `_ \| |/ _ \ `_ ` _ \
    | |_) | | | (_) | |_) | |  __/ | | | | |
    | .__/|_|  \___/|_.__/|_|\___|_| |_| |_|
    |_|
    */

        Two datasets are left joined to a master dataset, dataset_1.
        Howver the second dataset has to be pivoted to generate the name and year key before joining,


          1 pivot longer insurance datasets

            INPUT

            SD1.INSURANCE total obs=3 15MAY2023:07:51:46

            Obs    NAMES     _2014    _2015    _2016    _2017    _2018

             1     Guyle      158      169      221      NA       200
             2     Porter     154      185      NA       450      101
             3     Frank      160      NA       NA       691      321

             PIVOTED

             WORK.INSXPO total obs=15 15MAY2023:14:05:13

             Obs    NAMES     _NAME_    COL1

               1    Frank     _2014      160
               2    Frank     _2015        .
               3    Frank     _2016        .
               4    Frank     _2017      691
               5    Frank     _2018      321
               6    Guyle     _2014      158
               7    Guyle     _2015      169
               8    Guyle     _2016      221
               9    Guyle     _2017        .
              10    Guyle     _2018      200
              11    Porter    _2014      154
              12    Porter    _2015      185
              13    Porter    _2016        .
              14    Porter    _2017      450
              15    Porter    _2018      101

          2. Two left joins starting with the largest master dataset_1 leftmost, inxpo, then dataset3 on names and year

                            SD1.DATASET_1                      WORK.INSXPO                      SD1.DATASET3
                                                         |                   |
                                       NET_              |                   |                    RISK_      ACREAGE_
             Obs     NAME     YEAR    PROFIT      NAMES  |   _NAME_    COL1  |  NAME     YEAR    ATTITUDE       HA       ADOPTION
                                                         |                   |
               1    Frank     2014     2536       Frank  |   _2014      160  | Frank     2016    seeking       12.00      early
               2    Frank     2015     2604       Frank  |   _2015        .  | Guyle     2016    averse        17.63      late
               3    Frank     2016     2984       Frank  |   _2016        .  | Porter    2016    seeking       26.00      middle
               4    Frank     2017     2881       Frank  |   _2017      691  |
               5    Frank     2018     3100       Frank  |   _2018      321  |
               6    Guyle     2014     2970       Guyle  |   _2014      158  |
               7    Guyle     2015     2798       Guyle  |   _2015      169  |
               8    Guyle     2016     2769       Guyle  |   _2016      221  |
               9    Guyle     2017     2686       Guyle  |   _2017        .  |
              10    Guyle     2018     2806       Guyle  |   _2018      200  |
              11    Kojo      2016     2825       Porter |   _2014      154  |
              12    Porter    2014     3096       Porter |   _2015      185  |
              13    Porter    2015     2776       Porter |   _2016        .  |
              14    Porter    2016     2993       Porter |   _2017      450  |
              15    Porter    2017     2829       Porter |   _2018      101  |

          3 OUTPUT

             SD1.WANT total obs=16 15MAY2023:14:15:19

                                       NET_                   RISK_      ACREAGE_
             Obs     NAME     YEAR    PROFIT    INSURANCE    ATTITUDE       HA       ADOPTION

               1    Frank     2014     2536        160                       .
               2    Frank     2015     2604          .                       .
               3    Frank     2016     2984          .       seeking       12.00      early
               4    Frank     2017     2881        691                       .
               5    Frank     2018     3100        321                       .
               6    Guyle     2014     2970        158                       .
               7    Guyle     2015     2798        169                       .
               8    Guyle     2016     2769        221       averse        17.63      late
               9    Guyle     2017     2686          .                       .
              10    Guyle     2018     2806        200                       .
              11    Kojo      2016     2825          .                       .
              12    Porter    2014     3096        154                       .
              13    Porter    2015     2776        185                       .
              14    Porter    2016     2993          .       seeking       26.00      middle
              15    Porter    2017     2829        450                       .
              16    Porter    2018     3090        101                       .

    /*                   _
    (_)_ __  _ __  _   _| |_
    | | `_ \| `_ \| | | | __|
    | | | | | |_) | |_| | |_
    |_|_| |_| .__/ \__,_|\__|
            |_|
    */

    options valivvarname=upcase;
    data sd1.dataset_1;informat
    NAME $6.
    YEAR 8.
    NET_PROFIT 8.
    ;input
    NAME YEAR NET_PROFIT;
    cards4;
    Frank 2014 2536
    Frank 2015 2604
    Frank 2016 2984
    Frank 2017 2881
    Frank 2018 3100
    Guyle 2014 2970
    Guyle 2015 2798
    Guyle 2016 2769
    Guyle 2017 2686
    Guyle 2018 2806
    Kojo 2016 2825
    Porter 2014 3096
    Porter 2015 2776
    Porter 2016 2993
    Porter 2017 2829
    Porter 2018 3090
    ;;;;
    run;quit;

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /*  Up to 40 obs from SD1.DATASET_1 total obs=16 15MAY2023:16:28:11                                                       */
    /*                            NET_                                                                                        */
    /*  Obs     NAME     YEAR    PROFIT                                                                                       */
    /*                                                                                                                        */
    /*    1    Frank     2014     2536                                                                                        */
    /*    2    Frank     2015     2604                                                                                        */
    /*    3    Frank     2016     2984                                                                                        */
    /*    4    Frank     2017     2881                                                                                        */
    /*    5    Frank     2018     3100                                                                                        */
    /*    6    Guyle     2014     2970                                                                                        */
    /*    7    Guyle     2015     2798                                                                                        */
    /*    8    Guyle     2016     2769                                                                                        */
    /*    9    Guyle     2017     2686                                                                                        */
    /*   10    Guyle     2018     2806                                                                                        */
    /*   11    Kojo      2016     2825                                                                                        */
    /*   12    Porter    2014     3096                                                                                        */
    /*   13    Porter    2015     2776                                                                                        */
    /*   14    Porter    2016     2993                                                                                        */
    /*   15    Porter    2017     2829                                                                                        */
    /*   16    Porter    2018     3090                                                                                        */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    data sd1.dataset3;informat
    NAME $6.
    YEAR 8.
    RISK_ATTITUDE $7.
    ACREAGE_HA 8.
    ADOPTION $6.
    ;input
    NAME YEAR RISK_ATTITUDE ACREAGE_HA ADOPTION;
    cards4;
    Frank 2016 seeking 12 early
    Guyle 2016 averse 17.63 late
    Porter 2016 seeking 26 middle
    ;;;;
    run;quit;

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /* Up to 40 obs from SD1.DATASET3 total obs=3 15MAY2023:16:29:12                                                          */
    /*                                                                                                                        */
    /*                           RISK_      ACREAGE_                                                                          */
    /* Obs     NAME     YEAR    ATTITUDE       HA       ADOPTION                                                              */
    /*                                                                                                                        */
    /*  1     Frank     2016    seeking       12.00      early                                                                */
    /*  2     Guyle     2016    averse        17.63      late                                                                 */
    /*  3     Porter    2016    seeking       26.00      middle                                                               */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    data sd1.insurance;informat
    NAMES $6.
    _2014
    _2015
    _2016
    _2017
    _2018 8.
    ;input
    NAMES _2014 _2015 _2016 _2017 _2018;
    cards4;
    Frank 160 .  .  691 321
    Guyle 158 169 221 .  200
    Porter 154 185 .  450 101
    ;;;;
    run;quit;

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /* Up to 40 obs from SD1.INSURANCE total obs=3 15MAY2023:16:30:04                                                         */
    /*                                                                                                                        */
    /* Obs    NAMES     _2014    _2015    _2016    _2017    _2018                                                             */
    /*                                                                                                                        */
    /*  1     Frank      160        .        .      691      321                                                              */
    /*  2     Guyle      158      169      221        .      200                                                              */
    /*  3     Porter     154      185        .      450      101                                                              */
    /*                                                                                                                        */
    /**************************************************************************************************************************/


    /*                         __              _                                                        _
    / | __      ___ __  ___   / /__  __ _ ___ | |_ _ __ __ _ _ __  ___ _ __   ___  ___  ___   ___  __ _| |
    | | \ \ /\ / / `_ \/ __| / / __|/ _` / __|| __| `__/ _` | `_ \/ __| `_ \ / _ \/ __|/ _ \ / __|/ _` | |
    | |  \ V  V /| |_) \__ \/ /\__ \ (_| \__ \| |_| | | (_| | | | \__ \ |_) | (_) \__ \  __/ \__ \ (_| | |
    |_|   \_/\_/ | .__/|___/_/ |___/\__,_|___/ \__|_|  \__,_|_| |_|___/ .__/ \___/|___/\___| |___/\__, |_|
                 |_|                                                  |_|                            |_|
    */



    proc transpose data=SD1.INSURANCE out=insXpo;
    by names;
    var _201:;
    run;quit;

    proc sql;
      create
         table sd1.want as
      select
         l.*
        ,c.col1 as insurance
        ,r.RISK_ATTITUDE
        ,r.ACREAGE_HA
        ,r.ADOPTION
      from
         SD1.DATASET_1 as l
         left join work.insXpo  as c on l.name = c.names and l.year = input(substr(c._name_,2),best.)
         left join sd1.dataset3 as r on l.name = r.name  and l.year = r.year
    ;quit;

    /*
    __      ___ __  ___
    \ \ /\ / / `_ \/ __|
     \ V  V /| |_) \__ \
      \_/\_/ | .__/|___/
             |_|
    */

    proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

    %utl_submit_wps64x('

    options validvarname=any;

    libname sd1 "d:/sd1";
    proc transpose data=SD1.INSURANCE out=insXpo;
    by names;
    var _201:;
    run;quit;

    proc sql;
      create
         table sd1.want as
      select
         l.*
        ,c.col1 as insurance
        ,r.risk_attitude
        ,r.acreage_ha
        ,r.adoption
      from
         sd1.dataset_1 as l
         left join work.insxpo  as c on l.name = c.names and l.year = input(substr(c._name_,2),best.)
         left join sd1.dataset3 as r on l.name = r.name  and l.year = r.year
    ;quit;

    proc print data=sd1.want;
    run;quit;

    ');

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /* The WPS System                                                                                                         */
    /*                                                                                                                        */
    /* Obs     NAME     YEAR    NET_PROFIT    insurance    RISK_ATTITUDE    ACREAGE_HA    ADOPTION                            */
    /*                                                                                                                        */
    /*   1    Frank     2014       2536          160                             .                                            */
    /*   2    Frank     2015       2604            .                             .                                            */
    /*   3    Frank     2016       2984            .          seeking          12.00       early                              */
    /*   4    Frank     2017       2881          691                             .                                            */
    /*   5    Frank     2018       3100          321                             .                                            */
    /*   6    Guyle     2014       2970          158                             .                                            */
    /*   7    Guyle     2015       2798          169                             .                                            */
    /*   8    Guyle     2016       2769          221          averse           17.63       late                               */
    /*   9    Guyle     2017       2686            .                             .                                            */
    /*  10    Guyle     2018       2806          200                             .                                            */
    /*  11    Kojo      2016       2825            .                             .                                            */
    /*  12    Porter    2014       3096          154                             .                                            */
    /*  13    Porter    2015       2776          185                             .                                            */
    /*  14    Porter    2016       2993            .          seeking          26.00       middle                             */
    /*  15    Porter    2017       2829          450                             .                                            */
    /*  16    Porter    2018       3090          101                             .                                            */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*___                          __                                                _
    |___ \  __      ___ __  ___   / /__  __ _ ___   _ __  _   _ _ __ ___   ___  __ _| |
      __) | \ \ /\ / / `_ \/ __| / / __|/ _` / __| | `_ \| | | | `__/ _ \ / __|/ _` | |
     / __/   \ V  V /| |_) \__ \/ /\__ \ (_| \__ \ | |_) | |_| | | |  __/ \__ \ (_| | |
    |_____|   \_/\_/ | .__/|___/_/ |___/\__,_|___/ | .__/ \__,_|_|  \___| |___/\__, |_|
                     |_|                           |_|                            |_|
    */

    proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

    proc sql;
      create
         table sd1.want as
      select
         l.*
        ,c.insurance
        ,r.RISK_ATTITUDE
        ,r.ACREAGE_HA
        ,r.ADOPTION
      from
         SD1.DATASET_1 as l
         left join (
            select names, input(substr('_2014',2) ,best.) as year, _2014 as insurance from sd1.insurance union all
            select names, input(substr('_2015',2) ,best.) as year, _2015 as insurance from sd1.insurance union all
            select names, input(substr('_2016',2) ,best.) as year, _2016 as insurance from sd1.insurance union all
            select names, input(substr('_2017',2) ,best.) as year, _2017 as insurance from sd1.insurance union all
            select names, input(substr('_2018',2) ,best.) as year, _2018 as insurance from sd1.insurance
            ) as c                   on l.name = c.names and l.year = c.year
         left join sd1.dataset3 as r on l.name = r.name  and l.year = r.year
    ;quit;

    /*
    __      ___ __  ___
    \ \ /\ / / `_ \/ __|
     \ V  V /| |_) \__ \
      \_/\_/ | .__/|___/
             |_|
    */

    proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

    %utl_submit_wps64x('

    options validvarname=any;

    libname sd1 "d:/sd1";

    proc sql;
      create
         table sd1.want as
      select
         l.*
        ,c.insurance
        ,r.RISK_ATTITUDE
        ,r.ACREAGE_HA
        ,r.ADOPTION
      from
         SD1.DATASET_1 as l
         left join (
            select names, input(substr("_2014",2) ,best.) as year, _2014 as insurance from sd1.insurance union all
            select names, input(substr("_2015",2) ,best.) as year, _2015 as insurance from sd1.insurance union all
            select names, input(substr("_2016",2) ,best.) as year, _2016 as insurance from sd1.insurance union all
            select names, input(substr("_2017",2) ,best.) as year, _2017 as insurance from sd1.insurance union all
            select names, input(substr("_2018",2) ,best.) as year, _2018 as insurance from sd1.insurance
            ) as c                   on l.name = c.names and l.year = c.year
         left join sd1.dataset3 as r on l.name = r.name  and l.year = r.year
    ;quit;

    proc print data=sd1.want;
    run;quit;

    ');

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /* The WPS System                                                                                                         */
    /*                                                                                                                        */
    /* Obs     NAME     YEAR    NET_PROFIT    insurance    RISK_ATTITUDE    ACREAGE_HA    ADOPTION                            */
    /*                                                                                                                        */
    /*   1    Frank     2014       2536          160                             .                                            */
    /*   2    Frank     2015       2604            .                             .                                            */
    /*   3    Frank     2016       2984            .          seeking          12.00       early                              */
    /*   4    Frank     2017       2881          691                             .                                            */
    /*   5    Frank     2018       3100          321                             .                                            */
    /*   6    Guyle     2014       2970          158                             .                                            */
    /*   7    Guyle     2015       2798          169                             .                                            */
    /*   8    Guyle     2016       2769          221          averse           17.63       late                               */
    /*   9    Guyle     2017       2686            .                             .                                            */
    /*  10    Guyle     2018       2806          200                             .                                            */
    /*  11    Kojo      2016       2825            .                             .                                            */
    /*  12    Porter    2014       3096          154                             .                                            */
    /*  13    Porter    2015       2776          185                             .                                            */
    /*  14    Porter    2016       2993            .          seeking          26.00       middle                             */
    /*  15    Porter    2017       2829          450                             .                                            */
    /*  16    Porter    2018       3090          101                             .                                            */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*___                _   _                                            _
    |___ \   _ __   __ _| |_(_)_   _____   _ __   _ __   ___    ___  __ _| |
      __) | | `_ \ / _` | __| \ \ / / _ \ | `__| | `_ \ / _ \  / __|/ _` | |
     / __/  | | | | (_| | |_| |\ V /  __/ | |    | | | | (_) | \__ \ (_| | |
    |_____| |_| |_|\__,_|\__|_| \_/ \___| |_|    |_| |_|\___/  |___/\__, |_|
                                                                       |_|
    */

    proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

    %utl_submit_wps64x('
    libname sd1 "d:/sd1";
    proc r;
    export data=sd1.dataset_1 r=dataset_1;
    export data=sd1.dataset3  r=dataset3 ;
    export data=sd1.insurance r=insurance;
    submit;
    library(tidyverse);
    df1<-dataset_1 |>
      left_join(
        insurance |>
          pivot_longer(
            -NAMES,
            names_to = "YEAR",
            names_transform = as.numeric,
            values_to = "INSURANCE",
            values_transform = as.numeric
          ),
        by = join_by(NAME == NAMES, YEAR)
      );
    want <-  df1 |> left_join(dataset3, by = join_by(NAME, YEAR));
    want;
    endsubmit;
    import data=sd1.want r=want;
    run;quit;
    ');

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /*  The WPS System                                                                                                        */
    /*                                                                                                                        */
    /*       NAME YEAR NET_PROFIT INSURANCE RISK_ATTITUDE ACREAGE_HA ADOPTION                                                 */
    /*                                                                                                                        */
    /*  1   Frank 2014       2536        NA          <NA>         NA     <NA>                                                 */
    /*  2   Frank 2015       2604        NA          <NA>         NA     <NA>                                                 */
    /*  3   Frank 2016       2984        NA       seeking      12.00    early                                                 */
    /*  4   Frank 2017       2881        NA          <NA>         NA     <NA>                                                 */
    /*  5   Frank 2018       3100        NA          <NA>         NA     <NA>                                                 */
    /*  6   Guyle 2014       2970        NA          <NA>         NA     <NA>                                                 */
    /*  7   Guyle 2015       2798        NA          <NA>         NA     <NA>                                                 */
    /*  8   Guyle 2016       2769        NA        averse      17.63     late                                                 */
    /*  9   Guyle 2017       2686        NA          <NA>         NA     <NA>                                                 */
    /*  10  Guyle 2018       2806        NA          <NA>         NA     <NA>                                                 */
    /*  11   Kojo 2016       2825        NA          <NA>         NA     <NA>                                                 */
    /*  12 Porter 2014       3096        NA          <NA>         NA     <NA>                                                 */
    /*  13 Porter 2015       2776        NA          <NA>         NA     <NA>                                                 */
    /*  14 Porter 2016       2993        NA       seeking      26.00   middle                                                 */
    /*  15 Porter 2017       2829        NA          <NA>         NA     <NA>                                                 */
    /*  16 Porter 2018       3090        NA          <NA>         NA     <NA>                                                 */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*  _              _           _               _
    | || |    _ __    (_)_   _ ___| |_   ___  __ _| |
    | || |_  | `__|   | | | | / __| __| / __|/ _` | |
    |__   _| | |      | | |_| \__ \ |_  \__ \ (_| | |
       |_|   |_|     _/ |\__,_|___/\__| |___/\__, |_|
                    |__/                        |_|
    */

    proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

    %utl_submit_wps64x('

    options validvarname=any;

    libname sd1 "d:/sd1";

    proc r;

    export data=sd1.dataset_1 r=dataset_1;
    export data=sd1.dataset3  r=dataset3 ;
    export data=sd1.insurance r=insurance;

    submit;
    library(sqldf);
    want <- sqldf("
      select
         l.*
        ,c.insurance
        ,r.risk_attitude
        ,r.acreage_ha
        ,r.adoption
      from
         dataset_1 as l
         left join (
            select names, cast(`2014` as nmeric) as year, _2014 as insurance from insurance union all
            select names, cast(`2015` as nmeric) as year, _2015 as insurance from insurance union all
            select names, cast(`2016` as nmeric) as year, _2016 as insurance from insurance union all
            select names, cast(`2017` as nmeric) as year, _2017 as insurance from insurance union all
            select names, cast(`2018` as nmeric) as year, _2018 as insurance from insurance
            ) as c                   on l.name = c.names and l.year = c.year
         left join dataset3 as r on l.name = r.name  and l.year = r.year
       ");
    want;
    endsubmit;
    import data=sd1.want r=want;
    run;quit;
    ');

    proc print data=sd1.want;
    run;quit;

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /*  The WPS System                                                                                                        */
    /*                                                                                                                        */
    /*       NAME YEAR NET_PROFIT insurance RISK_ATTITUDE ACREAGE_HA ADOPTION                                                 */
    /*                                                                                                                        */
    /*  1   Frank 2014       2536       160          <NA>         NA     <NA>                                                 */
    /*  2   Frank 2015       2604        NA          <NA>         NA     <NA>                                                 */
    /*  3   Frank 2016       2984        NA       seeking      12.00    early                                                 */
    /*  4   Frank 2017       2881       691          <NA>         NA     <NA>                                                 */
    /*  5   Frank 2018       3100       321          <NA>         NA     <NA>                                                 */
    /*  6   Guyle 2014       2970       158          <NA>         NA     <NA>                                                 */
    /*  7   Guyle 2015       2798       169          <NA>         NA     <NA>                                                 */
    /*  8   Guyle 2016       2769       221        averse      17.63     late                                                 */
    /*  9   Guyle 2017       2686        NA          <NA>         NA     <NA>                                                 */
    /*  10  Guyle 2018       2806       200          <NA>         NA     <NA>                                                 */
    /*  11   Kojo 2016       2825        NA          <NA>         NA     <NA>                                                 */
    /*  12 Porter 2014       3096       154          <NA>         NA     <NA>                                                 */
    /*  13 Porter 2015       2776       185          <NA>         NA     <NA>                                                 */
    /*  14 Porter 2016       2993        NA       seeking      26.00   middle                                                 */
    /*  15 Porter 2017       2829       450          <NA>         NA     <NA>                                                 */
    /*  16 Porter 2018       3090       101          <NA>         NA     <NA>                                                 */
    /*                                                                                                                        */
    /**************************************************************************************************************************/


    /*___                _   _                     _           _               _
    | ___|   _ __  _   _| |_| |__   ___  _ __     (_)_   _ ___| |_   ___  __ _| |
    |___ \  | `_ \| | | | __| `_ \ / _ \| `_ \    | | | | / __| __| / __|/ _` | |
     ___) | | |_) | |_| | |_| | | | (_) | | | |   | | |_| \__ \ |_  \__ \ (_| | |
    |____/  | .__/ \__, |\__|_| |_|\___/|_| |_|  _/ |\__,_|___/\__| |___/\__, |_|
            |_|    |___/                        |__/                        |_|
    */             `

    %utl_submit_wps64x('
       libname sd1 `d:\sd1`;
       proc python;

         export data=sd1.dataset_1 python=dataset_1;
         export data=sd1.dataset3  python=dataset3 ;
         export data=sd1.insurance python=insurance;

         submit;
         from os import path;
         import pandas as pd;
         import numpy as np;
         import pandas as pd;
         from pandasql import sqldf;
         mysql = lambda q: sqldf(q, globals());
         from pandasql import PandaSQL;
         pdsql = PandaSQL(persist=True);
         sqlite3conn = next(pdsql.conn.gen).connection.connection;
         sqlite3conn.enable_load_extension(True);
         sqlite3conn.load_extension(`c:/temp/libsqlitefunctions.dll`);
         mysql = lambda q: sqldf(q, globals());
         want = pdsql("""
           select
              l.*
             ,c.insurance
             ,r.risk_attitude
             ,r.acreage_ha
             ,r.adoption
           from
              dataset_1 as l
              left join (
                 select names, cast(`2014` as nmeric) as year, _2014 as insurance from insurance union all
                 select names, cast(`2015` as nmeric) as year, _2015 as insurance from insurance union all
                 select names, cast(`2016` as nmeric) as year, _2016 as insurance from insurance union all
                 select names, cast(`2017` as nmeric) as year, _2017 as insurance from insurance union all
                 select names, cast(`2018` as nmeric) as year, _2018 as insurance from insurance
                 ) as c                   on l.name = c.names and l.year = c.year
              left join dataset3 as r on l.name = r.name  and l.year = r.year
            """);
         print(want);
    endsubmit;
    import data=sd1.want python=want;
    run;quit;
    ');

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /* The WPS System                                                                                                         */
    /*                                                                                                                        */
    /* The PYTHON Procedure                                                                                                   */
    /*                                                                                                                        */
    /*       NAME    YEAR  NET_PROFIT  insurance RISK_ATTITUDE  ACREAGE_HA ADOPTION                                           */
    /* 0   Frank   2014.0      2536.0      160.0          None         NaN     None                                           */
    /* 1   Frank   2015.0      2604.0        NaN          None         NaN     None                                           */
    /* 2   Frank   2016.0      2984.0        NaN       seeking       12.00   early                                            */
    /* 3   Frank   2017.0      2881.0      691.0          None         NaN     None                                           */
    /* 4   Frank   2018.0      3100.0      321.0          None         NaN     None                                           */
    /* 5   Guyle   2014.0      2970.0      158.0          None         NaN     None                                           */
    /* 6   Guyle   2015.0      2798.0      169.0          None         NaN     None                                           */
    /* 7   Guyle   2016.0      2769.0      221.0       averse        17.63   late                                             */
    /* 8   Guyle   2017.0      2686.0        NaN          None         NaN     None                                           */
    /* 9   Guyle   2018.0      2806.0      200.0          None         NaN     None                                           */
    /* 10  Kojo    2016.0      2825.0        NaN          None         NaN     None                                           */
    /* 11  Porter  2014.0      3096.0      154.0          None         NaN     None                                           */
    /* 12  Porter  2015.0      2776.0      185.0          None         NaN     None                                           */
    /* 13  Porter  2016.0      2993.0        NaN       seeking       26.00   middle                                           */
    /* 14  Porter  2017.0      2829.0      450.0          None         NaN     None                                           */
    /* 15  Porter  2018.0      3090.0      101.0          None         NaN     None                                           */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*              _
      ___ _ __   __| |
     / _ \ `_ \ / _` |
    |  __/ | | | (_| |
     \___|_| |_|\__,_|

    */
