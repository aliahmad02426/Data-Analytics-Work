DBI::dbDisconnect(con)
rm(list = ls())

# ======================================================================
# libraries

library(tidyverse)
library(lubridate)
library(here)

# ======================================================================
# database connection

schema_name <- "amovilsatmap"

# this file has the username and password for the connection
# it is for security purposes; not revealing the credentials
source("login_credentials.R")

con <- DBI::dbConnect(
  #RMariaDB::MariaDB(),
  RMySQL::MySQL(),
  db = schema_name,
  #host = "10.31.128.204",
  #host = "10.31.128.209",
  host = "10.29.192.195",
  port = 3307,
  #user = db_username,
  #password = db_password
  user = "muhammad.aahmed",
  password = "P@ssword1"
)

# ======================================================================
# MySQL environment setup

DBI::dbSendQuery(con, "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
DBI::dbSendQuery(con, "SET SQL_SAFE_UPDATES = 0")
DBI::dbSendQuery(con, "SET SESSION wait_timeout=99999999999999")
DBI::dbSendQuery(con, "SET SESSION MAX_EXECUTION_TIME=999999999999999")

# ======================================================================
# declare R variables

# number of seconds to sleep before performaing library loading / unloading
sleep_seconds <- 1

# set flags
flag_prod_run        <- FALSE
flag_purge_env_vars  <- FALSE
flag_execute_sensors <- FALSE

# in-scope skills
v_retention_external <- c(695,649,1199,645,698,595)
v_retention_transfer <- c(529)
v_retention_internal <- c(534,537,566,654,657,702,723,724,725,726,739,756,757,
                          766, 669, 606, 771, 699)

v_in_scope_skills <- c(v_retention_external,
                       v_retention_transfer,
                       v_retention_internal)

#v_in_scope_skills <- c(v_retention_transfer)
                       
str_bpo <- "aec"
dt_bgn  <- "2019-09-01"
dt_end  <- "2019-09-02"
# ======================================================================
# relevant tables

tbl_echi_raw <-
  tbl(con, dbplyr::in_schema(schema_name, "`amovil_aec.echi`"))

tbl_outcomes_raw <-
  tbl(con, dbplyr::in_schema(schema_name, "`amovil_aec.outcomes`"))

tbl_agent_mapping_raw <-
  tbl(con, dbplyr::in_schema(schema_name, "`amovil_aec.agentmapping`")) 

tbl_aca <-
  tbl(con, dbplyr::in_schema(schema_name, "`amovil_aec.aca`"))

tbl_crm_raw <-
  tbl(con, dbplyr::in_schema(schema_name, "`amovil_aec.crm`")) %>%
  filter(
    startDate <= dt_bgn,
    endDate >= dt_end
  ) %>%
  select(contract,
         DAT_MOVIMENTO,            # reference month
         DAT_NASCIMENTO,           # date of birth
         NUM_TELEFONE_RESIDENCIAL, # landline / home phone number
         NUM_TELEFONE_FIXO_NET,    # NET landline number
         NUM_CELULAR,              # mobile phone number
         NUM_TELEFONE_COMERCIAL,   # office phone number
         NUM_TELEFONE_OUTROS       # other phone number for contact
  )

# ======================================================================
# setup production environment
  
# auto populate scipt begin and end dates in production setting
if(flag_prod_run) {
  dt_bgn <-
    tbl_aca %>%
    summarize(aca_max_date = as.Date(max(segstart))) %>%
    pull()
  
  dt_end <- today()
}

if( as.integer(difftime(dt_end, dt_bgn, units = "days")) != 1 ) {
  stop("This script will execute on a daily basis only.")
  exit()
}

# ======================================================================
# load and clean agent mapping

df_agent_mapping_raw <-
  tbl_agent_mapping_raw %>%
  collect() %>%
  mutate(
    LOGIN_TELEFONIA = as.character(LOGIN_TELEFONIA),
    LOGIN_NET = as.character(LOGIN_NET %>% str_extract("[:alnum:]+") %>% str_trim()),
    Startdate = ymd(Startdate),
    Enddate = ymd(Enddate) - days(1)
  ) %>%
  filter(
    str_detect(TERCEIRA, pattern = paste0("(?i)", str_bpo)) &
      LOGIN_TELEFONIA != "0" &
      str_length(LOGIN_TELEFONIA) > 1 &
      str_length(LOGIN_NET) > 1
  ) %>%
  select(LOGIN_TELEFONIA, LOGIN_NET, Startdate, Enddate) %>%
  arrange(Startdate, Enddate)

# ======================================================================
# load and clean calls data

df_calls_raw <-
  tbl_echi_raw %>% 
  filter(segstart >= dt_bgn & segstart < dt_end) %>%
  collect()

v_calls_colnames <- setdiff(names(df_calls_raw), c('Filename', 'CreatedDate', 'UpdatedDate'))

# extract valid btn from the first leg of the call with valid phone number
df_calls_btn <-
  df_calls_raw %>%
  filter(str_length(calling_pty) > 9) %>%
  arrange(callid, segment) %>%
  group_by(callid) %>%
  filter(row_number() == 1) %>% # take the value from first leg with a valid phone number
  ungroup() %>%
  select(callid, calling_pty)

df_calls_answered_with_btn_lookup <-
  df_calls_raw %>%
  filter(disposition == 2) %>% # keep valid calls
  left_join( # use valid btn from the first leg if answered leg has invalid btn
    df_calls_btn,
    by     = c("callid" = "callid"),
    suffix = c("", ".y")
  ) %>%
  mutate( # add helper columns
    calling_pty_final = if_else(str_length(calling_pty) == 7 & !(is.na(calling_pty.y)), calling_pty.y, calling_pty),
    calls_rn = row_number(),
    segstart = ymd_hms(segstart),
    segstop = ymd_hms(segstop),
    call_dt  = date(segstart),
    call_mn  = month(segstart),
    talk_start_tm = segstart + seconds(disptime),
    talk_start_tm_with_buffer = segstart + seconds(disptime) + seconds(2*60),
    talk_end_tm = segstart + seconds(disptime + talktime + ansholdtime),
    isInScopeSkill = ifelse(dispsplit %in% v_in_scope_skills, 1, 0),
    contract = str_sub(gsub("^'", "", str_trim(ASAIUUI)), 1, 12),
    contract_string_status = 
      case_when(
        str_detect(contract, "^0+$")        ~ "zero",
        str_length(str_trim(contract)) == 0 ~ "blank",
        TRUE                                ~ "valid"
      ),
    is_contract_originally_populated = if_else(contract_string_status == "valid", 1, 0),
    retention_type = 
      case_when(
        dispsplit %in% v_retention_external    ~ "retention_external",
        dispsplit %in% v_retention_transfer    ~ "retention_transfer",
        dispsplit %in% v_retention_internal    ~ "retention_internal",
        TRUE                                   ~ "other"
      )
  ) %>%
  select( # remove unwanted columns
    -c(calling_pty.y)
  ) %>%
  arrange(anslogin, segstart) %>%
  group_by(anslogin, call_dt) %>%
  mutate(
    # note that this mutate statement does the calcuations by the grouped columns
    # adding next call start time column; in case there is no next call by agent, add 5 mins to call end
    next_talk_start_tm = lead(talk_start_tm, default = max(talk_end_tm) + 5*60)
  ) %>%
  ungroup()

# ======================================================================
# code to get the active agents to filter outcomes

df_agent_per_day <-
  df_calls_answered_with_btn_lookup %>%
  filter(isInScopeSkill == 1) %>%
  select(call_dt, anslogin) %>%
  distinct()

# load data table
Sys.sleep(sleep_seconds)
library(data.table)

# create temp tables
df_agent_mapping_raw_tmp <- df_agent_mapping_raw

# convert data frames to data table
setDT(df_agent_per_day)
setDT(df_agent_mapping_raw_tmp)

df_agent_per_day[, `:=`(call_dt_tempJoin = call_dt,
                        anslogin_tempJoin = anslogin)]

# join agents from calls data to agent mapping on per day basis
df_agent_per_day_with_mapping <-
  df_agent_mapping_raw_tmp[df_agent_per_day,
                           on=.(LOGIN_TELEFONIA=anslogin_tempJoin,
                                Startdate<=call_dt_tempJoin,
                                Enddate>call_dt_tempJoin)] 

setDF(df_agent_per_day_with_mapping)

df_agentPerDay_tmp <-
  df_agent_per_day_with_mapping %>%
  distinct(call_dt, anslogin, LOGIN_NET)

rm(df_agent_per_day)
rm(df_agent_mapping_raw_tmp)
rm(df_agent_per_day_with_mapping)

Sys.sleep(sleep_seconds)
# unloading data.table and re-loading tidyverse and lubridate to take care of object masking by data.table library
#detach("package:data.table", unload=TRUE)
library(tidyverse)
library(lubridate)

# ======================================================================
# load and clean outcomes data

df_outcomes_raw <-
  tbl_outcomes_raw %>% 
  filter(DH_ABT_OCOR >= dt_bgn & DH_ABT_OCOR < dt_end) %>%
  collect()

v_outcomes_colnames <- setdiff(names(df_outcomes_raw), c('Filename', 'CreatedDate', 'UpdatedDate'))

df_outcomes_in_scope_initial <-
  df_outcomes_raw %>%
  mutate(
    threat_type = str_extract(tolower(str_trim(NM_TIPO_OCORRENCIA)), "^co\\d{1,2}"),
    contract_outcome = paste0( str_pad(CD_OPERADORA, 3, "left", pad = "0"), str_pad(NR_CONTRATO, 9, "left", pad = "0") ),
    isSaved = ifelse(NM_SUB_INDICADOR_INTELIGENCIA == "RETIDO", 1, 0),
    is_cancelled = ifelse(NM_SUB_INDICADOR_INTELIGENCIA == "RETIDO", 0, 1),
    is_in_scope_outcome = 
      if_else(
        NM_VISAO_ANALISE == "DOMICÍLIO" &
          NM_INDICADOR_NEGOCIO == "LEADS RETENCAO" &
          NM_TIPO_CANCELAMENTO == "PRODUTOS" &
          NM_SUB_INDICADOR_NEGOCIO %in% c("RETIDO", "NAO RETIDO") &
          threat_type %in% c("co2","co4","co11","co13","co15","co17","co19", "co48", "co49", "co50") &
          NM_EPS %in% c('AEC SINGLE','AEC','AEC URA')
        , 1, 0)
  ) %>%
  filter(
    #NM_VISAO_ANALISE == "DOMICÍLIO" &
    NM_INDICADOR_NEGOCIO == "LEADS RETENCAO" &
      NM_TIPO_CANCELAMENTO == "PRODUTOS" &
      NM_SUB_INDICADOR_NEGOCIO %in% c("RETIDO", "NAO RETIDO") &
      threat_type %in% c("co2","co4","co11","co13","co15","co17","co19", "co48", "co49", "co50") &
      NM_EPS %in% c('AEC SINGLE','AEC','AEC URA')
  ) %>%
  mutate(
    outcomes_rn = row_number(),
    DH_ABT_OCOR = ymd_hms(DH_ABT_OCOR),
    call_dt = date(DH_ABT_OCOR),
    call_mn = month(DH_ABT_OCOR)
  ) %>%
  inner_join( # inner join is filtering the agents on in-scope skills only
    df_agentPerDay_tmp,
    by = c("call_dt" = "call_dt", "CD_LOGIN_USUARIO" = "LOGIN_NET"),
    suffix = c("", ".y")
  )

# ======================================================================
# load and clean crm data

# *****

v_contracts_to_look_for <-
  unique(
    c(
      (df_calls_answered_with_btn_lookup %>% filter(contract_string_status == "valid") %>% select(contract) %>% distinct() %>% pull()), # TODO: 00000 contracts may appear
      (df_outcomes_in_scope_initial %>% filter(length(contract_outcome) > 1) %>% select(contract_outcome) %>%distinct() %>% pull())
    )
  )

v_phone_numbers_to_look_for <-
  df_calls_answered_with_btn_lookup %>%
  select(calling_pty_final) %>%
  filter(length(calling_pty_final) > 9) %>%
  filter(!is.na(calling_pty_final)) %>%
  distinct() %>%
  pull()

#v_contracts_to_look_for <- head(v_contracts_to_look_for, 100)
#v_phone_numbers_to_look_for <- head(v_phone_numbers_to_look_for, 100)

# *****

df_crm_raw <-
  tbl_crm_raw %>%
  collect() %>%
  filter(
      (contract                                                            %in% v_contracts_to_look_for)     |
      (str_length(NUM_TELEFONE_RESIDENCIAL) > 9 & NUM_TELEFONE_RESIDENCIAL %in% v_phone_numbers_to_look_for) |
      (str_length(NUM_TELEFONE_FIXO_NET)    > 9 & NUM_TELEFONE_FIXO_NET    %in% v_phone_numbers_to_look_for) |
      (str_length(NUM_CELULAR)              > 9 & NUM_CELULAR              %in% v_phone_numbers_to_look_for) |
      (str_length(NUM_TELEFONE_COMERCIAL)   > 9 & NUM_TELEFONE_COMERCIAL   %in% v_phone_numbers_to_look_for) |
      (str_length(NUM_TELEFONE_OUTROS)      > 9 & NUM_TELEFONE_OUTROS      %in% v_phone_numbers_to_look_for)
  ) %>%
  mutate(
    contract_lookup = contract,
    crm_key = paste0( NUM_TELEFONE_RESIDENCIAL, "_", DAT_NASCIMENTO )
  ) %>%
  select(-contract) %>%
  arrange(desc(DAT_MOVIMENTO)) %>%
  group_by(contract_lookup) %>%
  filter(row_number() == 1) %>% # pick the contract that was last updated in crm
  ungroup()

# lookup contracts against btn's in the calls
df_contract_against_btn <-
  df_crm_raw %>%
  select(contract_lookup,
         DAT_MOVIMENTO,
         NUM_TELEFONE_RESIDENCIAL,
         NUM_TELEFONE_FIXO_NET,
         NUM_CELULAR,
         NUM_TELEFONE_COMERCIAL,
         NUM_TELEFONE_OUTROS
  ) %>%
  gather(., key = btn_type, value = btn, -c(contract_lookup, DAT_MOVIMENTO), na.rm = TRUE, factor_key = FALSE) %>%
  arrange(
    factor(btn_type, levels = c('NUM_TELEFONE_RESIDENCIAL', 'NUM_TELEFONE_FIXO_NET', 'NUM_CELULAR', 'NUM_TELEFONE_COMERCIAL', 'NUM_TELEFONE_OUTROS')),
    desc(DAT_MOVIMENTO)) %>%
  group_by(btn) %>%
  filter(row_number() == 1) %>%
  ungroup() %>%
  select(btn, contract_lookup) %>%
  inner_join(
    df_calls_answered_with_btn_lookup %>% select(calling_pty_final),
    by = c("btn" = "calling_pty_final"),
    suffix = c("", ".y")
  ) %>%
  distinct()

# compile all contracts together from calls and outcomes
# including those looked up via btn in calls data
df_contracts_calling_retention <-
  dplyr::union((df_outcomes_in_scope_initial %>% mutate(contract = contract_outcome) %>% select(contract)),
               (df_calls_answered_with_btn_lookup %>% filter(contract_string_status == "valid") %>% select(contract)),
               (df_contract_against_btn %>% select(contract))
  ) %>%
  distinct(contract)

# filter CRM keeping only the records needed for the query
df_customers_calling_retention <-
  df_crm_raw %>%
  inner_join(
    df_contracts_calling_retention,
    by = c("contract_lookup" = "contract"),
    suffix = c("", ".y")
  )

# create a map of contract vs. customer's unique ID from CRM
df_contract_customer_map <-
  df_crm_raw %>%
  mutate(
    NUM_TELEFONE_RESIDENCIAL = str_trim(NUM_TELEFONE_RESIDENCIAL),
    DAT_NASCIMENTO = str_trim(DAT_NASCIMENTO)
  ) %>%
  filter(
    !(
      is.na(NUM_TELEFONE_RESIDENCIAL) |
        is.na(DAT_NASCIMENTO) |
        str_length(NUM_TELEFONE_RESIDENCIAL) < 9 |
        str_length(DAT_NASCIMENTO) < 5 |
        
        # get rid of dates that seems default
        DAT_NASCIMENTO == '1950-04-08 00:00:00' |
        DAT_NASCIMENTO == '2001-01-01 00:00:00' |
        str_sub(DAT_NASCIMENTO, 1, 4) > 2019
    )
  ) %>%
  inner_join(
    df_customers_calling_retention,
    by = c("crm_key" = "crm_key"),
    suffix = c("", ".y")
  ) %>%
  distinct(crm_key, contract_lookup)

if(flag_purge_env_vars) {
  rm(df_crm_raw)
  rm(df_contracts_calling_retention)
  rm(df_customers_calling_retention)
}

# ======================================================================
# calls data - join from crm

df_calls_answered <-
  df_calls_answered_with_btn_lookup %>%
  left_join(
    # populate contract from CRM using btn
    df_contract_against_btn,
    by = c("calling_pty_final" = "btn"),
    suffix = c("", ".y")
  ) %>%
  mutate(
    # make a new column, contract_final, with contracts from original calls as well as CRM lookup
    contract = if_else(contract_string_status == "valid", contract, NA_character_),
    contract_final = if_else(is.na(contract) & str_length(contract_lookup) == 12,
                             contract_lookup, contract),
    contract_string_status = if_else(is.na(contract) & str_length(contract_lookup) == 12,
                                     "valid_lookup", contract_string_status)
  ) %>%
  left_join(
    # populate customer's unique ID from CRM using contract
    df_contract_customer_map,
    by = c("contract_final" = "contract_lookup"),
    suffix = c("", ".y")
  ) %>%
  mutate(
    next_talk_start_tm_with_buffer_strong_join =
      talk_end_tm + seconds(60*60),
    next_talk_start_tm_with_buffer_weak_join =
      pmin(
        next_talk_start_tm + seconds(2*60),
        talk_end_tm + seconds(5*60)
      )
  ) %>% # new code to fix low MR ***
  arrange(call_dt, anslogin, contract_final, calling_pty_final, segstart) %>%
  group_by(call_dt, anslogin, contract_final, calling_pty_final) %>%
  mutate(
    # note that this mutate statement does the calcuations by the grouped columns
    # adding next call start time column; for the strongest join consider the rest of the day
         # For the odd cases where same agent / customer talks more than once in a day, consider the next talk start time
    next_talk_start_tm_strong_join = lead(talk_start_tm),
  ) %>%
  ungroup() %>%
  mutate(
    next_talk_start_tm_strong_join = if_else(is.na(next_talk_start_tm_strong_join),
                                             #talk_end_tm + seconds(5*60),
                                             ceiling_date(segstart, "day"),
                                             next_talk_start_tm_strong_join)
  )

if(flag_purge_env_vars) {
  rm(df_calls_btn)
  rm(df_contract_against_btn)
  rm(df_calls_raw)
}

# ======================================================================

# add customer's unique id from CRM based on contract
df_outcomes_in_scope <-
  df_outcomes_in_scope_initial %>%
  left_join(
    df_contract_customer_map,
    by = c("contract_outcome" = "contract_lookup"),
    suffix = c("", ".y")
  )

# ======================================================================
# arrange the data frames w.r.t time to use closest time during joins

df_calls_answered <-
  df_calls_answered %>%
  arrange(anslogin, segstart)

df_outcomes_in_scope <-
  df_outcomes_in_scope %>%
  arrange(DH_ABT_OCOR)

# ======================================================================
# convert to data table to perform inequality join
# then convert back to data frame

# load data table
Sys.sleep(sleep_seconds)
library(data.table)

# convert data frames to data table
setDT(df_calls_answered)
setDT(df_outcomes_in_scope)
setDT(df_agent_mapping_raw)

# ----------------------------------------------------------------------
# outcomes to agent map

# add temporary columns that will be used in join
df_agent_mapping_raw[, `:=`(LOGIN_NET_tempJoin = LOGIN_NET,
                            Startdate_tempJoin = Startdate,
                            Enddate_tempJoin = Enddate
                            
)]

# add temporary columns that will be used in join
df_outcomes_in_scope[, `:=`(call_dt_tempJoin = call_dt,
                            CD_LOGIN_USUARIO_tempJoin = CD_LOGIN_USUARIO)]

# join outcomes to agent mapping
df_outcomes_in_scope_with_agent_map <-
  df_agent_mapping_raw[df_outcomes_in_scope,
                       on=.(LOGIN_NET_tempJoin  = CD_LOGIN_USUARIO_tempJoin,
                            Startdate_tempJoin <= call_dt_tempJoin,
                            Enddate_tempJoin   >= call_dt_tempJoin),
                       mult = 'last']

# remove temporary columns added for data table join
df_agent_mapping_raw[, grep(".*_tempJoin$", colnames(df_agent_mapping_raw)):=NULL]
df_outcomes_in_scope[, grep(".*_tempJoin$", colnames(df_outcomes_in_scope)):=NULL]
df_outcomes_in_scope_with_agent_map[, grep(".*_tempJoin$", colnames(df_outcomes_in_scope_with_agent_map)):=NULL]

# check if there were duplicate agent mappings
if (count(df_outcomes_in_scope) != count(df_outcomes_in_scope_with_agent_map))
{ warning( "Outcomes count after Agent Mapping" ) }

# ----------------------------------------------------------------------
# Step 1 of calls to outcomes join based on agent, contract, and date

# remove calls that have invalid contract
df_calls_answered_step_1 <-
  df_calls_answered[ contract_string_status %in% c("valid", "valid_lookup") ]

df_outcomes_step_1 <-
  df_outcomes_in_scope_with_agent_map

# add temporary columns that will be used in join
df_outcomes_step_1[, `:=`(call_dt_tempJoin = call_dt,
                          LOGIN_TELEFONIA_tempJoin = LOGIN_TELEFONIA,
                          contract_outcome_tempJoin = contract_outcome,
                          DH_ABT_OCOR_tempJoin = DH_ABT_OCOR
)]

# add temporary columns that will be used in join
df_calls_answered_step_1[, `:=`(call_dt_tempJoin = call_dt,
                                anslogin_tempJoin = anslogin,
                                contract_final_tempJoin = contract_final,
                                talk_start_tm_tempJoin = talk_start_tm,
                                next_talk_start_tm_with_buffer_tempJoin = next_talk_start_tm_strong_join # TODO: check without time join
                                #next_talk_start_tm_with_buffer_tempJoin = next_talk_start_tm_with_buffer_strong_join # TODO: check without time join
)]

# join calls to outcomes
df_c_j_o_step_1 <-
  df_outcomes_step_1[df_calls_answered_step_1,
                     on = .(call_dt_tempJoin = call_dt_tempJoin,
                            LOGIN_TELEFONIA_tempJoin = anslogin_tempJoin,
                            contract_outcome_tempJoin = contract_final_tempJoin,
                            DH_ABT_OCOR_tempJoin >= talk_start_tm_tempJoin,
                            DH_ABT_OCOR_tempJoin < next_talk_start_tm_with_buffer_tempJoin
                     ),
                     mult = 'last']

# remove temporary columns added for data table join
df_outcomes_step_1[, grep(".*_tempJoin$", colnames(df_outcomes_step_1)):=NULL]
df_calls_answered_step_1[, grep(".*_tempJoin$", colnames(df_calls_answered_step_1)):=NULL]
df_c_j_o_step_1[, grep(".*_tempJoin$", colnames(df_c_j_o_step_1)):=NULL]

# take care of cases where multiple calls can be linked to one outcome
# give priority to the last call (closest to the outcome)
map_step_1 <-
  setDF(df_c_j_o_step_1) %>%
  filter(contract_string_status %in% c("valid", "valid_lookup")) %>%
  arrange(outcomes_rn, desc(segstart)) %>%
  group_by(outcomes_rn) %>%
  filter(row_number() == 1) %>% # keep last call
  ungroup() %>%
  #filter(!is.na(outcomes_rn)) %>%
  distinct(calls_rn, outcomes_rn) %>%
  mutate(join_step = 1)

# ----------------------------------------------------------------------
# Step 2 of calls to outcomes join based on agent, customer's national ID, and datetime

# remove calls that have invalid contract
# remove calls that have invalid unique customer ID
df_calls_answered_step_2 <-
  df_calls_answered[ contract_string_status %in% c("valid", "valid_lookup") & !is.na(crm_key) ]

# remove outcomes that are already joined in step 1
# remove outcomes that have invalid unique customer ID
df_outcomes_step_2 <-
  df_outcomes_in_scope_with_agent_map[ !(outcomes_rn %in% map_step_1$outcomes_rn) & !is.na(crm_key) ]

setDT(df_calls_answered_step_2)
setDT(df_outcomes_step_2)

df_outcomes_step_2[, `:=`(call_dt_tempJoin = call_dt,
                          LOGIN_TELEFONIA_tempJoin = LOGIN_TELEFONIA,
                          crm_key_tempJoin = crm_key,
                          DH_ABT_OCOR_tempJoin = DH_ABT_OCOR
)]

# add temporary columns that will be used in join
df_calls_answered_step_2[, `:=`(call_dt_tempJoin = call_dt,
                                anslogin_tempJoin = anslogin,
                                crm_key_tempJoin = crm_key,
                                talk_start_tm_tempJoin = talk_start_tm,
                                next_talk_start_tm_with_buffer_tempJoin = next_talk_start_tm_strong_join # TODO: check without time join
                                #next_talk_start_tm_with_buffer_tempJoin = next_talk_start_tm_with_buffer_strong_join
)]

# join calls to outcomes
df_c_j_o_step_2 <-
  df_outcomes_step_2[df_calls_answered_step_2,
                     on = .(call_dt_tempJoin = call_dt_tempJoin,
                            LOGIN_TELEFONIA_tempJoin = anslogin_tempJoin,
                            crm_key_tempJoin = crm_key_tempJoin,
                            DH_ABT_OCOR_tempJoin >= talk_start_tm_tempJoin,
                            DH_ABT_OCOR_tempJoin < next_talk_start_tm_with_buffer_tempJoin
                     ),
                     mult = 'all']

# remove temporary columns added for data table join
df_outcomes_step_2[, grep(".*_tempJoin$", colnames(df_outcomes_step_2)):=NULL]
df_calls_answered_step_2[, grep(".*_tempJoin$", colnames(df_calls_answered_step_2)):=NULL]
df_c_j_o_step_2[, grep(".*_tempJoin$", colnames(df_c_j_o_step_2)):=NULL]

# take care of cases where:
#   multiple outcomes can be linked to one call
#     remove cases where contract in calls and outcomes is the same (already taken care of in step 1)
#     in such cases make sure calls and outcomes have different contract (for the same customer)
#     give priority to the last outcome for each call-contract pair
#   multiple calls can be linked to one outcome
#     give priority to the last call (closest to the outcome)
map_step_2 <-
  setDF(df_c_j_o_step_2)  %>%
  filter(contract_final != contract_outcome) %>%
  arrange(calls_rn, contract_final, desc(DH_ABT_OCOR)) %>%
  group_by(calls_rn, contract_final) %>%
  filter(row_number() == 1) %>% # keep last outcome against each call-contract pair
  ungroup() %>%
  arrange(outcomes_rn, desc(segstart)) %>%
  group_by(outcomes_rn) %>%
  filter(row_number() == 1) %>% # keep last call against a single outcome
  ungroup() %>%
  distinct(calls_rn, outcomes_rn) %>%
  #filter(!calls_rn %in% (map_step_1 %>% select(calls_rn) %>% pull())) %>%
  mutate(join_step = 2)

# ----------------------------------------------------------------------
# Step 3 of calls to outcomes join based on agent, and datetime

# call where we have a valid contract id
v_valid_contracts <-
  df_calls_answered %>%
  filter(contract_string_status %in% c("valid", "valid_lookup")) %>%
  select(calls_rn) %>%
  pull()

# remove calls with valid contract id - already taken care of in previous joins
df_calls_answered_step_3 <-
  df_calls_answered[!(calls_rn %in% v_valid_contracts)]

# remove outcomes that are already joined in step 1, 2
df_outcomes_step_3 <-
  df_outcomes_in_scope_with_agent_map[!(outcomes_rn %in% c(map_step_1$outcomes_rn, map_step_2$outcomes_rn))]

# add temporary columns that will be used in join
df_outcomes_step_3[, `:=`(call_dt_tempJoin = call_dt,
                          LOGIN_TELEFONIA_tempJoin = LOGIN_TELEFONIA,
                          DH_ABT_OCOR_tempJoin = DH_ABT_OCOR
)]

# add temporary columns that will be used in join
df_calls_answered_step_3[, `:=`(call_dt_tempJoin = call_dt,
                                anslogin_tempJoin = anslogin,
                                talk_start_time_with_buffer_tempJoin = talk_start_tm_with_buffer,
                                next_talk_start_tm_with_buffer_tempJoin = next_talk_start_tm_with_buffer_weak_join
)]

# join calls to outcomes
df_c_j_o_step_3 <-
  df_outcomes_step_3[df_calls_answered_step_3,
                     on = .(call_dt_tempJoin = call_dt_tempJoin,
                            LOGIN_TELEFONIA_tempJoin = anslogin_tempJoin,
                            DH_ABT_OCOR_tempJoin >= talk_start_time_with_buffer_tempJoin,
                            DH_ABT_OCOR_tempJoin < next_talk_start_tm_with_buffer_tempJoin
                     ),
                     mult = 'last']

# remove temporary columns added for data table join
df_outcomes_step_3[, grep(".*_tempJoin$", colnames(df_outcomes_step_3)):=NULL]
df_calls_answered_step_3[, grep(".*_tempJoin$", colnames(df_calls_answered_step_3)):=NULL]
df_c_j_o_step_3[, grep(".*_tempJoin$", colnames(df_c_j_o_step_3)):=NULL]

# take care of cases where multiple calls can be linked to one outcome
# give priority to the last call (closest to the outcome)
map_step_3 <-
  setDF(df_c_j_o_step_3)  %>%
  arrange(outcomes_rn, desc(segstart)) %>%
  group_by(outcomes_rn) %>%
  filter(row_number() == 1) %>% # keep last call
  ungroup() %>%
  distinct(calls_rn, outcomes_rn) %>%
  #filter(!calls_rn %in% (map_step_1 %>% select(calls_rn) %>% pull())) %>%
  mutate(join_step = 3)

map_steps <-
  rbind(map_step_1, map_step_2, map_step_3) %>%
  filter(!is.na(outcomes_rn)) %>%
  arrange(join_step) %>%
  group_by(calls_rn) %>%
  filter(row_number() == 1) %>%
  ungroup()

setDF(df_calls_answered)
setDF(df_outcomes_in_scope)
setDF(df_outcomes_in_scope_with_agent_map)
Sys.sleep(sleep_seconds)

# free up RAM
if(flag_purge_env_vars) {
  rm(tbl_agent_mapping_raw)
  rm(tbl_outcomes_raw)
  rm(df_outcomes_raw)
  rm(map_step_1)
  rm(map_step_2)
  rm(df_c_j_o_step_1)
  rm(df_c_j_o_step_2)
  rm(df_c_j_o_step_3)
  rm(df_calls_answered_step_2)
  rm(df_outcomes_step_2)
  rm(df_outcomes_in_scope_with_agent_map)
}

# ======================================================================

# unloading data.table and re-loading tidyverse and lubridate to take care of object masking by data.table library
#detach("package:data.table", unload=TRUE)
library(tidyverse)
library(lubridate)

df_final_join <-
  df_calls_answered %>%
  left_join(
    map_steps,
    by = c("calls_rn" = "calls_rn"),
    suffix = c("", ".y")
  ) %>%
  left_join(
    df_outcomes_in_scope,
    by = c("outcomes_rn" = "outcomes_rn"),
    suffix = c("", ".y")
  ) %>%
  mutate(
    isJoined = if_else(is.na(outcomes_rn), 0, 1),
    isSaved = if_else(isJoined == 0, 1, isSaved)
  )

# check if there are duplicate calls
total_calls <- df_final_join %>% filter(!is.na(calls_rn)) %>% select(calls_rn) %>% count()
distinct_calls <- df_final_join %>% filter(!is.na(calls_rn)) %>% distinct(calls_rn) %>% count()
if (total_calls != distinct_calls)
{ warning( "Duplicate calls" ) }

# check if there are duplicate outcomes
total_outcomes <- df_final_join %>% filter(!is.na(outcomes_rn)) %>% select(outcomes_rn) %>% count()
distinct_outcomes <- df_final_join %>% filter(!is.na(outcomes_rn)) %>% distinct(outcomes_rn) %>% count()
if (total_outcomes != distinct_outcomes)
{ warning( "Duplicate outcomes" ) }

df_final_join_filtered <-
  df_final_join %>%
  filter(isInScopeSkill == 1)

df_final_join_filtered_grouped <-
  df_final_join_filtered %>%
  group_by(calls_rn, contract_final, calling_pty_final, crm_key, dispsplit, dispvdn, segstart, segstop) %>%
  summarize(
    outcomes_count = n(),
    number_of_cancels = sum(is_cancelled, na.rm = TRUE),
    outcomes_rn_concat = paste0(outcomes_rn, collapse = ","),
    NM_SUB_INDICADOR_INTELIGENCIA_concat = paste0(NM_SUB_INDICADOR_INTELIGENCIA, collapse = ","),
    NM_TIPO_OCORRENCIA_concat = paste0(NM_TIPO_OCORRENCIA, collapse = ","),
    DH_ABT_OCOR_concat = paste0(DH_ABT_OCOR, collapse = ","),
    CD_OPERADORA_concat = paste0(CD_OPERADORA, collapse = ","), 
  )

# ======================================================================
# calculate rates

calls_to_outcomes_dr <-
  df_final_join_filtered %>%
  group_by(call_dt) %>%
  summarise(
    #    outcomes_count_inScope = sum(isFiltered), #, na.rm = TRUE),
    calls_count_inScope = n(),
    calls_count_isJoined = sum(isJoined, na.rm = TRUE),
    calls_count_inScope_isJoined = sum(isInScopeSkill & isJoined, na.rm = TRUE),
    disposition_rate = calls_count_isJoined/calls_count_inScope*100,
    disposition_rate_step_1 = sum(isJoined & join_step == 1, na.rm = TRUE)/calls_count_inScope*100,
    disposition_rate_step_2 = sum(isJoined & join_step == 2, na.rm = TRUE)/calls_count_inScope*100,
    #calls_count_isAgentMapped = sum(isAgentMapped, na.rm = TRUE),
    #agent_mapping_rate = calls_count_isAgentMapped/calls_count*100,
    calls_count_isSaved = sum(isSaved, na.rm = TRUE),
    save_Rate = calls_count_isSaved/calls_count_inScope*100,
    calls_count_isSaved_conservative = sum(isInScopeSkill & isJoined & isSaved, na.rm = TRUE),
    save_Rate_conservative = calls_count_isSaved_conservative/calls_count_inScope*100,
    save_rate_conservative_dispositioned_only = calls_count_isSaved_conservative/calls_count_inScope_isJoined*100,
  )

# Match Rate Code
df_outcomes_join_calls <-
  df_outcomes_in_scope %>%
  left_join(
    map_steps,
    by = c("outcomes_rn" = "outcomes_rn"),
    suffix = c("", ".y")
  ) %>%
  left_join(
    df_calls_answered,
    by = c("calls_rn" = "calls_rn"),
    suffix = c("", ".y")
  ) %>%
  mutate(
    isJoined = ifelse(is.na(calls_rn), 0, 1)
  )

outcomes_to_calls_mr <-
  df_outcomes_join_calls %>%
  group_by(call_dt) %>%
  summarise(
    #    outcomes_count_inScope = sum(isFiltered), #, na.rm = TRUE),
    outcomes_count_inScope = sum(is_in_scope_outcome, na.rm = TRUE),
    outcomes_count_isJoined = sum(is_in_scope_outcome & isJoined, na.rm = TRUE),
    outcomes_count_isJoined_step_1 = sum(is_in_scope_outcome & isJoined & join_step == 1, na.rm = TRUE),
    outcomes_count_isJoined_step_2 = sum(is_in_scope_outcome & isJoined & join_step == 2, na.rm = TRUE),
    outcomes_count_isJoined_step_3 = sum(is_in_scope_outcome & isJoined & join_step == 3, na.rm = TRUE),
    match_rate = outcomes_count_isJoined / outcomes_count_inScope * 100,
    match_rate_step_1 = sum(is_in_scope_outcome & isJoined & join_step == 1, na.rm = TRUE) / outcomes_count_inScope * 100,
    match_rate_step_2 = sum(is_in_scope_outcome & isJoined & join_step == 2, na.rm = TRUE) / outcomes_count_inScope * 100,
    match_rate_step_3 = sum(is_in_scope_outcome & isJoined & join_step == 3, na.rm = TRUE) / outcomes_count_inScope * 100,
  )

calls_to_outcomes_dr
outcomes_to_calls_mr

# ======================================================================
# logic to add agent mapping against calls to locate missing agents

#df_agent_mapping_final <-
#  rbind(
#    df_agentPerDay_tmp,
#    merge(
#      df_final_join %>% distinct(call_dt),
#      df_agent_mapping_raw %>%
#        filter(!(LOGIN_NET %in% (df_agentPerDay_tmp %>% select(LOGIN_NET) %>% pull()))) %>%
#        distinct(LOGIN_TELEFONIA, LOGIN_NET) %>%
#        rename(anslogin = LOGIN_TELEFONIA),
#      all = TRUE
#    )
#  ) %>%
#  distinct(call_dt, anslogin, LOGIN_NET)

df_agent_mapping_final <-
  df_agentPerDay_tmp %>%
  distinct(call_dt, anslogin, LOGIN_NET) %>%
  group_by(call_dt, anslogin) %>%
  filter(row_number() == 1) %>%
  ungroup()

df_final_join_filtered_withAgentMap <-
  df_final_join_filtered %>%
  left_join(
    df_agent_mapping_final,
    by = c("call_dt" = "call_dt", "anslogin" = "anslogin"),
    suffix = c("", ".y")
  ) %>%
  mutate(
    isAgentMapped = if_else(is.na(LOGIN_NET), 0, 1)
  )

# ======================================================================
# make a df with specific column sequence and names - for ACA

v_new_colnames <- 
  c(
    "isInScopeSkill",
    "calling_pty_final",
    "calls_rn",
    "call_dt",
    "call_mn",
    "contract",
    "contract_final",
    "is_contract_originally_populated",
    "contract_string_status",
    "retention_type",
    "talk_start_tm",
    "next_talk_start_tm",
    "talk_end_tm",
    "outcomes_rn",
    "join_step",
    "threat_type",
    "isSaved",
    "isJoined",
    "LOGIN_NET",
    "isAgentMapped"
  )

v_colnames <- 
  c(
    v_calls_colnames,
    v_outcomes_colnames,
    v_new_colnames
  )

df_aca <-
  df_final_join_filtered_withAgentMap %>% select(v_colnames)

names(df_aca) <-
  c(
    paste0("echi_", v_calls_colnames),
    paste0("outcomes_", v_outcomes_colnames),
    v_new_colnames
  )

# ======================================================================
# sensors

if(flag_execute_sensors) {
  df_aca <-
    tbl_aca %>%
    filter(
      echi_segstart >= dt_bgn,
      echi_segstart < dt_end
    ) %>%
    collect()
  
  snsr_disposition_rate <-
    df_aca %>%
    summarise(
      #    outcomes_count_inScope = sum(isFiltered), #, na.rm = TRUE),
      
      calls_count_inScope = sum(isInScopeSkill, na.rm = TRUE),
      calls_count_isJoined = sum(isInScopeSkill & isJoined, na.rm = TRUE),
      disposition_rate = calls_count_isJoined/calls_count_inScope*100,
      #calls_count_isAgentMapped = sum(isAgentMapped, na.rm = TRUE),
      #agent_mapping_rate = calls_count_isAgentMapped/calls_count_inScope*100,
      calls_count_isSaved = sum(isSaved, na.rm = TRUE),
      save_Rate = calls_count_isSaved/calls_count_inScope*100,
      
    )
  
  
  # disposition rate
  calls_to_outcomes_dr$disposition_rate
  
  # match rate
  outcomes_to_calls_mr$match_rate
  
  # conversion rate
  calls_to_outcomes_dr$save_rate
  calls_to_outcomes_dr$save_Rate_conservative
  calls_to_outcomes_dr$save_rate_conservative_dispositioned_only
  
  # zero agent
  
  #	calls-agent match rate
  
}

#==================================
# scratch code below this line





