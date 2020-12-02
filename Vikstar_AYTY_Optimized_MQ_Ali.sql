## Muhammad Ali Ahmad: muhammad.aahmed 


DELIMITER $$
CREATE DEFINER=`dba_amaim`@`%` PROCEDURE `TfonicaVikstar_AYTY.sp_aca`(IN startdateparam DATETIME, IN enddateparam DATETIME)
BEGIN
  
  
  #############################################################################################################
  ###################################
  #####				
  #####	   AKIVA MATCHING QUERY END
  #####				
  ###################################
  #############################################################################################################
  #############################################################################################################
  ###################################
  #####				
  #####	  AYTY MATCHING QUERY START
  #####				
  ###################################
  #############################################################################################################
  
  
  SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
  SET SQL_SAFE_UPDATES = 0;
  
  SET @startdate    :=   IFNULL(startdateparam, (select date_add(date_add(LAST_DAY(curdate()-interval 1 day),interval 1 DAY),interval -1 MONTH)));
  SET @enddate    :=     IFNULL(enddateparam, curdate());
  
  ### Process Calls Data
  DROP    TEMPORARY TABLE IF EXISTS  tempdb.CallsData;
  CREATE  TEMPORARY TABLE   IF NOT EXISTS tempdb.CallsData
  SELECT  a.*,
          (CASE
              WHEN STATUS LIKE '%Venda confirmada%' THEN 1
              ELSE 0
              END) AS Is_Sale,
          CONCAT(telefone, '_', login_telefonia) AS CB_key,
          `tipo_de_liga??o` AS tipo_de_ligao,
          `id_tipo_de_liga??o` AS id_tipo_de_ligao
  FROM    vikstarsatmap.`TfonicaVikstar_AYTY.dialerextract_attend` a
  WHERE   dt_chamada >= @startdate AND dt_chamada < @enddate;
  
  ALTER TABLE tempdb.CallsData  ADD INDEX `idx_tipo_de_ligao` (tipo_de_ligao);
  
  # Call Backs
  DROP    TEMPORARY TABLE IF EXISTS  tempdb.CallBacks;
  CREATE  TEMPORARY TABLE   IF NOT EXISTS tempdb.CallBacks
  SELECT  *
  FROM    tempdb.callsdata a
  WHERE   tipo_de_ligao = 'Atendimento no Ativo';
  
  # Predictive Calls
  SET     @cb_key='' , @R:=1;
  DROP    TEMPORARY TABLE IF EXISTS  tempdb.PC1;
  CREATE  TEMPORARY TABLE IF NOT EXISTS tempdb.PC1
  SELECT  *,
          @R:=(CASE WHEN (@cb_key = cb_key) THEN @R+1 ELSE 1 END) RN,    
          @cb_key:=cb_key
  FROM    tempdb.callsdata a
  WHERE   tipo_de_ligao <> 'Atendimento no Ativo'
  ORDER BY    cb_key;
  
  DROP TEMPORARY TABLE IF EXISTS  tempdb.PC2;
  CREATE TEMPORARY TABLE tempdb.PC2 like tempdb.PC1;
  
  INSERT INTO tempdb.PC2
  SELECT * FROM tempdb.PC1;
  
  ALTER TABLE tempdb.PC1  ADD INDEX `idx_CB_key_RN` (CB_key,RN);
  ALTER TABLE tempdb.PC2  ADD INDEX `idx_CB_key_RN` (CB_key,RN);
  
  # Self JOINing to add next call time for the next predictive call with same cb_key
  DROP    TEMPORARY TABLE IF EXISTS tempdb.PredCalls ;
  CREATE  TEMPORARY TABLE tempdb.PredCalls  
  SELECT  a.*,
          b.dt_chamada AS Next_Call_Time,
          b.id_call_ayty AS id_call_ayty_second
  FROM    tempdb.PC1 a
          LEFT JOIN tempdb.PC2 b ON (a.CB_key = b.CB_key AND a.RN + 1 = b.RN); 
  
  ALTER TABLE tempdb.PredCalls  ADD INDEX `idx_CB_key_Next_Call_Time` (CB_key,Next_Call_Time);
              
  # to avoid incorrect JOINs for CASE WHEN - consective predictiver calls even after/before a manual callback 
  DROP    TEMPORARY TABLE IF EXISTS tempdb.repeatkeys ;
  CREATE  TEMPORARY TABLE tempdb.repeatkeys
  SELECT  cb_key 
  FROM    tempdb.PredCalls 
  WHERE   Next_Call_Time IS NULL;
  
  ALTER TABLE tempdb.PredCalls  ADD INDEX `idx_Next_Call_Time` (dt_chamada);
  ALTER TABLE tempdb.repeatkeys  ADD INDEX `idx_CB_key` (CB_key);
  
  UPDATE tempdb.`PredCalls` 
  SET     Next_Call_Time = date_add(last_day(dt_chamada), interval 1 day)
  WHERE   (Next_Call_Time IS NULL
          AND cb_key IN (
              SELECT  cb_key
              FROM    tempdb.repeatkeys));
                              
  ALTER TABLE tempdb.PredCalls  ADD INDEX `idx_CB_key_dt_chamada_Next_Call_Time` (CB_key,dt_chamada,Next_Call_Time);
  ALTER TABLE tempdb.CallBacks  ADD INDEX `idx_CB_key_dt_chamada` (CB_key,dt_chamada);
  
  # JOINing Callbacks to the predictive calls based ON cb_keys and 
  # ensuring that the callback is NOT JOINed to aNOTher predictive call with same cb_key
  DROP    TEMPORARY TABLE IF EXISTS tempdb.Dialer_Callbacks;
  CREATE  TEMPORARY TABLE tempdb.Dialer_Callbacks 
  SELECT 
          a.*,
          TIMEDIFF(b.dt_chamada, a.dt_chamada) time_dIFference,
          b.is_sale AS Is_sale_CallBack,
          b.status AS Status_CallBack,
          b.id_call_ayty AS id_call_ayty_CallBack,
          b.dt_chamada AS DateTime_Callback,
          b.benchmark_afiniti AS benchmark_afiniti_Callback,
          b.login_telefonia AS login_telefonia_CallBack,
          b.telefone AS Telefone_callback,
          b.tipo_de_ligao AS tipo_de_ligao_CallBack
  
  FROM    TEMPDB.PredCalls a
          LEFT JOIN TEMPDB.CallBacks b ON a.cb_key = b.cb_key
          AND (a.dt_chamada < b.dt_chamada
  			AND b.dt_chamada < a.Next_Call_Time AND b.is_Sale=1);
  
  ALTER TABLE tempdb.dialer_callbacks  ADD INDEX `idx_CB_key_time_dIFference` (CB_key,time_dIFference);
  
  # Removing dupes based ON time dIFference
  SET     @cb_key1='' , @R:=1;
  DROP    TEMPORARY TABLE IF EXISTS tempdb.RowNumber_by_timedIFf;
  CREATE  TEMPORARY TABLE tempdb.RowNumber_by_timedIFf 
  SELECT  *,
          @R:=(CASE
                  WHEN (@cb_key1 = cb_key) THEN @R + 1
                  ELSE 1
                  END) R,
          @cb_key1:=cb_key,
          Greatest(Is_Sale,IFnull(Is_sale_CallBack,0)) `Is_Sale_Final` -- int(1) NOT NULL DEFAULT '0'
  FROM    tempdb.dialer_callbacks
  ORDER BY cb_key , time_dIFference DESC;
  
  # finalized Dialer logs extract - ready to be JOINed to ECL
  DROP    TEMPORARY TABLE IF EXISTS tempdb.DialerLogs;
  CREATE  TEMPORARY TABLE tempdb.DialerLogs
  SELECT 	id_call_ipbx,
          ddd,
          id_campanha,
          code_mailing_client,
          id_status,
          Status,
          id_operador,
          login_telefonia,
          id_call_ayty,
          id_status_ipbx,
          desconexao,
          telefone,
          dt_chamada,
          rota,
          duracao,
          nome_mailing_ayty,
          grupo_de_atendimento,
          benchmark_afiniti,
          has_used_agent_SELECTed_afiniti,
          id_tipo_de_ligao,
          tipo_de_ligao,
          LoadDate,
          LoadDate AS UpdateDate,
          DB_instance,
          Is_Sale,
          CB_key,
          is_Sale_final,
          time_dIFference,
          Is_sale_CallBack,
          Status_CallBack,
          id_call_ayty_CallBack,
          DateTime_Callback,
          benchmark_afiniti_Callback,
          login_telefonia_CallBack,
          Telefone_callback,
          tipo_de_ligao_CallBack
  FROM    tempdb.RowNumber_by_timedIFf
  UNION
  SELECT  id_call_ipbx,
  	    ddd,
          id_campanha,
          code_mailing_client,
          id_status,
          Status,
          id_operador,
          login_telefonia,
          id_call_ayty,
          id_status_ipbx,
          desconexao,
          telefone,
          dt_chamada,
          rota,
          duracao,
          nome_mailing_ayty,
          grupo_de_atendimento,
          benchmark_afiniti,
          has_used_agent_SELECTed_afiniti,
          id_tipo_de_ligao,
          tipo_de_ligao,
          LoadDate,
          UpdateDate,
          DB_instance,
          Is_Sale,
          CB_key,
          Is_Sale AS is_Sale_final,  # change this statement to NULL sale for CB record is NOT to be countedtowards the final sale flag
          NULL,
          NULL,
          NULL,
          NULL,
          NULL,
          NULL,
          NULL,
          NULL,
          NULL
  FROM    tempdb.CallBacks;
   
   
  
  DROP    TEMPORARY TABLE IF EXISTS tempdb.ecl;
  CREATE  TEMPORARY TABLE tempdb.ecl (
          `CallID` varchar(50)  DEFAULT NULL,
          `CallrouteRepeat` int(11) DEFAULT NULL,
          `BTN` varchar(20)  DEFAULT NULL,
          `callTime` datetime DEFAULT NULL,
          `agentID_ECL` varchar(50)  DEFAULT NULL,
          `on_off` int(11) DEFAULT NULL,
          `EngineID` varchar(20)  DEFAULT NULL,
          `CallGUID` varchar(64)  DEFAULT NULL,
          `abandon` int(11) DEFAULT NULL,
          `Skill_ECL` varchar(64)  DEFAULT NULL,
          `LastRouteResponse` varchar(64)  DEFAULT NULL,
          `NOTroutereason`varchar(64)  DEFAULT NULL,
          `switcherror` varchar(200)  DEFAULT NULL ,
          `reason` varchar(250) DEFAULT NULL,
          key (callid)
          ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
  -- change-1  ( addition of index and engineid = "Tfonica_Vikstar_AYTY")
  # ECL- ready to be JOINed to Dialer logs
  INSERT INTO tempdb.ecl   
  SELECT      CallID,
              CallrouteRepeat,
              BTN,callTime,
              agentID,
              ON_off,
              EngineID,
              CallGUID,
              abandon,
              skill,
              LastRouteResponse ,
              NOTroutereason,
              switcherror,
              reason
  FROM        `Vikstarsatmap`.`Engine.ECL`
  WHERE       abandon=0 AND  calltime >= @startdate AND calltime < @enddate and engineid = "Tfonica_Vikstar_AYTY";
    
  DELETE FROM tempdb.ecl 
  WHERE       reason LIKE '%SATMAP_SHUTDOWN%'; 
    
    
  ALTER TABLE tempdb.ECL  ADD INDEX `idx_callid_agentid` (agentID_ECL,callid);
  ALTER TABLE tempdb.DialerLogs  ADD INDEX `id_call_ipbx_login_telefonia` (login_telefonia,id_call_ipbx); 
  
  
  # ACA - ECL JOINed to Dialer logs
  DROP    TEMPORARY TABLE IF EXISTS tempdb.Dialer_ECL;
  CREATE  TEMPORARY TABLE tempdb.Dialer_ECL
  SELECT  a.*,
          b.CallID,
          b.CallrouteRepeat,
          b.BTN,
          b.calltime,
          b.agentID_ECL,
          b.on_off,
          b.EngineID,
          b.callguid,
          b.abandon,
          b.Skill_ECL,
          b.LastRouteResponse,
          b.NOTRouteReason,
          b.SwitchError
  FROM    tempdb.DialerLogs a
          LEFT JOIN tempdb.ecl b ON (a.login_telefonia = b.agentID_ECL) AND (a.id_call_ipbx = b.CALLID);
  		-- ON ( convert(a.login_telefonia using LATIN1) = b.agentid  AND
  -- convert(a.id_call_ipbx using LATIN1) = b.CallID )	 ;
  
  ALTER TABLE tempdb.Dialer_ECL  ADD INDEX `idx_id_call_ipbx` (id_call_ipbx);
  
  # Fetching dupliacting NON-sale repeating CallIDs
  DROP    TEMPORARY TABLE IF EXISTS tempdb.Repating_ID_count;
  CREATE  TEMPORARY TABLE tempdb.Repating_ID_count 
  SELECT  id_Call_ipbx, COUNT(*)
  FROM    tempdb.Dialer_ECL
  GROUP BY    1
  HAVING      COUNT(*) > 1
  ORDER BY    2 DESC;
  
  
  SET     @id_call_ipbx='' , @R:=1;
  DROP    TEMPORARY TABLE IF EXISTS tempdb.Repating_ID;
  CREATE  TEMPORARY TABLE tempdb.Repating_ID 
  SELECT  *
  FROM
          (SELECT 
                  $.*,
                  @R:=(CASE
                          WHEN (@id_call_ipbx = id_call_ipbx) THEN @R + 1
                          ELSE 1
                          END) R,
                  @id_call_ipbx:=id_call_ipbx
          FROM    tempdb.Dialer_ECL $
          ORDER BY    id_call_ipbx ,is_sale_final desc, UpdateDate DESC) x
  WHERE   id_Call_ipbx IN (
          SELECT  id_Call_ipbx
          FROM    tempdb.Repating_ID_count);
             
  # Keeping ONly the correct callrecords to be readded
  DELETE FROM tempdb.`Repating_ID` 
  WHERE   r <> 1; 
  
  # Removing the dupliacting NON-sale repeating CallIDs
  DELETE FROM tempdb.Dialer_ECL 
  WHERE   id_Call_ipbx IN (
          SELECT  id_Call_ipbx
          FROM    tempdb.Repating_ID); 
              
             
              
  # Inserting the Calls that were removed because repeating CallIDs        
  INSERT INTO tempdb.Dialer_ECL 
  SELECT  
          id_call_ipbx,
          ddd,
          id_campanha,
          code_mailing_client,
          id_status,
          `Status`,
          id_operador,
          login_telefonia,
          id_call_ayty,
          id_status_ipbx,
          desconexao,
          telefone,
          dt_chamada,
          rota,
          duracao,
          nome_mailing_ayty,
          grupo_de_atendimento,
          benchmark_afiniti,
          has_used_agent_SELECTed_afiniti,
          id_tipo_de_ligao,
          tipo_de_ligao,
          LoadDate,
          UpdateDate,
          DB_instance,
          Is_Sale,
          CB_key,
          is_Sale_final,
          time_dIFference,
          Is_sale_CallBack,
          Status_CallBack,
          id_call_ayty_CallBack,
          DateTime_Callback,
          benchmark_afiniti_Callback,
          login_telefonia_CallBack,
          Telefone_callback,
          tipo_de_ligao_CallBack,
          CallID,
          CallrouteRepeat,
          BTN,
          calltime,
          agentID_ECL,
          ON_off,
          EngineID,
          callguid,
          abandon,
          Skill_ECL,
          LastRouteResponse,
          NOTRouteReason,
          SwitchError
  FROM    tempdb.`Repating_ID` ;
  
  delete from `TfonicaVikstar_AYTY.aca` where dt_chamada >= @startdate and dt_chamada < @enddate;
  insert into `TfonicaVikstar_AYTY.aca`
  select * from `tempdb`.`Dialer_ECL` ;
  
  #############################################################################################################
  ###################################
  #####				
  #####	    AYTY MATCHING QUERY END
  #####				
  ###################################
  #############################################################################################################
  #############################################################################################################
  ###################################
  #####				
  #####	   COMBINED ACA QUERY START
  #####				
  ###################################
  #############################################################################################################
  
  
  SET @startdate_for_aca := date_add(date_add(LAST_DAY(@startdate),interval 1 DAY),interval -1 MONTH);
  
  ## AKIVA Column harmonization with AYTY
  DROP    TEMPORARY TABLE IF EXISTS tempdb.akiva_union;
  CREATE  TEMPORARY TABLE tempdb.akiva_union
  SELECT
          'AKIVA' AS switch_name,
          ID,
          CONN_ID,
          Interaction_ID,
          ACA_BTN,
          Campaign_Name,
          Agent_Login_ID,
          START_TS,
          END_TS,
          classIFica_id,
          DURATION,
          ClassIFication_Description,
          Mailing_FileName_Original,
          final_contact,
          final_telephone,
          success,
          refuse,
          CallRouteRepeat,
          Campaign_Description,
          CallID,
          ON_off,
          BTN,
          callTime,
          EngineID,
          CallGUID,
          AgentExten,
          finalRec,
          Plan,
          Mailing_Description,
          callback_agent,
          callback_time,
          callback_BTN,
          callback_success,
          Callback_Conn_id,
          Success_Final,
          Success_Final2,
          Type_Success_Outcome,
          "NA" AS ON_off_callback,
          'NA' AS CB_key,
          'NA' AS desconexao,
          'NA' AS time_dIFference,
          'NA' AS ddd,
          'NA' AS telefone,
          'NA' AS id_campanha,
          'NA' AS id_operador,
          'NA' AS code_mailing_client,
          'NA' AS id_call_ayty,
          'NA' AS id_status_ipbx,
          'NA' AS rota,
          'NA' AS nome_mailing_ayty,
          'NA' AS has_used_agent_SELECTed_afiniti,
          'NA' AS id_tipo_de_ligao,
          'NA' AS tipo_de_ligao,
          'NA' AS LoadDate,
          'NA' AS UpdateDate,
          'NA' AS DB_instance,
          'NA' AS benchmark_afiniti
  ### This query can ONly be used for ONe month at a time
  FROM    vikstarsatmap.`vikstartfn.aca` 
  WHERE   start_ts >= @startdate_for_aca AND start_ts < @enddate;
  
  
  
  ## AYTY Union Column Harmonization
  ### Preparing agent mapping for JOINing with ayty aca
  
  -- WHEN we have scd implemented by etl, we will remove distinct and use the applicable logic instead
  -- change-2 (Addition of index )
  DROP    TEMPORARY TABLE IF EXISTS tempdb.agent_mapping;
  CREATE  TEMPORARY TABLE tempdb.agent_mapping
    ( INDEX `idcodusuario_3` (`scd_start`),
  INDEX `idcodusuario_4` (`scd_end`),
  INDEX `idcodusuario_5` (`idcodusuario`),
  INDEX `idcodusuario_6` (`short_cpf`))
  SELECT 	DISTINCT fornecedor, idcodusuario, cpf, LEFT(cpf,9) AS short_cpf, scd_start,scd_end
  FROM 	etl_dev.`tfonicavikstar_ayty.agent_mapping`
  WHERE	fornecedor = "AYTY";
  
  
  DROP    TEMPORARY TABLE  IF EXISTS tempdb.ecl_1;
  CREATE  TEMPORARY TABLE  tempdb.ecl_1
  SELECT  * 
  FROM    `Vikstarsatmap`.`Engine.ECL` 
  WHERE   calltime >= @startdate 
          AND calltime < @enddate 
          AND engineid = "Tfonica_Vikstar_AYTY"
          AND reason NOT LIKE '%SATMAP_SHUTDOWN%';
  
  ALTER TABLE tempdb.dialer_ecl Add index `aytyunion`(callguid);
  ALTER TABLE tempdb.ecl_1 Add index `aytyunion2`(callguid);
  
  ##### Preparing ayty aca for union
  DROP    TEMPORARY TABLE IF EXISTS tempdb.ayty_union;
  CREATE  TEMPORARY TABLE tempdb.ayty_union
  SELECT
          'AYTY' AS switch_name,
          CAST(NULL AS CHAR(100)) AS ID_ayty,
          a.id_call_ipbx CONN_ID_ayty,
          CAST(NULL AS CHAR(100))AS Interaction_ID_ayty,
          IFNULL(b.CalledNumber, concat(a.ddd,a.telefone)) AS ACA_BTN_ayty,
          a.grupo_de_atendimento AS Campaign_Name_ayty,
          a.login_telefonia AS Agent_Login_ID_ayty,
          a.dt_chamada AS START_TS_ayty,
          ADDTIME(dt_chamada,SEC_TO_TIME(duracao)) AS END_TS_AYTY,
          a.id_status AS classIFica_id_ayty,
          SEC_TO_TIME(duracao) AS DURATION_ayty,
          a.Status AS ClassIFication_Description_ayty,
          "NA" AS Mailing_FileName_Original_ayty,
          "NA" AS final_contact_ayty,
          "NA" AS final_telephone_ayty,
          CASE 
              WHEN a.is_sale = 0 THEN "N" 
              WHEN a.is_sale = 1 THEN "Y" 
              END AS success_ayty,  ###a.Is_Sale AS success_ayty, ### N or 0
          "NA" AS refuse_ayty,
          b.CallRouteRepeat AS CallRouteRepeat_ayty,
          "NA" AS Campaign_Description_ayty, ####??
          a.CallID AS CallID_ayty,
          coalesce(b.on_off,0) AS ON_off_ayty,
          a.BTN AS BTN_ayty,
          a.callTime AS callTime_ayty,
          a.EngineID AS EngineID_ayty,
          a.CallGUID AS CallGUID_ayty, #### ??
          b.agentExt AS AgentExten_ayty,
          "NA" AS finalRec_ayty,
          "NA" AS Plan_ayty,
          "NA" AS Mailing_Description_ayty,
          a.login_telefonia_CallBack AS callback_agent_ayty,
          a.DateTime_Callback AS callback_time_ayty,
          "NA" AS callback_BTN_ayty,
          CASE 
              WHEN a.Is_sale_CallBack  = 0 THEN "N" 
              WHEN a.Is_sale_CallBack  = 1 THEN "Y" 
              END AS callback_success_ayty,  ###a.Is_sale_CallBack AS callback_success_ayty,
          "NA" AS Callback_Conn_id_ayty,
          (CASE
  		    WHEN a.tipo_de_ligao = 'Atendimento no Ativo' THEN 'N'
              WHEN coalesce(a.is_sale_callback,0)=1 THEN "Y"
              WHEN a.is_sale = 1 THEN "Y" 
              ELSE "N"
              END) AS Success_Final_ayty, 
          "NA" AS Success_Final2_ayty,
          "NA"  AS Type_Success_Outcome_ayty,
          CASE 
              WHEN a.benchmark_afiniti_Callback  = "Y" THEN 1 
              ELSE 0 
              END AS benchmark_afiniti_callback_ayty,
          a.CB_key,
          a.desconexao,
          a.time_dIFference,
          a.ddd,
          a.telefone,
          a.id_campanha,
          a.id_operador,
          a.code_mailing_client,
          a.id_call_ayty,
          a.id_status_ipbx,
          a.rota,
          a.nome_mailing_ayty,
          a.has_used_agent_SELECTed_afiniti,
          a.id_tipo_de_ligao,
          a.tipo_de_ligao,
          a.LoadDate,
          a.UpdateDate,
          a.DB_instance,
          a.benchmark_afiniti
  FROM    tempdb.dialer_ecl a 
          LEFT JOIN  tempdb.ecl_1 b on convert(a.callguid using latin1) =  b.callguid  ;
  
   -- change-3 (Addition of index while creating  temp table)
  DROP    TEMPORARY TABLE IF EXISTS tempdb.combined_aca;
  CREATE  TEMPORARY TABLE tempdb.combined_aca
   (INDEX `id_operador_1` (`switch_name`),
  index `id_operador_2`(`start_ts`),
  index `id_operador_3`(`agent_login_id`))
  SELECT  a.*,
          date(START_TS) AS call_dt,
          "NA" AS Extra_Col_1,
          "NA" AS Extra_Col_2,
          "NA" AS Extra_Col_3  
  FROM    (SELECT * FROM tempdb.akiva_union 
          UNION 
          SELECT * FROM tempdb.ayty_union) a;
  
  /*
  ALTER TABLE tempdb.combined_aca ADD INDEX `id_operador_1`(switch_name, id_operador,start_ts );
  ALTER TABLE tempdb.combined_aca ADD INDEX `id_operador_2`(switch_name, agent_login_id ,start_ts);
  
  ALTER TABLE  tempdb.agent_mapping ADD INDEX `idcodusuario_3` (scd_start,scd_end, idcodusuario);
  ALTER TABLE  tempdb.agent_mapping ADD INDEX `idcodusuario_4` (scd_start,scd_end, short_cpf);
  
  
  DROP    TEMPORARY TABLE IF EXISTS tempdb.combined_aca_final;
  CREATE  TEMPORARY TABLE tempdb.combined_aca_final
  SELECT	a.*,
  		CASE    
              WHEN a.switch_name = 'AYTY' THEN b.short_cpf
              WHEN a.switch_name = 'AKIVA' THEN a.agent_login_id
  		    END AS short_cpf_combined,
  		CASE 
              WHEN a.switch_name = 'AYTY' THEN a.agent_login_id
  			WHEN a.switch_name = 'AKIVA' THEN b.idcodusuario
  		    END AS login_telefonia_combined
  FROM 	tempdb.combined_aca a
  		LEFT JOIN tempdb.agent_mapping b
          ON (a.switch_name = 'AYTY' AND a.start_ts >= b.scd_start AND a.start_ts < b.scd_end AND a.agent_login_id = b.IDCodUsuario)
  		OR (a.switch_name = 'AKIVA' AND a.start_ts >= b.scd_start AND a.start_ts < b.scd_end AND a.agent_login_id = b.short_cpf);
  */
  -- change-4 ( Removal of 'OR Join' from  tempdb.combined_aca_final )
    DROP    TEMPORARY TABLE IF EXISTS tempdb.combined_aca_final;
  CREATE  TEMPORARY TABLE tempdb.combined_aca_final
      SELECT    a.*,
              
            b.short_cpf AS short_cpf_combined,
           a.agent_login_id as login_telefonia_combined
  FROM     tempdb.combined_aca a
          left  JOIN tempdb.agent_mapping b
          ON (a.switch_name = 'AYTY' AND a.start_ts >= b.scd_start AND a.start_ts <
          b.scd_end AND a.agent_login_id = b.IDCodUsuario); -- 206
          
  update    tempdb.combined_aca_final a left  JOIN tempdb.agent_mapping b    
          on  (a.switch_name = 'AKIVA' AND a.start_ts >= b.scd_start AND a.start_ts < b.scd_end AND a.agent_login_id = b.short_cpf)
              set  short_cpf_combined =a.agent_login_id,
            login_telefonia_combined =b.idcodusuario; -- 304 sec
          
    
  select date(start_ts),count(*) from tempdb.combined_aca_final where short_cpf_combined is null group by 1;
  
  delete from `TfonicaVikstar_AYTY.aca_combined` where start_ts >= @startdate and start_ts < @enddate;
  insert into `TfonicaVikstar_AYTY.aca_combined`
    SELECT `switch_name`,  `ID`,  `CONN_ID`,  `Interaction_ID`,  `ACA_BTN`,  `Campaign_Name`,  `Agent_Login_ID`,  
    `START_TS`,  `END_TS`,  `classifica_id`,  `DURATION`,  `Classification_Description`,  `Mailing_FileName_Original`,  `final_contact`,  
    `final_telephone`,  `success`,  `refuse`,  `CallRouteRepeat`,  `Campaign_Description`,  `CallID`,  `on_off`,  `BTN`,  `callTime`,  `EngineID`,  
    `CallGUID`,  
    case when `AgentExten` is null then 'NA' else `AgentExten` end as `AgentExten`,  `finalRec`,  `Plan`,  `Mailing_Description`,  `callback_agent`,  `callback_time`,  `callback_BTN`, 
     `callback_success`,  `Callback_Conn_id`,  `Success_Final`,  `Success_Final2`,  `Type_Success_Outcome`,  `on_off_callback`,  `CB_key`,  
     `desconexao`,  `time_difference`,  `ddd`,  `telefone`,  `id_campanha`,  `id_operador`,  `code_mailing_client`,  `id_call_ayty`,  
     `id_status_ipbx`,  `rota`,  `nome_mailing_ayty`,  `has_used_agent_selected_afiniti`,  `id_tipo_de_ligao`,  `tipo_de_ligao`,  `LoadDate`,  
     `UpdateDate`,  `DB_instance`,  `benchmark_afiniti`,  `call_dt`,  `Extra_Col_1`,  `Extra_Col_2`,  `Extra_Col_3`,  `short_cpf_combined`,  
     `login_telefonia_combined` FROM tempdb.combined_aca_final; 
  -- select * from tempdb.combined_aca_final where switch_name = "AYTY";
  
  CALL `vikstarsatmap`.`TfonicaVikstar_AYTY.ACA_Sensors`();
  
  
  replace into  vikstarsatmap.`etl.cc_sensors_data_staging`
  (Sensors_Date, ProgramID, CustomGroup1, CustomGroup2, Sensors_Name, Sensors_Value_Num)
  SELECT cast(start_ts as date) as Sensors_Date, 'TfonicaVikstar_AYTY' as ProgramID, 
  '' as CustomGroup1, '' as CustomGroup2, 'Time_Difference' as Sensors_Name,
  ifnull(ABS(AVG(TIME_TO_SEC(TIMEDIFF(b.answtime,a.start_ts)))),0) as Sensors_Value_Num
  FROM vikstarsatmap.`TfonicaVikstar_AYTY.aca_combined` a 
  left join `Vikstarsatmap`.`Engine.ECL` b on a.callguid = b.callguid
  WHERE start_ts >= @startdate 
  AND end_ts < @enddate
  AND switch_name = "AYTY" group by 1;  
    
  END$$
DELIMITER ;
