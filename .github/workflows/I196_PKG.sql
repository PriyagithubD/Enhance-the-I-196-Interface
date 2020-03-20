create or replace PACKAGE BODY      FTL_WIP_SF_TO_WIP_UPLOAD_PKG
AS
-- $FTL_CUSTHeader: FTL_SF_TO_WIP_UPLOAD_PKG.plb 1.0 26-SEP-2013 mmohanty $
----------------------------------------------------------------------------------
/*
-- Package Name : FTL_SF_TO_WIP_UPLOAD_PKG
-- Author's Name : Manisha Mohanty
-- RICEW Object ID : WIP_I-196
-- Date written : 25-Oct-2013
-- Description : Main package for shop floor to WIP interface
--
-- Maintenance History:

 Date Version Name Remarks
 ----------- ------- ------------------- ----------------------------------------------------------------------------------------------------
 25-Oct-2013 1.0 Manisha Mohanty Initial development.
 20-Dec-2013 1.1 Manisha Mohanty Defect # 383 - the second quality reason
                                 code value to be included in the Misc Receipt
                                 transaction record that is generated in Oracle.
 02-Jan-2014 1.2 Manisha Mohanty Defect # 370 - Need the ability to pass
                                 negative values in the scrap quantity column
                                 which should then reduce the previously declared
                                 scrap quantity associated to the work order.
 29-Jan-2014 1.3 Manisha Mohanty Added exception handling messages
 28-Apr-2014 1.4 Manisha Mohanty Added NVL while fetching group id
 12-JUN-2014 1.5 Hari R Srinivasan Commented the condition Firm_planned_flag =2
 17-JUN-2014 1.6 Manisha Mohanty 1. Change Request -117 -
                                  Allow processing of WIP Scrap transactions and
                                  transact to the proper scrap account for each organization.
                                  The default Account Alias "DZ TO RAG" will be used.
                                 2. Pass completion date of WO for every update to maintain
                                 correct scheduling info.
                                 3. Change the WO component preprocessing logic.
 23-MAR-2015 1.7 Manisha Mohanty Changes corresponding to MD50 version 3.0
                                 For all the records in the staging table ?FTL_WO_HDR? that are
                                 processed during the I-196 run; an additional column ?Released to PKMS?
                                 (Attribute 2 in Discrete job DFF) needs to be passed into oracle
                                 interface table with the following value
                                 Attribute 2 = 'Y'
 27-MAR-2015 1.8 Manisha Mohanty 1. Add a new status In-process (IP) to the all the records in the staging table.
                                 This status will be applied to all the records that are picked up by the program
                                 to be processed and hence will eliminate the possibility of them being picked again.
                                 2. If there is no data is any particular staging table, then the coressponding processes should be skipped.
 24-JUN-2015 1.9 C. D.           Setting creation date, created by to SYSDATE at start if NULL
 19-Nov-2015 1.10 Manisha Mohanty Build 2.1 changes
                                  1. Update work order header attribute 4(original completion date) and attribute 5(target quantity).
                                  2. Scheduling logic is updated ? If start date and completion date are passed, then manual scheduling, else auto scheduling.
                                  3. Close work order procedure added.

 25-FEB-2016  2   Rajesh alagarsamy  WIP Mass Load program processing the records in a wrong sequence
 26-MAY-2016  2.1   Rajesh alagarsamy  Added CC Job Close Work order program to close the work order
 20-JUN-2017  2.2   Chandana M        Changed the transaction_date to SYSDATE while inserting records into mtl_transactions_interface
                                      in PROCESS_MATERIAL_TRANSACTION procedure as a part of AMSEBS-943
 21-FEB-2018  2.3   Priya		 Code added for SR00413772
 16-MAR-2018  2.4   Priya			  Exception handled as part of Jira AMSEBS-1317 and condition added to avoid bom stuck in PREPROCESS status
 10-APR-2018  2.5   Priya		 Code added for SR00424971 as part of AMSEBS-1322 to trigger Close WO after processing pending tranactions in mtl_transactions_interface and wip_move_txn_interface
 23-APR-2018  3.0 	Priya		 Code changes done for I196 Parallel Processing 
 10-Aug-2018  4.0       Karthi           MFG-SR00453359 - Timezone modification on transaction_date 
 24-May-2019  4.1   Priya 		 Code change done in Create_WIP procedure to validate Assembly item for Route. (INC#IN00567095)
 08-AUG-2019  5.1	Priya		 Code change in CANCEL_WIP_JOB procedure to skip inserting status change to complete in wip_job_schedule_interface if the WO is already closed
 27-AUG-2019  6.1	Priya		 Code change added in Close WIP procedure to add initial 5 minutes wait time - Jira#AMSEBS-2030 (IN00592489)
 18-Nov-2019  7.1	Priya		 Change to add error message RITM000004445
*/
-------------------------------------------------------------------------------------------------------------------------------------------------
 ------------------------------------------------------------------------------------------------------------------------------------------------
 -----------------------< set_cnv_env >--------------------------------
 ----------------------------------------------------------------------

 ----global variables---------
 g_create VARCHAR2 (10) := 'CREATE';
 g_update VARCHAR2 (10) := 'UPDATE';
 g_preprocess VARCHAR2 (20) := 'PREPROCESS';
 g_interface VARCHAR2 (20) := 'INTERFACE';
 g_delete VARCHAR2 (20) := 'DELETE COMPONENT';
 g_issue VARCHAR2 (20) := 'WIP ISSUE';
 g_return VARCHAR2 (20) := 'WIP RETURN';
 g_wo_hdr_cnt NUMBER := 0;
 g_wo_bom_cnt NUMBER := 0;
 g_wo_txn_cnt NUMBER := 0;
 g_mat_txn_cnt NUMBER := 0;
 g_bulk_col_lim CONSTANT NUMBER := 200000 ;
----------------------------------------------------------------------
/*
Procedure Name: SET_CNV_ENV_PRC
Authors name: Manisha Mohanty
Date written: 25-Oct-2013
RICEW Object id: WIP_I-196
Description: Set Conversion Environment
Program Style: Subordinate
Change History:
Date Issue# Name Remarks
----------- ------- ------------------ ------------------------------
25-Oct-2013 1.0 Manisha Mohanty Initial development.
*/
----------------------------------------------------------------------

 PROCEDURE set_cnv_env_prc (p_org_code IN VARCHAR2)
 IS
  l_error_code NUMBER := xx_emf_cn_pkg.cn_success;
 l_module_name VARCHAR2 (60) := 'set_cnv_env_prc';

 BEGIN

 g_batch_id := null;
 g_org_code := p_org_code;

 -- Set the environment
 l_error_code := xx_emf_pkg.set_env;
 xx_emf_pkg.propagate_error (l_error_code);

 EXCEPTION WHEN OTHERS THEN
	fnd_file.put_line (fnd_file.LOG, l_module_name || ' Message:' || SQLERRM );
	RAISE xx_emf_pkg.g_e_env_not_set;
 END set_cnv_env_prc;

----------------------------------------------------------------------
------------------< mark_records_for_processing >---------------------
----------------------------------------------------------------------
----------------------------------------------------------------------
 /*
 Procedure Name: MARK_RECORD_FOR_PROCESSING_PRC
 Authors name: Manisha Mohanty
 Date written: 25-Oct-2013
 RICEW Object id: WIP_I-196
 Description: Set Conversion Environment
 Program Style: Subordinate
 Change History:
 Date Issue# Name Remarks
 ----------- ------- ------------------ ------------------------------
 25-Oct-2013 1.0 Manisha Mohanty Initial development.
 24-JUN-2015 1.1 C. D.
 */
 ----------------------------------------------------------------------
 PROCEDURE mark_record_for_processing_prc
 IS
 l_last_update_date DATE := SYSDATE;
 l_last_updated_by NUMBER := fnd_global.user_id;
 l_last_update_login NUMBER := fnd_global.user_id;
 l_module_name VARCHAR2 (60) := 'mark_record_for_processing_prc';
 PRAGMA AUTONOMOUS_TRANSACTION;

 BEGIN


FOR rec1 IN (SELECT work_order_number FROM ftl_wo_hdr WHERE org_code = g_org_code AND WORK_ORDER_STATUS='Released' AND INT_PROCESS_CODE = 'New'  GROUP BY work_order_number HAVING COUNT(1) > 1)
 LOOP

 UPDATE ftl_wo_hdr
	SET request_id = xx_emf_pkg.g_request_id,
 int_process_code = 'PROCESSED',
 int_error_message ='DUPLICATE',
 last_updated_by = l_last_updated_by,
 last_update_date = l_last_update_date,
 last_update_login = l_last_update_login
 WHERE org_code = g_org_code
  AND INT_PROCESS_CODE = 'New'
  AND  work_order_number = rec1.work_order_number
  AND  WORK_ORDER_STATUS='Released'
 AND TIMESTAMP < ( SELECT MAX(TIMESTAMP) FROM ftl_wo_hdr 
					WHERE org_code = g_org_code 
					AND WORK_ORDER_STATUS='Released' 
					AND INT_PROCESS_CODE = 'New'  
					AND work_order_number = rec1.work_order_number);
 END LOOP; 

 FOR recb1 IN (SELECT COMPONENT_ITEM_CODE, work_order_number FROM ftl_wo_bom WHERE org_code = g_org_code AND INT_PROCESS_CODE = 'New' GROUP BY work_order_number, COMPONENT_ITEM_CODE HAVING COUNT(COMPONENT_ITEM_CODE) > 1 )
 LOOP

 UPDATE ftl_wo_bom
	SET request_id = xx_emf_pkg.g_request_id,
 int_process_code = 'PROCESSED',
 int_error_message ='DUPLICATE',
 last_updated_by = l_last_updated_by,
 last_update_date = l_last_update_date,
 last_update_login = l_last_update_login
 WHERE org_code = g_org_code
 AND int_process_code = 'New'
 AND  work_order_number = recb1.work_order_number
 AND  COMPONENT_ITEM_CODE = recb1.COMPONENT_ITEM_CODE
 AND TIMESTAMP < ( 	SELECT MAX(TIMESTAMP) FROM ftl_wo_bom 
							WHERE org_code = g_org_code 
							AND INT_PROCESS_CODE = 'New' 
							AND work_order_number = recb1.work_order_number 
							AND COMPONENT_ITEM_CODE = recb1.COMPONENT_ITEM_CODE)
						;

 END LOOP;


FOR rec1 IN (SELECT work_order_number FROM ftl_wo_hdr WHERE org_code = g_org_code AND WORK_ORDER_STATUS='Closed' AND INT_PROCESS_CODE = 'New' GROUP BY work_order_number HAVING COUNT(1) > 1)
 LOOP

 UPDATE ftl_wo_hdr
	SET request_id = xx_emf_pkg.g_request_id,
 int_process_code = 'PROCESSED',
 int_error_message ='DUPLICATE',
 last_updated_by = l_last_updated_by,
 last_update_date = l_last_update_date,
 last_update_login = l_last_update_login
 WHERE org_code = g_org_code
  AND INT_PROCESS_CODE = 'New'
  AND  work_order_number = rec1.work_order_number
  AND  WORK_ORDER_STATUS='Closed'
  AND TIMESTAMP < ( SELECT MAX(TIMESTAMP) FROM ftl_wo_hdr 
						WHERE org_code = g_org_code 
						AND WORK_ORDER_STATUS='Closed' 
						AND INT_PROCESS_CODE = 'New' 
						AND work_order_number = rec1.work_order_number);
 END LOOP; 

 UPDATE ftl_wo_hdr
	SET request_id = xx_emf_pkg.g_request_id,
 int_error_code = xx_emf_cn_pkg.cn_null,
 int_process_code = xx_emf_cn_pkg.cn_in_prog,
 created_by = NVL(created_by, l_last_updated_by),       
 creation_date = NVL(creation_date, l_last_update_date), 
 last_updated_by = l_last_updated_by,
 last_update_date = l_last_update_date,
 last_update_login = l_last_update_login
 WHERE org_code = g_org_code
 AND int_process_code = xx_emf_cn_pkg.cn_new
;

 g_wo_hdr_cnt := sql%rowcount;
 fnd_file.put_line(fnd_file.LOG, ' g_wo_hdr_cnt :' || g_wo_hdr_cnt );

 UPDATE ftl_wo_bom
	SET request_id = xx_emf_pkg.g_request_id,
 int_error_code = xx_emf_cn_pkg.cn_null,
 int_process_code = xx_emf_cn_pkg.cn_in_prog,
 created_by = NVL(created_by, l_last_updated_by),       
 creation_date = NVL(creation_date, l_last_update_date), 
 last_updated_by = l_last_updated_by,
 last_update_date = l_last_update_date,
 last_update_login = l_last_update_login
 WHERE org_code = g_org_code
 AND int_process_code = xx_emf_cn_pkg.cn_new
 ;

 g_wo_bom_cnt := sql%rowcount;
 fnd_file.put_line(fnd_file.LOG, ' g_wo_bom_cnt :' || g_wo_bom_cnt );

 UPDATE ftl_wo_txn
	SET request_id = xx_emf_pkg.g_request_id,
 int_error_code = xx_emf_cn_pkg.cn_null,
 int_process_code = xx_emf_cn_pkg.cn_in_prog,
 created_by = NVL(created_by, l_last_updated_by),   
 creation_date = NVL(creation_date, l_last_update_date),  
 last_updated_by = l_last_updated_by,
 last_update_date = l_last_update_date,
 last_update_login = l_last_update_login
 WHERE org_code = g_org_code
 AND int_process_code IN ('New', 'NB')
;

  g_wo_txn_cnt := sql%rowcount;
  fnd_file.put_line(fnd_file.LOG, ' g_wo_txn_cnt :' || g_wo_txn_cnt );

 UPDATE ftl_mat_txn
	SET request_id = xx_emf_pkg.g_request_id,
 int_error_code = xx_emf_cn_pkg.cn_null,
 int_process_code = xx_emf_cn_pkg.cn_in_prog,
 created_by = NVL(created_by, l_last_updated_by),    
 creation_date = NVL(creation_date, l_last_update_date), 
 last_updated_by = l_last_updated_by,
 last_update_date = l_last_update_date,
 last_update_login = l_last_update_login
 WHERE org_code = g_org_code
 AND int_process_code IN ('New', 'NB')
 ;

 g_mat_txn_cnt := sql%rowcount;
 fnd_file.put_line(fnd_file.LOG, ' g_mat_txn_cnt :' || g_mat_txn_cnt );

 COMMIT;

 EXCEPTION WHEN OTHERS THEN
	fnd_file.put_line (fnd_file.LOG, l_module_name || ' Message:' || SQLERRM);
 END mark_record_for_processing_prc;

----------------------------------------------------------------------
/*
Function Name: VALIDATE_ORG
Authors name: Manisha Mohanty
Date written: 25-Oct-2013
RICEW Object id: WIP_I-196
Description: Set Conversion Environment
Program Style: Subordinate
Change History:
Date Issue# Name Remarks
----------- ------- ------------------ ------------------------------
25-Oct-2013 1.0 Manisha Mohanty Initial development.
*/
----------------------------------------------------------------------
 FUNCTION validate_org (p_rec_id NUMBER, p_org VARCHAR2)
 RETURN NUMBER
 IS
 l_error_code NUMBER := xx_emf_cn_pkg.cn_success;
 l_org_id wip_parameters.organization_id%TYPE;
 BEGIN
 l_error_code := xx_emf_cn_pkg.cn_success;

 SELECT wp.organization_id
	INTO l_org_id
 FROM wip_parameters wp, mtl_parameters mp
	WHERE mp.organization_code = p_org
	AND wp.organization_id = mp.organization_id;

 RETURN (l_error_code);

 EXCEPTION WHEN NO_DATA_FOUND
 THEN
	l_error_code := xx_emf_cn_pkg.cn_rec_err;
	xx_emf_pkg.error (p_severity => xx_emf_cn_pkg.cn_low,
						p_category => xx_emf_cn_pkg.cn_valid,
						p_error_text => ' Organization is invalid ' || p_org,
						p_record_identifier_1 => p_rec_id
					);
	RETURN l_error_code;
 WHEN OTHERS THEN
	l_error_code := xx_emf_cn_pkg.cn_rec_err;
	xx_emf_pkg.error (p_severity => xx_emf_cn_pkg.cn_low,
						p_category => xx_emf_cn_pkg.cn_valid,
						p_error_text => ' Error in function validate_org ->' || SQLERRM,
						p_record_identifier_1 => p_rec_id
					);
 RETURN l_error_code;

 END validate_org;

----------------------------------------------------------------------
/*
Function Name: validate_wip_job
Authors name: Manisha Mohanty
Date written: 25-Oct-2013
RICEW Object id: WIP_I-196
Description: Set Conversion Environment
Program Style: Subordinate
Change History:
Date Issue# Name Remarks
----------- ------- ------------------ ------------------------------
25-Oct-2013 1.0 Manisha Mohanty Initial development.
*/
----------------------------------------------------------------------
 FUNCTION validate_wip_job (
 p_rec_id NUMBER,
 p_job_name VARCHAR2,
 p_org VARCHAR2
 )
 RETURN NUMBER
 IS
 l_error_code NUMBER := xx_emf_cn_pkg.cn_success;
 l_job NUMBER := 0;
 BEGIN
 l_error_code := xx_emf_cn_pkg.cn_success;
 l_job := 0;

 SELECT COUNT (we.wip_entity_id)
	INTO l_job
 FROM wip_entities we, mtl_parameters mp
	WHERE we.wip_entity_name = p_job_name
	AND we.organization_id = mp.organization_id
	AND mp.organization_code = p_org
	AND we.entity_type = 1; --standard job only

 IF l_job = 0 THEN
	RETURN xx_emf_cn_pkg.cn_success;
 ELSE
	xx_emf_pkg.error (p_severity => xx_emf_cn_pkg.cn_low,
						p_category => xx_emf_cn_pkg.cn_valid,
						p_error_text => ' WIP job already exists ->' || p_job_name,
						p_record_identifier_1 => p_rec_id
					);
	RETURN xx_emf_cn_pkg.cn_rec_err;
 END IF;
 EXCEPTION WHEN OTHERS
 THEN
	l_error_code := xx_emf_cn_pkg.cn_rec_err;
	xx_emf_pkg.error(p_severity => xx_emf_cn_pkg.cn_low,
						p_category => xx_emf_cn_pkg.cn_valid,
						p_error_text => ' Error in function validate_wip_job ->' || SQLERRM,
						p_record_identifier_1 => p_rec_id
					);
	RETURN l_error_code;
 END validate_wip_job;

----------------------------------------------------------------------
/*
Function Name: validate_parent_wip_job
Authors name: Manisha Mohanty
Date written: 25-Oct-2013
RICEW Object id: WIP_I-196
Description: Set Conversion Environment
Program Style: Subordinate
Change History:
Date Issue# Name Remarks
----------- ------- ------------------ ------------------------------
25-Oct-2013 1.0 Manisha Mohanty Initial development.
*/
----------------------------------------------------------------------
 FUNCTION validate_parent_wip_job (
 p_rec_id NUMBER,
 p_parent_job_name VARCHAR2,
 p_org VARCHAR2
 )
 RETURN NUMBER
 IS
 l_error_code NUMBER := xx_emf_cn_pkg.cn_success;
 l_job NUMBER := 0;
 BEGIN
 l_error_code := xx_emf_cn_pkg.cn_success;
 l_job := 0;

 SELECT COUNT (we.wip_entity_id)
	INTO l_job
 FROM wip_entities we, mtl_parameters mp
	WHERE we.wip_entity_name = p_parent_job_name
	AND we.organization_id = mp.organization_id
	AND mp.organization_code = p_org
	AND we.entity_type = 1; --standard job only

	RETURN (l_error_code);
 EXCEPTION WHEN NO_DATA_FOUND
 THEN
	l_error_code := xx_emf_cn_pkg.cn_rec_err;
	xx_emf_pkg.error(p_severity => xx_emf_cn_pkg.cn_low,
						p_category => xx_emf_cn_pkg.cn_valid,
						p_error_text => ' Parent WIP job does not exist ' || p_parent_job_name,
						p_record_identifier_1 => p_rec_id
					);
	RETURN l_error_code;
 WHEN OTHERS THEN
	l_error_code := xx_emf_cn_pkg.cn_rec_err;
	xx_emf_pkg.error(p_severity => xx_emf_cn_pkg.cn_low,
						p_category => xx_emf_cn_pkg.cn_valid,
						p_error_text => ' Error in function validate_parent_wip_job ->' || SQLERRM,
						p_record_identifier_1 => p_rec_id
					);
	RETURN l_error_code;
 END validate_parent_wip_job;

----------------------------------------------------------------------
/*
Function Name: validate_acct_class
Authors name: Manisha Mohanty
Date written: 25-Oct-2013
RICEW Object id: WIP_I-196
Description: Set Conversion Environment
Program Style: Subordinate
Change History:
Date Issue# Name Remarks
----------- ------- ------------------ ------------------------------
25-Oct-2013 1.0 Manisha Mohanty Initial development.
*/
----------------------------------------------------------------------
 FUNCTION validate_acct_class (
 p_rec_id NUMBER,
 p_class_code VARCHAR2,
 p_org VARCHAR2
 )
 RETURN NUMBER
 IS
 l_error_code NUMBER := xx_emf_cn_pkg.cn_success;
 l_class_code wip_accounting_classes.class_code%TYPE;
 BEGIN
 l_error_code := xx_emf_cn_pkg.cn_success;

 SELECT wac.class_code
	INTO l_class_code
 FROM wip_accounting_classes wac, mtl_parameters mp
	WHERE wac.class_code = p_class_code
	AND wac.organization_id = mp.organization_id
	AND mp.organization_code = p_org;

	RETURN (l_error_code);
 EXCEPTION WHEN NO_DATA_FOUND
	THEN
	l_error_code := xx_emf_cn_pkg.cn_rec_err;
	xx_emf_pkg.error (p_severity => xx_emf_cn_pkg.cn_low,
						p_category => xx_emf_cn_pkg.cn_valid,
						p_error_text => ' Class code is invalid ' || p_class_code,
						p_record_identifier_1 => p_rec_id
					);
	RETURN l_error_code;
 WHEN OTHERS THEN
	l_error_code := xx_emf_cn_pkg.cn_rec_err;
	xx_emf_pkg.error (p_severity => xx_emf_cn_pkg.cn_low,
						p_category => xx_emf_cn_pkg.cn_valid,
						p_error_text => ' Error in function validate_acct_class ->' || SQLERRM,
						p_record_identifier_1 => p_rec_id
					);
 RETURN l_error_code;
 END validate_acct_class;

----------------------------------------------------------------------
/*
Function Name: validate_status_type
Authors name: Manisha Mohanty
Date written: 25-Oct-2013
RICEW Object id: WIP_I-196
Description: Set Conversion Environment
Program Style: Subordinate
Change History:
Date Issue# Name Remarks
----------- ------- ------------------ ------------------------------
25-Oct-2013 1.0 Manisha Mohanty Initial development.
*/
----------------------------------------------------------------------
 FUNCTION validate_status_type (p_rec_id NUMBER, p_status_type VARCHAR2)
 RETURN NUMBER
 IS
 l_error_code NUMBER := xx_emf_cn_pkg.cn_success;
 l_status_type mfg_lookups.meaning%TYPE;
 BEGIN
 l_error_code := xx_emf_cn_pkg.cn_success;

 SELECT meaning
	INTO l_status_type
 FROM mfg_lookups
	WHERE lookup_type = 'WIP_JOB_STATUS'
	AND meaning = p_status_type
	AND enabled_flag = xx_emf_cn_pkg.cn_yes;

	RETURN (l_error_code);
 EXCEPTION WHEN NO_DATA_FOUND
 THEN
	l_error_code := xx_emf_cn_pkg.cn_rec_err;
	xx_emf_pkg.error (p_severity => xx_emf_cn_pkg.cn_low,
						p_category => xx_emf_cn_pkg.cn_valid,
						p_error_text => ' Status type is invalid ' || p_status_type,
						p_record_identifier_1 => p_rec_id
					);
	RETURN l_error_code;
 WHEN OTHERS THEN
	l_error_code := xx_emf_cn_pkg.cn_rec_err;
	xx_emf_pkg.error(p_severity => xx_emf_cn_pkg.cn_low,
						p_category => xx_emf_cn_pkg.cn_valid,
						p_error_text => ' Error in function validate_status_type ->' || SQLERRM,
						p_record_identifier_1 => p_rec_id
					);
	RETURN l_error_code;
 END validate_status_type;

 ----------------------------------------------------------------------
 /*
 Function Name: validate_wo_item
 Authors name: Manisha Mohanty
 Date written: 25-Oct-2013
 RICEW Object id: WIP_I-196
 Description: Set Conversion Environment
 Program Style: Subordinate
 Change History:
 Date Issue# Name Remarks
 ----------- ------- ------------------ ------------------------------
 25-Oct-2013 1.0 Manisha Mohanty Initial development.
 */
 ----------------------------------------------------------------------
 FUNCTION validate_wo_item ( 	p_rec_id NUMBER,
								p_wo_item VARCHAR2,
								p_org VARCHAR2
 )
RETURN NUMBER
IS
 l_error_code NUMBER := xx_emf_cn_pkg.cn_success;
 l_inventory_item_id mtl_system_items_b.inventory_item_id%TYPE;
 BEGIN
 l_error_code := xx_emf_cn_pkg.cn_success;

 SELECT msib.inventory_item_id
	INTO l_inventory_item_id
 FROM mtl_system_items_b msib, mtl_parameters mp
	WHERE msib.segment1 = p_wo_item
	AND mp.organization_code = p_org
	AND msib.organization_id = mp.organization_id;

 RETURN (l_error_code);
 EXCEPTION WHEN NO_DATA_FOUND
 THEN
	l_error_code := xx_emf_cn_pkg.cn_rec_err;
	xx_emf_pkg.error (p_severity => xx_emf_cn_pkg.cn_low,
						p_category => xx_emf_cn_pkg.cn_valid,
						p_error_text => ' Inventory item is invalid ' || p_wo_item,
						p_record_identifier_1 => p_rec_id
					);
 RETURN l_error_code;

 WHEN OTHERS
 THEN
	l_error_code := xx_emf_cn_pkg.cn_rec_err;
	xx_emf_pkg.error(p_severity => xx_emf_cn_pkg.cn_low,
						p_category => xx_emf_cn_pkg.cn_valid,
						p_error_text => ' Error in function validate_wo_item ->' || SQLERRM,
						p_record_identifier_1 => p_rec_id
					);
 RETURN l_error_code;

 END validate_wo_item;

----------------------------------------------------------------------
 /*
 Procedure Name: update_record_count_prc
 Authors name: Manisha Mohanty
 Date written: 25-Oct-2013
 RICEW Object id: WIP_I-196
 Description: Set Conversion Environment
 Program Style: Subordinate
 Change History:
 Date Issue# Name Remarks
 ----------- ------- ------------------ ------------------------------
 25-Oct-2013 1.0 Manisha Mohanty Initial development.
 */
 ----------------------------------------------------------------------
 PROCEDURE update_record_count_prc
 IS
 l_module_name VARCHAR2 (60) := 'update_record_count_prc';
 l_total_cnt_wo_hdr NUMBER;
 l_error_cnt_wo_hdr NUMBER;
 l_warn_cnt_wo_hdr NUMBER;
 l_success_cnt_wo_hdr NUMBER;
 l_total_cnt_wo_bom NUMBER;
 l_error_cnt_wo_bom NUMBER;
 l_warn_cnt_wo_bom NUMBER;
 l_success_cnt_wo_bom NUMBER;
 l_total_cnt_wo_txn NUMBER;
 l_error_cnt_wo_txn NUMBER;
 l_warn_cnt_wo_txn NUMBER;
 l_success_cnt_wo_txn NUMBER;
 l_total_cnt_mat_txn NUMBER;
 l_error_cnt_mat_txn NUMBER;
 l_warn_cnt_mat_txn NUMBER;
 l_success_cnt_mat_txn NUMBER;
 BEGIN
 -----------------------(FTL_WO_HDR record count)--------------------------------
 SELECT COUNT (1)
	INTO l_total_cnt_wo_hdr
 FROM ftl_wo_hdr
 WHERE org_code = g_org_code
 ;

 SELECT COUNT (1)
	INTO l_error_cnt_wo_hdr
 FROM ftl_wo_hdr
 WHERE org_code = g_org_code
 AND int_error_code = xx_emf_cn_pkg.cn_rec_err;

 SELECT COUNT (1)
	INTO l_warn_cnt_wo_hdr
 FROM ftl_wo_hdr
 WHERE org_code = g_org_code
 AND int_error_code = xx_emf_cn_pkg.cn_rec_warn;

 SELECT COUNT (1)
	INTO l_success_cnt_wo_hdr
 FROM ftl_wo_hdr
 WHERE org_code = g_org_code
 AND int_error_code = xx_emf_cn_pkg.cn_success;

 -----------------------(FTL_WO_BOM record count)--------------------------------
 SELECT COUNT (1)
	INTO l_total_cnt_wo_bom
 FROM ftl_wo_bom
 WHERE org_code = g_org_code;

 SELECT COUNT (1)
	INTO l_error_cnt_wo_bom
 FROM ftl_wo_bom
 WHERE org_code = g_org_code
 AND int_error_code = xx_emf_cn_pkg.cn_rec_err;

 SELECT COUNT (1)
	INTO l_warn_cnt_wo_bom
 FROM ftl_wo_bom
 WHERE org_code = g_org_code
 AND int_error_code = xx_emf_cn_pkg.cn_rec_warn;

 SELECT COUNT (1)
	INTO l_success_cnt_wo_bom
 FROM ftl_wo_bom
 WHERE org_code = g_org_code
 AND int_error_code = xx_emf_cn_pkg.cn_success;

 -----------------------(FTL_WO_TXN record count)--------------------------------
 SELECT COUNT (1)
	INTO l_total_cnt_wo_txn
 FROM ftl_wo_txn
 WHERE org_code = g_org_code;

 SELECT COUNT (1)
	INTO l_error_cnt_wo_txn
 FROM ftl_wo_txn
 WHERE org_code = g_org_code
 AND int_error_code = xx_emf_cn_pkg.cn_rec_err;

 SELECT COUNT (1)
	INTO l_warn_cnt_wo_txn
 FROM ftl_wo_txn
 WHERE org_code = g_org_code
 AND int_error_code = xx_emf_cn_pkg.cn_rec_warn;

 SELECT COUNT (1)
	INTO l_success_cnt_wo_txn
 FROM ftl_wo_txn
 WHERE org_code = g_org_code
 AND int_error_code = xx_emf_cn_pkg.cn_success;

 -----------------------(FTL_MAT_TXN record count)--------------------------------
 SELECT COUNT (1)
	INTO l_total_cnt_mat_txn
 FROM ftl_mat_txn
 WHERE org_code = g_org_code
 ;

 SELECT COUNT (1)
	INTO l_error_cnt_mat_txn
 FROM ftl_mat_txn
 WHERE org_code = g_org_code
 AND int_error_code = xx_emf_cn_pkg.cn_rec_err;

 SELECT COUNT (1)
	INTO l_warn_cnt_mat_txn
 FROM ftl_mat_txn
 WHERE org_code = g_org_code
 AND int_error_code = xx_emf_cn_pkg.cn_rec_warn;

 SELECT COUNT (1)
	INTO l_success_cnt_mat_txn
 FROM ftl_mat_txn
 WHERE org_code = g_org_code
 AND int_error_code = xx_emf_cn_pkg.cn_success;

 fnd_file.put_line (fnd_file.LOG, 'FTL_WO_HDR record count - ' || CHR (13) );
 fnd_file.put_line (fnd_file.LOG, 'Total records :=' || l_total_cnt_wo_hdr || CHR (13) );
 fnd_file.put_line (fnd_file.LOG, 'Error records :=' || l_error_cnt_wo_hdr || CHR (13) );
 fnd_file.put_line (fnd_file.LOG, 'Warning records :=' || l_warn_cnt_wo_hdr || CHR (13) );
 fnd_file.put_line (fnd_file.LOG, 'Success records :=' || l_success_cnt_wo_hdr || CHR (13) );
 fnd_file.put_line (fnd_file.LOG, CHR (13));

 fnd_file.put_line (fnd_file.LOG, 'FTL_WO_BOM record count - ' || CHR (13) );
 fnd_file.put_line (fnd_file.LOG, 'Total records :=' || l_total_cnt_wo_bom || CHR (13) );
 fnd_file.put_line (fnd_file.LOG, 'Error records :=' || l_error_cnt_wo_bom || CHR (13) );
 fnd_file.put_line (fnd_file.LOG, 'Warning records :=' || l_warn_cnt_wo_bom || CHR (13) );
 fnd_file.put_line (fnd_file.LOG, 'Success records :=' || l_success_cnt_wo_bom || CHR (13) );
 fnd_file.put_line (fnd_file.LOG, CHR (13));

 fnd_file.put_line (fnd_file.LOG, 'FTL_WO_TXN record count - ' || CHR (13) );
 fnd_file.put_line (fnd_file.LOG, 'Total records :=' || l_total_cnt_wo_txn || CHR (13) );
 fnd_file.put_line (fnd_file.LOG, 'Error records :=' || l_error_cnt_wo_txn || CHR (13) );
 fnd_file.put_line (fnd_file.LOG, 'Warning records :=' || l_warn_cnt_wo_txn || CHR (13) );
 fnd_file.put_line (fnd_file.LOG, 'Success records :=' || l_success_cnt_wo_txn || CHR (13) );
 fnd_file.put_line (fnd_file.LOG, CHR (13));

 fnd_file.put_line (fnd_file.LOG, 'FTL_MAT_TXN record count - ' || CHR (13) );
 fnd_file.put_line (fnd_file.LOG, 'Total records :=' || l_total_cnt_mat_txn || CHR (13) );
 fnd_file.put_line (fnd_file.LOG, 'Error records :=' || l_error_cnt_mat_txn || CHR (13) );
 fnd_file.put_line (fnd_file.LOG, 'Warning records :=' || l_warn_cnt_mat_txn || CHR (13) );
 fnd_file.put_line (fnd_file.LOG, 'Success records :=' || l_success_cnt_mat_txn || CHR (13) );

 EXCEPTION WHEN OTHERS
 THEN
	fnd_file.put_line (fnd_file.LOG, l_module_name || ' Message:' || SQLERRM );
	xx_emf_pkg.write_log (xx_emf_cn_pkg.cn_low, 'error in procedure update_record_count_prc' );
 END update_record_count_prc;

----------------------------------------------------------------------
 /*
 Procedure Name: mark_records_complete_prc
 Authors name: Manisha Mohanty
 Date written: 25-Oct-2013
 RICEW Object id: WIP_I-196
 Description: Set Conversion Environment
 Program Style: Subordinate
 Change History:
 Date Issue# Name Remarks
 ----------- ------- ------------------ ------------------------------
 25-Oct-2013 1.0 Manisha Mohanty Initial development.
 */
 ----------------------------------------------------------------------
 PROCEDURE mark_records_complete_prc (
 p_rec_id IN NUMBER,
 p_error_code IN NUMBER,
 p_process_code IN VARCHAR2,
 p_table IN VARCHAR2
 )
 IS
 l_last_update_date DATE := SYSDATE;
 l_last_updated_by NUMBER := fnd_global.user_id;
 l_last_update_login NUMBER := fnd_global.user_id;
 l_wo_hdr_tbl VARCHAR2 (60) := 'FTL_WO_HDR';
 l_wo_bom_tbl VARCHAR2 (60) := 'FTL_WO_BOM';
 l_wo_txn_tbl VARCHAR2 (60) := 'FTL_WO_TXN';
 l_mat_txn_tbl VARCHAR2 (60) := 'FTL_MAT_TXN';
 l_module_name VARCHAR2 (60) := 'mark_records_complete_prc';
 PRAGMA AUTONOMOUS_TRANSACTION;
 BEGIN

 IF p_table = l_wo_hdr_tbl THEN
	UPDATE ftl_wo_hdr
		SET int_process_code = p_process_code,
		int_error_code = NVL (p_error_code, xx_emf_cn_pkg.cn_success),
		last_updated_by = l_last_updated_by,
		last_update_date = l_last_update_date,
		last_update_login = l_last_update_login
	WHERE record_id = p_rec_id
	AND org_code = g_org_code;

 ELSIF p_table = l_wo_bom_tbl THEN
	UPDATE ftl_wo_bom
		SET int_process_code = p_process_code,
		int_error_code = NVL (p_error_code, xx_emf_cn_pkg.cn_success),
		last_updated_by = l_last_updated_by,
		last_update_date = l_last_update_date,
		last_update_login = l_last_update_login
	WHERE record_id = p_rec_id
	AND org_code = g_org_code;

 ELSIF p_table = l_mat_txn_tbl THEN
	UPDATE ftl_mat_txn
		SET int_process_code = p_process_code,
		int_error_code = NVL (p_error_code, xx_emf_cn_pkg.cn_success),
		last_updated_by = l_last_updated_by,
		last_update_date = l_last_update_date,
		last_update_login = l_last_update_login
	WHERE record_id = p_rec_id
	AND org_code = g_org_code;
 ELSIF p_table = l_wo_txn_tbl THEN
	UPDATE ftl_wo_txn
		SET int_process_code = p_process_code,
		int_error_code = NVL (p_error_code, xx_emf_cn_pkg.cn_success),
		last_updated_by = l_last_updated_by,
		last_update_date = l_last_update_date,
		last_update_login = l_last_update_login
	WHERE record_id = p_rec_id
	AND org_code = g_org_code;

 END IF;

 COMMIT;

 EXCEPTION WHEN OTHERS
 THEN
	fnd_file.put_line (fnd_file.LOG, l_module_name || ' Message:' || SQLERRM );
 END mark_records_complete_prc;

----------------------------------------------------------------------
/*
Function Name: call_wip_mass_load
Authors name: Manisha Mohanty
Date written: 25-Oct-2013
RICEW Object id: WIP_I-196
Description: Set Conversion Environment
Program Style: Subordinate
Change History:
Date Issue# Name Remarks
----------- ------- ------------------ ------------------------------
25-Oct-2013 1.0 Manisha Mohanty Initial development.
*/
----------------------------------------------------------------------
 FUNCTION call_wip_mass_load (p_group_id IN NUMBER)
 RETURN NUMBER
 IS
 l_error_code NUMBER := xx_emf_cn_pkg.cn_success;
 l_request_id NUMBER;
 l_phase VARCHAR2 (100);
 l_status VARCHAR2 (100);
 l_dev_phase VARCHAR2 (100);
 l_dev_status VARCHAR2 (100);
 l_message VARCHAR2 (240);
 l_completed BOOLEAN;
 l_normal VARCHAR2 (60) := 'NORMAL';
 l_complete VARCHAR2 (60) := 'COMPLETE';
 PRAGMA AUTONOMOUS_TRANSACTION;
 BEGIN

 l_error_code := xx_emf_cn_pkg.cn_success;
 -- Submitting Work Order Open Interface Import Program
 xx_emf_pkg.write_log (xx_emf_cn_pkg.cn_low, 'Inside call_wip_mass_load Group_id: ' || p_group_id );

 IF p_group_id IS NOT NULL THEN
	l_request_id := fnd_request.submit_request (application => 'WIP',
												program => 'WICMLP',
												argument1 => p_group_id,
												argument2 => NULL,
												argument3 => 1
												);
	COMMIT;
	--Wait for the completion of the concurrent request (if submitted successfully)
	l_completed := fnd_concurrent.wait_for_request	(request_id => l_request_id,
												INTERVAL => 60,
												max_wait => 3600,
												--increased the max wait interval to 1 hour
												phase => l_phase,
												status => l_status,
												dev_phase => l_dev_phase,
												dev_status => l_dev_status,
												MESSAGE => l_message
												);

 IF l_completed THEN
	IF (l_dev_phase != l_complete OR l_dev_status != l_normal)
	THEN
		xx_emf_pkg.write_log(xx_emf_cn_pkg.cn_low,'Work Order Open Interface Program completed in Error.'|| l_request_id);
		l_error_code := xx_emf_cn_pkg.cn_rec_err;
	ELSE
		xx_emf_pkg.write_log(xx_emf_cn_pkg.cn_low,'Work Order Import Program Completed - SUCCESS :'|| l_request_id);
	END IF;
 END IF;
 END IF; --ended if for groupid check
	fnd_file.put_line (fnd_file.LOG, to_char(SYSDATE, 'DD-MON-YYYY HH:MI:SS') || 'Request ID  : '||l_request_id || ' Org - ' || g_org_code );
	RETURN (l_error_code);
 EXCEPTION WHEN OTHERS
 THEN
	l_error_code := xx_emf_cn_pkg.cn_rec_err;
	xx_emf_pkg.error(p_severity => xx_emf_cn_pkg.cn_low,
						p_category => xx_emf_cn_pkg.cn_valid,
						p_error_text => ' Error in function call_wip_mass_load ->'|| SQLERRM
					);
	xx_emf_pkg.write_log (xx_emf_cn_pkg.cn_low,	'Error in call_wip_mass_load : ' || SQLERRM	);
	RETURN l_error_code;

 END call_wip_mass_load;

----------------------------------------------------------------------
/*
Procedure Name: mark_interface_errors_hdr
Authors name: Manisha Mohanty
Date written: 25-Oct-2013
RICEW Object id: WIP_I-196
Description: Set Conversion Environment
Program Style: Subordinate
Change History:
Date Issue# Name Remarks
----------- ------- ------------------ ------------------------------
25-Oct-2013 1.0 Manisha Mohanty Initial development.
*/
----------------------------------------------------------------------
 PROCEDURE mark_interface_errors_hdr (
										p_group_id IN NUMBER,
										p_mode IN VARCHAR2,
										p_stage IN VARCHAR2,
										p_org_code IN VARCHAR2
									)
 IS
 l_last_update_date DATE := SYSDATE;
 l_last_updated_by NUMBER := fnd_global.user_id;
 l_last_update_login NUMBER := fnd_global.user_id;
 l_record_count NUMBER;
 l_module_name VARCHAR2 (60) := 'mark_interface_errors_hdr';
 PRAGMA AUTONOMOUS_TRANSACTION;

 BEGIN
 xx_emf_pkg.write_log (xx_emf_cn_pkg.cn_low, 'Inside Mark Record for Interface Error');

 UPDATE ftl_wo_hdr fwh
	SET INT_PROCESS_CODE = 'ERROR', 
	int_error_code = xx_emf_cn_pkg.cn_rec_err,
	int_error_message =
	'INTERFACE Error : Errored out inside WIP_JOB_SCHEDULE_INTERFACE',
	last_updated_by = l_last_updated_by,
	last_update_date = l_last_update_date,
	last_update_login = l_last_update_login
 WHERE int_error_code IN
							(xx_emf_cn_pkg.cn_success, xx_emf_cn_pkg.cn_rec_warn)
							AND int_transaction_type = p_mode -- 'CREATE'
							AND org_code = p_org_code
							AND EXISTS (
											SELECT 1
											FROM wip_job_schedule_interface wjsi
											WHERE wjsi.job_name = fwh.work_order_number
											AND wjsi.primary_item_segments = fwh.assembly_item_code
											AND wjsi.organization_code = fwh.org_code
											AND wjsi.load_type IN (1, 3) /*create,update*/
											AND wjsi.GROUP_ID = p_group_id
											AND wjsi.process_status = 3 /*Error*/
										);

 l_record_count := SQL%ROWCOUNT;
 xx_emf_pkg.write_log(xx_emf_cn_pkg.cn_low, 'No of WO Header Record Marked with API Error=>' || l_record_count );

 COMMIT;

 EXCEPTION WHEN OTHERS
 THEN
	fnd_file.put_line (fnd_file.LOG,l_module_name || ' Message:' || SQLERRM	);
	xx_emf_pkg.write_log(xx_emf_cn_pkg.cn_low,	'error in procedure mark_interface_errors_hdr'|| SQLERRM	);
 END mark_interface_errors_hdr;

----------------------------------------------------------------------
/*
Procedure Name: mark_interface_errors_comp
Authors name: Manisha Mohanty
Date written: 25-Oct-2013
RICEW Object id: WIP_I-196
Description: Set Conversion Environment
Program Style: Subordinate
Change History:
Date Issue# Name Remarks
----------- ------- ------------------ ------------------------------
25-Oct-2013 1.0 Manisha Mohanty Initial development.
*/
----------------------------------------------------------------------
 PROCEDURE mark_interface_errors_comp (
										p_group_id IN NUMBER,
										p_mode IN VARCHAR2,
										p_stage IN VARCHAR2
										)
 IS
 l_last_update_date DATE := SYSDATE;
 l_last_updated_by NUMBER := fnd_global.user_id;
 l_last_update_login NUMBER := fnd_global.user_id;
 l_record_count NUMBER;
 l_module_name VARCHAR2 (60) := 'mark_interface_errors_comp';
 PRAGMA AUTONOMOUS_TRANSACTION;

 BEGIN
	xx_emf_pkg.write_log (xx_emf_cn_pkg.cn_low, 'Inside Mark Record for Interface Error' );

 UPDATE ftl_wo_bom fwh
	SET INT_PROCESS_CODE = 'ERROR', 
	int_error_code = xx_emf_cn_pkg.cn_rec_err,
	int_error_message =
	'INTERFACE Error : Errored out inside WIP_JOB_SCHEDULE_INTERFACE',
	last_updated_by = l_last_updated_by,
	last_update_date = l_last_update_date,
	last_update_login = l_last_update_login
 WHERE int_error_code IN
							(xx_emf_cn_pkg.cn_success, xx_emf_cn_pkg.cn_rec_warn)
							AND int_transaction_type = p_mode -- 'CREATE'
							AND request_id = xx_emf_pkg.g_request_id
							AND EXISTS (
										SELECT 1
										FROM wip_job_schedule_interface wjsi
										WHERE wjsi.job_name = fwh.work_order_number
										AND wjsi.primary_item_segments = fwh.assembly_item_code
										AND wjsi.organization_code = fwh.org_code
										AND wjsi.load_type IN (1, 3) /*create,update*/
										AND wjsi.GROUP_ID = p_group_id
										AND wjsi.process_status = 3 /*Error*/
										);

 l_record_count := SQL%ROWCOUNT;
 xx_emf_pkg.write_log (xx_emf_cn_pkg.cn_low, 'No of WO component Record Marked with API Error=>' || l_record_count );
 COMMIT;

 EXCEPTION WHEN OTHERS
 THEN
	fnd_file.put_line (fnd_file.LOG,l_module_name || ' Message:' || SQLERRM	);
	xx_emf_pkg.write_log(xx_emf_cn_pkg.cn_low,'error in procedure mark_interface_errors_comp'|| SQLERRM	);
 END mark_interface_errors_comp;

----------------------------------------------------------------------
/*
Procedure Name: create_wip_job
Authors name: Manisha Mohanty
Date written: 25-Oct-2013
RICEW Object id: WIP_I-196
Description: Set Conversion Environment
Program Style: Subordinate
Change History:
Date Issue# Name Remarks
----------- ------- ------------------ ------------------------------
25-Oct-2013 1.0 Manisha Mohanty Initial development.
*/
----------------------------------------------------------------------
 PROCEDURE create_wip_job
 IS
 l_module_name VARCHAR2 (60) := 'create_wip_job';
 l_error_code NUMBER := xx_emf_cn_pkg.cn_success;
 l_group_id NUMBER := 0;
 l_header_id NUMBER;
 l_org_id NUMBER;
 l_primary_item_id NUMBER;
 l_wip_entity_id NUMBER;
 l_scheduled_completion_date wip_discrete_jobs.scheduled_completion_date%TYPE;
 l_wo_hdr_tbl VARCHAR2 (60) := 'FTL_WO_HDR';
 l_wo_bom_rec NUMBER ;  
 l_release_date VARCHAR2(60); 
 l_org_gp_id NUMBER := 0 ; 
 l_org_id_rt NUMBER := 0 ;
 l_route_exists VARCHAR2(2);

 CURSOR cr_wo_header (cp_process_status VARCHAR2)
 IS
 SELECT *
	FROM ftl_wo_hdr
 WHERE request_id = xx_emf_pkg.g_request_id
	AND org_code = g_org_code
	AND int_process_code = cp_process_status
	AND NVL (int_error_code, xx_emf_cn_pkg.cn_success) IN
	(xx_emf_cn_pkg.cn_success, xx_emf_cn_pkg.cn_rec_warn)
	AND int_transaction_type = g_create
	and WORK_ORDER_STATUS <>'Closed' 
ORDER BY TIMESTAMP;


 TYPE p_tbl_wo_hdr IS TABLE OF cr_wo_header%ROWTYPE INDEX BY BINARY_INTEGER; 
 l_tbl_wo_hdr       p_tbl_wo_hdr;

 PRAGMA AUTONOMOUS_TRANSACTION;
 BEGIN
--------------------------------------(Data processing)-------------------------------------------

BEGIN

	SELECT  ORGANIZATION_ID 
		INTO l_org_gp_id
		FROM org_organization_definitions
		 WHERE ORGANIZATION_code = g_org_code;

EXCEPTION WHEN OTHERS THEN
	l_org_gp_id := 0 ;
END;

 SELECT TRUNC(SYSDATE) - TRUNC(SYSDATE , 'Year') || l_org_gp_id|| 1
	INTO l_group_id
 FROM dual; 
/*
 SELECT (NVL (MAX (GROUP_ID), 0) + 1) || l_org_gp_id|| 1
	INTO l_group_id
 FROM wip_job_schedule_interface;
*/
 fnd_file.put_line (fnd_file.LOG, to_char(SYSDATE, 'DD-MON-YYYY HH:MI:SS') || ' Group ID for Create WIP JOB  : '||l_group_id || ' Org - ' || g_org_code );


 OPEN cr_wo_header (g_preprocess) ;
   FETCH cr_wo_header BULK COLLECT INTO l_tbl_wo_hdr LIMIT g_bulk_col_lim;

IF (l_tbl_wo_hdr.COUNT >0 ) THEN

	fnd_file.put_line (fnd_file.LOG, to_char(SYSDATE, 'DD-MON-YYYY HH:MI:SS') || ' Create WIP JOB count  : '|| l_tbl_wo_hdr.count );
FOR i IN 1.. l_tbl_wo_hdr.count    
 LOOP

 BEGIN
 l_error_code := xx_emf_cn_pkg.cn_success;

 BEGIN
  SELECT MIN(TIMESTAMP) 
		INTO l_release_date 
	FROM FTL_WO_HDR 
 WHERE WORK_ORDER_NUMBER= l_tbl_wo_hdr(i).work_order_number
	AND WORK_ORDER_STATUS = 'Released'
	AND org_code = g_org_code ;

 EXCEPTION WHEN OTHERS THEN
  fnd_file.put_line (fnd_file.LOG, 'Exception in Get WO Release date : '||l_tbl_wo_hdr(i).work_order_number );
  l_release_date := null;
 END;


 BEGIN
  SELECT ORGANIZATION_ID
		INTO l_org_id_rt 
	FROM org_organization_definitions 
  WHERE organization_code = l_tbl_wo_hdr(i).ORG_CODE
	 ;

 EXCEPTION WHEN OTHERS THEN
  fnd_file.put_line (fnd_file.LOG, 'Exception in Getting Org ID : '||l_tbl_wo_hdr(i).work_order_number );
  l_org_id_rt := 0;
 END;
 --Code added as part of Change IN00567095
  BEGIN
  l_route_exists := 'N' ;
  SELECT 'Y'
		INTO l_route_exists 
	FROM bom_operational_routings 
  WHERE ASSEMBLY_ITEM_ID = (SELECT INVENTORY_ITEM_ID 
									FROM MTL_SYSTEM_ITEMS_B 
									WHERE SEGMENT1= l_tbl_wo_hdr(i).ASSEMBLY_ITEM_CODE
									AND ORGANIZATION_ID = 103
							)
	AND ORGANIZATION_ID = l_org_id_rt
	 ;

 EXCEPTION WHEN OTHERS THEN
  fnd_file.put_line (fnd_file.LOG, 'Exception in Getting route details : '||l_tbl_wo_hdr(i).work_order_number );
  l_route_exists := 'N';
 END;

 IF l_route_exists = 'Y' THEN
 --End of Change IN00567095
 BEGIN
 INSERT INTO wip_job_schedule_interface
 ( last_update_date,
 last_updated_by,
 last_updated_by_name,
 creation_date, 
 created_by_name,
 created_by, 
 last_update_login ,
 GROUP_ID,
 process_phase, 
 process_status,
 organization_code,
 load_type,
 status_type, 
 first_unit_start_date ,
 last_unit_completion_date,
 scheduling_method ,
 completion_subinventory ,
 class_code,
 job_name,
 firm_planned_flag,
 start_quantity,
 attribute2 ,
 attribute4 ,
 attribute5 , 
 attribute6 , 
 attribute7 ,
 primary_item_segments ,
 parent_job_name ,
 date_released
 )
 VALUES ( 
 l_tbl_wo_hdr(i).last_update_date,
 l_tbl_wo_hdr(i).last_updated_by,
 l_tbl_wo_hdr(i).last_updated_by_name,
 SYSDATE, 
 l_tbl_wo_hdr(i).created_by_name,
 l_tbl_wo_hdr(i).created_by, 
 l_tbl_wo_hdr(i).last_update_login ,
 l_group_id, 
 2, -- process_phase 2 Validation 3 Explosion 4 Complete 5 Creation
 1, -- process_status 1 Pending 2 Running 3 Error 4 Complete 5 Warning
 l_tbl_wo_hdr(i).org_code, --organization_code
 1, --loadtype
 /*
 1 Create Standard Discrete Job
 2 Create Pending Repetitive Schedule
 3 Update Standard or Non-Standard Discrete Job
 4 Create Non-Standard Discrete Job
 */
 (SELECT lookup_code
 FROM mfg_lookups
 WHERE lookup_type = 'WIP_JOB_STATUS'
 AND meaning = l_tbl_wo_hdr(i).work_order_status
 AND enabled_flag = xx_emf_cn_pkg.cn_yes) , 
 l_tbl_wo_hdr(i).target_start_date ,
 l_tbl_wo_hdr(i).target_completion_date, 
 CASE
     WHEN (l_tbl_wo_hdr(i).target_start_date IS NOT NULL and l_tbl_wo_hdr(i).target_completion_date IS NOT NULL) THEN 3
     ELSE NULL
   END ,
 l_tbl_wo_hdr(i).compl_subinventory ,
 l_tbl_wo_hdr(i).wip_accounting_class,
 l_tbl_wo_hdr(i).work_order_number ,
 l_tbl_wo_hdr(i).firm_planned_flag,
 l_tbl_wo_hdr(i).target_completion_quantity, 
 'Y' , 
 l_tbl_wo_hdr(i).target_completion_date , 
 l_tbl_wo_hdr(i).target_completion_quantity,
 l_tbl_wo_hdr(i).attribute6, 
 l_tbl_wo_hdr(i).attribute7,
 l_tbl_wo_hdr(i).assembly_item_code,
 l_tbl_wo_hdr(i).parent_wo_number,
 l_release_date
 );
 EXCEPTION WHEN OTHERS
 THEN
	l_error_code := xx_emf_cn_pkg.cn_rec_err;
	fnd_file.put_line(fnd_file.LOG,	'Error while inserting WO header CREATE records in interface : '|| SQLERRM);
 END;

 mark_records_complete_prc (l_tbl_wo_hdr(i).record_id,
							l_error_code,
							g_interface,
							l_wo_hdr_tbl
							);
 xx_emf_pkg.propagate_error (l_error_code);

 --Code added as part of Change IN00567095
 ELSE
	UPDATE FTL_WO_HDR
		SET INT_TRANSACTION_TYPE = NULL, 
			INT_PROCESS_CODE = 'ERROR',  
			INT_ERROR_CODE = '2', 
			INT_ERROR_MESSAGE = 'Route Does Not Exist for this Org and Assembly Item', 
			REQUEST_ID = NULL
	WHERE RECORD_ID = l_tbl_wo_hdr(i).record_id
	AND work_order_number = l_tbl_wo_hdr(i).work_order_number
	;
	COMMIT;
 END IF;
 --End of Change IN00567095
 EXCEPTION
 -- If HIGH error then it will be propagated to the next level
 -- IF the process has to continue maintain it as a medium severity
 WHEN xx_emf_pkg.g_e_rec_error
 THEN
	fnd_file.put_line (fnd_file.LOG,' In Exception 1:' || SQLERRM);
	xx_emf_pkg.write_log (xx_emf_cn_pkg.cn_high,xx_emf_cn_pkg.cn_rec_err);
 WHEN xx_emf_pkg.g_e_prc_error
 THEN
	fnd_file.put_line (fnd_file.LOG,' In Exception 2:' || SQLERRM);
	xx_emf_pkg.write_log(xx_emf_cn_pkg.cn_high,	'Process Level Error in Data Validations');
	raise_application_error (-20199, xx_emf_cn_pkg.cn_prc_err);
 WHEN OTHERS
 THEN
	fnd_file.put_line (fnd_file.LOG, ' In Exception:' || SQLERRM);
	xx_emf_pkg.error(p_severity => xx_emf_cn_pkg.cn_medium,
						p_category => xx_emf_cn_pkg.cn_tech_error,
						p_error_text => xx_emf_cn_pkg.cn_exp_unhand,
						p_record_identifier_1 => l_tbl_wo_hdr(i).record_id,
						p_record_identifier_3 => 'Stage :Data Validation'
					);
 END;

 END LOOP;

 l_tbl_wo_hdr.delete;

 END IF;
 CLOSE cr_wo_header;

 COMMIT;
 ---------------------------(call wip mass load)--------------------------------------
 l_error_code := call_wip_mass_load (l_group_id);
 mark_interface_errors_hdr (l_group_id, g_create, g_interface, g_org_code);

 ---------------------------(Delete components)---------------------------------------
 l_group_id := NULL;
 l_org_gp_id := 0 ; 


BEGIN

	SELECT  ORGANIZATION_ID 
		INTO l_org_gp_id
		FROM org_organization_definitions
		 WHERE ORGANIZATION_code = g_org_code;

EXCEPTION WHEN OTHERS THEN
	l_org_gp_id := 0 ;
END;

SELECT TRUNC(SYSDATE) - TRUNC(SYSDATE , 'Year') || l_org_gp_id|| 2
	INTO l_group_id
 FROM dual; 
/* 
 SELECT (NVL (MAX (GROUP_ID), 0) + 1) || l_org_gp_id || 2
	INTO l_group_id
 FROM wip_job_schedule_interface;
 */

 fnd_file.put_line (fnd_file.LOG, 'Group ID for DELETE Component JOB  : '||l_group_id || ' Org - ' || g_org_code );


OPEN cr_wo_header (g_interface) ;
   FETCH cr_wo_header BULK COLLECT INTO l_tbl_wo_hdr LIMIT g_bulk_col_lim;

IF (l_tbl_wo_hdr.COUNT >0 ) THEN
	fnd_file.put_line (fnd_file.LOG, to_char(SYSDATE, 'DD-MON-YYYY HH:MI:SS') || ' Create WIP JOB Del comp count  : '|| l_tbl_wo_hdr.count );

FOR i IN 1.. l_tbl_wo_hdr.count    
 LOOP

 BEGIN
 l_error_code := xx_emf_cn_pkg.cn_success;
 l_header_id := NULL;
 l_org_id := NULL;
 l_primary_item_id := NULL;
 l_wip_entity_id := NULL;
 l_scheduled_completion_date := NULL;

 SELECT (NVL (MAX (header_id), 0) + 1)
	INTO l_header_id
 FROM wip_job_schedule_interface;

 BEGIN

 SELECT wdj.organization_id, 
		wdj.primary_item_id,
		wdj.wip_entity_id,
		wdj.scheduled_completion_date
	INTO 	l_org_id, 
			l_primary_item_id,
			l_wip_entity_id,
			l_scheduled_completion_date --added 17-JUN-2014
 FROM 	wip_entities we,
		wip_discrete_jobs wdj,
		mtl_parameters mp
 WHERE we.wip_entity_name = l_tbl_wo_hdr(i).work_order_number
	AND we.organization_id = mp.organization_id
	AND mp.organization_code = l_tbl_wo_hdr(i).org_code
	AND we.wip_entity_id = wdj.wip_entity_id
	AND we.organization_id = wdj.organization_id;


BEGIN

 SELECT COUNT(1)
	INTO l_wo_bom_rec
 FROM 	wip_entities we,
		wip_discrete_jobs wdj,
		wip_requirement_operations wro,
		mtl_parameters mp
 WHERE we.wip_entity_name = l_tbl_wo_hdr(i).work_order_number
	AND we.organization_id = mp.organization_id
	AND mp.organization_code = l_tbl_wo_hdr(i).org_code
	AND we.wip_entity_id = wdj.wip_entity_id
	AND we.organization_id = wdj.organization_id
	AND we.wip_entity_id = wro.wip_entity_id
	AND we.organization_id = wro.organization_id;

EXCEPTION WHEN NO_DATA_FOUND THEN
  fnd_file.put_line (fnd_file.LOG,  'Exception NO DATA FOUND in BOM comp deletion count check for create Job'  );
  l_wo_bom_rec := NULL;
 WHEN OTHERS THEN
	fnd_file.put_line (fnd_file.LOG,  'Exception in BOM comp deletion count check for create Job '  );
	l_wo_bom_rec := NULL;
END;
 IF l_wo_bom_rec IS NOT NULL THEN  

 INSERT INTO wip_job_schedule_interface
 ( 
 last_update_date,
 last_updated_by, 
 creation_date,
 created_by, 
 last_update_login,
 GROUP_ID, 
 process_phase, 
 process_status,
 organization_id, 
 load_type, 
 primary_item_id,
 job_name, 
 wip_entity_id,
 attribute6, 
 attribute7, 
 header_id,
 last_unit_completion_date 
 )
 VALUES ( 
 l_tbl_wo_hdr(i).last_update_date,
 l_tbl_wo_hdr(i).last_updated_by, 
 SYSDATE,
 l_tbl_wo_hdr(i).created_by, 
 l_tbl_wo_hdr(i).last_update_login,
 l_group_id, 
 2 -- process_phase 2 Validation 3 Explosion 4 Complete 5 Creation
 , 1 -- process_status 1 Pending 2 Running 3 Error 4 Complete 5 Warning
 , l_org_id, 
 3 --load type
 /*
 1 Create Standard Discrete Job
 2 Create Pending Repetitive Schedule
 3 Update Standard or Non-Standard Discrete Job
 4 Create Non-Standard Discrete Job
 */
 , l_primary_item_id,
 l_tbl_wo_hdr(i).work_order_number, 
 l_wip_entity_id,
 l_tbl_wo_hdr(i).attribute6, 
 l_tbl_wo_hdr(i).attribute7, 
 l_header_id,
 l_scheduled_completion_date 
 );

 FOR rec_comp IN (SELECT wro.*
					FROM 	wip_entities we,
							wip_discrete_jobs wdj,
							wip_requirement_operations wro,
							mtl_parameters mp
					WHERE we.wip_entity_name = l_tbl_wo_hdr(i).work_order_number
					AND we.organization_id = mp.organization_id
					AND mp.organization_code = l_tbl_wo_hdr(i).org_code
					AND we.wip_entity_id = wdj.wip_entity_id
					AND we.organization_id =wdj.organization_id
					AND we.wip_entity_id = wro.wip_entity_id
					AND we.organization_id = wro.organization_id
					)
 LOOP

 INSERT INTO wip_job_dtls_interface
									(organization_id,
									operation_seq_num,
									wip_entity_id,
									inventory_item_id_old,
									wip_supply_type,
									date_required, GROUP_ID,
									parent_header_id, load_type,
									substitution_type,
									process_phase, process_status,
									last_update_date,
									last_updated_by,
									creation_date, created_by,
									last_update_login
									)
									VALUES (rec_comp.organization_id,
									rec_comp.operation_seq_num,
									rec_comp.wip_entity_id,
									rec_comp.inventory_item_id
									,
									rec_comp.wip_supply_type,
									rec_comp.date_required, l_group_id,
									l_header_id, 
									2, --load_type 1. resource 2. component 3. operation 4. multiple resource usage
									1, --substitution_type 1.Delete, 2.Add 3.Change
									2, --process_phase
									1, --process_status
									rec_comp.last_update_date,
									rec_comp.last_updated_by,
									rec_comp.creation_date, 
									rec_comp.created_by,
									rec_comp.last_update_login
									);

 END LOOP;

 mark_records_complete_prc (l_tbl_wo_hdr(i).record_id,
							l_error_code,
							g_delete,
							l_wo_hdr_tbl
							);
 xx_emf_pkg.propagate_error (l_error_code);
 END IF; 


 EXCEPTION WHEN OTHERS
 THEN
	l_error_code := xx_emf_cn_pkg.cn_rec_err;
	fnd_file.put_line(fnd_file.LOG,	'Error while inserting WO header DELETE records in interface : '|| SQLERRM);
 END;

 EXCEPTION
 -- If HIGH error then it will be propagated to the next level
 -- IF the process has to continue maintain it as a medium severity
 WHEN xx_emf_pkg.g_e_rec_error
 THEN
	fnd_file.put_line (fnd_file.LOG, ' In Exception 1:' || SQLERRM);
	xx_emf_pkg.write_log (xx_emf_cn_pkg.cn_high, xx_emf_cn_pkg.cn_rec_err );
 WHEN xx_emf_pkg.g_e_prc_error THEN
	fnd_file.put_line (fnd_file.LOG, ' In Exception 2:' || SQLERRM);
	xx_emf_pkg.write_log (xx_emf_cn_pkg.cn_high, 'Process Level Error in Data Validations' );
	raise_application_error (-20199, xx_emf_cn_pkg.cn_prc_err);
 WHEN OTHERS
 THEN
	fnd_file.put_line (fnd_file.LOG, ' In Exception:' || SQLERRM);
	xx_emf_pkg.error (p_severity => xx_emf_cn_pkg.cn_medium,
						p_category => xx_emf_cn_pkg.cn_tech_error,
						p_error_text => xx_emf_cn_pkg.cn_exp_unhand,
						p_record_identifier_1 => l_tbl_wo_hdr(i).record_id,
						p_record_identifier_3 => 'Stage :Data Validation'
					);
 END;
 END LOOP;

 l_tbl_wo_hdr.delete;
 END IF;
 CLOSE cr_wo_header;

 COMMIT;
 ---------------------------(call wip mass load)--------------------------------------
 l_error_code := call_wip_mass_load (l_group_id);
 mark_interface_errors_hdr (l_group_id, g_create, g_delete, g_org_code);

 EXCEPTION WHEN OTHERS
 THEN
	fnd_file.put_line (fnd_file.LOG, l_module_name || ' Message:' || SQLERRM );
	xx_emf_pkg.write_log (xx_emf_cn_pkg.cn_low, 'error in procedure create_wip_job' );
 END create_wip_job;

----------------------------------------------------------------------
/*
Procedure Name: create_wip_component
Authors name: Manisha Mohanty
Date written: 25-Oct-2013
RICEW Object id: WIP_I-196
Description: Set Conversion Environment
Program Style: Subordinate
Change History:
Date Issue# Name Remarks
----------- ------- ------------------ ------------------------------
25-Oct-2013 1.0 Manisha Mohanty Initial development.
*/
----------------------------------------------------------------------
 PROCEDURE create_wip_component
 IS
 l_module_name VARCHAR2 (60) := 'create_wip_component';
 l_error_code NUMBER := xx_emf_cn_pkg.cn_success;
 l_group_id NUMBER := 0;
 l_last_wo_number VARCHAR2 (250);
 l_org_id NUMBER;
 l_primary_item_id NUMBER;
 l_wip_entity_id NUMBER;
 l_inventory_item_id NUMBER;
 l_header_id NUMBER;
 l_scheduled_completion_date wip_discrete_jobs.scheduled_completion_date%TYPE;
 l_wo_bom_tbl VARCHAR2 (60) := 'FTL_WO_BOM';
 l_org_gp_id NUMBER := 0 ; 

 CURSOR cr_wo_header (cp_process_status VARCHAR2)
 IS
 SELECT *
	FROM ftl_wo_bom
 WHERE request_id = xx_emf_pkg.g_request_id
	AND org_code = g_org_code
	AND int_process_code = cp_process_status
	AND NVL (int_error_code, xx_emf_cn_pkg.cn_success) IN
	(xx_emf_cn_pkg.cn_success, xx_emf_cn_pkg.cn_rec_warn)
	AND int_transaction_type = g_create
 ORDER BY TIMESTAMP, WORK_ORDER_NUMBER;


 TYPE p_tbl_wo_bom_preprocess IS TABLE OF cr_wo_header%ROWTYPE INDEX BY BINARY_INTEGER; 
 l_tbl_wo_bom_preprocess       p_tbl_wo_bom_preprocess;

 PRAGMA AUTONOMOUS_TRANSACTION;
 BEGIN
 ---------------------------(Process components)---------------------------------------

BEGIN

	SELECT  ORGANIZATION_ID 
		INTO l_org_gp_id
	FROM org_organization_definitions
		 WHERE ORGANIZATION_code = g_org_code;

EXCEPTION WHEN OTHERS THEN
	l_org_gp_id := 0 ;
END;

 SELECT TRUNC(SYSDATE) - TRUNC(SYSDATE , 'Year') || l_org_gp_id|| 3
	INTO l_group_id
 FROM dual; 
/*
 SELECT (NVL (MAX (GROUP_ID), 0) + 1) || l_org_gp_id || 3
	INTO l_group_id
 FROM wip_job_schedule_interface;
*/
 fnd_file.put_line (fnd_file.LOG, 'Group ID for Create WO BOM : '||l_group_id || ' Org - ' || g_org_code );

 l_last_wo_number := 'DUMMY111111';

OPEN cr_wo_header (g_preprocess) ;
FETCH cr_wo_header BULK COLLECT INTO l_tbl_wo_bom_preprocess LIMIT g_bulk_col_lim;

IF (l_tbl_wo_bom_preprocess.COUNT >0 ) THEN
	fnd_file.put_line (fnd_file.LOG, to_char(SYSDATE, 'DD-MON-YYYY HH:MI:SS') || ' Create WO BOM count  : '|| l_tbl_wo_bom_preprocess.count );

FOR i IN 1.. l_tbl_wo_bom_preprocess.count    
 LOOP

 BEGIN
 l_error_code := xx_emf_cn_pkg.cn_success;
 l_org_id := NULL;
 l_primary_item_id := NULL;
 l_wip_entity_id := NULL;
 l_inventory_item_id := NULL;
 l_scheduled_completion_date := NULL;

 BEGIN 
	SELECT wdj.organization_id, 
			wdj.primary_item_id,
			wdj.wip_entity_id,
			wdj.scheduled_completion_date 
		INTO l_org_id, l_primary_item_id,
		l_wip_entity_id,
		l_scheduled_completion_date
	FROM wip_entities we, wip_discrete_jobs wdj, mtl_parameters mp
	WHERE we.wip_entity_name = l_tbl_wo_bom_preprocess(i).work_order_number
	AND we.organization_id = mp.organization_id
	AND mp.organization_code = l_tbl_wo_bom_preprocess(i).org_code
	AND we.wip_entity_id = wdj.wip_entity_id
	AND we.organization_id = wdj.organization_id;
EXCEPTION WHEN OTHERS THEN
	fnd_file.put_line (fnd_file.LOG, ' In Exception WIP Component creation check in wip entities' || SQLERRM);
	l_wip_entity_id := null;
END;


BEGIN 

 SELECT inventory_item_id
	INTO l_inventory_item_id
 FROM mtl_system_items_b
 WHERE segment1 = l_tbl_wo_bom_preprocess(i).component_item_code
 AND organization_id = l_org_id;

EXCEPTION WHEN OTHERS THEN
	fnd_file.put_line (fnd_file.LOG, ' In Exception WIP Component creation check for Item ID' || SQLERRM);
END;

IF l_wip_entity_id IS NOT NULL THEN 
 BEGIN

 IF l_last_wo_number <> l_tbl_wo_bom_preprocess(i).work_order_number THEN

 l_header_id := NULL;

 SELECT (NVL (MAX (header_id), 0) + 1)
	INTO l_header_id
 FROM wip_job_schedule_interface;

 INSERT INTO wip_job_schedule_interface
 ( 
 last_update_date,
 last_updated_by,
 creation_date, created_by,
 last_update_login, GROUP_ID, process_phase,
 process_status, organization_id, load_type,
 primary_item_id, job_name,
 wip_entity_id, 
 attribute6, attribute7,
 header_id,
 last_unit_completion_date 
 )
 VALUES ( 
 l_tbl_wo_bom_preprocess(i).last_update_date,
 l_tbl_wo_bom_preprocess(i).last_updated_by,
 SYSDATE, 
 l_tbl_wo_bom_preprocess(i).created_by,
 l_tbl_wo_bom_preprocess(i).last_update_login, 
 l_group_id, 
 2, -- process_phase 2 Validation 3 Explosion 4 Complete 5 Creation
 1, -- process_status 1 Pending 2 Running 3 Error 4 Complete 5 Warning
 l_org_id, 
 3 --load type --loadtype
 /*
 1 Create Standard Discrete Job
 2 Create Pending Repetitive Schedule
 3 Update Standard or Non-Standard Discrete Job
 4 Create Non-Standard Discrete Job
 */
 ,
 l_primary_item_id, l_tbl_wo_bom_preprocess(i).work_order_number,
 l_wip_entity_id,
 l_tbl_wo_bom_preprocess(i).attribute6, l_tbl_wo_bom_preprocess(i).attribute7, 
 l_header_id,
 l_scheduled_completion_date 
 );
 END IF;

 INSERT INTO wip_job_dtls_interface
 (organization_id, 
 operation_seq_num,
 wip_entity_id,
 inventory_item_id_new
 ,GROUP_ID, 
 parent_header_id, 
 load_type,
 substitution_type,
 process_phase, 
 process_status,
 last_update_date,
 last_updated_by, 
 creation_date,
 created_by, 
 last_update_login,
 quantity_per_assembly, 
 supply_subinventory
 )
 VALUES (l_org_id, 
 l_tbl_wo_bom_preprocess(i).operation_number,
 l_wip_entity_id,
 l_inventory_item_id 
 , l_group_id, 
 l_header_id, 
 2 --load_type 1. resource 2. component 3. operation 4. multiple resource usage
 , 2 --substitution_type 1.Delete, 2.Add 3.Change
 , 2 --process_phase
 , 1 --process_status
 , l_tbl_wo_bom_preprocess(i).last_update_date,
 l_tbl_wo_bom_preprocess(i).last_updated_by, 
 l_tbl_wo_bom_preprocess(i).creation_date,
 l_tbl_wo_bom_preprocess(i).created_by, 
 l_tbl_wo_bom_preprocess(i).last_update_login,
 l_tbl_wo_bom_preprocess(i).per_assembly_qty, 
 l_tbl_wo_bom_preprocess(i).supply_subinv
 );

 --refresh last WO#
  mark_records_complete_prc (l_tbl_wo_bom_preprocess(i).record_id,
 l_error_code,
 g_interface,
 l_wo_bom_tbl
 );

 l_last_wo_number := l_tbl_wo_bom_preprocess(i).work_order_number;
 EXCEPTION WHEN OTHERS
 THEN
	l_error_code := xx_emf_cn_pkg.cn_rec_err;
	xx_emf_pkg.write_log(xx_emf_cn_pkg.cn_low,'Error while inserting WO component CREATE records in interface : '|| SQLERRM);
 END;

 ELSE
	fnd_file.put_line (fnd_file.LOG, ' No WO Exists to create BOM: ' || l_tbl_wo_bom_preprocess(i).work_order_number );
	mark_records_complete_prc (l_tbl_wo_bom_preprocess(i).record_id,
							'2',
							'ERROR',
							l_wo_bom_tbl
							);
		-- Added as part of MFG 2.1					
	UPDATE FTL_WO_BOM SET INT_ERROR_MESSAGE = 'WO doesn’t exist in base table cannot apply bom component'
	 WHERE record_id = l_tbl_wo_bom_preprocess(i).record_id
	 AND work_order_number = l_tbl_wo_bom_preprocess(i).work_order_number
	 ;
		-- End of MFG 2.1 changes					
END IF; 



 xx_emf_pkg.propagate_error (l_error_code);

 EXCEPTION
 -- If HIGH error then it will be propagated to the next level
 -- IF the process has to continue maintain it as a medium severity
 WHEN xx_emf_pkg.g_e_rec_error THEN
	fnd_file.put_line (fnd_file.LOG, ' In Exception 1:' || SQLERRM);
	xx_emf_pkg.write_log (xx_emf_cn_pkg.cn_high, xx_emf_cn_pkg.cn_rec_err );

 WHEN xx_emf_pkg.g_e_prc_error THEN
	fnd_file.put_line (fnd_file.LOG,' In Exception 2:' || SQLERRM);
	xx_emf_pkg.write_log(xx_emf_cn_pkg.cn_high,	'Process Level Error in Data Validations');
	raise_application_error (-20199, xx_emf_cn_pkg.cn_prc_err);

 WHEN OTHERS THEN
	fnd_file.put_line (fnd_file.LOG, ' In Exception:' || SQLERRM);
	xx_emf_pkg.error(p_severity => xx_emf_cn_pkg.cn_medium,	
						p_category => xx_emf_cn_pkg.cn_tech_error,
						p_error_text => xx_emf_cn_pkg.cn_exp_unhand,
						p_record_identifier_1 => l_tbl_wo_bom_preprocess(i).record_id,
						p_record_identifier_3 => 'Stage :Data Validation'
					);
 END;

 END LOOP;

 l_tbl_wo_bom_preprocess.delete;

 END IF;
 CLOSE cr_wo_header;
 COMMIT;

 ---------------------------(call wip mass load)--------------------------------------
 l_error_code := call_wip_mass_load (l_group_id);
 mark_interface_errors_comp (l_group_id, g_create, g_interface);
 EXCEPTION WHEN OTHERS
 THEN
	fnd_file.put_line (fnd_file.LOG,l_module_name || ' Message:' || SQLERRM	);
	xx_emf_pkg.write_log (xx_emf_cn_pkg.cn_low,	'error in procedure create_wip_job'	);
 END create_wip_component;

----------------------------------------------------------------------
/*
Procedure Name: update_wip_job
Authors name: Manisha Mohanty
Date written: 25-Oct-2013
RICEW Object id: WIP_I-196
Description: Set Conversion Environment
Program Style: Subordinate
Change History:
Date Issue# Name Remarks
----------- ------- ------------------ ------------------------------
25-Oct-2013 1.0 Manisha Mohanty Initial development.
*/
----------------------------------------------------------------------
 PROCEDURE update_wip_job
 IS
 l_module_name VARCHAR2 (60) := 'update_wip_job';
 l_error_code NUMBER := xx_emf_cn_pkg.cn_success;
 l_group_id NUMBER := 0;
 l_job_name wip_entities.wip_entity_name%TYPE;
 l_organization_id wip_entities.organization_id%TYPE;
 l_status_type wip_discrete_jobs.status_type%TYPE;
 l_org_id NUMBER;
 l_primary_item_id NUMBER;
 l_wip_entity_id NUMBER;
 l_scheduled_completion_date wip_discrete_jobs.scheduled_completion_date%TYPE;
 l_scheduled_start_date wip_discrete_jobs.scheduled_start_date%TYPE;
 l_wo_hdr_tbl VARCHAR2 (60) := 'FTL_WO_HDR';
 l_released VARCHAR2 (60) := 'Released';
 l_cancelled VARCHAR2 (60) := 'Cancelled';
 l_closed VARCHAR2(60) := 'Closed';
 l_orig_start_date wip_discrete_jobs.attribute4%TYPE;
 l_target_qty wip_discrete_jobs.attribute5%TYPE;
 l_org_gp_id NUMBER := 0 ; 

 CURSOR cr_wo_header (cp_process_status VARCHAR2)
 IS
 SELECT *
	FROM ftl_wo_hdr
 WHERE request_id = xx_emf_pkg.g_request_id
	AND org_code = g_org_code
	AND int_process_code = cp_process_status
	AND NVL (int_error_code, xx_emf_cn_pkg.cn_success) IN
	(xx_emf_cn_pkg.cn_success, xx_emf_cn_pkg.cn_rec_warn)
	AND int_transaction_type = g_update
	AND NVL (work_order_status, l_released) NOT IN (l_cancelled,l_closed) 
 ORDER BY TIMESTAMP;


 TYPE p_tbl_wo_hdr IS TABLE OF cr_wo_header%ROWTYPE INDEX BY BINARY_INTEGER; 
 l_tbl_wo_hdr       p_tbl_wo_hdr;

 PRAGMA AUTONOMOUS_TRANSACTION;
 BEGIN
 ------------------------(update work order records)----------------------------------
 l_group_id := 0;

BEGIN

	SELECT  ORGANIZATION_ID 
		INTO l_org_gp_id
	FROM org_organization_definitions
		 WHERE ORGANIZATION_code = g_org_code;

EXCEPTION WHEN OTHERS THEN
	l_org_gp_id := 0 ;
END;

	 SELECT TRUNC(SYSDATE) - TRUNC(SYSDATE , 'Year') || l_org_gp_id || 4
		INTO l_group_id
	 FROM dual;

	/*SELECT (NVL (MAX (GROUP_ID), 0) + 1) || l_org_gp_id || 4
		INTO l_group_id
	FROM wip_job_schedule_interface;
	*/
	fnd_file.put_line (fnd_file.LOG, to_char(SYSDATE, 'DD-MON-YYYY HH:MI:SS') || ' Group ID for Update WIP JOB  : '||l_group_id || ' Org - ' || g_org_code );


OPEN cr_wo_header (g_preprocess) ;
  FETCH cr_wo_header BULK COLLECT INTO l_tbl_wo_hdr LIMIT g_bulk_col_lim;

IF (l_tbl_wo_hdr.COUNT > 0 ) THEN

	fnd_file.put_line (fnd_file.LOG, to_char(SYSDATE, 'DD-MON-YYYY HH:MI:SS') || ' Group ID for Update WIP JOB  : '||l_tbl_wo_hdr.count   );
	fnd_file.put_line (fnd_file.LOG, 'Update WIP Job Request ID: ' || xx_emf_pkg.g_request_id );

FOR i IN 1.. l_tbl_wo_hdr.count    
 LOOP

 BEGIN
 l_error_code := xx_emf_cn_pkg.cn_success;
 l_org_id := NULL;
 l_primary_item_id := NULL;
 l_wip_entity_id := NULL;
 l_scheduled_completion_date := NULL;

BEGIN
 SELECT wdj.organization_id, 
		wdj.primary_item_id,
		wdj.wip_entity_id,
		wdj.scheduled_completion_date, 
		wdj.scheduled_start_date, 
		wdj.attribute4,
		wdj.attribute5 
			INTO l_org_id, l_primary_item_id,
			l_wip_entity_id,
			l_scheduled_completion_date,
			l_scheduled_start_date,
			l_orig_start_date,
			l_target_qty
	FROM wip_entities we, wip_discrete_jobs wdj, mtl_parameters mp
 WHERE we.wip_entity_name = l_tbl_wo_hdr(i).work_order_number
		AND we.organization_id = mp.organization_id
		AND mp.organization_code = l_tbl_wo_hdr(i).org_code
		AND we.wip_entity_id = wdj.wip_entity_id
		AND we.organization_id = wdj.organization_id;
EXCEPTION WHEN OTHERS THEN
	fnd_file.put_line (fnd_file.LOG, ' WO not exists : ' || l_tbl_wo_hdr(i).work_order_number);

		UPDATE ftl_wo_hdr
			SET int_process_code = 'ERROR', 
			--INT_ERROR_MESSAGE = 'WO Not created to perform Update',
			INT_ERROR_CODE='2'
		WHERE request_id = xx_emf_pkg.g_request_id
		AND org_code = g_org_code
		AND int_transaction_type = g_update
		AND WORK_ORDER_NUMBER = l_tbl_wo_hdr(i).work_order_number
		AND RECORD_ID = l_tbl_wo_hdr(i).RECORD_ID
		;
		COMMIT;
END;

 BEGIN
 INSERT INTO wip_job_schedule_interface
 ( 	last_update_date,
	last_updated_by,
	last_updated_by_name,
	creation_date, 
	created_by_name,
	created_by, 
	last_update_login,
	GROUP_ID , 
	process_phase, 
	process_status,
	organization_id, load_type,
 status_type
 , first_unit_start_date
 , last_unit_completion_date
 ,
 primary_item_id
 , completion_subinventory
 , class_code
 , job_name
 , start_quantity,
 wip_entity_id
 , attribute4 
 , attribute5 
 , attribute6 
 , attribute7  

 )
 VALUES ( l_tbl_wo_hdr(i).last_update_date,
 l_tbl_wo_hdr(i).last_updated_by,
 l_tbl_wo_hdr(i).last_updated_by_name,
 SYSDATE, l_tbl_wo_hdr(i).created_by_name,
 l_tbl_wo_hdr(i).created_by, l_tbl_wo_hdr(i).last_update_login
 , l_group_id
 , 2 -- process_phase 2 Validation 3 Explosion 4 Complete 5 Creation
 , 1 -- process_status 1 Pending 2 Running 3 Error 4 Complete 5 Warning
 , l_org_id
 , 3 --loadtype
 /*
 1 Create Standard Discrete Job
 2 Create Pending Repetitive Schedule
 3 Update Standard or Non-Standard Discrete Job
 4 Create Non-Standard Discrete Job
 */
 ,
 (SELECT lookup_code
 FROM mfg_lookups
 WHERE lookup_type = 'WIP_JOB_STATUS'
 AND meaning = l_tbl_wo_hdr(i).work_order_status
 AND enabled_flag = xx_emf_cn_pkg.cn_yes)
 --status_type
 , NVL(l_tbl_wo_hdr(i).target_start_date,l_scheduled_start_date) 
 ,NVL(l_tbl_wo_hdr(i).target_completion_date,l_scheduled_completion_date) 
 , l_primary_item_id
 , l_tbl_wo_hdr(i).compl_subinventory
 , l_tbl_wo_hdr(i).wip_accounting_class
 , l_tbl_wo_hdr(i).work_order_number
 , l_tbl_wo_hdr(i).target_completion_quantity
 --start_quantity
 , l_wip_entity_id
 , DECODE(l_orig_start_date,NULL,NVL(l_tbl_wo_hdr(i).target_completion_date,l_scheduled_completion_date),l_orig_start_date) 
 , DECODE(l_target_qty,NULL,l_tbl_wo_hdr(i).target_completion_quantity,l_target_qty) 
 , l_tbl_wo_hdr(i).attribute6  
 , l_tbl_wo_hdr(i).attribute7  
 );
 EXCEPTION WHEN OTHERS
 THEN
	l_error_code := xx_emf_cn_pkg.cn_rec_err;
	xx_emf_pkg.write_log (xx_emf_cn_pkg.cn_low, 'Error while inserting WO header UPDATE records in interface : ' || SQLERRM );
 END;

	mark_records_complete_prc (l_tbl_wo_hdr(i).record_id,
								l_error_code,
								g_interface,
								l_wo_hdr_tbl
								);
	xx_emf_pkg.propagate_error (l_error_code);
 EXCEPTION
 -- If HIGH error then it will be propagated to the next level
 -- IF the process has to continue maintain it as a medium severity
 WHEN xx_emf_pkg.g_e_rec_error
 THEN
	fnd_file.put_line (fnd_file.LOG,' In Exception 1:' || SQLERRM);
	xx_emf_pkg.write_log (xx_emf_cn_pkg.cn_high,xx_emf_cn_pkg.cn_rec_err);
 WHEN xx_emf_pkg.g_e_prc_error
 THEN
	fnd_file.put_line (fnd_file.LOG,' In Exception 2:' || SQLERRM);
	xx_emf_pkg.write_log (xx_emf_cn_pkg.cn_high,'Process Level Error in Data Processing'	);
	raise_application_error (-20199, xx_emf_cn_pkg.cn_prc_err);
 WHEN OTHERS
 THEN
	fnd_file.put_line (fnd_file.LOG, ' In Exception:' || SQLERRM);
	xx_emf_pkg.error(p_severity => xx_emf_cn_pkg.cn_medium,
						p_category => xx_emf_cn_pkg.cn_tech_error,
						p_error_text => xx_emf_cn_pkg.cn_exp_unhand,
						p_record_identifier_1 => l_tbl_wo_hdr(i).record_id,
						p_record_identifier_3 => 'Stage :Data Processing'
					);
 END;
 END LOOP;
 l_tbl_wo_hdr.delete ;
 END IF;
 CLOSE cr_wo_header;
 COMMIT; 
 ---------------------------(call wip mass load)--------------------------------------
 l_error_code := call_wip_mass_load (l_group_id);
 mark_interface_errors_hdr (l_group_id, g_update, g_interface, g_org_code);
 EXCEPTION
 WHEN OTHERS
 THEN
 fnd_file.put_line (fnd_file.LOG, l_module_name || ' Message:' || SQLERRM );
 xx_emf_pkg.write_log (xx_emf_cn_pkg.cn_low,
 'error in procedure update_wip_job'
 );
 END update_wip_job;

----------------------------------------------------------------------
/*
Procedure Name: update_wip_comp
Authors name: Manisha Mohanty
Date written: 25-Oct-2013
RICEW Object id: WIP_I-196
Description: Set Conversion Environment
Program Style: Subordinate
Change History:
Date Issue# Name Remarks
----------- ------- ------------------ ------------------------------
25-Oct-2013 1.0 Manisha Mohanty Initial development.
*/
----------------------------------------------------------------------
 PROCEDURE update_wip_comp
 IS
 l_module_name VARCHAR2 (60) := 'update_wip_comp';
 l_error_code NUMBER := xx_emf_cn_pkg.cn_success;
 l_group_id NUMBER := 0;
 l_job_name wip_entities.wip_entity_name%TYPE;
 l_organization_id wip_entities.organization_id%TYPE;
 l_status_type wip_discrete_jobs.status_type%TYPE;
 l_org_id NUMBER;
 l_primary_item_id NUMBER;
 l_wip_entity_id NUMBER;
 l_inventory_item_id NUMBER;
 l_last_wo_number VARCHAR2 (250);
 l_header_id NUMBER;
 l_scheduled_completion_date wip_discrete_jobs.scheduled_completion_date%TYPE;
 l_wo_bom_tbl VARCHAR2 (60) := 'FTL_WO_BOM';
 l_org_gp_id NUMBER := 0 ; 

 CURSOR cr_wo_component (cp_process_status VARCHAR2)
 IS
 SELECT *
 FROM ftl_wo_bom
 WHERE request_id = xx_emf_pkg.g_request_id
 AND org_code = g_org_code
 AND int_process_code = cp_process_status
 AND NVL (int_error_code, xx_emf_cn_pkg.cn_success) IN
 (xx_emf_cn_pkg.cn_success, xx_emf_cn_pkg.cn_rec_warn)
 AND int_transaction_type = g_update
 ORDER BY TIMESTAMP;

 TYPE p_tbl_wo_component IS TABLE OF cr_wo_component%ROWTYPE INDEX BY BINARY_INTEGER; 
 l_tbl_wo_component       p_tbl_wo_component;

 PRAGMA AUTONOMOUS_TRANSACTION;
 BEGIN
 ------------------------(update work order component records)----------------------------------
 l_group_id := 0;

BEGIN

	SELECT  ORGANIZATION_ID 
		INTO l_org_gp_id
		FROM org_organization_definitions
		 WHERE ORGANIZATION_code = g_org_code;

EXCEPTION WHEN OTHERS THEN
	l_org_gp_id := 0 ;
	fnd_file.put_line (fnd_file.LOG, 'Exception in Org ID for group in WO BOM update : '||l_org_gp_id );
END;

	 SELECT TRUNC(SYSDATE) - TRUNC(SYSDATE , 'Year') || l_org_gp_id || 5
		INTO l_group_id
	 FROM dual;

 /*SELECT (NVL (MAX (GROUP_ID), 0) + 1) || l_org_gp_id || 5
 INTO l_group_id
 FROM wip_job_schedule_interface;
*/
 fnd_file.put_line (fnd_file.LOG, to_char(SYSDATE, 'DD-MON-YYYY HH:MI:SS') || ' Group ID for Update WO BOM  : '||l_group_id || ' Org - ' || g_org_code );

 l_last_wo_number := 'DUMMY1111111';


  OPEN cr_wo_component (g_preprocess) ;
   FETCH cr_wo_component BULK COLLECT INTO l_tbl_wo_component LIMIT g_bulk_col_lim;

 IF (l_tbl_wo_component.COUNT >0 ) THEN

  fnd_file.put_line (fnd_file.LOG, to_char(SYSDATE, 'DD-MON-YYYY HH:MI:SS') || ' Group ID for Update WO BOM  : '||l_tbl_wo_component.count   );

 FOR i IN 1.. l_tbl_wo_component.count    
 LOOP

 BEGIN
 l_error_code := xx_emf_cn_pkg.cn_success;
 l_org_id := NULL;
 l_primary_item_id := NULL;
 l_wip_entity_id := NULL;
 l_inventory_item_id := NULL;
 l_scheduled_completion_date := NULL;

 SELECT wdj.organization_id, 
		wdj.primary_item_id,
		wdj.wip_entity_id, 
		wdj.scheduled_completion_date
	INTO 	l_org_id, 
			l_primary_item_id,
			l_wip_entity_id, 
			l_scheduled_completion_date
 FROM wip_entities we, 
	  wip_discrete_jobs wdj, 
	  mtl_parameters mp
 WHERE we.wip_entity_name = l_tbl_wo_component(i).work_order_number
 AND we.organization_id = mp.organization_id
 AND mp.organization_code = l_tbl_wo_component(i).org_code
 AND we.wip_entity_id = wdj.wip_entity_id
 AND we.organization_id = wdj.organization_id;


 SELECT inventory_item_id
	INTO l_inventory_item_id
 FROM mtl_system_items_b
	WHERE segment1 = l_tbl_wo_component(i).component_item_code
	AND organization_id = l_org_id;

 BEGIN

 IF l_last_wo_number <> l_tbl_wo_component(i).work_order_number THEN

 l_header_id := NULL;

 SELECT (NVL (MAX (header_id), 0) + 1)
	INTO l_header_id
 FROM wip_job_schedule_interface;

 INSERT INTO wip_job_schedule_interface
 ( 
			last_update_date,
			last_updated_by,
			creation_date, 
			created_by,
			last_update_login, 
			GROUP_ID, 
			process_phase,
			process_status, 
			organization_id, 
			load_type,
			primary_item_id, 
			job_name,
			wip_entity_id, 
			attribute6, 
			attribute7, 
			header_id,
			last_unit_completion_date 
			)
			VALUES ( 
			l_tbl_wo_component(i).last_update_date,
			l_tbl_wo_component(i).last_updated_by,
			SYSDATE, 
			l_tbl_wo_component(i).created_by,
			l_tbl_wo_component(i).last_update_login, 
			l_group_id, 
			2,			-- process_phase 2 Validation 3 Explosion 4 Complete 5 Creation
			1,			-- process_status 1 Pending 2 Running 3 Error 4 Complete 5 Warning
			l_org_id, 
			3,			--load type --loadtype
			/*
			1 Create Standard Discrete Job
			2 Create Pending Repetitive Schedule
			3 Update Standard or Non-Standard Discrete Job
			4 Create Non-Standard Discrete Job
			*/
			l_primary_item_id, 
			l_tbl_wo_component(i).work_order_number,
			l_wip_entity_id, 
			l_tbl_wo_component(i).attribute6, 
			l_tbl_wo_component(i).attribute7,
			l_header_id,
			l_scheduled_completion_date 
			);
 END IF;

 INSERT INTO wip_job_dtls_interface
			(organization_id, 
			operation_seq_num,
			wip_entity_id, 
			inventory_item_id_old,
			inventory_item_id_new,
			GROUP_ID, 
			parent_header_id, 
			load_type,
			substitution_type,
			process_phase, 
			process_status,
			last_update_date,
			last_updated_by, 
			creation_date,
			created_by, 
			last_update_login,
			quantity_per_assembly
			)
			VALUES (
			l_org_id, 
			l_tbl_wo_component(i).operation_number,
			l_wip_entity_id, 
			l_inventory_item_id,
			l_inventory_item_id, --component inventory item id
			l_group_id, 
			l_header_id, 
			2,			--load_type 1. resource 2. component 3. operation 4. multiple resource usage
			3, --substitution_type 1.Delete, 2.Add 3.Change
			2, --process_phase
			1, --process_status
			l_tbl_wo_component(i).last_update_date,
			l_tbl_wo_component(i).last_updated_by, 
			l_tbl_wo_component(i).creation_date,
			l_tbl_wo_component(i).created_by, 
			l_tbl_wo_component(i).last_update_login,
			l_tbl_wo_component(i).per_assembly_qty
			);

 --refresh last WO#
 l_last_wo_number := l_tbl_wo_component(i).work_order_number;

 EXCEPTION WHEN OTHERS
 THEN
 l_error_code := xx_emf_cn_pkg.cn_rec_err;
 xx_emf_pkg.write_log
 (xx_emf_cn_pkg.cn_low,
 'Error while inserting WO component CREATE records in interface : '
 || SQLERRM
 );
 END;

 mark_records_complete_prc (l_tbl_wo_component(i).record_id,
 l_error_code,
 g_interface,
 l_wo_bom_tbl
 );
 xx_emf_pkg.propagate_error (l_error_code);
 EXCEPTION
 -- If HIGH error then it will be propagated to the next level
 -- IF the process has to continue maintain it as a medium severity
 WHEN xx_emf_pkg.g_e_rec_error
 THEN
	fnd_file.put_line (fnd_file.LOG, ' In Exception 1:' || SQLERRM);
	xx_emf_pkg.write_log (xx_emf_cn_pkg.cn_high, xx_emf_cn_pkg.cn_rec_err );
 WHEN xx_emf_pkg.g_e_prc_error
 THEN
	fnd_file.put_line (fnd_file.LOG, ' In Exception 2:' || SQLERRM);
	xx_emf_pkg.write_log (xx_emf_cn_pkg.cn_high, 'Process Level Error in Data Processing' );
	raise_application_error (-20199, xx_emf_cn_pkg.cn_prc_err);
 WHEN OTHERS
 THEN
	fnd_file.put_line (fnd_file.LOG, ' In Exception:' || SQLERRM);
	xx_emf_pkg.error
					(p_severity => xx_emf_cn_pkg.cn_medium,
					p_category => xx_emf_cn_pkg.cn_tech_error,
					p_error_text => xx_emf_cn_pkg.cn_exp_unhand,
					p_record_identifier_1 => l_tbl_wo_component(i).record_id,
					p_record_identifier_3 => 'Stage :Data Processing'
					);
 END;
 END LOOP;

 l_tbl_wo_component.delete ;
 END IF;

 CLOSE cr_wo_component;
 COMMIT;
 ---------------------------(call wip mass load)--------------------------------------
 l_error_code := call_wip_mass_load (l_group_id);
 mark_interface_errors_comp (l_group_id, g_update, g_interface);

 EXCEPTION
 WHEN OTHERS
 THEN
	fnd_file.put_line (fnd_file.LOG, l_module_name || ' Message:' || SQLERRM );
	xx_emf_pkg.write_log (xx_emf_cn_pkg.cn_low, 'error in procedure update_wip_comp' );
 END update_wip_comp;

----------------------------------------------------------------------
/*
Procedure Name: cancel_wip_job
Authors name: Manisha Mohanty
Date written: 25-Oct-2013
RICEW Object id: WIP_I-196
Description: Set Conversion Environment
Program Style: Subordinate
Change History:
Date Issue# Name Remarks
----------- ------- ------------------ ------------------------------
25-Oct-2013 1.0 Manisha Mohanty Initial development.
*/
----------------------------------------------------------------------
 PROCEDURE cancel_wip_job (
							errbuf OUT NOCOPY VARCHAR2,
							retcode OUT NOCOPY VARCHAR2,
							p_org_code IN VARCHAR2,
							p_wait_time IN NUMBER,
							p_max_wait  IN NUMBER 
 )
 IS
 l_module_name VARCHAR2 (60) := 'cancel_wip_job';
 l_error_code NUMBER := xx_emf_cn_pkg.cn_success;
 l_group_id NUMBER := 0;
 l_job_name wip_entities.wip_entity_name%TYPE;
 l_organization_id wip_entities.organization_id%TYPE;
 l_status_type wip_discrete_jobs.status_type%TYPE;
 l_org_id NUMBER;
 l_primary_item_id NUMBER;
 l_wip_entity_id NUMBER;
 l_scheduled_completion_date wip_discrete_jobs.scheduled_completion_date%TYPE;
 l_released VARCHAR2 (60) := 'Released';
 l_cancelled VARCHAR2 (60) := 'Cancelled';
 l_complete VARCHAR2 (60) := 'Complete'; 
 l_wo_hdr_tbl VARCHAR2 (60) := 'FTL_WO_HDR';
 l_cancel_counter NUMBER := 0; 
 l_int_count NUMBER :=0 ; 
 l_org_id_chk NUMBER :=0 ;
 l_wo_mtl_int_cnt 	NUMBER := 0; 
 l_wo_mv_int_cnt 	NUMBER := 0; 
 l_wo_cst_int_cnt 	NUMBER := 0; 
 l_org_gp_id NUMBER := 0 ; 
 l_wait_time	NUMBER := 0 ;
 l_max_wait_sec	NUMBER := 0 ;
 l_max_wait	NUMBER := 0 ;
 l_close_count NUMBER:= 0; --Added for changes in version 5.1

 l_in_prg_cnt NUMBER := 0; 
 l_int_err_cnt number := 0;

 CURSOR cr_wo_header (cp_process_status VARCHAR2)
 IS
 SELECT *
	FROM ftl_wo_hdr
 WHERE  org_code = p_org_code 
	AND int_process_code = cp_process_status
	AND NVL (int_error_code, xx_emf_cn_pkg.cn_success) IN
	(xx_emf_cn_pkg.cn_success, xx_emf_cn_pkg.cn_rec_warn)
	AND int_transaction_type = g_update
	AND NVL (work_order_status, l_released) IN (l_cancelled, l_complete ) 
 ORDER BY TIMESTAMP;

 PRAGMA AUTONOMOUS_TRANSACTION;
 BEGIN
 fnd_file.put_line (fnd_file.LOG,
 '--------------------------------------------------'
 );
 fnd_file.put_line (fnd_file.LOG, 'Cancel WIP jobs');
 fnd_file.put_line (fnd_file.LOG,
 '--------------------------------------------------'
 );
 ------------------------(cancel work order records)----------------------------------
fnd_file.put_line (fnd_file.log, 'Begin Cancel for Org ' || p_org_code || ' ' ||TO_CHAR(sysdate, 'dd-mon-yyyy hh24:mi:ss'));
 g_org_code := p_org_code;
 l_group_id := 0;

 l_wait_time := p_wait_time * 60 ;
 l_max_wait_sec := p_max_wait * 60 ;

 BEGIN
	SELECT organization_id
		INTO l_org_id_chk
	FROM   org_organization_definitions
	WHERE organization_code = g_org_code ;

 EXCEPTION WHEN OTHERS THEN
	  l_org_id_chk := 0 ;
 END;

 BEGIN
 fnd_file.put_line (fnd_file.LOG,'Before Pending interface record Count ' || to_char(SYSDATE, 'DD-MON-YYYY HH:MI:SS'));
 LOOP
 BEGIN

 SELECT COUNT(1) 
	INTO l_int_count
 FROM ftl_wo_hdr hdr
	WHERE org_code = p_org_code 
	AND int_process_code = 'In Progress'
	AND NVL (int_error_code, xx_emf_cn_pkg.cn_success) IN
	(xx_emf_cn_pkg.cn_success, xx_emf_cn_pkg.cn_rec_warn)
	AND int_transaction_type = g_update
	AND NVL (work_order_status, l_released) IN (l_cancelled, l_complete )
	AND (EXISTS (SELECT '1'
			FROM   mtl_transactions_interface
				WHERE source_code ='Job or Schedule'
		and PROCESS_FLAG in ('1', '2')
				AND transaction_source_name = hdr.WORK_ORDER_NUMBER
				AND ORGANIZATION_ID = l_org_id_chk
		)
		OR EXISTS ( SELECT '1'
			FROM   wip_move_txn_interface
		where  PROCESS_STATUS in (1, 2)
				AND WIP_ENTITY_NAME =  hdr.WORK_ORDER_NUMBER
				AND ORGANIZATION_CODE = p_org_code
		)
		OR EXISTS ( SELECT '1'
			FROM   WIP_COST_TXN_INTERFACE
		where wip_entity_id =  ( SELECT we.wip_entity_id
										FROM wip_entities we
											, wip_discrete_jobs wdj
										WHERE we.wip_entity_name = HDR.work_order_number
											AND we.organization_id = l_org_id_chk
											AND we.wip_entity_id = wdj.wip_entity_id
											AND we.organization_id = wdj.organization_id)
											AND ORGANIZATION_ID = l_org_id_chk)

		OR EXISTS (SELECT '1'
				FROM MTL_MATERIAL_TRANSACTIONS_TEMP MMTT
				WHERE ORGANIZATION_ID = l_org_id_chk
				AND TRANSACTION_SOURCE_NAME = HDR.work_order_number
					)
					)

 ;
fnd_file.put_line (fnd_file.LOG,'After Pending interface record Count ' || to_char(SYSDATE, 'DD-MON-YYYY HH:MI:SS'));
	fnd_file.put_line (fnd_file.LOG, 'Wait time for WO to picked up from interface count in Cancel: '||l_int_count);

 EXCEPTION 
 WHEN NO_DATA_FOUND THEN
			fnd_file.put_line (fnd_file.LOG,'No Record found exception for WO while getting interface count  in Cancel' );
			l_int_count :=  0;
 WHEN OTHERS THEN
			fnd_file.put_line (fnd_file.LOG,'Record for WO already pending in interface  in Cancel' || l_int_count);
			l_int_count :=  0;
	END;


	IF l_int_count <> 0 THEN
	fnd_file.put_line (fnd_file.log, 'Start of wait time ' || ' ' ||TO_CHAR(sysdate, 'dd-mon-yyyy hh24:mi:ss'));
		DBMS_LOCK.sleep(l_wait_time); --Sleep for l_wait_time Seconds
		l_max_wait := l_max_wait + l_wait_time ;
		fnd_file.put_line (fnd_file.LOG,'l_max_wait ' || l_max_wait);
		fnd_file.put_line (fnd_file.LOG,'l_int_count ' || l_int_count);
	END IF;

 EXIT WHEN l_int_count = 0 OR l_max_wait >= l_max_wait_sec; -- Maximum wait l_max_wait_sec;
 END LOOP;
 fnd_file.put_line (fnd_file.LOG,'End Time wait ' || to_char(SYSDATE, 'DD-MON-YYYY HH:MI:SS'));
 EXCEPTION WHEN OTHERS THEN
	fnd_file.put_line (fnd_file.LOG,'Exception in loop for checking pending interface transactions  in Cancel');
 END;

 BEGIN
	SELECT COUNT(1) 
		INTO l_in_prg_cnt
    FROM
    ( SELECT  work_order_number
        FROM  ftl_mat_txn
        WHERE org_code = p_org_code
        AND int_process_code = 'In Progress'
    UNION ALL
        SELECT work_order_number
        FROM   ftl_wo_txn
        WHERE  org_code = p_org_code
        AND int_process_code = 'In Progress'
    );
	fnd_file.put_line (fnd_file.log, 'After In progress record count from material and move stage table ' || ' ' ||TO_CHAR(sysdate, 'dd-mon-yyyy hh24:mi:ss'));
 fnd_file.put_line (fnd_file.LOG, 'In Progress count from stage table in Cancel: '||l_in_prg_cnt);
 EXCEPTION WHEN OTHERS THEN
	fnd_file.put_line (fnd_file.LOG,'Exception in l_count FTL_MAT_TXN AND ftl_wo_txn IN Cancel :' || l_in_prg_cnt);
	l_in_prg_cnt := 0 ;
 END;


BEGIN

	SELECT  ORGANIZATION_ID 
		INTO l_org_gp_id
	FROM org_organization_definitions
		WHERE ORGANIZATION_code = g_org_code;

EXCEPTION WHEN OTHERS THEN
	l_org_gp_id := 0 ;
END;

 SELECT TRUNC(SYSDATE) - TRUNC(SYSDATE , 'Year') || l_org_gp_id|| 6
	INTO l_group_id
 FROM dual;

/* SELECT (NVL (MAX (GROUP_ID), 0) + 1) || l_org_gp_id
	INTO l_group_id
 FROM wip_job_schedule_interface;
*/
	fnd_file.put_line (fnd_file.LOG, to_char(SYSDATE, 'DD-MON-YYYY HH:MI:SS') || ' Group ID for Cancel WIP JOB  : '||l_group_id || ' Org - ' || g_org_code );

 FOR rec_hdr IN cr_wo_header ('In Progress')
 LOOP
	l_cancel_counter := l_cancel_counter + 1;
	fnd_file.put_line (fnd_file.LOG,'Beginning of Cancel WO loop ' || to_char(SYSDATE, 'DD-MON-YYYY HH:MI:SS'));
 BEGIN
 l_error_code := xx_emf_cn_pkg.cn_success;
 l_org_id := NULL;
 l_primary_item_id := NULL;
 l_wip_entity_id := NULL;
 l_scheduled_completion_date := NULL; --added 17-JUN-2014

  IF l_in_prg_cnt > 0 THEN
	UPDATE ftl_wo_hdr
	SET int_process_code = 'New',
	int_error_code = '0',
	REQUEST_ID = null, 
	INT_TRANSACTION_TYPE= NULL,
	int_error_message = 'In Progress WO stage transactions exist for this org, cannot cancel/complete WO'
	WHERE  org_code = p_org_code 
		AND int_process_code = 'In Progress'
		AND work_order_status IN (l_cancelled, l_complete ) 
	;
	COMMIT;
	--EXIT cancel_wip_job;
	fnd_file.put_line (fnd_file.LOG, 'Cancel program to exit ' );
	fnd_file.put_line (fnd_file.LOG,'After update staging records to New if there is any In progress records ' || to_char(SYSDATE, 'DD-MON-YYYY HH:MI:SS'));
	retcode     := xx_emf_cn_pkg.cn_rec_warn;
	EXIT;
 END IF;

 SELECT wdj.organization_id, 
		wdj.primary_item_id,
		wdj.wip_entity_id, 
		wdj.scheduled_completion_date
	INTO l_org_id, 
		 l_primary_item_id,
		 l_wip_entity_id, 
		 l_scheduled_completion_date
 FROM wip_entities we, 
	  wip_discrete_jobs wdj, 
	  mtl_parameters mp
 WHERE we.wip_entity_name = rec_hdr.work_order_number
	AND we.organization_id = mp.organization_id
	AND mp.organization_code = rec_hdr.org_code
	AND we.wip_entity_id = wdj.wip_entity_id
	AND we.organization_id = wdj.organization_id;



	fnd_file.put_line (fnd_file.LOG,'Before Pending interface record Count ' || to_char(SYSDATE, 'DD-MON-YYYY HH:MI:SS'));
BEGIN	
	SELECT COUNT(1) 
	INTO l_int_err_cnt
 FROM ftl_wo_hdr hdr
	WHERE org_code = p_org_code 
	AND int_process_code = 'In Progress'
	AND NVL (int_error_code, xx_emf_cn_pkg.cn_success) IN
	(xx_emf_cn_pkg.cn_success, xx_emf_cn_pkg.cn_rec_warn)
	AND int_transaction_type = g_update
	AND NVL (work_order_status, l_released) IN (l_cancelled, l_complete )
	and hdr.work_order_number = rec_hdr.work_order_number --Added for MFG 2.1
	AND (EXISTS (SELECT '1'
			FROM   mtl_transactions_interface
				WHERE source_code ='Job or Schedule'
				and transaction_source_name = rec_hdr.work_order_number --Added for MFG 2.1
				AND transaction_source_name = hdr.WORK_ORDER_NUMBER
				AND ORGANIZATION_ID = l_org_id_chk
		)
		OR EXISTS ( SELECT '1'
			FROM   wip_move_txn_interface
		where  WIP_ENTITY_NAME =  hdr.WORK_ORDER_NUMBER
				and WIP_ENTITY_NAME = rec_hdr.work_order_number --Added for MFG 2.1
				AND ORGANIZATION_CODE = p_org_code
		)
		OR EXISTS ( SELECT '1'
			FROM   ftl_mat_txn
		where  work_order_number =  hdr.WORK_ORDER_NUMBER
				and work_order_number = rec_hdr.work_order_number --Added for MFG 2.1
				AND ORG_CODE = p_org_code
				AND int_error_code <> '0'
		)
		OR EXISTS ( SELECT '1'
			FROM   ftl_wo_txn
		where  work_order_number =  hdr.WORK_ORDER_NUMBER
				and work_order_number = rec_hdr.work_order_number --Added for MFG 2.1
				AND ORG_CODE = p_org_code
				AND int_error_code <> '0'
		)
		OR EXISTS ( SELECT '1'
			FROM   WIP_COST_TXN_INTERFACE
		where wip_entity_id =  ( SELECT we.wip_entity_id
										FROM wip_entities we
											, wip_discrete_jobs wdj
										WHERE we.wip_entity_name = HDR.work_order_number
											and we.wip_entity_name = rec_hdr.work_order_number --Added for MFG 2.1
											AND we.organization_id = l_org_id_chk
											AND we.wip_entity_id = wdj.wip_entity_id
											AND we.organization_id = wdj.organization_id)
											AND ORGANIZATION_ID = l_org_id_chk)

		OR EXISTS (SELECT '1'
				FROM MTL_MATERIAL_TRANSACTIONS_TEMP MMTT
				WHERE ORGANIZATION_ID = l_org_id_chk
				and TRANSACTION_SOURCE_NAME = rec_hdr.work_order_number --Added for MFG 2.1
				AND TRANSACTION_SOURCE_NAME = HDR.work_order_number
					)
					)

 ;
 fnd_file.put_line (fnd_file.LOG,'After Pending interface record Count ' || to_char(SYSDATE, 'DD-MON-YYYY HH:MI:SS'));
 EXCEPTION WHEN OTHERS THEN
			fnd_file.put_line (fnd_file.LOG,'Exception : Record for WO already in error in interface interface tables in cancel');
			l_int_err_cnt :=  0;
 END;
fnd_file.put_line (fnd_file.LOG, 'l_int_err_cnt in Cancel: '||l_int_err_cnt);

--Added for changes in version 5.1
l_close_count := 0;

IF UPPER(rec_hdr.work_order_status) = 'COMPLETE' THEN

BEGIN

	SELECT COUNT(1) 
			INTO l_close_count
			FROM apps.wip_discrete_jobs wdj, apps.wip_entities we 
		WHERE we.WIP_ENTITY_ID = wdj.WIP_ENTITY_ID 
			AND we.ORGANIZATION_ID = wdj.ORGANIZATION_ID 
			AND we.WIP_ENTITY_NAME = rec_hdr.work_order_number
			AND we.ORGANIZATION_ID = l_org_id_chk
			AND wdj.STATUS_TYPE = 12;

EXCEPTION WHEN OTHERS THEN
	l_close_count := 0 ;
	fnd_file.put_line (fnd_file.LOG, 'Error in getting closed WO to skip complete: ');
END;

END IF;
-- End of changes in version 5.1
IF  l_int_err_cnt = 0 THEN 

IF l_close_count = 0 THEN	--Added for changes in version 5.1

 BEGIN
 INSERT INTO wip_job_schedule_interface
				( 
				last_update_date,
				last_updated_by,
				last_updated_by_name,
				creation_date, 
				created_by_name,
				created_by, 
				last_update_login,
				GROUP_ID,
				process_phase, 
				process_status,
				organization_id, 
				load_type,
				status_type,
				last_unit_completion_date,
				primary_item_id, 
				completion_subinventory,
				class_code,
				job_name,
				start_quantity,
				wip_entity_id,
				attribute6,
				attribute7
				)
				VALUES ( 
				rec_hdr.last_update_date,
				rec_hdr.last_updated_by,
				rec_hdr.last_updated_by_name,
				SYSDATE, 
				rec_hdr.created_by_name,
				rec_hdr.created_by, 
				rec_hdr.last_update_login,
				l_group_id,
				2,				-- process_phase 2 Validation 3 Explosion 4 Complete 5 Creation
				1,				-- process_status 1 Pending 2 Running 3 Error 4 Complete 5 Warning
				l_org_id,
				3, --loadtype
				/*
				1 Create Standard Discrete Job
				2 Create Pending Repetitive Schedule
				3 Update Standard or Non-Standard Discrete Job
				4 Create Non-Standard Discrete Job
				*/
				(SELECT lookup_code
				FROM mfg_lookups
				WHERE lookup_type = 'WIP_JOB_STATUS'
				AND meaning = rec_hdr.work_order_status
				AND enabled_flag = xx_emf_cn_pkg.cn_yes)
				,
				NVL
				(rec_hdr.target_completion_date,
				l_scheduled_completion_date
				) ,
				l_primary_item_id,
				rec_hdr.compl_subinventory,
				rec_hdr.wip_accounting_class,
				rec_hdr.work_order_number,
				rec_hdr.target_completion_quantity,
				l_wip_entity_id,
				rec_hdr.attribute6,  
				rec_hdr.attribute7				
				);

fnd_file.put_line (fnd_file.LOG,'After Cancel record insert ' || to_char(SYSDATE, 'DD-MON-YYYY HH:MI:SS'));

 EXCEPTION
 WHEN OTHERS
 THEN
	l_error_code := xx_emf_cn_pkg.cn_rec_err;
	xx_emf_pkg.write_log
		(xx_emf_cn_pkg.cn_low,'Error while inserting WO header CANCEL records in interface : '|| SQLERRM);
 END;
END IF; --IF l_close_count = 0 --Added for changes in version 5.1
 mark_records_complete_prc (rec_hdr.record_id,
							l_error_code,
							g_interface,
							l_wo_hdr_tbl
							);
	fnd_file.put_line (fnd_file.LOG,'After mark records to complete ' || to_char(SYSDATE, 'DD-MON-YYYY HH:MI:SS'));
 xx_emf_pkg.propagate_error (l_error_code);

 ELSE 
	UPDATE ftl_wo_hdr
		SET INT_PROCESS_CODE = 'New', INT_ERROR_CODE = '0', REQUEST_ID = null, INT_TRANSACTION_TYPE= NULL
		, INT_ERROR_MESSAGE = 'In Progress/Error transactions exist for this org in stage, cannot cancel/complete WO'
	 WHERE work_order_number= rec_hdr.work_order_number
		AND org_code = p_org_code 
		AND int_process_code = 'In Progress'
		AND work_order_status IN (l_cancelled, l_complete ) 
		;
	COMMIT;
	fnd_file.put_line (fnd_file.LOG,'After update records to New in staging ' || to_char(SYSDATE, 'DD-MON-YYYY HH:MI:SS'));
 END IF;


 EXCEPTION
 -- If HIGH error then it will be propagated to the next level
 -- IF the process has to continue maintain it as a medium severity
 WHEN xx_emf_pkg.g_e_rec_error
 THEN
	fnd_file.put_line (fnd_file.LOG,' In Exception 1:' || SQLERRM);
	mark_records_complete_prc (rec_hdr.record_id,
							l_error_code,
							'ERROR',
							l_wo_hdr_tbl
							);
	xx_emf_pkg.write_log (xx_emf_cn_pkg.cn_high,xx_emf_cn_pkg.cn_rec_err);
 WHEN xx_emf_pkg.g_e_prc_error
 THEN
	fnd_file.put_line (fnd_file.LOG,' In Exception 2:' || SQLERRM);
	mark_records_complete_prc (rec_hdr.record_id,
							l_error_code,
							'ERROR',
							l_wo_hdr_tbl
							);
	xx_emf_pkg.write_log (xx_emf_cn_pkg.cn_high,'Process Level Error in Data Processing');
	raise_application_error (-20199, xx_emf_cn_pkg.cn_prc_err);
 WHEN OTHERS
 THEN
	fnd_file.put_line (fnd_file.LOG, ' In Exception:' || SQLERRM);
	mark_records_complete_prc (rec_hdr.record_id,
							l_error_code,
							'ERROR',
							l_wo_hdr_tbl
							);
	xx_emf_pkg.error (p_severity => xx_emf_cn_pkg.cn_medium,
						p_category => xx_emf_cn_pkg.cn_tech_error,
						p_error_text => xx_emf_cn_pkg.cn_exp_unhand,
						p_record_identifier_1 => rec_hdr.record_id,
						p_record_identifier_3 => 'Stage :Data Processing'
						);
 END;
 END LOOP;

 COMMIT;
 fnd_file.put_line (fnd_file.LOG,'End of WO loop ' || to_char(SYSDATE, 'DD-MON-YYYY HH:MI:SS'));
 ---------------------------(call wip mass load)--------------------------------------
 IF l_cancel_counter > 0 THEN
	l_error_code := call_wip_mass_load (l_group_id);
	mark_interface_errors_hdr (l_group_id, g_update, g_interface, g_org_code);
 END IF;
	fnd_file.put_line (fnd_file.LOG,
	'--------------------------------------------------'
	);
	fnd_file.put_line (fnd_file.LOG, 'Display record count');
	fnd_file.put_line (fnd_file.LOG,
	'--------------------------------------------------'
	);
	update_record_count_prc;
 EXCEPTION WHEN OTHERS
 THEN
	fnd_file.put_line (fnd_file.LOG,l_module_name || ' Message:' || SQLERRM	);
	xx_emf_pkg.write_log (xx_emf_cn_pkg.cn_low,	'error in procedure cancel_wip_job'	);
 END cancel_wip_job;
 ----------------------------------------------------------------------
/*
Procedure Name: close_wip_job
Authors name: Manisha Mohanty
Date written: 18-nov-2015
RICEW Object id: WIP_I-196
Description: Set Conversion Environment
Program Style: Subordinate
Change History:
Date Issue# Name Remarks
----------- ------- ------------------ ------------------------------
18-Nov-2015 1.0 Manisha Mohanty Initial development.
*/
----------------------------------------------------------------------
 PROCEDURE close_wip_job (
						errbuf OUT NOCOPY VARCHAR2,
						retcode OUT NOCOPY VARCHAR2,
						p_org_code IN VARCHAR2
						,p_wait_time IN NUMBER,
						p_max_wait  IN NUMBER  
 )
 IS
 l_module_name VARCHAR2 (60) := 'close_wip_job';
 l_error_code NUMBER := xx_emf_cn_pkg.cn_success;
 l_group_id NUMBER := 0;
 l_job_name wip_entities.wip_entity_name%TYPE;
 l_organization_id wip_entities.organization_id%TYPE;
 l_status_type wip_discrete_jobs.status_type%TYPE;
 l_org_id NUMBER;
 l_primary_item_id NUMBER;
 l_wip_entity_id NUMBER;
 l_scheduled_completion_date wip_discrete_jobs.scheduled_completion_date%TYPE;
 l_released VARCHAR2 (60) := 'Released';
 l_cancelled VARCHAR2 (60) := 'Cancelled';
 l_closed VARCHAR2(60) := 'Closed';
 l_wo_hdr_tbl VARCHAR2 (60) := 'FTL_WO_HDR';
 l_close_counter NUMBER := 0;
 l_request_id NUMBER := 0;
 l_close_group_id NUMBER :=0;
 l_sysdate DATE;
 l_msg_data              VARCHAR2(200);
 l_msg_data1               VARCHAR2(200);
 l_wo_mtl_int_cnt 	NUMBER := 0; 
 l_wo_mv_int_cnt 	NUMBER := 0; 
 l_int_count NUMBER := 0; 
 l_wo_cst_int_cnt NUMBER := 0; 
 l_pn_int_cnt  NUMBER := 0; 
 l_wait_time	NUMBER := 0 ;
 l_max_wait_sec	NUMBER := 0 ;
 l_max_wait	NUMBER := 0 ;
 l_in_prg_cnt NUMBER :=0 ;
 l_timezone         VARCHAR2(50); -- Added for Timezone modification on transaction_date SR00453359
 l_transaction_Date DATE;         -- Added for Timezone modification on transaction_date SR00453359
 l_errm VARCHAR2(1000);			  -- Added for Timezone modification on transaction_date SR00453359
 l_int_err_cnt number := 0 ;
 l_rsr_date		DATE; 	--Added as part of MFG 2.1 bug fix 08/Feb/19	
 l_cls_date		DATE; 	--Added as part of MFG 2.1 bug fix 08/Feb/19	
 l_close_date	DATE;  	--Added as part of MFG 2.1 bug fix 08/Feb/19	
 l_init_wait 	NUMBER ; -- Added as part of Jira#AMSEBS-2030 (IN00592489)

 CURSOR cr_wo_header (cp_process_status VARCHAR2,
                      cp_org_code VARCHAR2)
 IS
 SELECT *
	FROM ftl_wo_hdr
 WHERE org_code = p_org_code 
	AND int_process_code = cp_process_status
	AND NVL (int_error_code, xx_emf_cn_pkg.cn_success) IN
					(xx_emf_cn_pkg.cn_success, xx_emf_cn_pkg.cn_rec_warn)
	AND int_transaction_type = g_update
	AND NVL (work_order_status, l_released) = l_closed
	AND org_code = cp_org_code
 ORDER BY TIMESTAMP;

 CURSOR cr_close_wo(cp_process_status VARCHAR2)
 IS
 SELECT DISTINCT ORG_CODE,
		(select organization_id from mtl_parameters where organization_code = org_code) org_id
 FROM ftl_wo_hdr
	WHERE org_code = p_org_code
	AND int_process_code = cp_process_status
	AND NVL (int_error_code, xx_emf_cn_pkg.cn_success) IN
	(xx_emf_cn_pkg.cn_success, xx_emf_cn_pkg.cn_rec_warn)
	AND int_transaction_type = g_update
	AND NVL (work_order_status, l_released) = l_closed;


 PRAGMA AUTONOMOUS_TRANSACTION;
 BEGIN
 fnd_file.put_line (fnd_file.LOG,
 '--------------------------------------------------'
 );
 fnd_file.put_line (fnd_file.LOG, 'Close WIP jobs');
 fnd_file.put_line (fnd_file.LOG,
 '--------------------------------------------------'
 );
 ------------------------(Close work order records)----------------------------------
 fnd_file.put_line (fnd_file.log, 'Begin Close for Org ' || p_org_code || ' ' ||TO_CHAR(sysdate, 'dd-mon-yyyy hh24:mi:ss'));

 g_org_code := p_org_code;
 l_sysdate := SYSDATE;

 l_wait_time := p_wait_time * 60 ;
 l_max_wait_sec := p_max_wait * 60 ;

 -- Added as part of Jira#AMSEBS-2030 (IN00592489)
 LOOP
	l_init_wait := 0 ;
     fnd_file.put_line (fnd_file.log, 'Initial 5 mins wait time start : '  || ' ' ||TO_CHAR(sysdate, 'dd-mon-yyyy hh24:mi:ss'));
	DBMS_LOCK.sleep(300); --Sleep for 5 minutes
 EXIT WHEN l_init_wait = 0 ;
 END LOOP;
 l_init_wait := 5 ;
 fnd_file.put_line (fnd_file.log, 'Initial 5 mins wait time completed : '  || ' ' ||TO_CHAR(sysdate, 'dd-mon-yyyy hh24:mi:ss'));
 -- End of Code change for Jira#AMSEBS-2030 (IN00592489)

 fnd_file.put_line (fnd_file.LOG, ' fnd_global.CONC_REQUEST_ID :'|| fnd_global.CONC_REQUEST_ID);

 --------------------------------Call CP to Close jobs----------------------------------
 FOR rec_close_job IN cr_close_wo('In Progress')
 LOOP
 l_request_id := 0;
 l_close_group_id := NULL;
 l_organization_id := rec_close_job.org_id;

	fnd_file.put_line (fnd_file.log, 'Start of the Loop' || ' ' ||TO_CHAR(sysdate, 'dd-mon-yyyy hh24:mi:ss'));
 SELECT WIP_DJ_CLOSE_TEMP_S.nextval
	INTO l_close_group_id
  FROM   DUAL
  ;

 BEGIN
 fnd_file.put_line (fnd_file.LOG,'Time wait ' || to_char(SYSDATE, 'DD-MON-YYYY HH:MI:SS'));

 LOOP
 BEGIN
fnd_file.put_line (fnd_file.log, 'Before In progress record count' || ' ' ||TO_CHAR(sysdate, 'dd-mon-yyyy hh24:mi:ss'));
 SELECT COUNT(1) 
	INTO l_int_count
 FROM ftl_wo_hdr hdr
	WHERE org_code = p_org_code 
	AND int_process_code = 'In Progress'
	AND NVL (int_error_code, xx_emf_cn_pkg.cn_success) IN
	(xx_emf_cn_pkg.cn_success, xx_emf_cn_pkg.cn_rec_warn)
	AND int_transaction_type = g_update
	AND NVL (work_order_status, l_released) = l_closed
	AND (EXISTS (SELECT '1'
			FROM   mtl_transactions_interface
				WHERE source_code ='Job or Schedule'
				and PROCESS_FLAG in ('1', '2')
				AND transaction_source_name = hdr.WORK_ORDER_NUMBER
				AND ORGANIZATION_ID = rec_close_job.org_id
				)
		OR EXISTS ( SELECT '1'
			FROM   wip_move_txn_interface
		where   PROCESS_STATUS in (1, 2)
				AND WIP_ENTITY_NAME =  hdr.WORK_ORDER_NUMBER
				AND ORGANIZATION_CODE = rec_close_job.ORG_CODE
					)
		OR EXISTS ( SELECT '1'
			FROM   WIP_COST_TXN_INTERFACE
		where	NVL(ORGANIZATION_ID, rec_close_job.org_id) = rec_close_job.org_id
					AND wip_entity_id =  ( SELECT we.wip_entity_id
										FROM wip_entities we
											, wip_discrete_jobs wdj
										WHERE we.wip_entity_name = HDR.work_order_number
											AND we.organization_id = rec_close_job.org_id
											AND we.wip_entity_id = wdj.wip_entity_id
											AND we.organization_id = wdj.organization_id)
					AND ORGANIZATION_ID = rec_close_job.org_id)
					)
		OR EXISTS (SELECT '1'
				FROM MTL_MATERIAL_TRANSACTIONS_TEMP MMTT
				WHERE ORGANIZATION_ID = rec_close_job.org_id
					  AND TRANSACTION_SOURCE_NAME = HDR.work_order_number
					)

 ;
 fnd_file.put_line (fnd_file.log, 'After In progress record count' || ' ' ||TO_CHAR(sysdate, 'dd-mon-yyyy hh24:mi:ss'));
 fnd_file.put_line (fnd_file.LOG, 'Wait time for WO to picked up from interface count: '||l_int_count);

 EXCEPTION 
 WHEN NO_DATA_FOUND THEN
			fnd_file.put_line (fnd_file.LOG,'No Record found exception for WO while getting interface count ' );
			l_int_count :=  0;
 WHEN OTHERS THEN
			fnd_file.put_line (fnd_file.LOG,'Record for WO already pending in interface ' || l_int_count);
			l_int_count :=  0;
	END;

	IF l_int_count <> 0 THEN
		fnd_file.put_line (fnd_file.log, 'Start of wait time' || ' ' ||TO_CHAR(sysdate, 'dd-mon-yyyy hh24:mi:ss'));
		DBMS_LOCK.sleep(l_wait_time); --Sleep for l_wait_time Seconds
		l_max_wait := l_max_wait + l_wait_time ;
	END IF;
	fnd_file.put_line (fnd_file.LOG,'l_max_wait ' || l_max_wait);
	fnd_file.put_line (fnd_file.LOG,'l_int_count ' || l_int_count);
 EXIT WHEN l_int_count = 0 OR l_max_wait >= l_max_wait_sec; -- Maximum wait l_max_wait_sec
 END LOOP;

 fnd_file.put_line (fnd_file.LOG,'End Time wait ' || to_char(SYSDATE, 'DD-MON-YYYY HH:MI:SS'));
 EXCEPTION WHEN OTHERS THEN
	fnd_file.put_line (fnd_file.LOG,'Exception in loop for checking pending interface transactions ');
 END;


 BEGIN
	SELECT COUNT(1) 
		INTO l_in_prg_cnt
    FROM
    ( SELECT  work_order_number
        FROM  ftl_mat_txn
        WHERE org_code = p_org_code
        AND int_process_code = 'In Progress'
    UNION ALL
        SELECT work_order_number
        FROM   ftl_wo_txn
        WHERE  org_code = p_org_code
        AND int_process_code = 'In Progress'
    );
 fnd_file.put_line (fnd_file.LOG, 'In Progress records stage table for Close: '||l_in_prg_cnt);
 EXCEPTION WHEN OTHERS THEN
	fnd_file.put_line (fnd_file.LOG,'Exception in l_count FTL_MAT_TXN AND ftl_wo_txn IN Cancel :' || l_in_prg_cnt);
	l_in_prg_cnt := 0 ;
 END;
 fnd_file.put_line (fnd_file.log, 'Staging In progress count ' || ' ' ||TO_CHAR(sysdate, 'dd-mon-yyyy hh24:mi:ss'));

 IF l_in_prg_cnt > 0 THEN
	UPDATE ftl_wo_hdr
	SET int_process_code = 'New',
	int_error_code = '0',
	REQUEST_ID = null, 
	INT_TRANSACTION_TYPE= NULL,
	int_error_message = 'In Progress/Error transactions exist for this org in stage, cannot close WO'
	WHERE  org_code = p_org_code 
		AND int_process_code = 'In Progress'
		AND work_order_status = 'Closed' 
	;
COMMIT;
fnd_file.put_line (fnd_file.log, 'After update In Progress records to New ' || ' ' ||TO_CHAR(sysdate, 'dd-mon-yyyy hh24:mi:ss'));
	--EXIT close_wip_job;
	retcode     := xx_emf_cn_pkg.cn_rec_warn;
	EXIT ;

 END IF;

 UPDATE FTL_WO_HDR SET REQUEST_ID = fnd_global.CONC_REQUEST_ID
 WHERE ORG_CODE = rec_close_job.ORG_CODE  
 AND INT_PROCESS_CODE = 'In Progress'
 AND WORK_ORDER_STATUS = 'Closed'
 ;
COMMIT;
fnd_file.put_line (fnd_file.log, 'Request ID updated for Close records ' || ' ' ||TO_CHAR(sysdate, 'dd-mon-yyyy hh24:mi:ss'));
 FOR rec_close IN cr_wo_header('In Progress',rec_close_job.ORG_CODE)
 LOOP

  fnd_file.put_line (fnd_file.log, 'Before interface error count query ' || ' ' ||TO_CHAR(sysdate, 'dd-mon-yyyy hh24:mi:ss'));
 BEGIN	
	SELECT COUNT(1) 
	INTO l_int_err_cnt
 FROM ftl_wo_hdr hdr
	WHERE org_code = p_org_code 
	AND int_process_code = 'In Progress'
	AND NVL (int_error_code, xx_emf_cn_pkg.cn_success) IN
	(xx_emf_cn_pkg.cn_success, xx_emf_cn_pkg.cn_rec_warn)
	AND int_transaction_type = g_update
	AND NVL (work_order_status, l_released) IN ('Closed' )
	 AND hdr.work_order_number = rec_close.work_order_number --Added for MFG 2.1
	AND (EXISTS (SELECT '1'
			FROM   mtl_transactions_interface
				WHERE source_code ='Job or Schedule'
				AND transaction_source_name = hdr.WORK_ORDER_NUMBER
				AND transaction_source_name = rec_close.work_order_number --Added for MFG 2.1
				AND ORGANIZATION_ID = rec_close_job.org_id
		)
		OR EXISTS ( SELECT '1'
			FROM   wip_move_txn_interface
		where  WIP_ENTITY_NAME =  hdr.WORK_ORDER_NUMBER
				AND WIP_ENTITY_NAME = rec_close.work_order_number --Added for MFG 2.1
				AND ORGANIZATION_CODE = p_org_code
		)
		OR EXISTS ( SELECT '1'
			FROM   ftl_mat_txn
		where  work_order_number =  hdr.WORK_ORDER_NUMBER
				AND work_order_number = rec_close.work_order_number --Added for MFG 2.1
				AND ORG_CODE = p_org_code
				AND int_error_code <> '0'
		)
		OR EXISTS ( SELECT '1'
			FROM   ftl_wo_txn
		where  work_order_number =  hdr.WORK_ORDER_NUMBER
				AND work_order_number = rec_close.work_order_number --Added for MFG 2.1
				AND ORG_CODE = p_org_code
				AND int_error_code <> '0'
		)
		OR EXISTS ( SELECT '1'
			FROM   WIP_COST_TXN_INTERFACE
		where wip_entity_id =  ( SELECT we.wip_entity_id
										FROM wip_entities we
											, wip_discrete_jobs wdj
										WHERE we.wip_entity_name = HDR.work_order_number
											AND wip_entity_name = rec_close.work_order_number --Added for MFG 2.1
											AND we.organization_id = rec_close_job.org_id
											AND we.wip_entity_id = wdj.wip_entity_id
											AND we.organization_id = wdj.organization_id)
											AND ORGANIZATION_ID = rec_close_job.org_id)

		OR EXISTS (SELECT '1'
				FROM MTL_MATERIAL_TRANSACTIONS_TEMP MMTT
				WHERE ORGANIZATION_ID = rec_close_job.org_id
				AND TRANSACTION_SOURCE_NAME = rec_close.work_order_number --Added for MFG 2.1
				AND TRANSACTION_SOURCE_NAME = HDR.work_order_number
					)
					)
 ;
 fnd_file.put_line (fnd_file.log, 'After interface error count query ' || ' ' ||TO_CHAR(sysdate, 'dd-mon-yyyy hh24:mi:ss'));
 EXCEPTION WHEN OTHERS THEN
			fnd_file.put_line (fnd_file.LOG,'Exception : Record for WO already in error in interface interface tables in cancel');
			l_int_err_cnt :=  0;
 END;
fnd_file.put_line (fnd_file.LOG, 'Interface records in any status l_int_err_cnt in Close : '||l_int_err_cnt);

IF  l_int_err_cnt = 0 THEN 



	BEGIN
   SELECT we.wip_entity_id
         ,we.primary_item_id
         ,wdj.status_type
	INTO l_wip_entity_id
	    ,l_primary_item_id
		,l_status_type
    FROM wip_entities we
	   , wip_discrete_jobs wdj
   WHERE we.wip_entity_name = rec_close.work_order_number
     AND we.organization_id = l_organization_id
	 AND we.wip_entity_id = wdj.wip_entity_id
	 AND we.organization_id = wdj.organization_id;
	EXCEPTION WHEN OTHERS THEN
		Fnd_file.put_line (fnd_file.LOG, 'WO does not exist in Oracle to Close : '||l_wip_entity_id);
		UPDATE ftl_wo_hdr
		SET INT_PROCESS_CODE = 'ERROR', INT_ERROR_CODE = '2'
		, INT_ERROR_MESSAGE = 'WO not found in Oracle'
	 WHERE work_order_number= rec_close.work_order_number
		AND org_code = p_org_code 
		AND int_process_code = 'In Progress'
		AND work_order_status = 'Closed'
		;
	END;

	BEGIN

	--Added as part of MFG 2.1 bug fix 08/Feb/19	
	BEGIN
	SELECT max(transaction_date)
		INTO l_rsr_date
		FROM wip_transactions_v
	WHERE wip_entity_name = rec_close.work_order_number
		AND organization_id = l_organization_id
	 ;

	 Fnd_file.put_line (fnd_file.LOG, 'l_rsr_date: '||l_rsr_date ); 
	EXCEPTION WHEN OTHERS THEN
		Fnd_file.put_line (fnd_file.LOG, 'Exception in Get resource date: '||l_wip_entity_id || ' '|| rec_close.work_order_number ||' ' || l_organization_id);
		--l_rsr_date := null ;
	END;
	--End of MFG 2.1 bug fix 08/Feb/19


	 --Added for Timezone modification on transaction_date SR00453359
      fnd_file.put_line (fnd_file.log, 'Timezone modification Starts' || ' ' ||TO_CHAR(sysdate, 'dd-mon-yyyy hh24:mi:ss'));
      BEGIN
        SELECT timezone_code
        INTO l_timezone
        FROM hr_locations_v hlv,
          hr_organization_units_v houv,
          mtl_parameters mp
        WHERE houv.organization_id = mp.organization_id
        AND houv.location_id       = hlv.location_id
        AND mp.organization_code = rec_close.org_code;
      EXCEPTION
      WHEN OTHERS THEN
        l_errm := SQLERRM;
        fnd_file.put_line (fnd_file.log, 'Failed in fetching Timezone for Org: '|| rec_close.org_code || l_errm);
      END;
      IF l_timezone        IS NULL THEN
        l_transaction_Date := rec_close.timestamp;
      ELSE
        -- l_transaction_Date := NEW_TIME(TO_DATE(l_array_hdr1(z).transaction_date, 'DD-MON-YYYY HH24:MI:SS'), l_timezone, 'CST');
        l_transaction_Date := CAST (from_tz(CAST (rec_close.timestamp AS TIMESTAMP),l_timezone) at TIME zone 'America/Chicago' AS DATE);
      END IF;
      fnd_file.put_line (fnd_file.log, 'Timezone modification Ends' || ' ' ||TO_CHAR(sysdate, 'dd-mon-yyyy hh24:mi:ss'));
      fnd_file.put_line (fnd_file.log, 'Transaction Date := ' ||TO_CHAR(l_transaction_Date,'dd-mon-yyyy hh24:mi:ss'));
      --End of Timezone modification on transaction_date SR00453359

	--Added as part of MFG 2.1 bug fix 08/Feb/19	
		BEGIN
		Fnd_file.put_line (fnd_file.LOG, 'l_transaction_Date: '|| TO_CHAR(l_transaction_Date,'dd-mon-yyyy hh24:mi:ss') );
		Fnd_file.put_line (fnd_file.LOG, 'l_rsr_date: '||l_rsr_date );
		Fnd_file.put_line (fnd_file.LOG, 'TO_date(trim(l_transaction_Date): '|| TO_date(trim(l_transaction_Date),'dd-mon-yyyy hh24:mi:ss') );

		SELECT  MAX(l_date)
			INTO l_cls_date  
		FROM (  SELECT l_transaction_Date l_date FROM dual 
									UNION  
				SELECT l_rsr_date l_date FROM dual
				)  
				; 
		Fnd_file.put_line (fnd_file.LOG, 'l_close_date: '||l_cls_date ||'for WO ' || l_wip_entity_id);
		Fnd_file.put_line (fnd_file.LOG, 'l_close_date 1: '||TO_CHAR(l_cls_date,'dd-mon-yyyy hh24:mi:ss') );
		--l_close_date := l_cls_date ;
    EXCEPTION WHEN OTHERS THEN
        Fnd_file.put_line (fnd_file.LOG, 'Exception in Get l_close_date: '||l_wip_entity_id);
    END;

	--End of MFG 2.1 bug fix 08/Feb/19

   INSERT INTO WIP_DJ_CLOSE_TEMP
              ( WIP_ENTITY_ID
			  , ORGANIZATION_ID
			  , WIP_ENTITY_NAME
			  , PRIMARY_ITEM_ID
			  , STATUS_TYPE
			  , ACTUAL_CLOSE_DATE
			  , GROUP_ID)
       VALUES( l_Wip_Entity_Id
	         , l_Organization_Id
			 , rec_close.work_order_number
			 , l_Primary_Item_Id
			 , l_status_type
            --, rec_close.TIMESTAMP
			--,l_transaction_Date
			,l_cls_date --Added as part of MFG 2.1 bug fix 08/Feb/19
			 , l_close_group_id);
	fnd_file.put_line (fnd_file.log, 'After Close WO insert ' || ' ' ||TO_CHAR(sysdate, 'dd-mon-yyyy hh24:mi:ss'));		 
 mark_records_complete_prc (rec_close.record_id,
 l_error_code,
 g_interface,
 l_wo_hdr_tbl
 );
fnd_file.put_line (fnd_file.log, 'After mark records to complete ' || ' ' ||TO_CHAR(sysdate, 'dd-mon-yyyy hh24:mi:ss'));
 EXCEPTION WHEN OTHERS
 THEN
 l_error_code := xx_emf_cn_pkg.cn_rec_err;
 mark_records_complete_prc (rec_close.record_id,
							l_error_code,
							'ERROR',
							l_wo_hdr_tbl
							);
 fnd_file.put_line (fnd_file.LOG, l_module_name || ' Message:' || SQLERRM );
 END;

 ELSE
	UPDATE ftl_wo_hdr
		SET INT_PROCESS_CODE = 'New', INT_ERROR_CODE = '0', REQUEST_ID = null, INT_TRANSACTION_TYPE= NULL
		,int_error_message = 'In Progress/Error transactions exist for this org in stage, cannot close WO'
	 WHERE work_order_number= rec_close.work_order_number
		AND org_code = p_org_code 
		AND int_process_code = 'In Progress'
		AND work_order_status = 'Closed'
		;
	COMMIT;
	fnd_file.put_line (fnd_file.log, 'After update status if WO have In prog transaction ' || ' ' ||TO_CHAR(sysdate, 'dd-mon-yyyy hh24:mi:ss'));
END IF;
 END LOOP;
 COMMIT;

fnd_file.put_line (fnd_file.log, 'Before API Call ' || ' ' ||TO_CHAR(sysdate, 'dd-mon-yyyy hh24:mi:ss'));
 wip_jobclose_priv.WIP_CLOSE_MGR
(
      ERRBUF               => l_msg_data,
      RETCODE              => l_msg_data1,
      p_organization_id    => l_organization_id,
      p_class_type         => NULL,
      p_from_class         => NULL ,
      p_to_class           => NULL ,
      p_from_job           => NULL ,
      p_to_job             => NULL ,
      p_from_release_date  => NULL ,
      p_to_release_date    => NULL ,
      p_from_start_date    => NULL ,
      p_to_start_date      => NULL ,
      p_from_completion_date => NULL ,
      p_to_completion_date => NULL ,
      p_status             => NULL ,
      p_group_id           => l_close_group_id,
      p_select_jobs        => 2,
      p_exclude_reserved_jobs => NULL,
      p_uncompleted_jobs   => NULL,
      p_exclude_pending_txn_jobs => NULL,
      p_report_type        => 1,
      p_act_close_date     => NULL
);
 COMMIT;
 fnd_file.put_line (fnd_file.log, 'After API call ' || ' ' ||TO_CHAR(sysdate, 'dd-mon-yyyy hh24:mi:ss'));


 fnd_file.put_line (fnd_file.LOG, 'Current request ID: '|| xx_emf_pkg.g_request_id );
 fnd_file.put_line (fnd_file.LOG, 'Current request ID: '|| fnd_global.conc_request_id );
fnd_file.put_line (fnd_file.log, 'Before Unclosed WO update ' || ' ' ||TO_CHAR(sysdate, 'dd-mon-yyyy hh24:mi:ss'));
 FOR close_fail IN (SELECT INT_TRANSACTION_TYPE, INT_BATCH_ID, ORG_CODE, WORK_ORDER_NUMBER, WORK_ORDER_STATUS, INT_PROCESS_CODE, INT_ERROR_CODE, RECORD_ID
							FROM ftl_wo_hdr hdr
								WHERE org_code = p_org_code
								AND int_transaction_type = g_update
								AND NVL (work_order_status, l_released) = l_closed
								AND org_code = rec_close_job.ORG_CODE
								AND request_id = fnd_global.CONC_REQUEST_ID
								AND INT_PROCESS_CODE IN ('INTERFACE', 'ERROR', 'In Progress')
								AND  EXISTS  ( SELECT 'X'
													FROM wip_entities we
														, wip_discrete_jobs wdj
												WHERE we.wip_entity_name = hdr.work_order_number
												AND we.organization_id = rec_close_job.org_id
												AND we.wip_entity_id = wdj.wip_entity_id
												AND we.organization_id = wdj.organization_id
												AND wdj.STATUS_TYPE <> 12
											)
						)
  LOOP
	fnd_file.put_line (fnd_file.LOG, 'Updated record to New WO: '|| close_fail.work_order_number );

	UPDATE ftl_wo_hdr
		SET INT_PROCESS_CODE = 'New', INT_ERROR_CODE = '0', REQUEST_ID = null, INT_TRANSACTION_TYPE= NULL
		, INT_ERROR_MESSAGE = 'WO Close failed in API'
		WHERE work_order_number= close_fail.work_order_number
		AND WORK_ORDER_STATUS = 'Closed'
		AND RECORD_ID = close_fail.RECORD_ID
		;
	COMMIT;
  END LOOP;
fnd_file.put_line (fnd_file.log, 'After Unclosed WO update ' || ' ' ||TO_CHAR(sysdate, 'dd-mon-yyyy hh24:mi:ss'));
 END LOOP;
 --short name = WICDCL
fnd_file.put_line (fnd_file.log, 'End of main Loop WO update ' || ' ' ||TO_CHAR(sysdate, 'dd-mon-yyyy hh24:mi:ss'));
 fnd_file.put_line (fnd_file.LOG,
 '--------------------------------------------------'
 );
 fnd_file.put_line (fnd_file.LOG, 'Display record count');
 fnd_file.put_line (fnd_file.LOG,
 '--------------------------------------------------'
 );
 update_record_count_prc;

 EXCEPTION
 WHEN OTHERS
 THEN

 fnd_file.put_line (fnd_file.LOG, l_module_name || ' Message:' || SQLERRM );
 xx_emf_pkg.write_log (xx_emf_cn_pkg.cn_low, 'error in procedure close_wip_job' );
 END close_wip_job;

----------------------------------------------------------------------
/*
Procedure Name: update_transaction_type
Authors name: Manisha Mohanty
Date written: 25-Oct-2013
RICEW Object id: WIP_I-196
Description: Set Conversion Environment
Program Style: Subordinate
Change History:
Date Issue# Name Remarks
----------- ------- ------------------ ------------------------------
25-Oct-2013 1.0 Manisha Mohanty Initial development.
*/
----------------------------------------------------------------------
 PROCEDURE update_transaction_type (
 p_mode IN VARCHAR2,
 p_organization_code IN VARCHAR2,
 p_wo_number IN VARCHAR2,
 p_timestamp IN DATE,
 p_record_id IN NUMBER,
 p_transaction_type IN VARCHAR2
 )
 IS
 l_header VARCHAR2 (20) := 'HEADER';
 l_bom VARCHAR2 (20) := 'BOM';
 l_mat VARCHAR2 (20) := 'MAT';
 PRAGMA AUTONOMOUS_TRANSACTION;
 BEGIN
 IF p_mode = l_header THEN

	UPDATE ftl_wo_hdr
		SET int_transaction_type = p_transaction_type
	WHERE 1 = 1 
		AND record_id = p_record_id
		AND org_code = p_organization_code
		AND work_order_number = p_wo_number;
 ELSIF p_mode = l_bom
 THEN
	UPDATE ftl_wo_bom
		SET int_transaction_type = p_transaction_type
	WHERE 1 = 1
		AND record_id = p_record_id
		AND org_code = p_organization_code
		AND work_order_number = p_wo_number;
 ELSIF p_mode = l_mat
 THEN
	UPDATE ftl_mat_txn
		SET int_transaction_type = p_transaction_type
	WHERE 1 = 1
		AND record_id = p_record_id
		AND org_code = p_organization_code
		AND work_order_number = p_wo_number;
 ELSE
	NULL;
 END IF;

 COMMIT;
 END update_transaction_type;

----------------------------------------------------------------------
/*
Function Name: get_child_wo_qty
Authors name: Manisha Mohanty
Date written: 25-Oct-2013
RICEW Object id: WIP_I-196
Description: Set Conversion Environment
Program Style: Subordinate
Change History:
Date Issue# Name Remarks
----------- ------- ------------------ ------------------------------
25-Oct-2013 1.0 Manisha Mohanty Initial development.
*/
----------------------------------------------------------------------
 FUNCTION get_child_wo_qty (
 p_record_id IN NUMBER,
 p_transaction_type IN VARCHAR2,
 p_wo_status IN VARCHAR2,
 p_wo_number IN VARCHAR2,
 p_org_code IN VARCHAR2,
 p_timestamp IN DATE,
 p_cur_child_wo_qty IN NUMBER
 )
 RETURN NUMBER
 IS
 l_prev_child_wo_qty NUMBER;
 l_cancelled VARCHAR2 (60) := 'Cancelled';
 l_closed VARCHAR2(60) := 'Closed';
 BEGIN
 IF p_transaction_type = g_create
 THEN
	/*For create transactions, return current child WO qty*/
	RETURN p_cur_child_wo_qty;

 ELSIF ( p_transaction_type = g_update AND (UPPER (p_wo_status) NOT IN (UPPER (l_cancelled),UPPER(l_closed ))) )
 THEN
 /*For update transactions, and for status not equal to Cancelled, 
  return difference of previous child WO qty and current child WO qty */
 BEGIN

 /*Get the previous child WO update record from FTL_WO_HDR*/
 SELECT hdr1.target_completion_quantity
	INTO l_prev_child_wo_qty
 FROM ftl_wo_hdr hdr1
 WHERE hdr1.int_batch_id = g_batch_id
	AND hdr1.org_code = p_org_code
	AND hdr1.work_order_number = p_wo_number
	AND hdr1.TIMESTAMP < p_timestamp
	AND hdr1.TIMESTAMP =
	(SELECT MAX (hdr2.TIMESTAMP)
	FROM ftl_wo_hdr hdr2
	WHERE hdr2.int_batch_id = g_batch_id
	AND hdr2.org_code = p_org_code
	AND hdr2.work_order_number = p_wo_number
	AND hdr2.TIMESTAMP < p_timestamp)
	;

	RETURN (p_cur_child_wo_qty - l_prev_child_wo_qty);

 EXCEPTION
 WHEN NO_DATA_FOUND
 THEN
	--check if the child wo exists in oracle wip
	SELECT wdj.start_quantity
		INTO l_prev_child_wo_qty
	FROM wip_entities we,
	mtl_parameters mp,
	wip_discrete_jobs wdj
	WHERE we.wip_entity_name = p_wo_number
	AND we.organization_id = mp.organization_id
	AND mp.organization_code = p_org_code
	AND we.wip_entity_id = wdj.wip_entity_id
	AND we.organization_id = wdj.organization_id;

	RETURN (p_cur_child_wo_qty - l_prev_child_wo_qty);
 WHEN OTHERS
 THEN
	RETURN p_cur_child_wo_qty;
 END;
 ELSE
	/*For update transactions,when the child WO status is cancelled, return negative of current child WO qty*/
	RETURN - (p_cur_child_wo_qty);
 END IF;
 END get_child_wo_qty;

----------------------------------------------------------------------
/*
Function Name: get_cur_parent_wo_qty
Authors name: Manisha Mohanty
Date written: 25-Oct-2013
RICEW Object id: WIP_I-196
Description: Set Conversion Environment
Program Style: Subordinate
Change History:
Date Issue# Name Remarks
----------- ------- ------------------ ------------------------------
25-Oct-2013 1.0 Manisha Mohanty Initial development.
*/
----------------------------------------------------------------------
 FUNCTION get_cur_parent_wo_qty (
								p_wo_number IN VARCHAR2,
								p_org_code IN VARCHAR2
								)
 RETURN NUMBER
 IS
 l_cur_parent_wo_qty NUMBER;
 l_released VARCHAR2 (60) := 'Released';
 l_cancelled VARCHAR2 (60) := 'Cancelled';
 l_closed VARCHAR2(60) := 'Closed';
 BEGIN
 BEGIN
	SELECT hdr1.target_completion_quantity
		INTO l_cur_parent_wo_qty
	FROM ftl_wo_hdr hdr1
	WHERE hdr1.int_batch_id = g_batch_id
		AND hdr1.org_code = p_org_code
		AND hdr1.work_order_number = p_wo_number
		AND hdr1.int_transaction_type = g_update
		AND NVL (hdr1.work_order_status, l_released) NOT IN (l_cancelled,l_closed) --added 18-Nov-2015
		AND hdr1.TIMESTAMP = (SELECT MAX (hdr2.TIMESTAMP)
								FROM ftl_wo_hdr hdr2
								WHERE hdr1.int_batch_id = hdr2.int_batch_id
									AND hdr1.org_code = hdr2.org_code
									AND hdr1.work_order_number = hdr2.work_order_number
									AND hdr1.int_transaction_type =
									hdr1.int_transaction_type
									AND NVL (hdr2.work_order_status, l_released) NOT IN (l_cancelled,l_closed)
							)
	ORDER BY TIMESTAMP DESC;

	RETURN l_cur_parent_wo_qty;
 EXCEPTION WHEN NO_DATA_FOUND
 THEN
	--check if the parent wo exists in oracle wip
	SELECT wdj.start_quantity
		INTO l_cur_parent_wo_qty
	FROM wip_entities we, mtl_parameters mp, wip_discrete_jobs wdj
		WHERE we.wip_entity_name = p_wo_number
		AND we.organization_id = mp.organization_id
		AND mp.organization_code = p_org_code
		AND we.wip_entity_id = wdj.wip_entity_id
		AND we.organization_id = wdj.organization_id;

	RETURN l_cur_parent_wo_qty;
 WHEN OTHERS
 THEN
 RETURN 0;
 END;
 END get_cur_parent_wo_qty;

----------------------------------------------------------------------
/*
Function Name: get_parent_wo_end_date
Authors name: Manisha Mohanty
Date written: 25-Oct-2013
RICEW Object id: WIP_I-196
Description: Set Conversion Environment
Program Style: Subordinate
Change History:
Date Issue# Name Remarks
----------- ------- ------------------ ------------------------------
25-Oct-2013 1.0 Manisha Mohanty Initial development.
*/
----------------------------------------------------------------------
 FUNCTION get_parent_wo_end_date (
 p_wo_number IN VARCHAR2,
 p_org_code IN VARCHAR2
 )
 RETURN DATE
 IS
 l_end_date DATE;
 BEGIN
 SELECT wdj.scheduled_completion_date
	INTO l_end_date
 FROM wip_entities we, mtl_parameters mp, wip_discrete_jobs wdj
	WHERE we.wip_entity_name = p_wo_number
	AND we.organization_id = mp.organization_id
	AND mp.organization_code = p_org_code
	AND we.wip_entity_id = wdj.wip_entity_id
	AND we.organization_id = wdj.organization_id;

	RETURN l_end_date;
 EXCEPTION WHEN OTHERS
 THEN
 RETURN NULL;
 END get_parent_wo_end_date;

----------------------------------------------------------------------
 /*
 Procedure Name: insert_parent_wo_rec
 Authors name: Manisha Mohanty
 Date written: 25-Oct-2013
 RICEW Object id: WIP_I-196
 Description: Set Conversion Environment
 Program Style: Subordinate
 Change History:
 Date Issue# Name Remarks
 ----------- ------- ------------------ ------------------------------
 25-Oct-2013 1.0 Manisha Mohanty Initial development.
 */
 ----------------------------------------------------------------------
 PROCEDURE insert_parent_wo_rec (
 p_transaction_type IN VARCHAR2,
 p_timestamp IN DATE,
 p_batch_id IN VARCHAR2,
 p_org_code IN VARCHAR2,
 p_work_order_number IN VARCHAR2,
 p_wip_accounting_class IN VARCHAR2,
 p_work_order_status IN VARCHAR2,
 p_parent_wo_number IN VARCHAR2,
 p_assembly_item_code IN VARCHAR2,
 p_assembly_item_uom IN VARCHAR2,
 p_target_start_date IN DATE,
 p_target_completion_date IN DATE,
 p_target_completion_quantity IN NUMBER,
 p_compl_subinventory IN VARCHAR2,
 p_int_process_code IN VARCHAR2,
 p_record_id IN NUMBER
 )
 IS
 l_last_update_date DATE := SYSDATE;
 l_last_updated_by NUMBER := fnd_global.user_id;
 l_last_update_login NUMBER := fnd_global.user_id;
 PRAGMA AUTONOMOUS_TRANSACTION;
 BEGIN
 INSERT INTO ftl_wo_hdr
			(int_transaction_type, 
			TIMESTAMP, 
			int_batch_id, 
			org_code,
			work_order_number, 
			wip_accounting_class,
			work_order_status, 
			parent_wo_number,
			assembly_item_code, 
			assembly_item_uom,
			target_start_date, 
			target_completion_date,
			target_completion_quantity, 
			compl_subinventory,
			int_process_code, 
			record_id, 
			request_id,
			int_error_code, 
			created_by,
			creation_date, 
			last_updated_by,
			last_update_date, 
			last_update_login
			)
			VALUES (p_transaction_type, 
			p_timestamp, 
			p_batch_id, 
			p_org_code,
			p_work_order_number, 
			p_wip_accounting_class,
			p_work_order_status, 
			p_parent_wo_number,
			p_assembly_item_code, 
			p_assembly_item_uom,
			p_target_start_date, 
			p_target_completion_date,
			p_target_completion_quantity, 
			p_compl_subinventory,
			p_int_process_code, 
			p_record_id, 
			xx_emf_pkg.g_request_id,
			xx_emf_cn_pkg.cn_null, 
			l_last_updated_by,
			l_last_update_date, 
			l_last_updated_by,
			l_last_update_date, 
			l_last_update_login
			);

 COMMIT;
 END insert_parent_wo_rec;

----------------------------------------------------------------------
/*
Procedure Name: wo_hdr_preprocessing
Authors name: Manisha Mohanty
Date written: 25-Oct-2013
RICEW Object id: WIP_I-196
Description: Set Conversion Environment
Program Style: Subordinate
Change History:
Date Issue# Name Remarks
----------- ------- ------------------ ------------------------------
25-Oct-2013 1.0 Manisha Mohanty Initial development.
*/
----------------------------------------------------------------------
 PROCEDURE wo_hdr_preprocessing
 IS
 l_transaction_type VARCHAR2 (10);
 l_wo_exists NUMBER;
 l_wocls_exists  NUMBER;
 l_wo_exists_in_stg NUMBER;
 l_error_code NUMBER := xx_emf_cn_pkg.cn_success;
 l_child_wo_qty NUMBER;
 l_current_parent_wo_qty NUMBER;
 l_new_parent_wo_qty NUMBER;
 l_parent_wo_end_date DATE;
 l_p_wo_cancelled NUMBER;
 l_p_wo_cancelled_in_stg NUMBER;
 l_released VARCHAR2 (60) := 'Released';
 l_cancelled VARCHAR2 (60) := 'Cancelled';
 l_wo_hdr_tbl VARCHAR2 (60) := 'FTL_WO_HDR';
 l_header VARCHAR2 (20) := 'HEADER';

 CURSOR cr_mark_wo_hdr
 IS
 SELECT *
	FROM ftl_wo_hdr
 WHERE org_code = g_org_code
	AND NVL (int_process_code, xx_emf_cn_pkg.cn_in_prog) = xx_emf_cn_pkg.cn_in_prog
	--AND WORK_ORDER_STATUS = 'Released'
	AND WORK_ORDER_STATUS NOT IN ( 'Closed', 'Cancelled', 'Complete')
 ORDER BY TIMESTAMP;

 CURSOR cr_mark_wo_clse
 IS
 SELECT *
	FROM ftl_wo_hdr
 WHERE org_code = g_org_code
	AND NVL (int_process_code, xx_emf_cn_pkg.cn_in_prog) = xx_emf_cn_pkg.cn_in_prog
	AND WORK_ORDER_STATUS IN ( 'Closed', 'Cancelled', 'Complete')
 ORDER BY TIMESTAMP;

 BEGIN
 FOR hdr_preprocess_rec IN cr_mark_wo_hdr
 LOOP
 l_transaction_type := NULL;
 l_wo_exists := NULL;
 l_wo_exists_in_stg := NULL;
 
 -- Change for RITM000004445
 BEGIN
	UPDATE FTL_WO_HDR 
		SET INT_PROCESS_CODE = 'ERROR', INT_ERROR_CODE= '0', INT_ERROR_MESSAGE='Target Quantity cannot be 0'
		WHERE NVL(TARGET_COMPLETION_QUANTITY, 0) = 0
		AND org_code = g_org_code
		AND NVL (int_process_code, xx_emf_cn_pkg.cn_in_prog) = xx_emf_cn_pkg.cn_in_prog
		AND WORK_ORDER_STATUS NOT IN ( 'Closed', 'Cancelled', 'Complete')
		;
	COMMIT;
 EXCEPTION 
  WHEN OTHERS THEN
  fnd_file.put_line (fnd_file.LOG, 'Exception in update for WO with target quantity as zero ');
 END;
 -- End of Change RITM000004445

 BEGIN
 SELECT COUNT (1)
	INTO l_wo_exists
 FROM wip_entities we, mtl_parameters mp
	WHERE we.wip_entity_name = hdr_preprocess_rec.work_order_number
	AND we.organization_id = mp.organization_id
	AND mp.organization_code = hdr_preprocess_rec.org_code
	AND we.entity_type = 1;
 EXCEPTION 
  WHEN OTHERS THEN
  fnd_file.put_line (fnd_file.LOG, 'Exception in l_wo_exists ');
  l_wo_exists := 0;
 END;

 IF l_wo_exists > 0
 THEN
 l_transaction_type := g_update;

 ELSE
 BEGIN

 /*SELECT COUNT (*)
	INTO l_wo_exists_in_stg
 FROM ftl_wo_hdr
 WHERE org_code = hdr_preprocess_rec.org_code
	AND work_order_number = hdr_preprocess_rec.work_order_number
	AND NVL(INT_ERROR_MESSAGE, 'XX') <> 'DUPLICATE' -- Added condition to avoid records stucking in PREPROCESS process code as part of Jira AMSEBS-1317
	AND TIMESTAMP < hdr_preprocess_rec.TIMESTAMP;
	*/
SELECT COUNT (1)
	INTO l_wo_exists_in_stg
 FROM wip_job_schedule_interface
 WHERE ORGANIZATION_CODE = hdr_preprocess_rec.org_code
	AND JOB_NAME = hdr_preprocess_rec.work_order_number
	;
 EXCEPTION 
  WHEN OTHERS THEN
  fnd_file.put_line (fnd_file.LOG, 'Exception in l_wo_exists_in_stg ');
  l_wo_exists_in_stg := 0;
 END;

 IF l_wo_exists_in_stg > 0
 THEN
 l_transaction_type := g_update;
  ELSE
 l_transaction_type := g_create;
 END IF;
 END IF;

 update_transaction_type (l_header,
 hdr_preprocess_rec.org_code,
 hdr_preprocess_rec.work_order_number,
 hdr_preprocess_rec.TIMESTAMP,
 hdr_preprocess_rec.record_id,
 l_transaction_type
 );
 END LOOP;

FOR hdr_preprocess_clse IN cr_mark_wo_clse
 LOOP
 l_transaction_type := NULL;
 l_wocls_exists := NULL;

l_transaction_type := g_update;

 update_transaction_type (l_header,
 hdr_preprocess_clse.org_code,
 hdr_preprocess_clse.work_order_number,
 hdr_preprocess_clse.TIMESTAMP,
 hdr_preprocess_clse.record_id,
 l_transaction_type
 );
 END LOOP;

--------------------------(Data validations)-----------------------------------------------------
 FOR rec_hdr IN cr_mark_wo_hdr
 LOOP
 BEGIN
 l_error_code := xx_emf_cn_pkg.cn_success;
--Commented below as part of MFG 2.1
 /*IF rec_hdr.int_transaction_type = g_create
 THEN
	l_error_code :=
	validate_wip_job (rec_hdr.record_id,
	rec_hdr.work_order_number,
	rec_hdr.org_code
	);
 END IF;
*/
/* IF rec_hdr.parent_wo_number IS NOT NULL
 THEN
	l_error_code :=
	validate_parent_wip_job (rec_hdr.record_id,
	rec_hdr.parent_wo_number,
	rec_hdr.org_code
	);
 END IF;
 */

 l_error_code := validate_wo_item (rec_hdr.record_id,
					rec_hdr.assembly_item_code,
					rec_hdr.org_code
					);
 l_error_code := validate_org (rec_hdr.record_id, rec_hdr.org_code);

 IF rec_hdr.wip_accounting_class IS NOT NULL
 THEN
	l_error_code :=
	validate_acct_class (rec_hdr.record_id,
	rec_hdr.wip_accounting_class,
	rec_hdr.org_code
	);
 END IF;

 IF rec_hdr.work_order_status IS NOT NULL
 THEN
	l_error_code :=
	validate_status_type (rec_hdr.record_id,
	rec_hdr.work_order_status
	);
 END IF;


 mark_records_complete_prc (rec_hdr.record_id,
 l_error_code,
 g_preprocess,
 l_wo_hdr_tbl
 );


 xx_emf_pkg.propagate_error (l_error_code);
 --End of MFG 2.1 changes
 EXCEPTION
 -- If HIGH error then it will be propagated to the next level
 -- IF the process has to continue maintain it as a medium severity
 WHEN xx_emf_pkg.g_e_rec_error
 THEN
 fnd_file.put_line (fnd_file.LOG, ' In Exception 1:' || SQLERRM);
 xx_emf_pkg.write_log (xx_emf_cn_pkg.cn_high, xx_emf_cn_pkg.cn_rec_err );
 WHEN xx_emf_pkg.g_e_prc_error
 THEN
	fnd_file.put_line (fnd_file.LOG,
	' In Exception 2:' || SQLERRM);
	xx_emf_pkg.write_log
	(xx_emf_cn_pkg.cn_high,
	'Process Level Error in Data Validations'
	);
	raise_application_error (-20199, xx_emf_cn_pkg.cn_prc_err);
 WHEN OTHERS
 THEN
	fnd_file.put_line (fnd_file.LOG, ' In Exception:' || SQLERRM);
	xx_emf_pkg.error
	(p_severity => xx_emf_cn_pkg.cn_medium,
	p_category => xx_emf_cn_pkg.cn_tech_error,
	p_error_text => xx_emf_cn_pkg.cn_exp_unhand,
	p_record_identifier_1 => rec_hdr.record_id,
	p_record_identifier_3 => 'Stage :Data Validation'
	);
 END;
 END LOOP;

 --Added below as part of MFG 2.1
 --IF l_error_code = '2' THEN
 BEGIN
	UPDATE FTL_WO_HDR 
		SET INT_PROCESS_CODE = 'ERROR', INT_ERROR_MESSAGE = 'WO validations item/accounting class/wo status not valid'
	WHERE org_code = g_org_code
	AND REQUEST_ID = fnd_global.CONC_REQUEST_ID
	AND INT_ERROR_CODE = '2'
	;
	COMMIT;
 EXCEPTION WHEN OTHERS THEN
	fnd_file.put_line (fnd_file.LOG, ' Exception in wo_hdr_preprocessing update error stmt:' 
						|| SQLERRM );
 END;
 --END IF;

 END wo_hdr_preprocessing;

----------------------------------------------------------------------
/*
Procedure Name: wo_bom_preprocessing
Authors name: Manisha Mohanty
Date written: 25-Oct-2013
RICEW Object id: WIP_I-196
Description: Set Conversion Environment
Program Style: Subordinate
Change History:
Date Issue# Name Remarks
----------- ------- ------------------ ------------------------------
25-Oct-2013 1.0 Manisha Mohanty Initial development.
*/
----------------------------------------------------------------------
 PROCEDURE wo_bom_preprocessing
 IS
 l_transaction_type VARCHAR2 (10);
 l_error_code NUMBER := xx_emf_cn_pkg.cn_success;
 l_wo_exists NUMBER;
 l_wo_exists_in_stg NUMBER;
 l_wo_bom_tbl VARCHAR2 (60) := 'FTL_WO_BOM';
 l_bom VARCHAR2 (20) := 'BOM';
 l_entity_id NUMBER := 0 ;
 l_item_id 	NUMBER := 0 ;
 l_org_id  NUMBER := 0;

 CURSOR cr_mark_wo_bom
 IS
 SELECT *
	FROM ftl_wo_bom
 WHERE org_code = g_org_code
	AND NVL (int_process_code, xx_emf_cn_pkg.cn_in_prog) =
	xx_emf_cn_pkg.cn_in_prog
 ORDER BY TIMESTAMP;

 TYPE p_tbl_wo_bom_preprocess IS TABLE OF cr_mark_wo_bom%ROWTYPE INDEX BY BINARY_INTEGER; 
 l_tbl_wo_bom_preprocess       p_tbl_wo_bom_preprocess;

 BEGIN
 OPEN cr_mark_wo_bom;
   FETCH cr_mark_wo_bom BULK COLLECT INTO l_tbl_wo_bom_preprocess LIMIT g_bulk_col_lim;

 IF (l_tbl_wo_bom_preprocess.COUNT >0 ) THEN

fnd_file.put_line (fnd_file.LOG, to_char(SYSDATE, 'DD-MON-YYYY HH:MI:SS') || ' l_tbl_wo_bom_preprocess.COUNT : ' || l_tbl_wo_bom_preprocess.COUNT);

FOR i IN 1.. l_tbl_wo_bom_preprocess.count    
 LOOP
 l_transaction_type := NULL;

 l_entity_id := 0 ;
 l_item_id 	:= 0 ;
 l_org_id  := 0;

 SELECT COUNT (wro.inventory_item_id) 
	INTO l_wo_exists
 FROM 	wip_entities we,
		wip_discrete_jobs wdj,
		wip_requirement_operations wro,
		mtl_parameters mp,
		mtl_system_items_b msib 
 WHERE we.wip_entity_name = l_tbl_wo_bom_preprocess(i).work_order_number
	AND we.organization_id = mp.organization_id
	AND mp.organization_code = l_tbl_wo_bom_preprocess(i).org_code
	AND we.wip_entity_id = wdj.wip_entity_id
	AND we.organization_id = wdj.organization_id
	AND we.wip_entity_id = wro.wip_entity_id
	AND we.organization_id = wro.organization_id
	AND msib.segment1 = l_tbl_wo_bom_preprocess(i).component_item_code
	AND msib.inventory_item_id = wro.inventory_item_id 
	AND msib.organization_id = wro.organization_id 
 ;

 IF l_wo_exists > 0
 THEN
	l_transaction_type := g_update;
 ELSE

	/*SELECT COUNT (*)
		INTO l_wo_exists_in_stg
	FROM ftl_wo_bom
 WHERE org_code = g_org_code
	AND org_code = l_tbl_wo_bom_preprocess(i).org_code
	AND work_order_number = l_tbl_wo_bom_preprocess(i).work_order_number
	AND component_item_code = l_tbl_wo_bom_preprocess(i).component_item_code
	AND TIMESTAMP < l_tbl_wo_bom_preprocess(i).TIMESTAMP;*/

	BEGIN
		SELECT we.wip_entity_id
			INTO l_entity_id
		FROM 	wip_entities we,
				wip_discrete_jobs wdj,
				mtl_parameters mp 
		WHERE we.wip_entity_name = l_tbl_wo_bom_preprocess(i).work_order_number
			AND we.organization_id = mp.organization_id
			AND mp.organization_code = l_tbl_wo_bom_preprocess(i).org_code
			AND we.wip_entity_id = wdj.wip_entity_id
			AND we.organization_id = wdj.organization_id
		;
	EXCEPTION WHEN OTHERS THEN
		l_entity_id := 0 ;
		fnd_file.put_line (fnd_file.LOG, ' Exception in wo preprocessing l_entity_id : ' || l_entity_id );
	END;


	BEGIN
		SELECT msib.inventory_item_id , ood.organization_id
			INTO l_item_id,  l_org_id
		FROM mtl_system_items_b msib, org_organization_definitions ood 
		WHERE msib.segment1 = l_tbl_wo_bom_preprocess(i).component_item_code
		AND msib.organization_id =  ood.organization_id
		AND ood.organization_code = l_tbl_wo_bom_preprocess(i).org_code
		;
	EXCEPTION WHEN OTHERS THEN
		l_item_id := 0 ;
		l_org_id := 0;
		fnd_file.put_line (fnd_file.LOG, ' Exception in wo preprocessing l_item_id : ' || l_item_id );
	END;

	BEGIN
		SELECT COUNT (1)
			INTO l_wo_exists_in_stg
		FROM wip_job_dtls_interface
			WHERE ORGANIZATION_ID = l_org_id
		AND WIP_ENTITY_ID = l_entity_id
		AND INVENTORY_ITEM_ID_OLD = l_item_id
		;
	EXCEPTION WHEN OTHERS THEN
		l_wo_exists_in_stg := 0 ;
		fnd_file.put_line (fnd_file.LOG, ' Exception in l_wo_exists_in_stg : ' || l_wo_exists_in_stg );
	END;

 IF l_wo_exists_in_stg > 0
 THEN
	l_transaction_type := g_update;
 ELSE
	l_transaction_type := g_create;
 END IF;
 END IF;

 update_transaction_type (l_bom,
 l_tbl_wo_bom_preprocess(i).org_code,
 l_tbl_wo_bom_preprocess(i).work_order_number,
 l_tbl_wo_bom_preprocess(i).TIMESTAMP,
 l_tbl_wo_bom_preprocess(i).record_id,
 l_transaction_type
 );

 mark_records_complete_prc (l_tbl_wo_bom_preprocess(i).record_id,
 l_error_code,
 g_preprocess,
 l_wo_bom_tbl
 );
 END LOOP;
 l_tbl_wo_bom_preprocess.delete ;
 END IF;
 CLOSE cr_mark_wo_bom;
 END wo_bom_preprocessing;

----------------------------------------------------------------------
/*
Procedure Name: material_txn_preprocess
Authors name: Manisha Mohanty
Date written: 25-Oct-2013
RICEW Object id: WIP_I-196
Description: Set Conversion Environment
Program Style: Subordinate
Change History:
Date Issue# Name Remarks
----------- ------- ------------------ ------------------------------
25-Oct-2013 1.0 Manisha Mohanty Initial development.
*/
----------------------------------------------------------------------
 PROCEDURE material_txn_preprocess
 IS
 l_transaction_type VARCHAR2 (10);
 l_error_code NUMBER := xx_emf_cn_pkg.cn_success;
 l_mat VARCHAR2 (10) := 'MAT';
 l_mat_txn_tbl VARCHAR2 (60) := 'FTL_MAT_TXN';

 CURSOR cr_mark_mat_txn
 IS
 SELECT *
 FROM ftl_mat_txn
 WHERE org_code = g_org_code
 AND NVL (int_process_code, xx_emf_cn_pkg.cn_in_prog) = xx_emf_cn_pkg.cn_in_prog
 ORDER BY TIMESTAMP;

 TYPE p_tbl_mat_txn_preprocess IS TABLE OF cr_mark_mat_txn%ROWTYPE INDEX BY BINARY_INTEGER; 
 l_tbl_mat_txn_preprocess       p_tbl_mat_txn_preprocess;

 BEGIN

  OPEN cr_mark_mat_txn;
   FETCH cr_mark_mat_txn BULK COLLECT INTO l_tbl_mat_txn_preprocess LIMIT g_bulk_col_lim;

IF (l_tbl_mat_txn_preprocess.COUNT >0 ) THEN

fnd_file.put_line (fnd_file.LOG,to_char(SYSDATE, 'DD-MON-YYYY HH:MI:SS') || ' l_tbl_mat_txn_preprocess.COUNT :' || l_tbl_mat_txn_preprocess.COUNT	);

FOR i IN 1.. l_tbl_mat_txn_preprocess.count    
 LOOP
 l_transaction_type := NULL;
 l_error_code := xx_emf_cn_pkg.cn_success; --MFG 2.1 bug fix

 IF ( l_tbl_mat_txn_preprocess(i).transaction_issue_quantity IS NOT NULL 
     AND l_tbl_mat_txn_preprocess(i).issue_subinventory IS NOT NULL )
 THEN
	l_transaction_type := g_issue;
 ELSE
	l_transaction_type := g_return;
 END IF;


 update_transaction_type (l_mat,
 l_tbl_mat_txn_preprocess(i).org_code,
 l_tbl_mat_txn_preprocess(i).work_order_number,
 l_tbl_mat_txn_preprocess(i).TIMESTAMP,
 l_tbl_mat_txn_preprocess(i).record_id,
 l_transaction_type
 );

 mark_records_complete_prc (l_tbl_mat_txn_preprocess(i).record_id, --mat_txn_rec.record_id,
 l_error_code,
 g_preprocess,
 l_mat_txn_tbl
 );
 END LOOP;
 l_tbl_mat_txn_preprocess.delete;
END IF; 
 CLOSE cr_mark_mat_txn;

END material_txn_preprocess;

----------------------------------------------------------------------
/*
Procedure Name: process_material_transaction
Authors name: Manisha Mohanty
Date written: 25-Oct-2013
RICEW Object id: WIP_I-196
Description: Set Conversion Environment
Program Style: Subordinate
Change History:
Date Issue# Name Remarks
----------- ------- ------------------ ------------------------------
25-Oct-2013 1.0 Manisha Mohanty Initial development.
*/
----------------------------------------------------------------------
 PROCEDURE process_material_transaction
 IS
 l_error_code NUMBER := xx_emf_cn_pkg.cn_success;
 l_module_name VARCHAR2 (60) := 'process_material_transaction';
 l_last_update_date DATE := SYSDATE;
 l_last_updated_by NUMBER := fnd_global.user_id;
 l_last_update_login NUMBER := fnd_global.user_id;
 l_org_id NUMBER;
 l_inv_item_id NUMBER;
 l_mat_txn_tbl VARCHAR2 (60) := 'FTL_MAT_TXN';
 l_errm               VARCHAR2(1000); 	-- Added for Timezone modification on transaction_date SR00453359
 l_timezone           VARCHAR2(50); 	-- Added for Timezone modification on transaction_date SR00453359
 l_transaction_Date   DATE;         	-- Added for Timezone modification on transaction_date SR00453359
 l_opr_seq      NUMBER:= NULL;
 --l_tmp_entity_id NUMBER := 0 ;

 CURSOR cr_mat_txn (cp_process_status VARCHAR2)
 IS
 SELECT *
 FROM ftl_mat_txn
 WHERE request_id = xx_emf_pkg.g_request_id
 AND org_code = g_org_code
 AND int_process_code = cp_process_status
 AND NVL (int_error_code, xx_emf_cn_pkg.cn_success) IN
 (xx_emf_cn_pkg.cn_success, xx_emf_cn_pkg.cn_rec_warn)
 ORDER BY TIMESTAMP;

 TYPE p_tbl_mat_txn IS TABLE OF cr_mat_txn%ROWTYPE INDEX BY BINARY_INTEGER; 
 l_tbl_mat_txn       p_tbl_mat_txn;


 PRAGMA AUTONOMOUS_TRANSACTION;
 BEGIN


  OPEN cr_mat_txn(g_preprocess);
   FETCH cr_mat_txn BULK COLLECT INTO l_tbl_mat_txn LIMIT g_bulk_col_lim;

IF (l_tbl_mat_txn.COUNT > 0) THEN

	fnd_file.put_line (fnd_file.LOG,to_char(SYSDATE, 'DD-MON-YYYY HH:MI:SS') ||' l_tbl_mat_txn.COUNT :' || l_tbl_mat_txn.COUNT	);



FOR i IN 1.. l_tbl_mat_txn.count
 LOOP
 BEGIN
 l_org_id := NULL;
 l_inv_item_id := NULL;
 l_error_code := xx_emf_cn_pkg.cn_success; -- Added for MFG 2.1 bug fix
BEGIN -- Added for MFG 2.1 bug fix
 SELECT msib.organization_id, msib.inventory_item_id
	INTO l_org_id, l_inv_item_id
 FROM mtl_system_items_b msib, mtl_parameters mp
	WHERE msib.organization_id = mp.organization_id
	AND mp.organization_code = l_tbl_mat_txn(i).org_code 
	AND msib.segment1 = l_tbl_mat_txn(i).component_item_code 
 ;
 -- Added for MFG 2.1 bug fix
 EXCEPTION WHEN OTHERS THEN
    l_error_code := xx_emf_cn_pkg.cn_rec_err;
    fnd_file.put_line (fnd_file.log, 'Exception in material txn get item id for ' || l_tbl_mat_txn(i).work_order_number );
 END;
-- End of MFG 2.1 bug fix



BEGIN -- Added for MFG 2.1 bug fix

SELECT nvl(min(wro.OPERATION_SEQ_NUM ), 0)
    INTO l_opr_seq
FROM wip_entities we, WIP_REQUIREMENT_OPERATIONS wro 
    WHERE we.WIP_ENTITY_ID = wro.WIP_ENTITY_ID 
    AND we.WIP_ENTITY_NAME= l_tbl_mat_txn(i).work_order_number
    AND wro.INVENTORY_ITEM_ID = l_inv_item_id
    AND we.organization_id = wro.organization_id
;
fnd_file.put_line (fnd_file.log, 'l_opr_seq with BOM item : ' || l_opr_seq) ;
 -- Added for MFG 2.1 bug fix
 EXCEPTION 
 WHEN NO_DATA_FOUND THEN
 fnd_file.put_line (fnd_file.log, 'Exception no data found in material txn get Opr seq for ' || l_tbl_mat_txn(i).work_order_number || ' '|| l_opr_seq );
    BEGIN
		SELECT min(wro.OPERATION_SEQ_NUM )
			INTO l_opr_seq
		FROM wip_entities we, WIP_REQUIREMENT_OPERATIONS wro 
			WHERE we.WIP_ENTITY_ID = wro.WIP_ENTITY_ID 
			AND we.WIP_ENTITY_NAME= l_tbl_mat_txn(i).work_order_number
			AND we.organization_id = wro.organization_id
		;
		fnd_file.put_line (fnd_file.log, 'l_opr_seq with BOM item : ' || l_opr_seq) ;
	EXCEPTION WHEN OTHERS THEN
		fnd_file.put_line (fnd_file.log, 'Exception in getting Opr seq for WO' || l_tbl_mat_txn(i).work_order_number  );
	END;

 WHEN OTHERS THEN
	 l_error_code := xx_emf_cn_pkg.cn_rec_err;
     fnd_file.put_line (fnd_file.log, 'Exception in material txn get Opr seq for ' || l_tbl_mat_txn(i).work_order_number || ' '|| l_opr_seq );
 END;

 IF l_opr_seq = 0 THEN
	BEGIN
		SELECT min(wro.OPERATION_SEQ_NUM )
			INTO l_opr_seq
		FROM wip_entities we, WIP_REQUIREMENT_OPERATIONS wro 
			WHERE we.WIP_ENTITY_ID = wro.WIP_ENTITY_ID 
			AND we.WIP_ENTITY_NAME= l_tbl_mat_txn(i).work_order_number
			AND we.organization_id = wro.organization_id
		;
		fnd_file.put_line (fnd_file.log, 'l_opr_seq with BOM item : ' || l_opr_seq) ;
	EXCEPTION WHEN OTHERS THEN
		fnd_file.put_line (fnd_file.log, 'Exception in getting Opr seq for WO' || l_tbl_mat_txn(i).work_order_number  );
	END;
 END IF;
-- End of MFG 2.1 bug fix


 BEGIN
 /*l_tmp_entity_id := NULL; 

	SELECT WE.WIP_ENTITY_ID 
		INTO l_tmp_entity_id
		FROM wip_discrete_jobs WDJ, 
			 wip_entities WE , 
			 WIP_REQUIREMENT_OPERATIONS WRO , 
			 mtl_system_items_b MSIB,
			 mtl_parameters mp
	WHERE  we.wip_entity_id = wdj.wip_entity_id
	AND we.organization_id = wdj.organization_id 
    and WRO.wip_entity_id = wdj.wip_entity_id
    and WRO.organization_id = wdj.organization_id
    AND WRO.INVENTORY_ITEM_ID = MSIB.INVENTORY_ITEM_ID
    AND  MSIB.organization_id = wdj.organization_id
	AND msib.organization_id = mp.organization_id
	AND mp.organization_id = wdj.organization_id
	AND we.wip_entity_name = l_tbl_mat_txn(i).work_order_number
	AND msib.segment1 = l_tbl_mat_txn(i).component_item_code 
	AND mp.organization_code = l_tbl_mat_txn(i).org_code
    ; 
  */

  --Added for Timezone modification on transaction_date SR00453359
      fnd_file.put_line (fnd_file.log, 'Timezone modification Starts' || ' ' ||TO_CHAR(sysdate, 'dd-mon-yyyy hh24:mi:ss'));
      BEGIN
        SELECT timezone_code
        INTO l_timezone
        FROM hr_locations_v hlv,
          hr_organization_units_v houv
        WHERE houv.organization_id = l_org_id
        AND houv.location_id       = hlv.location_id;
      EXCEPTION
      WHEN OTHERS THEN
        l_errm := SQLERRM;
        fnd_file.put_line (fnd_file.log, 'Failed in fetching Timezone for Org: '|| l_org_id || l_errm);
      END;
      IF l_timezone        IS NULL THEN
        l_transaction_Date := l_tbl_mat_txn(i).transaction_date;
      ELSE
        -- l_transaction_Date := NEW_TIME(TO_DATE(l_array_hdr1(z).transaction_date, 'DD-MON-YYYY HH24:MI:SS'), l_timezone, 'CST');
        l_transaction_Date := CAST (from_tz(CAST (l_tbl_mat_txn(i).transaction_date AS TIMESTAMP),l_timezone) at TIME zone 'America/Chicago' AS DATE);
      END IF;
      fnd_file.put_line (fnd_file.log, 'Timezone modification Ends' || ' ' ||TO_CHAR(sysdate, 'dd-mon-yyyy hh24:mi:ss'));
      fnd_file.put_line (fnd_file.log, 'Transaction Date := ' ||TO_CHAR(l_transaction_Date,'dd-mon-yyyy hh24:mi:ss'));
      --End of Timezone modification on transaction_date SR00453359

 IF l_tbl_mat_txn(i).int_transaction_type = g_issue
 THEN
	INSERT INTO apps.mtl_transactions_interface
				(transaction_uom,
				transaction_date,
				source_code, 
				process_flag,
				last_update_date, 
				last_updated_by,
				creation_date, 
				created_by,
				inventory_item_id, 
				subinventory_code,
				organization_id,
				transaction_quantity,
				transaction_type_id,
				TRANSACTION_REFERENCE , 
				transaction_source_name, 
				source_header_id,
				source_line_id, 
				transaction_mode
				,OPERATION_SEQ_NUM       --Commented For MFG 2.1 change 
				)
				VALUES (l_tbl_mat_txn(i).component_item_uom,
				--SYSDATE,		-- Commented for SR00453359
				l_transaction_Date,  -- Modified for Timezone modification on transaction_date SR00453359
				'Job or Schedule', 								--source_code
				1, 												--process_flag
				l_last_update_date, 
				l_last_updated_by,
				l_last_update_date, 
				l_last_updated_by,
				l_inv_item_id,									/* --inventory item id for the component */
				l_tbl_mat_txn(i).issue_subinventory, 			--rec_mat_txn.issue_subinventory
				l_org_id, 										/* --organization id*/
				- (l_tbl_mat_txn(i).transaction_issue_quantity),				/* --transaction quantity negative for issue */
				35, 											/*transaction type id = 35 WIPIssue*/
				l_tbl_mat_txn(i).SOA_TRANSMISSION_ID, 
				l_tbl_mat_txn(i).work_order_number,				/* transaction_source_name = WO# */
				99,
				99, 
				3
				,l_opr_seq  --Commented For MFG 2.1 change 
				);
 ELSE
	INSERT INTO apps.mtl_transactions_interface
				(transaction_uom,
				transaction_date,
				source_code, 
				process_flag,
				last_update_date, 
				last_updated_by,
				creation_date, 
				created_by,
				inventory_item_id, 
				subinventory_code,
				organization_id,
				transaction_quantity,
				transaction_type_id,
				TRANSACTION_REFERENCE , 
				transaction_source_name, source_header_id,
				source_line_id, transaction_mode
				,OPERATION_SEQ_NUM       --Commented For MFG 2.1 change 
				)
				VALUES (l_tbl_mat_txn(i).component_item_uom,
				--SYSDATE,			 --Commented for SR00453359
				l_transaction_date,  --Modified for Timezone modification on transaction_date SR00453359
				'Job or Schedule', 									--source_code
				1, 													--process_flag
				l_last_update_date, 
				l_last_updated_by,
				l_last_update_date, 
				l_last_updated_by,
				l_inv_item_id,										/* --inventory item id for the component */
				l_tbl_mat_txn(i).return_subinventory, 				/* --To subinventory code */
				l_org_id, 											/* --organization id*/
				l_tbl_mat_txn(i).transaction_return_quantity, 		/* --transaction quantity positive for return */
				43, 												/*transaction type id = 43 WIP returns*/
				l_tbl_mat_txn(i).SOA_TRANSMISSION_ID,
				l_tbl_mat_txn(i).work_order_number, 				/* transaction_source_name = WO# */
				99,
				99, 
				3
				,l_opr_seq    --Commented For MFG 2.1 change 
				);
 END IF;

 EXCEPTION WHEN OTHERS THEN
	/*l_tmp_entity_id := null;
	IF l_tmp_entity_id IS NULL THEN
		mark_records_complete_prc (l_tbl_mat_txn(i).record_id,
							'2',
							'ERROR',
							l_mat_txn_tbl
							);
	END IF;*/
	fnd_file.put_line (fnd_file.LOG,
	' Exception in l_tmp_entity_id mat txn :' || SQLERRM );
 END;
 EXCEPTION
 WHEN OTHERS
 THEN
	l_error_code := xx_emf_cn_pkg.cn_rec_err;
	fnd_file.put_line
	(fnd_file.LOG,
	' Error while inserting data into MTL_MATERIAL_TRANSACTIONS for :' || l_tbl_mat_txn(i).work_order_number || ' ' 
	|| SQLERRM
	);
 END;
 mark_records_complete_prc (l_tbl_mat_txn(i).record_id, --rec_mat_txn.record_id,
							l_error_code,
							g_interface,
							l_mat_txn_tbl
							);
    l_error_code := xx_emf_cn_pkg.cn_success; -- Added for MFG 2.1 bug fix
 END LOOP;

l_tbl_mat_txn.delete;
END IF; 

CLOSE cr_mat_txn; 

 COMMIT;
 EXCEPTION
 WHEN OTHERS
 THEN
	fnd_file.put_line (fnd_file.LOG,
	l_module_name || ' Message:' || SQLERRM
	);
	xx_emf_pkg.write_log
	(xx_emf_cn_pkg.cn_low,
	'error in procedure process_material_transaction'
	);
 END;

----------------------------------------------------------------------
/*
Procedure Name: main
Authors name: Manisha Mohanty
Date written: 25-Oct-2013
RICEW Object id: WIP_I-196
Description: Set Conversion Environment
Program Style: Subordinate
Change History:
Date Issue# Name Remarks
----------- ------- ------------------ ------------------------------
25-Oct-2013 1.0 Manisha Mohanty Initial development.
*/
----------------------------------------------------------------------
 PROCEDURE main (
 errbuf OUT NOCOPY VARCHAR2,
 retcode OUT NOCOPY VARCHAR2,
 p_org_code IN VARCHAR2
 )
 IS
 l_module_name VARCHAR2 (60) := 'main';
 l_error_code NUMBER := xx_emf_cn_pkg.cn_success;
 l_group_cnt NUMBER := 0 ;


 BEGIN
 fnd_file.put_line (fnd_file.LOG,
 '--------------------------------------------------'
 );
 fnd_file.put_line (fnd_file.LOG,
 'Main procedure for Shopfloor to WIP upload Begin'
 );
 fnd_file.put_line (fnd_file.LOG,
 '--------------------------------------------------'
 );
 retcode := xx_emf_cn_pkg.cn_success;
 -- Set environment for EMF (Error Management Framework)
 -- If you want the process to continue even after the emf env not being set
 fnd_file.put_line (fnd_file.LOG,
 '--------------------------------------------------'
 );
 fnd_file.put_line (fnd_file.LOG, 'Set Environment');
 fnd_file.put_line (fnd_file.LOG,
 '--------------------------------------------------'
 );
 set_cnv_env_prc (p_org_code);
 -- include all the parameters to the conversion main here
 -- as medium log messages
 fnd_file.put_line (fnd_file.LOG,
 'Starting main process'
 || CHR (13)
 || 'with the following parameters'
 );
 fnd_file.put_line (fnd_file.LOG,

 'Main:Param - p_org_code ' || p_org_code
 );
 -- Call procedure to update records with the current request_id
 -- So that we can process only those records
 -- This gives a better handling of re startability
 mark_record_for_processing_prc;
 fnd_file.put_line (fnd_file.LOG,
 '--------------------------------------------------'
 );
 fnd_file.put_line (fnd_file.LOG, 'WO Header Preprocessing');
 fnd_file.put_line (fnd_file.LOG,
 '--------------------------------------------------'
 );
 IF g_wo_hdr_cnt > 0 THEN
 wo_hdr_preprocessing;
 END IF;
 fnd_file.put_line (fnd_file.LOG,
 '--------------------------------------------------'
 );
 fnd_file.put_line (fnd_file.LOG, 'Create WIP jobs');
 fnd_file.put_line (fnd_file.LOG,
 '--------------------------------------------------'
 );
 IF g_wo_hdr_cnt > 0 THEN
 create_wip_job;
 END IF;
 fnd_file.put_line (fnd_file.LOG,
 '--------------------------------------------------'
 );
 fnd_file.put_line (fnd_file.LOG, 'WO Component Preprocessing');
 fnd_file.put_line (fnd_file.LOG,
 '--------------------------------------------------'
 );
 IF g_wo_bom_cnt > 0 THEN
 wo_bom_preprocessing;
 END IF;
 fnd_file.put_line (fnd_file.LOG,
 '--------------------------------------------------'
 );
 fnd_file.put_line (fnd_file.LOG, 'Create WIP job components');
 fnd_file.put_line (fnd_file.LOG,
 '--------------------------------------------------'
 );
 IF g_wo_bom_cnt > 0 THEN
 create_wip_component;
 END IF;
 fnd_file.put_line (fnd_file.LOG,
 '--------------------------------------------------'
 );
 fnd_file.put_line (fnd_file.LOG, 'Update WIP jobs and WIP components');
 fnd_file.put_line (fnd_file.LOG,
 '--------------------------------------------------'
 );
 IF g_wo_hdr_cnt > 0 THEN
	update_wip_job;
 END IF;
 IF g_wo_bom_cnt > 0 THEN
	update_wip_comp;
 END IF;


 fnd_file.put_line (fnd_file.LOG,
 '--------------------------------------------------'
 );
 fnd_file.put_line (fnd_file.LOG, 'Process Material transaction');
 fnd_file.put_line (fnd_file.LOG,
 '--------------------------------------------------'
 );
 IF g_mat_txn_cnt > 0 THEN
	material_txn_preprocess;
	process_material_transaction;
 END IF;

 EXCEPTION
 WHEN xx_emf_pkg.g_e_env_not_set
 THEN
	xx_emf_pkg.write_log (xx_emf_cn_pkg.cn_low, 'Environment not set.');
	fnd_file.put_line (fnd_file.output, xx_emf_pkg.cn_env_not_set);
	DBMS_OUTPUT.put_line (xx_emf_pkg.cn_env_not_set);
	retcode := xx_emf_cn_pkg.cn_rec_err;
	update_record_count_prc;
	xx_emf_pkg.create_report;
 WHEN xx_emf_pkg.g_e_rec_error
 THEN
	retcode := xx_emf_cn_pkg.cn_rec_err;
	fnd_file.put_line (fnd_file.LOG,
	l_module_name || ' Message:' || SQLERRM
	);
	update_record_count_prc;
	xx_emf_pkg.create_report;
 WHEN xx_emf_pkg.g_e_prc_error
 THEN
	retcode := xx_emf_cn_pkg.cn_prc_err;
	fnd_file.put_line (fnd_file.LOG,
	l_module_name || ' Message:' || SQLERRM
	);
	update_record_count_prc;
	xx_emf_pkg.create_report;
 WHEN OTHERS
 THEN
	retcode := xx_emf_cn_pkg.cn_prc_err;
	fnd_file.put_line (fnd_file.LOG,
	l_module_name || ' Message:' || SQLERRM
	);
	update_record_count_prc;
	xx_emf_pkg.create_report;
 END main;
END ftl_wip_sf_to_wip_upload_pkg;
