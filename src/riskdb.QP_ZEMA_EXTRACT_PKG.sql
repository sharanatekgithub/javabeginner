CREATE OR REPLACE PACKAGE BODY RISKDB.qp_zema_extract_pkg AS

   -- Error Record
   PROCESS_LOG_REC RISKDB.PROCESS_LOG%ROWTYPE;
   g_minlogid          NUMBER := 0;



PROCEDURE Get_current_COBDATE (
  pcobdate OUT DATE 
 ,pout_rtn_code     OUT  NOCOPY NUMBER  
 ,pout_rtn_msg      OUT  NOCOPY VARCHAR2 
 )
 IS
 V_section VARCHAR2(100);
 BEGIN
 
    PROCESS_LOG_REC.STAGE        := 'Get_Current_COB_DATE';
    V_section := 'Get_Current_COB_DATE';
    
    SELECT MAX(DMV.M2M_COB_DATE) COB_DATE INTO pcobdate
    FROM   RD.DATES_DIM DMV
    WHERE  DMV.M2M_COB_DATE_SEQ = 0
    ;
    
    pout_rtn_code := c_success;
    pout_rtn_msg := ''; --Null means Success 
  
 EXCEPTION
 WHEN NO_DATA_FOUND THEN 
   pcobdate := TO_DATE('31-DEC-9999','DD-MON-YYYY');
   pout_rtn_code := c_failure;
   pout_rtn_msg := v_section||'-Current COB DATE NOT FOUND';
 WHEN OTHERS THEN 
   pcobdate := TO_DATE('31-DEC-9999','DD-MON-YYYY');
   pout_rtn_code := c_failure;
   pout_rtn_msg := v_section||'-'||SUBSTR(SQLERRM,1,200);
 END Get_current_COBDATE;

FUNCTION Get_Hours (      
                             pLocation          IN   VARCHAR2
                           , PHourType          IN   VARCHAR2
                           , pcontractYearMonth IN   NUMBER
                    ) 
RETURN NUMBER
IS
 vc_HOurType        VARCHAR2(30);
 vc_section         Varchar2(1000) := 'Get_Hours';
 v1_hours           PHOENIX.ISO_MONTHLY_PEAK_HOURS_MV.HOURS%TYPE;
 v_rtn_code         NUMBER;
 vc_rtn_msg          VARCHAR2(1000);    

                     
 BEGIN     
 
 IF PLOCATION IS NULL THEN RETURN NULL; END IF;
 
  vc_section :=  vc_section||'Location='||pLocation
  ||'HourType='|| PHourType; 
  
 IF pHourtype = 'N/A' THEN 
    vc_Hourtype := '7X24';
 ELSE
    vc_Hourtype := pHourType;
 END IF;
  
 
 IF Vc_HourType is NULL THEN 
   BEGIN  
    
     SELECT 
      MAX(m.HOUR_TYPE) INTO vc_Hourtype
      FROM 
      RISKDB.PRICE_CURVE_MAPPING_MV m
      where
      m.basis_Name = pLocation
      and active = 'Y'
      and rownum < 2;
      
   EXCEPTION
   WHEN NO_DATA_FOUND THEN 
     vc_HourType := NULL;
   WHEN OTHERS THEN 
     vc_HourType := NULL;
         
   END;
   
     vc_section :=  vc_section||'Derived Hr Type='||vc_HourType;
  
 END IF;
   
 --dbms_output.put_line('vc_Hourtype='||vc_Hourtype);
 
  vc_section :=  vc_section||' contractMOnth='||pcontractYearmonth;
  
 /*select month,hours, hour_type_name
 from PHOENIX.ISO_MONTHLY_PEAK_HOURS_MV mph,
 Riskdb.m2m_work_area mwa where
 mwa.contract_month = mph.month
 and mwa.peak_type = mph.hour_type_name;*/
 
 BEGIN
            Select mv.hours 
            INTO 
            v1_hours 
            from
            PHOENIX.ISOS iso,
            PHOENIX.ISO_MONTHLY_PEAK_HOURS_MV mv
            where
            ISO.id = MV.ISO_ID
            and MV.MONTH = pcontractYearmonth
            and iso.name = substr(
                       pLocation
                       , 1
                       , instr( pLocation
                               ,'-'
                               )-1
                       )
            and (Upper(MV.HOUR_TYPE_NAME)) =( upper( vc_Hourtype))
            ;
   
                      
 EXCEPTION
 WHEN NO_DATA_FOUND THEN 
 
 
   v1_hours := NULL;
  
   v_rtn_code := c_failure;
   vc_rtn_msg := vc_section   ;
   --dbms_output.put_line(pout_rtn_msg);
   
   DMR.PROCESS_LOG_PKG.WRITE_LOG(
                        P_PROCESS_NAME => PROCESS_LOG_REC.PROCESS_NAME,
                        P_STAGE=> PROCESS_LOG_REC.STAGE,
                        P_STATUS=>PROCESS_LOG_PKG.C_STATUS_ERRORS,
                        P_MESSAGE=>NVL( vc_rtn_msg, SQLERRM) 
                        );     
               
 WHEN TOO_MANY_ROWS THEN
   
   v1_hours := NULL;
 
   v_rtn_code := c_failure;
   vc_rtn_msg := vc_section   ;
   
    DMR.PROCESS_LOG_PKG.WRITE_LOG(
                        P_PROCESS_NAME => PROCESS_LOG_REC.PROCESS_NAME,
                        P_STAGE=> PROCESS_LOG_REC.STAGE,
                        P_STATUS=>PROCESS_LOG_PKG.C_STATUS_ERRORS,
                        P_MESSAGE=>NVL( vc_rtn_msg, SQLERRM) 
                        );
              
 WHEN OTHERS THEN 
  
   v1_hours := NULL;
                        
   v_rtn_code := c_failure;
   vc_rtn_msg :=  vc_section||'-'||SUBSTR(SQLERRM,1,200);
   
   DMR.PROCESS_LOG_PKG.WRITE_LOG(
                        P_PROCESS_NAME => PROCESS_LOG_REC.PROCESS_NAME,
                        P_STAGE=> PROCESS_LOG_REC.STAGE,
                        P_STATUS=>PROCESS_LOG_PKG.C_STATUS_ERRORS,
                        P_MESSAGE=>NVL( vc_rtn_msg, SQLERRM) 
                        );
                        
 END ;
 
 
 RETURN v1_hours;
 
 
 END Get_Hours;
 
  
FUNCTION Get_Monthly_Original_FAS_level (
 pLocation               IN VARCHAR2
, pCOMMODITY             IN VARCHAR2
 ,pcobdate              IN DATE
 ,pContractYearMonth    IN NUMBER
 ) RETURN NUMBER 
IS
vn_FASLevel         NUMBER;
vc_FASType          VARCHAR2(100);
vc_Location         VARCHAR2(1000);
vn_finalFAS         NUMBER;
vc_section          VARCHAR2(1000) := 'Get_FAS_level';
vn_rtn_code         NUMBER;
vc_rtn_msg          VARCHAR2(1000);

e_FAS_NOTFOUND         EXCEPTION;
   PRAGMA EXCEPTION_INIT( e_FAS_NOTFOUND , -20050);

BEGIN

IF PLOCATION IS NULL THEN RETURN NULL; END IF;
 
  BEGIN

        Select  distinct
         (
            CASE 
            WHEN pContractYearMonth between 
                    TO_NUMBER(TO_CHAR(pcobdate, 'YYYYMM')) AND 
                    TO_NUMBER(NVL(END_CM_1_CH , -1 ))   THEN FAS_LEVEL_1
            WHEN (
                 pContractYearMonth between 
                    TO_NUMBER(NVL(END_CM_1_CH, -1)) AND 
                    TO_NUMBER(NVL(END_CM_2_NCH , -1 )) 
                  ) AND 
                  --  level 2 end date needs to be gretaer than level 1 end date
                  -- otherwise , if both are null or both are equal or level2 < level 1 end date
                  --go to fas levl 3 check
                  (
                  TO_NUMBER(NVL(END_CM_2_NCH, -1)) >
                  TO_NUMBER(NVL(END_CM_1_CH , -1 ))
                  )    THEN FAS_LEVEL_2
            WHEN pcontractYearMonth between 
                    TO_NUMBER(NVL(END_CM_2_NCH , -1 )) AND 
                    TO_NUMBER(NVL(END_CM_3_NCH , -1 ))   THEN FAS_LEVEL_3 
            ELSE
              NULL
            END ) original_fas_level
         , 'Percent Value'   
        INTO 
        vn_FASlevel
        , vc_FASType
        from riskdb.FAS157_CAT f1
        where
        f1.cob_Date = ( select MAX(cob_Date) 
                        from riskdb.FAS157_CAT f2
                         where 
                         f2.cob_date <= pcobdate
                         and f2.active = 'Y' 
                      )
        and F1.active = 'Y'
        and f1.COmmodity = pcommodity
        and f1.basis_point = pLocation
        ;
        
      
         
    EXCEPTION
    WHEN NO_DATA_FOUND THEN 
   
    DBMS_OUTPUT.PUT_LINE ('FAS not found '
     ||'PLocation='||pLocation
     ||'pcommodity='||pcommodity
     );
     
      vn_FASlevel := NULL;
    END;

  RETURN vn_FASlevel;

END Get_Monthly_Original_FAS_level;
 
  
PROCEDURE Get_FAS_level (
 pOriginalLocation      IN VARCHAR2
, pOverrideLocation      IN VARCHAR2
 ,pcobdate              IN DATE
 ,pStartYearMonth       IN NUMBER
 ,PendYearMonth         IN NUMBER
 ,pUnderlyingCom        IN VARCHAR2
 ,pcurrentMonth         IN NUMBER
 ,ppricevol            IN VARCHAR2 
 ,pm2mData             IN OUT NOCOPY m2m_by_Loc_tab
 ,pFASLevel            OUT NUMBER
 ,pFASValue            OUT NUMBER 
,pout_rtn_code     OUT  NOCOPY NUMBER  
,pout_rtn_msg      OUT  NOCOPY VARCHAR2 
 ) 
IS
vn_FASLevel         NUMBER;
vc_FASType          VARCHAR2(100);
vc_Location         VARCHAR2(1000);
vc_section          VARCHAR2(1000) := 'Get_FAS_level';
vn_rtn_code         NUMBER;
vc_rtn_msg          VARCHAR2(1000);

e_FAS_NOTFOUND         EXCEPTION;
   PRAGMA EXCEPTION_INIT( e_FAS_NOTFOUND , -20050);

BEGIN
 

IF POverrideLocation is NOT NULL THEN 
 
  For FASrecs IN (
    WITH 
    distinctFAS as (
       SELECT m.OVERRIDE_ORIG_FAS_LEVEL
       FROM
       (Select DISTINCT
            OVERRIDE_ORIG_FAS_LEVEL
        from
        TABLE(pm2mData) m2m
        --RISKDB.QP_INT_FWD_Curves_raw_data m2m
        where
        cob_date = pcobDATE
        and Underlying_Commodity = pUnderlyingCom
        and PRICE_VOL_INDICATOR = ppricevol
        and Location = poriginalLOCATION -- Always original basis Point 
    --    and to_number(m2m.CONTRACTYEAR_MONTH)  > pCurrentMonth
        and to_number(m2m.contractYear_month) Between  pstartYearMonth 
                                                 AND   PEndYearMOnth
        and OVERRIDE_ORIG_FAS_LEVEL IS NOT NULL                                         
        )m
      )  
     , m2mSet AS ( 
        Select DISTINCT
        m.OVERRIDE_ORIG_FAS_LEVEL 
        , sum(ABS(m.LEGGED_M2M_VALUE)) Over ( partition by m.OVERRIDE_ORIG_FAS_LEVEL )      LEGGED_M2M_VALUE
     --   , sum(ABS(m2m.LEGGED_M2M_VALUE)) Over ( partition by m2m.original_FAS157_Level ) ABS_LEGGED_M2M_VALUE
        from 
        (
        SELECT 
        OVERRIDE_ORIG_FAS_LEVEL
        , SUM(ABS(m2m.LEGGED_M2M_VALUE)) LEGGED_M2M_VALUE
        FROM
        TABLE(pm2mData) m2m
       -- RISKDB.QP_INT_FWD_Curves_raw_data m2m
        where
--        partition_bit = 2 
        cob_date = pcobDATE
        and m2m.Underlying_commodity =     pUNDERLYINGcom 
        and m2m.price_vol_indicator =  ppricevol 
        and m2m.LOCATION = pOverrideLOCATION  
        and m2m.OVERRIDE_ORIG_FAS_LEVEL IS NOT NULL
        --and to_number(m2m.CONTRACTYEAR_MONTH)  > pCurrentMonth
        and to_number(m2m.contractYear_month) Between  pstartYearMonth 
                                                 AND   PEndYearMOnth
        and OVERRIDE_ORIG_FAS_LEVEL is NOT NULL                                         
        GROUP BY OVERRIDE_ORIG_FAS_LEVEL
        ) m
        )
        Select 
         m2mset.OVERRIDE_ORIG_FAS_LEVEL
         , m2mset.LEGGED_M2M_VALUE
        FROM 
        m2mSet
        , distinctFAS
        where
        m2mSet.OVERRIDE_ORIG_FAS_LEVEL = distinctFAS.OVERRIDE_ORIG_FAS_LEVEL
        order by m2mset.LEGGED_M2M_VALUE desc NULLS LAST , m2mset.OVERRIDE_ORIG_FAS_LEVEL asc
       ) 
    LOOP
        
        vn_FASLevel :=  FASRecs.OVERRIDE_ORIG_FAS_LEVEL;
        
--        DBMS_OUTPUT.PUT_LINE('overridden case , Blended FAS = '||vn_FASLevel);
        
        -- For the FAS get the Original Definition
        
        BEGIN
        
          SELECT 
            FAS_PERCENT_VALUE   
          INTO   
            pFASValue
          FROM 
          RISKDB.QP_FAS_TOLERANCE  f
          where
          FAS_LEVEL = vn_FASLevel
          and pcobdate Between  Effective_start_date 
                          AND   NVL(Effective_end_date, pcobdate)
          and ACTIVE_FLAG = 'Y';
 
        EXCEPTION
        WHEN NO_DATA_FOUND THEN 
           pFASValue := NULL;
        END;
        
        EXIT; --exit the Loop once we have original FAS
        
    END LOOP;


END IF;

IF POverrideLocation is NULL THEN 
 
  For FASrecs IN (
    WITH 
    distinctFAS as (
       SELECT m.Original_FAS157_level
       FROM
       (Select DISTINCT
            original_FAS157_Level
        from
        TABLE(pm2mData) m2m
        --RISKDB.QP_INT_FWD_Curves_raw_data m2m
        where
        cob_date = pcobDATE
        and Underlying_Commodity = pUnderlyingCom
        and PRICE_VOL_INDICATOR = ppricevol
        and Location = poriginalLOCATION -- Always original basis Point 
    --    and to_number(m2m.CONTRACTYEAR_MONTH)  > pCurrentMonth
        and to_number(m2m.contractYear_month) Between  pstartYearMonth 
                                                 AND   PEndYearMOnth
        and original_FAS157_Level IS NOT NULL                                         
        )m
      )  
     , m2mSet AS ( 
        Select DISTINCT
        m.original_FAS157_Level 
        , sum(ABS(m.LEGGED_M2M_VALUE)) Over ( partition by m.original_FAS157_Level )      LEGGED_M2M_VALUE
     --   , sum(ABS(m2m.LEGGED_M2M_VALUE)) Over ( partition by m2m.original_FAS157_Level ) ABS_LEGGED_M2M_VALUE
        from 
        (
        SELECT 
        original_FAS157_level
        , SUM(ABS(m2m.LEGGED_M2M_VALUE)) LEGGED_M2M_VALUE
        FROM
        TABLE(pm2mData) m2m
       -- RISKDB.QP_INT_FWD_Curves_raw_data m2m
        where
--        partition_bit = 2 
        cob_date = pcobDATE
        and m2m.Underlying_commodity =     pUNDERLYINGcom 
        and m2m.price_vol_indicator =  ppricevol 
        and m2m.LOCATION = poriginalLOCATION  
        and m2m.Original_FAS157_level IS NOT NULL
        --and to_number(m2m.CONTRACTYEAR_MONTH)  > pCurrentMonth
        and to_number(m2m.contractYear_month) Between  pstartYearMonth 
                                                 AND   PEndYearMOnth
        and original_FAS157_Level is NOT NULL                                         
        GROUP BY original_FAS157_level
        ) m
        )
        Select 
         m2mset.original_FAS157_Level
         , m2mset.LEGGED_M2M_VALUE
        FROM 
        m2mSet
        , distinctFAS
        where
        m2mSet.original_FAS157_Level = distinctFAS.original_FAS157_Level
        order by m2mset.LEGGED_M2M_VALUE desc NULLS LAST, m2mset.original_FAS157_Level asc
       ) 
    LOOP
        
        vn_FASLevel :=  FASRecs.original_FAS157_Level;
        
--       DBMS_OUTPUT.PUT_LINE('not overridden case , Blended FAS = '||vn_FASLevel);
        -- For the FAS get the Original Definition
        
        BEGIN
        
          SELECT 
            FAS_PERCENT_VALUE   
          INTO   
            pFASValue
          FROM 
          RISKDB.QP_FAS_TOLERANCE  f
          where
          FAS_LEVEL = vn_FASLevel
          and pcobdate Between  Effective_start_date 
                          AND   NVL(Effective_end_date, pcobdate)
          and ACTIVE_FLAG = 'Y';
 
        EXCEPTION
        WHEN NO_DATA_FOUND THEN 
           pFASValue := NULL;
        END;
        
        EXIT; --exit the Loop once we have original FAS
        
    END LOOP;

END IF;
-- end of original Location FAS check 


    
EXCEPTION
WHEN NO_DATA_FOUND THEN 
  pFASLevel := NULL;
  pFASValue := NULL;
  
WHEN OTHERS THEN 
  pFASLevel := NULL;
  pFASValue := NULL;
  

END Get_FAS_Level ;

 
-- Get_effective_date 
  FUNCTION Get_Effective_date RETURN DATE
  IS
  vd_default_effective_date  DATE;
  BEGIN
  
   vd_default_effective_date := TRUNC(SYSDATE -1);
   RETURN vd_default_effective_date;
   
  END Get_Effective_date;
  
  PROCEDURE Validate_Tenorstrip (
      pcobdate          IN            DATE,
      porigCommodity    IN            VARCHAR2 ,
      poverCommodity    IN            VARCHAR2 , 
      pOrigLocation     IN            VARCHAR2 ,
      pOverrideLoc      IN            VARCHAR2 ,    
      pnettingGroup     IN            VARCHAR2 ,
      pstartYearMonth   IN            VARCHAR2 ,
      pendYearMonth     IN            VARCHAR2 ,
      PpricevolInd      IN            VARCHAR2 ,
      pbackbone_flag    IN            VARCHAR2 ,
      pOverride_bb_flag IN            VARCHAR2 ,
      pmonthlym2m       IN OUT NOCOPY m2m_by_Loc_tab,
      pexcp_price_flag     OUT        VARCHAR2,
      pout_rtn_code        OUT NOCOPY NUMBER,
      pout_rtn_msg         OUT NOCOPY VARCHAR2
      )
   IS
      vc_section                   VARCHAR2 (1000) := 'Validate_contract_data1';
      vn_tenor_months              NUMBER;
      vn_actual_tenor_months       NUMBER;
      vn_pos_count                 NUMBER;
      vn_rank_count                NUMBER;
      vn_abs_m2mcount              NUMBER;
      vn_price_months              NUMBER;
      v_excp_price_flag            VARCHAR2 (1):='N';

 max_month          NUMBER;
 
 e_ValidCurveRAnk                   EXCEPTION;
   PRAGMA EXCEPTION_INIT( e_ValidCurveRAnk , -20100);

 e_Nopos_NoM2m                      EXCEPTION;
   PRAGMA EXCEPTION_INIT(  e_NoPos_NoM2m, -20101); 
    
 e_NoInternalPrice_forstrip         EXCEPTION;
   PRAGMA EXCEPTION_INIT( e_NoInternalPrice_forstrip, -20102);   
 e_ValidMonthlyCurve                EXCEPTION;
   PRAGMA EXCEPTION_INIT( e_NoInternalPrice_forstrip, -20202);

BEGIN

--dbms_output.Put_line('start='||PStartYearMOnth||' and end ='||PEndYearMOnth);

vn_tenor_months := MOnths_between( to_date(PEndYearMOnth||'01', 'YYYYMMDD') 
               ,  to_date(PStartYearMOnth||'01', 'YYYYMMDD')
                ) + 1;


--dbms_output.put_line ('tenor Months='||vn_tenor_months
--||'start='||pstartyearMonth 
--||'end='|| PEndYearMonth
--); 
               
--Step 1 is curve ranked ,if Yes By pass validation 

Select count(*) 
into vn_rank_count
FROM 
TABLE(pmonthlym2m) m2m
--RISKDB.QP_INT_FWD_Curves_raw_data m2m
where
m2m.LOCATION = pOrigLocation
and m2m.netting_Group_id = pnettingGroup
and m2m.price_vol_indicator = PpricevolInd 
and NVL( INITIAL_RANK , 0) = 1 
-- and to_number(m2m.contractYear_month) Between   pstartyearMonth and PEndYearMonth
; 
        
 IF   vn_rank_count > 0 THEN
  
  Raise e_ValidCurveRAnk;

 ELSE 
  
-- step2 Find out there is atleast one position <> 0 or one m2m <> 0 in all Forward months

       IF  PpricevolInd = 'PRICE' THEN 
        
            Select COunt(*)
            INTO vn_pos_count
            FROM 
            TABLE(pmonthlym2m) m2m
            --RISKDB.QP_INT_FWD_Curves_raw_data m2m
            where
            netting_group_id = pnettingGroup
            and m2m.price_vol_indicator = PpricevolInd
            and m2m.LOCATION = pOrigLocation
            and  ( m2m.LOCATION_DELTA_POSITION <> 0
     --              OR m2m.LEGGED_M2M_VALUE <> 0
                   OR NVL(m2m.ABSOLUTE_LEGGED_M2M_VALUE,0) <> 0 
     --              OR NVL(ABSOLUTE_LEGGED_M2M_VALUE_VOL,0) <> 0
                 ) 
           -- and to_number(m2m.contractYear_month) Between   pstartyearMonth and PEndYearMonth
            ; 
    --     dbms_output.put_line ('pos='||vn_pos_count); 
       
       END IF;

       IF  PpricevolInd = 'VOL' THEN 
        
            Select COunt(*)
            INTO vn_pos_count
            FROM 
            TABLE(pmonthlym2m) m2m
            --RISKDB.QP_INT_FWD_Curves_raw_data m2m
            where
            netting_group_id = pnettingGroup
            and m2m.price_vol_indicator = PpricevolInd
            and m2m.LOCATION = pOrigLocation
            and  ( m2m.LOCATION_DELTA_POSITION <> 0
            --    OR m2m.LEGGED_M2M_VALUE <> 0
            --    OR NVL(m2m.ABSOLUTE_LEGGED_M2M_VALUE,0) <> 0 
                   OR NVL(ABSOLUTE_LEGGED_M2M_VALUE_VOL,0) <> 0
                 ) 
           -- and to_number(m2m.contractYear_month) Between   pstartyearMonth and PEndYearMonth
            ; 
    --     dbms_output.put_line ('pos='||vn_pos_count); 
       
       END IF;
      
     
     IF vn_pos_count = 0 THEN 
         -- Means NO m2m for this location 
        Raise  e_NoPos_Nom2m ;
  
     Else
            null; -- let the code fall through rest of the Logic
     END IF;
     -- end of vn_pos_count = 0 check 


   
   
  -- enter this Logic only for NON Monthly CUrves 
  -- monthly curves should not be created when price missing 
  --sk 12/28/2015 
  
  IF pstartyearMonth <> PEndYearMonth THEN  
  
   
  --Confirm atleast one month of a Tenor available in WOrk area Collection.
  
          SELECT COUNT (DISTINCT m2m.contractYear_month)
           INTO vn_actual_tenor_months
           FROM TABLE (pmonthlym2m) m2m
          WHERE     
          netting_group_id = pnettingGroup
          AND m2m.price_vol_indicator = PpricevolInd
          AND m2m.LOCATION = pOrigLocation
          AND EXCEPTION_PRICE_FLAG = 'N'
          AND TO_NUMBER (m2m.contractYear_month) BETWEEN pstartyearMonth
                                                     AND PEndYearMonth
          ;



--     dbms_output.put_line (
--     'Location='||pOrigLocation
--     ||'OverrideLoc='||pOverrideLoc
--     ||'start='||pstartyearMonth
--     ||'End='||PEndYearMonth
--     ||'Actual tenor Months='||vn_actual_tenor_months
--     );

   -- this check is yo ensure that atleast some deal months are 
   -- available in work area 
    IF  vn_actual_tenor_months > 0 THEN   

        -- There must be atleast one contract month available in workarea.
        -- If so  , the remaining gaps needs to be filled from the Price available
        -- in Projection_Curves/Proj_location_vol_curve
        
         IF vn_actual_tenor_months < vn_tenor_months
         THEN
         
--          dbms_output.Put_line('internal price < contract Months');
          
            CASE
            WHEN PpricevolInd = 'PRICE'
            THEN
                  SELECT  /*+ index(RISKDB.PROJECTION_CURVES pc_pk) */  COUNT (
                            CASE
                               WHEN poverrideLoc is NOT NULL and poverride_bb_flag = 'Y' THEN PROJ_LOCATION_AMT
                               WHEN poverrideLoc is NULL AND pbackbone_flag = 'Y' THEN  PROJ_LOCATION_AMT
                               ELSE PROJ_BASIS_AMT
                            END)
                    INTO vn_price_months
                    FROM RISKDB.PROJECTION_CURVES
                   WHERE  EFFECTIVE_DATE =  pcobdate 
                   AND PARTITION_BIT =2
                   AND commodity = NVL(pOverCommodity, pOrigCommodity )
                   AND BASIS_POINT = NVL(pOverrideLoc, pOrigLocation )
                   AND TO_NUMBER (contract_month) BETWEEN pstartyearMonth
                                                  AND PEndYearMonth
                   ;

 
               
              IF vn_price_months < vn_tenor_months
              THEN
                
--                DBMS_OUTPUT.PUT_LINE('Price curve PRICE mONTHS='||VN_price_months
--                ||'tenor Months = '||vn_tenor_months);
                
                     RAISE e_NoInternalPrice_forstrip;
              ELSE
                     v_excp_price_flag := 'Y';
              END IF;
                 
                 
            WHEN PpricevolInd = 'VOL'
            THEN
 
                  SELECT COUNT (DAILY_VOL)
                    INTO vn_price_months
                    FROM PROJ_LOCATION_VOL_CURVE
                   WHERE   
                   EFFECTIVE_DATE =  pcobdate 
                   AND PARTITION_BIT =2
                   AND COMMODITY = NVL(pOverCommodity, pOrigCommodity )
                   AND BASIS_POINT = NVL(pOverrideLoc, pOrigLocation )
                   AND DAILY_VOL IS NOT NULL
                   AND TO_NUMBER (contract_month) BETWEEN pstartyearMonth
                                                        AND PEndYearMonth
                  ;

              
                  IF vn_price_months < vn_tenor_months
                  THEN
                
--                   DBMS_OUTPUT.PUT_LINE('VOL curve PRICE mONTHS '||VN_price_months
--                   ||'< tenor Months  '||vn_tenor_months);
                
                     RAISE e_NoInternalPrice_forstrip;
                  ELSE
                     v_excp_price_flag := 'Y';
                  END IF;

            END CASE;
            
            pexcp_price_flag := v_excp_price_flag ;
                
--           dbms_output.Put_line('pexcp_price_flag :='||v_excp_price_flag);
         END IF; ---end if for vn_actual_tenor_months < vn_tenor_months
    ELSE
--       dbms_output.Put_line('else clause raise no internal Price ');
         Raise e_NoInternalPrice_forstrip;  
    END IF;
     -- end of vn_actual_tenor_months > 0 check 
  
  ELSE
  
    null; -- do Nothing 
  
  END IF;
   -- IF pstartyearMonth <> PEndYearMonth 
  
  END IF; ---for vn_rank_count 



      -- curve ranked , so contniue as Success
      pout_rtn_code := 0;
      pout_rtn_msg := '';
                  
                  
EXCEPTION
WHEN e_ValidMonthlyCurve   THEN 
    pout_rtn_code := 0;
    pout_rtn_msg := '';
WHEN e_ValidCurveRAnk THEN 
    pout_rtn_code := 0;
    pout_rtn_msg := '';
    
    dbms_output.Put_line('Ranked ');
    
WHEN  e_Nopos_NoM2m THEN
  pout_rtn_code := 0;
  pout_rtn_msg := 'STOP'; 
  dbms_output.Put_line('Validate_Tenorstrip'
  ||'no pos and m2m failed '
  ||'Loc='||NVL(pOverrideLoc, PorigLocation)
  );
WHEN  e_NoInternalPrice_forstrip  THEN 
  pout_rtn_code := 0;
  pout_rtn_msg := 'STOP';
  dbms_output.Put_line(
  'Validate_Tenorstrip'
  ||' faield , NOt even a single price in work area data'
  ||'Start='||pstartyearMonth||', end='||PEndYearMonth
  ||'Loc='||NVL(pOverrideLoc, PorigLocation)
  );   
WHEN NO_DATA_FOUND THEN 
 pout_rtn_code := 0;
 pout_rtn_msg := 'STOP';
  dbms_output.Put_line('Validate_Tenorstrip'||'No Data Found');
WHEN OTHERS THEN 
 pout_rtn_code := 0;
 pout_rtn_msg := 'STOP'; 
dbms_output.Put_line('Validate_Tenorstrip'||'OTHERS'); 
END Validate_Tenorstrip;


Procedure Validate_contract_data (
                 pcobdate           IN DATE
               , pLocation          IN VARCHAR2
               , pnettingGroup      IN VARCHAR2
               , pstartYearMonth    IN VARCHAR2
               , pendYearMonth      IN VARCHAR2
               , PpricevolInd       IN VARCHAR2
               , pout_rtn_code      OUT  NOCOPY NUMBER    
               , pout_rtn_msg       OUT  NOCOPY VARCHAR2 
            )
IS
vc_section         Varchar2(1000) := 'Validate_contract_data'; 
vn_tenor_months         NUMBER;
vn_actual_tenor_months  NUMBER;
vn_m2m_count            NUMBER;
vn_rank_count           NUMBER;
BEGIN

------dbms_output.Put_line('beginning of validate contract Data');

vn_tenor_months := MOnths_between( to_date(PEndYearMOnth||'01', 'YYYYMMDD') 
               ,  to_date(PStartYearMOnth||'01', 'YYYYMMDD')
                ) + 1;
                
--Step 1 is curve ranked ,if Yes By pass validation 
BEGIN 

Select count(*) into vn_rank_count
FROM 
RISKDB.QP_INT_FWD_Curves_raw_data m2m
--RISKDB.QP_INT_FWD_Curves_raw_data m2m
where
cob_date =  pcobdate
and m2m.LOCATION = pLocation
and NVL( INITIAL_RANK , 0) = 1;

END;
 
-- and to_number(m2m.contractYear_month) Between   pstartyearMonth and PEndYearMonth

        
 IF   vn_rank_count = 0 THEN 
-- step2 Find out there is atleast m2m in all Forward months
-- step 2 find out there is Price available for all these curves in Projection_curves table 
 
  BEGIN
    
      IF PpricevolInd = 'PRICE' THEN 
        
            Select COunt(ABS_M2M_LEG_VALUE_AMT)
            INTO vn_m2m_count
            FROM 
            RISKDB.QP_INT_FWD_Curves_raw_data m2m
            --RISKDB.QP_INT_FWD_Curves_raw_data m2m
            where
            cob_date =  pcobdate
            and netting_group_id = pnettingGroup
            and m2m.price_vol_indicator = PpricevolInd
            and m2m.LOCATION = pLocation
            and ABS_M2M_LEG_VALUE_AMT <> 0 
           -- and to_number(m2m.contractYear_month) Between   pstartyearMonth and PEndYearMonth
            ; 
     END IF;
 
     IF PpricevolInd = 'VOL' THEN
         Select COunt(ABS_M2M_LEG_VALUE_AMT_VOL)
        INTO vn_m2m_count
        FROM 
        RISKDB.QP_INT_FWD_Curves_raw_data m2m
        --RISKDB.QP_INT_FWD_Curves_raw_data m2m
        where
        cob_date =  pcobdate
        and netting_group_id = pnettingGroup
        and m2m.price_vol_indicator = PpricevolInd
        and m2m.LOCATION = pLocation
        and ABS_M2M_LEG_VALUE_AMT_VOL <> 0 
       -- and to_number(m2m.contractYear_month) Between   pstartyearMonth and PEndYearMonth
        ; 
     END IF;
 
        
        IF vn_m2m_count = 0 THEN 
         -- Means NO m2m for this location 
            Raise NO_DATA_FOUND ;
        Else
            null; -- let the code fall through rest of the Logic
        END IF;
        
  EXCEPTION
  WHEN NO_DATA_FOUND THEN 
                -- Insufficient contract Months to support Tenor
        pout_rtn_code := 0;
        pout_rtn_msg := 'STOP';  
        Raise  ;
  WHEN OTHERS THEN 
      pout_rtn_msg := 'STOP'; 
      --dbms_output.put_line(pout_rtn_msg);
      Raise;   
  END ;
  

     
   BEGIN 

  --- Verify availability of contract Price in internal Stage 2 ==>
  -- inturn this is a source from work area   
      
       Select COunt(DISTINCT ContractYear_month)
        INTO vn_actual_tenor_months
        FROM 
        RISKDB.QP_INT_FWD_Curves_raw_data m2m
        where
        COB_DATE =  pcobdate
        and netting_group_id = pnettingGroup
        and m2m.price_vol_indicator = PpricevolInd
        and m2m.LOCATION = pLocation
        and to_number(m2m.contractYear_month) Between   pstartyearMonth and PEndYearMonth
        and INTERNAL_PRICE is NOT NULL
        ;
        
        
       IF vn_actual_tenor_months <> vn_tenor_months THEN
        
                       pout_rtn_code := 0;
                       pout_rtn_msg := 'STOP';

       ELSE

            -- Success
            pout_rtn_code := 0;
            pout_rtn_msg := '';

        END IF ;
        

        
        
   EXCEPTION     
   WHEN OTHERS THEN
     pout_rtn_code := 0;
     pout_rtn_msg := 'STOP';  
    Raise;         
   END ;
   
 ELSE 

      -- curve ranked , so contniue as Success
            pout_rtn_code := 0;
            pout_rtn_msg := '';
            
 END IF ;
 

EXCEPTION
WHEN NO_DATA_FOUND THEN 
 pout_rtn_code := 0;
 pout_rtn_msg := 'STOP';
WHEN OTHERS THEN 
 pout_rtn_code := 0;
 pout_rtn_msg := 'STOP'; 
----dbms_output.put_line(pout_rtn_msg);
END Validate_contract_data;
  
     
/******************************************************************************/
FUNCTION upd_detail_rec_data(rec_detail IN  riskdb.qp_zema_profile_detail%ROWTYPE
                            ,o_rtn_msg  OUT VARCHAR2
                             ) RETURN INTEGER IS
BEGIN
   UPDATE riskdb.qp_zema_profile_detail
   SET    status           = rec_detail.status
         ,last_run_url     = rec_detail.last_run_url
         ,data_xml         = rec_detail.data_xml
         ,data_clob        = rec_detail.data_clob
         ,date_updated     = SYSDATE
         ,updated_by       = USER
   WHERE  profile_id = rec_detail.profile_id
   ;
   COMMIT;
   RETURN c_success;
EXCEPTION
   WHEN OTHERS THEN
      o_rtn_msg := SUBSTR(SQLERRM,1,200);
      RETURN c_failure;
END upd_detail_rec_data;


/******************************************************************************/
FUNCTION upd_detail_rec(rec_detail IN riskdb.qp_zema_profile_detail%ROWTYPE
                       ,o_rtn_msg  OUT VARCHAR2
                        ) RETURN INTEGER IS
BEGIN
   UPDATE riskdb.qp_zema_profile_detail
   SET    status           = rec_detail.status
         ,last_run_url     = rec_detail.last_run_url
         ,Start_date       = rec_detail.start_date  
         ,end_date         = rec_detail.end_date    
         --,data_xml         = rec_detail.data_xml
         --,data_clob        = rec_detail.data_clob
         ,last_run_date    = rec_detail.last_run_date
         ,dtl_rec_total    = rec_detail.dtl_rec_total
         ,dtl_rec_inserted = rec_detail.dtl_rec_inserted
         ,dtl_rec_updated  = rec_detail.dtl_rec_updated
         ,date_updated     = SYSDATE
         ,updated_by       = USER
   WHERE  profile_id = rec_detail.profile_id
   ;
   COMMIT;
   RETURN c_success;
EXCEPTION
   WHEN OTHERS THEN
      o_rtn_msg := SUBSTR(SQLERRM,1,200);
      RETURN c_failure;
END upd_detail_rec;


/******************************************************************************/
FUNCTION fetch_control(rec_control OUT riskdb.zema_control%ROWTYPE
                      ,o_rtn_msg   OUT VARCHAR2
                       ) RETURN INTEGER IS
BEGIN
   SELECT *
   INTO   rec_control
   FROM   riskdb.zema_control
   ;
   RETURN c_success;
EXCEPTION
   WHEN OTHERS THEN
      o_rtn_msg := SUBSTR(SQLERRM,1,200);
      RETURN c_failure;
END fetch_control;


/******************************************************************************/
FUNCTION fetch_Profile_detail(pzemaprofile_id IN riskdb.qp_zema_profile_detail.profile_id%TYPE
                             ,rec_Detail OUT riskdb.qp_zema_profile_detail%ROWTYPE
                             ,o_rtn_msg   OUT VARCHAR2
                              ) RETURN INTEGER IS
BEGIN
   SELECT *
   INTO   rec_Detail
   FROM   riskdb.qp_zema_profile_detail
   WHERE  Profile_id = pzemaprofile_id
        AND active = 'Y'
   ;
   RETURN c_success;
EXCEPTION
   WHEN NO_DATA_FOUND THEN 
      o_rtn_msg := 'NO active Profiles Found for this Profile ID='||pzemaprofile_id;
      RETURN c_failure;
   WHEN OTHERS THEN
      o_rtn_msg := SUBSTR(SQLERRM,1,200);
      RETURN c_failure;
END fetch_Profile_detail;


/******************************************************************************/
PROCEDURE Prepare_ZEMA_URL(pzema_ctl_rec     IN     riskdb.zema_control%ROWTYPE
                          ,pZema_profile_Rec IN OUT riskdb.qp_zema_profile_detail%ROWTYPE
                          ,p_url_string      OUT    VARCHAR2 
                          ,pout_rtn_code     OUT    NUMBER
                          ,pout_rtn_msg      OUT    VARCHAR2
                           ) IS
   v_rtn_code    NUMBER;
   v_rtn_msg     VARCHAR2(500);
   v_url_string  VARCHAR2(3000);
   v_amp         VARCHAR2(1):= '&';
   v_section     VARCHAR2(100);
   v_rtn         NUMBER;

   e_Zema_Prof_dtl_upd_failed    Exception;
      PRAGMA EXCEPTION_INIT( e_Zema_Prof_dtl_upd_failed , -20009);

BEGIN
 
 -- r_rec := pZema_profile_Rec;
  
  v_section := '-- Set Detail Rec Status';
--   set_log(r_rec.Profile_id, v_section, 'Start');
   pZema_profile_Rec.status           := 'R';
   pZema_profile_Rec.last_run_date    := SYSDATE ;  
   pZema_profile_Rec.last_run_url     := NULL;
   pZema_profile_Rec.data_xml         := NULL;
   pZema_profile_Rec.dtl_rec_total    := 0;
   pZema_profile_Rec.dtl_rec_inserted := 0;
   pZema_profile_Rec.dtl_rec_updated  := 0;
   v_rtn_msg              := NULL;

   v_rtn := upd_detail_rec(pZema_profile_Rec, v_rtn_msg);
   
   IF v_rtn != c_success THEN
       Raise e_Zema_Prof_dtl_upd_failed;
   END IF;
   
  v_section := '-- Build the URL String';
    
-- Assign Base URL Values 
  v_url_string := pzema_ctl_rec.base_url||
                   'command='||pzema_ctl_rec.command||
                   v_amp||'username='||pzema_ctl_rec.username||
                   v_amp||'password='||pzema_ctl_rec.control||
                   v_amp||'id='||pZema_profile_Rec.client||
                   v_amp||'profilename='||pZema_profile_Rec.profile_name||
                   v_amp||'profileowner='||pZema_profile_Rec.profile_owner||
                   v_amp||'style='||pZema_profile_Rec.style;

-- Based on Details of URL

   IF pZema_profile_Rec.run_type = 'D' THEN
      v_url_string := v_url_string||
                      v_amp||'startDate='||TO_CHAR(pZema_profile_Rec.start_date, 'MM-DD-YYYY')||
                      v_amp||'endDate='||TO_CHAR(pZema_profile_Rec.end_date,'MM-DD-YYYY');
   ELSIF pZema_profile_Rec.run_type = 'I' AND to_char(sysdate, 'd') = '2' THEN
      v_url_string := v_url_string||
                      v_amp||'startDate='||TO_CHAR((SYSDATE - pZema_profile_Rec.interval),'MM-DD-YYYY')||
                      v_amp||'endDate='||TO_CHAR(SYSDATE,'MM-DD-YYYY');
   ELSE
       v_url_string := v_url_string||
                      v_amp||'startDate='||TO_CHAR((SYSDATE - 1),'MM-DD-YYYY')||
                      v_amp||'endDate='||TO_CHAR(SYSDATE,'MM-DD-YYYY');
   END IF;
   
   
   pZema_profile_Rec.last_run_url := v_url_string;
   v_rtn := upd_detail_rec(pZema_profile_Rec, v_rtn_msg);
   
   IF v_rtn != c_success THEN
      Raise e_Zema_Prof_dtl_upd_failed;
   END IF;
   
--- assign Zema info to Zema Record 
   p_url_string  := v_url_string;
   pZema_profile_Rec.last_run_url := v_url_string;
   
   pout_rtn_code := c_success;
   pout_rtn_msg := v_rtn_msg; --Null means Success
  

EXCEPTION
WHEN e_Zema_Prof_dtl_upd_failed THEN 
       p_url_string  := v_url_string;
       pout_rtn_code := c_failure;
       pout_rtn_msg := 'Building URL Operation Failed'||'-'||SUBSTR(SQLERRM,1,200);
WHEN OTHERS THEN
       p_url_string  := v_url_string;
       pout_rtn_code := c_failure;
       pout_rtn_msg := v_section||'-'||SUBSTR(SQLERRM,1,200);
END Prepare_ZEMA_URL;
/******************************************************************************/

PROCEDURE Xtract_ZEMA_Data(
  rec_detail IN OUT riskdb.qp_zema_profile_detail%ROWTYPE
, pout_rtn_code    OUT NUMBER
, pout_rtn_msg     OUT VARCHAR2
)
IS
v_clob      CLOB;
v_http_req  UTL_HTTP.req   := NULL;
v_http_resp UTL_HTTP.resp  := NULL;
v_text          VARCHAR2(32767);
v_max_size      NUMBER := 32767;
v_section       VARCHAR2(1000);
v_rtn_msg       VARCHAR2(1000);
v_rtn           NUMBER;
e_Zema_Prof_dtl_upd_failed    EXCEPTION;
   PRAGMA EXCEPTION_INIT( e_Zema_Prof_dtl_upd_failed , -20009);
e_zema_reported_error         EXCEPTION;
   PRAGMA EXCEPTION_INIT ( e_zema_reported_error , -20010);  
v_name          VARCHAR2(100);
v_value          VARCHAR2(100);
b_error_found   Boolean := FALSE;
v_error_text    VARCHAR2(5000);
vn_temp         INTEGER;
BEGIN

    v_section := 'Initialize CLOB';
    PROCESS_LOG_REC.MESSAGE      := 'Initialize CLOB';
    
    DMR.PROCESS_LOG_PKG.WRITE_LOG(
            P_PROCESS_NAME => PROCESS_LOG_REC.PROCESS_NAME,
            P_STAGE=> PROCESS_LOG_REC.STAGE,
            P_STATUS=>PROCESS_LOG_REC.STATUS,
            P_MESSAGE=> PROCESS_LOG_REC.MESSAGE);


     DBMS_LOB.createtemporary(v_clob, FALSE );
  
    
   
   --request detailed Error Message 
   UTL_HTTP.SET_DETAILED_EXCP_SUPPORT (TRUE);
   
    v_section := 'Begin HTTP Request';
    PROCESS_LOG_REC.MESSAGE      := 'Begin HTTP Request';
    
    DMR.PROCESS_LOG_PKG.WRITE_LOG(
            P_PROCESS_NAME => PROCESS_LOG_REC.PROCESS_NAME,
            P_STAGE=> PROCESS_LOG_REC.STAGE,
            P_STATUS=>PROCESS_LOG_REC.STATUS,
            P_MESSAGE=> PROCESS_LOG_REC.MESSAGE);
   
    UTL_HTTP.set_transfer_timeout(c_timeout);
             
   --Http request
   
 v_http_req  := UTL_HTTP.begin_request(rec_detail.Last_run_url , 'GET', 'HTTP/1.1' );
 utl_http.set_header(v_http_req, 'Content-Type', 'text/csv');   
   
    
--    ----dbms_output.Put_line (v_section);
      
    v_section := 'Get HTTP Response';
    
    PROCESS_LOG_REC.MESSAGE      := 'Get HTTP Response';
   
--   ----dbms_output.Put_line (v_section);
    
    DMR.PROCESS_LOG_PKG.WRITE_LOG(
            P_PROCESS_NAME => PROCESS_LOG_REC.PROCESS_NAME,
            P_STAGE=> PROCESS_LOG_REC.STAGE,
            P_STATUS=>PROCESS_LOG_REC.STATUS,
            P_MESSAGE=> PROCESS_LOG_REC.MESSAGE);
 
    v_http_resp := UTL_HTTP.get_response(v_http_req);
  

--   dbms_output.PUT_LINE('HTTP Response Status Code: ' || v_http_resp.status_code);
--   dbms_output.PUT_LINE('HTTP Response Reason Phrase: ' || v_http_resp.reason_phrase);
--   dbms_output.PUT_LINE('HTTP Response Version: ' || v_http_resp.http_version);


    v_section := 'Read Http Response';
    PROCESS_LOG_REC.MESSAGE      := 'Read Http Response';
    
    DMR.PROCESS_LOG_PKG.WRITE_LOG(
            P_PROCESS_NAME => PROCESS_LOG_REC.PROCESS_NAME,
            P_STAGE=> PROCESS_LOG_REC.STAGE,
            P_STATUS=>PROCESS_LOG_REC.STATUS,
            P_MESSAGE=> PROCESS_LOG_REC.MESSAGE);
   --read clob Data         
   BEGIN
      LOOP
--         dbms_output.Put_line ('before reading http response');
         UTL_HTTP.read_text(v_http_resp, v_text, v_max_size);

         DBMS_LOB.writeappend (v_clob, LENGTH(v_text), v_text);

         IF INSTR(v_text , '<error>',1) > 0 THEN
           b_error_found := TRUE;
            v_error_text := v_error_text||v_text;
         ELSE
           -- once error found rest of the text must be related to error itself.
           IF b_error_found  THEN 

             v_error_text := v_error_text||v_text;

           END IF;
           
         END IF;

      END LOOP;
   EXCEPTION
     WHEN UTL_HTTP.end_of_body THEN
--          dbms_output.PUT_LINE ( 'End of Body Exception');
          UTL_HTTP.end_response(v_http_resp);
   END;
   
 
   
--    dbms_output.PUT_LINE ( 'read '||NVL(LENGTH(v_clob),0)||' Bytes');
   
      v_section := '-- Store the CLOB';
      
      rec_detail.status       := 'P';
      rec_detail.data_clob    := v_clob;
      
      v_rtn_msg := NULL ;
      
      v_rtn := upd_detail_rec_data(rec_detail, v_rtn_msg);
     
       IF v_rtn != c_success THEN
          Raise e_Zema_Prof_dtl_upd_failed;
       END IF;
   
   ----
    v_section := '-- Ending HTTP reponse';
    PROCESS_LOG_REC.MESSAGE      := 'Ending Http Response';
    
    DMR.PROCESS_LOG_PKG.WRITE_LOG(
            P_PROCESS_NAME => PROCESS_LOG_REC.PROCESS_NAME,
            P_STAGE=> PROCESS_LOG_REC.STAGE,
            P_STATUS=>PROCESS_LOG_REC.STATUS,
            P_MESSAGE=> PROCESS_LOG_REC.MESSAGE);
   
    dbms_output.PUT_LINE ( v_section);
    v_section := ' Release CLOB';
    DBMS_LOB.freetemporary(v_clob);

--    ----dbms_output.PUT_LINE ( v_section);

 PROCESS_LOG_REC.MESSAGE      := v_section;
    
    DMR.PROCESS_LOG_PKG.WRITE_LOG(
            P_PROCESS_NAME => PROCESS_LOG_REC.PROCESS_NAME,
            P_STAGE=> PROCESS_LOG_REC.STAGE,
            P_STATUS=>PROCESS_LOG_REC.STATUS,
            P_MESSAGE=> PROCESS_LOG_REC.MESSAGE);
    
     IF b_error_found THEN
 
           Raise e_zema_reported_error;
     ELSE
           pout_rtn_code := c_success;
           pout_rtn_msg := v_rtn_msg; --Null means Success       
     END IF;   
      
EXCEPTION
WHEN UTL_HTTP.BAD_ARGUMENT THEN
  V_section := 'Http Request Failed, With Bad Argument Exception.';
  DBMS_LOB.freetemporary(v_clob); 
  pout_rtn_code := c_failure;
  pout_rtn_msg := v_section||'-'||SUBSTR(SQLERRM,1,200);
  UTL_HTTP.END_REQUEST(v_http_req);
  UTL_HTTP.END_RESPONSE(v_http_resp);
WHEN UTL_HTTP.BAD_URL THEN
  V_section := 'Http Request Failed, With Bad URL Exception.'; 
  DBMS_LOB.freetemporary(v_clob);
  pout_rtn_code := c_failure;
  pout_rtn_msg := v_section||'-'||SUBSTR(SQLERRM,1,200);
  UTL_HTTP.END_REQUEST(v_http_req);
  UTL_HTTP.END_RESPONSE(v_http_resp);
WHEN UTL_HTTP.PROTOCOL_ERROR THEN
  V_section := 'Http Request Failed, With Protocol Error Exception.'; 
  DBMS_LOB.freetemporary(v_clob);
  pout_rtn_code := c_failure;
  pout_rtn_msg := v_section||'-'||SUBSTR(SQLERRM,1,200); 
  UTL_HTTP.END_REQUEST(v_http_req);
  UTL_HTTP.END_RESPONSE(v_http_resp);
WHEN UTL_HTTP.HEADER_NOT_FOUND THEN
  V_section := 'Http Request Failed, With Header NOT Found Exception.'; 
  DBMS_LOB.freetemporary(v_clob);
  pout_rtn_code := c_failure;
  pout_rtn_msg := v_section||'-'||SUBSTR(SQLERRM,1,200);
  UTL_HTTP.END_REQUEST(v_http_req);
  UTL_HTTP.END_RESPONSE(v_http_resp);
WHEN UTL_HTTP.ILLEGAL_CALL THEN
  V_section := 'Http Request Failed, With Illegal Call Exception.'; 
  DBMS_LOB.freetemporary(v_clob);
  pout_rtn_code := c_failure;
  pout_rtn_msg := v_section||'-'||SUBSTR(SQLERRM,1,200);
  UTL_HTTP.END_REQUEST(v_http_req);
  UTL_HTTP.END_RESPONSE(v_http_resp);       
WHEN UTL_HTTP.HTTP_CLIENT_ERROR THEN
  V_section := 'Http Request Failed, With Http CLient Error Exception.'; 
  DBMS_LOB.freetemporary(v_clob);
  pout_rtn_code := c_failure;
  pout_rtn_msg := v_section||'-'||SUBSTR(SQLERRM,1,200);
  UTL_HTTP.END_REQUEST(v_http_req);
  UTL_HTTP.END_RESPONSE(v_http_resp); 
WHEN UTL_HTTP.HTTP_SERVER_ERROR THEN
  V_section := 'Http Request Failed, With Http Server Error Exception.'; 
  DBMS_LOB.freetemporary(v_clob);
  pout_rtn_code := c_failure;
  pout_rtn_msg := v_section||'-'||SUBSTR(SQLERRM,1,200);
  UTL_HTTP.END_REQUEST(v_http_req);
  UTL_HTTP.END_RESPONSE(v_http_resp);  
WHEN UTL_HTTP.TOO_MANY_REQUESTS THEN
  V_section := 'Http Request Failed, With Too Many Requests Exception.'; 
  DBMS_LOB.freetemporary(v_clob);
  pout_rtn_code := c_failure;
  pout_rtn_msg := v_section||'-'||SUBSTR(SQLERRM,1,200);
  UTL_HTTP.END_REQUEST(v_http_req);
  UTL_HTTP.END_RESPONSE(v_http_resp); 
WHEN UTL_HTTP.REQUEST_FAILED THEN
  V_section := 'Http Request Failed, With Request Failed Exception.'; 
  DBMS_LOB.freetemporary(v_clob);
  pout_rtn_code := c_failure;
  pout_rtn_msg := v_section||'-'||SUBSTR(SQLERRM,1,200);
  UTL_HTTP.END_REQUEST(v_http_req);
  UTL_HTTP.END_RESPONSE(v_http_resp);         
WHEN UTL_HTTP.TRANSFER_TIMEOUT THEN
  V_section := 'Http Request Failed, With Transfer Timeout Exception.'; 
  DBMS_LOB.freetemporary(v_clob);
  pout_rtn_code := c_failure;
  pout_rtn_msg := v_section||'-'||SUBSTR(SQLERRM,1,200);
  UTL_HTTP.END_REQUEST(v_http_req);
  UTL_HTTP.END_RESPONSE(v_http_resp);
WHEN e_zema_reported_error THEN 
--  ----dbms_output.put_line('exception e_zema_reported_error');
  pout_rtn_code := c_failure;
  pout_rtn_msg := v_error_text; 
--  ----dbms_output.put_line(v_error_text);
  DBMS_LOB.freetemporary(v_clob);   
-- ----dbms_output.PUT_LINE('pout_rtn_msg');
WHEN e_Zema_Prof_dtl_upd_failed THEN 
       pout_rtn_code := c_failure;
       pout_rtn_msg := v_section||'-Updating CLOB data to Zema_Profile_detail Failed'||'-'||SUBSTR(SQLERRM,1,200);
  UTL_HTTP.END_RESPONSE(v_http_resp);
      DBMS_LOB.freetemporary(v_clob);
WHEN OTHERS THEN 
  ----dbms_output.PUT_LINE( 'In oThers Exceptions');
  pout_rtn_code := c_failure;
  pout_rtn_msg := v_section||'-'||SUBSTR(SQLERRM,1,200);
   DBMS_LOB.freetemporary(v_clob);
   UTL_HTTP.END_RESPONSE(v_http_resp);  
END Xtract_ZEMA_Data;
/******************************************************************************/


PROCEDURE Get_Profile_mapping (
pzemaprofile_id         IN riskdb.qp_zema_profile_detail.Profile_id%TYPE 
, pcobdate              IN DATE
,pcollection            IN OUT  Zema_Xtract_t
, pout_rtn_code         OUT NUMBER
, pout_rtn_msg          OUT VARCHAR2
)
IS

v_section               VARCHAR2(1000);
v_Space                 VARCHAR2(1) := CHR(32);
v_Ampersand             VARCHAR2(1) := CHR(38);
v_count                 NUMBER;

begindx     PLS_INTEGER;
endindx     PLS_INTEGER;


CURSOR Column_mapping_cur IS
SELECT *
From
RISKDB.QP_EXT_NEER_COL_MAPPINGS 
where
ZEMA_PROFILE_ID = pzemaprofile_id
and ACTIVE_FLAG = 'Y'
AND NEER_TABLE_OWNER = 'RISKDB'
AND NEER_TABLE_NAME = 'QP_EXT_PROVIDER_QUOTES'
AND TRUNC(pcobdate) Between Effective_start_date and NVL(Effective_end_date, trunc(pcobdate))
ORder by COLUMN_ORDER; 


CURSOR Column_concat_cur (p_neer_name VARCHAR2) IS
SELECT 
Column_order
, concatenate_sequence
From
RISKDB.QP_EXT_NEER_COL_MAPPINGS 
where
ZEMA_PROFILE_ID = pzemaprofile_id
and ACTIVE_FLAG = 'Y'
and NEER_column_NAME = p_neer_name
AND TRUNC(pcobdate) Between Effective_start_date and NVL(Effective_end_date, trunc(pcobdate))
ORder by concatenate_sequence; 

col_mapping_rec     RISKDB.QP_EXT_NEER_COL_MAPPINGS%ROWTYPE;

e_Invalid_Column_Mapping         EXCEPTION;
   PRAGMA EXCEPTION_INIT(e_Invalid_Column_Mapping , -20008);     

BEGIN

v_section := 'Get_Profile_Mapping';


 FOR recs IN Column_mapping_cur LOOP
 
--    ----dbms_output.PUT_LINE (recs.COLUMN_ORDER||'-'||NVL(recs.EXT_COLUMN_NAME, 'NULL'));


 --Validate That Both  EXT_COLUMN AND  NEER_COLUMN IS NULL then Mapping Error
     IF trim(recs.EXT_COLUMN_NAME) is NULL AND Recs.NEER_COLUMN_NAME IS NULL THEN
      pout_rtn_code    := c_failure;
      pout_rtn_msg     := v_section||'-'||Upper(recs.EXT_COLUMN_NAME)||' And a NEER_COLUMN_NAME Both Cannot be NULL, Please fix Mapping Error';
      Raise e_Invalid_Column_Mapping;
     END IF;


--Validate That WHEN  EXT_COLUMN is There 
--AND IF  NEER_COLUMN IS NULL then LOAD_STAGE_FLAG should not be Y,  Mapping Error

    IF trim(recs.EXT_COLUMN_NAME) IS NOT NULL AND trim(recs.NEER_COLUMN_NAME) is NULL AND Recs.LOAD_STAGE_FLAG ='Y' THEN
      pout_rtn_code    := c_failure;
      pout_rtn_msg     := v_section||'-'||recs.EXT_COLUMN_NAME||' Is not Mapped, But marked LOAD_STAGE_FLAG as Y, Please fix the Mapping Error';
      Raise e_Invalid_Column_Mapping;
    END IF ;
    
     

 --Validate That when EXT_COLUMN is NULL NEER_COLUMN Is NOT NULL
 -- AND LOAD_STAGE_FLAG is marked as Y (Means to be loaded)
 -- Then FIXED_VALUE Must be Provided. DIsplay Mapping ERROR
 
     IF trim(recs.EXT_COLUMN_NAME) is NULL AND Recs.NEER_COLUMN_NAME IS NOT NULL
        AND recs.Fixed_Value is NULL 
        AND ( Recs.LOAD_STAGE_FLAG IS NULL OR Recs.LOAD_STAGE_FLAG = 'Y' )   THEN
      pout_rtn_code    := c_failure;
      pout_rtn_msg     := v_section||'-'||'When EXT_COLUMN_NAME is NULL Then '||Upper(recs.NEER_COLUMN_NAME)||' Must be mapped to a '||
      'Fixed Constant Value and LOAD_STAGE_FLAG must be Y , Please Fix Mapping Errors';
      Raise e_Invalid_Column_Mapping;
     END IF; 

 --Validate That None of the column SHOULD 
 --HAVE a COLUMN ORDER AS 0 or NULL . DIsplay Mapping ERROR

  
    IF NVL(Recs.COLUMN_ORDER, 0) = 0 THEN
      pout_rtn_code    := c_failure ;
      pout_rtn_msg     := v_section||'-'||'COLUMN_ORDER 0 is INVALID, Please fix the Mapping Error';
      Raise e_Invalid_Column_Mapping;
    END IF;
 
 
  --Validate That No More than One ACTIVE COLUMN , SHOULD HAVE the SAME COLUMN ORDER
 -- DIsplay Mapping ERROR

    
    Select count(*) Into v_count
    FROM RISKDB.QP_EXT_NEER_COL_MAPPINGS
    where
    ZEMA_PROFILE_ID = pzemaprofile_id
    AND ACTIVE_FLAG = 'Y'
    AND COLUMN_ORDER = recs.COLUMN_ORDER
    AND trunc(pcobdate) Between Effective_start_date and NVL(Effective_end_date, trunc(pcobdate))
    ;
    
    IF v_count > 1 THEN
      pout_rtn_code    := c_failure;
      pout_rtn_msg     := v_section||'-'||'COLUMN_ORDER'||recs.COLUMN_ORDER||' Must be Unique, Please fix the Mapping Error';
      Raise e_Invalid_Column_Mapping;
    END IF;
    
 
 --- Validate NEER_COLUMN_NAME Physically available in a Database
 -- If NEER_COLUMN_NAME is not Physically AVailable in DATABASE
 -- Then Display Mapping ERROR 
 
  IF recs.NEER_COLUMN_NAME IS NOT NULL THEN 
    Select count(*) 
    INTO v_count
    from all_tab_columns
    where 
    OWNER = 'RISKDB'
    AND TABLE_NAME = 'QP_EXT_PROVIDER_QUOTES'
    AND COLUMN_NAME = Upper(recs.NEER_COLUMN_NAME);
    
    IF v_count = 0 THEN
      pout_rtn_code    := c_failure;
      pout_rtn_msg     := v_section||'-'||Upper(recs.NEER_COLUMN_NAME)||' is Not a valid Column in QP_EXT_PROVIDER_QUOTES table , Please Verify';
      Raise e_Invalid_Column_Mapping;
    END IF;
  
  END IF;
  
 -- Validate That if there EXISTS ACTIVE NEER_COLUMN_NAME mapped to More than one Broker External column 
 -- ensure all Broker columns are marked as to be loaded with Load_stage_flag = 'Y' 
 -- so all Values can be conatenated and  
 
 -- No of COLUMNS and NO of collection Elements Must always be EQUAL
 
     CASE 

     WHEN  NVL(pcollection.LAST, 0) < recs.COLUMN_ORDER THEN

--        ----dbms_output.PUT_LINE('The Outer collection has '||NVL(pcollection.LAST, 0)||' Elements'||
--        'Needs '||(recs.COLUMN_ORDER -NVL(pcollection.LAST, 0))||' More to store '||
--        recs.COLUMN_ORDER
--        );  
          
          begindx := 1;
          
          -- How many more needs to be inserted?
          endindx := (recs.COLUMN_ORDER -NVL(pcollection.LAST, 0));
          
         FOR Indx in begindx..endindx 
         LOOP

            pcollection.Extend;
            -- Needs to be extended as many times as parent/Container collection
            IF pcollection(pcollection.LAST).Mapping_rec IS NULL THEN
                -- Intialize with empty collection
                -- Extend it by one element
                pcollection(pcollection.LAST).Mapping_rec := PROF_MAPPING_T();
                pcollection(pcollection.LAST).Mapping_rec.EXTEND;
            ELSE
                pcollection(pcollection.LAST).Mapping_rec.EXTEND;
            END IF;
                                     
         END LOOP;
         
      --sk 3/12/2015 
      -- If the EXT_COLUMN_NAME is NULL DO not G thrugh
      --REPLACE check
       IF  recs.EXT_COLUMN_NAME IS NOT NULL THEN             
           pcollection(pcollection.LAST).EXT_COLUMN := 
           REPLACE(recs.EXT_COLUMN_NAME,v_Space, v_Ampersand);
           
           --find out this ext_column is mapped to NEER_COLUMN 
           -- and if part of any NEER column capture details
           -- such as column order and concatenate sequence
           --other wise store NULL element into this member collection
           
           IF recs.NEER_COLUMN_NAME is NULL THEN 
--            ----dbms_output.Put_line ('assining a null element to concat groups');
            
                -- extend member collection concat_groups 
                IF pcollection(pcollection.LAST).concat_groups IS NULL THEN
                    -- Intialize with empty collection
                    -- Extend it by one element
                    pcollection(pcollection.LAST).concat_groups := CONCAT_REC_T();
                    pcollection(pcollection.LAST).concat_groups.EXTEND;               
                else
                    pcollection(pcollection.LAST).concat_groups.EXTEND;
                END IF; 


              pcollection(pcollection.LAST).concat_groups(pcollection(pcollection.LAST).concat_groups.LAST):= 
                NULL;
           ELSE
             --store column order and concatenate sequence 
             -- into member collection concat groups
             FOR concat_recs IN Column_concat_cur (recs.NEER_COLUMN_NAME) LOOP
             
               -- extend member collection concat_groups 
                IF pcollection(pcollection.LAST).concat_groups IS NULL THEN
                    -- Intialize with empty collection
                    -- Extend it by one element
                    pcollection(pcollection.LAST).concat_groups := CONCAT_REC_T();
                    pcollection(pcollection.LAST).concat_groups.EXTEND;               
                else
                    pcollection(pcollection.LAST).concat_groups.EXTEND;
                END IF; 


--             ----dbms_output.Put_line ('column_order='||concat_recs.column_order||' and seq='||concat_recs.CONCATENATE_SEQUENCE);
               pcollection(pcollection.LAST).concat_groups(pcollection(pcollection.LAST).concat_groups.LAST).COLUMN_ORDER :=
                 concat_recs.COLUMN_ORDER;
               pcollection(pcollection.LAST).concat_groups(pcollection(pcollection.LAST).concat_groups.LAST).CONCAT_SEQUENCE :=
                 concat_recs.CONCATENATE_SEQUENCE;
                 
             END LOOP; 
           END IF;
           
       END IF;
           
         --- initialize this flag to FALSE to mark it later as FOUND
         -- when Header Label in the actual ZEMA CLOB data is found 
         pcollection(pcollection.LAST).EXT_COLUMN_FOUND := FALSE;         
        
 
     --Get the Entire COLUMN MAPPING RECORD    
         Select * 
         into  col_mapping_rec
         From RISKDB.QP_EXT_NEER_COL_MAPPINGS
         where
         COLUMN_MAPPING_ID =  recs.COLUMN_MAPPING_ID
         and ACTIVE_FLAG = 'Y'
         AND  trunc(pcobdate) Between Effective_start_date and NVL(Effective_end_date,  trunc(pcobdate))
         ;
         

         --assign col_mapping_rec for this external column
         pcollection(pcollection.LAST).Mapping_rec(pcollection(pcollection.LAST).Mapping_rec.LAST) :=
         col_mapping_rec;

     Else -- Case else
         null; -- do nothing          
     END CASE;
        
 END LOOP;
 
   pout_rtn_code := c_success;
   pout_rtn_msg := ''; --Null means Success
 
 
EXCEPTION

WHEN e_Invalid_Column_Mapping THEN 
 null; --do nothing messages are already assigned in validation section
WHEN OTHERS THEN
       pout_rtn_code := c_failure;
       pout_rtn_msg := v_section||'-'||SUBSTR(SQLERRM,1,200);
END Get_Profile_mapping;
/******************************************************************************/



PROCEDURE Parse_Header_and_det_data (
          pclob             IN  OUT NOCOPY   CLOB 
        , PEffective_DATE   IN   RISKDB.QP_EXT_PROVIDER_QUOTES.EXT_EFF_DATE%TYPE
        , pcollection       IN OUT NOCOPY Zema_Xtract_t
        , pout_rtn_code     OUT  NOCOPY NUMBER 
        , pout_rtn_msg      OUT  NOCOPY VARCHAR2  
 )
IS
v_section               VARCHAR2(1000);
Loc1                    NUMBER;
v_DataLoc               NUMBER;
v_Start_from_position   NUMBER;
v_Occurence             NUMBER := 1;
v_CHAR_Buffer           VARCHAR2(32767);
v_data_buffer           VARCHAR2(32767);
v_temp                  VARCHAR2(1000);
v_read_clobData         Boolean := FALSE;
v_header_Record_found   Boolean := FALSE;
v_data_record_found     Boolean := FALSE;
v_cur_hdr_label_found   Boolean := FALSE; 
crlf                    VARCHAR2(2) := chr(13)||chr(10); 
v_space                 VARCHAR2(1) := CHR(32);
v_Ampersand             VARCHAR2(1) := CHR(38);
v_colValue              VARCHAR2(1000);
v_startPosition         NUMBER;
v_commaLoc              NUMBER;
v_read_data             Boolean := FALSE;
eof_record              Boolean := FALSE;
v_no_of_columns         NUMBER;  --Data Occurence Tracker
v_n_Pattern_pos         NUMBER;
v_n_minus_one_Pattern_pos NUMBER;   
v_n_occurence             NUMBER;
v_n_minus_one_occurence   NUMBER;

v_data_Records            NUMBER := 0;     
v_n_Pattern               VARCHAR2(3);
v_n_minus_one_pattern     VARCHAR2(3); 


  
BEGIN
 v_section := 'Parsing Header data';

 PROCESS_LOG_REC.STAGE        := 'PARSE_HEADER_AND_DET_DATA';
 
  --Parse the clob Data 

 
 
  -- look for Linefeed character , which is CHR(10) from start position 
  --for the 1st occurence from starting position.
  
  v_Start_from_position := 1;
  v_read_clobData := TRUE;
  
  loc1   := DBMS_LOB.INSTR( pclob, crlf, v_Start_from_position, v_Occurence );
       
  -- if found and needs to read more data then loop
  
    WHILE ( Loc1 > 0 and v_read_clobData ) 
    LOOP
    
    BEGIN
       
        V_CHAR_Buffer := TRIM(DBMS_LOB.SUBSTR(pclob, (loc1-v_Start_from_position), v_Start_from_position ));
        
        --trim quote and , if any 
        V_CHAR_Buffer := TRIM(BOTH '"' FROM V_CHAR_BUFFER) ;
        V_CHAR_Buffer := TRIM(BOTH ',' FROM V_CHAR_BUFFER) ;
        
--        ----dbms_output.PUT_LINE (V_CHAR_Buffer);
               
        -- if Data record found , the first column needs to be 
        --effective_date and the remaining record Length 
        -- needs to be more than length 1 to be certain that it is a Good DATA RECORD.
        
         v_DataLoc := INSTR( V_CHAR_Buffer,TO_CHAR(PEffective_DATE, 'MM/DD/YYYY'),1,1);
         v_temp    :=  SUBSTR(V_CHAR_Buffer,11,length(V_CHAR_Buffer));
         
         
         -- If Data Record is FOUND then Set Data Record FOund flag
 
         IF (  NVL(v_DataLoc,0) > 0 AND length(NVL(v_temp,' ')) > 1 ) THEN 
            
            
            v_data_record_found := TRUE; -- turn on Data
            v_header_record_found := FALSE;  
            v_data_buffer := V_CHAR_Buffer;
                        
--          ----dbms_output.PUT_LINE ( 'Data Record Found ');
---      Data Record not Found 
         ELSE
          
          -- if last one was DATA Record  then 
          -- do not let header to be set as TRUE AGAIN rather
          -- make it FALSE , This could Just be an empty line in Data records section of FILE
           
           IF  v_data_record_found THEN 
           
            v_header_Record_found := FALSE;
           
           ELSE
           
            -- Header is TRue only until data is Found
            -- once datarecord found header should never be TRUE again 
            v_header_Record_found := TRUE;
--            ----dbms_output.PUT_LINE ( 'Header Record Found ');
           
           END IF;
        END IF;
        
        
         
       CASE 
       WHEN  v_header_record_found THEN
         
--           ----dbms_output.PUT_LINE ( 'Case HEader Record Found!'); 

         IF length(V_CHAR_Buffer) > 0 THEN      
          
           For indx in  pcollection.FIRST .. pcollection.LAST LOOP
           
            IF pcollection(indx).EXT_COLUMN IS NOT NULL THEN 
             
--              ----dbms_output.PUT_LINE ('pcollection('||indx||').EXT_COLUMN='||
--               pcollection(indx).EXT_COLUMN );
              
              IF 
              -- if EXT column header name found in Header LABEL  
              ( INSTR( REPLACE(Upper(V_CHAR_Buffer), v_space,v_Ampersand),
                        pcollection(indx).EXT_COLUMN,1,1) > 0
               ) 
              AND  
              -- if EXT column header name found in Header Record
              -- Replace all spaces with Amersand character
              ( Length(REPLACE(Upper(V_CHAR_Buffer), v_space ,v_Ampersand)) =
              Length(pcollection(indx).EXT_COLUMN)
              ) 
              THEN
              
                 -- mark it as Column Found 
                v_cur_hdr_label_found := TRUE; 
                pcollection(indx).EXT_COLUMN_FOUND := TRUE;
--                ----dbms_output.PUT_LINE (  'pcollection('||indx||').EXT_COLUMN = '||
--                 pcollection(indx).EXT_COLUMN||' Found!');
                 EXIT;
              ELSE
--                -- keep looking until we found or until all columns are checked
                CONTINUE;
              END IF ;
            
                        
            END IF; 
            -- end of IF pcollection(indx).EXT_COLUMN IS NOT NULL
            
           END LOOP;
          
          END IF;

       WHEN v_Data_Record_found THEN


         v_section := 'Parsing Detail data';
       
         PROCESS_LOG_REC.STAGE        := 'PARSE_DATA_Record';
       
        
            IF NVL(v_DataLoc,0) > 0 THEN 

               -- take the column mappings from 1 to n-1 column
               --Because commas guaranteed upto last but one column in a 
               --DATA RECORD      
               
             v_data_Records := v_data_Records + 1;
             
--             ----dbms_output.Put_line('------------'||v_data_Records||'-----------');   
              

              
               v_n_pattern_pos := 1;
               
               For indx in  pcollection.FIRST .. pcollection.LAST LOOP  

                v_colValue := '';
               
                -- IF EXTERNAL COLUMN is NULL DO not GO THRU PARSING LOGIC 
                -- SKIP IT  
                IF pcollection(indx).EXT_COLUMN IS NOT NULL THEN
                
                    --Usually Most files Comes with Pattern as en dof first element                  
                     v_n_Pattern := ',"';
                     v_n_pattern_pos := INSTR(V_Data_Buffer, v_n_Pattern ,1,1);
                     
                     CASE
                     WHEN  v_n_pattern_pos = 1   THEN
        
                           v_colValue := NULL;
                               
                     WHEN  v_n_pattern_pos > 1 THEN 
                        

                            v_colValue := SUBSTR(V_data_Buffer
                                                ,1
                                                , v_n_pattern_pos-1
                                               );
                                               
                            --advance pointer to new position i.e after pattern                    
                            v_n_pattern_pos := length(v_colValue)+length(v_n_Pattern);

                             ---Strip the Found column value from DATA Buffer along with pattern
                            v_data_buffer := SUBSTR(V_data_Buffer,v_n_pattern_pos );
                            

                            --strip any quotes and camma around the value
                            v_colValue := TRIM(BOTH '"' FROM v_colValue) ;
                            v_colValue := TRIM(BOTH ',' FROM v_colValue) ;
                           
                         -- Check content of Data Buffer reduced to empty? 
                           IF nvl(v_data_buffer,'0') = '0' THEN   
                              eof_record := TRUE;
                           ELSE
                              eof_record := FALSE;
                           END IF;
                       
                     ELSE -- case Else means No pattern found  

                           -- Until end of string its one value
                            v_colValue := SUBSTR(V_data_Buffer
                                                ,1
                                                );
                            
                            --advance pointer to new position i.e after pattern
                            v_n_pattern_pos := length(v_colValue)+length(v_n_Pattern);

                             ---Strip the Found column value from Buffer along with pattern
                            v_data_buffer := SUBSTR(V_data_Buffer,v_n_pattern_pos );
                            
                           --strip any quotes and camma around the value
                            v_colValue := TRIM(BOTH '"' FROM v_colValue) ;
                            v_colValue := TRIM(BOTH ',' FROM v_colValue) ;
                            
                           IF nvl(v_data_buffer,'0') = '0' THEN  
                              eof_record := TRUE;
                           ELSE
                              eof_record := FALSE;
                           END IF;  
                                            
                     END CASE; 
                                   

                      --Reset Variables for NEXT DATA ROW 
                ELSE 
                 -- If EXT COLUMN is NULL
                
                      --add fixed Value here 
                      v_colvalue := pcollection(indx).MAPPING_REC( pcollection(indx).MAPPING_REC.LAST).FIXED_VALUE;
                        
                END IF;
                            
                 --check if collection Is not initialized then 
                 --intitialize it first
                 IF pcollection(indx).Col_VALUE_REC IS NULL THEN
                     pcollection(indx).Col_VALUE_REC := COL_VALUE_T();
                 END IF;
                
        
                 -- If Collection member doesn't exist then Extend collection
                     -- add colValue to it       
                      IF NOT pcollection(indx).Col_VALUE_REC.EXISTS(v_data_Records) THEN 
                              
                         pcollection(indx).Col_VALUE_REC.EXTEND;
                           
                           --If Mapping found 
                           IF  pcollection(indx).EXT_COLUMN_FOUND THEN  
                              pcollection(indx).Col_VALUE_REC(v_data_Records) := v_colValue;
                                   
--                                ----dbms_output.PUT_LINE(
--                                'pcollection('||indx||').Col_VALUE_REC('||v_data_Records||')='||
--                                 pcollection(indx).Col_VALUE_REC(v_data_Records)
--                                ); 
                                null;
                           ELSE
                           
                             -- If EXT_COLUMN NULL THen Definitly 
                             -- Load Fixed Value to the collection
                             --which is collected into v_colValue above 
                              IF pcollection(indx).EXT_COLUMN IS  NULL THEN
                              
                                pcollection(indx).Col_VALUE_REC(v_data_Records) := v_colvalue;

--                                ----dbms_output.PUT_LINE('ASsigned Fixed Value'|| v_colvalue||' To '||
--                                'pcollection('||indx||').Col_VALUE_REC('||v_data_Records||')'
--                                ); 

                              ELSE
                                pcollection(indx).Col_VALUE_REC(v_data_Records) :=NULL;
                              END IF;
                              
                           END IF;
                                 
           
                      ELSE
                                    null; --Case element exists this should never happen 

                      END IF;


                
                
                 v_colValue := '';
               END LOOP; -- end of FOR LOOP   

            END IF;--IF NVL(v_DataLoc,0) > 0 THEN 
        
       ELSE  
        -- Case ekse
          ----dbms_output.PUT_LINE ( 'Must be Empty data Record close to EOF!');
          null;
                
       END CASE;
         
         --carriage return and linefeed makes up 2 characters
         -- so skip 2 positions from the position where last occurence was found
            
         v_Start_from_position := loc1+2 ;
         loc1   := DBMS_LOB.INSTR( pclob, crlf, v_Start_from_position, v_Occurence );
         
         --reset Variables
         v_DataLoc := 0;
         v_temp := '';
         v_cur_hdr_label_found := FALSE;
         
       EXCEPTION
       WHEN NO_DATA_FOUND THEN 
         ----dbms_output.PUT_LINE (v_section||'End of Reading');
         v_read_clobData :=  FALSE;
       END;  
     

--        ----dbms_output.Put_line('processed row='||v_data_Records); 

--       IF v_data_Records = 3 THEN 
--          v_read_clobData :=  FALSE;
--       END IF;
       
     END LOOP;
       
 
     pout_rtn_code  := c_success;
     pout_rtn_msg   := '';
     
EXCEPTION
WHEN OTHERS THEN 
    pout_rtn_code := c_failure;
    pout_rtn_msg := v_section||'-'||SUBSTR(SQLERRM,1,200);
    ----dbms_output.put_line(pout_rtn_msg);
END Parse_Header_and_det_data;
/******************************************************************************/


PROCEDURE DELETE_RECORDS ( pDelete_statement IN  VARCHAR2
                           ,pout_rtn_code     OUT  NOCOPY NUMBER  
                           ,pout_rtn_msg      OUT  NOCOPY VARCHAR2 
                           )
  IS
  PRAGMA AUTONOMOUS_TRANSACTION;
  delete_cursor         INTEGER;
  rows_deleted          INTEGER;

  v_section             VARCHAR2(1000);
BEGIN

PROCESS_LOG_REC.STAGE        := 'DELETE_RECORDS';
PROCESS_LOG_REC.MESSAGE := 'Attempting to DELETE: '||pDelete_statement ;


v_section             := 'Dynamic SQL DELETE Execute ';
delete_cursor := dbms_sql.open_cursor;


DBMS_SQL.PARSE( delete_cursor
               , pDelete_statement 
               , DBMS_SQL.NATIVE
               ); 
               
    rows_deleted := DBMS_SQL.EXECUTE(delete_cursor);                

    dbms_output.PUT_LINE('rows deleted'||rows_deleted);
    
 -- Commit and close all cursors: 
     COMMIT; 
     DBMS_SQL.CLOSE_CURSOR(delete_cursor ); 
  
  pout_rtn_code := c_success;
  pout_rtn_msg := ''; --Null means Success     
     
EXCEPTION
WHEN OTHERS THEN 
   pout_rtn_code := c_failure;
   pout_rtn_msg := v_section||'-'||SUBSTR(SQLERRM,1,200);
  --dbms_output.put_line(pout_rtn_msg);
END DELETE_RECORDS;


procedure add_to_value_list (
pext_profileId   IN NUMBER 
, pvaluelist       IN OUT NOCOPY value_list_tab
, pout_rtn_code     OUT  NOCOPY NUMBER  
,pout_rtn_msg      OUT  NOCOPY VARCHAR2      
)
is
PRAGMA AUTONOMOUS_TRANSACTION;
vc_section         Varchar2(1000) := 'add_to_value_list'; 
v_rtn_code                   NUMBER;
v_rtn_msg                    VARCHAR2(500);
v_delete_sql_stmt           VARCHAR2(2000);


dml_errors                  EXCEPTION;
PRAGMA EXCEPTION_INIT(dml_errors, -24381);
v_error_index               NUMBER;

e_delete_ValueList_error    EXCEPTION;
   PRAGMA EXCEPTION_INIT( e_delete_ValueList_error , -20045);
   
begin
 PROCESS_LOG_REC.STAGE        := 'add_to_value_list'; 
 


    --- Dynamic SQL operation  to Delete if any for current Effective date
     v_delete_sql_stmt := 'DELETE RISKDB.QP_EXT_VALUES_LIST WHERE '||
                         'EXT_PROFILE_ID ='||pext_profileId;
        
                        

    Delete_records (
    pDelete_statement => v_delete_sql_stmt
    , pout_rtn_code     => v_rtn_code 
    , pout_rtn_msg      => v_rtn_msg  
    );
      
       --check success/failure
   IF v_rtn_code <> c_success THEN    
      RAISE e_delete_ValueList_error;
   END IF;
 
 -- insert value list Values  
  BEGIN
    
     
     INSERT INTO RISKDB.QP_EXT_VALUES_LIST(
     EXT_PROFILE_ID
     ,ZEMA_PROFILE_ID
     ,EXT_COLUMN_ORDER
     ,EXT_COLUMN_NAME
     , CONCAT_SEQUENCE_ID
     , CONCAT_COL_Value
     , ACTIVE_FLAG 
     ) 
     SELECT DISTINCT
     p.EXT_PROFILE_ID
     , p.ZEMA_PROFILE_ID
     , p.EXT_COLUMN_ORDER
     , p.EXT_COLUMN_NAME
     , p.CONCAT_SEQUENCE_ID
     , p.CONCAT_COL_Value
     , 'Y'
     FROM 
     TABLE( pvaluelist ) P
     where
     p.EXT_PROFILE_ID = pext_profileId
     ;
     
  EXCEPTION
  WHEN DML_ERRORS THEN
     pout_rtn_code := c_failure;
     
    DMR.PROCESS_LOG_PKG.WRITE_LOG(
                    P_PROCESS_NAME => PROCESS_LOG_REC.PROCESS_NAME,
                    P_STAGE=> PROCESS_LOG_REC.STAGE,
                    P_STATUS=>PROCESS_LOG_PKG.C_STATUS_ERRORS,
                    P_MESSAGE=>NVL( pout_rtn_msg, SQLERRM) 
                    ); 
                    
  WHEN OTHERS THEN 
  
    pout_rtn_msg := SQLERRM(SQLCODE);
           

    DMR.PROCESS_LOG_PKG.WRITE_LOG(
                        P_PROCESS_NAME => PROCESS_LOG_REC.PROCESS_NAME,
                        P_STAGE=> PROCESS_LOG_REC.STAGE,
                        P_STATUS=>PROCESS_LOG_PKG.C_STATUS_ERRORS,
                        P_MESSAGE=>NVL( pout_rtn_msg, SQLERRM) 
                        );

  END;

  COMMIT; -- commit whatever is good 
  --set to NUL to release collection data 
  
  pvaluelist := value_list_tab();
       
  pout_rtn_code := c_success;
  pout_rtn_msg := ''; --Null means Success

EXCEPTION  
WHEN e_delete_ValueList_error THEN 
       ROLLBACK;
       pout_rtn_code := c_failure;
       pout_rtn_msg := SUBSTR(SQLERRM,1,200);
WHEN OTHERS THEN 
       ROLLBACK;
       pout_rtn_code := c_failure;
       pout_rtn_msg := vc_section||SUBSTR(SQLERRM,1,200);
       ----dbms_output.put_line(pout_rtn_msg);
end add_to_value_list;


 PROCEDURE  Prepare_Dataset_to_Load (
    pzemaprof_id      IN   riskdb.qp_zema_profile_detail.Profile_id%TYPE
   ,PEffective_DATE   IN   RISKDB.QP_EXT_PROVIDER_QUOTES.EXT_EFF_DATE%TYPE 
   ,pcollection       IN   Zema_Xtract_t
   , pqvarstage       IN OUT NOCOPY QVAR_Stage_T
   , pvaluelist       IN OUT NOCOPY value_list_tab
   ,pout_rtn_code     OUT  NOCOPY NUMBER  
   ,pout_rtn_msg      OUT  NOCOPY VARCHAR2   
 ) 
IS
vc_section         Varchar2(1000) := 'Prepare_Dataset_to_Load'; 
vl_prov_quote_seq     NUMBER;  
vl_ext_profileid       RISKDB.QP_EXT_PROFILES.EXT_PROFILE_ID%TYPE;
vl_ext_providerid      RISKDB.QP_EXT_PROFILES.EXT_PROFILE_ID%TYPE;
vc_underlying_comdty   RISKDB.QP_EXT_PROFILES.UNDERLYING_COMMODITY%TYPE;
vc_process_level      RISKDB.QP_EXT_PROVIDER_QUOTES.PROCESS_LEVEL%TYPE := '1';
vc_Active_flag        RISKDB.QP_EXT_PROVIDER_QUOTES.ACTIVE_FLAG%TYPE := 'Y' ;
vc_comments           RISKDB.QP_EXT_PROVIDER_QUOTES.Comments%TYPE := 'ZEMA RAW DATA';    
v_cncat_val           VARCHAR2(1000);
li_colord_indx        NUMBER;

v_rtn_code             NUMBER;
v_rtn_msg              VARCHAR2(500);
vl_extloc_colorder     NUMBER(2); 

e_valuelist_exception         EXCEPTION;
       PRAGMA EXCEPTION_INIT(e_valuelist_exception, -20045);

                
-- Intialize Collection 
Procedure INITIALIZE_QVARStg_collection IS
BEGIN
   pqvarstage := QVAR_Stage_T();
   pvaluelist := value_list_tab(); -- object array with no elements
END INITIALIZE_QVARStg_collection;


    PROCEDURE Assign_QVAR_stg_Collection (
    pColumnName       IN VARCHAR2
    ,pcolvalue        IN VARCHAR2
    ) 
    IS

    e_Unknown_QVAR_stg_col         EXCEPTION;
       PRAGMA EXCEPTION_INIT(e_Unknown_QVAR_stg_col, -20010);
    
    vc_singlequote         VARCHAR2(1) := CHR(39);
    vc_process_Level       VARCHAR2(3) :=vc_singlequote||'1'||vc_singlequote;
    vc_comments            RISKDB.QP_EXT_PROVIDER_QUOTES.COMMENTS%TYPE := vc_singlequote||'ZEMA RAW DATA'||vc_singlequote;
    -- Fresh insert would always have active Flag as 'Y'
    vc_activeflag          VARCHAR2(3) := vc_singlequote||'Y'||vc_singlequote;
    vl_col_order           RISKDB.QP_EXT_NEER_COL_MAPPINGS.COLUMN_ORDER%TYPE;
    vc_col_Datatype        RISKDB.QP_EXT_NEER_COL_MAPPINGS.DATA_TYPE%TYPE;
    vc_display_format      RISKDB.QP_EXT_NEER_COL_MAPPINGS.DISPLAY_FORMAT%TYPE;
    vc_seperator           VARCHAR2(3) := ' , ';
    vc_dateFormat          VARCHAR2(12) := vc_singlequote||'MM/DD/YYYY'||vc_singlequote;
    vd_date_val            DATE;
    vc_varchar_val         VARCHAR2(400);
    vl_Number_no_dec       NUMBER;
    vl_number_with_dec     NUMBER(12,3);
                      

    BEGIN

      
        CASE 
        WHEN Upper(pColumnName) = 'EXT_PROVIDER_QUOTE_ID' THEN
          pqvarStage(pqvarstage.LAST).EXT_PROVIDER_QUOTE_ID :=  TO_NUMBER(pcolvalue);
        WHEN Upper(pColumnName) = 'EXT_PROVIDER_ID'       THEN 
          pqvarStage(pqvarstage.LAST).EXT_PROVIDER_ID :=  TO_NUMBER(pcolvalue);
        WHEN Upper(pColumnName) = 'EXT_PROFILE_ID'        THEN
         pqvarStage(pqvarstage.LAST).EXT_PROFILE_ID := TO_NUMBER(pcolvalue);
        WHEN Upper(pColumnName) = 'UNDERLYING_COMMODITY'  THEN
         pqvarStage(pqvarstage.LAST).UNDERLYING_COMMODITY := pcolvalue;
        WHEN Upper(pColumnName) = 'EXT_EFF_DATE'          THEN
         pqvarStage(pqvarstage.LAST).EXT_EFF_DATE := TO_DATE(pcolvalue, vc_dateFormat);
        WHEN Upper(pColumnName) = 'EXT_RAW_CONTRACT'      THEN
         pqvarStage(pqvarstage.LAST).EXT_RAW_CONTRACT := pcolvalue;
        WHEN Upper(pColumnName) = 'MID'                   THEN
         pqvarStage(pqvarstage.LAST).MID := TO_NUMBER(pcolvalue, '999999999999.0000000000');
        WHEN Upper(pColumnName) = 'BID'                   THEN
         pqvarStage(pqvarstage.LAST).BID := TO_NUMBER(pcolvalue, '999999999999.0000000000');
        WHEN Upper(pColumnName) = 'ASK'                   THEN
         pqvarStage(pqvarstage.LAST).ASK := TO_NUMBER(pcolvalue, '999999999999.0000000000');
        WHEN Upper(pColumnName) = 'ON_MID'                THEN
         pqvarStage(pqvarstage.LAST).ON_MID := TO_NUMBER(pcolvalue, '999999999999.0000000000');
        WHEN Upper(pColumnName) = 'ON_BID'                THEN
         pqvarStage(pqvarstage.LAST).ON_BID := TO_NUMBER(pcolvalue, '999999999999.0000000000');
        WHEN Upper(pColumnName) = 'ON_ASK'                THEN
         pqvarStage(pqvarstage.LAST).ON_ASK := TO_NUMBER(pcolvalue, '999999999999.0000000000');
        WHEN Upper(pColumnName) = 'OFF_MID'               THEN
         pqvarStage(pqvarstage.LAST).OFF_MID := TO_NUMBER(pcolvalue, '999999999999.0000000000');
        WHEN Upper(pColumnName) = 'OFF_BID'               THEN
         pqvarStage(pqvarstage.LAST).OFF_BID := TO_NUMBER(pcolvalue, '999999999999.0000000000');
        WHEN Upper(pColumnName) = 'OFF_ASK'               THEN
         pqvarStage(pqvarstage.LAST).OFF_ASK := TO_NUMBER(pcolvalue, '999999999999.0000000000');
        WHEN Upper(pColumnName) = 'STRIP_TENOR'           THEN
         pqvarStage(pqvarstage.LAST).STRIP_TENOR:= pcolvalue;
        WHEN Upper(pColumnName) = 'CONTRACT_MONTH'        THEN
         pqvarStage(pqvarstage.LAST).CONTRACT_MONTH := pcolvalue;
        WHEN Upper(pColumnName) = 'CONTRACT_YEAR'         THEN
         pqvarStage(pqvarstage.LAST).CONTRACT_YEAR := pcolvalue;
        WHEN Upper(pColumnName) = 'EXT_CATEGORY'          THEN
         pqvarStage(pqvarstage.LAST).EXT_CATEGORY := pcolvalue;
        WHEN Upper(pColumnName) = 'EXT_LOCATION'          THEN
         pqvarStage(pqvarstage.LAST).EXT_Location := pcolvalue;
        WHEN Upper(pColumnName) = 'NEER_HUB'              THEN
         pqvarStage(pqvarstage.LAST).NEER_HUB := pcolvalue;
        WHEN Upper(pColumnName) = 'NEER_LOCATION'         THEN
         pqvarStage(pqvarstage.LAST).NEER_LOCATION := pcolvalue;
        WHEN Upper(pColumnName) = 'PEAK_TYPE'             THEN
         pqvarStage(pqvarstage.LAST).PEAK_TYPE := pcolvalue;
        WHEN Upper(pColumnName) = 'PRICE_TYPE'            THEN
         pqvarStage(pqvarstage.LAST).PRICE_TYPE := pcolvalue;
        WHEN Upper(pColumnName) = 'HOUR_TYPE'             THEN
         pqvarStage(pqvarstage.LAST).HOUR_TYPE := pcolvalue;
        WHEN Upper(pColumnName) = 'VALUE'                 THEN
         pqvarStage(pqvarstage.LAST).VALUE := TO_NUMBER(pcolvalue, '999999999999.0000000000');
        WHEN Upper(pColumnName) = 'PROCESS_LEVEL'         THEN
         pqvarStage(pqvarstage.LAST).PROCESS_LEVEL := pcolvalue;
        WHEN Upper(pColumnName) = 'COMMENTS'              THEN
         pqvarStage(pqvarstage.LAST).COMMENTS := pcolvalue;
        WHEN Upper(pColumnName) = 'ACTIVE_FLAG'            THEN
         pqvarStage(pqvarstage.LAST).ACTIVE_FLAG := pcolvalue;
        WHEN Upper(pColumnName) = 'EXT_HUB_MAPPED_FLAG'    THEN
          pqvarStage(pqvarstage.LAST).EXT_HUB_MAPPED_FLAG := pcolvalue;
        WHEN Upper(pColumnName) = 'EXT_LOC_MAPPED_FLAG'    THEN
          pqvarStage(pqvarstage.LAST).EXT_LOC_MAPPED_FLAG := pcolvalue;
        WHEN Upper(pColumnName) = 'NEER_LOC_MAPPED_FLAG'   THEN
          pqvarStage(pqvarstage.LAST).NEER_LOC_MAPPED_FLAG := pcolvalue;
        WHEN Upper(pColumnName) = 'NEER_HUB_MAPPED_FLAG'   THEN
          pqvarStage(pqvarstage.LAST).NEER_HUB_MAPPED_FLAG := pcolvalue;     
        WHEN Upper(pColumnName) = 'STRIP_TENOR_MAPPED_FLAG' THEN
          pqvarStage(pqvarstage.LAST).STRIP_TENOR_MAPPED_FLAG := pcolvalue;
        WHEN Upper(pColumnName) = 'SECONDARY_CALCULATION_FLAG' THEN
         pqvarStage(pqvarstage.LAST).SECONDARY_CALCULATION_FLAG := pcolvalue;                         
        WHEN Upper(pColumnName) = 'DATA_STATUS' THEN
         pqvarStage(pqvarstage.LAST).DATA_STATUS := pcolvalue;
        ELSE 
          RAISE e_Unknown_QVAR_stg_col;
        END CASE;
    
    EXCEPTION
    WHEN e_Unknown_QVAR_stg_col THEN 
     Raise_application_Error (-20010, 'Unrecognised QVAR Stage column, failed to prepare Value');
     
    END Assign_QVAR_stg_Collection;



 BEGIN
 
 IF pqvarstage is NULL then 

  INITIALIZE_QVARStg_collection;
 
 END IF;
 
 

 -- Get the surrogate key values 
 -- for provider and profile id 
    Select 
    pr.EXT_PROVIDER_ID
    , pr.EXT_PROFILE_ID
    , pr.UNDERLYING_COMMODITY 
    ,( select map.COLUMN_ORDER
      FROM RISKDB.QP_EXT_NEER_COL_MAPPINGS map
      where
      map.ZEMA_PROFILE_ID = pr.ZEMA_PROFILE_ID
      and EXT_COLUMN_NAME = 'EXT_LOCATION'
      and ACTIVE_FLAG = 'Y'
      and trunc(PEffective_DATE) BETWEEN trunc(EFFECTIVE_START_DATE) and 
                                         NVL(trunc(EFFECTIVE_END_DATE), trunc(PEffective_DATE))
    ) extloc_colorder
    INTO 
    vl_ext_providerid 
    , vl_ext_profileid 
    , vc_underlying_comdty
    , vl_extloc_colorder
    FROM RISKDB.QP_EXT_PROFILES pr
    where
    pr.SOURCE_SYSTEM = 'QVAR'
    and pr.ACTIVE_FLAG = 'Y'
    and pr.ZEMA_PROFILE_ID = pzemaprof_id
    and trunc(pEffective_DATE) BETWEEN trunc(pr.EFFECTIVE_START_DATE) and 
                                         NVL(trunc(pr.EFFECTIVE_END_DATE), trunc(pEffective_DATE))

    ;
 



 FOR data_indx in pcollection(1).Col_VALUE_REC.FIRST .. pcollection(1).Col_VALUE_REC.LAST LOOP
 
  ------dbms_output.put_line( 'data_indx='||data_indx);
  
 -- check that QVAR stage table is ready to recieve member values
 -- if not EXTEND and start storing values 
 
 
      IF NOT pqvarstage.exists(data_indx) THEN 
         pqvarstage.EXTEND;
         
         Select RISKDB.QP_EXT_PRVDR_QUOTES_SEQ.NEXTVAL
         into vl_prov_quote_seq
         FROM DUAL;
         
        -- Prepare all constant values that needs to be generated 
          Assign_QVAR_stg_Collection (
            'EXT_PROVIDER_QUOTE_ID'
            ,vl_prov_quote_seq
            ); 
    
           Assign_QVAR_stg_Collection (
            'EXT_PROVIDER_ID'
            ,vl_ext_providerid
            ); 

           Assign_QVAR_stg_Collection (
            'EXT_PROFILE_ID'
            ,vl_ext_profileid
            ); 
            
           Assign_QVAR_stg_Collection (
            'UNDERLYING_COMMODITY'
            ,vc_underlying_comdty
            ); 

           Assign_QVAR_stg_Collection (
            'PROCESS_LEVEL'
            ,vc_process_level
            ); 

           Assign_QVAR_stg_Collection (
            'ACTIVE_FLAG'
            ,vc_active_flag
            ); 

           Assign_QVAR_stg_Collection (
            'COMMENTS'
            ,vc_comments
            ); 
           
           Assign_QVAR_stg_Collection (
            'EXT_HUB_MAPPED_FLAG'
            ,'N'
           ); 

           Assign_QVAR_stg_Collection (
            'EXT_LOC_MAPPED_FLAG'
            ,'N'
           ); 

           Assign_QVAR_stg_Collection (
            'NEER_LOC_MAPPED_FLAG'
            ,'N'
           ); 

           Assign_QVAR_stg_Collection (
            'NEER_HUB_MAPPED_FLAG'
            ,'N'
           ); 

           Assign_QVAR_stg_Collection (
            'STRIP_TENOR_MAPPED_FLAG'
            ,'N'
           ); 

           Assign_QVAR_stg_Collection (
            'SECONDARY_CALCULATION_FLAG'
            ,'N'
           ); 

           Assign_QVAR_stg_Collection (
            'DATA_STATUS'
            ,'R'
           ); 
           
      ELSE

         null;
         
      END IF ;
        
     
      FOR  col_maps_idx in pcollection.FIRST .. pcollection.LAST LOOP

           v_cncat_val := ''; -- clear concatenated string
           li_colord_indx := NULL; -- clear
         
         -- column values provided by Zema profile
         -- and marked as to be loaded then we are interested in this value
        IF  pcollection(col_maps_idx).mapping_rec(1).LOAD_STAGE_FLAG = 'Y' THEN
        
        -- find out is this column is a member of any concatenation groups 
          IF pcollection(col_maps_idx).EXt_COLUMN is NOT NULL AND 
             pcollection(col_maps_idx).concat_groups.COUNT > 1 THEN
          
           
           FOR group_idx in pcollection(col_maps_idx).concat_groups.FIRST .. pcollection(col_maps_idx).concat_groups.LAST
           LOOP
           
            IF  pcollection(col_maps_idx).concat_groups.EXISTS(group_idx)THEN

--             ----dbms_output.PUT_LINE(pcollection(col_maps_idx).EXT_COLUMN||' is '||
--             'Grouped Under '||pcollection(col_maps_idx).mapping_rec(1).NEER_COLUMN_NAME||
--             ' '||' col ord='||pcollection(col_maps_idx).concat_groups(group_idx).column_order||
--             ' '||' seq ='||pcollection(col_maps_idx).concat_groups(group_idx).concat_sequence
--             );

                li_colord_indx := pcollection(col_maps_idx).concat_groups(group_idx).column_order;
               
                -- li_colord_indx represent which Zema column extcolumn
                -- data_indx represents which ROw are included in catenated Value  
                v_cncat_val := v_cncat_val||pcollection(li_colord_indx).Col_VALUE_REC(data_indx)||'|'; 
               
                v_rtn_code  := NULL;
                v_rtn_msg   := NULL;            
                -- add Value list Logic here
                -- only for QVAR SYSTEMS vl_ext_profileid > 0 
               IF pcollection(li_colord_indx).Mapping_rec(1).NEER_COLUMN_NAME = 'EXT_LOCATION' THEN  
                



--                     null;
--                    add_to_value_list (
--                      pzemaprof_id     => pzemaprof_id
--                    , pext_prof_id     => vl_ext_profileid
--                    , pcolOrder        => li_colord_indx
--                    , pcolumnName      => pcollection(col_maps_idx).EXT_COLUMN
--                    , pcolSequence     => pcollection(col_maps_idx).concat_groups(group_idx).concat_sequence
--                    , pcolseq_value    => pcollection(li_colord_indx).Col_VALUE_REC(data_indx)
--                    , pext_location    => pcollection(vl_extloc_colorder).Col_VALUE_REC(data_indx)
--                    , pout_rtn_code    => v_rtn_code 
--                    , pout_rtn_msg      => v_rtn_msg
--                   );
--                   
               pvaluelist.EXTEND;
                   
               pvaluelist(Pvaluelist.count):= VALUE_LIST_T( vl_ext_profileid
                                                         ,pzemaprof_id
                                                         ,li_colord_indx
                                                         , pcollection(li_colord_indx).EXT_COLUMN
                                                         , pcollection(li_colord_indx).concat_groups(group_idx).concat_sequence
                                                         , pcollection(li_colord_indx).Col_VALUE_REC(data_indx)
                                                        );  
                   
                 
--               ----dbms_output.PUT_LINE(
--               vl_ext_profileid||','||
--               pzemaprof_id||','||
--               li_colord_indx||','||
--               pcollection(li_colord_indx).EXT_COLUMN||','||
--               pcollection(li_colord_indx).concat_groups(group_idx).concat_sequence||','||
--               pcollection(li_colord_indx).Col_VALUE_REC(data_indx)
--               );                   
--                 IF v_rtn_code <> c_success THEN
--                    RAISE e_valuelist_exception;
--                 END IF;
    
                   
               END IF;
                   
            END IF;
            
           END LOOP;
           
           -- end of concat groups
           --strip Trailing Pipe character
           v_cncat_val := TRIM(TRAILING '|' FROM v_cncat_val) ;
--           ----dbms_output.PUT_LINE('concatenated value is '||v_cncat_val);
              
          ELSE
--             ----dbms_output.PUT_LINE(col_maps_idx||pcollection(col_maps_idx).EXT_COLUMN||' Not Grouped '); 
     
                -- col_maps_idx represent which Zema column extcolumn
                -- data_indx represents which ROw are included in catenated Value  
             v_cncat_val := pcollection(col_maps_idx).Col_VALUE_REC(data_indx); 
            
--              ----dbms_output.PUT_LINE( v_cncat_val);
           
            IF pcollection(col_maps_idx).Mapping_rec(1).NEER_COLUMN_NAME = 'EXT_LOCATION' THEN  
                



               pvaluelist.EXTEND;
                   
--               ----dbms_output.PUT_LINE(pvaluelist.COUNT);
                
               pvaluelist(pvaluelist.COUNT):= VALUE_LIST_T( vl_ext_profileid
                                                         ,pzemaprof_id
                                                         ,pcollection(col_maps_idx).mapping_rec(1).COLUMN_ORDER
                                                         , pcollection(col_maps_idx).EXT_COLUMN
                                                         , NULL
                                                         , v_cncat_val
                                                        );  
                   
--               ----dbms_output.PUT_LINE('constructed');
                 
--               ----dbms_output.PUT_LINE(
--               col_maps_idx ||','||
--               vl_ext_profileid||','||
--               pzemaprof_id||','||
--               pcollection(col_maps_idx).mapping_rec(1).COLUMN_ORDER||','||
--               pcollection(col_maps_idx).EXT_COLUMN||','||
--               ' '||','||
--               pcollection(col_maps_idx).Col_VALUE_REC(data_indx)
--               );
             END IF ; --end of ext_location check 
                   
          END IF;
          
           -- assign this to QVAR Appropriate QVAR collection
           Assign_QVAR_stg_Collection (
            pcollection(col_maps_idx).Mapping_rec(1).NEER_COLUMN_NAME
            ,v_cncat_val
            ); 
           
        ELSE
        -- if loading flag <> Y OR NEER_COLUMN_NAME is NULL then 

           null;
        END IF; 
          
          
      END LOOP;
  -- end of column Mapping collection 
 
 END LOOP;
 -- end of data values 
 
 -- add the External values of this of this Profile
 -- only if its QVAR system 
 
 v_rtn_code  := NULL;
 v_rtn_msg   := NULL;     
                
 IF vl_ext_profileid > 0 then 
    add_to_value_list (
    pext_profileId      => vl_ext_profileid  
    , pvaluelist        =>  pvaluelist
    , pout_rtn_code     =>  v_rtn_code  
    ,pout_rtn_msg       =>  v_rtn_msg      
    );
 END IF;
 

 
   pout_rtn_code := c_success;
   pout_rtn_msg := ''; --Null means Success



EXCEPTION    
WHEN OTHERS THEN 

       pout_rtn_code := c_failure;
       pout_rtn_msg := vc_section||SUBSTR(SQLERRM,1,200);
       ----dbms_output.put_line(pout_rtn_msg);  
END Prepare_Dataset_to_Load;
     

 PROCEDURE   Load_target (pcollection       IN OUT NOCOPY QVAR_Stage_T
                          ,pout_rtn_code     OUT  NOCOPY NUMBER  
                          ,pout_rtn_msg      OUT  NOCOPY VARCHAR2
                          )
IS
vc_section         Varchar2(1000) := 'Load_target'; 
dml_errors EXCEPTION;
PRAGMA EXCEPTION_INIT(dml_errors, -24381);
v_error_index       NUMBER;

BEGIN  



FORALL i IN INDICES OF pcollection SAVE EXCEPTIONS
 INSERT INTO RISKDB.QP_EXT_PROVIDER_QUOTES VALUES pcollection(i);


   pout_rtn_code := c_success;
   pout_rtn_msg := ''; --Null means Success

EXCEPTION
WHEN DML_ERRORS THEN
 pout_rtn_code := c_failure;
     
FOR i IN 1..SQL%BULK_EXCEPTIONS.COUNT LOOP
pout_rtn_msg := SQLERRM(-(SQL%BULK_EXCEPTIONS(i).ERROR_CODE));
v_error_index := SQL%BULK_EXCEPTIONS(i).ERROR_INDEX;
pout_rtn_msg := pout_rtn_msg||'Error_index='|| v_error_index;



   BEGIN
   
      INSERT INTO RISKDB.QP_EXT_PROVIDER_QUOTES VALUES pcollection(v_error_index);
      
   EXCEPTION
   WHEN OTHERS THEN 
   pout_rtn_msg := SQLERRM(SQLCODE);
   

    DMR.PROCESS_LOG_PKG.WRITE_LOG(
                        P_PROCESS_NAME => PROCESS_LOG_REC.PROCESS_NAME,
                        P_STAGE=> PROCESS_LOG_REC.STAGE,
                        P_STATUS=>PROCESS_LOG_PKG.C_STATUS_ERRORS,
                        P_MESSAGE=>NVL( pout_rtn_msg, SQLERRM) 
                        );
   END;                        
END LOOP;                        

WHEN OTHERS THEN 

       pout_rtn_code := c_failure;
       pout_rtn_msg := vc_section||SUBSTR(SQLERRM,1,200);
      ----dbms_output.put_line(pout_rtn_msg);
      
END   Load_target;

       
PROCEDURE EXTERNAL_LOAD(
pzemaprofile_id    IN  riskdb.qp_zema_profile_detail.profile_id%TYPE 
, PEffective_DATE  IN  DATE  
)
IS
   vd_cobdate    DATE; 
   vc_cobdate    VARCHAR2(20);
   
v_active                     VARCHAR2(1);
v_rtn_code                   NUMBER;
v_rtn_msg                    VARCHAR2(500);
v_url_string                 VARCHAR2(3000);
r_control_rec                Riskdb.Zema_control%ROWTYPE;
r_rec                        riskdb.qp_zema_profile_detail%ROWTYPE;
   
v_rtn                        NUMBER;
b_try_http_request           Boolean :=  FALSE;
v_http_attempts              PLS_INTEGER := 0; 
xtracted_data                Zema_Xtract_t;
v_clob                       riskdb.qp_zema_profile_detail.DATA_CLOB%TYPE;

v_Start_from_position        NUMBER := 1;

v_commit_interval            NUMBER := 1000;

v_singlequote               VARCHAR2(1) := CHR(39);
v_seperator                 VARCHAR2(3) := ' , ';

v_ext_profileid             RISKDB.QP_EXT_PROFILES.EXT_PROFILE_ID%TYPE;
v_ext_providerid            RISKDB.QP_EXT_PROFILES.EXT_PROFILE_ID%TYPE;
v_underlying_comdty         RISKDB.QP_EXT_PROFILES.UNDERLYING_COMMODITY%TYPE;

v_delete_sql_stmt           VARCHAR2(2000);

v_dateFormat                VARCHAR2(12) := v_singlequote||'MM/DD/YYYY'||v_singlequote;
v_eff_date                  VARCHAR2(100);

v_qvarstage_collection       QVAR_Stage_T;
v_valuelist                  value_list_tab;

 e_Invalid_ProfileId_error      EXCEPTION;
   PRAGMA EXCEPTION_INIT(e_Invalid_ProfileId_error, -20001);

 e_Missing_EffectiveDate_error  EXCEPTION;
   PRAGMA EXCEPTION_INIT(e_Missing_EffectiveDate_error, -20002);
   
 e_Invalid_Zema_ctl_rec         EXCEPTION;
   PRAGMA EXCEPTION_INIT(e_Invalid_Zema_ctl_rec, -20003);
 
 e_Invalid_Zema_profile_rec     EXCEPTION;
    PRAGMA EXCEPTION_INIT(e_Invalid_Zema_profile_rec, -20004);

 e_Appl_error                   EXCEPTION;
   PRAGMA EXCEPTION_INIT( e_Appl_error , -20005);
 
 e_Invalid_FileFormat_error     EXCEPTION;
   PRAGMA EXCEPTION_INIT( e_Invalid_FileFormat_error , -20006);
   
 e_CLOB_Value_Null              EXCEPTION;
   PRAGMA EXCEPTION_INIT( e_CLOB_Value_Null , -20007);
       
 e_Invalid_Column_Mapping       EXCEPTION;
     PRAGMA EXCEPTION_INIT( e_Invalid_Column_Mapping , -20008); 
 e_Zema_Prof_dtl_upd_failed     EXCEPTION;
   PRAGMA EXCEPTION_INIT( e_Zema_Prof_dtl_upd_failed , -20009); 
       

 
 ----??????????????????????????????????????
Procedure INIT_collection IS
BEGIN
   xtracted_data := Zema_Xtract_t();
END INIT_collection;

BEGIN

    --Set application Name for the Log Process
    DMR.PROCESS_LOG_PKG.CONSTRUCTOR ('QVAR_APP' );
    
     --Set Process Name and status for the Log Process                   
    PROCESS_LOG_REC.PROCESS_NAME :=  'QP_ZEMA_EXTRACT_PKG';
    PROCESS_LOG_REC.PARENT_NAME  := 'EXTERNAL_LOAD';
    PROCESS_LOG_REC.STAGE        := 'EXTERNAL_LOAD';
    PROCESS_LOG_REC.STATUS       := PROCESS_LOG_PKG.C_STATUS_STARTED;
    PROCESS_LOG_REC.MESSAGE      := 'EXTERNAL_LOAD EXTRACT process Started...';
    
    DMR.PROCESS_LOG_PKG.SET_PROCESS_NAME(
    P_PROCESS_NAME =>PROCESS_LOG_REC.PROCESS_NAME
    , P_PARENT_NAME => PROCESS_LOG_REC.PARENT_NAME
    );
   

   -- Log Message first
   DMR.PROCESS_LOG_PKG.WRITE_LOG(
                            P_PROCESS_NAME => PROCESS_LOG_REC.PROCESS_NAME,
                            P_STAGE=> PROCESS_LOG_REC.STAGE,
                            P_STATUS=>PROCESS_LOG_REC.STATUS,
                            P_MESSAGE=> PROCESS_LOG_REC.MESSAGE);
  

    IF  NVL(pzemaprofile_id,0) = 0 
    THEN
         Raise e_Invalid_ProfileId_error;
    END IF;



    IF PEffective_DATE is NULL THEN 

     Get_current_COBDATE (
      pcobdate =>   vd_cobdate  
     ,pout_rtn_code     => v_rtn_code
     ,pout_rtn_msg      => v_rtn_msg 
     );
     
     vc_cobdate       := to_CHAR( vd_cobdate, 'DD-MON-YYYY');
     vd_cobdate       := to_date( vc_cobdate, 'DD-MON-YYYY');
     
    ELSE


    vc_cobdate       := to_CHAR(PEffective_DATE, 'DD-MON-YYYY');
    vd_cobdate       := to_date(vc_cobdate, 'DD-MON-YYYY');

    --vd_default_effective_Date := vd_cobdate;

    END IF;


   PROCESS_LOG_REC.STAGE        := 'fetch_control'; 
   v_active := '%';
    
   v_rtn     := fetch_control(
                                rec_control => r_control_rec
                                , o_rtn_msg => v_rtn_msg
                             );
   
    IF v_rtn <> c_success THEN
          RAISE e_appl_error;
    END IF;
   
    -- if successful fetch Validate values                  
    IF ( Trim(r_control_rec.base_url) IS NULL
         OR Trim(r_control_rec.command) IS NULL
         OR Trim(r_control_rec.username) IS NULL
         OR Trim(r_control_rec.control) IS NULL 
        )  
    THEN 
        Raise e_Invalid_Zema_ctl_rec;
    END IF;
   
 
    PROCESS_LOG_REC.MESSAGE      := 'Get the Zema Profile Record profile='||pzemaprofile_id;
    
    DMR.PROCESS_LOG_PKG.WRITE_LOG(
            P_PROCESS_NAME => PROCESS_LOG_REC.PROCESS_NAME,
            P_STAGE=> PROCESS_LOG_REC.STAGE,
            P_STATUS=>PROCESS_LOG_REC.STATUS,
            P_MESSAGE=> PROCESS_LOG_REC.MESSAGE);
    
 
   PROCESS_LOG_REC.STAGE        := 'fetch_Profile_detail';  
    -- Fetch Profile Detail record 
    v_rtn     := fetch_Profile_detail (
                          pzemaprofile_id => pzemaprofile_id
                        , rec_Detail => r_rec
                        , o_rtn_msg  => v_rtn_msg 
                        );
    
    -- if any error raise application Error                     
     IF v_rtn <> c_success THEN
      
      RAISE e_appl_error;
 
     END IF;
     
    -- Set Start date and end date of the Profile rec as PEffective_DATE
    r_rec.Start_date := TO_DATE(to_char(vd_CobDate, 'MM-DD-YYYY'), 'MM-DD-YYYY');
    r_rec.End_date := TO_DATE(to_char(vd_cobDate, 'MM-DD-YYYY'), 'MM-DD-YYYY');
    
   CASE
    WHEN  (UPPER(r_rec.style) = 'CSV' AND Upper(r_rec.run_type) IN ( 'D', 'I'))  THEN
  
      PROCESS_LOG_REC.MESSAGE      := 'Prepare Zema URL For CSV for cobdate='||to_char(r_rec.Start_date, 'MM-DD-YYYY');
      v_rtn_code := NULL;
      v_rtn_msg := NULL;
             
      DMR.PROCESS_LOG_PKG.WRITE_LOG(
                    P_PROCESS_NAME => PROCESS_LOG_REC.PROCESS_NAME,
                    P_STAGE=> PROCESS_LOG_REC.STAGE,
                    P_STATUS=>PROCESS_LOG_REC.STATUS,
                    P_MESSAGE=> PROCESS_LOG_REC.MESSAGE
                    );

      PROCESS_LOG_REC.STAGE        := 'Prepare_ZEMA_URL';                              
      Prepare_ZEMA_URL(  pzema_ctl_rec     => r_control_rec    
                       , pZema_profile_Rec => r_rec
                       , p_url_string      => v_url_string
                       , pout_rtn_code => v_rtn_code
                       , pout_rtn_msg =>  v_rtn_msg
                       );
                       
   WHEN  (UPPER(r_rec.style) <> 'CSV') THEN
        Raise   e_Invalid_FileFormat_error;
   ELSE 
        Raise   e_Invalid_FileFormat_error;       
   END CASE;
             
       
       --check success/failure
    IF v_rtn_code <> c_success THEN 
      RAISE e_appl_error;
    END IF;
        
        -- Set the URL returned from Prepare_ZEMA_URL
        --r_rec.last_run_url := v_url_string;
     ----dbms_output.PUT_LINE(v_url_string);
    
    -- Get the Zema Data via HTTP Request and Response
    b_try_http_request := TRUE;
    v_http_attempts := 0; 

      PROCESS_LOG_REC.MESSAGE      := 'Extract Zema Data ';
      v_rtn_code := NULL;
      v_rtn_msg := NULL;
             
      DMR.PROCESS_LOG_PKG.WRITE_LOG(
                    P_PROCESS_NAME => PROCESS_LOG_REC.PROCESS_NAME,
                    P_STAGE=> PROCESS_LOG_REC.STAGE,
                    P_STATUS=>PROCESS_LOG_REC.STATUS,
                    P_MESSAGE=> PROCESS_LOG_REC.MESSAGE
                    );
                         
    WHILE (b_try_http_request AND v_http_attempts <= 5 ) 
    LOOP
        
        v_http_attempts := v_http_attempts  + 1 ;
        
             
        PROCESS_LOG_REC.MESSAGE  := 'Extract Zema Data attempt='||v_http_attempts;
        v_rtn_code := NULL;
        v_rtn_msg := NULL;

       PROCESS_LOG_REC.STAGE        := 'Xtract_ZEMA_Data'; 
             
        Xtract_ZEMA_Data(
         rec_detail => r_rec
         , pout_rtn_code => v_rtn_code
         , pout_rtn_msg =>  v_rtn_msg
        );
        
     
         --check success/failure
         IF v_rtn_code <> c_success THEN 
         
            -- IF HTTP Request failed Error 
            -- May be bad connection try few more times to be sure 
              IF INSTR( v_rtn_msg, 'ORA-29273') > 0 THEN 
                 
                 -- Sleep for 60 Seconds to be sure the HTTP buffer is cleared
                
                DBMS_LOCK.SLEEP(60);
                
                -- Go Back to Loop.. Must continue to try 5 times at MAX 
              ELSE   
                    b_try_http_request := FALSE;
                    RAISE e_appl_error;
              END IF;
         ELSE
              --In case of success set the loop to be terminated
                b_try_http_request := FALSE;
         END IF;
        
     END LOOP;
  
     IF ( v_rtn_code <> c_success 
          AND INSTR( v_rtn_msg, 'ORA-29273') > 0 
          AND v_http_attempts > 5 ) THEN
         
         v_rtn_code := c_Failure;
         v_rtn_msg  := 'Connection to Zema Service Failed More than 5 times';
         RAISE e_appl_error;
     END IF;
 
  
 --Parse the Zema Data 
-- ----dbms_output.PUT_LINE('Before Parse zema parse');
 
 
 
    PROCESS_LOG_REC.MESSAGE      := 'Get Column Mapping for zema Profile_id= '||r_rec.Profile_id;
  
    v_rtn_code := NULL;
    v_rtn_msg := NULL;
             
    DMR.PROCESS_LOG_PKG.WRITE_LOG(
                    P_PROCESS_NAME => PROCESS_LOG_REC.PROCESS_NAME,
                    P_STAGE=> PROCESS_LOG_REC.STAGE,
                    P_STATUS=>PROCESS_LOG_REC.STATUS,
                    P_MESSAGE=> PROCESS_LOG_REC.MESSAGE
                    );

    IF xtracted_data IS NULL THEN 
      --- Initialize the collection 
      INIT_collection;
    END IF;

   PROCESS_LOG_REC.STAGE        := 'Get_Profile_mapping'; 

 
      v_rtn_code := NULL;
      v_rtn_msg := NULL;
             
      DMR.PROCESS_LOG_PKG.WRITE_LOG(
                    P_PROCESS_NAME => PROCESS_LOG_REC.PROCESS_NAME,
                    P_STAGE=> PROCESS_LOG_REC.STAGE,
                    P_STATUS=>PROCESS_LOG_REC.STATUS,
                    P_MESSAGE=> PROCESS_LOG_REC.MESSAGE
                    );
                        
      Get_Profile_mapping (
      pzemaprofile_id => r_rec.profile_id
      , pcobdate      => vd_cobdate         --COBDATE 
      ,pcollection => xtracted_data
      , pout_rtn_code  => v_rtn_code
      , pout_rtn_msg  => v_rtn_msg
      );
  
      
       --check success/failure
       IF v_rtn_code <> c_success THEN    
          RAISE e_appl_error;
       Else
         ----dbms_output.PUT_LINE(' sucess Get_Profile_mapping');  
         null; 
       END IF;
 
      PROCESS_LOG_REC.MESSAGE      := 'Parse Column Headers for Zema Profile_id= '||r_rec.Profile_id;
      v_rtn_code := NULL;
      v_rtn_msg := NULL;
  
        BEGIN
            SELECT data_clob
            INTO v_clob
            FROM riskdb.qp_zema_profile_detail
            WHERE  PROFILE_ID = pzemaprofile_id;
        EXCEPTION    
        WHEN NO_DATA_FOUND THEN 
           v_rtn_msg := 'No Zema Data ia available for this Provider and COB_DATE ='
                        ||TO_CHAR(vd_cobDate, 'MM/DD/YYYY') ;
           Raise e_CLOB_Value_Null;   
        END;
         
        IF  v_clob IS NULL OR length(v_clob) = 0 THEN 
         v_rtn_msg := 'No Zema Data ia available for this Provider and COB_DATE ='
                        ||TO_CHAR(vd_cobDate, 'MM/DD/YYYY') ;
          Raise e_CLOB_Value_Null;
        END IF;

       PROCESS_LOG_REC.STAGE        := 'Parse_Header_and_detl_data'; 
 
      v_rtn_code := NULL;
      v_rtn_msg := NULL;
             
      DMR.PROCESS_LOG_PKG.WRITE_LOG(
                    P_PROCESS_NAME => PROCESS_LOG_REC.PROCESS_NAME,
                    P_STAGE=> PROCESS_LOG_REC.STAGE,
                    P_STATUS=>PROCESS_LOG_REC.STATUS,
                    P_MESSAGE=> PROCESS_LOG_REC.MESSAGE
                    );
                      
       Parse_Header_and_det_data (
              pclob             => v_clob 
            , PEffective_DATE   => vd_cobDate 
            , pcollection       => xtracted_data
            , pout_rtn_code     => v_rtn_code 
            , pout_rtn_msg      => v_rtn_msg  
        );
      
       ----dbms_output.PUT_LINE(' After column Header parse'); 
    
         --check success/failure
       IF v_rtn_code <> c_success THEN    
          RAISE e_appl_error;
       END IF;
     
                     
     ----dbms_output.PUT_LINE(' Prepare data set to Load ');

     PROCESS_LOG_REC.STAGE        := 'Prepare_Dataset_to_Load'; 
     
     v_rtn_code := NULL;
     v_rtn_msg := NULL;
    PROCESS_LOG_REC.MESSAGE := 'Prepare Collection Data ready to Load';      
          DMR.PROCESS_LOG_PKG.WRITE_LOG(
                        P_PROCESS_NAME => PROCESS_LOG_REC.PROCESS_NAME,
                        P_STAGE=> PROCESS_LOG_REC.STAGE,
                        P_STATUS=>PROCESS_LOG_REC.STATUS,
                        P_MESSAGE=> PROCESS_LOG_REC.MESSAGE
                        );


      Prepare_Dataset_to_Load (
        pzemaprof_id => r_rec.profile_id
      , PEffective_DATE   => vd_cobDate
      , pcollection       => xtracted_data
      , pqvarstage        => v_qvarstage_collection
      , pvaluelist        => v_valuelist
      , pout_rtn_code     => v_rtn_code 
      , pout_rtn_msg      => v_rtn_msg  
      );
      
       --check success/failure
       IF v_rtn_code <> c_success THEN    
          RAISE e_appl_error;
       END IF;
       

      v_rtn_code := NULL;
      v_rtn_msg := NULL;   

      v_eff_date           := TO_CHAR(vd_cobDate, 'MM/DD/YYYY');
      PROCESS_LOG_REC.MESSAGE      := 'Delete Records for Effective_date= '||v_eff_date;
         
    --  ----dbms_output.PUT_LINE(' delete Records ');

         PROCESS_LOG_REC.STAGE        := 'delete_records'; 

                 
           DMR.PROCESS_LOG_PKG.WRITE_LOG(
                        P_PROCESS_NAME => PROCESS_LOG_REC.PROCESS_NAME,
                        P_STAGE=> PROCESS_LOG_REC.STAGE,
                        P_STATUS=>PROCESS_LOG_REC.STATUS,
                        P_MESSAGE=> PROCESS_LOG_REC.MESSAGE
                        );

            Select 
            EXT_PROFILE_ID
            INTO 
            v_ext_profileid 
            FROM RISKDB.QP_EXT_PROFILES
            where
            SOURCE_SYSTEM = 'QVAR'
            and ACTIVE_FLAG = 'Y'
            and trunc(vd_cobDate) BETWEEN trunc(EFFECTIVE_START_DATE) and 
                                         NVL(trunc(EFFECTIVE_END_DATE), trunc(vd_cobDate))
            AND ZEMA_PROFILE_ID = pzemaprofile_id;


    --- Dynamic SQL operation  to Delete if any for current Effective date
    v_delete_sql_stmt := 'DELETE RISKDB.QP_EXT_PROVIDER_QUOTES WHERE '||
                         'EXT_PROFILE_ID ='||v_ext_profileid||
                         ' AND '||
                         ' EXT_EFF_DATE = '||
                         'TO_DATE('||v_singlequote||v_eff_date||v_singlequote ||v_seperator||v_dateFormat||') ';
                        

      Delete_records (
      pDelete_statement => v_delete_sql_stmt
      , pout_rtn_code     => v_rtn_code 
      , pout_rtn_msg      => v_rtn_msg  
      );
      
       --check success/failure
       IF v_rtn_code <> c_success THEN    
          RAISE e_appl_error;
       END IF;



    PROCESS_LOG_REC.MESSAGE      := 'Load Target table';
     
--  ----dbms_output.PUT_LINE(' delete Records ');

     PROCESS_LOG_REC.STAGE        := 'LOAD_TARGET'; 

             
     DMR.PROCESS_LOG_PKG.WRITE_LOG(
                    P_PROCESS_NAME => PROCESS_LOG_REC.PROCESS_NAME,
                    P_STAGE=> PROCESS_LOG_REC.STAGE,
                    P_STATUS=>PROCESS_LOG_REC.STATUS,
                    P_MESSAGE=> PROCESS_LOG_REC.MESSAGE
                    );
                                                
     Load_target (
      pcollection       => v_qvarstage_collection
     , pout_rtn_code     => v_rtn_code 
     , pout_rtn_msg      => v_rtn_msg  
      );

    COMMIT;

     ----dbms_output.PUT_LINE('After  Load_target ');  
           --check success/failure
--       IF v_rtn_code <> c_success THEN    
--          RAISE e_appl_error;
--       END IF;
        
        
        PROCESS_LOG_REC.STATUS       := PROCESS_LOG_PKG.C_STATUS_COMPLETED;
        PROCESS_LOG_REC.MESSAGE      := 'Zema Extract Completed';
       
         DMR.PROCESS_LOG_PKG.WRITE_LOG(
                                P_PROCESS_NAME => PROCESS_LOG_REC.PROCESS_NAME,
                                P_STAGE=> PROCESS_LOG_REC.STAGE,
                                P_STATUS=>PROCESS_LOG_REC.STATUS,
                                P_MESSAGE=> PROCESS_LOG_REC.MESSAGE
                                );
  

EXCEPTION
 WHEN e_Invalid_ProfileId_error THEN
       v_rtn_msg := 'Zema Profile ID is Cannot Be NULL. Please Provide Valid Profile ID';
       DMR.PROCESS_LOG_PKG.WRITE_LOG(
                        P_PROCESS_NAME => PROCESS_LOG_REC.PROCESS_NAME,
                        P_STAGE=> PROCESS_LOG_REC.STAGE,
                        P_STATUS=>PROCESS_LOG_PKG.C_STATUS_ERRORS,
                        P_MESSAGE=> v_rtn_msg);
 WHEN e_Missing_EffectiveDate_error THEN
       v_rtn_msg := 'Effective Date is required and is NULL. Please Provide Valid Effective Date';
       DMR.PROCESS_LOG_PKG.WRITE_LOG(
                        P_PROCESS_NAME => PROCESS_LOG_REC.PROCESS_NAME,
                        P_STAGE=> PROCESS_LOG_REC.STAGE,
                        P_STATUS=>PROCESS_LOG_PKG.C_STATUS_ERRORS,
                        P_MESSAGE=> v_rtn_msg);
                                               
WHEN e_Invalid_Zema_ctl_rec THEN 

      v_rtn_msg := 'Invalid OR NULL Zema Control record, please verify';
      
      DMR.PROCESS_LOG_PKG.WRITE_LOG(
                P_PROCESS_NAME => PROCESS_LOG_REC.PROCESS_NAME,
                P_STAGE=> PROCESS_LOG_REC.STAGE,
                P_STATUS=>PROCESS_LOG_PKG.C_STATUS_ERRORS,
                P_MESSAGE=> v_rtn_msg);
                       
--       raise_application_error( -20000 , pout_rtn_msg);

WHEN e_Invalid_Zema_profile_rec THEN 

      v_rtn_msg := 'Invalid OR NULL Zema Profile Information, please verify';
      
      DMR.PROCESS_LOG_PKG.WRITE_LOG(
        P_PROCESS_NAME => PROCESS_LOG_REC.PROCESS_NAME,
        P_STAGE=> PROCESS_LOG_REC.STAGE,
        P_STATUS=>PROCESS_LOG_PKG.C_STATUS_ERRORS,
        P_MESSAGE=> v_rtn_msg);
        
--          raise_application_error( -20002 , v_rtn_msg);
WHEN e_Invalid_FileFormat_error THEN 
    
    v_rtn_msg := 'Data For this Profile is Expected to be in CSV Format. Please Verify Profile Setup';
     
    DMR.PROCESS_LOG_PKG.WRITE_LOG(
                        P_PROCESS_NAME => PROCESS_LOG_REC.PROCESS_NAME,
                        P_STAGE=> PROCESS_LOG_REC.STAGE,
                        P_STATUS=>PROCESS_LOG_PKG.C_STATUS_ERRORS,
                        P_MESSAGE=>NVL( v_rtn_msg, SQLERRM) 
                        );
WHEN e_Zema_Prof_dtl_upd_failed THEN 

 DMR.PROCESS_LOG_PKG.WRITE_LOG(
                        P_PROCESS_NAME => PROCESS_LOG_REC.PROCESS_NAME,
                        P_STAGE=> PROCESS_LOG_REC.STAGE,
                        P_STATUS=>PROCESS_LOG_PKG.C_STATUS_ERRORS,
                        P_MESSAGE=>NVL( v_rtn_msg, SQLERRM) 
                        );    
WHEN e_CLOB_Value_Null THEN 
       DMR.PROCESS_LOG_PKG.WRITE_LOG(
                        P_PROCESS_NAME => PROCESS_LOG_REC.PROCESS_NAME,
                        P_STAGE=> PROCESS_LOG_REC.STAGE,
                        P_STATUS=>PROCESS_LOG_PKG.C_STATUS_INFO,
                        P_MESSAGE=>NVL( v_rtn_msg, SQLERRM) 
                        );                                                                 
WHEN e_appl_error THEN 
    
    DMR.PROCESS_LOG_PKG.WRITE_LOG(
                        P_PROCESS_NAME => PROCESS_LOG_REC.PROCESS_NAME,
                        P_STAGE=> PROCESS_LOG_REC.STAGE,
                        P_STATUS=>PROCESS_LOG_PKG.C_STATUS_ERRORS,
                        P_MESSAGE=>NVL( v_rtn_msg, SQLERRM) 
                        );
  
 WHEN OTHERS THEN
       DMR.PROCESS_LOG_PKG.WRITE_LOG(
                        P_PROCESS_NAME => PROCESS_LOG_REC.PROCESS_NAME,
                        P_STAGE=> PROCESS_LOG_REC.STAGE,
                        P_STATUS=>PROCESS_LOG_PKG.C_STATUS_ERRORS,
                        P_MESSAGE=>NVL( v_rtn_msg, SQLERRM)
                        );
  --    RAISE;

END EXTERNAL_LOAD;
/******************************************************************************/

   -- Refresh External Brokerdata/Zema Data  for  A Given Profile AND 
   -- Current Effective Date 
PROCEDURE EXTERNAL_LOAD             ( pzemaprofile_id     IN  riskdb.qp_zema_profile_detail.profile_id%TYPE 
                                    )
                                  
IS
   vd_cobdate    DATE; 
   vc_cobdate    VARCHAR2(20);

v_rtn_code                   NUMBER;
v_rtn_msg                    VARCHAR2(500);
   
BEGIN                                     
  


     Get_current_COBDATE (
      pcobdate =>   vd_cobdate  
     ,pout_rtn_code     => v_rtn_code
     ,pout_rtn_msg      => v_rtn_msg 
     );
     
     vc_cobdate       := to_CHAR( vd_cobdate, 'DD-MON-YYYY');
     vd_cobdate       := to_date( vc_cobdate, 'DD-MON-YYYY');
     

  
  EXTERNAL_LOAD(
   pzemaprofile_id  =>   pzemaprofile_id 
   , PEffective_DATE  =>  vd_cobdate
  );
  
                                      
                                        
END   EXTERNAL_LOAD;
                                      

-- Refresh External Brokerdata/Zema Data  for  A Given Effective Date
-- And ALL QVAR Profiles                                     
PROCEDURE EXTERNAL_LOAD             (
                                     PCOBDATE IN  DATE
                                    , PSTATUSCODE OUT VARCHAR2
                                    , PSTATUSMSG OUT VARCHAR2  
                                    )
IS

   vd_cobdate    DATE; 
   vc_cobdate    VARCHAR2(20);

vn_starttimeStamp             DATE := SYSDATE ;

v_rtn_code                   NUMBER;
v_rtn_msg                    VARCHAR2(500);
   
CURSOR Zema_Profiles_cur IS
Select ZEMA_PROFILE_ID 
from QP_EXT_PROFILES prof
WHERE
SOURCE_SYSTEM = 'QVAR'
AND ACTIVE_FLAG = 'Y'
AND TRUNC(vd_cobdate) BETWEEN EFFECTIVE_START_DATE AND NVL(EFFECTIVE_END_DATE, trunc(vd_cobdate))
order by ZEMA_PROFILE_ID
;

li_minlogid    NUMBER;
li_ERRORS      NUMBER;
BEGIN

-- Before launching get Minimum Log id logged
 
SELECT MIN(log_id)
INTO li_minlogid
FROM 
DMR.PROCESS_LOG l
WHERE
Application_name = 'QVAR_APP'
and process_Name = 'QP_ZEMA_EXTRACT_PKG' 
and Parent_Name = 'EXTERNAL_LOAD'
--and STATUS = 'ERRORS'
;


    IF pcobdate is NULL THEN 

     Get_current_COBDATE (
      pcobdate =>   vd_cobdate  
     ,pout_rtn_code     => v_rtn_code
     ,pout_rtn_msg      => v_rtn_msg 
     );
     
     vc_cobdate       := to_CHAR( vd_cobdate, 'DD-MON-YYYY');
     vd_cobdate       := to_date( vc_cobdate, 'DD-MON-YYYY');
     
    ELSE


    vc_cobdate       := to_CHAR(pcobdate, 'DD-MON-YYYY');
    vd_cobdate       := to_date(vc_cobdate, 'DD-MON-YYYY');

    --vd_default_effective_Date := vd_cobdate;

    END IF;
    
For Profiles In Zema_Profiles_cur LOOP

  EXTERNAL_LOAD(
   pzemaprofile_id  =>   Profiles.ZEMA_PROFILE_ID 
   , PEffective_DATE  => vd_cobdate
  );
END LOOP;

li_ERRORS := 0;

SELECT count(*) 
INTO li_ERRORS
FROM 
DMR.PROCESS_LOG l
WHERE
Application_name = 'QVAR_APP'
and process_Name = 'QP_ZEMA_EXTRACT_PKG' 
and Parent_Name = 'EXTERNAL_LOAD'
and STATUS = 'ERRORS'
and l.log_id > li_minlogid
and TIMESTAMP > vn_starttimeStamp ;


IF li_ERRORS > 0 THEN 
   PSTATUSCODE := 'SUCCESS';
   PSTATUSMSG := 'Process Completed with Errors, Please check Error Log DMR.PROCESS_LOG   for details';
ELSE
    PSTATUSCODE := 'SUCCESS';
    PSTATUSMSG := 'Process Completed Successfully';      
END IF;

EXCEPTION
WHEN OTHERS THEN 
 PSTATUSCODE := 'ERRORS';
 PSTATUSMSG := SUBSTR(SQLERRM||' - '||'Process Completed with Errors, Please check DMR.PROCESS_LOG for details',1,300);
  
END EXTERNAL_LOAD;


-- Refresh External Brokerdata/Zema Data  for Every QVAR Profile and 
-- for  A Current Effective Date
                                    
PROCEDURE EXTERNAL_LOAD   
IS

   vd_cobdate    DATE; 
   vc_cobdate    VARCHAR2(20);

v_rtn_code                   NUMBER;
v_rtn_msg                    VARCHAR2(500);
   
CURSOR Zema_Profiles_cur IS
Select ZEMA_PROFILE_ID 
from QP_EXT_PROFILES prof
WHERE
SOURCE_SYSTEM = 'QVAR'
AND ACTIVE_FLAG = 'Y'
AND TRUNC(vd_cobdate) BETWEEN EFFECTIVE_START_DATE AND NVL(EFFECTIVE_END_DATE, trunc(vd_cobdate));

BEGIN

  --Get Default Effective DAte 
 

     Get_current_COBDATE (
      pcobdate =>   vd_cobdate  
     ,pout_rtn_code     => v_rtn_code
     ,pout_rtn_msg      => v_rtn_msg 
     );
     
     vc_cobdate       := to_CHAR( vd_cobdate, 'DD-MON-YYYY');
     vd_cobdate       := to_date( vc_cobdate, 'DD-MON-YYYY');
     


For Profiles In Zema_Profiles_cur LOOP
  
  EXTERNAL_LOAD(
   pzemaprofile_id  =>   Profiles.ZEMA_PROFILE_ID
   , PEffective_DATE  =>  vd_cobdate
  );

END LOOP;


END EXTERNAL_LOAD;

PROCEDURE QP_DELETE_HISTORY IS
    BEGIN

      dmr.process_log_pkg.constructor(p_application_name=>'RISKDB.QP_DELETE_HISTORY');
      dmr.process_log_pkg.set_date(sysdate,'RUN DATE');

      dmr.process_log_pkg.write_log( p_stage=>'DELETION_STARTED',
                                     p_status=>process_log_pkg.c_status_started,
                                     p_message=> 'DELETING PROCESS STARTED'
                                   );


    DELETE FROM riskdb.qp_ext_provider_quotes qepq
    WHERE not exists ( SELECT  m2m_cob_date
                       FROM   rd.r_ctrl_pe_liqc_cob_v d
                       WHERE  D.pe_flag=1
                       AND   d.m2m_cob_date = qepq.ext_eff_date
                     );


    IF SQL%ROWCOUNT = 0 THEN

      dmr.process_log_pkg.write_log( p_stage=>'NO HISTORY DATA FOUND',
                                     p_status=>process_log_pkg.c_status_info ,
                                     p_message=> 'NO QVAR HISTORY DATA FOUND'
                                   );
                                   
    ELSE
      dmr.process_log_pkg.write_log( p_stage=>'DELETED',
                                     p_status=>process_log_pkg.c_status_deleted ,
                                     p_message=> 'DELETED HISTORY AND '||SQL%ROWCOUNT||' '||'rows'||' DELETED'
                                   );
    END IF;

    IF SQL%ROWCOUNT = 0  THEN
      ----dbms_output.put_line('No QVAR history data found');
      null;
    END IF;
     COMMIT;
     dmr.process_log_pkg.write_log( p_stage=>'DELETION_ENDED',
                                     p_status=>process_log_pkg.c_status_completed ,
                                     p_message=> 'DELETING PROCESS COMPLETED'
                                   );
    

      EXCEPTION

       WHEN OTHERS THEN
          ROLLBACK;
          dmr.process_log_pkg.write_log( p_stage=>'DELETION_ERRORS',
                                     p_status=>process_log_pkg.c_status_errors  ,
                                     p_message=> 'ERROR WHILE DELETING: '||(SUBSTR(sqlerrm, 1, 255))
                                   );
       END QP_DELETE_HISTORY;

 
PROCEDURE CHeck_Qvar_status_for_COBDATE ( 
                                        PCOBDATE        IN DATE 
                                        , pQVARStaus    OUT VARCHAR2
                                        , pQVARrunID    OUT NUMBER
                                        , ptimestamp    OUT DATE
                                        ,pout_rtn_code  OUT  NOCOPY NUMBER  
                                        ,pout_rtn_msg   OUT  NOCOPY VARCHAR2 
                                        )
Is

vn_runid    RISKDB.QP_QVAR_RUN.QVAR_RUN_ID%TYPE; 
vc_status   RISKDB.QP_QVAR_RUN.RUN_STATUS%TYPE;
vd_timestamp    RISKDB.QP_QVAR_RUN.MODIFY_DATE%TYPE;

v_section  Varchar2(100) := 'CHeck_Qvar_status_for_COBDATE';
  
BEGIN

    PROCESS_LOG_REC.STAGE        := 'CHeck_Qvar_status_for_COBDATE';
    PROCESS_LOG_REC.Message      := 'Get Qvar status ';
    
    write_log;
    
    BEGIN
        Select 
        QVAR_RUN_ID
        , RUN_STATUS
        ,  MODIFY_DATE
        into vn_runid,vc_status , vd_timestamp
        from RISKDB.QP_QVAR_RUN
        where
        COB_DATE = PCOBDATE;
        
        pQVARrunID := vn_runid;
        pQVARStaus := vc_status;
        ptimestamp := vd_timestamp;
        pout_rtn_code := c_success;
        pout_rtn_msg := ''; --Null means Success 
        
    EXCEPTION
      WHEN NO_DATA_FOUND THEN 
        pQVARrunID := -1; -- means no data present yet
        pQVARStaus := 'N';
        pout_rtn_code := c_success;
        pout_rtn_msg := ''; --Null means Success
    END;


EXCEPTION
WHEN OTHERS THEN 
    pout_rtn_code := c_failure;
    pout_rtn_msg := v_section||'-'||SUBSTR(SQLERRM,1,200);
    
END CHeck_Qvar_status_for_COBDATE; 

PROCEDURE  Insert_COBDATE(
         pCOBDATE          IN DATE 
        ,pout_rtn_code     OUT  NOCOPY NUMBER  
        ,pout_rtn_msg      OUT  NOCOPY VARCHAR2 
       )
IS
PRAGMA AUTONOMOUS_TRANSACTION;
v_section  Varchar2(100) := 'Inser_COB_DATE';
vd_cobdate DATE := TO_DATE(TO_CHAR(pcobdate,'DD-MON-YYYY'),'DD-MON-YYYY');
BEGIN

    PROCESS_LOG_REC.STAGE        := 'Insert_COBDATE';

    INSERT INTO RISKDB.QP_QVAR_RUN (
      QVAR_RUN_ID  
      , COB_DATE 
      , RUN_STATUS   
      , ACTIVE_FLAG  
      , COMMENTS     
      ) VALUES (
      RISKDB.QP_ALL_LOGS_SEQ.NEXTVAL
      , vd_cobdate
      , 'I'
      , 'Y'
      ,'Qvar Internal Load '
      );
    
      COMMIT;
    
      
    pout_rtn_code := c_success;
    pout_rtn_msg := ''; --Null means Success 
    
EXCEPTION
WHEN OTHERS THEN 
    pout_rtn_code := c_failure;
    pout_rtn_msg := v_section||'-'||SUBSTR(SQLERRM,1,200);
    ----dbms_output.put_line(pout_rtn_msg);
END Insert_COBDATE;


PROCEDURE  Update_QVAR_Status(
           prunid           IN Number
         , pstatus         IN VARCHAR2 
         ,pout_rtn_code     OUT  NOCOPY NUMBER  
         ,pout_rtn_msg      OUT  NOCOPY VARCHAR2 
       )
IS
PRAGMA AUTONOMOUS_TRANSACTION;
v_section  Varchar2(100) := 'Update_QVAR_Status';
--vd_cobdate DATE := TO_DATE(TO_CHAR(pcobdate,'DD-MON-YYYY'),'DD-MON-YYYY');
BEGIN

    PROCESS_LOG_REC.STAGE        := 'Update QP_QVAR_RUN';

 --   ----dbms_output.Put_line( 'id='||prunid||';status='||pstatus);
    
    UPDATE RISKDB.QP_QVAR_RUN r
    SET RUN_STATUS = pstatus
    WHERE
    QVAR_RUN_ID = prunid ;

    COMMIT;
--    ----dbms_output.Put_line( 'after update id='||prunid||';status='||pstatus);
      
    pout_rtn_code := c_success;
    pout_rtn_msg := ''; --Null means Success 
    
EXCEPTION
WHEN OTHERS THEN 
    pout_rtn_code := c_failure;
    pout_rtn_msg := v_section||'-'||SUBSTR(SQLERRM,1,200);
    ----dbms_output.put_line(pout_rtn_msg);
END Update_QVAR_Status;

PROCEDURE LOAD_GTT_Session_Values(  
            pcobdate           IN VARCHAR2
            ,pout_rtn_code      OUT  NOCOPY NUMBER  
            ,pout_rtn_msg       OUT  NOCOPY VARCHAR2  
           )
IS
vc_sql_stmt                    VARCHAR2(10000);
vl_rtn_code                    NUMBER;
vc_rtn_msg                     VARCHAR2(500);
Vc_section                     VARCHAR2(1000);
vc_singleQuote                 VARCHAR2(1) := CHR(39);
vc_current_cobdate             VARCHAR2(30);
vc_previous_cobdate            VARCHAR2(30);
BEGIN

  PROCESS_LOG_REC.STAGE        := 'LOAD_GTT_Session_Values';
  vc_section := 'Load GTT Session level SYS context VariablesValues ';
  PROCESS_LOG_REC.MESSAGE      := vc_section;
  write_log;
 
 riskdb.Context_pkg.set_Parameter(p_Name=>'CurrentCobDate', p_Value=>pcobdate );
-- RISKDB.m2m_ctrlm_util.set_context ( 'CobDate', pcobdate ) ;
  
  
Exception
WHEN OTHERS THEN 
   pout_rtn_code := c_failure;
   pout_rtn_msg := vc_section||'-'||SUBSTR(SQLERRM,1,200);

  ----dbms_output.PUT_LINE (pout_rtn_msg);
END LOAD_GTT_Session_Values;
       
                  
PROCEDURE LOAD_GTT_M2MDMR(  
            pcobdate           IN VARCHAR2
            ,pout_rtn_code      OUT  NOCOPY NUMBER  
            ,pout_rtn_msg       OUT  NOCOPY VARCHAR2  
           )
IS
vc_sql_stmt                    VARCHAR2(10000);
Vc_section                     VARCHAR2(100);
vc_singleQuote                 VARCHAR2(1) := CHR(39);
li_count                       NUMBER;

BEGIN

  PROCESS_LOG_REC.STAGE        := 'INTERNAL_LOAD';
  vc_section := 'Creating and Loading GTT m2m Granular data table ';
 
-- join m2m dmr with QVAR tables to get a granular monthly data
  vc_sql_stmt := 'INSERT INTO RISKDB.GTT_M2MGRANULAR_DATA '||
                ' select * from riskdb.qp_m2mGranular_data_v ';


  EXECUTE IMMEDIATE vc_sql_stmt;
  
  -- aggregate QVAR data to Location and contract Month Level 
  vc_section := 'Creating and Loading GTT m2m dmr table ';
    
  vc_sql_stmt := 'INSERT INTO RISKDB.GTT_M2MDMR '||
                ' select * from RISKDB.QP_Internal_curves_Raw_Data_V ';


  EXECUTE IMMEDIATE vc_sql_stmt;

Select count(*) into li_count 
from RISKDB.GTT_M2MDMR
;

dbms_output.Put_line ('total_recs = '||li_count);

--riskdb.M2M_CTRLM_UTIL.reset_context;
  
Exception
WHEN OTHERS THEN 
   pout_rtn_code := c_failure;
   pout_rtn_msg := vc_section||'-'||SUBSTR(SQLERRM,1,200);

  ----dbms_output.PUT_LINE (pout_rtn_msg);
END LOAD_GTT_M2MDMR;



PROCEDURE LOAD_GTT_Basis_Curves(  
            pcobdate           IN VARCHAR2
            ,pout_rtn_code      OUT  NOCOPY NUMBER  
            ,pout_rtn_msg       OUT  NOCOPY VARCHAR2  
           )
IS
vc_sql_stmt                    VARCHAR2(10000);
Vc_section                     VARCHAR2(100);
vc_singleQuote                 VARCHAR2(1) := CHR(39);

BEGIN

  PROCESS_LOG_REC.STAGE        := 'GTT Basis Curves Load';
  vc_section := 'Loading GTT Basis table ';
 
 -- riskdb.Context_pkg.set_Parameter(p_Name=>'CurrentCobDate', p_Value=>pcobdate ); 
 -- riskdb.M2M_CTRLM_UTIL.set_context(pName=>'cobDate', pValue=>pcobdate ); 
   
  vc_sql_stmt := 'INSERT INTO RISKDB.GTT_Basis_Curves '||
                 ' SELECT  '||
                 '   backbone_name  '||
                 '    ,basis_name   '||
                 '    , fm_commodity  '||
                 '    , fm_basis_point  '||
                 '    , Hour_Type  '||
                 '    , Basis_NodeId  '||
                 '    , BEG_EFFECTIVE_DATE  '||
                 '    , END_EFFECTIVE_DATE  '||
                 '    , BACKBONE_MARKET_NODEID  '||
                 '    , basis_market_nodeid  '||
                 '    , curve_id  '||
                 '    , QUOTE_DATE  '||
                 '    , term_start  '||
                 '    , term_end  '||
                 '    , basis_term_end  '||
                 '    , ON_OFF_Indicator  '||
                 '   ,  basis_ask  '||
                 '    , basis_bid  '||
                 '    , basis_quote_mid_price  '||
                 '    , broker        '||            
                 ' FROM     '||
                 '  RISKDB.qp_basis_power_curves_v ';
 
 

------dbms_output.PUT_LINE (vc_sql_stmt);

EXECUTE IMMEDIATE vc_sql_stmt;

--riskdb.M2M_CTRLM_UTIL.reset_context;
  
Exception
WHEN OTHERS THEN 
   pout_rtn_code := c_failure;
   pout_rtn_msg := vc_section||'-'||SUBSTR(SQLERRM,1,200);

  ----dbms_output.PUT_LINE (pout_rtn_msg);
END LOAD_GTT_Basis_Curves;


PROCEDURE  Collect_ALL_Internal_Curves(
     pcobdate           IN VARCHAR2
    ,pInternal_curves   IN OUT NOCOPY Internal_curves_T
    ,pout_rtn_code      OUT  NOCOPY NUMBER  
    ,pout_rtn_msg       OUT  NOCOPY VARCHAR2
     )
IS
vn_reccount                  NUMBER := 0;
v_rtn_code                   NUMBER;
v_rtn_msg                    VARCHAR2(500);
v_section                    Varchar2(1000) := 'Collect_ALL_Internal_Curves';
vd_cobdate                   DATE := TO_DATE(pcobdate , 'DD-MON-YYYY');
v_curvetype_indx             NUMBER; -- 1 for PRICE curve and 2 when VOL curve 
vs_curve_Type                VARCHAR2(10);
vc_overcom                   RISKDB.QP_INT_FWD_CURVES_RAW_DATA.LOCATION%TYPE;
vc_overLoc                   RISKDB.QP_INT_FWD_CURVES_RAW_DATA.LOCATION%TYPE;


CURSOR internalCurves_cur (ccobdate DATE) is 
--- Loading Curves, netting group , Business Unit and FAS levels 
with 
 basis_q as (
    Select 
      BACKBONE_NAME           ,
      BASIS_NAME              ,
      FM_COMMODITY            ,
      FM_BASIS_POINT          ,
      HOUR_TYPE               ,
      BASIS_NODEID            ,
      BEG_EFFECTIVE_DATE      ,
      END_EFFECTIVE_DATE      ,
      BACKBONE_MARKET_NODEID  ,
      BASIS_MARKET_NODEID     ,
      CURVE_ID                ,
      QUOTE_DATE              ,
      TERM_START              ,
      TERM_END                ,
      BASIS_TERM_END          ,
      ON_OFF_INDICATOR        ,
      BASIS_ASK               ,
      BASIS_BID               ,
      BASIS_QUOTE_MID_PRICE   ,
      BROKER                  
    FROM RISKDB.GTT_Basis_Curves
) 
, fas as (
Select /*+ materialize */ distinct
 cob_date
, commodity
, basis_point
,FAS_level_1
, end_cm_1_ch
, fas_level_2
, end_cm_2_ch
, end_cm_2_nch
, FAS_level_3
, end_cm_3_NCH 
from riskdb.FAS157_CAT 
where
cob_Date = ( select MAX(cob_Date) 
             from riskdb.FAS157_CAT 
             where 
             cob_date <= CCOBDATE
             and active = 'Y' 
            )
and active = 'Y'
)
, m2m as 
(
 SELECT * From riskdb.gtt_m2mdmr
-- where
-- Underlying_commodity = 'ELECTRICITY'
-- and Location = 'NEPOOL-.H.INTERNAL_HUB_MLC-5x16'
-- and contractyear_Month = '201601'
)
Select 
  m2m.COB_DATE              
, m2m.NETTING_GROUP Netting_Group_Id      
, m2m.UNDERLYING_COMMODITY  
, m2m.COMMODITY             
, m2m.LOCATION              
, m2m.CONTRACTYear_MONTH
, m2m.Hour_Type
, m2m.Hour_TYPE_ON_OFF    
--, m2m.Volatility_flag   
, m2m.PRICE_CURVE
, m2m.VOL_CURVE
, m2m.PROJ_LOC_AMT 
--, m2m.DEAL_TYPE             
--, m2m.SOURCE                
--, m2m.VOLUME_UOM 
, m2m.m2m_value   
, m2m.m2m_Value_VOL        
, m2m.Legged_m2m_value 
, m2m.legged_m2m_value_VOL                                 
, m2m.Location_DELTA_POSITION 
, m2m.Location_Delta_Position_VOL
, m2m.commodity_Delta_position    
, m2m.Commodity_Delta_Position_VOL 
, m2m.INITIAL_RANK
, m2m.partition_bit
, fas.commodity   fas157_commodity
, fas.Basis_Point fas157_basis_point 
, fas.END_CM_1_CH 
, fas.FAS_LEVEL_1
, fas.END_CM_2_NCH
, fas.FAS_LEVEL_2
, fas.END_CM_3_NCH
, fas.FAS_LEVEL_3
, (
    CASE 
    WHEN TO_NUMBER(m2m.contractYear_month) between 
            TO_NUMBER(TO_CHAR(CCOBDATE, 'YYYYMM')) AND 
            TO_NUMBER(NVL(END_CM_1_CH , -1 ))   THEN FAS_LEVEL_1
    WHEN (
         TO_NUMBER(m2m.contractyear_month) between 
            TO_NUMBER(NVL(END_CM_1_CH, -1)) AND 
            TO_NUMBER(NVL(END_CM_2_NCH , -1 )) 
          ) AND 
          --  level 2 end date needs to be gretaer than level 1 end date
          -- otherwise , if both are null or both are equal or level2 < level 1 end date
          --go to fas levl 3 check
          (
          TO_NUMBER(NVL(END_CM_2_NCH, -1)) >
          TO_NUMBER(NVL(END_CM_1_CH , -1 ))
          )    THEN FAS_LEVEL_2
    WHEN TO_NUMBER(m2m.contractYear_month) between 
            TO_NUMBER(NVL(END_CM_2_NCH , -1 )) AND 
            TO_NUMBER(NVL(END_CM_3_NCH , -1 ))   THEN FAS_LEVEL_3 
    ELSE
      NULL
    END ) original_fas_level
, TO_NUMBER(TO_CHAR(LAST_DAY(to_DATE(m2m.contractYear_MOnth||'01','YYYYMMDD')), 'DD')) Days_in_ContractMOnth
, basis_q.fm_basis_point phoenix_basis_name
, (Case 
   WHEN  basis_q.Basis_market_Nodeid is not null then 'Y'
   Else 'N'
   END
   ) basis_curve_indicator
,  basis_q.Quote_date Basis_Quote_date
,  (CCOBDATE - basis_q.quote_date)  Basis_quote_age
,  NVL((CASE
    WHEN  (TRUNC(CCOBDATE)-(basis_q.quote_date)) > 74
        -- OR btq.quote_date = '31-DEC-9999'
    THEN
       'Y'
    WHEN  (TRUNC(CCOBDATE)-(basis_q.quote_date)) <= 74
    THEN
       'N'
    ELSE NULL
    END
   ), 'N') Using_100prcnt_hist_method
, basis_q.basis_qUOtE_Mid_price Trader_Basis_Quote_Price    
,basis_q.term_start            
,basis_q.term_end              
,basis_q.basis_ask            
,basis_q.Basis_bid           
,basis_q.Broker
,MONTHLY_VOL                  
,PROJ_LOCATION_AMT            
,PROJ_PREMIUM_AMT              
,PROJ_BASIS_AMT               
,MAX_CONTRACTYEAR_MONTH      
,MIN_CONTRACTYEAR_MONTH       
,TOT_CONTRACTYEAR_MONTH      
--changes for absolute m2m values 10/27/2015     
,ABS_M2M_VALUE_AMT  
,ABS_M2M_LEG_VALUE_AMT  
,ABS_M2M_VALUE_AMT_VOL  
,ABS_M2M_LEG_VALUE_AMT_VOL          
FROM 
m2m left outer join fas
ON (
m2m.commodity = fas.commodity
and m2m.Location = fas.Basis_point
) left outer join basis_q 
ON (
Upper(m2m.COMMODITY) = Upper(basis_q.Backbone_Name)
and Upper(m2m.LOcation) = Upper(basis_q.Basis_Name)
and to_number(m2m.Contractyear_Month) = to_number(basis_q.Basis_term_END)
and DECODE(Upper(m2m.hour_Type), 'N/A', 'OFF',Upper(m2m.hour_Type) ) = DECODE(Upper(basis_q.Hour_Type), 'N/A', 'OFF',Upper(basis_q.Hour_Type))
and DECODE(Upper(m2m.Hour_TYPE_ON_OFF), 'N/A', 'OFF',Upper(m2m.Hour_TYPE_ON_OFF)) = DECODE(Upper(Basis_q.ON_OFF_Indicator), 'N/A', 'OFF',Upper(Basis_q.ON_OFF_Indicator))
--and DECODE(Upper(m2m.Hour_TYPE_ON_OFF), 'N/A', 'OFF',Upper(m2m.Hour_TYPE_ON_OFF)) = Upper(Basis_q.ON_OFF_Indicator)
)
Order by 
  m2m.COB_DATE              
, m2m.NETTING_GROUP       
, m2m.UNDERLYING_COMMODITY  
, m2m.COMMODITY             
, m2m.LOCATION              
, m2m.CONTRACTYEAR_MONTH
;


 /************************************************************************** 
  * Procedure to get total number of power peak hours per contract month *
 ***************************************************************************/

PROCEDURE Get_Hours_Info ( ptargetIndex           IN       NUMBER
                            ,pout_rtn_code     OUT  NOCOPY NUMBER  
                            ,pout_rtn_msg      OUT  NOCOPY VARCHAR2 
                           ) 
 IS
 vc_section         Varchar2(1000) := 'Get_Hours_Info';
 v1_hours           PHOENIX.ISO_MONTHLY_PEAK_HOURS_MV.HOURS%TYPE;

                     
 BEGIN     
 
 IF pInternal_curves(ptargetIndex).Hour_type = 'N/A' THEN 
    pInternal_curves(ptargetIndex).Hour_type := '7X24';
 END IF;
    
 
 /*select month,hours, hour_type_name
 from PHOENIX.ISO_MONTHLY_PEAK_HOURS_MV mph,
 Riskdb.m2m_work_area mwa where
 mwa.contract_month = mph.month
 and mwa.peak_type = mph.hour_type_name;*/
            Select mv.hours 
            INTO 
            v1_hours 
            from
            PHOENIX.ISOS iso,
            PHOENIX.ISO_MONTHLY_PEAK_HOURS_MV mv
            where
            ISO.id = MV.ISO_ID
            and MV.MONTH = pInternal_curves(ptargetIndex).contractYear_month
            and iso.name = substr(
                       pInternal_curves(ptargetIndex).Location
                       , 1
                       , instr( pInternal_curves(ptargetIndex).Location
                               ,'-'
                               )-1
                       )
            and (Upper(MV.HOUR_TYPE_NAME)) =( upper( pInternal_curves(ptargetIndex).Hour_type))
            ;



 -- if successfu in getting FAS value then Looad into target table 
 pInternal_curves(ptargetIndex).tot_hours_per_contract_month := v1_hours;
 

 
 --- Return Success info 
    pout_rtn_code := c_success;
    pout_rtn_msg := ''; --Null means Success 
    
                      
 EXCEPTION
 WHEN NO_DATA_FOUND THEN 
   pout_rtn_code := c_failure;
   pout_rtn_msg := 'There are no hours available for '||
   'Commodity ='||pInternal_curves(ptargetIndex).commodity||
   ' Location='||pInternal_curves(ptargetIndex).Location||
   ' HOur Type = '|| Upper(pInternal_curves(ptargetIndex).Hour_type)||
   ' contractMOnth = '||pInternal_curves(ptargetIndex).contractYear_month
   ;
   
               
 WHEN TOO_MANY_ROWS THEN
   pout_rtn_code := c_failure;
   pout_rtn_msg := 'The total number of hours for'||'Peak type='||Upper(pInternal_curves(ptargetIndex).Hour_type)
                   ||' Location='||pInternal_curves(ptargetIndex).Location
                   ||'Contract_month='||pInternal_curves(ptargetIndex).Contract_month
                   ||'found to have more than one record.Please check your data';                 
 WHEN OTHERS THEN 
 
   vc_section :=  'Get_Hours_Info'
                  || 'The total number of hours for'||'Peak type='
                  ||Upper(pInternal_curves(ptargetIndex).Hour_type)
                  ||' Location='||pInternal_curves(ptargetIndex).Location
                  ||'Contract_month='||pInternal_curves(ptargetIndex).Contract_month
                  ;
                   
  
   pout_rtn_code := c_failure;
   pout_rtn_msg :=  vc_section||' - '||SUBSTR(SQLERRM,1,200);
   --dbms_output.put_line(pout_rtn_msg);
 END Get_Hours_Info;
 
 
 
   
 /********************************************** 
  *   Procedure to get the Current FAS level 
 ************************************************/
 
 PROCEDURE Get_FAS_level_percent ( ptargetIndex           IN       NUMBER
                                   ,pout_rtn_code     OUT  NOCOPY NUMBER  
                                   ,pout_rtn_msg      OUT  NOCOPY VARCHAR2 
                                 ) 
 IS
 vc_section         Varchar2(1000) := 'Get_FAS_level_percent';  
 vl_FAS_Value       RISKDB.QP_FAS_TOLERANCE.FAS_PERCENT_VALUE%TYPE;
                     
 BEGIN     
 

    
 SELECT FAS_PERCENT_VALUE INTO vl_FAS_Value
 FROM RISKDB.QP_FAS_TOLERANCE
 where
 FAS_LEVEL = pInternal_curves(ptargetIndex).Original_FAS157_Level
 AND TRUNC(vd_cobdate ) Between EFFECTIVE_START_DATE AND NVl(EFFECTIVE_END_DATE , vd_cobdate ) --ADDED FAS CHECK 12/15 SIRI
 AND ACTIVE_FLAG = 'Y'
 ;
 

 -- if successfu in getting FAS value then Looad into target table 
 pInternal_curves(ptargetIndex).Original_FAS157_percent :=   vl_FAS_Value;
  
 --- Return Success info 
    pout_rtn_code := c_success;
    pout_rtn_msg := ''; --Null means Success 
    
                      
 EXCEPTION
 WHEN NO_DATA_FOUND THEN 
   pout_rtn_code := c_failure;
   pout_rtn_msg := 'The FAS Level'||pInternal_curves(ptargetIndex).Original_FAS157_Level||
                   ' Is Not defined, Please Configre the FAS Value';
               
 WHEN TOO_MANY_ROWS THEN
   pout_rtn_code := c_failure;
   pout_rtn_msg := 'The Current FAS override for FAS Level '||pInternal_curves(ptargetIndex).Original_FAS157_Level||
                   '  Is Found to have more than one record, Please verify FAS Override configuration';                 
 WHEN OTHERS THEN 
   pout_rtn_code := c_failure;
   pout_rtn_msg :=  vc_section||'-'||SUBSTR(SQLERRM,1,200);
 END Get_FAS_level_percent;   
 
 
    
 /********************************************** 
  *   Procedure to get Get_FAS_Override
 ************************************************/
 
 PROCEDURE Get_FAS_Override (     ptargetIndex       IN         NUMBER
                                , pCurveType         IN         VARCHAR2
                                , pvolAmount         IN         NUMBER
                                , pout_rtn_code     OUT  NOCOPY NUMBER  
                                , pout_rtn_msg      OUT  NOCOPY VARCHAR2 
                             ) 
 IS
 
 vc_section                 Varchar2(1000) := 'Get_FAS_Override';  
 vl_FAS_ovrride             RISKDB.QP_FAS_TOLERANCE_OVERRIDE.OVERRIDE_FAS_PERCENT_VALUE%TYPE;
 vc_FAS_Value_Type          VARCHAR2(30);
 vc_FAS_override_indicator  VARCHAR2(1) ;
 vn_priceamount             NUMBER;                    
 BEGIN     
 
      vc_section := vc_section ||pInternal_curves(ptargetIndex).Location;
 
            vl_FAS_ovrride    := NULL;
            vc_FAS_Value_Type := NULL;
        
       IF   pCurveType = 'PRICE' THEN
          vn_priceamount := pInternal_curves(ptargetIndex).Internal_Price;
       ELSE
          vn_priceamount := pvolAmount;  
       END IF;
       
            
       /*******************************************/
       /*   Check for FAS PRICE Threshold     */
       /******************************************/        

      BEGIN
      
        Select 
        ( CASE 
        WHEN  ABS(vn_priceamount) < fas_ovr.STRIP_PRICE_CONDITION THEN fas_ovr.PRICE_DELTA_THRESHOLD
        ELSE
          to_Number(pInternal_curves(ptargetIndex).Original_FAS157_percent)
        END
        ) Price_Threshold
        ,
        ( CASE 
        WHEN  ABS(vn_priceamount) < fas_ovr.STRIP_PRICE_CONDITION THEN 'Price Override'
        ELSE
          'Percent Value'
        END
        ) Override_Type
        , ( CASE 
        WHEN  ABS(vn_priceamount) < fas_ovr.STRIP_PRICE_CONDITION THEN  'Y'
        ELSE
        'N'
        END
        )
        INTO   
        vl_FAS_ovrride
        , vc_FAS_Value_Type
        ,vc_FAS_override_indicator
        FROM 
        RISKDB.QP_FAS_PRICE_OVERRIDE fas_ovr
        where
        UNDERLYING_COMMODITY = pInternal_curves(ptargetIndex).Underlying_Commodity
        and PRICE_VOL_INDICATOR = pCurveType
        and Trunc(vd_cobdate) Between  EFFECTIVE_START_DATE and NVL( EFFECTIVE_END_DATE, trunc(vd_cobdate))
        and active_flag = 'Y'
        ; 
        
        IF vc_FAS_Value_Type = 'Percent Value'  THEN 
           Raise NO_DATA_FOUND;
        END IF;
           
      EXCEPTION
      WHEN NO_DATA_FOUND THEN 
      
            vl_FAS_ovrride    := NULL;
            vc_FAS_Value_Type := NULL;
            
       /*******************************************/
       /*   Check for FAS Tolerance Threshold     */
       /******************************************/        
          BEGIN  
          
          IF pcurveType = 'PRICE' THEN   
          
            Select 
            OVERRIDE_FAS_PERCENT_VALUE
            , 'Percent Value' Override_Type
            , 'Y'
            INTO   
            vl_FAS_ovrride
            , vc_FAS_Value_Type
            , vc_FAS_override_indicator
            from 
            RISKDB.QP_FAS_TOLERANCE_OVERRIDE overfas_tol
            where
            PRICE_VOL_IND = pInternal_curves(ptargetIndex).PRICE_VOL_Indicator
            and Upper(COmmodity) = upper(pInternal_curves(ptargetIndex).Commodity)
            and Upper(Basis_point) = upper(pInternal_curves(ptargetIndex).Location)
            and FAS_Level = pInternal_curves(ptargetIndex).Original_FAS157_Level  
            and Trunc(vd_cobdate) Between  EFFECTIVE_START_DATE and NVL( EFFECTIVE_END_DATE, trunc(vd_cobdate))
            and active_flag = 'Y'    
            ;
          
          ELSE
            /* VOL curve FAS Override */
            Select 
            OVERRIDE_FAS_PERCENT_VALUE
            , 'Percent Value' Override_Type
            , 'Y'
            INTO   
            vl_FAS_ovrride
            , vc_FAS_Value_Type
            , vc_FAS_override_indicator
            from 
            RISKDB.QP_FAS_TOLERANCE_OVERRIDE overfas_tol
            where
            PRICE_VOL_IND = pInternal_curves(ptargetIndex).PRICE_VOL_Indicator
            and Upper(COmmodity) = upper(pInternal_curves(ptargetIndex).Commodity)
            and Upper(Basis_point)= upper( pInternal_curves(ptargetIndex).Location)
            and FAS_Level = pInternal_curves(vn_reccount).Original_FAS157_Level
            and Trunc(vd_cobdate) Between  EFFECTIVE_START_DATE and NVL( EFFECTIVE_END_DATE, Trunc(vd_cobdate))
            and active_flag = 'Y'    
            ;
            
          END IF;
              
          EXCEPTION
          WHEN NO_DATA_FOUND THEN 
            
            vl_FAS_ovrride    := NULL;
            vc_FAS_Value_Type := 'Percent Value';
            vc_FAS_override_indicator := 'N';
            pout_rtn_code := c_Success;
            pout_rtn_msg := NULL;
     
          WHEN TOO_MANY_ROWS THEN
           
           pout_rtn_code := c_Failure;
           vl_FAS_ovrride    := NULL;
           vc_FAS_Value_Type := 'Percent Value';
           vc_FAS_override_indicator := 'N'; 
           pout_rtn_code := c_failure;
           pout_rtn_msg := 'The Current FAS Threshold Override info for '||'commodity='
                           ||pInternal_curves(ptargetIndex).Commodity
                           ||'Location='||pInternal_curves(ptargetIndex).Location
                           ||' PRICE_VOL_IND ='||pInternal_curves(ptargetIndex).PRICE_VOL_Indicator
                           ||'  Is Found to have more than one record, Please verify FAS Override configuration';
          WHEN OTHERS THEN 
       
            vl_FAS_ovrride    := NULL;
            vc_FAS_Value_Type := 'Percent Value';
            vc_FAS_override_indicator := 'N';
                
             pout_rtn_code := c_failure;
             pout_rtn_msg := 'Error while Querying Current FAS Threshold Override info for '||'Underlying_commodity='
                             ||pInternal_curves(ptargetIndex).Commodity
                             ||'Location='||pInternal_curves(ptargetIndex).Location
                             ||' PRICE_VOL_IND ='||pInternal_curves(ptargetIndex).PRICE_VOL_Indicator
                             ||'-'||SUBSTR(SQLERRM,1,100);                                  
          END; 
      
        
        
       /******************************************/
      WHEN TOO_MANY_ROWS THEN 
        
        vl_FAS_ovrride    := NULL;
        vc_FAS_Value_Type := NULL;
        vc_FAS_override_indicator := 'N';
            
         pout_rtn_code := c_failure;
         pout_rtn_msg := 'The Current FAS Price Override info for '||'Underlying_commodity='
                         ||pInternal_curves(ptargetIndex).Underlying_Commodity
                         ||' PRICE_VOL_IND ='||pInternal_curves(ptargetIndex).PRICE_VOL_Indicator
                         ||'  Is Found to have more than one record, Please verify FAS Override configuration';
      WHEN OTHERS THEN 
       
        vl_FAS_ovrride    := NULL;
        vc_FAS_Value_Type := NULL;
        vc_FAS_override_indicator := 'N';
            
         pout_rtn_code := c_failure;
         pout_rtn_msg := 'Error while Querying Current FAS Price Override info for '||'Underlying_commodity='
                         ||pInternal_curves(ptargetIndex).Underlying_Commodity
                         ||' PRICE_VOL_IND ='||pInternal_curves(ptargetIndex).PRICE_VOL_Indicator
                         ||'-'||SUBSTR(SQLERRM,1,100);     
        ----dbms_output.put_line(pout_rtn_msg);                             
      END;
  
   
 
 -- if successfu in getting FAS value then Looad into target table
 pInternal_curves(ptargetIndex).Final_FAS157_Value :=  vl_FAS_ovrride;
 pInternal_curves(ptargetIndex).Final_FAS157_Type :=   vc_FAS_Value_Type;
 pInternal_curves(ptargetIndex).FAS157_Override_Indicator :=  vc_FAS_override_indicator;
 
 
 ------dbms_output.PUT_LINE ( 'After collection '||pInternal_curves(ptargetIndex).Original_FAS157_Value);
 
 --- Return Success info 
 IF pout_rtn_code <> c_failure THEN 
    pout_rtn_code := c_success;
    pout_rtn_msg := ''; --Null means Success
 END IF;
     
    
                      
 EXCEPTION
 WHEN OTHERS THEN 
   pout_rtn_code := c_failure;
   pout_rtn_msg :=  vc_section||'-'||SUBSTR(SQLERRM,1,200);
   ----dbms_output.put_line(pout_rtn_msg);
 END Get_FAS_Override;   
 

PROCEDURE Get_Location_override_info ( ptargetIndex      IN   NUMBER
                                      , pcurveType       IN   VARCHAR2     
                                      , poverrideCom     OUT  VARCHAR2
                                      , poverrideLoc     OUT  VARCHAR2     
                                      ,pout_rtn_code     OUT  NOCOPY NUMBER  
                                      ,pout_rtn_msg      OUT  NOCOPY VARCHAR2 
                                 ) 
IS
vc_section         Varchar2(1000) := 'Get_Location_override_info'; 
vc_over_commodity     RISKDB.QP_BASIS_POINT_OVERRIDE.OVERRIDE_COMMODITY%TYPE;
vc_over_location      RISKDB.QP_BASIS_POINT_OVERRIDE.OVERRIDE_BASIS_POINT%TYPE;
vc_ovr_indicator      VARCHAR2(1);
vn_market_price       RISKDB.QP_INT_FWD_CURVES_RAW_DATA.PROJ_LOC_AMT%TYPE;  
vn_bp_ovr_id          RISKDB.QP_INT_FWD_CURVES_RAW_DATA.Basispoint_Override_ID%TYPE;
BEGIN

    SELECT 
    Basis_point_Override_ID
    , OVERRIDE_COMMODITY 
     , OVERRIDE_BASIS_POINT
     , 'Y'
     INTO
     vn_bp_ovr_id
     ,  vc_over_commodity
     , vc_over_location
     , vc_ovr_indicator
     FROM 
     RISKDB.QP_BASIS_POINT_OVERRIDE
     where
     upper(COmmodity) = Upper(pInternal_curves(ptargetIndex).Commodity)
     and Upper(Basis_point) = upper(pInternal_curves(ptargetIndex).Location)
     and PRICE_VOL_INDICATOR = pcurveType
     and pInternal_curves(ptargetIndex).COB_DATE Between  EFFECTIVE_START_DATE and NVL( EFFECTIVE_END_DATE, pInternal_curves(ptargetIndex).COB_DATE)
     and active_flag = 'Y'    
     ;


-- If success then get Price for override location 
pInternal_curves(ptargetIndex).BasisPoint_Override_Indicator := 'Y';
pInternal_curves(ptargetIndex).Basispoint_Override_ID := vn_bp_ovr_id;
poverrideCom := vc_over_commodity;
poverrideLoc := vc_over_location;

  
 
EXCEPTION
WHEN  NO_DATA_FOUND THEN 
   pInternal_curves(ptargetIndex).BasisPoint_Override_Indicator := 'N';
  
    poverrideCom := NULL;
    poverrideLoc := NULL;

   pout_rtn_code := c_Success;
   pout_rtn_msg := NULL;
WHEN TOO_MANY_ROWS THEN 
   pInternal_curves(ptargetIndex).BasisPoint_Override_Indicator := 'N';
   
   poverrideCom := NULL;
   poverrideLoc := NULL;
    
   pout_rtn_code := c_Failure;
   pout_rtn_msg := 'Location Override info for '||'commodity='
                   ||pInternal_curves(ptargetIndex).Commodity
                   ||'Location='||pInternal_curves(ptargetIndex).Location
                   ||' Contract Month ='||pInternal_curves(vn_reccount).COntractYEAR_Month
                   ||' CurveType = '||pcurveType
--                   ||' PRICE_VOL_IND ='||pInternal_curves(ptargetIndex).PRICE_VOL_Indicator
                   ||'  Is Found to have more than one record, Please verify Location Override configuration';
   
WHEN OTHERS THEN 
   pInternal_curves(ptargetIndex).BasisPoint_Override_Indicator := 'N';

   poverrideCom := NULL;
   poverrideLoc := NULL;
   
   pout_rtn_code := c_failure;
   pout_rtn_msg :=  vc_section||'-'||SUBSTR(SQLERRM,1,200);
 
   dbms_output.put_line(pout_rtn_msg);
   
END Get_Location_override_info;

--added new procedure forgetting the internal price 10/28/2015

/************************************************************************** 
  * Procedure to get Internal_price depending upon BackBone_Flag *
 ***************************************************************************/

PROCEDURE Get_Internal_price (ptargetIndex    IN            NUMBER,
                              pout_rtn_code      OUT NOCOPY NUMBER,
                              pout_rtn_msg       OUT NOCOPY VARCHAR2)
IS
   vc_section        VARCHAR2 (1000) := 'Get_Internal_price ';
   v_backbone_flag   RISKDB.QP_INT_FWD_CURVES_RAW_DATA.backbone_flag%TYPE;
BEGIN

PROCESS_LOG_REC.STAGE        := 'Collect_ALL_Internal_Curves';

CASE 
 WHEN  pInternal_curves (ptargetIndex).UNDERLYING_COMMODITY = 'CAPACITY'  THEN
         pInternal_curves (ptargetIndex).backbone_flag := 'Y';
         pInternal_curves (ptargetIndex).Internal_price :=
         pInternal_curves (ptargetIndex).PROJ_LOCATION_AMT;
  
 WHEN  pInternal_curves (ptargetIndex).UNDERLYING_COMMODITY = 'ELECTRICITY' THEN
        -- THIS QUERY RETURNS A SET OF LOCATIONS WITH A SET BACKBONE FLAG THAT COMES FROM THE POWER TRANSFER POINTS TABLE
        --ELECTRICITY
     BEGIN
            SELECT DISTINCT 
            ptp.BACKBONE_FLAG
              INTO v_backbone_flag
              FROM PHOENIX.POWER_TRANSFER_POINTS ptp
                   INNER JOIN RISKDB.PRICE_CURVE_MAPPING_MV m
                      ON ( ptp.ISO_REF = m.BASIS_NODEID -- THE BASIS_NODEID AND ISO_REF ARE THE JOIN FIELDS FOR THESE TWO TABLES
                         and  m.ISO_ID = ptp.ISO_ID )
             WHERE m.BASIS_NAME = pInternal_curves (ptargetIndex).Location --pLocation
          GROUP BY m.BASIS_NAME, ptp.BACKBONE_FLAG;

          IF v_backbone_flag = 'Y'
          THEN
             pInternal_curves (ptargetIndex).backbone_flag := 'Y';
           
           IF pInternal_curves (ptargetIndex).PRICE_VOL_INDICATOR = 'PRICE' THEN   
             pInternal_curves (ptargetIndex).Internal_price :=
             pInternal_curves (ptargetIndex).PROJ_LOCATION_AMT;
           END IF;
             
          ELSIF v_backbone_flag  = 'N' THEN 
             pInternal_curves (ptargetIndex).backbone_flag := 'N';
             
            IF pInternal_curves (ptargetIndex).PRICE_VOL_INDICATOR = 'PRICE' THEN  
             pInternal_curves (ptargetIndex).Internal_price :=
             pInternal_curves (ptargetIndex).PROJ_BASIS_AMT;
            END IF; 
            
          ELSE -- Price defaulted to PROJ_BASIS_AMT 
             pInternal_curves (ptargetIndex).backbone_flag := v_backbone_flag;
             
           IF pInternal_curves (ptargetIndex).PRICE_VOL_INDICATOR = 'PRICE' THEN 
             pInternal_curves (ptargetIndex).Internal_price :=
             pInternal_curves (ptargetIndex).PROJ_BASIS_AMT;
           END IF;  
                
          END IF;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN 
         v_backbone_flag := NULL ;
         pInternal_curves (ptargetIndex).Internal_price := pInternal_curves (ptargetIndex).PROJ_BASIS_AMT;
        WHEN TOO_MANY_ROWS THEN 
         v_backbone_flag := NULL ;
         pInternal_curves (ptargetIndex).Internal_price := pInternal_curves (ptargetIndex).PROJ_BASIS_AMT;
        WHEN OTHERS THEN 
         v_backbone_flag := NULL ;
         pInternal_curves (ptargetIndex).Internal_price := pInternal_curves (ptargetIndex).PROJ_BASIS_AMT;
    END; 
   
  --NON ELECTRICITY
  WHEN  pInternal_curves (ptargetIndex).UNDERLYING_COMMODITY <>
            'ELECTRICITY' AND pInternal_curves (ptargetIndex).UNDERLYING_COMMODITY <> 'CAPACITY'
   THEN
       BEGIN 
       
         Select 
            CASE 
            WHEN nc.price_id = nc.bb_price_id THEN 'Y' ELSE 'N' 
            END   INTO v_backbone_flag
         FROM 
            nucdba.commodities cm
            , riskdb.nuc_curve nc
         WHERE
            nc.cm_commodity = cm.commodity
            AND cm.unc_underlying_commodity =
                                       pInternal_curves(ptargetIndex).Underlying_commodity 
            and nc.bp_basis_point = pInternal_curves(ptargetIndex).LOCATION
         ;

        EXCEPTION
        WHEN NO_DATA_FOUND THEN 
         v_backbone_flag := NULL ;
        WHEN TOO_MANY_ROWS THEN 
         v_backbone_flag := NULL ;
        WHEN OTHERS THEN 
         v_backbone_flag := 'N' ;
        END;

      IF v_backbone_flag IS NOT NULL AND v_backbone_flag = 'Y'THEN 
      
        pInternal_curves (ptargetIndex).backbone_flag := 'Y';
      
      IF pInternal_curves (ptargetIndex).PRICE_VOL_INDICATOR = 'PRICE' THEN
        pInternal_curves (ptargetIndex).Internal_price :=
        pInternal_curves (ptargetIndex).PROJ_LOCATION_AMT;
      END IF;
      
      ELSIF v_backbone_flag IS NOT NULL AND v_backbone_flag = 'N' THEN
         pInternal_curves (ptargetIndex).backbone_flag  := 'N';
        
        IF pInternal_curves (ptargetIndex).PRICE_VOL_INDICATOR = 'PRICE' THEN  
         pInternal_curves (ptargetIndex).Internal_price :=
         pInternal_curves (ptargetIndex).PROJ_BASIS_AMT;
        END IF;
        
      ELSE
         pInternal_curves (ptargetIndex).backbone_flag  := NULL;
        
        IF pInternal_curves (ptargetIndex).PRICE_VOL_INDICATOR = 'PRICE' THEN 
         pInternal_curves (ptargetIndex).Internal_price := pInternal_curves (ptargetIndex).PROJ_BASIS_AMT;
        END IF;
         
      END IF;
ELSE
  NULL;
END CASE; 

   --- Return Success info
   pout_rtn_code := c_success;
   pout_rtn_msg := '';                                    --Null means Success

END Get_Internal_price;

 
BEGIN 
 
 PROCESS_LOG_REC.STAGE        := 'Collect_ALL_Internal_Curves';
 
  
 FOr recs in  internalCurves_cur(vd_cobdate) 
 LOOP
 
 --dbms_output.PUT_LINE(recs.LOCATION);
        
      -- if this Location is related to VOL curve (means DAILY VOL available)
      -- need to replicate VOL Elements and Measures in collection
      IF recs.Abs_M2m_leg_value_amt_vol IS NOT NULL THEN  
         vs_curve_Type :=  'VOL'; 
         --dbms_output.PUT_LINE('vol curve!,  repeat loop twice'); 
      ELSE
          vs_curve_Type :=  'PRICE';  
      END IF;
  
  
      pInternal_curves.EXTEND;
      vn_reccount := vn_reccount + 1 ;  
  
--      dbms_output.PUT_LINE(
--      'UC='||recs.UNDERLYING_COMMODITY||
--      'Commodity='||recs.Commodity ||
--      ' Location='||recs.LOCATION ||
--      ' ConractMonth ='||recs.CONTRACTyear_MONTH ||
--      ' curveType='||vs_curve_Type||
--      ' indx='||curvetype_loop ||
--      ' recindx'||vn_reccount
--      );
  
      --Default values first
      pInternal_curves(vn_reccount).ACTIVE_FLAG            :=  'Y' ;
      pInternal_curves(vn_reccount).Basis_Curve_Indicator  
                                 := NVL(recs.Basis_Curve_Indicator, 'N');
      
      pInternal_curves(vn_reccount).BasisPoint_Override_Indicator  
                                 := 'N';
      pInternal_curves(vn_reccount).FAS157_Override_Indicator  
                                 := 'N';
      
                                                                                   
      pInternal_curves(vn_reccount).COB_DATE   := recs.cob_date;
      
      pInternal_curves(vn_reccount).UNDERLYING_COMMODITY   
                                 := recs.UNDERLYING_COMMODITY;
       
      pInternal_curves(vn_reccount).NETTING_GROUP_ID      := recs.NETTING_GROUP_ID ;
      
      pInternal_curves(vn_reccount).COMMODITY             := recs.COMMODITY     ;  
      
      pInternal_curves(vn_reccount).LOCATION              := recs.LOCATION      ;
      
      pInternal_curves(vn_reccount).Hour_TYPE             := recs.Hour_TYPE      ;
      pInternal_curves(vn_reccount).on_off_peak_Indicator  
                                  := recs.Hour_type_on_OFF      ;
        
      pInternal_curves(vn_reccount).Initial_Rank          := recs.Initial_Rank ;
      

      pInternal_curves(vn_reccount).Original_FAS157_Level :=   recs.original_fas_level ;

      pInternal_curves(vn_reccount).PRICE_VOL_Indicator  
                                    :=   vs_curve_Type ;
                                    

      IF vs_curve_Type = 'PRICE' THEN

       pInternal_curves(vn_reccount).PROJ_LOC_AMT         :=   recs.PROJ_LOC_AMT ; 
       pInternal_curves(vn_reccount).PROJ_LOCATION_AMT    :=  recs.PROJ_LOCATION_AMT ;
       
       pInternal_curves(vn_reccount).PROJ_PREMIUM_AMT     := recs.PROJ_PREMIUM_AMT ; 
       pInternal_curves(vn_reccount).PROJ_BASIS_AMT       := recs.PROJ_BASIS_AMT ;  
       
     END IF;
     
      IF vs_curve_Type = 'VOL' THEN
         -- Internal Price stores VOL_CURVE , switched from assigning to PROJ_LOC_AMT
         -- to Internal_price column --sharana 10/31/2015
          pInternal_curves(vn_reccount).internal_price   := recs.VOL_CURVE ;
          pInternal_curves(vn_reccount).monthly_vol      :=   recs.monthly_vol ;      
      END IF;                                   

--calling the PROCEDURE Get_Internal_price to get the inter_price values  10/28/2015


    -- called first time for PRICE and 
       -- sedond time for VOL curve
    --  dbms_output.Put_line('ptargetindex='||vn_reccount); 
     
                                    
                                      
       Get_Location_override_info( 
                         ptargetIndex => vn_reccount
                       , pcurveType   => vs_curve_Type
                       , poverrideCom => vc_overcom   
                       , poverrideLoc => vc_overLoc
                       , pout_rtn_code  => v_rtn_code     
                       , pout_rtn_msg   => v_rtn_msg 
                        );
 
       IF v_rtn_code <> c_success THEN
      
        -- Log Error Messgae and Continue to process rest
         PROCESS_LOG_REC.STATUS       := PROCESS_LOG_PKG.C_STATUS_ERRORS;
         PROCESS_LOG_REC.MESSAGE      := v_rtn_msg;
         
         dmr.process_log_pkg.write_log( 
                                p_stage=>'Get_Location_override_info',
                                p_status=>PROCESS_LOG_PKG.C_STATUS_ERRORS, 
                                p_message=> v_rtn_msg
                                       );
       ELSE
       
--           IF vc_overcom IS NOT NULL AND vc_overLoc IS NOT NULL THEN 
--              
--                 pInternal_curves(vn_reccount).COMMODITY := vc_overcom     ;  
--      
--                 pInternal_curves(vn_reccount).LOCATION  := vc_overLoc      ;
--           END IF;    
            
            NULL;
                                                             
                               
       END IF; 



         Get_Internal_price ( ptargetIndex => vn_reccount
                   , pout_rtn_code  => v_rtn_code     
                   , pout_rtn_msg   => v_rtn_msg 
                       );  
    
         IF v_rtn_code <> c_success THEN
      
            -- Log Error Messgae and Continue to process rest
             PROCESS_LOG_REC.STATUS       := PROCESS_LOG_PKG.C_STATUS_ERRORS;
             PROCESS_LOG_REC.MESSAGE      := v_rtn_msg;
             
             dmr.process_log_pkg.write_log( 
                                    p_stage=>'Get_Internal_price',
                                    p_status=>PROCESS_LOG_PKG.C_STATUS_ERRORS, 
                                    p_message=> v_rtn_msg
                                           );
                                   
        END IF; 
        
        
--        --- IF Location was overridden in basis Point overide table
--        -- price was brought in for Override Location 
--        -- hence the process need to restore original Location and commodity
--        -- for final stage 2 data 
--        
--           IF vc_overcom IS NOT NULL AND vc_overLoc IS NOT NULL THEN 
--              
--                 pInternal_curves(vn_reccount).COMMODITY := recs.COMMODITY     ;    
--      
--                 pInternal_curves(vn_reccount).LOCATION  := recs.Location      ;
--
--           END IF;    
     
     --reset Local variables 
     vc_overcom :=  NULL;
     vc_overLoc := NULL;   
 
    
  -- End of changes
                                    
      IF vs_curve_Type = 'PRICE' THEN
         pInternal_curves(vn_reccount).Location_DELTA_POSITION  
                                  := recs.Location_Delta_position;
                                  
       pInternal_curves(vn_reccount).Commodity_Absolute_position 
                                  := recs.Commodity_Delta_position;

      pInternal_curves(vn_reccount).m2m_Value  := recs.M2M_Value;
      
       pInternal_curves(vn_reccount).LEGGED_M2M_Value  := recs.Legged_m2m_Value;
 
      -- Assinging absolute m2m values 10/27/2015
      pInternal_curves(vn_reccount).ABS_M2M_VALUE_AMT  := recs.ABS_M2M_VALUE_AMT;
      
       -- Assinging absolute m2m values 10/27/2015
      pInternal_curves(vn_reccount).ABS_M2M_LEG_VALUE_AMT  := recs.ABS_M2M_LEG_VALUE_AMT;

                            
      END IF;
      
      IF vs_curve_Type = 'VOL' THEN
      
      pInternal_curves(vn_reccount).Location_DELTA_POSITION  
                                  := recs.Location_Delta_position_VOL;

      pInternal_curves(vn_reccount).Commodity_Absolute_position 
                                  := recs.Commodity_Delta_position_VOL;

      pInternal_curves(vn_reccount).m2m_Value  := recs.M2M_Value_VOL;

      pInternal_curves(vn_reccount).LEGGED_M2M_Value  := recs.Legged_m2m_Value_VOL;
      
       -- Assinging absolute m2m values 10/27/2015
      pInternal_curves(vn_reccount).ABS_M2M_VALUE_AMT_VOL := recs.ABS_M2M_VALUE_AMT_VOL;

       -- Assinging absolute m2m values 10/27/2015 
      pInternal_curves(vn_reccount).ABS_M2M_LEG_VALUE_AMT_VOL  := recs.ABS_M2M_LEG_VALUE_AMT_VOL; 

      END IF; 
      
     
    --  pInternal_curves(vn_reccount).M2M_RISK_TYPE             := recs.SOURCE;
    --  pInternal_curves(vn_reccount).VOLUME_UOM                := recs.VOLUME_UOM ;
 
      
 
       pInternal_curves(vn_reccount).max_ContractYEAR_Month :=  recs.max_ContractYEAR_Month;
       pInternal_curves(vn_reccount).min_ContractYEAR_Month :=  recs.min_ContractYEAR_Month;
      
      
      pInternal_curves(vn_reccount).COntractYEAR_Month    := recs.CONTRACTYEAR_MONTH ;
      pInternal_curves(vn_reccount).CONTRACT_YEAR           
                                  := SUBSTR(recs.CONTRACTYEAR_MONTH,1,4) ;
      pInternal_curves(vn_reccount).CONTRACT_MONTH  
                                  := SUBSTR(recs.CONTRACTYEAR_MONTH,5,2) ;
     
      pInternal_curves(vn_reccount).cal_days_in_Contract_month 
                                    := TO_NUMBER(TO_CHAR(LAST_DAY(to_DATE(recs.contractYear_MOnth||'01','YYYYMMDD')), 'DD')); --recs.Days_in_ContractMOnth;
    

    
      pInternal_curves(vn_reccount).PARTITION_BIT        := recs.PARTITION_BIT;

      pInternal_curves(vn_reccount).Processed_Level := 1;
      
      pInternal_curves(vn_reccount).Basis_Curve_Indicator   
                                  := NVL(recs.Basis_Curve_Indicator, 'N');
      pInternal_curves(vn_reccount).Basis_Quote_date    := recs.Basis_Quote_date ;
      pInternal_curves(vn_reccount).Basis_quote_age     := recs.Basis_quote_age;           
      pInternal_curves(vn_reccount).Trader_Basis_Quote_Price 
                                  := recs.Trader_Basis_Quote_Price;                                       
      pInternal_curves(vn_reccount).using_100prcnt_Hist_method 
                                  := recs.using_100prcnt_Hist_method; 

      IF  recs.using_100prcnt_Hist_method = 'Y' THEN 
         pInternal_curves(vn_reccount).Initial_Rank  := 2; 
      END IF;

     pInternal_curves(vn_reccount).term_start := recs.term_start;
     pInternal_curves(vn_reccount).term_end   := recs.term_end;
     pInternal_curves(vn_reccount).basis_ask  := recs.basis_ask;
     pInternal_curves(vn_reccount).Basis_bid  := recs.Basis_bid;
     pInternal_curves(vn_reccount).Broker     := recs.Broker;

      pInternal_curves(vn_reccount).Phoenix_basis_name       
                                  := recs.Phoenix_basis_name;
      
      /* Get the FAS definition */
      PROCESS_LOG_REC.MESSAGE        := 'Get the Current FAS Level Percent Value';

      v_rtn_code                 := NULL;
      v_rtn_msg                  := NULL;
      v_section                  := 'Collect_ALL_Internal_Curves';
  
   
    
   
   IF pInternal_curves(vn_reccount).UNDERLYING_COMMODITY = 'ELECTRICITY' THEN 
   --- CALCULATE COntract HOURS for the contract_month 
           Get_Hours_Info ( ptargetIndex => vn_reccount
                       , pout_rtn_code  => v_rtn_code     
                       , pout_rtn_msg   => v_rtn_msg  
                           ) ;                          
 

           IF v_rtn_code <> c_success THEN
          
            -- Log Error Messgae and Continue to process rest
             PROCESS_LOG_REC.STATUS       := PROCESS_LOG_PKG.C_STATUS_ERRORS;
             PROCESS_LOG_REC.MESSAGE      := v_rtn_msg;
                     
             dmr.process_log_pkg.write_log( 
                                     p_stage=>'Get_Hours_Info',
                                     p_status=>PROCESS_LOG_PKG.C_STATUS_ERRORS, 
                                     p_message=> v_rtn_msg
                                     );

           END IF; 
                                  
  END IF;
 -- end of pInternal_curves(vn_reccount).UNDERLYING_COMMODITY = 'ELECTRICITY'     
 
  
  
   IF  pInternal_curves(vn_reccount).Original_FAS157_Level is NULL THEN 
  
     -- Log exception only once for PRICE Curve 
     -- Ignore for VOL Curve
       IF vs_curve_Type = 'PRICE' THEN 
       
           Begin
             INSERT INTO RISKDB.QP_INT_FWD_Curves_exceptions
                VALUES pInternal_curves(vn_reccount)
             ;
            -- null;
           Exception
           when Others then 
              ----dbms_output.pUT_LINE('FAS exception-'||vn_reccount); 
              null; 
           End;
             
      END IF;
        
                                   
   ELSE -- else for FAS level NULL check 
     
     IF vs_curve_Type = 'PRICE' THEN
       
       Get_FAS_level_percent( 
                        ptargetIndex => vn_reccount
                       , pout_rtn_code  => v_rtn_code     
                       , pout_rtn_msg   => v_rtn_msg 
                        );
        
     
                       
        IF v_rtn_code <> c_success THEN
  
        -- Log Error Messgae and Continue to process rest
         PROCESS_LOG_REC.STATUS       := PROCESS_LOG_PKG.C_STATUS_STARTED;
         PROCESS_LOG_REC.MESSAGE      := v_rtn_msg;
     
         dmr.process_log_pkg.write_log( 
                                      p_stage=>'Get_FAS_level_percent',
                                      p_status=>PROCESS_LOG_PKG.C_STATUS_ERRORS, 
                                      p_message=> v_rtn_msg
                                      );

        END IF;
        
        
      
     IF v_rtn_code = c_Success THEN  
       
            v_rtn_code                 := NULL;
            v_rtn_msg                  := NULL;
            v_section                  := 'Get_FAS_Override';
      
            PROCESS_LOG_REC.MESSAGE   := 'Get the Current FAS Level Override Value';
            
            Get_FAS_Override (       ptargetIndex   =>  vn_reccount
                                    , pCurveType    =>  vs_curve_Type
                                    , pvolAmount    => recs.VOL_CURVE
                                    ,pout_rtn_code  =>  v_rtn_code 
                                    ,pout_rtn_msg   =>  v_rtn_msg
                              );
                          
            IF v_rtn_code <> c_success THEN
      
            -- Log Error Messgae and Continue to process rest
             PROCESS_LOG_REC.STATUS       := PROCESS_LOG_PKG.C_STATUS_ERRORS;
             PROCESS_LOG_REC.MESSAGE      := v_rtn_msg;
         
             dmr.process_log_pkg.write_log( p_stage=>'Get_FAS_Override',
                                             p_status=>PROCESS_LOG_PKG.C_STATUS_ERRORS, 
                                             p_message=> v_rtn_msg
                                           );

            END IF; 
        
         
 
        IF pInternal_curves(vn_reccount).FAS157_Override_Indicator = 'N' THEN
           pInternal_curves(vn_reccount).Final_FAS157_Value :=  
                        pInternal_curves(vn_reccount).Original_FAS157_percent;
           pInternal_curves(vn_reccount).Final_FAS157_Type :=
                                'Percent Value';
                           
        END IF;
        


       END IF;
        -- end of IF v_rtn_code = c_Success THEN 
   ELSE
       
       -- FOR VOL FAS % same LIKE Previous PRICE level
       -- no need to recompute
       pInternal_curves(vn_reccount).Original_FAS157_percent 
          :=   pInternal_curves(vn_reccount-1).Original_FAS157_percent ;
     
       v_rtn_code := c_Success;
       
       
            v_rtn_code                 := NULL;
            v_rtn_msg                  := NULL;
            v_section                  := 'Get_FAS_Override';
      
            PROCESS_LOG_REC.MESSAGE   := 'Get the Current FAS Override for VOL';
            
            Get_FAS_Override (       ptargetIndex   =>  vn_reccount
                                    , pCurveType    =>  vs_curve_Type
                                    , pvolAmount    => recs.VOL_CURVE
                                    ,pout_rtn_code  =>  v_rtn_code 
                                    ,pout_rtn_msg   =>  v_rtn_msg
                              );
                          
            IF v_rtn_code <> c_success THEN
      
            -- Log Error Messgae and Continue to process rest
             PROCESS_LOG_REC.STATUS       := PROCESS_LOG_PKG.C_STATUS_ERRORS;
             PROCESS_LOG_REC.MESSAGE      := v_rtn_msg;
         
             dmr.process_log_pkg.write_log( p_stage=>'Get_FAS_Override',
                                             p_status=>PROCESS_LOG_PKG.C_STATUS_ERRORS, 
                                             p_message=> v_rtn_msg
                                           );

            END IF; 
        
         
 
        IF pInternal_curves(vn_reccount).FAS157_Override_Indicator = 'N' THEN
           pInternal_curves(vn_reccount).Final_FAS157_Value :=  
                        pInternal_curves(vn_reccount).Original_FAS157_percent;
           pInternal_curves(vn_reccount).Final_FAS157_Type :=
                                'Percent Value';
                           
        END IF;

   END IF;
    -- end of vs_curve_Type = 'PRICE'
   
   
   
   END IF; -- end of FAS Level NULL check 
     
    
     
                                     
--  pInternal_curves(pInternal_curves.LAST).OVERRIDE_COMMODITY        := NULL               ; 
--  pInternal_curves(pInternal_curves.LAST).OVERRIDE_LOCATION         := NULL               ;  

       vs_curve_Type :=  NULL; -- reset the Variable
   
   END LOOP; -- Price or VOL Iteration 

 
END Collect_ALL_Internal_Curves;


PROCEDURE   Load_Internal_target (
                           pInternal_curves   IN OUT NOCOPY Internal_curves_T
                          ,pout_rtn_code     OUT  NOCOPY NUMBER  
                          ,pout_rtn_msg      OUT  NOCOPY VARCHAR2
                          )
IS

vc_section VARCHAR2(30) := ' Load_Internal_target';
e_bulk_insert_errors EXCEPTION;
PRAGMA EXCEPTION_INIT(e_bulk_insert_errors, -24381);

vl_recCount           NUMBER;  
vl_recCount_start     NUMBER := 1;
vl_recCount_end       NUMBER := 0;
vn_excptn_indx        NUMBER;

BEGIN  

vl_recCount := pInternal_curves.COUNT;

----dbms_output.Put_line ('b4 bulk insert'||vl_recCount);


IF vl_recCount < 10000 THEN
   vl_recCount_start := 1;
   vl_recCount_end   := vl_recCount;

ELSE
 
       vl_recCount_start := 1;
       vl_recCount_end   := 10000;

END IF;
   
    WHILE  vl_recCount_start <= vl_recCount 
    LOOP
     

       
      ----dbms_output.PUT_LINE ('BULK FROM'||vl_recCount_start||' to '||vl_recCount_end) ;

        BEGIN

        FORALL i IN vl_recCount_start .. vl_recCount_end  SAVE EXCEPTIONS 
        INSERT INTO RISKDB.QP_INT_FWD_Curves_raw_data
         VALUES pInternal_curves(i)
        ;
         
         COMMIT;
             
        EXCEPTION
        When dup_val_on_index then
            
         dbms_output.put_line('exception inserting into RISKDB.QP_INT_FWD_Curves_raw_data'||sqlerrm); 
         null;
        WHEN e_bulk_insert_errors THEN
        
        pout_rtn_code := c_failure;
             
            FOR i IN 1..SQL%BULK_EXCEPTIONS.COUNT LOOP
            
                vn_excptn_indx := SQL%BULK_EXCEPTIONS(i).ERROR_INDEX;
                      
               IF nvl(pInternal_curves(vn_excptn_indx).ORIGINAL_FAS157_LEVEL, 0) = 0  
               Then   
                   --report the Issue otherwise taken care 
                   dbms_output.PUT_LINE(
                      'NG='||pInternal_curves(vn_excptn_indx).NETTING_GROUP_ID ||
                      'UC='||pInternal_curves(vn_excptn_indx).UNDERLYING_COMMODITY ||
                      ' COM='||pInternal_curves(vn_excptn_indx).COMMODITY ||
                      ' Loc='||pInternal_curves(vn_excptn_indx).LOCATION ||
                      ' ConractMonth ='||pInternal_curves(vn_excptn_indx).COntractYEAR_Month ||
                      ' curveType='|| pInternal_curves(vn_excptn_indx).PRICE_VOL_Indicator
                   );
                   
              END IF;
                                         
             END LOOP;
             
            END;

       -- Recalculate the Lower and Upper Bounds 
       vl_recCount_start := vl_recCount_end+1;
       vl_recCount_end   := vl_recCount_start + 10000;
       
       IF vl_recCount_end > vl_recCount THEN 
          vl_recCount_end := vl_recCount;
       END IF;
       
    END LOOP;
    
----------dbms_output.Put_line ('After bulk insert');
   pout_rtn_code := c_success;
   pout_rtn_msg := ''; --Null means Success

EXCEPTION
WHEN OTHERS THEN 

       pout_rtn_code := c_failure;
       pout_rtn_msg := vc_section||SUBSTR(SQLERRM,1,200);
      dbms_output.put_line(pout_rtn_msg);
END   Load_Internal_target;


PROCEDURE   Load_Target_Stage3 (
                           pmnthly_collectn  IN OUT NOCOPY strips_collectn_t
                          ,pout_rtn_code     OUT  NOCOPY NUMBER  
                          ,pout_rtn_msg      OUT  NOCOPY VARCHAR2
                          )
IS

vc_section VARCHAR2(30) := ' Load_Target_Stage3';
e_bulk_insert_errors EXCEPTION;

--- 24381 is a Fixed ERROR Code from ORACLE for BULK ERROR 
PRAGMA EXCEPTION_INIT(e_bulk_insert_errors, -24381);

vl_recCount           NUMBER;  
vl_recCount_start     NUMBER := 1;
vl_recCount_end       NUMBER := 0;
vn_excptn_indx        NUMBER;

BEGIN  

vl_recCount := pmnthly_collectn.COUNT;

--dbms_output.Put_line ('b4 bulk insert'||vl_recCount);


IF vl_recCount < 10000 THEN
   vl_recCount_start := 1;
   vl_recCount_end   := vl_recCount;

ELSE
 
       vl_recCount_start := 1;
       vl_recCount_end   := 10000;

END IF;
   
    WHILE  vl_recCount_start <= vl_recCount 
    LOOP
     

       
      dbms_output.PUT_LINE ('BULK FROM'||vl_recCount_start||' to '||vl_recCount_end) ;

        BEGIN

        FORALL i IN vl_recCount_start .. vl_recCount_end  SAVE EXCEPTIONS 
        INSERT INTO RISKDB.QP_STRIP_LVL_NEER_EXT_DATA
         VALUES pmnthly_collectn(i)
        ;
         
         COMMIT;
             
        EXCEPTION
       
        WHEN e_bulk_insert_errors THEN
        
        pout_rtn_code := c_failure;
             
            FOR i IN 1..SQL%BULK_EXCEPTIONS.COUNT LOOP
            
                vn_excptn_indx := SQL%BULK_EXCEPTIONS(i).ERROR_INDEX;
                      
                   --report the Issue otherwise taken care 
                    dbms_output.PUT_LINE(
                     ' COB_DATE ='||TO_CHAr(pmnthly_collectn(vn_excptn_indx).COB_DATE, 'YYYYMMDD')
                     ||', NETTING_GROUP_ID = '||pmnthly_collectn(vn_excptn_indx).NETTING_GROUP_ID
                     ||', UNDERLYING_COMMODITY='||pmnthly_collectn(vn_excptn_indx).UNDERLYING_COMMODITY
                     ||', NEER_COMMODITY='||pmnthly_collectn(vn_excptn_indx).NEER_COMMODITY
                     ||', NEER_LOCATION='||pmnthly_collectn(vn_excptn_indx).NEER_LOCATION
                     ||', ContractYr='||pmnthly_collectn(vn_excptn_indx).CONTRACT_YEAR
                     ||', PRICE_VOL_INDICATOR='||pmnthly_collectn(vn_excptn_indx).PRICE_VOL_INDICATOR
                     ||', STRIP_TENOR='||pmnthly_collectn(vn_excptn_indx).STRIP_TENOR
                     ||', STRIP_START_YEARMONTH='||pmnthly_collectn(vn_excptn_indx).STRIP_START_YEARMONTH
                     ||', STRIP_END_YEARMONTH='||pmnthly_collectn(vn_excptn_indx).STRIP_END_YEARMONTH
                     ||', EXT_PROVIDER_ID='||pmnthly_collectn(vn_excptn_indx).EXT_PROVIDER_ID
                     ||', EXT_PROFILE_ID='||pmnthly_collectn(vn_excptn_indx).EXT_PROFILE_ID
                     ||' EXT_LOCATION='||pmnthly_collectn(vn_excptn_indx).EXT_LOCATION
                     ||', EXT_MANUAL_QUOTE_ID='||pmnthly_collectn(vn_excptn_indx).EXT_MANUAL_QUOTE_ID
                     );
                     -- reinsert to Understand the Type of Error 
                    BEGIN 
                         INSERT INTO RISKDB.QP_STRIP_LVL_NEER_EXT_DATA
                         VALUES pmnthly_collectn(vn_excptn_indx) ;
                    EXCEPTION     
                    WHEN OTHERS THEN 

                        pout_rtn_code := c_failure;
                        pout_rtn_msg := vc_section||SUBSTR(SQLERRM,1,200); 
                        --dbms_output.Put_line(pout_rtn_msg);
                   END;
                              
             END LOOP;
             
            END;

       -- Recalculate the Lower and Upper Bounds 
       vl_recCount_start := vl_recCount_end+1;
       vl_recCount_end   := vl_recCount_start + 10000;
       
       IF vl_recCount_end > vl_recCount THEN 
          vl_recCount_end := vl_recCount;
       END IF;
       
    END LOOP;
  
    COMMIT;  

------dbms_output.Put_line ('After bulk insert');
   pout_rtn_code := c_success;
   pout_rtn_msg := ''; --Null means Success

EXCEPTION
WHEN OTHERS THEN 

       pout_rtn_code := c_failure;
       pout_rtn_msg := vc_section||SUBSTR(SQLERRM,1,200);
      ----dbms_output.put_line(pout_rtn_msg);
END   Load_Target_Stage3;


PROCEDURE   Load_Qvar_target (
                           pcollectn  IN OUT NOCOPY strips_collectn_t
                          ,pout_rtn_code     OUT  NOCOPY NUMBER  
                          ,pout_rtn_msg      OUT  NOCOPY VARCHAR2
                          )
IS

vc_section VARCHAR2(30) := ' Load_Target_Stage3';
vc_message  VARCHAR2(2000); 

e_bulk_insert_errors EXCEPTION;

--- 24381 is a Fixed ERROR Code from ORACLE for BULK ERROR 
PRAGMA EXCEPTION_INIT(e_bulk_insert_errors, -24381);

vl_recCount           NUMBER;  
vl_recCount_start     NUMBER := 1;
vl_recCount_end       NUMBER := 0;
vn_excptn_indx        NUMBER;

BEGIN  

vl_recCount := pcollectn.COUNT;

--dbms_output.Put_line ('b4 bulk insert'||vl_recCount);


 

IF vl_recCount < 10000 THEN
   vl_recCount_start := 1;
   vl_recCount_end   := vl_recCount;

ELSE
 
       vl_recCount_start := 1;
       vl_recCount_end   := 10000;

END IF;
   
    WHILE  vl_recCount_start <= vl_recCount 
    LOOP
     

       
      dbms_output.PUT_LINE ('BULK FROM'||vl_recCount_start||' to '||vl_recCount_end) ;

        BEGIN

        FORALL i IN vl_recCount_start .. vl_recCount_end  SAVE EXCEPTIONS 
        INSERT INTO RISKDB.QP_STRIP_LVL_NEER_EXT_DATA
         VALUES pcollectn(i)
        ;
         
         COMMIT;
             
        EXCEPTION
       
        WHEN e_bulk_insert_errors THEN
        
        pout_rtn_code := c_failure;
             
            FOR i IN 1..SQL%BULK_EXCEPTIONS.COUNT LOOP
            
              vn_excptn_indx := SQL%BULK_EXCEPTIONS(i).ERROR_INDEX;
                
              IF INSTR (  SQLERRM(-SQL%BULK_EXCEPTIONS(i).ERROR_CODE),'unique constraint', 1,1) = 0  THEN     
                   --report the Issue otherwise taken care 
                   vc_message := 
                     ' COB_DATE ='||TO_CHAr(pcollectn(vn_excptn_indx).COB_DATE, 'YYYYMMDD')
                     ||', NETTING_GROUP_ID = '||pcollectn(vn_excptn_indx).NETTING_GROUP_ID
                     ||', UNDERLYING_COMMODITY='||pcollectn(vn_excptn_indx).UNDERLYING_COMMODITY
                     ||', NEER_COMMODITY='||pcollectn(vn_excptn_indx).NEER_COMMODITY
                     ||', NEER_LOCATION='||pcollectn(vn_excptn_indx).NEER_LOCATION
                     ||', ContractYr='||pcollectn(vn_excptn_indx).CONTRACT_YEAR
                     ||', PRICE_VOL_INDICATOR='||pcollectn(vn_excptn_indx).PRICE_VOL_INDICATOR
                     ||', STRIP_TENOR='||pcollectn(vn_excptn_indx).STRIP_TENOR
                     ||', STRIP_START_YEARMONTH='||pcollectn(vn_excptn_indx).STRIP_START_YEARMONTH
                     ||', STRIP_END_YEARMONTH='||pcollectn(vn_excptn_indx).STRIP_END_YEARMONTH
                     ||', EXT_PROVIDER_ID='||pcollectn(vn_excptn_indx).EXT_PROVIDER_ID
                     ||', EXT_PROFILE_ID='||pcollectn(vn_excptn_indx).EXT_PROFILE_ID
                     ||' EXT_LOCATION='||pcollectn(vn_excptn_indx).EXT_LOCATION
                     ||', EXT_MANUAL_QUOTE_ID='||pcollectn(vn_excptn_indx).EXT_MANUAL_QUOTE_ID
                     ||'=>'||SUBSTR(SQLERRM(-SQL%BULK_EXCEPTIONS(i).ERROR_CODE),1,50)
                    ;
                    
                    dbms_output.Put_line( vn_excptn_indx||'-Unique constraint error '
 --                   ||vc_message
                    );
                    
                     -- reinsert to Understand the DETAILED ERROR MESSAGE  
--                    BEGIN 
--                         INSERT INTO RISKDB.QP_STRIP_LVL_NEER_EXT_DATA
--                         VALUES pcollectn(vn_excptn_indx) ;
--                    EXCEPTION     
--                    WHEN OTHERS THEN 
--                      
--                      IF INSTR(sqlerrm,'unique constraint') > 0 THEN
--                        null; -- ignore error 
--                      ELSE
--                        
--                        pout_rtn_code := c_failure;
--                        pout_rtn_msg := vc_section||SUBSTR(SQLERRM,1,200); 
--                        dbms_output.Put_line(vc_message||'==>'||pout_rtn_msg);
--                      END IF;
                      
--                   END;
              ELSE
                 null; --ignore unqie cpnstraint errors if any 
              END IF;
                              
             END LOOP;
             
            END;

       -- Recalculate the Lower and Upper Bounds 
       vl_recCount_start := vl_recCount_end+1;
       vl_recCount_end   := vl_recCount_start + 10000;
       
       IF vl_recCount_end > vl_recCount THEN 
          vl_recCount_end := vl_recCount;
       END IF;
       
    END LOOP;
  
    COMMIT;  

------dbms_output.Put_line ('After bulk insert');
   pout_rtn_code := c_success;
   pout_rtn_msg := ''; --Null means Success

EXCEPTION
WHEN OTHERS THEN 

       pout_rtn_code := c_failure;
       pout_rtn_msg := vc_section||SUBSTR(SQLERRM,1,200);
      ----dbms_output.put_line(pout_rtn_msg);
END   Load_qvar_target;

PROCEDURE WRITE_LOG 
IS
BEGIN

   -- Log Message first
   DMR.PROCESS_LOG_PKG.WRITE_LOG(
                            P_PROCESS_NAME => PROCESS_LOG_REC.PROCESS_NAME,
                            P_STAGE=> PROCESS_LOG_REC.STAGE,
                            P_STATUS=>PROCESS_LOG_REC.STATUS,
                            P_MESSAGE=> PROCESS_LOG_REC.MESSAGE
                            );
                            
END WRITE_LOG;
     

PROCEDURE INTERNAL_FWD_CURVES_LOAD   (
                                       PCOBDATE IN  DATE
                                       , PSTATUSCODE OUT VARCHAR2
                                       , PSTATUSMSG OUT VARCHAR2
                                      )
  Is
  
  li_errors                      NUMBER;
  Vc_section                     VARCHAR2(100);
  vn_elapsed_hrs                 NUMBER;
  vc_qvarStatus                  VARCHAR2(1);
  vn_qvarrun_id                  NUMBER;
  vd_timestamp                   date;   
  vn_rtn_code                    NUMBER;
  vc_rtn_msg                     VARCHAR2(500);
  vd_cobdate                     DATE;
  vc_cobDate                     VARCHAR2(11) ;
  vc_singleQuote                 VARCHAR2(1) := CHR(39);                 
  vc_sql_stmt                    VARCHAR2(1000);

  vb_continue_processing        Boolean := FALSE;
  
  vn_starttime                  NUMBER;
  vn_endtime                    NUMBER;
  vn_reccount                   NUMBER;
  
  vn_starttimeStamp             DATE := SYSDATE ;
  
  -- create a Local collection Variable 
  Internal_curves               Internal_curves_T;
  
   e_Default_COBDATE_NotFound     EXCEPTION;
   PRAGMA EXCEPTION_INIT( e_Default_COBDATE_NotFound , -20020);
   
   e_Appl_error                   EXCEPTION;
   PRAGMA EXCEPTION_INIT( e_Appl_error , -20021);
 
   e_QVAR_DATA_FINALIZED          EXCEPTION;
   PRAGMA EXCEPTION_INIT( e_QVAR_DATA_FINALIZED , -20022);

   e_Appl_Status_Pending         EXCEPTION;
   PRAGMA EXCEPTION_INIT( e_Appl_Status_Pending , -20023);  


-- Intialize Internal Stage1 Collection 
Procedure INITIALIZE_Stg1_collection IS
BEGIN
   Internal_curves := Internal_curves_T();
END INITIALIZE_Stg1_collection;


Procedure clean_up IS 
BEGIN

      /* This COB_DATE data is not finalized , so clean up  */
      PROCESS_LOG_REC.MESSAGE      := 'Clean up records from '
                                      ||'RISKDB.QP_INT_FWD_CURVES_RAW_DATA ,'
                                      ||vc_cobdate;
      vc_section := 'Clean up records from RISKDB.QP_INT_FWD_CURVES_RAW_DATA '||vc_cobdate;
      
--      ----dbms_output.Put_line(vc_section);
      
      /* This COB_DATE data is not finalized , so clean up RISKDB.QP_INT_FWD_CURVES_RAW_DATA  */
      vc_sql_stmt := 'DELETE FROM RISKDB.QP_INT_FWD_CURVES_RAW_DATA '||
                     ' WHERE COB_DATE = TO_DATE('||
                     vc_singlequote||vc_cobdate||vc_singlequote||', '||
                     vc_singlequote|| 'DD-MON-YYYY'||vc_singlequote||')'
                --adding location for testing  10/28/2015
---                || ' AND LOCATION = '|| vc_singlequote||'6JF16'||vc_singlequote 
;

      vn_rtn_code := NULL;
      vc_rtn_msg := '';
      
      dbms_output.Put_line('Delete Records');
      
     DELETE_RECORDS ( pDelete_statement => vc_sql_stmt 
                      ,pout_rtn_code => vn_rtn_code      
                      ,pout_rtn_msg => vc_rtn_msg  
                      );


     
     IF vn_rtn_code <> c_success THEN 
       RAISE e_appl_error;
     END IF;
     
    
     
     PROCESS_LOG_REC.STAGE        := 'INTERNAL_FWD_CURVES_LOAD';
     
     /* Truncate GTT tables   */
     
      vc_section    := 'Truncate  RISKDB.GTT_M2MGRANULAR_DATA ';
      PROCESS_LOG_REC.MESSAGE := vc_section ;
      
      write_log;
        
      vc_sql_stmt := 'Truncate table RISKDB.GTT_M2MGRANULAR_DATA';

      Execute Immediate vc_sql_stmt;
      
      vc_section    := 'Truncate  RISKDB.GTT_M2MDMR ';
      PROCESS_LOG_REC.MESSAGE := vc_section ;
      
      write_log;
        
      vc_sql_stmt := 'Truncate table RISKDB.GTT_M2MDMR';

      Execute Immediate vc_sql_stmt;


      vc_section    := 'Truncate  RISKDB.GTT_BASIS_CURVES ';
      PROCESS_LOG_REC.MESSAGE := vc_section ;
      
      write_log;
        
      vc_sql_stmt := 'Truncate table RISKDB.GTT_BASIS_CURVES';

      Execute Immediate vc_sql_stmt;
      
      vc_sql_stmt := 'Truncate table RISKDB.QP_INT_FWD_Curves_exceptions';
      
 --     ----dbms_output.PUT_LINE(vc_sql_stmt)  ;    
      
      

      Execute Immediate vc_sql_stmt;

       
     PROCESS_LOG_REC.MESSAGE      := 'Clean up records from RISKDB.QP_QVAR_RUN ';
     vc_section := 'Clean up records from RISKDB.QP_QVAR_RUN ';
     
     /* This COB_DATE data is not finalized ( Status = L ) , so clean up RISKDB.QP_QVAR_RUN  */
         
     vc_sql_stmt := 'DELETE FROM RISKDB.QP_QVAR_RUN '||
     'where COB_DATE = TO_DATE('||
     vc_singlequote||vc_cobdate||vc_singlequote||', '||vc_singlequote|| 'DD-MON-YYYY'||vc_singlequote||')'||
     ' AND RUN_STATUS NOT IN ( '||vc_singlequote||'L'||vc_singlequote||' , '||vc_singlequote||'R'||vc_singlequote||' ) ';

      vn_rtn_code := NULL;
      vc_rtn_msg := '';

      write_log;
      
      DELETE_RECORDS ( pDelete_statement => vc_sql_stmt 
                      ,pout_rtn_code => vn_rtn_code      
                      ,pout_rtn_msg => vc_rtn_msg  
                      );
      
END clean_up;


  
  BEGIN
  
  
 

    IF pcobdate is NULL THEN 

     Get_current_COBDATE (
      pcobdate =>   vd_cobdate  
     ,pout_rtn_code     => vn_rtn_code
     ,pout_rtn_msg      => vc_rtn_msg 
     );
     
     vc_cobdate       := to_CHAR( vd_cobdate, 'DD-MON-YYYY');
     vd_cobdate       := to_date( vc_cobdate, 'DD-MON-YYYY');
     
    ELSE


    vc_cobdate       := to_CHAR(pcobdate, 'DD-MON-YYYY');
    vd_cobdate       := to_date(vc_cobdate, 'DD-MON-YYYY');

    --vd_default_effective_Date := vd_cobdate;

    END IF;
    
 
     
--    vc_cobDate := to_CHAR(pCOBDATE, 'DD-MON-YYYY');
--    vd_cobDate := to_DATE(vc_cobDate, 'DD-MON-YYYY');
     
      --Set application Name for the Log Process
    DMR.PROCESS_LOG_PKG.CONSTRUCTOR ('QVAR_APP' );
    
     --Set Process Name and status for the Log Process                   
    PROCESS_LOG_REC.PROCESS_NAME := 'INTERNAL_FWD_CURVES_LOAD'; 
    PROCESS_LOG_REC.PARENT_NAME  := 'QP_ZEMA_EXTRACT_PKG';
    PROCESS_LOG_REC.STAGE        := 'INTERNAL_FWD_CURVES_LOAD';
    PROCESS_LOG_REC.STATUS       := PROCESS_LOG_PKG.C_STATUS_STARTED;
    PROCESS_LOG_REC.MESSAGE      := 'INTERNAL_FWD_CURVES_LOAD EXTRACT process Started with COBDATE='||vc_cobDate;
    
    DMR.PROCESS_LOG_PKG.SET_PROCESS_NAME(
    P_PROCESS_NAME =>PROCESS_LOG_REC.PROCESS_NAME
    , P_PARENT_NAME => PROCESS_LOG_REC.PARENT_NAME
    );
   

   -- Log Message first
   write_log;


  -- Generate Tenor Details
  -- Generate_Tenor_details;
  
    SELECT MIN(log_id)
    INTO g_minlogid
    FROM 
    DMR.PROCESS_LOG l
    WHERE
    application_Name = 'QVAR_APP'
    and Parent_Name = 'QP_ZEMA_EXTRACT_PKG'
    and process_Name = 'INTERNAL_FWD_CURVES_LOAD'
    and create_date between trunc(SYSDATE) - 1 and SYSDATE -- in last 2 days time frame
    --and STATUS = 'ERRORS'
    ;
    
  
      
    Vc_section := 'Check Qvar Load status for COBDATE...'||vc_cobdate; 
    PROCESS_LOG_REC.MESSAGE      := Vc_section ;   
 
   -- Check entry in QVAR  Run table
   --if its final return Code 'L'--> Locked and is Final 
   -- if COB_DATE already available Return 'P'--> available and Processing
   --                                      'I'--> INvalid  initial entry until VALID  
   --                                      'V'--> valid entry ( data available )
   -- if its not Available return code as 'N'--> NEW and needs to make new entry in table.
   -- QVAR Cycle goes through following cycle of status
   -- Invalid--> Processing-->Valid-->Locked
   --IF Process aborts before status turned to Valid the status would go back to Invalid
   
     ----dbms_output.Put_line('CHeck_Qvar_status_for_COBDATE '||vd_cobdate);
       
    CHeck_Qvar_status_for_COBDATE(
    PCOBDATE => vd_COBDATE
   , pQVARStaus => vc_qvarStatus
   , pQVARrunID => vn_qvarrun_id
   , ptimestamp => vd_timestamp
   , pout_rtn_code => vn_rtn_code
   , pout_rtn_msg => vc_rtn_msg
   );     
 

   PROCESS_LOG_REC.STAGE        := 'INTERNAL_FWD_CURVES_LOAD';
                          
   CASE
  
    WHEN vn_rtn_code <> c_success THEN
      RAISE e_appl_error;
  --  L => LOCKED 
    WHEN vn_rtn_code = c_success and ( vc_qvarStatus = 'L' OR vc_qvarStatus = 'R' )THEN
   
      vc_rtn_msg := 'QVAR Data is finalized/Reporcessing for COBDATE ='||vc_cobdate||' , Please Try for some Other COBDATE';
      ----dbms_output.Put_line(vc_rtn_msg);
      Raise e_QVAR_DATA_FINALIZED;
   
    WHEN vn_rtn_code = c_success and 
        ( ( vc_qvarStatus =  'P' ) OR 
           ( vc_qvarStatus =  'I' ) 
        )  THEN      
  
          vn_elapsed_hrs :=    ( (SYSDATE -  vd_timestamp )*24 );
         
      IF  vn_elapsed_hrs < 2 THEN 
          vc_rtn_msg := 'Process is Currently Running for a COBDATE ='||
                        vc_cobdate||' , Please Try AFter Sometime';
          ----dbms_output.Put_line(vc_rtn_msg);
          Raise e_Appl_Status_Pending;
       
      ELSE   
          clean_up; 
          vb_continue_processing := TRUE; --null; --continue through the end 
      END IF;
      
   
    ELSE
      --conrol comes here 
       
       -- in cases of 
        -- 'I --> Initiated and Terminated abruptly'
        --'V --> DAta is ready, re-run for same cobDATE scenario' 
        -- and 'N-> COBDATE not available in QVAR_RUN table'
      
      clean_up;
        
      
        
      ----dbms_output.Put_line('Calling insert cob date'||vd_cobdate);  
      
      PROCESS_LOG_REC.STAGE        := 'INTERNAL_FWD_CURVES_LOAD';
      PROCESS_LOG_REC.MESSAGE      := 'Insert_COB_DATE ';
     
      vn_rtn_code :=  NULL;
      vc_rtn_msg := NULL;
      
      write_log;
                               
       Insert_COBDATE(
        pCOBDATE => vd_COBDATE
        , pout_rtn_code => vn_rtn_code
        , pout_rtn_msg => vc_rtn_msg
       );

     PROCESS_LOG_REC.STAGE        := 'INTERNAL_FWD_CURVES_LOAD';
    
 --    ----dbms_output.Put_line('After cob date');
 
     IF vn_rtn_code <> c_success THEN 
        RAISE e_appl_error;
     END IF;

      vb_continue_processing := TRUE;

   END CASE;
   
   IF  vb_continue_processing THEN 
     
           CHeck_Qvar_status_for_COBDATE(
            PCOBDATE => vd_COBDATE
           , pQVARStaus => vc_qvarStatus
           , pQVARrunID => vn_qvarrun_id
           , ptimestamp => vd_timestamp
           , pout_rtn_code => vn_rtn_code
           , pout_rtn_msg => vc_rtn_msg
           );   
   
         IF vc_qvarStatus =  'I' THEN 
          -- chnage the QVAR status to P --> Processing 
          
          Update_QVAR_Status(
                 prunid         => vn_qvarrun_id
                , pstatus        => 'P'
                ,pout_rtn_code   => vn_rtn_code  
                ,pout_rtn_msg        => vc_rtn_msg
          );
          
         
         END IF ;
         

    --Initialize Stage 1 collection
     INITIALIZE_Stg1_collection;

      vn_rtn_code :=  NULL;
      vc_rtn_msg := NULL;
   
   
     PROCESS_LOG_REC.MESSAGE      := 'LOAD GTT COntext Variables';
     vc_section := 'LOAD GTT COntext Variables ';
          
      vn_rtn_code :=  NULL;
      vc_rtn_msg := NULL;
    
      write_log;
       
    LOAD_GTT_Session_Values ( 
     pcobdate           => vc_cobDate
    ,pout_rtn_code      => vn_rtn_code  
    ,pout_rtn_msg       => vc_rtn_msg 
     );
     
     IF vn_rtn_code <> c_success THEN 
       RAISE e_appl_error;
     END IF;
      
    PROCESS_LOG_REC.STAGE        := 'INTERNAL_FWD_CURVES_LOAD';
   
     vn_starttime := DBMS_UTILITY.GET_TIME ;
      
     PROCESS_LOG_REC.MESSAGE      := 'LOAD GTT_M2MDMR ';
     vc_section := 'Load GTT_M2MDMR ';
     
     write_log;
            
     vn_rtn_code :=  NULL;
     vc_rtn_msg := NULL;

    -- Change QVAR status to P 
    
         Update_QVAR_Status(
                prunid         => vn_qvarrun_id
                , pstatus        => 'P'
                ,pout_rtn_code   => vn_rtn_code  
                ,pout_rtn_msg        => vc_rtn_msg
          );
          


---  LOAD GTT m2m DMR Values      
    LOAD_GTT_M2MDMR(  
     pcobdate           => vc_cobDate
    ,pout_rtn_code      => vn_rtn_code  
    ,pout_rtn_msg       => vc_rtn_msg  
     );
     
     
     IF vn_rtn_code <> c_success THEN 
       RAISE e_appl_error;
     END IF;


     vc_sql_stmt := ' Select count(*) '||
      ' From  RISKDB.GTT_M2MDMR ' ; 
    
     Execute immediate vc_sql_stmt into vn_reccount;
     
     vn_endtime := DBMS_UTILITY.GET_TIME ;
    
     --dbms_output.Put_line('records in GTT m2mdmr='||vn_reccount);
     
     ----dbms_output.Put_line(To_CHAR((((vn_endtime - vn_starttime)/100)/60))||' Minutes');
   
     vn_starttime := vn_endtime;
      
     PROCESS_LOG_REC.MESSAGE      := 'LOAD GTT_Basis_Curves ';
     vc_section := ' LOAD GTT_Basis_Curves  ';
     
     write_log;
            
     vn_rtn_code :=  NULL;
     vc_rtn_msg := NULL;


---  LOAD GTT Basis Curves      
     LOAD_GTT_Basis_Curves(  
     pcobdate           => vc_cobDate
    ,pout_rtn_code      => vn_rtn_code  
    ,pout_rtn_msg       => vc_rtn_msg  
     );
     
 

     vc_sql_stmt := ' Select count(*) '||
      ' From  RISKDB.GTT_Basis_curves ' ; 
    
     Execute immediate vc_sql_stmt into vn_reccount;
     
     vn_endtime := DBMS_UTILITY.GET_TIME ;
         
     ----dbms_output.Put_line('records in GTT basis curves ='||vn_reccount);
     
     ----dbms_output.Put_line(To_CHAR((((vn_endtime - vn_starttime)/100)/60))||' Minutes');
          
     IF vn_rtn_code <> c_success THEN 
       RAISE e_appl_error;
     END IF;
          
     PROCESS_LOG_REC.STAGE        := 'INTERNAL_FWD_CURVES_LOAD';
     
     
       
     PROCESS_LOG_REC.MESSAGE      := 'Collect all Internal Curves from m2m view ';
     vc_section := 'Collect all Internal Curves from m2m view ';
          
     write_log;
       
     ----dbms_output.Put_line(vc_section);
            
     Collect_ALL_Internal_Curves(
     pcobDate => vc_cobdate
    , pInternal_curves => Internal_curves 
    , pout_rtn_code => vn_rtn_code 
    , pout_rtn_msg => vc_rtn_msg 
     );
    
    
    PROCESS_LOG_REC.STAGE        := 'INTERNAL_FWD_CURVES_LOAD';
     
     IF vn_rtn_code <> c_success THEN 
       --RAISE e_appl_error;
       null;
     END IF;
     
    PROCESS_LOG_REC.STAGE := ' Load_Internal_target';
    PROCESS_LOG_REC.MESSAGE      := 'Load_Internal_target ';
    vc_section := 'Load_Internal_target ';
    
    --dbms_output.Put_line(vc_section||Internal_curves.COUNT);
     
     Load_Internal_target (
                           pInternal_curves => Internal_curves 
                          , pout_rtn_code => vn_rtn_code 
                          , pout_rtn_msg => vc_rtn_msg 
                          );
    IF  vn_rtn_code = c_success THEN 
      COMMIT;
    ELSE
      COMMIT;
       Raise e_appl_error; 
    END IF;
                           
   END IF;
   
   ----dbms_output.Put_line('Loading collctn took'||To_CHAR((((vn_endtime - vn_starttime)/100)/60))||' Minutes');
   
       -- Change QVAR status to P 
    
         Update_QVAR_Status(
                prunid         => vn_qvarrun_id
                , pstatus        => 'V'
                ,pout_rtn_code   => vn_rtn_code  
                ,pout_rtn_msg        => vc_rtn_msg
          );
             
   
   PROCESS_LOG_REC.STAGE        := 'INTERNAL_FWD_CURVES_LOAD';
   PROCESS_LOG_REC.STATUS       := PROCESS_LOG_PKG.C_STATUS_COMPLETED;
   PROCESS_LOG_REC.MESSAGE      := 'LOAD GTT_M2MDMR ';
   vc_section := 'INTERNAL_FWD_CURVES_LOAD ';
     
   write_log;
  
   
    SELECT count(*) 
    INTO li_ERRORS
    FROM 
    DMR.PROCESS_LOG l
    WHERE
    application_Name = 'QVAR_APP'
    and Parent_Name = 'QP_ZEMA_EXTRACT_PKG'
    and process_Name = 'INTERNAL_FWD_CURVES_LOAD'
    and STATUS = 'ERRORS'
    and TIMESTAMP > vn_starttimeStamp
    and l.log_id > g_minlogid
   ;
    
     
   IF li_ERRORS > 0 THEN 
    
    PSTATUSCODE := 'SUCCESS';
    PSTATUSMSG := 'Process Completed with Errors, Please check Error Log DMR.PROCESS_LOG for details';
   
   ELSE
   
    PSTATUSCODE := 'SUCCESS';
    PSTATUSMSG := 'Process Completed Successfully';      
   
   END IF;
     
   --dbms_output.Put_line('Complete');
                            
  EXCEPTION
   WHEN e_Default_COBDATE_NotFound THEN 
    vn_rtn_code := c_failure;
    vc_rtn_msg := 'DEfault current COB Date Not Found'  ;
    
    DMR.PROCESS_LOG_PKG.WRITE_LOG(
            P_PROCESS_NAME => PROCESS_LOG_REC.PROCESS_NAME,
            P_STAGE=> PROCESS_LOG_REC.STAGE,
            P_STATUS=>PROCESS_LOG_PKG.C_STATUS_ERRORS,
            P_MESSAGE=>NVL( vc_rtn_msg, SQLERRM) 
            );
  
    PSTATUSCODE := 'ERRORS';
    PSTATUSMSG := 'Process Completed with Errors, Please check Error Log DMR.PROCESS_LOG for details';
 
         Update_QVAR_Status(
                prunid         => vn_qvarrun_id
                , pstatus        => 'V'
                ,pout_rtn_code   => vn_rtn_code  
                ,pout_rtn_msg        => vc_rtn_msg
          );
                
  WHEN    e_Appl_Status_Pending THEN 

    DMR.PROCESS_LOG_PKG.WRITE_LOG(
            P_PROCESS_NAME => PROCESS_LOG_REC.PROCESS_NAME,
            P_STAGE=> PROCESS_LOG_REC.STAGE,
            P_STATUS=>PROCESS_LOG_PKG.C_STATUS_INFO,
            P_MESSAGE=>NVL( vc_rtn_msg, SQLERRM) 
            );  

    PSTATUSCODE := 'ERRORS';
    PSTATUSMSG := 'Process Completed with Errors, Please check Error Log DMR.PROCESS_LOG for details';


         Update_QVAR_Status(
                prunid         => vn_qvarrun_id
                , pstatus        => 'V'
                ,pout_rtn_code   => vn_rtn_code  
                ,pout_rtn_msg        => vc_rtn_msg
          );
          
  WHEN e_appl_error THEN 
    
    DMR.PROCESS_LOG_PKG.WRITE_LOG(
                        P_PROCESS_NAME => PROCESS_LOG_REC.PROCESS_NAME,
                        P_STAGE=> PROCESS_LOG_REC.STAGE,
                        P_STATUS=>PROCESS_LOG_PKG.C_STATUS_ERRORS,
                        P_MESSAGE=>NVL( vc_rtn_msg, SQLERRM) 
                        );          

    PSTATUSCODE := 'ERRORS';
    PSTATUSMSG := 'Process Completed with Errors, Please check Error Log DMR.PROCESS_LOG for details';


         Update_QVAR_Status(
                prunid         => vn_qvarrun_id
                , pstatus        => 'V'
                ,pout_rtn_code   => vn_rtn_code  
                ,pout_rtn_msg        => vc_rtn_msg
          );
          
  WHEN OTHERS THEN 
      
        DMR.PROCESS_LOG_PKG.WRITE_LOG(
            P_PROCESS_NAME => PROCESS_LOG_REC.PROCESS_NAME,
            P_STAGE=> PROCESS_LOG_REC.STAGE,
            P_STATUS=>PROCESS_LOG_PKG.C_STATUS_ERRORS,
            P_MESSAGE=>NVL( vc_rtn_msg, SQLERRM) 
            );


    PSTATUSCODE := 'ERRORS';
    PSTATUSMSG := 'Process Completed with Errors, Please check Error Log DMR.PROCESS_LOG for details';


          Update_QVAR_Status(
                prunid         => vn_qvarrun_id
                , pstatus        => 'V'
                ,pout_rtn_code   => vn_rtn_code  
                ,pout_rtn_msg        => vc_rtn_msg
          );
   
    
            
  END    INTERNAL_FWD_CURVES_LOAD;




PROCEDURE Derive_StripLevel_Measures (
pindex                  IN NUMBER
, POriginalLocation     IN VARCHAR2
, POverrideLocation     IN VARCHAR2 
, pbackbone_flag        IN VARCHAR2
, pOverridebb_flag      IN VARCHAR2
, pCurrentMonth         IN NUMBER
, pstartYearMonth       IN NUMBER
, PEndYearMOnth         IN NUMBER
, pm2mData              IN OUT NOCOPY m2m_by_Loc_tab
, pcollection           IN OUT NOCOPY strips_collectn_t
, pcalcType             IN VARCHAR2
, pout_rtn_code         OUT  NOCOPY NUMBER    
, pout_rtn_msg          OUT  NOCOPY VARCHAR2 
) 
IS
vc_Calc_Type                VARCHAR2(100);
vn_loc_m2m                  NUMBER;

vc_stripTenor               VARCHAR2(1000);  

vc_section                  VARCHAR2(1000);

vn_distinctFAS_COunt        NUMBER; 
vn_contractYear_Month       NUMBER;         

vn_tenor_months             NUMBER;
vn_actual_tenor_months      NUMBER;
vc_flat_position            varchar2(1);

vc_deno_hours               NUMBER;
vc_deno_days                NUMBER;


vn_price_condition         RISKDB.QP_FAS_PRICE_OVERRIDE.STRIP_PRICE_CONDITION%TYPE;
vn_price_threshold          RISKDB.QP_FAS_PRICE_OVERRIDE.PRICE_DELTA_THRESHOLD%TYPE;

vn_FASLevel                NUMBER;
vn_FASValue                NUMBER;
vn_OverrideFASLevel        NUMBER;
vn_OverrideFASValue        NUMBER;
vn_OBP_FASLevel            NUMBER; -- overriding basis Points FAS level                    
vn_rtn_code                    NUMBER;
vc_rtn_msg                     VARCHAR2(500);

vc_overridecom           VARCHAR2(1000);

  
  
BEGIN

 PROCESS_LOG_REC.STAGE        := 'DERIVE_STRIPLEVEL_MEASURES';
 
-- dbms_output.PUT_LINE(pstartYearMonth||'-'||PEndYearMOnth||'-'||pcalcType);

---Calculate total aggregate m2m @ LOcation Level
--SELECT 
--SUM(m2m_value) INTO vn_loc_m2m
--FROM
--TABLE(pm2mData) m2m
--where
--m2m.Underlying_commodity =     pcollection(pIndex).UNDERLYING_COMMODITY 
--and m2m.price_vol_indicator =  pcollection(pIndex).PRICE_VOL_INDICATOR 
--and m2m.COMMODITY = pcollection(pIndex).NEER_COMMODITY 
--and to_number(m2m.CONTRACTYEAR_MONTH)  > pCurrentMonth
--;

CASE

WHEN pcalcType = 'LOCATION_LEVEL_M2M' THEN 

vc_section := 'LOCATION_LEVEL_M2M';

-- sum at tenor level 
SELECT 
m.STRIP_TENOR
, SUM(m.m2m_value)
, SUM (m.ABSOLUTE_M2M_VALUE )
, sum(m.LEGGED_M2M_VALUE)
, SUM (m.ABSOLUTE_LEGGED_M2M_VALUE)
, SUM(m.LOCATION_DELTA_POSITION)
, SUM(m.COMMODITY_ABSOLUTE_POSITION)
, SUM( m.MONTHLY_VOL ) MONTHLY_VOL                  
, SUM(m.ABSOLUTE_M2M_VALUE_VOL)
, SUM(m.ABSOLUTE_LEGGED_M2M_VALUE_VOL)
INTO 
vc_stripTenor
, pcollection(pIndex).M2M_VALUE
, pcollection(pIndex).ABSOLUTE_M2M_VALUE
, pcollection(pIndex).LEGGED_M2M_VALUE
, pcollection(pIndex).ABSOLUTE_LEGGED_M2M_VALUE 
, pcollection(pIndex).LOCATION_DELTA_POSITION
, pcollection(pIndex).COMMODITY_ABS_POSITION
, pcollection(pIndex).MOnthly_vol
, pcollection(pIndex).ABS_M2M_AMT_VOL
, pcollection(pIndex).ABS_LEGGED_M2M_VOL
FROM
( SELECT DISTINCT
m2m.LOCATION
, pCollection(pIndex).STRIP_TENOR strip_tenor
, m2m.contractYear_month
, m2m.m2m_value
, m2m.ABSOLUTE_M2M_VALUE 
, m2m.LEGGED_M2M_VALUE
, m2m.ABSOLUTE_LEGGED_M2M_VALUE
, m2m.LOCATION_DELTA_POSITION
, m2m.COMMODITY_ABSOLUTE_POSITION
, m2m.MONTHLY_VOL  MONTHLY_VOL                  
, m2m.ABSOLUTE_M2M_VALUE_VOL
, m2m.ABSOLUTE_LEGGED_M2M_VALUE_VOL
FROM
TABLE(pm2mData) m2m --PMONTHLY_M2M
--RISKDB.QP_INT_FWD_Curves_raw_data m2m
where
--partition_bit = 2 
 cob_date = pcollection(pIndex).COB_DATE 
and m2m.Underlying_commodity =  pcollection(pIndex).UNDERLYING_COMMODITY 
and m2m.price_vol_indicator  =  pcollection(pIndex).PRICE_VOL_INDICATOR 
and m2m.LOCATION             =  POriginalLocation  --pcollection(pIndex).NEER_LOCATION
and m2m.NETTING_GROUP_ID     = pcollection(pIndex).NETTING_GROUP_ID
--and to_number(m2m.CONTRACTYEAR_MONTH)  > pCurrentMonth
--and m2m.CONTRACT_YEAR IN (  TO_NUMBER(SUBSTR(vC_StartCYYYYMM,1,4) , to_NUMBER(vC_ENDCYYYYMM,1,4) )   -- for current Year ONLY
and to_number(m2m.contractYear_month) Between  pstartYearMonth 
                                         AND   PEndYearMOnth
) m
GROUP BY 
m.STRIP_TENOR
;


--Dbms_output.Put_line()

WHEN pcalcType = 'LOCATION_PRICE' THEN 

vc_section := 'LOCATION_PRICE';


CASE 
WHEN  pcollection(pIndex).PRICE_VOL_INDICATOR  = 'PRICE' AND POverrideLocation IS NOT NULL THEN

-- get Overridden Prices from Projection Curves for PRICE Curve

SELECT 
SUM(m.Override_price)  INTO pcollection(pIndex).Proj_Loc_amt
FROM 
(
 SELECT DISTINCT
     m2m.cob_date
    , m2m.OVERRIDE_BASISPOINT 
    ,  m2m.contractYear_month
    ,( CASE 
      WHEN  pcollection(pIndex).UNDERLYING_COMMODITY = 'ELECTRICITY' THEN
         m2m.OVERRIDE_PRICE * m2m.OVERRIDE_HOURS
      WHEN pcollection(pIndex).UNDERLYING_COMMODITY IN ( 'NATURAL GAS','GAS', 'NG 6 MOAVG') THEN 
          m2m.OVERRIDE_PRICE * m2m.CAL_DAYS_IN_CONTRACT_MONTH
      WHEN pcollection(pIndex).UNDERLYING_COMMODITY = 'OIL' THEN
          m2m.OVERRIDE_PRICE 
      WHEN pcollection(pIndex).UNDERLYING_COMMODITY = 'CAPACITY' THEN    
          m2m.OVERRIDE_PRICE
      WHEN pcollection(pIndex).UNDERLYING_COMMODITY = 'EMISSIONS' THEN
          m2m.OVERRIDE_PRICE
      else
       0 
     END 
    ) OVERRIDE_PRICE
    FROM
    TABLE(pm2mData) m2m --PMONTHLY_M2M
    --RISKDB.QP_INT_FWD_Curves_raw_data m2m
    where
    --partition_bit = 2 
     cob_date = pcollection(pIndex).COB_DATE 
    and m2m.Underlying_commodity =  pcollection(pIndex).UNDERLYING_COMMODITY 
    and m2m.price_vol_indicator =   pcollection(pIndex).PRICE_VOL_INDICATOR 
    and m2m.LOCATION =             POriginalLocation  
    and m2m.NETTING_GROUP_ID = pcollection(pIndex).NETTING_GROUP_ID
--    and to_number(m2m.CONTRACTYEAR_MONTH)  > pCurrentMonth
    and to_number(m2m.contractYear_month) Between  pstartYearMonth 
                                             AND   PEndYearMOnth
) m 
;





WHEN  pcollection(pIndex).PRICE_VOL_INDICATOR  = 'VOL' AND POverrideLocation IS NOT NULL THEN
-- get Overridden Prices from Projection Curves for VOL Curve
SELECT 
SUM(m.Override_price)  INTO pcollection(pIndex).Proj_Loc_amt
FROM 
(
 SELECT DISTINCT
     m2m.cob_date
    , m2m.OVERRIDE_BASISPOINT 
    ,  m2m.contractYear_month
    ,( CASE 
      WHEN  pcollection(pIndex).UNDERLYING_COMMODITY = 'ELECTRICITY' THEN
         m2m.OVERRIDE_PRICE * m2m.OVERRIDE_HOURS
      WHEN pcollection(pIndex).UNDERLYING_COMMODITY IN ( 'NATURAL GAS','GAS', 'NG 6 MOAVG') THEN 
          m2m.OVERRIDE_PRICE * m2m.CAL_DAYS_IN_CONTRACT_MONTH
      WHEN pcollection(pIndex).UNDERLYING_COMMODITY = 'OIL' THEN
          m2m.OVERRIDE_PRICE 
      WHEN pcollection(pIndex).UNDERLYING_COMMODITY = 'CAPACITY' THEN    
          m2m.OVERRIDE_PRICE
      WHEN pcollection(pIndex).UNDERLYING_COMMODITY = 'EMISSIONS' THEN
          m2m.OVERRIDE_PRICE
      else
       0 
     END 
    ) OVERRIDE_PRICE
    FROM
    TABLE(pm2mData) m2m --PMONTHLY_M2M
    --RISKDB.QP_INT_FWD_Curves_raw_data m2m
    where
    --partition_bit = 2 
     cob_date = pcollection(pIndex).COB_DATE 
    and m2m.Underlying_commodity =  pcollection(pIndex).UNDERLYING_COMMODITY 
    and m2m.price_vol_indicator =   pcollection(pIndex).PRICE_VOL_INDICATOR 
    and m2m.LOCATION =             POriginalLocation  
    and m2m.NETTING_GROUP_ID = pcollection(pIndex).NETTING_GROUP_ID
--    and to_number(m2m.CONTRACTYEAR_MONTH)  > pCurrentMonth
    and to_number(m2m.contractYear_month) Between  pstartYearMonth 
                                             AND   PEndYearMOnth
) m 
;


ELSE
-- get Regular Prices from Projection Curves for PRICE Curve

-- sum at tenor level 
SELECT 
SUM ( m.Proj_Loc_amt) 
INTO 
pcollection(pIndex).Proj_Loc_amt
FROM
( SELECT DISTINCT
 pCollection(pIndex).STRIP_TENOR strip_tenor
,  m2m.contractYear_month
,( CASE 
  WHEN  pcollection(pIndex).UNDERLYING_COMMODITY = 'ELECTRICITY' THEN
    m2m.internal_Price *  m2m.TOT_HOURS_PER_CONTRACT_MONTH
  WHEN pcollection(pIndex).UNDERLYING_COMMODITY IN ( 'NATURAL GAS','GAS', 'NG 6 MOAVG') THEN 
      m2m.Internal_price *   m2m.CAL_DAYS_IN_CONTRACT_MONTH
  WHEN pcollection(pIndex).UNDERLYING_COMMODITY = 'OIL' THEN
      m2m.Internal_price 
  WHEN pcollection(pIndex).UNDERLYING_COMMODITY = 'CAPACITY' THEN    
      m2m.Internal_price
  WHEN pcollection(pIndex).UNDERLYING_COMMODITY = 'EMISSIONS' THEN
      m2m.Internal_price
  else
   0 
 END ) Proj_Loc_amt
FROM
TABLE(pm2mData) m2m --PMONTHLY_M2M
--RISKDB.QP_INT_FWD_Curves_raw_data m2m
where
--partition_bit = 2 
 cob_date = pcollection(pIndex).COB_DATE 
and m2m.Underlying_commodity =  pcollection(pIndex).UNDERLYING_COMMODITY 
and m2m.price_vol_indicator =   pcollection(pIndex).PRICE_VOL_INDICATOR 
and m2m.LOCATION =             POriginalLocation  
and m2m.NETTING_GROUP_ID = pcollection(pIndex).NETTING_GROUP_ID
--and to_number(m2m.CONTRACTYEAR_MONTH)  > pCurrentMonth
and to_number(m2m.contractYear_month) Between  pstartYearMonth 
                                         AND   PEndYearMOnth
) m
GROUP BY 
m.STRIP_TENOR
;

END CASE;
                                        


-- sum of Hours and days at tenor level 
SELECT 
SUM(
m.TOT_HOURS_PER_CONTRACT_MONTH
) TOT_HOURS_PER_CONTRACT_MONTH
, SUM(m.CAL_DAYS_IN_CONTRACT_MONTH)
INTO 
 pcollection(pIndex).TOTAL_HOURS_PER_STRIP
, pcollection(pIndex).CALENDAR_DAYS_in_STRIP
FROM
( SELECT DISTINCT
  m2m.contractYear_month
, ( CASE 
  WHEN pOverrideLocation IS NULL THEN m2m.TOT_HOURS_PER_CONTRACT_MONTH
  WHEN pOverrideLocation IS NOT NULL THEN m2m.OVERRIDE_HOURS
  END
 ) TOT_HOURS_PER_CONTRACT_MONTH 
, m2m.CAL_DAYS_IN_CONTRACT_MONTH
FROM
TABLE(pm2mData) m2m --PMONTHLY_M2M
--RISKDB.QP_INT_FWD_Curves_raw_data m2m
where
cob_date = pcollection(pIndex).COB_DATE 
and m2m.Underlying_commodity =  pcollection(pIndex).UNDERLYING_COMMODITY 
and m2m.price_vol_indicator =   pcollection(pIndex).PRICE_VOL_INDICATOR 
and m2m.LOCATION =             POriginalLocation  
and m2m.NETTING_GROUP_ID = pcollection(pIndex).NETTING_GROUP_ID
--and to_number(m2m.CONTRACTYEAR_MONTH)  > pCurrentMonth
and to_number(m2m.contractYear_month) Between  pstartYearMonth 
                                         AND   PEndYearMOnth
) m
;


WHEN pcalcType = 'COMMODITY_LEVEL_SUM' THEN 


vc_section := 'COMMODITY_LEVEL_SUM';

-- sum at tenor level 

BEGIN 

SELECT 
m.STRIP_TENOR
, SUM(m.m2m_value)
, SUM (m.ABSOLUTE_M2M_VALUE )
, sum(m.LEGGED_M2M_VALUE)
, SUM (m.ABSOLUTE_LEGGED_M2M_VALUE)
, SUM(m.LOCATION_DELTA_POSITION)
, SUM(m.COMMODITY_ABSOLUTE_POSITION)
, SUM( m.MONTHLY_VOL ) MONTHLY_VOL                  
, SUM(m.ABSOLUTE_M2M_VALUE_VOL)
, SUM(m.ABSOLUTE_LEGGED_M2M_VALUE_VOL)
INTO 
vc_stripTenor
, pcollection(pIndex).M2M_VALUE
, pcollection(pIndex).ABSOLUTE_M2M_VALUE
, pcollection(pIndex).LEGGED_M2M_VALUE
, pcollection(pIndex).ABSOLUTE_LEGGED_M2M_VALUE 
, pcollection(pIndex).LOCATION_DELTA_POSITION
, pcollection(pIndex).COMMODITY_ABS_POSITION
, pcollection(pIndex).MOnthly_vol
, pcollection(pIndex).ABS_M2M_AMT_VOL
, pcollection(pIndex).ABS_LEGGED_M2M_VOL
FROM
( SELECT DISTINCT
m2m.LOCATION
, pCollection(pIndex).STRIP_TENOR strip_tenor
, m2m.contractYear_month
, m2m.m2m_value
, m2m.ABSOLUTE_M2M_VALUE 
, m2m.LEGGED_M2M_VALUE
, m2m.ABSOLUTE_LEGGED_M2M_VALUE
, m2m.LOCATION_DELTA_POSITION
, m2m.COMMODITY_ABSOLUTE_POSITION
, m2m.MONTHLY_VOL  MONTHLY_VOL                  
, m2m.ABSOLUTE_M2M_VALUE_VOL
, m2m.ABSOLUTE_LEGGED_M2M_VALUE_VOL
FROM
TABLE(pm2mData) m2m --PMONTHLY_M2M
--RISKDB.QP_INT_FWD_Curves_raw_data m2m
where
--partition_bit = 2 
 cob_date = pcollection(pIndex).COB_DATE 
and m2m.Underlying_commodity =  pcollection(pIndex).UNDERLYING_COMMODITY 
and m2m.price_vol_indicator  =  pcollection(pIndex).PRICE_VOL_INDICATOR 
and m2m.COMMODITY             =  pcollection(pIndex).NEER_COMMODITY
and m2m.NETTING_GROUP_ID     = pcollection(pIndex).NETTING_GROUP_ID
--and to_number(m2m.CONTRACTYEAR_MONTH)  > pCurrentMonth
--and m2m.CONTRACT_YEAR IN (  TO_NUMBER(SUBSTR(vC_StartCYYYYMM,1,4) , to_NUMBER(vC_ENDCYYYYMM,1,4) )   -- for current Year ONLY
and to_number(m2m.contractYear_month) Between  pstartYearMonth 
                                         AND   PEndYearMOnth
) m
GROUP BY 
m.STRIP_TENOR
;

EXCEPTION
WHEN NO_DATA_FOUND THEN 
  null; -- do nothing 
END;


WHEN pcalcType = 'TENOR_LEVEL_FAS' THEN 


vc_section := 'TENOR_LEVEL_FAS';


--  Devide m2m into dfferent segments based on DISTINCT FAS Level groups
-- for each FAS group compute sum legged m2m
-- group with highest m2m value will be picked up as current FAS
-- incase when all FAS have same value then aa Lowest FAS is picked up as FINAL FAS level 

IF  pOverrideLocation IS NULL THEN 


-- dbms_output.put_line (
--  'com='||pcollection(pIndex).NEER_COMMODITY 
--  ||'Bp='||pcollection(pIndex).NEER_LOCATION
--  ||'NG='||pcollection(pIndex).NETTING_GROUP_ID
--  ||'month='||pstartYearMonth ||'  AND '|| PEndYearMOnth
-- );



    For FASrecs IN 
    (
    WITH 
    distinctFAS as (
        Select DISTINCT
            m.original_FAS157_Level
        FROM
        ( 
           SELECT DISTINCT
              m2m.contractYear_month
            , m2m.original_FAS157_Level
            , m2m.LEGGED_M2M_VALUE
            FROM
            TABLE(pm2mData) m2m --PMONTHLY_M2M
           where
--        partition_bit = 2 
            cob_date = pcollection(pIndex).COB_DATE 
            and m2m.NETTING_GROUP_ID     = pcollection(pIndex).NETTING_GROUP_ID
            and Underlying_Commodity = pcollection(pIndex).UNDERLYING_COMMODITY
            and PRICE_VOL_INDICATOR = pcollection(pIndex).PRICE_VOL_INDICATOR
            and COMMODITY = pcollection(pIndex).NEER_COMMODITY 
            and m2m.LOCATION = POriginalLocation
            --and to_number(m2m.CONTRACTYEAR_MONTH)  > pCurrentMonth
            and to_number(m2m.contractYear_month) Between  pstartYearMonth 
                                                     AND   PEndYearMOnth
        ) m
      ) 
      , m2mSet AS ( 
        Select DISTINCT
        m.original_FAS157_Level 
        , sum(ABS(m.LEGGED_M2M_VALUE)) Over ( partition by m.original_FAS157_Level ) LEGGED_M2M_VALUE
        FROM
        ( 
           SELECT DISTINCT
              m2m.contractYear_month
            , m2m.original_FAS157_Level
            , m2m.LEGGED_M2M_VALUE
            FROM
            TABLE(pm2mData) m2m --PMONTHLY_M2M
           where
--        partition_bit = 2 
            cob_date = pcollection(pIndex).COB_DATE 
            and m2m.NETTING_GROUP_ID     = pcollection(pIndex).NETTING_GROUP_ID
            and Underlying_Commodity = pcollection(pIndex).UNDERLYING_COMMODITY
            and PRICE_VOL_INDICATOR = pcollection(pIndex).PRICE_VOL_INDICATOR
            and COMMODITY = pcollection(pIndex).NEER_COMMODITY 
            and m2m.LOCATION = POriginalLocation 
            --and to_number(m2m.CONTRACTYEAR_MONTH)  > pCurrentMonth
            and to_number(m2m.contractYear_month) Between  pstartYearMonth 
                                                     AND   PEndYearMOnth
        ) m
      )
        Select 
         m2mset.original_FAS157_Level
         , m2mset.LEGGED_M2M_VALUE
        FROM 
        m2mSet
        , distinctFAS
        where
        m2mSet.original_FAS157_Level = distinctFAS.original_FAS157_Level
        order by m2mset.LEGGED_M2M_VALUE desc NULLS LAST, m2mset.original_FAS157_Level asc
       ) 
    LOOP
    
    pcollection(pIndex).original_FAS157_Level :=  FASRecs.original_FAS157_Level;
    
--    dbms_output.put_line ( 'Tenor fas original_FAS157_Level='
--        ||FASRecs.original_FAS157_Level 
--     || ' m2m ='||FASRecs.LEGGED_M2M_VALUE );
     
    SELECT 
         m2m.ORIGINAL_FAS157_PERCENT 
    INTO 
         pcollection(pIndex).ORIGINAL_FAS157_PERCENT
    from 
    TABLE(pm2mData) m2m
        --RISKDB.QP_INT_FWD_Curves_raw_data m2m
    where
        cob_date = pcollection(pIndex).COB_DATE
        and m2m.NETTING_GROUP_ID     = pcollection(pIndex).NETTING_GROUP_ID
        and m2m.Underlying_commodity = pcollection(pIndex).UNDERLYING_COMMODITY 
        and m2m.price_vol_indicator =  pcollection(pIndex).PRICE_VOL_INDICATOR 
        and m2m.COMMODITY = pcollection(pIndex).NEER_COMMODITY
        and m2m.LOCATION =  pcollection(pIndex).NEER_LOCATION
        --and to_number(m2m.CONTRACTYEAR_MONTH)  > pCurrentMonth
        and to_number(m2m.contractYear_month)Between  pstartYearMonth 
                                           AND        PEndYearMOnth
        and m2m.original_FAS157_Level = FASRecs.original_FAS157_Level
        and rownum < 2;                                        
        
        
        EXIT; --exit the Loop once we have original FAS
        
    END LOOP;
    
            /*******************************************/
           /*   Check for FAS PRICE Threshold         */
           /******************************************/        
       BEGIN
           
        Select 
           ( CASE 
            WHEN  ABS(pcollection(pIndex).Proj_loc_amt) <  fas_ovr.STRIP_PRICE_CONDITION 
                                     THEN fas_ovr.PRICE_DELTA_THRESHOLD
            ELSE
            -- get whatever was available @ FAS Threshold override level Becomes FINAL FAS value
             to_NUmber( pcollection(pIndex).ORIGINAL_FAS157_PERCENT)  
            END
            ) Price_Threshold
            ,( CASE 
                WHEN  ABS(pcollection(pIndex).Proj_loc_amt) <  fas_ovr.STRIP_PRICE_CONDITION THEN  
                'Y'
              ELSE
               'N'
              END
             ) FAS157_Override_Indicator
          ,( CASE 
                WHEN  ABS(pcollection(pIndex).Proj_loc_amt) <  fas_ovr.STRIP_PRICE_CONDITION THEN  'Price Override'
              ELSE
               'Percent Value'
              END
           ) FINAL_FAS157_TYPE        
        INTO   
              pcollection(pIndex).Final_FAS157_Value
            , pcollection(pIndex).FAS157_Override_Indicator
            , pcollection(pIndex).FINAL_FAS157_TYPE
        FROM 
            RISKDB.QP_FAS_PRICE_OVERRIDE fas_ovr
        where
            UNDERLYING_COMMODITY = pcollection(pIndex).Underlying_Commodity
            and PRICE_VOL_INDICATOR = pcollection(pIndex).PRICE_VOL_Indicator
            and pcollection(pIndex).COB_DATE Between  EFFECTIVE_START_DATE and NVL( EFFECTIVE_END_DATE, pcollection(pIndex).COB_DATE)
            and active_flag = 'Y'
            ;
     
    
        
        -- If PRICE FOUND but < PRICE threshold then go far FAS Override check        
         IF pcollection(pIndex).FINAL_FAS157_TYPE <> 'Price Override' THEN 
           --Dbms_output.Put_Line ( 'raising nodatafound to start tol override ');
           Raise NO_DATA_FOUND;
         END IF;
         
            
        EXCEPTION    
        WHEN NO_DATA_FOUND THEN 
        
         -- Check do we have a FAS level Overridden ???
         
             
            /*******************************************/
           /*   Check for FAS LEVEL OVERIDDEN        */
           /******************************************/      
           
             BEGIN
             
               SELECT
                OVERRIDE_FAS_PERCENT_VALUE
                , 'Y'
                , 'Percent Value' Override_Type
               INTO   
                  pcollection(pIndex).Final_FAS157_Value
                , pcollection(pIndex).FAS157_Override_Indicator
                , pcollection(pIndex).FINAL_FAS157_TYPE
               FROM 
               RISKDB.QP_FAS_TOLERANCE_OVERRIDE overfas_tol
                where
                PRICE_VOL_IND = pcollection(pIndex).PRICE_VOL_INDICATOR
                and COmmodity = pcollection(pIndex).NEER_COMMODITY 
                and Basis_point = pcollection(pIndex).NEER_LOCATION 
                and FAS_Level = pcollection(pIndex).original_FAS157_Level  
                and pcollection(pIndex).COB_DATE Between  EFFECTIVE_START_DATE and NVL( EFFECTIVE_END_DATE
                                                           , pcollection(pIndex).COB_DATE)
                and active_flag = 'Y'    
                ;
                
                
 
 


             EXCEPTION
             WHEN NO_DATA_FOUND THEN 
             
--              dbms_output.put_line ( ' tol over not found ='
--              ||'com='||pcollection(pIndex).NEER_COMMODITY 
--              ||'Bp='||pcollection(pIndex).NEER_LOCATION
--              ||'NG='||pcollection(pIndex).NETTING_GROUP_ID
--              ||'month='||pstartYearMonth ||'  AND '|| PEndYearMOnth
--              ||' final='||pcollection(pIndex).Final_FAS157_Value ||' type= '||pcollection(pIndex).FINAL_FAS157_TYPE);
              
                pcollection(pIndex).Final_FAS157_Value := pcollection(pIndex).ORIGINAL_FAS157_PERCENT ;
                pcollection(pIndex).FAS157_Override_Indicator := 'N';
                pcollection(pIndex).FINAL_FAS157_TYPE := 'Percent Value';
             
 
             WHEN OTHERS THEN 
             
              dbms_output.put_line ( 'OTHERS EXCP tol over not found ='
              ||'com='||pcollection(pIndex).NEER_COMMODITY 
              ||'Bp='||pcollection(pIndex).NEER_LOCATION
              ||'NG='||pcollection(pIndex).NETTING_GROUP_ID
              ||'month='||pstartYearMonth ||'  AND '|| PEndYearMOnth
              ||' final='||pcollection(pIndex).Final_FAS157_Value ||' type= '||pcollection(pIndex).FINAL_FAS157_TYPE);
              
                pcollection(pIndex).Final_FAS157_Value := pcollection(pIndex).ORIGINAL_FAS157_PERCENT ;
                pcollection(pIndex).FAS157_Override_Indicator := 'N';
                pcollection(pIndex).FINAL_FAS157_TYPE := 'Percent Value';

             END;
            
        
        END;
       -- Main Block    
 
END IF ;
-- end of override Location  NULL check

IF  pOverrideLocation IS NOT NULL THEN 


  SELECT  DISTINCT 
   OVERRIDE_COMMODITY INTO vc_overridecom
  FROM
    TABLE(pm2mData) m2m --PMONTHLY_M2M
   where
    PRICE_VOL_INDICATOR = pcollection(pIndex).PRICE_VOL_INDICATOR
    and m2m.OVERRIDE_BASISPOINT = POverrideLocation
    and to_number(m2m.contractYear_month) Between  pstartYearMonth 
                                             AND   PEndYearMOnth
    and rownum < 2;
  

    For FASrecs IN 
    (
    WITH 
    distinctFAS as (
        Select DISTINCT
            m.original_FAS157_Level
        FROM
        ( 
           SELECT DISTINCT
              m2m.contractYear_month
            , m2m.original_FAS157_Level
            , m2m.LEGGED_M2M_VALUE
            FROM
            TABLE(pm2mData) m2m --PMONTHLY_M2M
           where
--        partition_bit = 2 
            cob_date = pcollection(pIndex).COB_DATE 
            and m2m.NETTING_GROUP_ID     = pcollection(pIndex).NETTING_GROUP_ID
            and Underlying_Commodity = pcollection(pIndex).UNDERLYING_COMMODITY
            and PRICE_VOL_INDICATOR =  pcollection(pIndex).PRICE_VOL_INDICATOR
            and OVERRIDE_COMMODITY =   vc_overridecom
            and m2m.OVERRIDE_BASISPOINT = POverrideLocation
            --and to_number(m2m.CONTRACTYEAR_MONTH)  > pCurrentMonth
            and to_number(m2m.contractYear_month) Between  pstartYearMonth 
                                                     AND   PEndYearMOnth
        ) m
      ) 
      , m2mSet AS ( 
        Select DISTINCT
        m.original_FAS157_Level 
        , sum(ABS(m.LEGGED_M2M_VALUE)) Over ( partition by m.original_FAS157_Level ) LEGGED_M2M_VALUE
        FROM
        ( 
           SELECT DISTINCT
              m2m.contractYear_month
            , m2m.original_FAS157_Level
            , m2m.LEGGED_M2M_VALUE
            FROM
            TABLE(pm2mData) m2m --PMONTHLY_M2M
           where
--        partition_bit = 2 
            cob_date = pcollection(pIndex).COB_DATE 
            and m2m.NETTING_GROUP_ID     = pcollection(pIndex).NETTING_GROUP_ID
            and Underlying_Commodity = pcollection(pIndex).UNDERLYING_COMMODITY
            and PRICE_VOL_INDICATOR = pcollection(pIndex).PRICE_VOL_INDICATOR
            and OVERRIDE_COMMODITY =   vc_overridecom
            and m2m.OVERRIDE_BASISPOINT = POverrideLocation 
            --and to_number(m2m.CONTRACTYEAR_MONTH)  > pCurrentMonth
            and to_number(m2m.contractYear_month) Between  pstartYearMonth 
                                                     AND   PEndYearMOnth
        ) m
      )
        Select 
         m2mset.original_FAS157_Level
         , m2mset.LEGGED_M2M_VALUE
        FROM 
        m2mSet
        , distinctFAS
        where
        m2mSet.original_FAS157_Level = distinctFAS.original_FAS157_Level
        order by m2mset.LEGGED_M2M_VALUE desc NULLS LAST, m2mset.original_FAS157_Level asc
       ) 
    LOOP
    
    pcollection(pIndex).original_FAS157_Level :=  FASRecs.original_FAS157_Level;
    
    SELECT 
         m2m.ORIGINAL_FAS157_PERCENT 
    INTO 
         pcollection(pIndex).ORIGINAL_FAS157_PERCENT
    from 
    TABLE(pm2mData) m2m
        --RISKDB.QP_INT_FWD_Curves_raw_data m2m
    where
        cob_date = pcollection(pIndex).COB_DATE
        and m2m.NETTING_GROUP_ID     = pcollection(pIndex).NETTING_GROUP_ID
        and m2m.Underlying_commodity = pcollection(pIndex).UNDERLYING_COMMODITY 
        and m2m.price_vol_indicator =  pcollection(pIndex).PRICE_VOL_INDICATOR 
        and OVERRIDE_COMMODITY =   vc_overridecom
        and m2m.OVERRIDE_BASISPOINT = POverrideLocation
        and to_number(m2m.contractYear_month)Between  pstartYearMonth 
                                           AND        PEndYearMOnth
        and m2m.original_FAS157_Level = FASRecs.original_FAS157_Level
        and rownum < 2;                                        
        
        
        EXIT; --exit the Loop once we have original FAS
        
    END LOOP;
    
            /*******************************************/
           /*   Check for FAS PRICE Threshold         */
           /******************************************/        
       BEGIN
           
        Select 
           ( CASE 
            WHEN  pcollection(pIndex).Proj_loc_amt <  fas_ovr.STRIP_PRICE_CONDITION 
                                     THEN fas_ovr.PRICE_DELTA_THRESHOLD
            ELSE
            -- get whatever was available @ FAS Threshold override level Becomes FINAL FAS value
              to_number(pcollection(pIndex).ORIGINAL_FAS157_PERCENT)  
            END
            ) Price_Threshold
            ,( CASE 
                WHEN  pcollection(pIndex).Proj_loc_amt <  fas_ovr.STRIP_PRICE_CONDITION THEN  'Y'
              ELSE
               'N'
              END
             ) FAS157_Override_Indicator
          ,( CASE 
                WHEN  pcollection(pIndex).Proj_loc_amt <  fas_ovr.STRIP_PRICE_CONDITION THEN  'Price Override'
              ELSE
               'Percent Value'
              END
           ) FINAL_FAS157_TYPE        
        INTO   
              pcollection(pIndex).Final_FAS157_Value
            , pcollection(pIndex).FAS157_Override_Indicator
            , pcollection(pIndex).FINAL_FAS157_TYPE
        FROM 
            RISKDB.QP_FAS_PRICE_OVERRIDE fas_ovr
        where
            UNDERLYING_COMMODITY = pcollection(pIndex).Underlying_Commodity
            and PRICE_VOL_INDICATOR = pcollection(pIndex).PRICE_VOL_Indicator
            and pcollection(pIndex).COB_DATE Between  EFFECTIVE_START_DATE and NVL( EFFECTIVE_END_DATE, pcollection(pIndex).COB_DATE)
            and active_flag = 'Y'
            ;
     
    
        -- If PRICE FOUND but < PRICE threshold then go far FAS Override check        
         IF pcollection(pIndex).FINAL_FAS157_TYPE <> 'Price Override' THEN 
          
          
           Raise NO_DATA_FOUND;
         END IF;
         
            
        EXCEPTION    
        WHEN NO_DATA_FOUND THEN 
        
         -- Check do we have a FAS level Overridden ???
         
             
            /*******************************************/
           /*   Check for FAS LEVEL OVERIDDEN        */
           /******************************************/      
           
             BEGIN
             
               SELECT
                OVERRIDE_FAS_PERCENT_VALUE
                , 'Y'
                , 'Percent Value' Override_Type
               INTO   
                  pcollection(pIndex).Final_FAS157_Value
                , pcollection(pIndex).FAS157_Override_Indicator
                , pcollection(pIndex).FINAL_FAS157_TYPE
               FROM 
               RISKDB.QP_FAS_TOLERANCE_OVERRIDE overfas_tol
                where
                PRICE_VOL_IND = pcollection(pIndex).PRICE_VOL_INDICATOR
                and COMMODITY =   vc_overridecom
                and BASIS_POINT = POverrideLocation
                and FAS_Level = pcollection(pIndex).original_FAS157_Level  
                and pcollection(pIndex).COB_DATE Between  EFFECTIVE_START_DATE and NVL( EFFECTIVE_END_DATE
                                                           , pcollection(pIndex).COB_DATE)
                and active_flag = 'Y'    
                ;
                
             EXCEPTION
             WHEN NO_DATA_FOUND THEN 
             
           
                pcollection(pIndex).Final_FAS157_Value := pcollection(pIndex).ORIGINAL_FAS157_PERCENT ;
                pcollection(pIndex).FAS157_Override_Indicator := 'N';
                pcollection(pIndex).FINAL_FAS157_TYPE := 'Percent Value';
             WHEN OTHERS THEN 
             
              dbms_output.put_line ( 'OTHERS EXCP tol over not found ='
              ||'com='||vc_overridecom 
              ||'Bp='||POverrideLocation
              ||'NG='||pcollection(pIndex).NETTING_GROUP_ID
              ||'month='||pstartYearMonth ||'  AND '|| PEndYearMOnth
              ||' final='||pcollection(pIndex).Final_FAS157_Value ||' type= '||pcollection(pIndex).FINAL_FAS157_TYPE);
              
              
                pcollection(pIndex).Final_FAS157_Value := pcollection(pIndex).ORIGINAL_FAS157_PERCENT ;
                pcollection(pIndex).FAS157_Override_Indicator := 'N';
                pcollection(pIndex).FINAL_FAS157_TYPE := 'Percent Value';
             
             END;
            
        
        END;
       -- Main Block    
 
END IF ;
-- end of override Location  NOT NULL check
         


WHEN pcalcType = 'COMMODITY_LEVEL_FAS' THEN 

vc_section := 'COMMODITY_LEVEL_FAS';

IF  pOverrideLocation IS NULL THEN 


-- dbms_output.put_line (
--  'com='||pcollection(pIndex).NEER_COMMODITY 
--  ||'Bp='||pcollection(pIndex).NEER_LOCATION
--  ||'NG='||pcollection(pIndex).NETTING_GROUP_ID
--  ||'month='||pstartYearMonth ||'  AND '|| PEndYearMOnth
-- );



    For FASrecs IN 
    (
    WITH 
    distinctFAS as (
        Select DISTINCT
            m.original_FAS157_Level
        FROM
        ( 
           SELECT DISTINCT
              m2m.contractYear_month
            , m2m.original_FAS157_Level
            , m2m.ABSOLUTE_LEGGED_M2M_VALUE
            FROM
            TABLE(pm2mData) m2m --PMONTHLY_M2M
           where
--        partition_bit = 2 
            cob_date = pcollection(pIndex).COB_DATE 
            and m2m.NETTING_GROUP_ID     = pcollection(pIndex).NETTING_GROUP_ID
            and Underlying_Commodity = pcollection(pIndex).UNDERLYING_COMMODITY
            and PRICE_VOL_INDICATOR = pcollection(pIndex).PRICE_VOL_INDICATOR
            and COMMODITY = pcollection(pIndex).NEER_COMMODITY 
           -- and m2m.LOCATION = POriginalLocation
            --and to_number(m2m.CONTRACTYEAR_MONTH)  > pCurrentMonth
            and to_number(m2m.contractYear_month) Between  pstartYearMonth 
                                                     AND   PEndYearMOnth
        ) m
      ) 
      , m2mSet AS ( 
        Select DISTINCT
        m.original_FAS157_Level 
        , sum(m.ABSOLUTE_LEGGED_M2M_VALUE) Over ( partition by m.original_FAS157_Level ) ABSOLUTE_LEGGED_M2M_VALUE
        FROM
        ( 
           SELECT DISTINCT
              m2m.contractYear_month
            , m2m.original_FAS157_Level
            , m2m.ABSOLUTE_LEGGED_M2M_VALUE
            FROM
            TABLE(pm2mData) m2m --PMONTHLY_M2M
           where
--        partition_bit = 2 
            cob_date = pcollection(pIndex).COB_DATE 
            and m2m.NETTING_GROUP_ID     = pcollection(pIndex).NETTING_GROUP_ID
            and Underlying_Commodity = pcollection(pIndex).UNDERLYING_COMMODITY
            and PRICE_VOL_INDICATOR = pcollection(pIndex).PRICE_VOL_INDICATOR
            and COMMODITY = pcollection(pIndex).NEER_COMMODITY 
           -- and m2m.LOCATION = POriginalLocation 
            --and to_number(m2m.CONTRACTYEAR_MONTH)  > pCurrentMonth
            and to_number(m2m.contractYear_month) Between  pstartYearMonth 
                                                     AND   PEndYearMOnth
        ) m
      )
        Select 
         m2mset.original_FAS157_Level
         , m2mset.ABSOLUTE_LEGGED_M2M_VALUE
        FROM 
        m2mSet
        , distinctFAS
        where
        m2mSet.original_FAS157_Level = distinctFAS.original_FAS157_Level
        order by m2mset.ABSOLUTE_LEGGED_M2M_VALUE desc NULLS LAST, m2mset.original_FAS157_Level asc
       ) 
    LOOP
    
    pcollection(pIndex).original_FAS157_Level :=  FASRecs.original_FAS157_Level;
    
--    dbms_output.put_line ( 'Tenor fas original_FAS157_Level='
--        ||FASRecs.original_FAS157_Level 
--     || ' m2m ='||FASRecs.LEGGED_M2M_VALUE );
     
    SELECT 
         m2m.ORIGINAL_FAS157_PERCENT 
    INTO 
         pcollection(pIndex).ORIGINAL_FAS157_PERCENT
    from 
    TABLE(pm2mData) m2m
        --RISKDB.QP_INT_FWD_Curves_raw_data m2m
    where
        cob_date = pcollection(pIndex).COB_DATE
        and m2m.NETTING_GROUP_ID     = pcollection(pIndex).NETTING_GROUP_ID
        and m2m.Underlying_commodity = pcollection(pIndex).UNDERLYING_COMMODITY 
        and m2m.price_vol_indicator =  pcollection(pIndex).PRICE_VOL_INDICATOR 
        and m2m.COMMODITY = pcollection(pIndex).NEER_COMMODITY
        and m2m.LOCATION =  pcollection(pIndex).NEER_LOCATION
        --and to_number(m2m.CONTRACTYEAR_MONTH)  > pCurrentMonth
        and to_number(m2m.contractYear_month)Between  pstartYearMonth 
                                           AND        PEndYearMOnth
        and m2m.original_FAS157_Level = FASRecs.original_FAS157_Level
        and rownum < 2;                                        
        
        
        EXIT; --exit the Loop once we have original FAS
        
    END LOOP;
    
            /*******************************************/
           /*   Check for FAS PRICE Threshold         */
           /******************************************/        
       BEGIN
           
        Select 
           ( CASE 
            WHEN  pcollection(pIndex).Proj_loc_amt <  fas_ovr.STRIP_PRICE_CONDITION 
                                     THEN fas_ovr.PRICE_DELTA_THRESHOLD
            ELSE
            -- get whatever was available @ FAS Threshold override level Becomes FINAL FAS value
             to_NUmber( pcollection(pIndex).ORIGINAL_FAS157_PERCENT)  
            END
            ) Price_Threshold
            ,( CASE 
                WHEN  pcollection(pIndex).Proj_loc_amt <  fas_ovr.STRIP_PRICE_CONDITION THEN  
                'Y'
              ELSE
               'N'
              END
             ) FAS157_Override_Indicator
          ,( CASE 
                WHEN  pcollection(pIndex).Proj_loc_amt <  fas_ovr.STRIP_PRICE_CONDITION THEN  'Price Override'
              ELSE
               'Percent Value'
              END
           ) FINAL_FAS157_TYPE        
        INTO   
              pcollection(pIndex).Final_FAS157_Value
            , pcollection(pIndex).FAS157_Override_Indicator
            , pcollection(pIndex).FINAL_FAS157_TYPE
        FROM 
            RISKDB.QP_FAS_PRICE_OVERRIDE fas_ovr
        where
            UNDERLYING_COMMODITY = pcollection(pIndex).Underlying_Commodity
            and PRICE_VOL_INDICATOR = pcollection(pIndex).PRICE_VOL_Indicator
            and pcollection(pIndex).COB_DATE Between  EFFECTIVE_START_DATE and NVL( EFFECTIVE_END_DATE, pcollection(pIndex).COB_DATE)
            and active_flag = 'Y'
            ;
     
    
        
        -- If PRICE FOUND but < PRICE threshold then go far FAS Override check        
         IF pcollection(pIndex).FINAL_FAS157_TYPE <> 'Price Override' THEN 
           --Dbms_output.Put_Line ( 'raising nodatafound to start tol override ');
           Raise NO_DATA_FOUND;
         END IF;
         
            
        EXCEPTION    
        WHEN NO_DATA_FOUND THEN 
        
         -- Check do we have a FAS level Overridden ???
         
             
            /*******************************************/
           /*   Check for FAS LEVEL OVERIDDEN        */
           /******************************************/      
           
             BEGIN
             
               SELECT
                OVERRIDE_FAS_PERCENT_VALUE
                , 'Y'
                , 'Percent Value' Override_Type
               INTO   
                  pcollection(pIndex).Final_FAS157_Value
                , pcollection(pIndex).FAS157_Override_Indicator
                , pcollection(pIndex).FINAL_FAS157_TYPE
               FROM 
               RISKDB.QP_FAS_TOLERANCE_OVERRIDE overfas_tol
                where
                PRICE_VOL_IND = pcollection(pIndex).PRICE_VOL_INDICATOR
                and COmmodity = pcollection(pIndex).NEER_COMMODITY 
                and Basis_point = pcollection(pIndex).NEER_LOCATION 
                and FAS_Level = pcollection(pIndex).original_FAS157_Level  
                and pcollection(pIndex).COB_DATE Between  EFFECTIVE_START_DATE and NVL( EFFECTIVE_END_DATE
                                                           , pcollection(pIndex).COB_DATE)
                and active_flag = 'Y'    
                ;
                
                
 


             EXCEPTION
             WHEN NO_DATA_FOUND THEN 
             
--              dbms_output.put_line ( ' tol over not found ='
--              ||'com='||pcollection(pIndex).NEER_COMMODITY 
--              ||'Bp='||pcollection(pIndex).NEER_LOCATION
--              ||'NG='||pcollection(pIndex).NETTING_GROUP_ID
--              ||'month='||pstartYearMonth ||'  AND '|| PEndYearMOnth
--              ||' final='||pcollection(pIndex).Final_FAS157_Value ||' type= '||pcollection(pIndex).FINAL_FAS157_TYPE);
              
                pcollection(pIndex).Final_FAS157_Value := pcollection(pIndex).ORIGINAL_FAS157_PERCENT ;
                pcollection(pIndex).FAS157_Override_Indicator := 'N';
                pcollection(pIndex).FINAL_FAS157_TYPE := 'Percent Value';
             
 
             WHEN OTHERS THEN 
             
              dbms_output.put_line ( 'OTHERS EXCP tol over not found ='
              ||'com='||pcollection(pIndex).NEER_COMMODITY 
              ||'Bp='||pcollection(pIndex).NEER_LOCATION
              ||'NG='||pcollection(pIndex).NETTING_GROUP_ID
              ||'month='||pstartYearMonth ||'  AND '|| PEndYearMOnth
              ||' final='||pcollection(pIndex).Final_FAS157_Value ||' type= '||pcollection(pIndex).FINAL_FAS157_TYPE);
              
                pcollection(pIndex).Final_FAS157_Value := pcollection(pIndex).ORIGINAL_FAS157_PERCENT ;
                pcollection(pIndex).FAS157_Override_Indicator := 'N';
                pcollection(pIndex).FINAL_FAS157_TYPE := 'Percent Value';

             END;
            
        
        END;
       -- Main Block    
 
END IF ;
-- end of override Location  NULL check

IF  pOverrideLocation IS NOT NULL THEN 


  SELECT  DISTINCT 
   OVERRIDE_COMMODITY INTO vc_overridecom
  FROM
    TABLE(pm2mData) m2m --PMONTHLY_M2M
   where
    PRICE_VOL_INDICATOR = pcollection(pIndex).PRICE_VOL_INDICATOR
    and m2m.OVERRIDE_BASISPOINT = POverrideLocation
    and to_number(m2m.contractYear_month) Between  pstartYearMonth 
                                             AND   PEndYearMOnth
    and rownum < 2;
  

    For FASrecs IN 
    (
    WITH 
    distinctFAS as (
        Select DISTINCT
            m.original_FAS157_Level
        FROM
        ( 
           SELECT DISTINCT
              m2m.contractYear_month
            , m2m.original_FAS157_Level
            , m2m.ABSOLUTE_LEGGED_M2M_VALUE
            FROM
            TABLE(pm2mData) m2m --PMONTHLY_M2M
           where
--        partition_bit = 2 
            cob_date = pcollection(pIndex).COB_DATE 
            and m2m.NETTING_GROUP_ID     = pcollection(pIndex).NETTING_GROUP_ID
            and Underlying_Commodity = pcollection(pIndex).UNDERLYING_COMMODITY
            and PRICE_VOL_INDICATOR =  pcollection(pIndex).PRICE_VOL_INDICATOR
            and OVERRIDE_COMMODITY =   vc_overridecom
            --and m2m.OVERRIDE_BASISPOINT = POverrideLocation
            --and to_number(m2m.CONTRACTYEAR_MONTH)  > pCurrentMonth
            and to_number(m2m.contractYear_month) Between  pstartYearMonth 
                                                     AND   PEndYearMOnth
        ) m
      ) 
      , m2mSet AS ( 
        Select DISTINCT
        m.original_FAS157_Level 
        , sum(ABS(m.ABSOLUTE_LEGGED_M2M_VALUE)) Over ( partition by m.original_FAS157_Level ) ABSOLUTE_LEGGED_M2M_VALUE
        FROM
        ( 
           SELECT DISTINCT
              m2m.contractYear_month
            , m2m.original_FAS157_Level
            , m2m.ABSOLUTE_LEGGED_M2M_VALUE
            FROM
            TABLE(pm2mData) m2m --PMONTHLY_M2M
           where
--        partition_bit = 2 
            cob_date = pcollection(pIndex).COB_DATE 
            and m2m.NETTING_GROUP_ID     = pcollection(pIndex).NETTING_GROUP_ID
            and Underlying_Commodity = pcollection(pIndex).UNDERLYING_COMMODITY
            and PRICE_VOL_INDICATOR = pcollection(pIndex).PRICE_VOL_INDICATOR
            and OVERRIDE_COMMODITY =   vc_overridecom
           -- and m2m.OVERRIDE_BASISPOINT = POverrideLocation 
            --and to_number(m2m.CONTRACTYEAR_MONTH)  > pCurrentMonth
            and to_number(m2m.contractYear_month) Between  pstartYearMonth 
                                                     AND   PEndYearMOnth
        ) m
      )
        Select 
         m2mset.original_FAS157_Level
         , m2mset.ABSOLUTE_LEGGED_M2M_VALUE
        FROM 
        m2mSet
        , distinctFAS
        where
        m2mSet.original_FAS157_Level = distinctFAS.original_FAS157_Level
        order by m2mset.ABSOLUTE_LEGGED_M2M_VALUE desc NULLS LAST, m2mset.original_FAS157_Level asc
       ) 
    LOOP
    
    pcollection(pIndex).original_FAS157_Level :=  FASRecs.original_FAS157_Level;
    
    SELECT 
         m2m.ORIGINAL_FAS157_PERCENT 
    INTO 
         pcollection(pIndex).ORIGINAL_FAS157_PERCENT
    from 
    TABLE(pm2mData) m2m
        --RISKDB.QP_INT_FWD_Curves_raw_data m2m
    where
        cob_date = pcollection(pIndex).COB_DATE
        and m2m.NETTING_GROUP_ID     = pcollection(pIndex).NETTING_GROUP_ID
        and m2m.Underlying_commodity = pcollection(pIndex).UNDERLYING_COMMODITY 
        and m2m.price_vol_indicator =  pcollection(pIndex).PRICE_VOL_INDICATOR 
        and OVERRIDE_COMMODITY =   vc_overridecom
        and m2m.OVERRIDE_BASISPOINT = POverrideLocation
        and to_number(m2m.contractYear_month)Between  pstartYearMonth 
                                           AND        PEndYearMOnth
        and m2m.original_FAS157_Level = FASRecs.original_FAS157_Level
        and rownum < 2;                                        
        
        
        EXIT; --exit the Loop once we have original FAS
        
    END LOOP;
    
            /*******************************************/
           /*   Check for FAS PRICE Threshold         */
           /******************************************/        
       BEGIN
           
        Select 
           ( CASE 
            WHEN  pcollection(pIndex).Proj_loc_amt <  fas_ovr.STRIP_PRICE_CONDITION 
                                     THEN fas_ovr.PRICE_DELTA_THRESHOLD
            ELSE
            -- get whatever was available @ FAS Threshold override level Becomes FINAL FAS value
              to_number(pcollection(pIndex).ORIGINAL_FAS157_PERCENT)  
            END
            ) Price_Threshold
            ,( CASE 
                WHEN  pcollection(pIndex).Proj_loc_amt <  fas_ovr.STRIP_PRICE_CONDITION THEN  'Y'
              ELSE
               'N'
              END
             ) FAS157_Override_Indicator
          ,( CASE 
                WHEN  pcollection(pIndex).Proj_loc_amt <  fas_ovr.STRIP_PRICE_CONDITION THEN  'Price Override'
              ELSE
               'Percent Value'
              END
           ) FINAL_FAS157_TYPE        
        INTO   
              pcollection(pIndex).Final_FAS157_Value
            , pcollection(pIndex).FAS157_Override_Indicator
            , pcollection(pIndex).FINAL_FAS157_TYPE
        FROM 
            RISKDB.QP_FAS_PRICE_OVERRIDE fas_ovr
        where
            UNDERLYING_COMMODITY = pcollection(pIndex).Underlying_Commodity
            and PRICE_VOL_INDICATOR = pcollection(pIndex).PRICE_VOL_Indicator
            and pcollection(pIndex).COB_DATE Between  EFFECTIVE_START_DATE and NVL( EFFECTIVE_END_DATE, pcollection(pIndex).COB_DATE)
            and active_flag = 'Y'
            ;
     
    
        -- If PRICE FOUND but < PRICE threshold then go far FAS Override check        
         IF pcollection(pIndex).FINAL_FAS157_TYPE <> 'Price Override' THEN 
          
           --Dbms_output.Put_Line ( 'raising nodatafound to start tol override ');
           Raise NO_DATA_FOUND;
         END IF;
         
            
        EXCEPTION    
        WHEN NO_DATA_FOUND THEN 
        
         -- Check do we have a FAS level Overridden ???
         
             
            /*******************************************/
           /*   Check for FAS LEVEL OVERIDDEN        */
           /******************************************/      
           
             BEGIN
             
               SELECT
                OVERRIDE_FAS_PERCENT_VALUE
                , 'Y'
                , 'Percent Value' Override_Type
               INTO   
                  pcollection(pIndex).Final_FAS157_Value
                , pcollection(pIndex).FAS157_Override_Indicator
                , pcollection(pIndex).FINAL_FAS157_TYPE
               FROM 
               RISKDB.QP_FAS_TOLERANCE_OVERRIDE overfas_tol
                where
                PRICE_VOL_IND = pcollection(pIndex).PRICE_VOL_INDICATOR
                and COMMODITY =   vc_overridecom
                and BASIS_POINT = POverrideLocation
                and FAS_Level = pcollection(pIndex).original_FAS157_Level  
                and pcollection(pIndex).COB_DATE Between  EFFECTIVE_START_DATE and NVL( EFFECTIVE_END_DATE
                                                           , pcollection(pIndex).COB_DATE)
                and active_flag = 'Y'    
                ;
                
             EXCEPTION
             WHEN NO_DATA_FOUND THEN 
             
           
                pcollection(pIndex).Final_FAS157_Value := pcollection(pIndex).ORIGINAL_FAS157_PERCENT ;
                pcollection(pIndex).FAS157_Override_Indicator := 'N';
                pcollection(pIndex).FINAL_FAS157_TYPE := 'Percent Value';
             WHEN OTHERS THEN 
             
              dbms_output.put_line ( 'OTHERS EXCP tol over not found ='
              ||'com='||vc_overridecom 
              ||'Bp='||POverrideLocation
              ||'NG='||pcollection(pIndex).NETTING_GROUP_ID
              ||'month='||pstartYearMonth ||'  AND '|| PEndYearMOnth
              ||' final='||pcollection(pIndex).Final_FAS157_Value ||' type= '||pcollection(pIndex).FINAL_FAS157_TYPE);
              
              
                pcollection(pIndex).Final_FAS157_Value := pcollection(pIndex).ORIGINAL_FAS157_PERCENT ;
                pcollection(pIndex).FAS157_Override_Indicator := 'N';
                pcollection(pIndex).FINAL_FAS157_TYPE := 'Percent Value';
             
             END;
            
        
        END;
       -- Main Block    
 
END IF ;
-- end of override Location  NOT NULL check

 
WHEN pcalcType = 'VALIDATE_A_CURVE' THEN
     

vc_section := 'VALIDATE_A_CURVE';

    -- If 100 historic Basis Method then validating using internal VS external
    -- is not required 
     
            -- 
       CASE
       

        --for commodity level 
        WHEN   pcollection(pIndex).NEER_COMMODITY =  pcollection(pIndex).NEER_LOCATION 
               AND pcollection(pIndex).COMMODITY_STRIP_FLAG = 'Y' THEN  
 
                    
                   CASE
                     WHEN pcollection (pIndex).Curve_Rank = 1
                     THEN
                        pcollection (pIndex).VALIDATED_FLAG := 'Y';
                        pcollection (pIndex).FULLY_VALIDATED_FLAG := 'Y';
                        pcollection (pIndex).Curve_Validation_Description :=
                           'FULLY VALIDATED, POSITION = 0/M2M <>0';
                     WHEN pcollection (pIndex).Curve_Rank = 2
                     THEN
                        pcollection (pIndex).Curve_Validation_Description :=
                           'FULLY VALIDATED, APPROVED SYSTEM METHODOLOGY'; --skip it , it could be basis curve ranking/ISOCON deals ranking
                        pcollection (pIndex).VALIDATED_FLAG := 'Y';
                        pcollection (pIndex).FULLY_VALIDATED_FLAG := 'Y';
                     WHEN   pcollection (pIndex).PROJ_LOC_AMT IS NULL
                     THEN
                        pcollection (pIndex).Curve_Validation_Description :=
                           'NO INTERNAL PRICE';
                        pcollection (pIndex).VALIDATED_FLAG := 'N';
                         pcollection (pIndex).FULLY_VALIDATED_FLAG := 'N';
                     WHEN (     pcollection (pIndex).MID IS NULL
                           AND  pcollection (pIndex).BID IS NULL
                           AND  pcollection (pIndex).ASK IS NULL)
                     THEN
                         pcollection (pIndex).Curve_Validation_Description :=
                           'NO QUOTE';
                         pcollection (pIndex).VALIDATED_FLAG := 'N';
                         pcollection (pIndex).FULLY_VALIDATED_FLAG := 'N';
                     ELSE
                        NULL;
                  END CASE;
            
       ELSE
           null; 
         
        END CASE;   
   IF   ( NVL(pcollection(pIndex).Curve_Rank , 0 ) <> 1 AND 
          NVL(pcollection(pIndex).Curve_Rank , 0 )  <> 2 ) THEN              
      -- Validation Level 1 
    --- delta Price % must be within FAS percent value 
    IF  pcollection(pIndex).FINAL_FAS157_TYPE ='Price Override'
    THEN
         IF pcollection(pIndex).DELTA_PRICE_VALUE  <= 
           pcollection(pIndex).FINAL_FAS157_VALUE THEN
           pcollection(pIndex).VALIDATED_FLAG := 'Y';
           pcollection(pIndex).FULLY_VALIDATED_FLAG := 'N';
           pcollection(pIndex).Curve_Validation_Description := 'BID/ASK';
           ELSE
           pcollection(pIndex).VALIDATED_FLAG := 'N';
           pcollection(pIndex).FULLY_VALIDATED_FLAG := 'N';
           pcollection(pIndex).Curve_Validation_Description := 'FAS';
          END IF;
    ELSE 
        IF pcollection(pIndex).DELTA_PRICE_PRCNT <= 
           (pcollection(pIndex).FINAL_FAS157_VALUE/100) THEN
                       
           pcollection(pIndex).VALIDATED_FLAG := 'Y';
           pcollection(pIndex).FULLY_VALIDATED_FLAG := 'N';
           pcollection(pIndex).Curve_Validation_Description := 'BID/ASK';
           
          ELSE
            pcollection(pIndex).VALIDATED_FLAG := 'N';
           pcollection(pIndex).FULLY_VALIDATED_FLAG := 'N';
           pcollection(pIndex).Curve_Validation_Description := 'FAS';
           --Validation Level 2 check MID is within BID and ASK 
        END IF;    
      END IF;
      
      --- END IF FOR THE PRICE OVER RIDE
           IF pcollection(pIndex).VALIDATED_FLAG = 'Y' THEN 
                       
             --check if Internam Price within BID and ASK range 
             IF pcollection(pIndex).PROJ_LOC_AMT BETWEEN 
                pcollection(pIndex).BID AND pcollection(pIndex).ASK 
             THEN 
                pcollection(pIndex).VALIDATED_FLAG := 'Y';
                pcollection(pIndex).FULLY_VALIDATED_FLAG := 'Y';
                pcollection(pIndex).Curve_Validation_Description := 'FULLY VALIDATED';
             ELSE -- ELSE OF BID/ ASK Range 
                pcollection(pIndex).VALIDATED_FLAG := 'Y';
                pcollection(pIndex).FULLY_VALIDATED_FLAG := 'N';
                pcollection(pIndex).Curve_Validation_Description := 'BID/ASK';  
             END IF ;
                         
           END IF;
                       
--        ELSE -- Not validated and failed in first step of validation 
--           pcollection(pIndex).VALIDATED_FLAG := 'N';
--           pcollection(pIndex).FULLY_VALIDATED_FLAG := 'N';
--           pcollection(pIndex).Curve_Validation_Description := 'FAS';
--         dbms_output.put_line ('fas2')  ;  
        END IF;   
   -- END OF THE RANK CHECK
 --  END IF;

Else
    null;          
                                          
END CASE;
                                         
 ----dbms_output.Put_line ('After bulk insert');
   pout_rtn_code := c_success;
   pout_rtn_msg := ''; 

EXCEPTION
WHEN OTHERS THEN 
       
  --dbms_output.PUT_LINE('EXCEPTION-'||pstartYearMonth||'-'||PEndYearMOnth||'-'||pcalcType);
       
   pout_rtn_code := c_failure;
   pout_rtn_msg := vc_section ||SUBSTR(SQLERRM,1,200); --Null means Success
   
   
   DMR.PROCESS_LOG_PKG.WRITE_LOG(
        P_PROCESS_NAME => PROCESS_LOG_REC.PROCESS_NAME,
        P_STAGE=> PROCESS_LOG_REC.STAGE,
        P_STATUS=>PROCESS_LOG_PKG.C_STATUS_ERRORS,
        P_MESSAGE=>pout_rtn_msg
   );      
END Derive_StripLevel_Measures;


PROCEDURE Derive_Secondary_calcs (
pindex                  IN NUMBER
, pstartYearMonth       IN NUMBER
, PEndYearMOnth         IN NUMBER
, pextquotes            IN OUT NOCOPY ext_quotes_tab
, pcollection           IN OUT NOCOPY strips_collectn_t
, pm2mData              IN  m2m_by_Loc_tab
, pout_rtn_code         OUT  NOCOPY NUMBER    
, pout_rtn_msg          OUT  NOCOPY VARCHAR2 
) 
IS

v_currentMonth VARCHAR2(10) :=  TO_CHAR(pcollection(pindex).COB_DATE, 'YYYYMM');
v_total_hrs         NUMBER;
v_total_days        NUMBER;
v_strip_count       NUMBER;
e_duplicate_record     EXCEPTION;
E_MISSING_MONTHS       EXCEPTION;
Cursor cur_secondary_quotes IS
with
yr as (
SELECT LEAST ( pcollection(pIndex).CONTRACT_YEAR, SUBSTR(pstartYearMonth, 1,4), SUBSTR(pEndYearMonth, 1,4)) least_contractyr
from dual
)
, HRS AS (
SELECT DISTINCT
COB_DATE 
, LOCATION
, COMMODITY
, EXT_PROFILE_ID
, CONTRACT_YEAR
, CONTRACTYEAR_MONTH
, EXT_LOCATION
,( CASE 
WHEN M.OVERRIDE_BASISPOINT IS NOT NULL THEN M.OVERRIDE_HOURS 
ELSE TOT_HOURS_PER_CONTRACT_MONTH
END
) TOT_HOURS_PER_CONTRACT_MONTH 
,M.CAL_DAYS_IN_CONTRACT_MONTH
FROM 
TABLE(PM2MDATA) M 
WHERE
 COB_DATE = pcollection(pindex).COB_DATE
AND LOCATION = pcollection(pindex).NEER_LOCATION
AND COMMODITY = pcollection(pindex).NEER_COMMODITY
AND EXT_LOCATION = pcollection(pindex).EXT_LOCATION
AND EXT_PROFILE_ID = pcollection(pindex).EXT_PROFILE_ID
AND CONTRACTYEAR_MONTH BETWEEN pstartYearMonth and pEndYearMonth
)
, extqt as (
Select
EXT_EFF_DATE
, ext_profile_id
, EXT_LOCATION
, STRIP_TENOR
, CONTRACT_YEAR
,( CASE 
WHEN Upper(STRIP_TENOR) = 'JANUARY'  THEN TO_NUMBER(qt.CONTRACT_YEAR||'01')
WHEN Upper(STRIP_TENOR) = 'FEBRUARY' THEN TO_NUMBER(qt.CONTRACT_YEAR||'02')
WHEN Upper(STRIP_TENOR) = 'MARCH' THEN TO_NUMBER(qt.CONTRACT_YEAR||'03')
WHEN Upper(STRIP_TENOR) = 'APRIL' THEN TO_NUMBER(qt.CONTRACT_YEAR||'04')
WHEN Upper(STRIP_TENOR) = 'MAY' THEN TO_NUMBER(qt.CONTRACT_YEAR||'05')
WHEN Upper(STRIP_TENOR) = 'JUNE' THEN TO_NUMBER(qt.CONTRACT_YEAR||'06')
WHEN Upper(STRIP_TENOR) = 'JULY' THEN TO_NUMBER(qt.CONTRACT_YEAR||'07')
WHEN Upper(STRIP_TENOR) = 'AUGUST' THEN TO_NUMBER(qt.CONTRACT_YEAR||'08')
WHEN Upper(STRIP_TENOR) = 'SEPTEMBER' THEN TO_NUMBER(qt.CONTRACT_YEAR||'09')
WHEN Upper(STRIP_TENOR) = 'OCTOBER' THEN TO_NUMBER(qt.CONTRACT_YEAR||'10')
WHEN Upper(STRIP_TENOR) = 'NOVEMBER' THEN TO_NUMBER(qt.CONTRACT_YEAR||'11')
WHEN Upper(STRIP_TENOR) = 'DECEMBER' THEN TO_NUMBER(qt.CONTRACT_YEAR||'12')
ELSE
  NULL
END 
) contractYear_Month          
, MID                  
,BID                  
,ASK                  
,ON_MID                
,OFF_MID               
,ON_BID                
,OFF_BID                  
,ON_ASK                   
,OFF_ASK              
,PRICE_TYPE          
,VALUE
,(Select count(*)
from qp_ext_provider_quotes qt1
where qt1.EXT_EFF_DATE = qt.EXT_EFF_DATE
and qt1.ext_profile_id = qt.ext_profile_id
and qt1.Ext_location =  qt.Ext_location
and qt1.CONTRACT_YEAR = qt.CONTRACT_YEAR  
and qt1.strip_tenor = qt.strip_tenor
and NVL(qt1.PRICE_TYPE , 1) = NVL(qt.PRICE_TYPE , 1)  
) count 
FROM 
qp_ext_provider_quotes qt
WHERE
qt.EXT_EFF_DATE =pcollection(pindex).COB_DATE   --('09/30/2015','MM/DD/YYYY')
and qt.CONTRACT_YEAR Between to_number(SUBSTR(pstartYearMonth, 1,4)) and to_number(SUBSTR(pEndYearMonth, 1,4))
and qt.EXT_LOCATION  = pCollection(pindex).EXT_LOCATION -- 'ERCOT Houston 2x16 Off-Peak'
and qt.ext_profile_id =  pCollection(pindex).ext_profile_id
)
Select
extqt.ext_profile_id
, extqt.EXT_LOCATION
,extqt.STRIP_TENOR          
,extqt.CONTRACT_YEAR 
, extqt.CONTRACTYEAR_MONTH      
--,qt.Location
, HRS.TOT_HOURS_PER_CONTRACT_MONTH 
,HRS.CAL_DAYS_IN_CONTRACT_MONTH
, extqt.MID                  
,extqt.BID                  
,extqt.ASK                  
,extqt.ON_MID                
,extqt.OFF_MID               
,extqt.ON_BID                
,extqt.OFF_BID                  
,extqt.ON_ASK                   
,extqt.OFF_ASK              
,extqt.PRICE_TYPE          
,extqt.VALUE    
, extqt.count
, yr.least_contractyr
FROM
HRS
INNER JOIN extqt ON (
 extqt.EXT_EFF_DATE = HRS.COB_DATE
and extqt.ext_profile_id = HRS.ext_profile_id
and extqt.Ext_location =  HRS.EXT_LOCATION
and extqt.CONTRACT_YEAR = HRS.CONTRACT_YEAR 
AND extqt.contractYear_Month =  HRS.contractYear_month
)
INNER JOIN YR ON ( 1=1) 
--AND (CASE
--WHEN Upper(STRIP_TENOR) = 'JANUARY' AND qt.CONTRACT_YEAR =to_number(SUBSTR(pstartYearMonth, 1,4)) THEN TO_NUMBER(SUBSTR(pstartYearMonth, 1,4)||'01')
--WHEN Upper(STRIP_TENOR) = 'JANUARY' AND qt.CONTRACT_YEAR = to_number(SUBSTR(pEndYearMonth, 1,4)) THEN TO_NUMBER(SUBSTR(pEndYearMonth, 1,4)||'01')
--WHEN Upper(STRIP_TENOR) = 'FEBRUARY' AND qt.CONTRACT_YEAR = to_number(SUBSTR(pstartYearMonth, 1,4)) THEN TO_NUMBER(SUBSTR(pstartYearMonth, 1,4)||'02')
--WHEN Upper(STRIP_TENOR) = 'FEBRUARY' AND qt.CONTRACT_YEAR = to_number(SUBSTR(pEndYearMonth, 1,4)) THEN TO_NUMBER(SUBSTR(pEndYearMonth, 1,4)||'02')
--WHEN Upper(STRIP_TENOR) = 'MARCH' AND qt.CONTRACT_YEAR = to_number(SUBSTR(pstartYearMonth, 1,4)) THEN TO_NUMBER(SUBSTR(pstartYearMonth, 1,4)||'03')
--WHEN Upper(STRIP_TENOR) = 'MARCH' AND qt.CONTRACT_YEAR = to_number(SUBSTR(pEndYearMonth, 1,4)) THEN TO_NUMBER(SUBSTR(pEndYearMonth, 1,4)||'03')
--WHEN Upper(STRIP_TENOR) = 'APRIL' AND qt.CONTRACT_YEAR = to_number(SUBSTR(pstartYearMonth, 1,4)) THEN TO_NUMBER(SUBSTR(pstartYearMonth, 1,4)||'04')
--WHEN Upper(STRIP_TENOR) = 'APRIL' AND qt.CONTRACT_YEAR = to_number(SUBSTR(pEndYearMonth, 1,4)) THEN TO_NUMBER(SUBSTR(pEndYearMonth, 1,4)||'04')
--WHEN Upper(STRIP_TENOR) = 'MAY' AND qt.CONTRACT_YEAR = to_number(SUBSTR(pstartYearMonth, 1,4)) THEN TO_NUMBER(SUBSTR(pstartYearMonth, 1,4)||'05')
--WHEN Upper(STRIP_TENOR) = 'MAY' AND qt.CONTRACT_YEAR = to_number(SUBSTR(pEndYearMonth, 1,4)) THEN TO_NUMBER(SUBSTR(pEndYearMonth, 1,4)||'05')
--WHEN Upper(STRIP_TENOR) = 'JUNE' AND qt.CONTRACT_YEAR = to_number(SUBSTR(pstartYearMonth, 1,4)) THEN TO_NUMBER(SUBSTR(pstartYearMonth, 1,4)||'06')
--WHEN Upper(STRIP_TENOR) = 'JUNE' AND qt.CONTRACT_YEAR = to_number(SUBSTR(pEndYearMonth, 1,4)) THEN TO_NUMBER(SUBSTR(pEndYearMonth, 1,4)||'06')
--WHEN Upper(STRIP_TENOR) = 'JULY' AND qt.CONTRACT_YEAR =to_number(SUBSTR(pstartYearMonth, 1,4)) THEN TO_NUMBER(SUBSTR(pstartYearMonth, 1,4)||'07')
--WHEN Upper(STRIP_TENOR) = 'JULY' AND qt.CONTRACT_YEAR = to_number(SUBSTR(pEndYearMonth, 1,4)) THEN TO_NUMBER(SUBSTR(pEndYearMonth, 1,4)||'07')
--WHEN Upper(STRIP_TENOR) = 'AUGUST' AND qt.CONTRACT_YEAR = to_number(SUBSTR(pstartYearMonth, 1,4)) THEN TO_NUMBER(SUBSTR(pstartYearMonth, 1,4)||'08')
--WHEN Upper(STRIP_TENOR) = 'AUGUST' AND qt.CONTRACT_YEAR = to_number(SUBSTR(pEndYearMonth, 1,4)) THEN TO_NUMBER(SUBSTR(pEndYearMonth, 1,4)||'08')
--WHEN Upper(STRIP_TENOR) = 'SEPTEMBER' AND qt.CONTRACT_YEAR = to_number(SUBSTR(pstartYearMonth, 1,4)) THEN TO_NUMBER(SUBSTR(pstartYearMonth, 1,4)||'09')
--WHEN Upper(STRIP_TENOR) = 'SEPTEMBER' AND qt.CONTRACT_YEAR = to_number(SUBSTR(pEndYearMonth, 1,4)) THEN TO_NUMBER(SUBSTR(pEndYearMonth, 1,4)||'09')
--WHEN Upper(STRIP_TENOR) = 'OCTOBER' AND qt.CONTRACT_YEAR = to_number(SUBSTR(pstartYearMonth, 1,4)) THEN TO_NUMBER(SUBSTR(pstartYearMonth, 1,4)||'10')
--WHEN Upper(STRIP_TENOR) = 'OCTOBER' AND qt.CONTRACT_YEAR = to_number(SUBSTR(pEndYearMonth, 1,4)) THEN TO_NUMBER(SUBSTR(pEndYearMonth, 1,4)||'10')
--WHEN Upper(STRIP_TENOR) = 'NOVEMBER' AND qt.CONTRACT_YEAR = to_number(SUBSTR(pstartYearMonth, 1,4)) THEN TO_NUMBER(SUBSTR(pstartYearMonth, 1,4)||'11')
--WHEN Upper(STRIP_TENOR) = 'NOVEMBER' AND qt.CONTRACT_YEAR = to_number(SUBSTR(pEndYearMonth, 1,4)) THEN TO_NUMBER(SUBSTR(pEndYearMonth, 1,4)||'11')
--WHEN Upper(STRIP_TENOR) = 'DECEMBER' AND qt.CONTRACT_YEAR = to_number(SUBSTR(pstartYearMonth, 1,4)) THEN TO_NUMBER(SUBSTR(pstartYearMonth, 1,4)||'12')
--WHEN Upper(STRIP_TENOR) = 'DECEMBER' AND qt.CONTRACT_YEAR = to_number(SUBSTR(pEndYearMonth, 1,4)) THEN TO_NUMBER(SUBSTR(pEndYearMonth, 1,4)||'12')
--  ELSE
--  NULL
-- END 
-- ) BETWEEN pstartYearMonth AND pEndYearMonth 
order by extqt.ext_Location ,extqt.CONTRACTYEAR_MONTH
;

lb_quotes_rec       Boolean := FALSE;

BEGIN

  -- start with default value as 'N' for secondary quotes
  pCollection(pIndex).SECONDARY_CALCULATION_FLAG := 'N';  

--DBMS_OUTPUT.PUT_LINE('in Secondary calculation'||pstartYearMonth||'-'||PEndYearMOnth);
 
  
BEGIN
SELECT COUNT(DISTINCT qt.CONTRACT_YEAR||STRIP_TENOR)
INTO v_strip_count
FROM qp_ext_provider_quotes qt
WHERE qt.EXT_EFF_DATE = pcollection(pindex).COB_DATE
AND qt.ext_profile_id = pCollection(pindex).ext_profile_id
AND qt.Ext_location   =  pCollection(pindex).Ext_location
AND  NVL(qt.PRICE_TYPE , 1) = NVL(qt.PRICE_TYPE , 1)   
AND qt.CONTRACT_YEAR BETWEEN to_number(SUBSTR(pstartYearMonth, 1,4)) AND to_number(SUBSTR(pEndYearMonth, 1,4))
AND (CASE
WHEN Upper(qt.STRIP_TENOR) = 'JANUARY' AND CONTRACT_YEAR =to_number(SUBSTR(pstartYearMonth, 1,4)) THEN TO_NUMBER(SUBSTR(pstartYearMonth, 1,4)||'01')
WHEN Upper(qt.STRIP_TENOR) = 'JANUARY' AND CONTRACT_YEAR = to_number(SUBSTR(pEndYearMonth, 1,4)) THEN TO_NUMBER(SUBSTR(pEndYearMonth, 1,4)||'01')
WHEN Upper(qt.STRIP_TENOR) = 'FEBRUARY' AND CONTRACT_YEAR = to_number(SUBSTR(pstartYearMonth, 1,4)) THEN TO_NUMBER(SUBSTR(pstartYearMonth, 1,4)||'02')
WHEN Upper(qt.STRIP_TENOR) = 'FEBRUARY' AND CONTRACT_YEAR = to_number(SUBSTR(pEndYearMonth, 1,4)) THEN TO_NUMBER(SUBSTR(pEndYearMonth, 1,4)||'02')
WHEN Upper(qt.STRIP_TENOR) = 'MARCH' AND CONTRACT_YEAR = to_number(SUBSTR(pstartYearMonth, 1,4)) THEN TO_NUMBER(SUBSTR(pstartYearMonth, 1,4)||'03')
WHEN Upper(qt.STRIP_TENOR) = 'MARCH' AND CONTRACT_YEAR = to_number(SUBSTR(pEndYearMonth, 1,4)) THEN TO_NUMBER(SUBSTR(pEndYearMonth, 1,4)||'03')
WHEN Upper(qt.STRIP_TENOR) = 'APRIL' AND CONTRACT_YEAR = to_number(SUBSTR(pstartYearMonth, 1,4)) THEN TO_NUMBER(SUBSTR(pstartYearMonth, 1,4)||'04')
WHEN Upper(qt.STRIP_TENOR) = 'APRIL' AND CONTRACT_YEAR = to_number(SUBSTR(pEndYearMonth, 1,4)) THEN TO_NUMBER(SUBSTR(pEndYearMonth, 1,4)||'04')
WHEN Upper(qt.STRIP_TENOR) = 'MAY' AND CONTRACT_YEAR = to_number(SUBSTR(pstartYearMonth, 1,4)) THEN TO_NUMBER(SUBSTR(pstartYearMonth, 1,4)||'05')
WHEN Upper(qt.STRIP_TENOR) = 'MAY' AND CONTRACT_YEAR = to_number(SUBSTR(pEndYearMonth, 1,4)) THEN TO_NUMBER(SUBSTR(pEndYearMonth, 1,4)||'05')
WHEN Upper(qt.STRIP_TENOR) = 'JUNE' AND CONTRACT_YEAR = to_number(SUBSTR(pstartYearMonth, 1,4)) THEN TO_NUMBER(SUBSTR(pstartYearMonth, 1,4)||'06')
WHEN Upper(qt.STRIP_TENOR) = 'JUNE' AND CONTRACT_YEAR = to_number(SUBSTR(pEndYearMonth, 1,4)) THEN TO_NUMBER(SUBSTR(pEndYearMonth, 1,4)||'06')
WHEN Upper(qt.STRIP_TENOR) = 'JULY' AND CONTRACT_YEAR =to_number(SUBSTR(pstartYearMonth, 1,4)) THEN TO_NUMBER(SUBSTR(pstartYearMonth, 1,4)||'07')
WHEN Upper(qt.STRIP_TENOR) = 'JULY' AND CONTRACT_YEAR = to_number(SUBSTR(pEndYearMonth, 1,4)) THEN TO_NUMBER(SUBSTR(pEndYearMonth, 1,4)||'07')
WHEN Upper(qt.STRIP_TENOR) = 'AUGUST' AND CONTRACT_YEAR = to_number(SUBSTR(pstartYearMonth, 1,4)) THEN TO_NUMBER(SUBSTR(pstartYearMonth, 1,4)||'08')
WHEN Upper(qt.STRIP_TENOR) = 'AUGUST' AND CONTRACT_YEAR = to_number(SUBSTR(pEndYearMonth, 1,4)) THEN TO_NUMBER(SUBSTR(pEndYearMonth, 1,4)||'08')
WHEN Upper(qt.STRIP_TENOR) = 'SEPTEMBER' AND CONTRACT_YEAR = to_number(SUBSTR(pstartYearMonth, 1,4)) THEN TO_NUMBER(SUBSTR(pstartYearMonth, 1,4)||'09')
WHEN Upper(qt.STRIP_TENOR) = 'SEPTEMBER' AND CONTRACT_YEAR = to_number(SUBSTR(pEndYearMonth, 1,4)) THEN TO_NUMBER(SUBSTR(pEndYearMonth, 1,4)||'09')
WHEN Upper(qt.STRIP_TENOR) = 'OCTOBER' AND CONTRACT_YEAR = to_number(SUBSTR(pstartYearMonth, 1,4)) THEN TO_NUMBER(SUBSTR(pstartYearMonth, 1,4)||'10')
WHEN Upper(qt.STRIP_TENOR) = 'OCTOBER' AND CONTRACT_YEAR = to_number(SUBSTR(pEndYearMonth, 1,4)) THEN TO_NUMBER(SUBSTR(pEndYearMonth, 1,4)||'10')
WHEN Upper(qt.STRIP_TENOR) = 'NOVEMBER' AND CONTRACT_YEAR = to_number(SUBSTR(pstartYearMonth, 1,4)) THEN TO_NUMBER(SUBSTR(pstartYearMonth, 1,4)||'11')
WHEN Upper(qt.STRIP_TENOR) = 'NOVEMBER' AND CONTRACT_YEAR = to_number(SUBSTR(pEndYearMonth, 1,4)) THEN TO_NUMBER(SUBSTR(pEndYearMonth, 1,4)||'11')
WHEN Upper(qt.STRIP_TENOR) = 'DECEMBER' AND CONTRACT_YEAR = to_number(SUBSTR(pstartYearMonth, 1,4)) THEN TO_NUMBER(SUBSTR(pstartYearMonth, 1,4)||'12')
WHEN Upper(qt.STRIP_TENOR) = 'DECEMBER' AND CONTRACT_YEAR = to_number(SUBSTR(pEndYearMonth, 1,4)) THEN TO_NUMBER(SUBSTR(pEndYearMonth, 1,4)||'12')
  ELSE
  NULL
 END 
 ) BETWEEN pstartYearMonth AND pEndYearMonth ;
 END;

       dbms_output.PUT_LINE('COUNT:= '||v_strip_count||
                            'TOTAL_MONTHS:='||  pCollection(pindex).TOTAL_TENOR_MONTHS); 
                            
   IF  v_strip_count  <> pCollection(pindex).TOTAL_TENOR_MONTHS
   THEN 
    dbms_output.put_line ('missing Month please verify');
   RAISE E_MISSING_MONTHS;
   END IF;

    For quotes_rec IN cur_secondary_quotes LOOP
     
--    DBMS_OUTPUT.PUT_LINE (
--    'profileId = '||quotes_rec.ext_profile_id
--    ||'Loc='||quotes_rec.EXT_LOCATION
--    ||'tenor='||quotes_rec.STRIP_TENOR          
--    ||'year='||quotes_rec.CONTRACTYEAR_MONTH       
--    ||'mid='||quotes_rec.MID                  
--    ||'Bid='||quotes_rec.BID                  
--    ||'ask='||quotes_rec.ASK                  
--    ||'on_mid='||quotes_rec.ON_MID                
--    ||'off_mid='||quotes_rec.OFF_MID               
--    ||'on_mid='||quotes_rec.ON_BID                
--    ||'off_mid='||quotes_rec.OFF_BID                  
--    ||'on_ask='||quotes_rec.ON_ASK                   
--    ||'off_ask='||quotes_rec.OFF_ASK              
--    ||'pricetype='||quotes_rec.PRICE_TYPE          
--    ||'value='||quotes_rec.VALUE    
--    ||'leastYR='||quotes_rec.least_contractyr 
--    );
    
         
         -- Quotes available   
      lb_quotes_rec := TRUE;   
         
                              
    IF quotes_rec.count > 1 THEN 
    dbms_output.put_line('dup found');
      RAISE e_duplicate_record;
    END IF;
    
   --- check adn assign MID/BID/ASK                  
       IF quotes_rec.MID is not null THEN 
 
       

          
             CASE 
              WHEN  pcollection(pIndex).UNDERLYING_COMMODITY = 'ELECTRICITY' THEN
                 pCollection(pIndex).MID :=
                        NVL(pCollection(pIndex).MID, 0)+ 
                        quotes_rec.MID * quotes_rec.TOT_HOURS_PER_CONTRACT_MONTH;
                        
              WHEN pcollection(pIndex).UNDERLYING_COMMODITY IN ( 'NATURAL GAS','GAS', 'NG 6 MOAVG') THEN 
                 pCollection(pIndex).MID :=
                        NVL(pCollection(pIndex).MID, 0)+
                                         quotes_rec.MID* quotes_rec.CAL_DAYS_IN_CONTRACT_MONTH;
              WHEN pcollection(pIndex).UNDERLYING_COMMODITY = 'OIL' THEN
                 pCollection(pIndex).MID :=
                        NVL(pCollection(pIndex).MID, 0)+
                        quotes_rec.MID ;
              WHEN pcollection(pIndex).UNDERLYING_COMMODITY = 'CAPACITY' THEN    
                 pCollection(pIndex).MID :=
                        NVL(pCollection(pIndex).MID, 0)+
                        quotes_rec.MID;
              WHEN pcollection(pIndex).UNDERLYING_COMMODITY = 'EMISSIONS' THEN
                 pCollection(pIndex).MID :=
                        NVL(pCollection(pIndex).MID, 0)+
                        quotes_rec.MID;
              else
                  null; -- do nothing  
              END CASE;
             
       END IF;
       
             
      IF quotes_rec.BID is not null THEN 
             CASE 
              WHEN  pcollection(pIndex).UNDERLYING_COMMODITY = 'ELECTRICITY' THEN
                 pCollection(pIndex).BID :=
                        NVL(pCollection(pIndex).BID, 0)+ 
                        quotes_rec.BID * quotes_rec.TOT_HOURS_PER_CONTRACT_MONTH;
                        
              WHEN pcollection(pIndex).UNDERLYING_COMMODITY IN ( 'NATURAL GAS','GAS', 'NG 6 MOAVG') THEN 
                 pCollection(pIndex).BID :=
                        NVL(pCollection(pIndex).BID, 0)+
                                         quotes_rec.BID * quotes_rec.CAL_DAYS_IN_CONTRACT_MONTH;
              WHEN pcollection(pIndex).UNDERLYING_COMMODITY = 'OIL' THEN
                 pCollection(pIndex).BID :=
                        NVL(pCollection(pIndex).BID, 0)+
                        quotes_rec.BID ;
              WHEN pcollection(pIndex).UNDERLYING_COMMODITY = 'CAPACITY' THEN    
                 pCollection(pIndex).BID :=
                        NVL(pCollection(pIndex).BID, 0)+
                        quotes_rec.BID;
              WHEN pcollection(pIndex).UNDERLYING_COMMODITY = 'EMISSIONS' THEN
                 pCollection(pIndex).BID :=
                        NVL(pCollection(pIndex).BID, 0)+
                        quotes_rec.BID;
              else
                 null; 
              END CASE;
      END IF;
       
      IF quotes_rec.ASK is not null THEN 
          
          CASE 
              WHEN  pcollection(pIndex).UNDERLYING_COMMODITY = 'ELECTRICITY' THEN
                 pCollection(pIndex).ASK :=
                        NVL(pCollection(pIndex).ASK, 0)+ 
                        quotes_rec.ASK * quotes_rec.TOT_HOURS_PER_CONTRACT_MONTH;
                        
              WHEN pcollection(pIndex).UNDERLYING_COMMODITY IN ( 'NATURAL GAS','GAS', 'NG 6 MOAVG') THEN 
                 pCollection(pIndex).ASK :=
                        NVL(pCollection(pIndex).ASK, 0)+
                                         quotes_rec.ASK* quotes_rec.CAL_DAYS_IN_CONTRACT_MONTH ;
              WHEN pcollection(pIndex).UNDERLYING_COMMODITY = 'OIL' THEN
                 pCollection(pIndex).ASK :=
                        NVL(pCollection(pIndex).ASK, 0)+
                        quotes_rec.ASK ;
              WHEN pcollection(pIndex).UNDERLYING_COMMODITY = 'CAPACITY' THEN    
                 pCollection(pIndex).ASK :=
                        NVL(pCollection(pIndex).ASK, 0)+
                        quotes_rec.ASK;
              WHEN pcollection(pIndex).UNDERLYING_COMMODITY = 'EMISSIONS' THEN
                 pCollection(pIndex).ASK :=
                        NVL(pCollection(pIndex).ASK, 0)+
                        quotes_rec.ASK;
              else
                 null; --do nothing 
              END CASE;
              
      END IF;
     
    IF quotes_rec.ON_BID is not null THEN 
        
        IF pcollection(pindex).ON_OFF_PEAK_INDICATOR = 'ON' THEN
            
         CASE 
          WHEN  pcollection(pIndex).UNDERLYING_COMMODITY = 'ELECTRICITY' THEN
             pCollection(pIndex).BID :=
                    NVL(pCollection(pIndex).BID, 0)+ 
                    quotes_rec.ON_BID * quotes_rec.TOT_HOURS_PER_CONTRACT_MONTH;
                        
          WHEN pcollection(pIndex).UNDERLYING_COMMODITY IN ( 'NATURAL GAS','GAS', 'NG 6 MOAVG') THEN 
             pCollection(pIndex).BID :=
                    NVL(pCollection(pIndex).BID, 0)+
                                     quotes_rec.ON_BID* quotes_rec.CAL_DAYS_IN_CONTRACT_MONTH ;
          WHEN pcollection(pIndex).UNDERLYING_COMMODITY = 'OIL' THEN
             pCollection(pIndex).BID :=
                    NVL(pCollection(pIndex).BID, 0)+
                    quotes_rec.ON_BID ;
          WHEN pcollection(pIndex).UNDERLYING_COMMODITY = 'CAPACITY' THEN    
             pCollection(pIndex).BID :=
                    NVL(pCollection(pIndex).BID, 0)+
                    quotes_rec.ON_BID;
          WHEN pcollection(pIndex).UNDERLYING_COMMODITY = 'EMISSIONS' THEN
             pCollection(pIndex).BID :=
                    NVL(pCollection(pIndex).BID, 0)+
                    quotes_rec.ON_BID;
          else
             null; 
          END CASE;
    
        END IF;
        
    END IF;
        
    IF quotes_rec.OFF_BID is not null THEN 
        
        IF pcollection(pindex).ON_OFF_PEAK_INDICATOR <> 'ON' THEN
             
         CASE 
          WHEN  pcollection(pIndex).UNDERLYING_COMMODITY = 'ELECTRICITY' THEN
             pCollection(pIndex).BID :=
                    NVL(pCollection(pIndex).BID, 0)+ 
                    quotes_rec.OFF_BID * quotes_rec.TOT_HOURS_PER_CONTRACT_MONTH;
                        
          WHEN pcollection(pIndex).UNDERLYING_COMMODITY IN ( 'NATURAL GAS','GAS', 'NG 6 MOAVG') THEN 
             pCollection(pIndex).BID :=
                    NVL(pCollection(pIndex).BID, 0)+
                                     quotes_rec.OFF_BID* quotes_rec.CAL_DAYS_IN_CONTRACT_MONTH ;
          WHEN pcollection(pIndex).UNDERLYING_COMMODITY = 'OIL' THEN
             pCollection(pIndex).BID :=
                    NVL(pCollection(pIndex).BID, 0)+
                    quotes_rec.OFF_BID ;
          WHEN pcollection(pIndex).UNDERLYING_COMMODITY = 'CAPACITY' THEN    
             pCollection(pIndex).BID :=
                    NVL(pCollection(pIndex).BID, 0)+
                    quotes_rec.OFF_BID;
          WHEN pcollection(pIndex).UNDERLYING_COMMODITY = 'EMISSIONS' THEN
             pCollection(pIndex).BID :=
                    NVL(pCollection(pIndex).BID, 0)+
                    quotes_rec.OFF_BID;
          else
                null; 
          END CASE;    
        END IF;
        
    END IF;
     
        
           
    IF quotes_rec.ON_MID is not null THEN 
        
        IF pcollection(pindex).ON_OFF_PEAK_INDICATOR = 'ON' THEN

         CASE 
          WHEN  pcollection(pIndex).UNDERLYING_COMMODITY = 'ELECTRICITY' THEN
             pCollection(pIndex).MID :=
                    NVL(pCollection(pIndex).MID, 0)+ 
                    quotes_rec.ON_MID * quotes_rec.TOT_HOURS_PER_CONTRACT_MONTH;
                        
          WHEN pcollection(pIndex).UNDERLYING_COMMODITY IN ( 'NATURAL GAS','GAS', 'NG 6 MOAVG') THEN 
             pCollection(pIndex).MID :=
                    NVL(pCollection(pIndex).MID, 0)+
                                     quotes_rec.ON_MID* quotes_rec.CAL_DAYS_IN_CONTRACT_MONTH ;
          WHEN pcollection(pIndex).UNDERLYING_COMMODITY = 'OIL' THEN
             pCollection(pIndex).MID :=
                    NVL(pCollection(pIndex).MID, 0)+
                    quotes_rec.ON_MID ;
          WHEN pcollection(pIndex).UNDERLYING_COMMODITY = 'CAPACITY' THEN    
             pCollection(pIndex).MID :=
                    NVL(pCollection(pIndex).MID, 0)+
                    quotes_rec.ON_MID;
          WHEN pcollection(pIndex).UNDERLYING_COMMODITY = 'EMISSIONS' THEN
             pCollection(pIndex).MID :=
                    NVL(pCollection(pIndex).MID, 0)+
                    quotes_rec.ON_MID;
          else
              null; 
          END CASE;
    
        END IF;
        
    END IF;
        
    IF quotes_rec.OFF_MID is not null THEN 
        
        IF pcollection(pindex).ON_OFF_PEAK_INDICATOR <> 'ON' THEN
        
         CASE 
          WHEN  pcollection(pIndex).UNDERLYING_COMMODITY = 'ELECTRICITY' THEN
             pCollection(pIndex).MID :=
                    NVL(pCollection(pIndex).MID, 0)+ 
                    quotes_rec.OFF_MID * quotes_rec.TOT_HOURS_PER_CONTRACT_MONTH;
                        
          WHEN pcollection(pIndex).UNDERLYING_COMMODITY IN ( 'NATURAL GAS','GAS', 'NG 6 MOAVG') THEN 
             pCollection(pIndex).MID :=
                    NVL(pCollection(pIndex).MID, 0)+
                                     quotes_rec.OFF_MID* quotes_rec.CAL_DAYS_IN_CONTRACT_MONTH ;
          WHEN pcollection(pIndex).UNDERLYING_COMMODITY = 'OIL' THEN
             pCollection(pIndex).MID :=
                    NVL(pCollection(pIndex).MID, 0)+
                    quotes_rec.OFF_MID ;
          WHEN pcollection(pIndex).UNDERLYING_COMMODITY = 'CAPACITY' THEN    
             pCollection(pIndex).MID :=
                    NVL(pCollection(pIndex).MID, 0)+
                    quotes_rec.OFF_MID;
          WHEN pcollection(pIndex).UNDERLYING_COMMODITY = 'EMISSIONS' THEN
             pCollection(pIndex).MID :=
                    NVL(pCollection(pIndex).MID, 0)+
                    quotes_rec.OFF_MID;
          else
             null; --do nothing 
          END CASE;   
           
        END IF;
        
    END IF;
               
        
    IF quotes_rec.ON_ASK is not null THEN 
        
        IF pcollection(pindex).ON_OFF_PEAK_INDICATOR = 'ON' THEN
         CASE 
          WHEN  pcollection(pIndex).UNDERLYING_COMMODITY = 'ELECTRICITY' THEN
             pCollection(pIndex).ASK :=
                    NVL(pCollection(pIndex).ASK, 0)+ 
                    quotes_rec.ON_ASK * quotes_rec.TOT_HOURS_PER_CONTRACT_MONTH;
                        
          WHEN pcollection(pIndex).UNDERLYING_COMMODITY IN ( 'NATURAL GAS','GAS', 'NG 6 MOAVG') THEN 
            pCollection(pIndex).ASK :=
                    NVL(pCollection(pIndex).ASK, 0)+ 
                                     quotes_rec.ON_ASK* quotes_rec.CAL_DAYS_IN_CONTRACT_MONTH ;
          WHEN pcollection(pIndex).UNDERLYING_COMMODITY = 'OIL' THEN
             pCollection(pIndex).ASK :=
                    NVL(pCollection(pIndex).ASK, 0)+ 
                                     quotes_rec.ON_ASK ;
          WHEN pcollection(pIndex).UNDERLYING_COMMODITY = 'CAPACITY' THEN    
            pCollection(pIndex).ASK :=
                    NVL(pCollection(pIndex).ASK, 0)+ 
                                     quotes_rec.ON_ASK;
          WHEN pcollection(pIndex).UNDERLYING_COMMODITY = 'EMISSIONS' THEN
            pCollection(pIndex).ASK :=
                    NVL(pCollection(pIndex).ASK, 0)+ 
                                     quotes_rec.ON_ASK;
          else
            null;  --- do nothing 
          END CASE;    
        END IF;
        
    END IF;
        
    IF quotes_rec.OFF_ASK is not null THEN 
        
      IF pcollection(pindex).ON_OFF_PEAK_INDICATOR <> 'ON' THEN
         CASE 
          WHEN  pcollection(pIndex).UNDERLYING_COMMODITY = 'ELECTRICITY' THEN
             pCollection(pIndex).ASK :=
                    NVL(pCollection(pIndex).ASK, 0)+ 
                    quotes_rec.OFF_ASK * quotes_rec.TOT_HOURS_PER_CONTRACT_MONTH;
                        
          WHEN pcollection(pIndex).UNDERLYING_COMMODITY IN ( 'NATURAL GAS','GAS', 'NG 6 MOAVG') THEN 
            pCollection(pIndex).ASK :=
                    NVL(pCollection(pIndex).ASK, 0)+ 
                                     quotes_rec.OFF_ASK* quotes_rec.CAL_DAYS_IN_CONTRACT_MONTH ;
          WHEN pcollection(pIndex).UNDERLYING_COMMODITY = 'OIL' THEN
             pCollection(pIndex).ASK :=
                    NVL(pCollection(pIndex).ASK, 0)+ 
                                     quotes_rec.OFF_ASK ;
          WHEN pcollection(pIndex).UNDERLYING_COMMODITY = 'CAPACITY' THEN    
            pCollection(pIndex).ASK :=
                    NVL(pCollection(pIndex).ASK, 0)+ 
                                     quotes_rec.OFF_ASK;
          WHEN pcollection(pIndex).UNDERLYING_COMMODITY = 'EMISSIONS' THEN
            pCollection(pIndex).ASK :=
                    NVL(pCollection(pIndex).ASK, 0)+ 
                                     quotes_rec.OFF_ASK;
          else
                null; -- do nothing 
          END CASE;
              
     END IF;
        
    END IF;  
        
       
      IF  quotes_rec.VALUE is NOT NULL AND Upper(quotes_rec.PRICE_TYPE) = 'MID' THEN 
      
       CASE 
              WHEN  pcollection(pIndex).UNDERLYING_COMMODITY = 'ELECTRICITY' THEN
               pCollection(pIndex).MID :=
                    NVL(pCollection(pIndex).MID,0)+quotes_rec.VALUE * quotes_rec.TOT_HOURS_PER_CONTRACT_MONTH;
              
              WHEN pcollection(pIndex).UNDERLYING_COMMODITY IN ( 'NATURAL GAS','GAS', 'NG 6 MOAVG') THEN 
                 pCollection(pIndex).MID :=
                    NVL(pCollection(pIndex).MID,0)+quotes_rec.VALUE* quotes_rec.CAL_DAYS_IN_CONTRACT_MONTH ;
              
              WHEN pcollection(pIndex).UNDERLYING_COMMODITY = 'OIL' THEN      
                 pCollection(pIndex).MID :=
                    NVL(pCollection(pIndex).MID,0)+quotes_rec.VALUE    ;
              
              WHEN pcollection(pIndex).UNDERLYING_COMMODITY = 'CAPACITY' THEN
                 pCollection(pIndex).MID :=
                    NVL(pCollection(pIndex).MID,0)+quotes_rec.VALUE ;              
              
              WHEN pcollection(pIndex).UNDERLYING_COMMODITY = 'EMISSIONS' THEN
                 pCollection(pIndex).MID :=
                    NVL(pCollection(pIndex).MID,0)+quotes_rec.VALUE ;              
              else
                   null; --do nothing  
              END CASE;     
                      
      END IF;
         
      IF  quotes_rec.VALUE is NOT NULL AND Upper(quotes_rec.PRICE_TYPE) = 'BID' THEN
      
      
            CASE 
              WHEN  pcollection(pIndex).UNDERLYING_COMMODITY = 'ELECTRICITY' THEN
               pCollection(pIndex).BID :=
                    NVL(pCollection(pIndex).BID,0)+quotes_rec.VALUE * quotes_rec.TOT_HOURS_PER_CONTRACT_MONTH;
              
              WHEN pcollection(pIndex).UNDERLYING_COMMODITY IN ( 'NATURAL GAS','GAS', 'NG 6 MOAVG') THEN 
                 pCollection(pIndex).BID :=
                    NVL(pCollection(pIndex).BID,0)+quotes_rec.VALUE * quotes_rec.CAL_DAYS_IN_CONTRACT_MONTH;
              
              WHEN pcollection(pIndex).UNDERLYING_COMMODITY = 'OIL' THEN      
                 pCollection(pIndex).BID :=
                    NVL(pCollection(pIndex).BID,0)+quotes_rec.VALUE  ;
              
              WHEN pcollection(pIndex).UNDERLYING_COMMODITY = 'CAPACITY' THEN
                 pCollection(pIndex).BID :=
                    NVL(pCollection(pIndex).BID,0)+quotes_rec.VALUE ;              
              
              WHEN pcollection(pIndex).UNDERLYING_COMMODITY = 'EMISSIONS' THEN
                 pCollection(pIndex).BID :=
                    NVL(pCollection(pIndex).BID,0)+quotes_rec.VALUE ;              
              else
                   null; --do nothing  
              END CASE;
              
     END IF;
     
     IF  quotes_rec.VALUE is NOT NULL AND Upper(quotes_rec.PRICE_TYPE) = 'ASK' THEN
       
     
          CASE 
              WHEN  pcollection(pIndex).UNDERLYING_COMMODITY = 'ELECTRICITY' THEN
               pCollection(pIndex).ASK :=
                    NVL(pCollection(pIndex).ASK,0)+quotes_rec.VALUE * quotes_rec.TOT_HOURS_PER_CONTRACT_MONTH;
              
              WHEN pcollection(pIndex).UNDERLYING_COMMODITY IN ( 'NATURAL GAS','GAS', 'NG 6 MOAVG') THEN 
                 pCollection(pIndex).ASK :=
                    NVL(pCollection(pIndex).ASK,0)+quotes_rec.VALUE * quotes_rec.CAL_DAYS_IN_CONTRACT_MONTH;
              
              WHEN pcollection(pIndex).UNDERLYING_COMMODITY = 'OIL' THEN      
                 pCollection(pIndex).ASK :=
                    NVL(pCollection(pIndex).ASK,0)+quotes_rec.VALUE  ;
              
              WHEN pcollection(pIndex).UNDERLYING_COMMODITY = 'CAPACITY' THEN
                 pCollection(pIndex).ASK :=
                    NVL(pCollection(pIndex).ASK,0)+quotes_rec.VALUE ;              
              
              WHEN pcollection(pIndex).UNDERLYING_COMMODITY = 'EMISSIONS' THEN
                 pCollection(pIndex).ASK :=
                    NVL(pCollection(pIndex).ASK,0)+quotes_rec.VALUE ;              
              else
                   null; --do nothing  
          END CASE;
          
      END IF;
                  

       pcollection(pIndex).ext_CONTRACT_YEAR   :=  quotes_rec.least_contractyr;
 

      DBMS_OUTPUT.PUT_LINE('end of loop');                                                     
    -- Rowise as PRICE_TYPE OR VALUE 
               
   END LOOP; -- end of quotes_rec Loop
   
  
  
  IF lb_quotes_rec THEN 
   
   dbms_output.PUT_LINE('Sec Quotes Found ');
   
--                dbms_output.Put_line (
--             'total Mid='||pCollection(pIndex).MID
--             ||'total Bid='||pCollection(pIndex).BID
--             ||'total ASK='||pCollection(pIndex).ASK
--             ||'-'||'tot hrs='||pCollection(pIndex).TOTAL_HOURS_PER_STRIP
--             ||'-'||'to Days='||pCollection(pIndex).CALENDAR_DAYS_IN_STRIP
--             ||'-TOTAL_MONTHS:='||  pCollection(pindex).TOTAL_TENOR_MONTHS
--             );
             
          pCollection(pIndex).SECONDARY_CALCULATION_FLAG := 'Y';
    
         CASE
          WHEN pCollection(pIndex).UNDERLYING_COMMODITY = 'ELECTRICITY' THEN 

            --HOur weighted Price 
            -- total Strip Price * totoal Hours/total Hours 
            
--             pBalance_Collectn(vn_rec_Index).Proj_loc_amt := 
--               ( pBalance_Collectn(vn_rec_Index).Proj_loc_amt * pBalance_Collectn(vn_rec_Index).TOTAL_HOURS_PER_STRIP )/NVL(pBalance_Collectn(vn_rec_Index).TOTAL_HOURS_PER_STRIP,1);   
             
            IF  pCollection(pIndex).MID is NOT NULL THEN                   
                pCollection(pIndex).MID := 
               ( pCollection(pIndex).MID )/NVL(pCollection(pIndex).TOTAL_HOURS_PER_STRIP,1);
            END IF;
                  
            IF  pCollection(pIndex).BID is NOT NULL THEN  
                pCollection(pIndex).BID := 
               ( pCollection(pIndex).BID )/NVL(pCollection(pIndex).TOTAL_HOURS_PER_STRIP,1);   
            END IF;
            
            IF  pCollection(pIndex).ASK is NOT NULL THEN  
                pCollection(pIndex).ASK := 
               ( pCollection(pIndex).ASK )/NVL(pCollection(pIndex).TOTAL_HOURS_PER_STRIP,1);   
            END IF;

         WHEN pCollection(pIndex).UNDERLYING_COMMODITY = 'OIL' THEN
           
            IF pCollection(pIndex).MID IS NOT NULL THEN   
            pCollection(pIndex).MID := 
                 pCollection(pIndex).MID/NVL(pCollection(pIndex).TOTAL_TENOR_MONTHS,1);
            END IF;
            
            IF pCollection(pIndex).BID IS NOT NULL THEN   
            pCollection(pIndex).BID := 
                 pCollection(pIndex).BID/NVL(pCollection(pIndex).TOTAL_TENOR_MONTHS,1);
            END IF;            
               
            IF pCollection(pIndex).ASK IS NOT NULL THEN   
            pCollection(pIndex).ASK := 
                 pCollection(pIndex).ASK/NVL(pCollection(pIndex).TOTAL_TENOR_MONTHS,1);
            END IF;            
           
        WHEN pCollection(pIndex).UNDERLYING_COMMODITY IN ( 'NATURAL GAS','GAS' , 'NG 6 MOAVG')        THEN
              -- Day weighted Price  
--              pBalance_Collectn(vn_rec_Index).Proj_loc_amt := 
--                  ( pBalance_Collectn(vn_rec_Index).Proj_loc_amt * pBalance_Collectn(vn_rec_Index).CALENDAR_DAYS_in_STRIP ) /NVL(pBalance_Collectn(vn_rec_Index).CALENDAR_DAYS_in_STRIP,1);
            IF pCollection(pIndex).MID is NOT NULL THEN  
              pCollection(pIndex).MID := 
                  ( pCollection(pIndex).MID) /NVL(pCollection(pIndex).CALENDAR_DAYS_in_STRIP,1);   
            END IF;
            IF pCollection(pIndex).BID is NOT NULL THEN  
              pCollection(pIndex).BID := 
                  ( pCollection(pIndex).BID) /NVL(pCollection(pIndex).CALENDAR_DAYS_in_STRIP,1);   
            END IF;
            
            IF pCollection(pIndex).ASK is NOT NULL THEN  
              pCollection(pIndex).ASK := 
                  ( pCollection(pIndex).ASK) /NVL(pCollection(pIndex).CALENDAR_DAYS_in_STRIP,1);   
            END IF;            
            

         WHEN pCollection(pIndex).UNDERLYING_COMMODITY = 'CAPACITY'    THEN
              
            IF pCollection(pIndex).MID IS NOT NULL THEN               
              pCollection(pIndex).MID := 
                    pCollection(pIndex).MID/NVL(pCollection(pIndex).TOTAL_TENOR_MONTHS,1);
            END IF;

            IF pCollection(pIndex).BID IS NOT NULL THEN               
              pCollection(pIndex).BID := 
                    pCollection(pIndex).BID/NVL(pCollection(pIndex).TOTAL_TENOR_MONTHS,1);
            END IF;            
               
            IF pCollection(pIndex).ASK IS NOT NULL THEN               
              pCollection(pIndex).ASK := 
                    pCollection(pIndex).ASK/NVL(pCollection(pIndex).TOTAL_TENOR_MONTHS,1);
            END IF;
                      
         WHEN pCollection(pIndex).UNDERLYING_COMMODITY = 'EMISSIONS'   THEN     
            
            IF pCollection(pIndex).MID IS NOT NULL THEN               
              pCollection(pIndex).MID := 
                    pCollection(pIndex).MID/NVL(pCollection(pIndex).TOTAL_TENOR_MONTHS,1);
            END IF;

            IF pCollection(pIndex).BID IS NOT NULL THEN               
              pCollection(pIndex).BID := 
                    pCollection(pIndex).BID/NVL(pCollection(pIndex).TOTAL_TENOR_MONTHS,1);
            END IF;            
               
            IF pCollection(pIndex).ASK IS NOT NULL THEN               
              pCollection(pIndex).ASK := 
                    pCollection(pIndex).ASK/NVL(pCollection(pIndex).TOTAL_TENOR_MONTHS,1);
            END IF;
            
          ELSE
            NULL; -- do nothing   
            
        END CASE;
   ELSE
     dbms_output.put_line('quote didnt found');         
   END IF;
   
   EXCEPTION 
   WHEN e_duplicate_record THEN 
   pout_rtn_code :=  c_failure;
   pout_rtn_msg  :='Duplicate Record found';
   
   WHEN E_MISSING_MONTHS  THEN
   pout_rtn_code :=  c_failure;
   pout_rtn_msg  :='Missing months';
   
END Derive_Secondary_calcs;

FUNCTION Get_granularity (
                          pcobdate                IN DATE
                          , pUnderlying_commodity IN VARCHAR2
                          , pPrice_vol_Indicator IN VARCHAR2
                          , ptenor_Months         IN NUMBER
                          ) 
               RETURN VARCHAR2
               RESULT_CACHE RELIES_ON ( RISKDB.QP_QUOTE_GRANULARITY ) 
is
/*
   Returns Granularity G for Granular
           Non Granularity 'NG' 
           Error E when ERROR/NO DATA FOUND in database 
*/
vc_Granular_condition       VARCHAR2(100);
vn_granular_maxVal          NUMBER;
vc_nonGranular_condition    VARCHAR2(100);
vn_nonGranular_maxval       NUMBER;
vCursor                     integer;
vc_Where_clause             VARCHAR2(1000);
vc_sql_stmt                 VARCHAR2(1000);
vb_continue                 Boolean := FALSE;
vc_Return_value             VARCHAR2(2);
vn_COnstant                 NUMBER;   
vn_ret_val                  INTEGER; 
vn_column_value             INTEGER;               
BEGIN

    --verify the given inputs are valid values
    BEGIN

    SELECT 
     Granular_condition
     , Granular_value
     , non_granular_condition
     , non_granular_value
     INTO 
     vc_Granular_condition       
    , vn_granular_maxVal          
    , vc_nonGranular_condition    
    , vn_nonGranular_maxval       
    FROM 
    RISKDB.QP_QUOTE_GRANULARITY g
    where
    g.Underlying_commodity = pUnderlying_commodity
    and g.PRICE_VOL_INDICATOR = pPrice_vol_Indicator 
    and g.active_flag = 'Y'
    and pcobdate between Effective_start_date and pcobdate
    ;

    vb_continue := TRUE; 

    EXCEPTION 
    WHEN NO_DATA_FOUND THEN 
      vc_return_value := 'E';  
    WHEN OTHERS THEN  
     vc_return_Value  :=  'E';
    END;

--- firrst check for Granular condition
IF vb_continue THEN 
    vCursor := dbms_sql.open_cursor;
ELSE
   Return vc_return_Value;
END IF ;
 
  -- configure Granular condition
   vc_where_clause := ptenor_Months 
                     || vc_Granular_condition 
                     ||vn_granular_maxVal ;

   vc_sql_stmt := 'SELECT '
                 ||' :vn_constant  from dual where '
                 ||vc_where_clause; 
   
   DBMS_SQL.PARSE(vCursor, vc_sql_stmt, DBMS_SQL.NATIVE);
   DBMS_SQL.DEFINE_COLUMN(Vcursor, 1, vn_constant );
   DBMS_SQL.BIND_VARIABLE(vCursor, ':vn_constant', 1 );
   vn_ret_val  := DBMS_SQL.EXECUTE(vCursor);
   
   vn_ret_val  := DBMS_SQL.FETCH_ROWS(vCursor); 
   
   DBMS_SQL.COLUMN_VALUE(vCursor, 1, vn_column_value );
--  ----dbms_output.put_line('After Dynamic SQL ');
   
 If NVL(vn_column_value  , 0) = 1 then 
    dbms_sql.CLOSE_CURSOR(vCursor);

    Return 'G';
 Else
     dbms_sql.CLOSE_CURSOR(vCursor);

     Return 'NG'; 
 END IF; 





END Get_granularity; 

 

   PROCEDURE Construct_m2m_object (
      pcobdate        IN            DATE,
      pmonthlym2m     IN OUT NOCOPY m2m_by_Loc_tab,
      pout_rtn_code      OUT NOCOPY NUMBER,
      pout_rtn_msg       OUT NOCOPY VARCHAR2)
   IS
      vc_message    VARCHAR2 (1000);
      vn_reccount   NUMBER;
      vc_cobdate    VARCHAR2 (12) := TO_CHAR (pcobdate, 'DD-MON-YYYY');
   BEGIN
      ------dbms_output.PUT_LINE ('Started msg here ');

      --Set Process Name and status for the Log Process
      PROCESS_LOG_REC.STAGE := 'Construct_Monthly_m2m_object';
      PROCESS_LOG_REC.STATUS := PROCESS_LOG_PKG.C_STATUS_STARTED;
      PROCESS_LOG_REC.MESSAGE :=
            'Construct_Monthly_m2m_object Started,  '
         || ' cobdate='
         || vc_cobdate;


      -- Log Message first
      write_log;

      -- ----dbms_output.PUT_LINE ('after write log ');

      BEGIN
         --- Limit Object construction only to a mapped Locations
         -- Data

         SELECT m2m_by_Loc_T (
                   COB_DATE                        => v.COB_DATE,
                   NETTING_GROUP_ID                => v.NETTING_GROUP_ID,
                   UNDERLYING_COMMODITY            => v.UNDERLYING_COMMODITY,
                   COMMODITY                       => v.COMMODITY,
                   LOCATION                        => v.LOCATION,
                   PRICE_VOL_INDICATOR             => v.PRICE_VOL_INDICATOR,
                   CONTRACTYEAR_MONTH              => v.CONTRACTYEAR_MONTH,
                   CONTRACT_YEAR                   => v.CONTRACT_YEAR,
                   CONTRACT_MONTH                  => v.CONTRACT_MONTH,
                   LOCATION_DELTA_POSITION         => v.LOCATION_DELTA_POSITION,
                   COMMODITY_ABSOLUTE_POSITION     => v.COMMODITY_ABSOLUTE_POSITION,
                   M2M_VALUE                       => v.M2M_VALUE,
                   LEGGED_M2M_VALUE                => v.LEGGED_M2M_VALUE,
                   ORIGINAL_FAS157_LEVEL           => v.ORIGINAL_FAS157_LEVEL,
                   ORIGINAL_FAS157_PERCENT         => v.ORIGINAL_FAS157_PERCENT,
                   FINAL_FAS157_VALUE              => v.FINAL_FAS157_VALUE,
                   FINAL_FAS157_TYPE               => v.FINAL_FAS157_TYPE,
                   PROJ_LOC_AMT                    => v.PROJ_LOC_AMT,
                   ON_OFF_PEAK_INDICATOR           => v.ON_OFF_PEAK_INDICATOR,
                   HOUR_TYPE                       => v.HOUR_TYPE,
                   TOT_HOURS_PER_CONTRACT_MONTH    => v.TOT_HOURS_PER_CONTRACT_MONTH,
                   CAL_DAYS_IN_CONTRACT_MONTH      => v.CAL_DAYS_IN_CONTRACT_MONTH,
                   INITIAL_RANK                    => v.INITIAL_RANK,
                   BASISPOINT_OVERRIDE_ID          => v.BASISPOINT_OVERRIDE_ID,
                   BASISPOINT_OVERRIDE_INDICATOR   => v.BASISPOINT_OVERRIDE_INDICATOR,
                   FAS157_OVERRIDE_INDICATOR       => v.FAS157_OVERRIDE_INDICATOR,
                   BASIS_CURVE_INDICATOR           => v.BASIS_CURVE_INDICATOR,
                   PHOENIX_BASIS_NAME              => v.PHOENIX_BASIS_NAME,
                   BASIS_QUOTE_DATE                => v.BASIS_QUOTE_DATE,
                   BASIS_QUOTE_AGE                 => v.BASIS_QUOTE_AGE,
                   TRADER_BASIS_QUOTE_PRICE        => v.TRADER_BASIS_QUOTE_PRICE,
                   USING_100PRCNT_HIST_METHOD      => v.USING_100PRCNT_HIST_METHOD,
                   EXT_PROVIDER_ID                 => v.EXT_PROVIDER_ID,
                   DATASOURCE_CATEGORY             => v.EXT_DATA_SOURCE_CATEGORY,
                   EXT_PROFILE_ID                  => v.EXT_PROFILE_ID,
                   EXT_LOCATION                    => v.EXTERNAL_COLUMN_VALUE,
                   PRICE_VOL_PROFILE               => v.PRICE_VOL_PROFILE,
                   ZEMA_PROFILE_ID                 => v.ZEMA_PROFILE_ID,
                   ZEMA_PROFILE_DISPLAY_NAME       => v.ZEMA_PROFILE_DISPLAY_NAME,
                   FULLY_VALIDATED_FLAG            => v.FULLY_VALIDATED_FLAG,
                   TERM_START                      => V.TERM_START,
                   TERM_END                        => V.TERM_END,
                   BASIS_ASK                       => V.BASIS_ASK,
                   BASIS_BID                       => V.BASIS_BID,
                   BROKER                          => V.BROKER,
                   MONTHLY_VOL                     => V.MONTHLY_VOL,
                   PROJ_LOCATION_AMT               => V.PROJ_LOCATION_AMT,
                   PROJ_PREMIUM_AMT                => V.PROJ_PREMIUM_AMT,
                   PROJ_BASIS_AMT                  => V.PROJ_BASIS_AMT,
                   MAX_CONTRACTYEAR_MONTH          => V.MAX_CONTRACTYEAR_MONTH,
                   MIN_CONTRACTYEAR_MONTH          => V.MIN_CONTRACTYEAR_MONTH,
                   TOT_CONTRACTYEAR_MONTH          => V.TOT_CONTRACTYEAR_MONTH,
                   Backbone_flag                   => V.BACKBONE_FLAG,
                   Internal_price                  => V.INTERNAL_PRICE,
                   ABSOLUTE_M2M_VALUE              => v.ABS_M2M_VALUE_AMT,
                   ABSOLUTE_LEGGED_M2M_VALUE       => v.ABS_M2M_LEG_VALUE_AMT,
                   ABSOLUTE_M2M_VALUE_VOL          => v.ABS_M2M_VALUE_AMT_VOL,
                   ABSOLUTE_LEGGED_M2M_VALUE_VOL   => v.ABS_M2M_LEG_VALUE_AMT_VOL ,
                   EXCEPTION_PRICE_FLAG            => 'N' , 
                   OVERRIDE_BASISPOINT             => NULL ,
                   OVERRIDE_COMMODITY              => NULL , 
                   OVERRIDE_HOUR_TYPE               => NULL ,
                   OVERRIDE_HOURS                   => NULL ,
                   OVERRIDE_PRICE                   => NULL ,
                   OVERRIDE_BACKBONE_FLAG           => NULL , 
                   OVERRIDE_ORIG_FAS_LEVEL          => NULL
                   )
           BULK COLLECT INTO pmonthlym2m
           FROM (WITH valuemap
                         AS (SELECT ep.EXT_PROVIDER_ID,
                                    ep.EXT_DATA_SOURCE_CATEGORY,
                                    epr.EXT_PROFILE_ID,
                                    NV.INTERNAL_COLUMN_VALUE,
                                    nv.EXTERNAL_COLUMN_VALUE,
                                    epr.PRICE_VOL_INDICATOR PRICE_VOL_PROFILE,
                                    epr.ZEMA_PROFILE_ID,
                                    epr.ZEMA_PROFILE_DISPLAY_NAME
                               FROM RISKDB.QP_EXT_NEER_VALUE_MAPPINGS nv,
                                    RISKDB.QP_EXT_PROFILES epr,
                                    RISKDB.QP_EXT_PROVIDERS ep
                              WHERE     nv.EXT_PROFILE_ID =
                                           epr.ext_profile_id
                                    --and epr.zema_profile_id = pzemaid --180
                                    AND epr.EXT_PROVIDER_ID =
                                           ep.EXT_PROVIDER_ID
                                    AND nv.active_flag = 'Y'
                                    AND epr.active_flag = 'Y'
                                    AND ep.ACTIVE_FLAG = 'Y'
                                    AND epr.SOURCE_SYSTEM = 'QVAR'
                                    -- and TO_DATE('04-MAY-2015','DD-MON-YYYY') Between nv.Effective_start_date and  NVL(nv.Effective_end_date, TO_DATE('04-MAY-2015','DD-MON-YYYY'))
                                    -- and TO_DATE('04-MAY-2015','DD-MON-YYYY') Between epr.Effective_start_date and  NVL(epr.Effective_end_date, TO_DATE('04-MAY-2015','DD-MON-YYYY'))
                                    -- and TO_DATE('04-MAY-2015','DD-MON-YYYY') Between ep.Effective_start_date and  NVL(ep.Effective_end_date, TO_DATE('04-MAY-2015','DD-MON-YYYY'))
                                    AND pcobdate BETWEEN nv.Effective_start_date
                                                     AND NVL (
                                                            nv.Effective_end_date,
                                                            pcobdate)
                                    AND pcobdate BETWEEN epr.Effective_start_date
                                                     AND NVL (
                                                            epr.Effective_end_date,
                                                            pcobdate)
                                    AND pcobdate BETWEEN ep.Effective_start_date
                                                     AND NVL (
                                                            ep.Effective_end_date,
                                                            pcobdate))
                 SELECT rd.COB_DATE,
                        rd.netting_group_id,
                        rd.UNDERLYING_COMMODITY,
                        rd.COMMODITY,
                        rd.LOCATION,
                        rd.PRICE_VOL_INDICATOR,
                        rd.CONTRACTYEAR_MONTH,
                        rd.CONTRACT_YEAR,
                        rd.CONTRACT_MONTH,
                        rd.LOCATION_DELTA_POSITION,
                        rd.COMMODITY_ABSOLUTE_POSITION,
                        rd.M2M_VALUE,
                        rd.LEGGED_M2M_VALUE,
                        rd.ORIGINAL_FAS157_LEVEL,
                        rd.ORIGINAL_FAS157_PERCENT,
                        rd.FINAL_FAS157_VALUE,
                        rd.FINAL_FAS157_TYPE,
                        rd.PROJ_LOC_AMT,
                        rd.ON_OFF_PEAK_INDICATOR,
                        rd.HOUR_TYPE,
                        rd.TOT_HOURS_PER_CONTRACT_MONTH,
                        rd.CAL_DAYS_IN_CONTRACT_MONTH,
                        rd.INITIAL_RANK,
                        rd.BASISPOINT_OVERRIDE_ID,
                        rd.BASISPOINT_OVERRIDE_INDICATOR,
                        rd.FAS157_OVERRIDE_INDICATOR,
                        rd.BASIS_CURVE_INDICATOR,
                        rd.PHOENIX_BASIS_NAME,
                        rd.BASIS_QUOTE_DATE,
                        rd.BASIS_QUOTE_AGE,
                        rd.TRADER_BASIS_QUOTE_PRICE,
                        rd.USING_100PRCNT_HIST_METHOD,
                        valuemap.EXT_PROVIDER_ID,
                        valuemap.EXT_DATA_SOURCE_CATEGORY,
                        valuemap.EXT_PROFILE_ID,
                        valuemap.EXTERNAL_COLUMN_VALUE,
                        valuemap.PRICE_VOL_PROFILE,
                        valuemap.ZEMA_PROFILE_ID,
                        valuemap.ZEMA_PROFILE_DISPLAY_NAME,
                        'N' FULLY_VALIDATED_FLAG,
                        rd.TERM_START,
                        rd.TERM_END,
                        rd.BASIS_ASK,
                        rd.BASIS_BID,
                        rd.BROKER,
                        rd.MONTHLY_VOL,
                        rd.PROJ_LOCATION_AMT,
                        rd.PROJ_PREMIUM_AMT,
                        rd.PROJ_BASIS_AMT,
                        rd.MAX_CONTRACTYEAR_MONTH,
                        rd.MIN_CONTRACTYEAR_MONTH,
                        rd.TOT_CONTRACTYEAR_MONTH,
                        rd.Backbone_flag,
                        rd.Internal_price,
                        rd.ABS_M2M_VALUE_AMT,
                        rd.ABS_M2M_LEG_VALUE_AMT,
                        rd.ABS_M2M_VALUE_AMT_VOL,
                        rd.ABS_M2M_LEG_VALUE_AMT_VOL ,
                        NULL  OVERRIDE_basispoint
                   FROM
                        RISKDB.QP_INT_FWD_CURVES_RAW_DATA rd
                        left outer join RISKDB.QP_BASIS_POINT_OVERRIDE bpo ON (
                        rd.Location = bpo.BASIS_POINT
                        and rd.price_vol_indicator = bpo.PRICE_VOL_INDICATOR 
                        and bpo.active_flag = 'Y'  
                        and pcobdate Between bpo.EFFECTIVE_START_DATE and NVL(bpo.EFFECTIVE_END_DATE,pcobdate )
                        )
                        LEFT OUTER JOIN valuemap
                           ON (    UPPER (rd.Location) =
                                      UPPER (valuemap.INTERNAL_COLUMN_VALUE)
                               AND rd.PRICE_VOL_INDICATOR =
                                      valuemap.PRICE_VOL_PROFILE)
                  WHERE     rd.cob_date = pcobdate --to_date('01/07/2015', 'MM/DD/YYYY')--
                        --        AND rd.Legged_M2M_VALUE <> 0
                        --        and DECODE( NVL(rd.Internal_price,-999999999), -999999999
                        --               , DECODE ( NVL(rd.INITIAL_RANK,-999999999),-999999999,-999999999, 1)
                        --               ,1) <> -999999999
                        AND rd.contractYear_Month >
                               TO_NUMBER (TO_CHAR (pcobdate, 'YYYYMM'))
                        --        and rd.LOCATION = 'TRANSCO/ZONE4'
                        --        and rd.PRICE_VOL_INDICATOR = 'VOL'
                        --        and rd.contractYear_Month =  '201601'
                        --        and valuemap.EXT_PROFILE_ID = 12
                        AND rd.active_flag = 'Y') v;

         SELECT COUNT (*) INTO vn_reccount FROM TABLE (pmonthlym2m);

         vc_message := vc_cobdate || '-total_recs->' || TO_CHAR (vn_reccount);

         DMR.PROCESS_LOG_PKG.WRITE_LOG (
            P_PROCESS_NAME   => PROCESS_LOG_REC.PROCESS_NAME,
            P_STAGE          => PROCESS_LOG_REC.STAGE,
            P_STATUS         => PROCESS_LOG_PKG.C_STATUS_INFO,
            P_MESSAGE        => vc_message);

         DBMS_OUTPUT.PUT_LINE (vc_message);

         pout_rtn_code := c_success;
         pout_rtn_msg := '';                              --Null means Success
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            pout_rtn_code := c_success;
            pout_rtn_msg := '';                           --Null means Success

            vc_message :=
               'There are no Records available for CobDAte = ' || vc_cobdate;

            ----dbms_output.PUT_LINE ( vc_message);

            DMR.PROCESS_LOG_PKG.WRITE_LOG (
               P_PROCESS_NAME   => PROCESS_LOG_REC.PROCESS_NAME,
               P_STAGE          => PROCESS_LOG_REC.STAGE,
               P_STATUS         => PROCESS_LOG_PKG.C_STATUS_INFO,
               P_MESSAGE        => vc_message);
      END;
   EXCEPTION
      WHEN OTHERS
      THEN
         vc_message := SUBSTR (SQLERRM, 1, 1000);

         pout_rtn_code := c_failure;
         pout_rtn_msg := vc_message;                      --Null means Success


         DMR.PROCESS_LOG_PKG.WRITE_LOG (
            P_PROCESS_NAME   => PROCESS_LOG_REC.PROCESS_NAME,
            P_STAGE          => PROCESS_LOG_REC.STAGE,
            P_STATUS         => PROCESS_LOG_PKG.C_STATUS_ERRORS,
            P_MESSAGE        => vc_message);
   END Construct_m2m_object;


   PROCEDURE Construct_Ext_quotes_object (
      pcobdate        IN            DATE,
      pmonthlym2m     IN OUT NOCOPY m2m_by_Loc_tab,
      pextquotes      IN OUT NOCOPY ext_quotes_tab,
      pout_rtn_code      OUT NOCOPY NUMBER,
      pout_rtn_msg       OUT NOCOPY VARCHAR2)
   IS
      vc_cobdate   VARCHAR2 (12) := TO_CHAR (pCOBDATE, 'DD-MON-YYYY');
   BEGIN
      PROCESS_LOG_REC.STAGE :=
         'Construct_Ext_quotes_object, ' || ' cobdate=' || vc_cobdate;

      BEGIN
         --- Limit Object construction only to a mapped Locations
         -- Data

         SELECT ext_quotes_T (
                   EXT_PROVIDER_QUOTE_ID        => eq.EXT_PROVIDER_QUOTE_ID,
                   EXT_PROVIDER_ID              => eq.EXT_PROVIDER_ID,
                   EXT_PROFILE_ID               => eq.EXT_PROFILE_ID,
                   UNDERLYING_COMMODITY         => eq.UNDERLYING_COMMODITY,
                   EXT_EFF_DATE                 => eq.EXT_EFF_DATE,
                   EXT_RAW_CONTRACT             => eq.EXT_RAW_CONTRACT,
                   MID                          => eq.MID,
                   BID                          => eq.BID,
                   ASK                          => eq.ASK,
                   STRIP_TENOR                  => eq.STRIP_TENOR,
                   CONTRACT_MONTH               => eq.CONTRACT_MONTH,
                   CONTRACT_YEAR                => eq.CONTRACT_YEAR,
                   EXT_CATEGORY                 => eq.EXT_CATEGORY,
                   EXT_PROVIDER                 => eq.EXT_PROVIDER,
                   EXT_LOCATION                 => eq.EXT_LOCATION,
                   NEER_HUB                     => eq.NEER_HUB,
                   NEER_LOCATION                => eq.NEER_LOCATION,
                   PEAK_TYPE                    => eq.PEAK_TYPE,
                   PROCESS_LEVEL                => eq.PROCESS_LEVEL,
                   EXT_HUB_MAPPED_FLAG          => eq.EXT_HUB_MAPPED_FLAG,
                   EXT_LOC_MAPPED_FLAG          => eq.EXT_LOC_MAPPED_FLAG,
                   NEER_LOC_MAPPED_FLAG         => eq.NEER_LOC_MAPPED_FLAG,
                   NEER_HUB_MAPPED_FLAG         => eq.NEER_HUB_MAPPED_FLAG,
                   STRIP_TENOR_MAPPED_FLAG      => eq.STRIP_TENOR_MAPPED_FLAG,
                   SECONDARY_CALCULATION_FLAG   => eq.SECONDARY_CALCULATION_FLAG,
                   ON_MID                       => eq.ON_MID,
                   OFF_MID                      => eq.OFF_MID,
                   ON_BID                       => eq.ON_BID,
                   OFF_BID                      => eq.OFF_BID,
                   ON_ASK                       => eq.ON_ASK,
                   OFF_ASK                      => eq.OFF_ASK,
                   PRICE_TYPE                   => eq.PRICE_TYPE,
                   VALUE                        => eq.VALUE,
                   HOUR_TYPE                    => eq.HOUR_TYPE)
           BULK COLLECT INTO pextquotes
           FROM (  SELECT DISTINCT ext_q.EXT_PROVIDER_QUOTE_ID,
                                   ext_q.EXT_PROVIDER_ID,
                                   ext_q.EXT_PROFILE_ID,
                                   ext_q.UNDERLYING_COMMODITY,
                                   ext_q.EXT_EFF_DATE,
                                   ext_q.EXT_RAW_CONTRACT,
                                   ext_q.MID,
                                   ext_q.BID,
                                   ext_q.ASK,
                                   ext_q.STRIP_TENOR,
                                   ext_q.CONTRACT_MONTH,
                                   ext_q.CONTRACT_YEAR,
                                   ep.EXT_DATA_SOURCE_CATEGORY EXT_CATEGORY,
                                   ep.EXT_PROVIDER EXT_PROVIDER,
                                   ext_q.EXT_LOCATION,
                                   NULL NEER_HUB,
                                   NULL NEER_LOCATION,
                                   NULL peaK_Type,
                                   ext_q.PROCESS_LEVEL,
                                   ext_q.EXT_HUB_MAPPED_FLAG,
                                   'Y' EXT_LOC_MAPPED_FLAG,
                                   'Y' NEER_LOC_MAPPED_FLAG,
                                   ext_q.NEER_HUB_MAPPED_FLAG,
                                   ext_q.STRIP_TENOR_MAPPED_FLAG,
                                   ext_q.SECONDARY_CALCULATION_FLAG,
                                   ext_q.ON_MID,
                                   ext_q.OFF_MID,
                                   ext_q.ON_BID,
                                   ext_q.OFF_BID,
                                   ext_q.ON_ASK,
                                   ext_q.OFF_ASK,
                                   ext_q.PRICE_TYPE,
                                   ext_q.VALUE,
                                   ext_q.DATA_STATUS,
                                   ext_q.COMMENTS,
                                   ext_q.ACTIVE_FLAG,
                                   ext_q.CREATE_USER,
                                   ext_q.CREATE_DATE,
                                   ext_q.MODIFY_USER,
                                   ext_q.MODIFY_DATE,
                                   ext_q.VERSION,
                                   NULL HOur_type
                     FROM RISKDB.QP_EXT_PROVIDER_QUOTES ext_q
                          INNER JOIN RISKDB.QP_EXT_NEER_VALUE_MAPPINGS nv
                             ON (UPPER (ext_q.EXT_Location) =
                                    UPPER (nv.EXTERNAL_COLUMN_VALUE))
                          INNER JOIN RISKDB.QP_EXT_PROFILES epr
                             ON (    NV.ext_profile_id = epr.EXT_PROFILE_ID
                                 AND epr.EXT_PROFILE_ID = ext_q.ext_profile_id-- and rd.PRICE_VOL_INDICATOR = epr.PRICE_VOL_INDICATOR
                                )
                          INNER JOIN RISKDB.QP_EXT_PROVIDERS ep
                             ON (epr.EXT_PROVIDER_ID = ep.EXT_PROVIDER_ID)
                    WHERE     ext_q.EXT_EFF_date = pcobdate --to_date('06/30/2015', 'MM/DD/YYYY')
                          AND epr.SOURCE_SYSTEM = 'QVAR'
                          --    and epr.Zema_profile_id = pzemaid
                          --    and rd.LOCATION = 'ERCOT-HB_HOUSTON-5x16'
                          AND nv.External_column_value IS NOT NULL
                          --    and epr.zema_profile_id = 181 -- limiting to one first to validate
                          --    and Upper(rd.location) Like Upper('NEPOOL-.H.INTERNAL_HUB-5x16%')
                          AND nv.active_flag = 'Y'
                          AND epr.active_flag = 'Y'
                          AND ep.active_flag = 'Y'
                          AND pcobdate BETWEEN nv.Effective_start_date
                                           AND NVL (nv.Effective_end_date,
                                                    pcobdate)
                          AND pcobdate BETWEEN epr.Effective_start_date
                                           AND NVL (epr.Effective_end_date,
                                                    pcobdate)
                          AND pcobdate BETWEEN ep.Effective_start_date
                                           AND NVL (ep.Effective_end_date,
                                                    pcobdate)
                 --        and to_date('06/30/2015', 'MM/DD/YYYY') Between nv.Effective_start_date and  NVL(nv.Effective_end_date, to_date('06/30/2015', 'MM/DD/YYYY'))
                 --        and to_date('06/30/2015', 'MM/DD/YYYY') Between epr.Effective_start_date and  NVL(epr.Effective_end_date, to_date('06/30/2015', 'MM/DD/YYYY'))
                 --        and to_date('06/30/2015', 'MM/DD/YYYY') Between ep.Effective_start_date and  NVL(ep.Effective_end_date, to_date('06/30/2015', 'MM/DD/YYYY'))
                 ORDER BY ext_q.EXT_LOCATION, TO_NUMBER (ext_q.CONTRACT_YEAR))
                eq;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            pout_rtn_code := c_failure;
            pout_rtn_msg :=
                  'There are no External Quotes available for CobDAte = '
               || TO_CHAR (pcobdate, 'DD-MON-YYYY');
            RAISE NO_DATA_FOUND;
      END;



      pout_rtn_code := c_success;
      pout_rtn_msg := '';                                 --Null means Success
   EXCEPTION
      WHEN OTHERS
      THEN
         pout_rtn_code := c_failure;
         pout_rtn_msg := NVL (pout_rtn_msg, SUBSTR (SQLERRM, 1, 200));
   END Construct_Ext_quotes_object;


Procedure    Compute_commodity_strip(
                      pindex                IN OUT NOCOPY NUMBER
                    , pOriginalLocation     IN VARCHAR2
                    , pOverrideLocation     IN VARCHAR2
                    , pbackbone_flag        IN VARCHAR2
                    , pOverridebb_flag      IN VARCHAR2  
                    , pCurrentMonth         IN NUMBER
                    , pstartYearMonth       IN NUMBER
                    , PEndYearMOnth         IN NUMBER
                    , pm2mData              IN OUT NOCOPY m2m_by_Loc_tab
                    , pcollection           IN OUT NOCOPY strips_collectn_t
                    , pcalcType             IN VARCHAR2
                    , pout_rtn_code         OUT  NOCOPY NUMBER    
                    , pout_rtn_msg          OUT  NOCOPY VARCHAR2 
                    )
IS

v_new_index         NUMBER;

 vn_rtn_code                    NUMBER;
 vc_rtn_msg                     VARCHAR2(500);
  e_Appl_error                   EXCEPTION;
   PRAGMA EXCEPTION_INIT( e_Appl_error , -20005);
  
BEGIN

-- PROCESS_LOG_REC.STAGE        := 'Compute_commodity_strip';
-- PROCESS_LOG_REC.MESSAGE      := 'Compute_commodity_strip Started start='
--                                  ||pcollection(pindex).NEER_COMMODITY
--                                  ||'-'
--                                  ||pcollection(pindex).NEER_LOCATION
--                                  ||pstartYearMonth 
--                                  ||' to '||pEndYearMonth;
-- write_log;

v_new_index := pindex + 1 ;

------dbms_output.Put_line('current index='||pindex
--                     ||'new index='||v_new_index);


-- extend collection by one element
pcollection.EXTEND;

--first assign new collection the previous member
--so all attributes associated with Location are inherited 
-- to current member element. 
pcollection(v_new_index) :=pcollection(pindex);



pcollection(v_new_index).COMMODITY_STRIP_FLAG := 'Y';

-- ranking could change as commodity level measures are recomputed
--hence clear location level ranking 
pcollection(v_new_index).COMMODITY_STRIP_FLAG := 'Y';

--if Rank inherited from Locaton level 
--and if rank 1 is derive for a location data then clear it
--as commodity curve may have different ABS Position 
IF pcollection(v_new_index).LOCATION_DELTA_POSITION = 0
and  pcollection(v_new_index).Curve_rank = 1 THEN 
   pcollection(v_new_index).Curve_rank := NULL;
   pcollection(v_new_index).Curve_Validation_Description:= NULL;
   pcollection(v_new_index).validated_flag := 'N';
   pcollection(v_new_index).Fully_validated_flag := 'N';
END IF ;

--assign Location level Values for this new index 
pcollection(v_new_index).TOTAL_HOURS_PER_STRIP  := pcollection(pindex).TOTAL_HOURS_PER_STRIP;
pcollection(v_new_index).CALENDAR_DAYS_IN_STRIP := pcollection(pindex).CALENDAR_DAYS_IN_STRIP;
                
------dbms_output.Put_line(PROCESS_LOG_REC.MESSAGE
--                     ||pcollection(v_new_index).COMMODITY_STRIP_FLAG
--                    );

      Derive_StripLevel_Measures(
                 pindex             =>  v_new_Index
               , pOriginalLocation  =>  pOriginalLocation
               , poverrideLocation  =>  pOverrideLocation  
               , pbackbone_flag     =>  pbackbone_flag
               , pOverridebb_flag   =>  pOverridebb_flag
               , pcUrrentMOnth      =>  pCurrentMonth
               , pstartYearMonth    =>  pstartYearMonth
               , PEndYearMOnth      =>  PEndYearMOnth
               , pm2mData           =>  pm2mData
               , pcollection        =>  pcollection
               , pcalcType          =>  'COMMODITY_LEVEL_SUM' 
               , pout_rtn_code      =>  vn_rtn_code    
               , pout_rtn_msg       =>  vc_rtn_msg 
               );
               
       Derive_StripLevel_Measures(
                 pindex             =>  v_new_Index
               , pOriginalLocation  =>  pOriginalLocation
               , poverrideLocation  =>  pOverrideLocation
               , pbackbone_flag     =>  pbackbone_flag
               , pOverridebb_flag   =>  pOverridebb_flag                  
               , pCurrentMonth      =>  pCurrentMonth
               , pstartYearMonth    =>  pStartYearMonth
               , PEndYearMOnth      =>  pEndYearMonth
               , pm2mData           =>  pm2mData
               , pcollection        =>  pCollection
               , pcalcType          =>  'COMMODITY_LEVEL_FAS'
               , pout_rtn_code      =>  vn_rtn_code    
               , pout_rtn_msg       =>  vc_rtn_msg 
        );        
               
     IF vn_rtn_code <> c_success THEN 
       RAISE e_appl_error;
     END IF;
   
   pout_rtn_code := c_success;
   pout_rtn_msg := ''; --Null means Success
   
   pindex := v_new_index;
   
Exception 
 WHEN OTHERS THEN 

       pout_rtn_code := c_failure;
       pout_rtn_msg := 'compute_commodity_strip '||SUBSTR(SQLERRM,1,200);
       
--       ----dbms_output.PUT_LINE( pout_rtn_msg);
          
END Compute_commodity_strip;


Procedure  Find_standard_tenor
   ( pcobdate      IN DATE 
    ,  pstripname  IN VARCHAR2
    , pstart_month IN OUT NOCOPY VARCHAR2
    , pend_month   IN OUT NOCOPY VARCHAR2
    , punderlyingCom IN VARCHAR2
    , ppricevolind    IN VARCHAR2
    , plocation    IN VARCHAR2
    , pmonthlym2m  IN OUT NOCOPY m2m_by_Loc_tab 
    , pout_rtn_code OUT NUMBER
    , pout_rtn_msg OUT VARCHAR2
    )
    
    is

    lb_tenor_found boolean := false;
    
    vc_CYYYY                VARCHAR2(10); -- CUrrent Year 
    vc_AYYYY                VARCHAR2(10); -- CUrrent Year   
    vc_AYYYYMM               VARCHAR2(10); -- strip Year and Month
  

    
    v_stripTenor            RISKDB.QP_EXT_PROVIDER_QUOTES.STRIP_TENOR%TYPE;
    
    vc_CurrentMonth          VARCHAR2(6); -- cob date's Month
    vc_PromptMonth           VARCHAR2(6); -- cob date's next month
    v_underlying_commodity  RISKDB.QP_STRIP_LVL_NEER_EXT_DATA.UNDERLYING_COMMODITY%TYPE;
    v_price_vol_indicator   RISKDB.QP_STRIP_LVL_NEER_EXT_DATA.PRICE_VOL_INDICATOR%TYPE; 
    
    VC_STARTCYYYYMM          VARCHAR2(6); --variable for CURRENT YEAR START MONTH for pstart_month to pass values into Manual Quotes
    VC_ENDCYYYYMM            VARCHAR2(6); --variable for CURRENT YEAR END MONTH
    
    VC_STARTAYYYYMM          VARCHAR2(6); --variable for ALL YEAR START MONTH
    VC_ENDAYYYYMM            VARCHAR2(6); --variable for ALL YEAR END MONTH
-- Define Monthly Tenor strips 
--Monthly Strips
Cursor cur_tenor is
Select 
tenor.TENOR_ID
,tenor.UNDERLYING_COMMODITY
,tenor.STRIP_NAME
,tenor.START_MONTH
,tenor.END_MONTH
,tenor.ADD_MONTHS
,tenor.PRICE_VOL_INDICATOR
,tenor.AGGREGATION_TYPE
,tenor.BALANCE_FLAG
,tenor.LITERAL_DISPLAY
,tenor.EFFECTIVE_START_DATE
,tenor.EFFECTIVE_END_DATE
,tenor.ACTIVE_FLAG
from 
RISKDB.QP_TENOR_MASTER tenor
where
 Upper(tenor.Strip_name) = Upper(pstripname)
 and PRICE_VOL_INDICATOR = ppricevolind
 and Underlying_commodity = punderlyingCom
 and active_flag = 'Y' 
  --and Balance_flag = 'N'
  and trunc(pcobdate) between 
            trunc(Effective_start_date) and 
            NVL(effective_end_date , pcobdate )
order by 
 tenor.Strip_name
 , tenor.price_vol_indicator
;

Cursor  cur_mly_contract_years is
SELECT
  MIN(internal.CONTRACT_YEAR) Start_year
, MAX(internal.CONTRACT_YEAR) End_year  
FROM
  TABLE( pmonthlym2m ) internal
where
 To_Number(internal.CONTRACTYEAR_MONTH) > to_number(vc_CurrentMonth) 
 and internal.UNDERLYING_COMMODITY = v_underlying_commodity
 and internal.PRICE_VOL_INDICATOR  = v_price_vol_indicator 
 and internal.LOCATION = plocation
; 


Cursor  cur_contract_years is
SELECT DISTINCT
internal.contract_year
FROM
  TABLE( pmonthlym2m ) internal
where
 To_Number(internal.CONTRACTYEAR_MONTH) > to_number(vc_CurrentMonth) 
 and internal.UNDERLYING_COMMODITY = v_underlying_commodity
 and internal.PRICE_VOL_INDICATOR  = v_price_vol_indicator 
 and internal.LOCATION = plocation
; 


   begin
   
   PROCESS_LOG_REC.STAGE        := 'Find_standard_tenor';
   
   vc_CurrentMonth := to_char(pcobdate , 'YYYYMM');
   vc_CYYYY := to_char(pcobdate , 'YYYY');
   vc_PromptMonth  := TO_CHAR(ADD_MONTHS(pcobdate,1), 'YYYYMM');
   
   for strips in cur_tenor
   loop
   lb_tenor_found := true;
   
   v_underlying_commodity := strips.UNDERLYING_COMMODITY;
   v_price_vol_indicator  := strips.price_vol_indicator;
   
     
   case 
   when strips.START_MONTH = strips.END_MONTH
        then 
       
   FOr contrct_yrs in cur_mly_contract_years
   LOOP
     
      pstart_month :=  contrct_yrs.Start_year;
      pend_month   :=  contrct_yrs.End_year;
      
   end loop;---end of Contract_years loop
           
   when strips.BALANCE_FLAG = 'Y'
        then 
      
        
       IF (   
           ( INSTR(Upper(strips.Start_month), 'MAX(CYYYYF01,PROMPTMONTH)',1,1) > 0 )
           AND ( INSTR(Upper(strips.END_month), 'CYYYYF12',1,1) > 0 
                  )
          ) then  
        
       
          Vc_STARTCYYYYMM := vc_PromptMonth;
          
                         
          Vc_ENDCYYYYMM := REPLACE(strips.END_month, 'CYYYYF', vc_CYYYY  );

--          ----dbms_output.PUT_LINE('Range is'||Vc_StartCYYYYMM||' to '||Vc_ENDCYYYYMM );
      
       ELSIF (  
             ( INSTR(Upper(strips.Start_month), 'MAX(CYYYYF06,PROMPTMONTH)',1,1) > 0 ) 
             AND ( NVL(strips.ADD_MONTHS, 0 ) > 0  )
             ) then  
          
             
            IF  TO_NUMBER( REPLACE ('CYYYYF06', 'CYYYYF', vc_CYYYY )) > to_Number(vc_CurrentMonth) THEN 
                  -- means strip not yet started 
                  VC_STARTCYYYYMM := REPLACE ('CYYYYF06', 'CYYYYF', vc_CYYYY );
                  
                  VC_ENDCYYYYMM := TO_CHAR(ADD_MONTHS(TO_DATE(vC_startCYYYYMM||'01', 'YYYYMMDD') 
                                            ,  strips.ADD_MONTHS 
                                            )
                                         , 'YYYYMM') ;
                  
            ELSE
                  ---   strip starting time could be between this year or prompt Year
                                
                  VC_STARTCYYYYMM := vc_PromptMonth;
                  VC_ENDCYYYYMM := TO_CHAR(ADD_MONTHS( TO_DATE(REPLACE ('CYYYYF06', 'CYYYYF', vc_CYYYY )||'01', 'YYYYMMDD') 
                                                      ,  strips.ADD_MONTHS 
                                                      )
                                           , 'YYYYMM'
                                         ) ;
            END IF; 
                  
                      
--           ----dbms_output.PUT_LINE('Range is'||VC_StartCYYYYMM||' to '||VC_ENDCYYYYMM );
       ELSIF ( 
             ( INSTR(Upper(strips.Start_month), 'MAX(CYYYYF04',1,1) > 0 ) 
             AND ( INSTR(strips.END_MONTH, 'CYYYYF10',1,1 ) > 0 ) 
             ) then        
       
       
       
             IF  TO_NUMBER( REPLACE ('CYYYYF04', 'CYYYYF', vc_CYYYY )) > to_number(vc_CurrentMonth) THEN 
                  -- means strip not yet started 
                  VC_STARTCYYYYMM := REPLACE ('CYYYYF04', 'CYYYYF', vc_CYYYY );
                  
                  
                  VC_ENDCYYYYMM :=  REPLACE(strips.END_MONTH,'CYYYYF', vc_CYYYY );
                  
            ELSE
                                                  
                  VC_STARTCYYYYMM := vc_PromptMonth;
                  VC_ENDCYYYYMM :=  REPLACE(strips.END_MONTH,'CYYYYF', vc_CYYYY );
                  
            END IF;
            
--            ----dbms_output.PUT_LINE('Range is'||VC_StartCYYYYMM||' to '||VC_ENDCYYYYMM );
            
       ELSIF ( 
               ( INSTR(Upper(strips.Start_month), 'MAX(CYYYYF11',1,1) > 0 ) 
               AND NVL(strips.ADD_MONTHS, 0 ) > 0 
             ) then        
       
             IF  TO_NUMBER( REPLACE ('CYYYYF11', 'CYYYYF', vc_CYYYY )) >= to_number(vc_CurrentMonth) THEN 
                  -- means strip not yet started 
                  VC_STARTCYYYYMM := REPLACE ('CYYYYF11', 'CYYYYF', vc_CYYYY );
                  
                  
                   VC_ENDCYYYYMM := TO_CHAR(ADD_MONTHS(TO_DATE(vC_startCYYYYMM||'01', 'YYYYMMDD') 
                                            ,  strips.ADD_MONTHS 
                                            )
                                         , 'YYYYMM') ;

                  
            ELSE
                                                  
                  VC_STARTCYYYYMM := vc_PromptMonth;
                  VC_ENDCYYYYMM := TO_CHAR(ADD_MONTHS(TO_DATE( REPLACE ('CYYYYF11', 'CYYYYF', vc_CYYYY )
                                                              ||'01' , 'YYYYMMDD'
                                                            ) 
                                                    ,  strips.ADD_MONTHS 
                                                   )
                                         , 'YYYYMM'
                                         ) ;

                  
            END IF;
            
--           ----dbms_output.PUT_LINE('Range is'||VC_StartCYYYYMM||' to '||VC_ENDCYYYYMM );
       
       END IF;
         
       pstart_month :=  VC_StartCYYYYMM;
       pend_month   :=  VC_ENDCYYYYMM ;    
        
   else --to identify non-balance strips
    
    
                
   FOr contrct_yrs in cur_mly_contract_years
   LOOP
     
      pstart_month :=  contrct_yrs.Start_year;
      pend_month   :=  contrct_yrs.End_year;
      
      
   
   end loop;---end of Contract_years loop
    
   end case;
   
   end loop;--end of Tenor loop
   
   if not lb_tenor_found
   then 
       pout_rtn_code := c_success;
       pout_rtn_msg := ''; --if tenor strip_name not found
       
       pstart_month := NULL;
       pend_month  := NULL;
   else
       pout_rtn_code := c_success;
       pout_rtn_msg := ''; 
   
   end if;
   
   end  Find_standard_tenor;


Procedure  Prepare_manual_qts(
                              pcobdate IN DATE
                            , pmonthlym2m       IN OUT NOCOPY m2m_by_Loc_tab 
                            , pmanual_quote_id  IN NUMBER
                            , pstrip_tenor      IN VARCHAR2
                            , pextCategory      IN VARCHAR2
                            , pDatasourceName   IN VARCHAR2                            
                            , pMID              IN NUMBER
                            , PBID              IN NUMBER
                            , PASK              IN NUMBER
                            , punderlyingcom    IN VARCHAR2
                            , ppricevolind      IN VARCHAR2
                            , plocation         IN VARCHAR2
                            , pcurrentMonth     IN VARCHAR2 
                            , pstart_Month      IN VARCHAR2
                            , pend_month        IN VARCHAR2
                            , pmnthly_collectn   IN OUT NOCOPY strips_collectn_t
                            , pout_rtn_code  OUT  NOCOPY NUMBER    
                            , pout_rtn_msg   OUT  NOCOPY VARCHAR2
                            )
Is

v_delta_price_deno      NUMBER;

v_contractYear          RISKDB.QP_EXT_PROVIDER_QUOTES.CONTRACT_YEAR%TYPE;
li_rec_Index            NUMBER := 0;


vc_return_val           VARCHAR2(2);

 
Cursor cur_m2m  is
SELECT DISTINCT
  m2m.COB_DATE                                   
 , m2m.NETTING_GROUP_ID                 
 , m2m.UNDERLYING_COMMODITY            
 , m2m.COMMODITY                       
 , m2m.LOCATION                      
 , m2m.PRICE_VOL_INDICATOR            
-- , m2m.CONTRACTYEAR_MONTH              
 , m2m.CONTRACT_YEAR                  
-- , m2m.CONTRACT_MONTH                 
-- ,  m2m.LOCATION_DELTA_POSITION        
-- , m2m.COMMODITY_ABSOLUTE_POSITION    
-- , m2m.M2M_VALUE                      
-- , m2m.LEGGED_M2M_VALUE            
-- , m2m.ORIGINAL_FAS157_LEVEL          
-- , m2m.ORIGINAL_FAS157_PERCENT        
-- , m2m.FINAL_FAS157_VALUE            
-- , m2m.FINAL_FAS157_TYPE              
-- , m2m.PROJ_LOC_AMT                   
 , m2m.ON_OFF_PEAK_INDICATOR          
 , m2m.HOUR_TYPE                     
-- , m2m.TOT_HOURS_PER_CONTRACT_MONTH   
-- , m2m.CAL_DAYS_IN_CONTRACT_MONTH     
 , m2m.INITIAL_RANK                   
 , m2m.BASISPOINT_OVERRIDE_ID         
 , m2m.BASISPOINT_OVERRIDE_INDICATOR  
-- , m2m.FAS157_OVERRIDE_INDICATOR      
-- , m2m.BASIS_CURVE_INDICATOR           
-- , m2m.PHOENIX_BASIS_NAME             
-- , m2m.BASIS_QUOTE_DATE               
-- , m2m.BASIS_QUOTE_AGE                
-- , m2m.TRADER_BASIS_QUOTE_PRICE       
-- , m2m.USING_100PRCNT_HIST_METHOD     
 , NULL EXT_PROVIDER_ID                  
 , NULL DATASOURCE_CATEGORY                         
 , NULL EXT_PROFILE_ID                     
 , NULL EXT_LOCATION                   
 , NULL PRICE_VOL_PROFILE               
 , NULL ZEMA_PROFILE_ID                   
 , NULL ZEMA_PROFILE_DISPLAY_NAME  
 , m2m.MAX_CONTRACTYEAR_MONTH      
 , m2m.MIN_CONTRACTYEAR_MONTH       
 , m2m.TOT_CONTRACTYEAR_MONTH   
FROM 
TABLE(pmonthlym2m) m2m
where
m2m.Underlying_commodity = punderlyingcom
and m2m.price_vol_indicator = ppricevolind
and m2m.LOCATION = plocation 
and to_number(m2m.CONTRACTYEAR_MONTH)  > to_Number(pCurrentMonth)
--and m2m.CONTRACT_YEAR IN (  TO_NUMBER(SUBSTR(vC_StartCYYYYMM,1,4) , to_NUMBER(vC_ENDCYYYYMM,1,4) )   -- for current Year ONLY
and to_number(m2m.contractYear_month) Between  to_Number(pStart_month) 
                                         AND   to_Number(pend_month)
--and ( NVL(m2m.LOCATION_DELTA_POSITION , 0)  <> 0
--      OR NVL(m2m.LEGGED_M2M_VALUE, 0) <> 0
--    )                                          
Order by 
   m2m.LOCATION                      
 , m2m.PRICE_VOL_INDICATOR
 , m2m.NETTING_GROUP_ID                 
 , m2m.UNDERLYING_COMMODITY 
 ;
 

vn_rtn_code             NUMBER;
vc_rtn_msg              VARCHAR2(1000);

v_current_Location      VARCHAR2(1000) := ' ';
v_Previous_Location      VARCHAR2(1000) := ' ';
v_current_flat_pos      VARCHAR2(1);

v_max_contractyr        PLS_INTEGER;
v_min_contractYr        PLS_INTEGER;
v_totContractMonths     PLS_INTEGER;

PROCEDURE Get_MAX_MIN_CONTRACTYR ( 
                                  pindex             IN NUMBER
                                , pLocation          IN VARCHAR2 
                                , pout_rtn_code      OUT  NOCOPY NUMBER  
                                , pout_rtn_msg       OUT  NOCOPY VARCHAR2
                               )
IS
 vn_rtn_code                    NUMBER;
 vc_rtn_msg                     VARCHAR2(500);
BEGIN

   SELECT 
   MAX(TO_NUMBER(contractYear_month))
   , MIN(TO_NUMBER(contractYear_month))
   INTO 
   pmnthly_collectn(pindex).MAX_CONTRACTYEAR_MONTH
   ,  pmnthly_collectn(pindex).MIN_CONTRACTYEAR_MONTH
   FROM 
   RISKDB.QP_INT_FWD_Curves_raw_data m2m
   WHERE
   COB_DATE =  pmnthly_collectn(pindex).COB_DATE
   AND LOCATION =  pmnthly_collectn(pindex).NEER_LOCATION
  -- AND LOCATION_DELTA_POSITION <> 0
   AND ACTIVE_FLAG = 'Y'
   ;
   
   
 IF pmnthly_collectn(pindex).max_ContractYEAR_Month IS NOT NULL 
     AND  pmnthly_collectn(pindex).min_ContractYEAR_Month IS NOT NULL 
  THEN  
                     
     pmnthly_collectn(pindex).tot_ContractYEAR_Month := 
        MOnths_between( to_date( pmnthly_collectn(pindex).max_ContractYEAR_Month||'01', 'YYYYMMDD') 
                       ,  to_date( pmnthly_collectn(pindex).min_ContractYEAR_Month||'01', 'YYYYMMDD')
                      ) + 1;                    
 ELSE
     pmnthly_collectn(pindex).tot_ContractYEAR_Month := NULL;
 END IF ;   
 
   pout_rtn_code := c_success;
   pout_rtn_msg := ''; --Null means Success
   

EXCEPTION 
WHEN NO_DATA_FOUND THEN 
  pmnthly_collectn(pindex).MAX_CONTRACTYEAR_MONTH := NULL;
 pmnthly_collectn(pindex).MIN_CONTRACTYEAR_MONTH := NULL;
WHEN OTHERS THEN 
   pmnthly_collectn(pindex).MAX_CONTRACTYEAR_MONTH := NULL;
   pmnthly_collectn(pindex).MIN_CONTRACTYEAR_MONTH := NULL;
END Get_MAX_MIN_CONTRACTYR;

 
BEGIN

  PROCESS_LOG_REC.STAGE        := 'Prepare_manual_qts';
    
  PROCESS_LOG_REC.MESSAGE      := 'Prepare_manual_qts Started Location='||plocation ;
        
   write_log;
   
   
   
   For m2m_recs in cur_m2m LOOP

               vc_rtn_msg := '';--reset 
              
               Validate_contract_Data (
                 pcobdate           => pcobdate
               , pLocation          => plocation
               , pnettingGroup      => m2m_recs.NETTING_GROUP_ID               
               , pstartYearMonth    => pstart_Month
               , pendYearMonth      => pend_month
               , PpricevolInd       => ppricevolind
               , pout_rtn_code      =>  vn_rtn_code    
               , pout_rtn_msg       =>  vc_rtn_msg     
               );
               
               
              IF vc_rtn_msg = 'STOP' THEN 
               
                 CONTINUE; -- go to next m2m Record , no sufficient data to support strip
              ELSE
                 vc_rtn_msg := ''; --Data is available continue as usual  
              END IF; 
              
   --columnwise in ON_BID/OFF_BID
   li_rec_Index := pmnthly_collectn.COUNT + 1 ;

    pmnthly_collectn.EXTEND; 
    pmnthly_collectn(li_rec_Index).MONTHLY_STRIP_FLAG := 'N'; 
    pmnthly_collectn(li_rec_Index).BALANCED_STRIP_FLAG := 'N';
    pmnthly_collectn(li_rec_Index).NONBALANCED_STRIP_FLAG := 'N';
    pmnthly_collectn(li_rec_Index).COMMODITY_STRIP_FLAG := 'N';                 
    pmnthly_collectn(li_rec_Index).SECONDARY_CALCULATION_FLAG := 'N'; 
    pmnthly_collectn(li_rec_Index).MANUAL_QUOTE_FLAG := 'Y';
    pmnthly_collectn(li_rec_Index).FULLY_VALIDATED_FLAG := 'N';                
    pmnthly_collectn(li_rec_Index).VALIDATED_FLAG := 'N';    
    pmnthly_collectn(li_rec_Index).ACTIVE_FLAG := 'Y';
    pmnthly_collectn(li_rec_Index).STRIP_TENOR := pstrip_tenor;
  
   --collect m2m info
   pmnthly_collectn(li_rec_Index).COB_DATE 
                                        := m2m_recs.COB_DATE;       
                                                 
   pmnthly_collectn(li_rec_Index).NETTING_GROUP_ID  
                                        := m2m_recs.NETTING_GROUP_ID ;
                                        
    pmnthly_collectn(li_rec_Index).UNDERLYING_COMMODITY
                                        := m2m_recs.UNDERLYING_COMMODITY;      
        
    pmnthly_collectn(li_rec_Index).NEER_COMMODITY  
                                        := m2m_recs.COMMODITY; 
                                                 
    pmnthly_collectn(li_rec_Index).NEER_LOCATION   
                            := m2m_recs.LOcation  ;
    
    pmnthly_collectn(li_rec_Index).PRICE_VOL_INDICATOR  
                    := m2m_recs.PRICE_VOL_INDICATOR ; 
   
    pmnthly_collectn(li_rec_Index).CONTRACT_YEAR := m2m_recs.contract_year;
                                                                 
                                    
--    pmnthly_collectn(li_rec_Index).FAS157_OVERRIDE_INDICATOR   
--                       := m2m_recs.FAS157_OVERRIDE_INDICATOR ;
--                                       
    pmnthly_collectn(li_rec_Index).ON_OFF_PEAK_INDICATOR   
                       := m2m_recs.ON_OFF_PEAK_INDICATOR;  
                                         
    pmnthly_collectn(li_rec_Index).HOUR_TYPE 
                       := m2m_recs.HOUR_TYPE;      

    pmnthly_collectn(li_rec_Index).DATASOURCE_CATEGORY := pextCategory;
    pmnthly_collectn(li_rec_Index).DATA_SOURCE_NAME := pDatasourceName;
   
--   ----dbms_output.Put_line('*****');
                    
   If pstart_Month = pend_Month then 
  
 --get basis information from stage 2 table
   
       BEGIN
       
        SELECT  DISTINCT
           m2m.CONTRACT_MONTH 
         , m2m.BASIS_CURVE_INDICATOR           
         , m2m.PHOENIX_BASIS_NAME             
         , m2m.BASIS_QUOTE_DATE               
         , m2m.BASIS_QUOTE_AGE                
         , m2m.TRADER_BASIS_QUOTE_PRICE       
         , NVL(m2m.USING_100PRCNT_HIST_METHOD, 'N') USING_100PRCNT_HIST_METHOD
         , m2m.TERM_START        
         , m2m.TERM_END               
         , m2m.BASIS_ASK              
         , m2m.BASIS_BID              
         , m2m.BROKER  
         , m2m.proj_loc_amt   
       INTO 
       pmnthly_collectn(li_rec_Index).CONTRACT_MONTH
       , pmnthly_collectn(li_rec_Index).BASIS_CURVE_INDICATOR
       , pmnthly_collectn(li_rec_Index).PHOENIX_BASIS_NAME
       , pmnthly_collectn(li_rec_Index).BASIS_QUOTE_DATE
       , pmnthly_collectn(li_rec_Index).BASIS_QUOTE_AGE
       , pmnthly_collectn(li_rec_Index).TRADER_BASIS_QUOTE_PRICE
       , pmnthly_collectn(li_rec_Index).USING_100PRCNT_HIST_METHOD
       , pmnthly_collectn(li_rec_Index).TERM_START
       , pmnthly_collectn(li_rec_Index).TERM_END
       , pmnthly_collectn(li_rec_Index).BASIS_ASK
       , pmnthly_collectn(li_rec_Index).BASIS_BID
       , pmnthly_collectn(li_rec_Index).BROKER
       , pmnthly_collectn(li_rec_Index).Proj_loc_amt
        FROM
        TABLE(pmonthlym2m) m2m 
       WHERE
       NETTING_GROUP_ID =  pmnthly_collectn(li_rec_Index).NETTING_GROUP_ID
       AND UNDERLYING_COMMODITY  = pmnthly_collectn(li_rec_Index).UNDERLYING_COMMODITY
       AND COB_DATE = m2m_recs.COB_DATE
       AND COMMODITY = m2m_recs.COMMODITY
       AND LOCATION = m2m_recs.LOcation
       AND PRICE_VOL_INDICATOR = m2m_recs.PRICE_VOL_INDICATOR
       AND CONTRACTYEAR_MONTH  = pstart_Month;
       
       EXCEPTION
       WHEN NO_DATA_FOUND THEN 
         null;
       END;
   
   else 
   
                 
                pmnthly_collectn(li_rec_Index).BASIS_CURVE_INDICATOR
                                := 'N'; --m2m_recs.BASIS_CURVE_INDICATOR; 
                                         
                pmnthly_collectn(li_rec_Index).PHOENIX_BASIS_NAME 
                                := NULL; --m2m_recs.PHOENIX_BASIS_NAME;    
                                        
                pmnthly_collectn(li_rec_Index).BASIS_QUOTE_DATE  
                                := NULL; --m2m_recs.BASIS_QUOTE_DATE;    
                                         
                pmnthly_collectn(li_rec_Index).BASIS_QUOTE_AGE 
                                := NULL; --m2m_recs.BASIS_QUOTE_AGE;  
                                                 
                pmnthly_collectn(li_rec_Index).TRADER_BASIS_QUOTE_PRICE  
                                := NULL; --m2m_recs.TRADER_BASIS_QUOTE_PRICE;  
                                    
                pmnthly_collectn(li_rec_Index).USING_100PRCNT_HIST_METHOD 
                                := 'N'; --m2m_recs.USING_100PRCNT_HIST_METHOD; 
                pmnthly_collectn(li_rec_Index).TERM_START := NULL;
                pmnthly_collectn(li_rec_Index).TERM_END  := NULL;
                pmnthly_collectn(li_rec_Index).BASIS_ASK := NULL;
                pmnthly_collectn(li_rec_Index).BASIS_BID := NULL;
                pmnthly_collectn(li_rec_Index).BROKER    := NULL;            
   
   end if;
   
    pmnthly_collectn(li_rec_Index).Curve_rank  
                                  := m2m_recs.INITIAL_RANK; 
                                  
    pmnthly_collectn(li_rec_Index).BASISPOINT_OVERRIDE_ID    
                            := m2m_recs.BASISPOINT_OVERRIDE_ID ;
                                    
    pmnthly_collectn(li_rec_Index).BASISPOINT_OVERRIDE_INDICATOR  
                          := m2m_recs.BASISPOINT_OVERRIDE_INDICATOR;

                 
  --  v_ext_location          := NULL; -- DUMMY VALUE 
    pmnthly_collectn(li_rec_Index).EXT_LOCATION 
                            := NULL;
  --  v_ext_Profile_id        := NULL ;  
    pmnthly_collectn(li_rec_Index).EXT_PROFILE_ID  
                            :=-1;
               
    pmnthly_collectn(li_rec_Index).PRICE_VOL_PROFILE    
                            := NULL ; 
    pmnthly_collectn(li_rec_Index).ZEMA_PROFILE_ID 
                            := NULL;
    pmnthly_collectn(li_rec_Index).ZEMA_PROFILE_DISPLAY_NAME 
                            := NULL;            
  --  v_ext_provider_id       := NULL;

    pmnthly_collectn(li_rec_Index).ext_Provider_id 
                            := -1; 
   
    pmnthly_collectn(li_rec_Index).EXT_EFF_DATE  
                            := m2m_recs.COB_DATE;                                         
                          
    pmnthly_collectn(li_rec_Index).STRIP_START_YEARMONTH := 
                                         pstart_Month;  
                                                    
    pmnthly_collectn(li_rec_Index).STRIP_END_YEARMONTH   :=    
                                         pend_month; 
               
    pmnthly_collectn(li_rec_Index).TOTAL_TENOR_MONTHS    := 
             MOnths_between( to_date(pend_month||'01', 'YYYYMMDD') 
                             ,  to_date(pstart_Month||'01', 'YYYYMMDD')
                            ) + 1;    

     vn_rtn_code := NULL;
                vc_rtn_msg := '';
              
---  calculate aggregate m2m amounts
--     ----dbms_output.put_line('loc lvl sum Derive strip measures ');      
             Derive_StripLevel_Measures(
                 pindex             =>  li_rec_Index
               , pOriginalLocation  =>  pmnthly_collectn(li_rec_Index).NEER_LOCATION
               , pOverrideLocation  =>  NULL  
               , pbackbone_flag     =>  NULL
               , pOverridebb_flag   =>  NULL              
               , pcUrrentMOnth      =>  TO_NUMBER(pcurrentMonth)
               , pstartYearMonth    =>  TO_NUMBER(pstart_Month)
               , PEndYearMOnth      =>  TO_NUMBER(pend_month)
               , pm2mData           =>  pmonthlym2m
               , pcollection        =>  pmnthly_Collectn
               , pcalcType          =>  'LOCATION_LEVEL_M2M' 
               , pout_rtn_code      =>  vn_rtn_code    
               , pout_rtn_msg       =>  vc_rtn_msg 
               );
           

                Derive_StripLevel_Measures(
                 pindex             =>  li_rec_Index
               , pOriginalLocation  =>  pmnthly_collectn(li_rec_Index).NEER_LOCATION
               , pOverrideLocation  =>  pmnthly_collectn(li_rec_Index).NEER_LOCATION  
               , pbackbone_flag     =>  NULL
               , pOverridebb_flag   =>  NULL                                 
               , pcUrrentMOnth      =>  TO_NUMBER(pcurrentMonth)
               , pstartYearMonth    =>  TO_NUMBER(pstart_Month)
               , PEndYearMOnth      =>  TO_NUMBER(pend_month)
               , pm2mData           =>  pmonthlym2m
               , pcollection        =>  pmnthly_Collectn
               , pcalcType          =>  'LOCATION_PRICE' 
               , pout_rtn_code      =>  vn_rtn_code    
               , pout_rtn_msg       =>  vc_rtn_msg 
               );
                   
 --     dbms_output.put_line('after lvl sum Derive strip measures '||pmnthly_collectn(li_rec_Index).proj_loc_amt); 
      
 
     IF  pstart_Month <> pend_Month then 
 
       -- Calculate strip Level PRICE    
           CASE
            WHEN trim(pmnthly_collectn(li_rec_Index).UNDERLYING_COMMODITY) = 'ELECTRICITY' THEN 
             
            --HOur weighted Price 
            -- total Strip Price * totoal Hours/total Hours 
            
--             pmnthly_collectn(li_rec_Index).Proj_loc_amt := 
--               ( pmnthly_collectn(li_rec_Index).Proj_loc_amt * pmnthly_collectn(li_rec_Index).TOTAL_HOURS_PER_STRIP )/NVL(pmnthly_collectn(li_rec_Index).TOTAL_HOURS_PER_STRIP,1);   


             pmnthly_collectn(li_rec_Index).Proj_loc_amt := 
               ( pmnthly_collectn(li_rec_Index).Proj_loc_amt )/NVL(pmnthly_collectn(li_rec_Index).TOTAL_HOURS_PER_STRIP,1);   


            WHEN trim(pmnthly_collectn(li_rec_Index).UNDERLYING_COMMODITY) = 'OIL' THEN
           
              pmnthly_collectn(li_rec_Index).Proj_loc_amt := 
                 pmnthly_collectn(li_rec_Index).Proj_loc_amt/NVL(pmnthly_collectn(li_rec_Index).TOTAL_TENOR_MONTHS,1);   
           



            WHEN trim(pmnthly_collectn(li_rec_Index).UNDERLYING_COMMODITY) IN ( 'NATURAL GAS','GAS', 'NG 6 MOAVG')     THEN
              -- Day weighted Price  
--              pmnthly_collectn(li_rec_Index).Proj_loc_amt := 
--                  ( pmnthly_collectn(li_rec_Index).Proj_loc_amt * pmnthly_collectn(li_rec_Index).CALENDAR_DAYS_in_STRIP ) /NVL(pmnthly_collectn(li_rec_Index).CALENDAR_DAYS_in_STRIP,1);   

              pmnthly_collectn(li_rec_Index).Proj_loc_amt := 
                  ( pmnthly_collectn(li_rec_Index).Proj_loc_amt ) /NVL(pmnthly_collectn(li_rec_Index).CALENDAR_DAYS_in_STRIP,1);   

 
            WHEN trim(pmnthly_collectn(li_rec_Index).UNDERLYING_COMMODITY) = 'CAPACITY'    THEN
              pmnthly_collectn(li_rec_Index).Proj_loc_amt := 
                    pmnthly_collectn(li_rec_Index).Proj_loc_amt/NVL(pmnthly_collectn(li_rec_Index).TOTAL_TENOR_MONTHS,1);   
          
            WHEN trim(pmnthly_collectn(li_rec_Index).UNDERLYING_COMMODITY) = 'EMISSIONS'   THEN     
            
              pmnthly_collectn(li_rec_Index).Proj_loc_amt := 
                    pmnthly_collectn(li_rec_Index).Proj_loc_amt/NVL(pmnthly_collectn(li_rec_Index).TOTAL_TENOR_MONTHS,1);   
            ELSE
              null;
            END CASE;
     
     END IF;
    
       

           IF  NVL(pmnthly_collectn(li_rec_Index).Curve_rank, 0)  <> 2 
               AND 
                ( 
                pmnthly_collectn(li_rec_Index).LOCATION_DELTA_POSITION = 0
                 AND pmnthly_collectn(li_rec_Index).LEGGED_M2M_VALUE <> 0 
                 ) THEN
              
                 pmnthly_collectn(li_rec_Index).Curve_rank := 1 ;
              
           END IF;
             

             -- IF its ISOCON FR curve keep ranking of 2 
            
              IF pmnthly_collectn(li_rec_Index).LEGGED_M2M_VALUE =  0 AND
                 pmnthly_collectn(li_rec_Index).LOCATION_DELTA_POSITION = 0 THEN 
              
                   -- make the strip as inVALID/INACTIVE
                   pmnthly_collectn(li_rec_Index).ACTIVE_FLAG := 'N';
               
              END IF; 
                       
--              ----dbms_output.put_line('loc tenor fas Derive strip measures '); 
 
               Derive_StripLevel_Measures(
                 pindex             =>  li_rec_Index
               , pOriginalLocation  =>  pmnthly_collectn(li_rec_Index).NEER_LOCATION
               , pOverrideLocation  =>  pmnthly_collectn(li_rec_Index).NEER_LOCATION 
               , pbackbone_flag     =>  NULL
               , pOverridebb_flag   =>  NULL                                  
               , pcUrrentMOnth      =>  TO_NUMBER(pcurrentMonth)
               , pstartYearMonth    =>  TO_NUMBER(pstart_Month)
               , PEndYearMOnth      =>  TO_NUMBER(pend_month)
               , pm2mData           =>  pmonthlym2m
               , pcollection        =>  pmnthly_Collectn
               , pcalcType          =>  'TENOR_LEVEL_FAS'
               , pout_rtn_code      =>  vn_rtn_code    
               , pout_rtn_msg       =>  vc_rtn_msg 
               );
               
--    ----dbms_output.put_line('after tenor fas Derive strip measures ');
    
        pmnthly_collectn(li_rec_Index).PROCESS_LEVEL          
                                            := '3';
        pmnthly_collectn(li_rec_Index).STRIP_START_YEARMONTH := 
                                             pstart_Month;             
        pmnthly_collectn(li_rec_Index).STRIP_END_YEARMONTH   :=    
                                             pend_month; 
               
        pmnthly_collectn(li_rec_Index).TOTAL_TENOR_MONTHS    := 
                 MOnths_between( to_date(pend_month||'01', 'YYYYMMDD') 
                                 ,  to_date(pstart_Month||'01', 'YYYYMMDD')
                                ) + 1;    
                                
        pmnthly_collectn(li_rec_Index).EXT_MANUAL_QUOTE_ID       
                                            := pmanual_quote_id;
    
       vc_return_val := Get_granularity (
              pcobdate  => pcobdate
              , pUnderlying_commodity => m2m_recs.UNDERLYING_COMMODITY 
              , pPrice_vol_Indicator => m2m_recs.PRICE_VOL_INDICATOR
              , ptenor_Months      =>   pmnthly_collectn(li_rec_Index).TOTAL_TENOR_MONTHS 
              ) ;
      
--       ----dbms_output.put_line('after granularity ');
                 
            --    ----dbms_output.PUT_LINE ('Granulairty = '||vc_return_val) ;
                
        IF   vc_return_val = 'G' THEN
           pmnthly_collectn(li_rec_Index).Granular_quote_indicator  :=
                                        'Y'  ;     
        ELSE
          pmnthly_collectn(li_rec_Index).Granular_quote_indicator  :=
                                        'N'  ;
        END IF;
                          
          
                                             
         --calculate missing MID/BID/ASk
         
         pmnthly_collectn(li_rec_Index).MID := pmid;
         pmnthly_collectn(li_rec_Index).BID := pbid;
         pmnthly_collectn(li_rec_Index).ASK := pask;
         
                 
         CASE
         WHEN ( pmnthly_collectn(li_rec_Index).MID is not null AND
                pmnthly_collectn(li_rec_Index).BID is not null AND
                pmnthly_collectn(li_rec_Index).ASK is not null
               )  THEN                           --when all of them given
             null;
          WHEN ( pmnthly_collectn(li_rec_Index).MID is null AND
                pmnthly_collectn(li_rec_Index).BID is null AND
                pmnthly_collectn(li_rec_Index).ASK is null
               )  THEN                               --none of them given
             null;
                    
          else
            --logic when any single value of MID/BID/ASK is given
           case 
               WHEN
                    ( pmnthly_collectn(li_rec_Index).MID is null AND
                      pmnthly_collectn(li_rec_Index).BID is not null AND
                      pmnthly_collectn(li_rec_Index).ASK is null
                    ) THEN
                pmnthly_collectn(li_rec_Index).MID := pmnthly_collectn(li_rec_Index).BID * (1+pmnthly_collectn(li_rec_Index).FINAL_FAS157_VALUE/100) ;
                pmnthly_collectn(li_rec_Index).ASK := pmnthly_collectn(li_rec_Index).MID + (pmnthly_collectn(li_rec_Index).MID-pmnthly_collectn(li_rec_Index).BID);
               WHEN  
                    ( pmnthly_collectn(li_rec_Index).MID is null AND
                      pmnthly_collectn(li_rec_Index).BID is null AND
                      pmnthly_collectn(li_rec_Index).ASK is not null
                    ) THEN
                    pmnthly_collectn(li_rec_Index).MID := pmnthly_collectn(li_rec_Index).ASK/(1 + pmnthly_collectn(li_rec_Index).FINAL_FAS157_VALUE/100) ;   
                    pmnthly_collectn(li_rec_Index).BID := pmnthly_collectn(li_rec_Index).MID - (pmnthly_collectn(li_rec_Index).ASK - pmnthly_collectn(li_rec_Index).MID);
               WHEN
                    ( pmnthly_collectn(li_rec_Index).MID is not null AND
                      pmnthly_collectn(li_rec_Index).BID is null AND
                      pmnthly_collectn(li_rec_Index).ASK is null
                    ) THEN
                pmnthly_collectn(li_rec_Index).ASK := pmnthly_collectn(li_rec_Index).MID * (1 + pmnthly_collectn(li_rec_Index).FINAL_FAS157_VALUE/100);
                pmnthly_collectn(li_rec_Index).BID := pmnthly_collectn(li_rec_Index).MID + pmnthly_collectn(li_rec_Index).MID - pmnthly_collectn(li_rec_Index).ASK ;
                        
                --logic when any two values of MID/BID/ASK are given
              WHEN
                    ( pmnthly_collectn(li_rec_Index).MID is null AND
                      pmnthly_collectn(li_rec_Index).BID is not null AND
                      pmnthly_collectn(li_rec_Index).ASK is not null
                    ) THEN
                    pmnthly_collectn(li_rec_Index).MID := (pmnthly_collectn(li_rec_Index).BID + pmnthly_collectn(li_rec_Index).ASK)/2;
             WHEN
                    ( pmnthly_collectn(li_rec_Index).MID is not null AND
                      pmnthly_collectn(li_rec_Index).BID is not null AND
                      pmnthly_collectn(li_rec_Index).ASK is null
                    ) THEN
                     pmnthly_collectn(li_rec_Index).ASK  := pmnthly_collectn(li_rec_Index).MID + (pmnthly_collectn(li_rec_Index).MID - pmnthly_collectn(li_rec_Index).BID);
             WHEN
                    ( pmnthly_collectn(li_rec_Index).MID is not null AND
                      pmnthly_collectn(li_rec_Index).BID is null AND
                      pmnthly_collectn(li_rec_Index).ASK is not null
                    ) THEN       
                    pmnthly_collectn(li_rec_Index).BID  := pmnthly_collectn(li_rec_Index).MID - (pmnthly_collectn(li_rec_Index).ASK - pmnthly_collectn(li_rec_Index).MID);
          end case ;
                 
         end CASE;
                 
 --                 ----dbms_output.put_line('after MID BID ASK ');
                  
                  /****Delta price percent****/
                
                IF pmnthly_collectn(li_rec_Index).MID IS NULL THEN
                   
                    v_delta_price_deno := NULL;
                ELSIF pmnthly_collectn(li_rec_Index).MID= 0 THEN
                    v_delta_price_deno := 1;
                ELSE
                     v_delta_price_deno := ABS(pmnthly_collectn(li_rec_Index).MID ) ;    
                END IF;
                
                pmnthly_collectn(li_rec_Index).DELTA_PRICE_VALUE 
                := abs(
                       pmnthly_collectn(li_rec_Index).PROJ_LOC_AMT - 
                       pmnthly_collectn(li_rec_Index).MID
                      );
                
  
                pmnthly_collectn(li_rec_Index).DELTA_PRICE_PRCNT 
                := pmnthly_collectn(li_rec_Index).DELTA_PRICE_VALUE /v_delta_price_deno;
          
               /*******
                   Validate the Curve 
               ******/
 --             ----dbms_output.put_line('Validate the Curve ');   
                          
                --- compute level 1 and level 2 validation 
                CASE
                WHEN 
                   ( pmnthly_collectn(li_rec_Index).USING_100PRCNT_HIST_METHOD = 'Y' 
                     OR pmnthly_collectn(li_rec_Index).Curve_rank = 2  )                                     
                    THEN 
                      pmnthly_collectn(li_rec_Index).Curve_rank :=  2 ;
                      pmnthly_collectn(li_rec_Index).Curve_Validation_Description := 'FULLY VALIDATED, APPROVED SYSTEM METHODOLOGY';
                      pmnthly_collectn(li_rec_Index).VALIDATED_FLAG := 'Y';
                      pmnthly_collectn(li_rec_Index).FULLY_VALIDATED_FLAG := 'Y';

                WHEN  ( pmnthly_collectn(li_rec_Index).Curve_rank = 1 ) THEN 
                     
                     pmnthly_collectn(li_rec_Index).Curve_Validation_Description := 'FULLY VALIDATED, POSITION = 0/M2M <>0';
                     pmnthly_collectn(li_rec_Index).VALIDATED_FLAG := 'Y';
                     pmnthly_collectn(li_rec_Index).FULLY_VALIDATED_FLAG := 'Y';    
                     
                WHEN pmnthly_collectn(li_rec_Index).PROJ_LOC_AMT is NULL THEN
            
                  pmnthly_collectn(li_rec_Index).Curve_Validation_Description := 'NO INTERNAL PRICE';    
                  
                  pmnthly_collectn(li_rec_Index).VALIDATED_FLAG := 'N';
                  pmnthly_collectn(li_rec_Index).FULLY_VALIDATED_FLAG := 'N';

                       
                WHEN  ( pmnthly_collectn(li_rec_Index).MID is NULL 
                        AND pmnthly_collectn(li_rec_Index).BID is NULL
                        AND pmnthly_collectn(li_rec_Index).ASK IS NULL ) THEN

                  pmnthly_collectn(li_rec_Index).Curve_Validation_Description := 'NO QUOTE';
                   
                  pmnthly_collectn(li_rec_Index).VALIDATED_FLAG := 'N';
                  pmnthly_collectn(li_rec_Index).FULLY_VALIDATED_FLAG := 'N';

                ELSE
                  null; -- skip it , falls through regular logic 
                END CASE;
               
              CASE
               WHEN  pmnthly_collectn(li_rec_Index).ACTIVE_FLAG = 'Y' 
                AND  ( pmnthly_collectn(li_rec_Index).Curve_rank = 1 OR
                       pmnthly_collectn(li_rec_Index).Curve_rank = 2 ) THEN 
                   
                   null; -- skip validation 
                   
              ELSE   
                            
                 Derive_StripLevel_Measures(
                      pindex             =>  li_rec_Index
                    , pOriginalLocation  =>  pmnthly_collectn(li_rec_Index).NEER_LOCATION
                    , pOverrideLocation  =>  pmnthly_collectn(li_rec_Index).NEER_LOCATION  
                    , pbackbone_flag     =>  NULL
                    , pOverridebb_flag   =>  NULL                       
                    , pcUrrentMOnth      =>  TO_NUMBER(pcurrentMonth)
                    , pstartYearMonth    =>  TO_NUMBER(pstart_Month)
                    , PEndYearMOnth      =>  TO_NUMBER(pend_month)
                    , pm2mData           =>  pmonthlym2m
                    , pcollection        =>  pmnthly_Collectn
                    , pcalcType          =>  'VALIDATE_A_CURVE' 
                    , pout_rtn_code      =>  vn_rtn_code    
                    , pout_rtn_msg       =>  vc_rtn_msg 
                 );   
               
              END CASE;
                
             -- Get the Validation Indicator ID and  

               BEGIN
                    
                SELECT 
                   VALIDATION_INDICATOR_ID
                 INTO 
                   pmnthly_collectn(li_rec_Index).VALIDATION_INDICATOR_ID
                 FROM
                  RISKDB.QP_CURVE_VALIDATION v
                 where
                  Upper(VALIDATION_DESCRIPTION)  = (pmnthly_collectn(li_rec_Index).Curve_Validation_Description)
                  AND ACTIVE_FLAG = 'Y'
                  AND pmnthly_collectn(li_rec_Index).COB_DATE BETWEEN  
                        TRUNC(EFFECTIVE_START_DATE) AND 
                     pmnthly_collectn(li_rec_Index).COB_DATE
                  ;
                      
               EXCEPTION
               WHEN NO_DATA_FOUND THEN 
                 pmnthly_collectn(li_rec_Index).VALIDATION_INDICATOR_ID := NULL;
               END;
               
               -- UNtil here , to Get the Indicator ID and
                --Validate a Curve at location level 
                           
-- --            IF pmnthly_collectn(li_rec_Index).ACTIVE_FLAG = 'Y' THEN   
--                 Derive_StripLevel_Measures(
--                      pindex             =>  li_rec_Index
--                    , pcUrrentMOnth      =>  TO_NUMBER(pcurrentMonth)
--                    , pstartYearMonth    =>  TO_NUMBER(pstart_Month)
--                    , PEndYearMOnth      =>  TO_NUMBER(pend_month)
--                    , pm2mData           =>  pmonthlym2m
--                    , pcollection        =>  pmnthly_collectn
--                    , pcalcType          =>  'LOCATION_M2M_PERCENT' 
--                    , pout_rtn_code      =>  vn_rtn_code    
--                    , pout_rtn_msg       =>  vc_rtn_msg 
--                 );
-- 
             
               
            IF v_current_Location <> v_previous_Location THEN 

                    v_current_flat_pos :=     pmnthly_collectn(li_rec_Index).FLAT_POSITION_FLAG ;
               
                     Get_MAX_MIN_CONTRACTYR ( 
                                  pindex             => li_rec_Index
                                , pLocation          => v_current_Location
                                , pout_rtn_code      =>  vn_rtn_code   
                                , pout_rtn_msg       =>  vc_rtn_msg 
                               );
                    
                    
                    v_max_contractYr  := pmnthly_collectn(li_rec_Index).max_ContractYEAR_Month ; 
                    v_min_contractYr  := pmnthly_collectn(li_rec_Index).min_ContractYEAR_Month ;
                    v_totContractMonths := pmnthly_collectn(li_rec_Index).tot_ContractYEAR_Month; 
            ELSE
              
              --- assign Value from a Local Variables
                 pmnthly_collectn(li_rec_Index).FLAT_POSITION_FLAG := v_current_flat_pos;
                 pmnthly_collectn(li_rec_Index).max_ContractYEAR_Month := v_max_contractYr ;
                 pmnthly_collectn(li_rec_Index).min_ContractYEAR_Month := v_min_contractYr ;
                 pmnthly_collectn(li_rec_Index).tot_ContractYEAR_Month := v_totContractMonths;
                 
            END IF; 
             
               PROCESS_LOG_REC.STAGE        := 'PREPARE_INTANDEXT_MONTHLY_QTS';     

                /********************/
                
                IF pmnthly_collectn(li_rec_Index).NEER_COMMODITY =
                   pmnthly_collectn(li_rec_Index).NEER_LOCATION  
                   AND pmnthly_collectn(li_rec_Index).UNDERLYING_COMMODITY = 'ELECTRICITY' 
                   and pmnthly_collectn(li_rec_Index).ACTIVE_FLAG = 'Y' THEN  
                 -- we have to add additional comodity record 
                 -- only change is m2m, delta position , legged m2m
                 -- and abolute delta would be sum @commdity level
                 --(all BAsis atributes same as LOC level )
                 -- FAS need to be recalculated at commodity
                    
                    Compute_commodity_strip(
                     pindex             =>  li_rec_Index
                    , pOriginalLocation  =>  pmnthly_collectn(li_rec_Index).NEER_LOCATION
                    , pOverrideLocation  =>  pmnthly_collectn(li_rec_Index).NEER_LOCATION
                    , pbackbone_flag     =>  NULL
                    , pOverridebb_flag   =>  NULL                        
                    , pcUrrentMOnth      =>  TO_NUMBER(pcurrentMonth)
                    , pstartYearMonth    =>  TO_NUMBER(pstart_Month)
                    , PEndYearMOnth      =>  TO_NUMBER(pend_month)
                    , pm2mData           =>  pmonthlym2m
                    , pcollection        =>  pmnthly_collectn
                    , pcalcType          =>  'COMMODITY_LEVEL_SUM' 
                    , pout_rtn_code      =>  vn_rtn_code    
                    , pout_rtn_msg       =>  vc_rtn_msg 
                    );
                    
                    select 
                    MIN(TO_NUMBER(v.contractYear_month))
                    , MAX (TO_NUMBER(v.contractYear_month))
                    INTO
                     pmnthly_collectn(li_rec_Index).min_ContractYEAR_Month
                     , pmnthly_collectn(li_rec_Index).max_ContractYEAR_Month
                    from 
                    RISKDB.QP_INT_FWD_CURVES_RAW_DATA v
                    where
                    v.cob_date = pmnthly_collectn(li_rec_Index).COB_DATE
                    and v.underlying_commodity = pmnthly_collectn(li_rec_Index).UNDERLYING_COMMODITY 
                    and v.commodity = pmnthly_collectn(li_rec_Index).NEER_COMMODITY
                   -- AND V.COMMODITY_ABSOLUTE_POSITION <> 0
                    AND V.ACTIVE_FLAG = 'Y'
                    ;
                    
 
                 
                 IF  pmnthly_collectn(li_rec_Index).max_ContractYEAR_Month IS NOT NULL 
                     AND pmnthly_collectn(li_rec_Index).min_ContractYEAR_Month IS NOT NULL 
                  THEN  
                     
                    pmnthly_collectn(li_rec_Index).tot_ContractYEAR_Month := 
                        MOnths_between( to_date(pmnthly_collectn(li_rec_Index).max_ContractYEAR_Month||'01', 'YYYYMMDD') 
                                       ,  to_date(pmnthly_collectn(li_rec_Index).min_ContractYEAR_Month||'01', 'YYYYMMDD')
                                      ) + 1;                    
                 ELSE
                    pmnthly_collectn(li_rec_Index).tot_ContractYEAR_Month := NULL;
                 END IF ;
                 
                 
                    -- Validate a curve
                    Derive_StripLevel_Measures(
                     pindex             =>  li_rec_Index
                    , pOriginalLocation  =>  pmnthly_collectn(li_rec_Index).NEER_LOCATION
                    , pOverrideLocation  =>  pmnthly_collectn(li_rec_Index).NEER_LOCATION 
                    , pbackbone_flag     =>  NULL
                    , pOverridebb_flag   =>  NULL                       
                    , pcUrrentMOnth      =>  TO_NUMBER(pcurrentMonth)
                    , pstartYearMonth    =>  TO_NUMBER(pstart_Month)
                    , PEndYearMOnth      =>  TO_NUMBER(pend_month)
                    , pm2mData           =>  pmonthlym2m
                    , pcollection        =>  pmnthly_collectn
                    , pcalcType          =>  'VALIDATE_A_CURVE' 
                    , pout_rtn_code      =>  vn_rtn_code    
                    , pout_rtn_msg       =>  vc_rtn_msg 
                    );
                    
       
                  PROCESS_LOG_REC.STAGE        := 'PREPARE_INTANDEXT_MONTHLY_QTS';  
                END IF;
    
                  
           
    
 
          -- assign Previous Location 
            v_Previous_location := v_current_Location;
                      
           END LOOP; -- end of m2m_recs Loop

--    ----dbms_output.Put_line('*****1');
 pout_rtn_code := c_success;
   pout_rtn_msg := ''; --Null means Success
   
Exception 
 WHEN OTHERS THEN 

       pout_rtn_code := c_failure;
       pout_rtn_msg := 'Quote_id='||
                       TO_CHAR(pmnthly_collectn(li_rec_Index).EXT_MANUAL_QUOTE_ID)||
                       SUBSTR(SQLERRM,1,100);
       
       ----dbms_output.PUT_LINE( pout_rtn_msg);

END Prepare_manual_qts; 
   


Procedure Load_Manual_strips( 
                              pcobdate       IN DATE
                              , PSTATUSCODE    OUT VARCHAR2
                              , PSTATUSMSG     OUT VARCHAR2
                              ) 
is 

li_errors                       NUMBER; 
vc_cobdateMonth                 VARCHAR2(6) := TO_CHAR(pCOBDATE  , 'YYYYMM');   
vc_cobdate                      VARCHAR2(12) := TO_CHAR(pCOBDATE, 'DD-MON-YYYY');
vc_CurrentMonth                 VARCHAR2(6); -- cob date's Month
v_underlying_commodity          RISKDB.QP_STRIP_LVL_NEER_EXT_DATA.UNDERLYING_COMMODITY%TYPE;
v_price_vol_indicator           RISKDB.QP_STRIP_LVL_NEER_EXT_DATA.PRICE_VOL_INDICATOR%TYPE; 
v_stripTenor                    RISKDB.QP_EXT_PROVIDER_QUOTES.STRIP_TENOR%TYPE;
vc_PromptMonth                  VARCHAR2(6);

--strip related Years
--QYYYY                           VARCHAR2(4); -- Quote Year
--CYYYY                           VARCHAR2(4); -- Current Year
--PYYYY                           VARCHAR2(4); -- Prompt Year
vc_AYYYY                           VARCHAR2(4); -- All Years


vc_startAYYYYMM                  VARCHAR2(6);
vc_endAYYYYMM                    VARCHAR2(6);

vc_startCYYYYMM                  VARCHAR2(6);
vc_endCYYYYMM                    VARCHAR2(6);

AYYYY_startmonth                 VARCHAR2(6);
AYYYY_endmonth                   VARCHAR2(6);
 
 vc_section                     VARCHAR2(100);
 vn_rtn_code                    NUMBER;
 vc_rtn_msg                     VARCHAR2(2000);
 vc_sql_stmt                    VARCHAR2(1000);
 vc_singleQuote                 VARCHAR2(1) := CHR(39);
 vc_AYYYYMM                     VARCHAR2(10);  --for looping any Strip year month of tenor_strip

--collection to hold manual strips
 manual_Collection             strips_collectn_t; 
 vn_records                     NUMBER := 0;
 

--- declare an Object variable of Monthly Objects collection

  manual_m2m_obj           m2m_by_Loc_T;  -- TYPE Object
  manual_m2m_tab           m2m_by_Loc_tab; -- Table of TYPE Objects
  
  ext_Quotes_obj            ext_quotes_T;  -- TYPE Object
  mapped_ext_quotes         ext_quotes_tab; -- Table of TYPE Objects

  e_Invalid_Strip_startMonth         EXCEPTION;
   PRAGMA EXCEPTION_INIT( e_Invalid_Strip_startMonth , -20040);

  e_Invalid_Strip_EndMonth          EXCEPTION;
   PRAGMA EXCEPTION_INIT( e_Invalid_Strip_EndMonth , -20041);
   
   e_Appl_error                     EXCEPTION;
   PRAGMA EXCEPTION_INIT( e_Appl_error , -20042); 
   

-- Define Monthly Manual strips 
--Monthly Strips                             
    Cursor cur_manual_quotes is
     Select 
     manual.EXT_MANUAL_QUOTE_ID
     ,manual.UNDERLYING_COMMODITY
     ,trunc(manual.EXT_EFF_DATE) EXT_EFF_DATE
     ,manual.MID
     ,manual.BID
     ,manual.ASK
     ,manual.STRIP_TENOR
     ,manual.NEER_LOCATION
     ,manual.PRICE_VOL_INDICATOR
     ,manual.FROM_DATE
     ,manual.TO_DATE
     ,manual.EXT_CATEGORY
     ,manual.DATA_SOURCE_NAME
     ,manual.ACTIVE_FLAG
    from 
     RISKDB.QP_EXT_MANUAL_QUOTES manual
    where 
     trunc(manual.ext_eff_date)=trunc(pcobdate)
     and active_flag = 'Y'   
--     and NEER_LOCATION = 'NEPOOL-.H.INTERNAL_HUB-5x16'
     order by 
     manual.underlying_commodity
    , manual.Strip_tenor
    , manual.price_vol_indicator
    ; 
 

Cursor cur_tenor is
Select 
tenor.TENOR_ID
,tenor.UNDERLYING_COMMODITY
,tenor.STRIP_NAME
,tenor.START_MONTH
,tenor.END_MONTH
,tenor.ADD_MONTHS
,tenor.PRICE_VOL_INDICATOR
, Tenor.Balance_flag
from 
RISKDB.QP_TENOR_MASTER tenor
where
 Upper(tenor.Strip_name) = Upper(v_stripTenor)
 and PRICE_VOL_INDICATOR = v_price_vol_indicator
 and Underlying_commodity = v_underlying_commodity
 and active_flag = 'Y' 
  --and Balance_flag = 'N'
  and trunc(pcobdate) between 
            trunc(Effective_start_date) and 
            NVL(effective_end_date , pcobdate )
order by 
 tenor.Strip_name
 , tenor.price_vol_indicator
;

-- Intialize montly strips collection
Procedure INIT_manual_collection IS
BEGIN
   manual_Collection := strips_collectn_t();
END INIT_manual_collection;

Begin


   IF NVL(g_minlogid , 0) = 0 THEN
   
    --stage 2 not involked , so find out minimum number logged for this stage 
    SELECT MAX(l.log_id) 
    INTO g_minlogid
    FROM 
    DMR.PROCESS_LOG l
    WHERE
    application_Name = 'QVAR_APPS'
    and process_Name = 'QP_ZEMA_EXTRACT_PKG'
    and Parent_Name = 'QVAR_INTERNAL_MANUALQUOTES'
    ;
 
   END IF ;
   
     --Set application Name for the Log Process
    DMR.PROCESS_LOG_PKG.CONSTRUCTOR ('QVAR_APPS' );
    
     --Set Process Name and status for the Log Process                   
    PROCESS_LOG_REC.PROCESS_NAME :=  'QP_ZEMA_EXTRACT_PKG';
    PROCESS_LOG_REC.PARENT_NAME  := 'QVAR_INTERNAL_MANUALQUOTES';
    PROCESS_LOG_REC.STAGE        := 'Load_Manual_Strips';
    PROCESS_LOG_REC.STATUS       := PROCESS_LOG_PKG.C_STATUS_STARTED;
    PROCESS_LOG_REC.MESSAGE      := 'Load_Manual_Strips process Started'||vc_cobdate;
    
    DMR.PROCESS_LOG_PKG.SET_PROCESS_NAME(
    P_PROCESS_NAME =>PROCESS_LOG_REC.PROCESS_NAME
    , P_PARENT_NAME => PROCESS_LOG_REC.PARENT_NAME
    );
   

   -- Log Message first
   write_log;
    INIT_manual_collection;
     
    vc_CurrentMonth := to_char(pcobdate , 'YYYYMM');
    vc_PromptMonth  := TO_CHAR(ADD_MONTHS(pcobdate,1), 'YYYYMM');
    
   -- Derive QuoteYear, promptYear, currentYear for the tenor strip
   -- based on available cobDate and contractYear information
   --get start Month and end Month
 
 
 
   /*************************************************************************
     Get All forward Data for the given COBDATE 
   *************************************************************************/                           
     
       Construct_m2m_object(
                              pcobdate     =>  pcobdate
                            , pmonthlym2m   =>  manual_m2m_tab  
                            , pout_rtn_code =>  vn_rtn_code 
                            , pout_rtn_msg =>  vc_rtn_msg
                           ) ;
   

   For strips in  cur_manual_quotes  
   loop   
   
    v_stripTenor :=  strips.STRIP_TENOR ;
    v_price_vol_indicator := strips.price_vol_indicator;
    v_underlying_commodity := strips.UNDERLYING_COMMODITY;
 
   
--      ----dbms_output.PUT_LINE
--      ( 'quoteid ='||strips.EXT_MANUAL_QUOTE_ID
--       ||'pstripname =>'|| strips.STRIP_TENOR
--      -- ||'pstart_month='|| AYYYY_startmonth
--      -- ||'pend_month='||
--      ||'plocation =>'||strips.NEER_LOCATION
--      );
   
   PROCESS_LOG_REC.STAGE        := 'Find_standard_tenor';
   
   Find_standard_tenor
   (  pcobdate   => pcobdate
    , pstripname => strips.STRIP_TENOR 
    , pstart_month => AYYYY_startmonth 
    , pend_month => AYYYY_endmonth 
    , punderlyingCom => strips.UNDERLYING_COMMODITY
    , ppricevolind =>  strips.price_vol_indicator
    , plocation => strips.NEER_LOCATION
    , pmonthlym2m => manual_m2m_tab
    , pout_rtn_code =>  vn_rtn_code 
    , pout_rtn_msg =>  vc_rtn_msg
    );
   
   
   IF vn_rtn_code <> c_success THEN 
        RAISE e_appl_error;
   END IF;
   
--   ----dbms_output.PUT_LINE
--      (
--       'after find pstart_month =>'|| AYYYY_startmonth 
--      -- ||'pstart_month='|| AYYYY_startmonth
--      -- ||'pend_month='||
--      ||'pEnd_month =>'||AYYYY_endmonth 
--     );   
   
 IF  AYYYY_startmonth IS NOT NULL AND  AYYYY_endmonth is NOT NULL 
 THEN 
 
   for tenors in  cur_tenor
   LOOP
   CASE
   WHEN   UPPER(strips.STRIP_TENOR)='JANUARY' THEN
   
   For recs in  to_number(AYYYY_startmonth) .. to_Number(AYYYY_endmonth)
    LOOP
       vc_AYYYYMM := to_CHAR(recs)||'01';
       
       
       Prepare_manual_qts(
                              pcobdate          => pcobdate
                            , pmonthlym2m       => manual_m2m_tab
                            , pmanual_quote_id  =>  strips.EXT_MANUAL_QUOTE_ID
                            , pstrip_tenor      => strips.STRIP_TENOR 
                            , pextCategory      => strips.EXT_CATEGORY
                            , pDatasourceName   => strips.DATA_SOURCE_NAME
                            , pMID              => Strips.MID
                            , pBID              => Strips.BID 
                            , pASK              => Strips.ASK
                            , punderlyingcom    => strips.UNDERLYING_COMMODITY
                            , ppricevolind      => strips.price_vol_indicator
                            , plocation         => strips.NEER_LOCATION
                            , pcurrentMonth      => vc_CurrentMonth
                            , pstart_Month      => AYYYY_startmonth 
                            , pend_month        => AYYYY_endmonth 
                            , pmnthly_collectn  => manual_collection
                            , pout_rtn_code     => vn_rtn_code  
                            , pout_rtn_msg   => vc_rtn_msg
                            );
                            
                            
    END LOOP; 
   
   WHEN  UPPER(strips.STRIP_TENOR)='FEBRUARY'  THEN 
   
   For recs in  to_number(AYYYY_startmonth) .. to_Number(AYYYY_endmonth)
    LOOP
       vc_AYYYYMM := to_CHAR(recs)||'02';

       
       Prepare_manual_qts(
                              pcobdate          => pcobdate
                            , pmonthlym2m       => manual_m2m_tab
                            , pmanual_quote_id  =>  strips.EXT_MANUAL_QUOTE_ID
                            , pstrip_tenor      => strips.STRIP_TENOR 
                            , pextCategory      => strips.EXT_CATEGORY
                            , pDatasourceName   => strips.DATA_SOURCE_NAME
                            , pMID              => Strips.MID
                            , pBID              => Strips.BID 
                            , pASK              => Strips.ASK
                            , punderlyingcom    => strips.UNDERLYING_COMMODITY
                            , ppricevolind      => strips.price_vol_indicator
                            , plocation         => strips.NEER_LOCATION
                            , pcurrentMonth      => vc_CurrentMonth
                            , pstart_Month      => AYYYY_startmonth 
                            , pend_month        => AYYYY_endmonth 
                            , pmnthly_collectn  => manual_collection
                            , pout_rtn_code     => vn_rtn_code  
                            , pout_rtn_msg   => vc_rtn_msg
                            );
    END LOOP; 
    
   WHEN   UPPER(strips.STRIP_TENOR)='MARCH' THEN 
   
   For recs in  to_number(AYYYY_startmonth) .. to_Number(AYYYY_endmonth)
    LOOP
       vc_AYYYYMM := to_CHAR(recs)||'03';

       
       Prepare_manual_qts(
                              pcobdate          => pcobdate
                            , pmonthlym2m       => manual_m2m_tab
                            , pmanual_quote_id  =>  strips.EXT_MANUAL_QUOTE_ID
                            , pstrip_tenor      => strips.STRIP_TENOR 
                            , pextCategory      => strips.EXT_CATEGORY
                            , pDatasourceName   => strips.DATA_SOURCE_NAME
                            , pMID              => Strips.MID
                            , pBID              => Strips.BID 
                            , pASK              => Strips.ASK
                            , punderlyingcom    => strips.UNDERLYING_COMMODITY
                            , ppricevolind      => strips.price_vol_indicator
                            , plocation         => strips.NEER_LOCATION
                            , pcurrentMonth      => vc_CurrentMonth
                            , pstart_Month      => AYYYY_startmonth 
                            , pend_month        => AYYYY_endmonth 
                            , pmnthly_collectn  => manual_collection
                            , pout_rtn_code     => vn_rtn_code  
                            , pout_rtn_msg   => vc_rtn_msg
                            );
    END LOOP; 
    
   WHEN  UPPER(strips.STRIP_TENOR)='APRIL' THEN 
   
   For recs in  to_number(AYYYY_startmonth) .. to_Number(AYYYY_endmonth)
    LOOP
       vc_AYYYYMM := to_CHAR(recs)||'04';
 
       Prepare_manual_qts(
                              pcobdate          => pcobdate
                            , pmonthlym2m       => manual_m2m_tab
                            , pmanual_quote_id  =>  strips.EXT_MANUAL_QUOTE_ID
                            , pstrip_tenor      => strips.STRIP_TENOR 
                            , pextCategory      => strips.EXT_CATEGORY
                            , pDatasourceName   => strips.DATA_SOURCE_NAME
                            , pMID              => Strips.MID
                            , pBID              => Strips.BID 
                            , pASK              => Strips.ASK
                            , punderlyingcom    => strips.UNDERLYING_COMMODITY
                            , ppricevolind      => strips.price_vol_indicator
                            , plocation         => strips.NEER_LOCATION
                            , pcurrentMonth      => vc_CurrentMonth
                            , pstart_Month      => AYYYY_startmonth 
                            , pend_month        => AYYYY_endmonth 
                            , pmnthly_collectn  => manual_collection
                            , pout_rtn_code     => vn_rtn_code  
                            , pout_rtn_msg   => vc_rtn_msg
                            );
    END LOOP; 
    
   WHEN  UPPER(strips.STRIP_TENOR)= 'MAY' THEN 
   For recs in  to_number(AYYYY_startmonth) .. to_Number(AYYYY_endmonth)
    LOOP
       vc_AYYYYMM := to_CHAR(recs)||'05';
 
       Prepare_manual_qts(
                              pcobdate          => pcobdate
                            , pmonthlym2m       => manual_m2m_tab
                            , pmanual_quote_id  =>  strips.EXT_MANUAL_QUOTE_ID
                            , pstrip_tenor      => strips.STRIP_TENOR 
                            , pextCategory      => strips.EXT_CATEGORY
                            , pDatasourceName   => strips.DATA_SOURCE_NAME
                            , pMID              => Strips.MID
                            , pBID              => Strips.BID 
                            , pASK              => Strips.ASK
                            , punderlyingcom    => strips.UNDERLYING_COMMODITY
                            , ppricevolind      => strips.price_vol_indicator
                            , plocation         => strips.NEER_LOCATION
                            , pcurrentMonth      => vc_CurrentMonth
                            , pstart_Month      => AYYYY_startmonth 
                            , pend_month        => AYYYY_endmonth 
                            , pmnthly_collectn  => manual_collection
                            , pout_rtn_code     => vn_rtn_code  
                            , pout_rtn_msg   => vc_rtn_msg
                            );
    END LOOP; 
    
   WHEN  UPPER(strips.STRIP_TENOR)='JUNE' THEN 
   For recs in  to_number(AYYYY_startmonth) .. to_Number(AYYYY_endmonth)
    LOOP
       vc_AYYYYMM := to_CHAR(recs)||'06';

       Prepare_manual_qts(
                              pcobdate          => pcobdate
                            , pmonthlym2m       => manual_m2m_tab
                            , pmanual_quote_id  =>  strips.EXT_MANUAL_QUOTE_ID
                            , pstrip_tenor      => strips.STRIP_TENOR 
                            , pextCategory      => strips.EXT_CATEGORY
                            , pDatasourceName   => strips.DATA_SOURCE_NAME
                            , pMID              => Strips.MID
                            , pBID              => Strips.BID 
                            , pASK              => Strips.ASK
                            , punderlyingcom    => strips.UNDERLYING_COMMODITY
                            , ppricevolind      => strips.price_vol_indicator
                            , plocation         => strips.NEER_LOCATION
                            , pcurrentMonth      => vc_CurrentMonth
                            , pstart_Month      => AYYYY_startmonth 
                            , pend_month        => AYYYY_endmonth 
                            , pmnthly_collectn  => manual_collection
                            , pout_rtn_code     => vn_rtn_code  
                            , pout_rtn_msg   => vc_rtn_msg
                            );
    END LOOP; 
    
   WHEN  UPPER(strips.STRIP_TENOR)='JULY' THEN 
    ----dbms_output.PUT_LINE('Entered JULY case');
    
   For recs in  to_number(AYYYY_startmonth) .. to_Number(AYYYY_endmonth)
    LOOP
       vc_AYYYYMM := to_CHAR(recs)||'07';
 
       
       
       Prepare_manual_qts(
                              pcobdate          => pcobdate
                            , pmonthlym2m       => manual_m2m_tab
                            , pmanual_quote_id  =>  strips.EXT_MANUAL_QUOTE_ID
                            , pstrip_tenor      => strips.STRIP_TENOR 
                            , pextCategory      => strips.EXT_CATEGORY
                            , pDatasourceName   => strips.DATA_SOURCE_NAME
                            , pMID              => Strips.MID
                            , pBID              => Strips.BID 
                            , pASK              => Strips.ASK
                            , punderlyingcom    => strips.UNDERLYING_COMMODITY
                            , ppricevolind      => strips.price_vol_indicator
                            , plocation         => strips.NEER_LOCATION
                            , pcurrentMonth      => vc_CurrentMonth
                            , pstart_Month      => AYYYY_startmonth 
                            , pend_month        => AYYYY_endmonth 
                            , pmnthly_collectn  => manual_collection
                            , pout_rtn_code     => vn_rtn_code  
                            , pout_rtn_msg   => vc_rtn_msg
                            );
    END LOOP; 
 
   WHEN  UPPER(strips.STRIP_TENOR)='AUGUST' THEN 
   For recs in  to_number(AYYYY_startmonth) .. to_Number(AYYYY_endmonth)
    LOOP
       vc_AYYYYMM := to_CHAR(recs)||'08';
 
       Prepare_manual_qts(
                              pcobdate          => pcobdate
                            , pmonthlym2m       => manual_m2m_tab
                            , pmanual_quote_id  =>  strips.EXT_MANUAL_QUOTE_ID
                            , pstrip_tenor      => strips.STRIP_TENOR 
                            , pextCategory      => strips.EXT_CATEGORY
                            , pDatasourceName   => strips.DATA_SOURCE_NAME
                            , pMID              => Strips.MID
                            , pBID              => Strips.BID 
                            , pASK              => Strips.ASK
                            , punderlyingcom    => strips.UNDERLYING_COMMODITY
                            , ppricevolind      => strips.price_vol_indicator
                            , plocation         => strips.NEER_LOCATION
                            , pcurrentMonth      => vc_CurrentMonth
                            , pstart_Month      => AYYYY_startmonth 
                            , pend_month        => AYYYY_endmonth 
                            , pmnthly_collectn  => manual_collection
                            , pout_rtn_code     => vn_rtn_code  
                            , pout_rtn_msg   => vc_rtn_msg
                            );
    END LOOP; 
    
   WHEN  UPPER(strips.STRIP_TENOR)= 'SEPTEMBER' THEN 
   For recs in  to_number(AYYYY_startmonth) .. to_Number(AYYYY_endmonth)
    LOOP
       vc_AYYYYMM := to_CHAR(recs)||'09';

       Prepare_manual_qts(
                              pcobdate          => pcobdate
                            , pmonthlym2m       => manual_m2m_tab
                            , pmanual_quote_id  =>  strips.EXT_MANUAL_QUOTE_ID
                            , pstrip_tenor      => strips.STRIP_TENOR 
                            , pextCategory      => strips.EXT_CATEGORY
                            , pDatasourceName   => strips.DATA_SOURCE_NAME
                            , pMID              => Strips.MID
                            , pBID              => Strips.BID 
                            , pASK              => Strips.ASK
                            , punderlyingcom    => strips.UNDERLYING_COMMODITY
                            , ppricevolind      => strips.price_vol_indicator
                            , plocation         => strips.NEER_LOCATION
                            , pcurrentMonth      => vc_CurrentMonth
                            , pstart_Month      => AYYYY_startmonth 
                            , pend_month        => AYYYY_endmonth 
                            , pmnthly_collectn  => manual_collection
                            , pout_rtn_code     => vn_rtn_code  
                            , pout_rtn_msg   => vc_rtn_msg
                            );
    END LOOP; 
    
   WHEN  UPPER(strips.STRIP_TENOR)='OCTOBER' THEN 
   For recs in  to_number(AYYYY_startmonth) .. to_Number(AYYYY_endmonth)
    LOOP
       vc_AYYYYMM := to_CHAR(recs)||'10';

       Prepare_manual_qts(
                              pcobdate          => pcobdate
                            , pmonthlym2m       => manual_m2m_tab
                            , pmanual_quote_id  =>  strips.EXT_MANUAL_QUOTE_ID
                            , pstrip_tenor      => strips.STRIP_TENOR 
                            , pextCategory      => strips.EXT_CATEGORY
                            , pDatasourceName   => strips.DATA_SOURCE_NAME
                            , pMID              => Strips.MID
                            , pBID              => Strips.BID 
                            , pASK              => Strips.ASK
                            , punderlyingcom    => strips.UNDERLYING_COMMODITY
                            , ppricevolind      => strips.price_vol_indicator
                            , plocation         => strips.NEER_LOCATION
                            , pcurrentMonth      => vc_CurrentMonth
                            , pstart_Month      => AYYYY_startmonth 
                            , pend_month        => AYYYY_endmonth 
                            , pmnthly_collectn  => manual_collection
                            , pout_rtn_code     => vn_rtn_code  
                            , pout_rtn_msg   => vc_rtn_msg
                            );
    END LOOP; 
    
   WHEN  UPPER(strips.STRIP_TENOR)= 'NOVEMBER' THEN 
   For recs in  to_number(AYYYY_startmonth) .. to_Number(AYYYY_endmonth)
    LOOP
       vc_AYYYYMM := to_CHAR(recs)||'11';

       Prepare_manual_qts(
                              pcobdate          => pcobdate
                            , pmonthlym2m       => manual_m2m_tab
                            , pmanual_quote_id  =>  strips.EXT_MANUAL_QUOTE_ID
                            , pstrip_tenor      => strips.STRIP_TENOR 
                            , pextCategory      => strips.EXT_CATEGORY
                            , pDatasourceName   => strips.DATA_SOURCE_NAME
                            , pMID              => Strips.MID
                            , pBID              => Strips.BID 
                            , pASK              => Strips.ASK
                            , punderlyingcom    => strips.UNDERLYING_COMMODITY
                            , ppricevolind      => strips.price_vol_indicator
                            , plocation         => strips.NEER_LOCATION
                            , pcurrentMonth      => vc_CurrentMonth
                            , pstart_Month      => AYYYY_startmonth 
                            , pend_month        => AYYYY_endmonth 
                            , pmnthly_collectn  => manual_collection
                            , pout_rtn_code     => vn_rtn_code  
                            , pout_rtn_msg   => vc_rtn_msg
                            );
    END LOOP; 
    
   WHEN  UPPER(strips.STRIP_TENOR)='DECEMBER'   THEN 
   For recs in  to_number(AYYYY_startmonth) .. to_Number(AYYYY_endmonth)
    LOOP
       vc_AYYYYMM := to_CHAR(recs)||'12';

       Prepare_manual_qts(
                              pcobdate          => pcobdate
                            , pmonthlym2m       => manual_m2m_tab
                            , pmanual_quote_id  =>  strips.EXT_MANUAL_QUOTE_ID
                            , pstrip_tenor      => strips.STRIP_TENOR 
                            , pextCategory      => strips.EXT_CATEGORY
                            , pDatasourceName   => strips.DATA_SOURCE_NAME
                            , pMID              => Strips.MID
                            , pBID              => Strips.BID 
                            , pASK              => Strips.ASK
                            , punderlyingcom    => strips.UNDERLYING_COMMODITY
                            , ppricevolind      => strips.price_vol_indicator
                            , plocation         => strips.NEER_LOCATION
                            , pcurrentMonth      => vc_CurrentMonth
                            , pstart_Month      => AYYYY_startmonth 
                            , pend_month        => AYYYY_endmonth 
                            , pmnthly_collectn  => manual_collection
                            , pout_rtn_code     => vn_rtn_code  
                            , pout_rtn_msg   => vc_rtn_msg
                            );
    END LOOP; 
    
   WHEN tenors.balance_flag = 'Y' THEN
--     ----dbms_output.put_line('Balanced '||AYYYY_startmonth||'-'||AYYYY_Endmonth);
     
       Prepare_manual_qts(
                              pcobdate          => pcobdate
                            , pmonthlym2m       => manual_m2m_tab
                            , pmanual_quote_id  =>  strips.EXT_MANUAL_QUOTE_ID
                            , pstrip_tenor      => strips.STRIP_TENOR 
                            , pextCategory      => strips.EXT_CATEGORY
                            , pDatasourceName   => strips.DATA_SOURCE_NAME
                            , pMID              => Strips.MID
                            , pBID              => Strips.BID 
                            , pASK              => Strips.ASK
                            , punderlyingcom    => strips.UNDERLYING_COMMODITY
                            , ppricevolind      => strips.price_vol_indicator
                            , plocation         => strips.NEER_LOCATION
                            , pcurrentMonth      => vc_CurrentMonth
                            , pstart_Month      => AYYYY_startmonth 
                            , pend_month        => AYYYY_endmonth 
                            , pmnthly_collectn  => manual_collection
                            , pout_rtn_code     => vn_rtn_code  
                            , pout_rtn_msg   => vc_rtn_msg
                            );
   
   WHEN tenors.balance_flag = 'N' THEN
   
   
    For recs in  to_number(AYYYY_startmonth) .. to_Number(AYYYY_endmonth)
    LOOP
 
     ----dbms_output.put_line('Non Balance '||AYYYY_startmonth||'-'||AYYYY_Endmonth);
       

      vc_AYYYY := to_CHAR(recs);

      
      
      CASE
      WHEN  INSTR(Upper(tenors.Start_month), 'PYYYYF',1,1) > 0 THEN 
        VC_STARTAYYYYMM := REPLACE (Upper(tenors.Start_month), 'PYYYYF', vc_AYYYY );
      WHEN  INSTR(Upper(tenors.Start_month), 'AYYYYF',1,1) > 0 THEN  
         VC_STARTAYYYYMM := REPLACE (Upper(tenors.Start_month), 'AYYYYF', vc_AYYYY );
      ELSE
         ------dbms_output.put_line('Non balnced skipped'||tenors.Start_month||tenors.End_month);
         null;
      END CASE;
                 
         
      IF  TO_NUMBER(VC_STARTAYYYYMM ) > to_Number(vc_CurrentMonth) THEN 
                  -- means strip not yet started 
                  
 
                  CASE
                  WHEN  INSTR(Upper(tenors.Start_month), 'PYYYYF',1,1) > 0 THEN 
                  VC_STARTAYYYYMM := REPLACE (Upper(tenors.Start_month), 'PYYYYF', vc_AYYYY );
                  
                  WHEN  INSTR(Upper(tenors.Start_month), 'AYYYYF',1,1) > 0 THEN  
                     VC_STARTAYYYYMM := REPLACE (Upper(tenors.Start_month), 'AYYYYF', vc_AYYYY );
                  ELSE
                    -- ----dbms_output.put_line('Non balnced skipped'||tenors.Start_month||tenors.End_month);
                    null;
                  END CASE;
               
               
                  CASE
                  WHEN  INSTR(Upper(tenors.End_month), 'PYYYYF',1,1) > 0 THEN 
                  
                  VC_ENDAYYYYMM := REPLACE (Upper(tenors.End_month), 'PYYYYF', vc_AYYYY );
                  
                                    
                  WHEN  INSTR(Upper(tenors.Start_month), 'AYYYYF',1,1) > 0 THEN  
                     VC_ENDAYYYYMM := REPLACE (Upper(tenors.End_month), 'AYYYYF', vc_AYYYY );
                  ELSE
                    -- ----dbms_output.put_line('Non balnced skipped'||tenors.Start_month||tenors.End_month);
                    null; 
                  END CASE;
           
            
                                   
      ELSE
                  ---   strip starting time could be between this year or prompt Year
                                
                  VC_STARTAYYYYMM := vc_PromptMonth;
                  CASE
                  WHEN  INSTR(Upper(tenors.End_month), 'PYYYYF',1,1) > 0 THEN 
                  
                  VC_ENDAYYYYMM := REPLACE (Upper(tenors.End_month), 'PYYYYF', vc_AYYYY );
                  
                                    
                  WHEN  INSTR(Upper(tenors.Start_month), 'AYYYYF',1,1) > 0 THEN  
                     VC_ENDAYYYYMM := REPLACE (Upper(tenors.End_month), 'AYYYYF', vc_AYYYY );
                  ELSE
                    -- ----dbms_output.put_line('Non balnced skipped'||tenors.Start_month||tenors.End_month);
                    null;
                  END CASE;
      
      
      END IF; 
      
--      ----dbms_output.PUT_LINE('Calling prepare manual ');

          Prepare_manual_qts(
                              pcobdate          => pcobdate
                            , pmonthlym2m       => manual_m2m_tab
                            , pmanual_quote_id  =>  strips.EXT_MANUAL_QUOTE_ID
                            , pstrip_tenor      => strips.STRIP_TENOR 
                            , pextCategory      => strips.EXT_CATEGORY
                            , pDatasourceName   => strips.DATA_SOURCE_NAME
                            , pMID              => Strips.MID
                            , pBID              => Strips.BID 
                            , pASK              => Strips.ASK
                            , punderlyingcom    => strips.UNDERLYING_COMMODITY
                            , ppricevolind      => strips.price_vol_indicator
                            , plocation         => strips.NEER_LOCATION
                            , pcurrentMonth      => vc_CurrentMonth
                            , pstart_Month     => VC_StartAYYYYMM 
                            , pend_month       => VC_ENDAYYYYMM 
                            , pmnthly_collectn  => manual_collection
                            , pout_rtn_code  => vn_rtn_code  
                            , pout_rtn_msg   => vc_rtn_msg
                            );
      
    end LOOP; -- end of contract years loop 
   
      
   ELSE
      null;
   END CASE; 
 
   END LOOP;-- end of tenors cursor
 
 
 ELSE 
     --related to custom quotes
 --    ----dbms_output.PUT_LINE(strips.TO_DATE||strips.FROM_DATE);
     
         IF strips.TO_DATE  > strips.FROM_DATE THEN 
         
                       Prepare_manual_qts(
                              pcobdate          => pcobdate
                            , pmonthlym2m       => manual_m2m_tab
                            , pmanual_quote_id  =>  strips.EXT_MANUAL_QUOTE_ID
                            , pstrip_tenor      => strips.STRIP_TENOR 
                            , pextCategory      => strips.EXT_CATEGORY
                            , pDatasourceName   => strips.DATA_SOURCE_NAME
                            , pMID              => Strips.MID
                            , pBID              => Strips.BID 
                            , pASK              => Strips.ASK
                            , punderlyingcom    => strips.UNDERLYING_COMMODITY
                            , ppricevolind      => strips.price_vol_indicator
                            , plocation         => strips.NEER_LOCATION
                            , pcurrentMonth      => vc_CurrentMonth
                            , pstart_Month     => strips.FROM_DATE 
                            , pend_month       => strips.TO_DATE 
                            , pmnthly_collectn  => manual_collection
                            , pout_rtn_code  => vn_rtn_code  
                            , pout_rtn_msg   => vc_rtn_msg
                            );
        END IF;
    
 END IF;
  
   
  

     
     
   end loop;    -- Manual Quotes end loop          
   
       /*** RISKDB.QP_STRIP_LVL_NEER_EXT_DATA ***/ 
     
      /* This COB_DATE data is not finalized , so clean up  */
      PROCESS_LOG_REC.MESSAGE      := 'Clean up records from '
                                      ||'RISKDB.QP_STRIP_LVL_NEER_EXT_DATA'
                                      ||vc_cobdate ;
      --vc_section := 'Clean up records from RISKDB.QP_STRIP_LVL_NEER_EXT_DATA '||PCOBDATE;
      
--      ----dbms_output.Put_line(PROCESS_LOG_REC.MESSAGE);
      
      /* This COB_DATE data is not finalized , so clean up RISKDB.QP_INT_FWD_CURVES_RAW_DATA  */
      vc_sql_stmt := 'DELETE FROM RISKDB.QP_STRIP_LVL_NEER_EXT_DATA '
                     ||' WHERE COB_DATE = TO_DATE('
                     ||vc_singlequote||vc_cobdate ||vc_singlequote||', '
                     ||vc_singlequote|| 'DD-MON-YYYY'||vc_singlequote
                     ||')'
                     ||' AND MANUAL_QUOTE_FLAG = '||vc_singlequote||'Y'
                     ||vc_singlequote
--                     ||' AND NEER_LOCATION = ' ||vc_singlequote||'NYISO-CAPITL-2x16'
--                     ||vc_singlequote
                     ;

      vn_rtn_code := NULL;
      vc_rtn_msg := '';  
     
     DELETE_RECORDS ( pDelete_statement => vc_sql_stmt 
                      ,pout_rtn_code => vn_rtn_code      
                      ,pout_rtn_msg => vc_rtn_msg  
                      );


     
     IF vn_rtn_code <> c_success THEN 
       RAISE e_appl_error;
     END IF; 
        
     Load_target_stage3(
       pmnthly_collectn   => manual_Collection
      , pout_rtn_code    => vn_rtn_code    
      , pout_rtn_msg     => vc_rtn_msg 
     );
      
     IF vn_rtn_code <> c_success THEN 
        RAISE e_appl_error;
     END IF;       
    
    PROCESS_LOG_REC.STAGE        := 'LOAD_MANUAL_STRIPS';
    PROCESS_LOG_REC.STATUS       := PROCESS_LOG_PKG.C_STATUS_COMPLETED;
    PROCESS_LOG_REC.MESSAGE      := 'Load_Manual_Strips Process Completed';
     
    write_log;  
   
    SELECT count(*) 
    INTO li_ERRORS
    FROM 
    DMR.PROCESS_LOG l
    WHERE
    application_Name = 'QVAR_APPS'
    and process_Name = 'QVAR_INTERNAL_MANUALQUOTES'
    and Parent_Name = 'QP_ZEMA_EXTRACT_PKG'
    and STATUS = 'ERRORS'
    and l.log_id > g_minlogid
   ;
    
   IF li_ERRORS > 0 THEN 
    
    PSTATUSCODE := 'SUCCESS';
    PSTATUSMSG := 'Process Completed with Errors, Please check Error Log DMR.PROCESS_LOG for details';
   
   ELSE
   
    PSTATUSCODE := 'SUCCESS';
    PSTATUSMSG := 'Process Completed Successfully';      
   
   END IF;
   
      
   
   EXCEPTION
  WHEN e_appl_error THEN 
  
    DMR.PROCESS_LOG_PKG.WRITE_LOG(
                        P_PROCESS_NAME => PROCESS_LOG_REC.PROCESS_NAME,
                        P_STAGE=> PROCESS_LOG_REC.STAGE,
                        P_STATUS=>PROCESS_LOG_PKG.C_STATUS_ERRORS,
                        P_MESSAGE=>NVL( vc_rtn_msg, SQLERRM) 
                        ); 
                         
    PSTATUSCODE := 'ERRORS';
    PSTATUSMSG := 'Process Completed with Errors, Please check Error Log DMR.PROCESS_LOG for details';
                        
  WHEN OTHERS THEN 
      
        DMR.PROCESS_LOG_PKG.WRITE_LOG(
            P_PROCESS_NAME => PROCESS_LOG_REC.PROCESS_NAME,
            P_STAGE=> PROCESS_LOG_REC.STAGE,
            P_STATUS=>PROCESS_LOG_PKG.C_STATUS_ERRORS,
            P_MESSAGE=>substr(PROCESS_LOG_REC.MESSAGE||SQLERRM , 1,250)
            );   

    PSTATUSCODE := 'ERRORS';
    PSTATUSMSG := 'Process Completed with Errors, Please check Error Log DMR.PROCESS_LOG for details';

                            
End Load_Manual_strips;


Procedure parse_strip (
                        PCOBDATE         IN  DATE
                       , ppromptMonth    IN VARCHAR2 
                       , pstartFormula   IN VARCHAR2
                       , pendFormula     IN VARCHAR2
                       , pAddMonths      IN NUMBER
                       , pcontractYear   IN VARCHAR2
                       , pstartYRPattern IN VARCHAR2
                       , pendYRPattern   IN VARCHAR2
                       , pstartValue     OUT NOCOPY VARCHAR2                       
                       , pendValue       OUT NOCOPY VARCHAR2            
                       ,pout_rtn_code  OUT  NOCOPY NUMBER  
                       ,pout_rtn_msg   OUT  NOCOPY VARCHAR2 
                      )
IS 
vc_startValue                   VARCHAR2(10);
vc_promptValue                  VARCHAR2(10);
vc_temp                         VARCHAR2(50); 
                      
begin


              vc_temp := Upper(pstartFormula) ;
              
              --strip  MAX( pattern 
             IF INSTR(Upper(pstartFormula) , 'MAX(' , 1, 1 ) > 0 THEN  
              vc_temp := REPLACE ( Upper(pstartFormula) , 'MAX(' ) ;
             ELSE 
              vc_temp :=Upper(pstartFormula) ;
             END IF;
              
--              DBMS_OUTPUT.PUT_LINE ('MAX( stripped = '||vc_temp);
              
              -- get the string pattern from 1 to comma position -1 
              vc_startValue := SUBSTR(ltrim(vc_temp) , 
                                     1 , 
                                     INSTR(ltrim(vc_temp) , ',',1,1) -1
                                    );
              
--              DBMS_OUTPUT.PUT_LINE ('start Value  = '||vc_startValue);
               
               
              vc_startValue := REPLACE(
                                     vc_startValue  
                                     , pstartYRPattern
                                     , pcontractyear
                                    );
                                    
--             DBMS_OUTPUT.PUT_LINE ('start Value after replace CY = '||vc_startValue);
              
                                                          
              vc_promptValue := ppromptMonth ;
            
            
                  IF to_number(vc_startValue)    > to_number(vc_promptValue) THEN 
                        pstartValue  := vc_startValue;   
                  ELSE
                        pstartValue  := vc_promptValue; 
                  END IF;
              
     
                                          
--              DBMS_OUTPUT.PUT_LINE ('start Month after MAX evaluation = '||vc_StartAYYYYMM);
          CASE
          WHEN  pendformula IS NOT NULL and paddMOnths is NULL THEN
             pendValue :=  REPLACE(
                                     Upper(pendformula) 
                                     , pendYRPattern
                                     , pcontractyear
                                    );
          WHEN pendformula IS NULL and paddMOnths is NOT NULL THEN
            pendValue :=  TO_CHAR(ADD_MONTHS(TO_DATE( vc_startValue||'01', 'YYYYMMDD') 
                                            ,  paddMOnths 
                                            )
                                         , 'YYYYMM'
                                 ) ;
          
          END CASE;
          

    pout_rtn_code := c_success;
    pout_rtn_msg := ''; --Null means Success 
    
    
end parse_strip;


Procedure Construct_Locm2m_object(
                              pcobdate                          IN DATE
                            , pNettingGroup                     IN NUMBER
                            , pUnderlyingCommodity              IN VARCHAR2
                            , pCommodity                        IN VARCHAR2
                            , pOverridelocation                 IN VARCHAR2
                            , POriginalLocation                 IN VARCHAR2
                            , ppricevol                         IN VARCHAR2
                            , pLocbbflag                        IN VARCHAR2
                            , poverridebbflag                   IN VARCHAR2  
                            , pmonthlym2m       IN OUT NOCOPY m2m_by_Loc_tab 
                            , pout_rtn_code      OUT  NOCOPY NUMBER  
                            , pout_rtn_msg       OUT  NOCOPY VARCHAR2
                            ) 
is
   vc_message       VARCHAR2(1000);
   vn_reccount      NUMBER;
   vc_cobdate       VARCHAR2(12) := to_CHAR(pcobdate, 'DD-MON-YYYY');
Begin

   ----dbms_output.PUT_LINE ('Started msg here ');
     
     --Set Process Name and status for the Log Process                   
    PROCESS_LOG_REC.STAGE        := 'Construct_Locm2m_object';
    PROCESS_LOG_REC.STATUS       := PROCESS_LOG_PKG.C_STATUS_STARTED;
    PROCESS_LOG_REC.MESSAGE      := 'Construct_Locm2m_object Started,  '||' cobdate='||vc_cobdate;
    
 
   -- Log Message first
   write_log;
    
   -- --dbms_output.PUT_LINE ('after write log ');
   
    BEGIN

    --- Limit Object construction only to a mapped Locations 
    -- Data 

    Select 
    m2m_by_Loc_T(
      COB_DATE                          =>  v.COB_DATE             
     , NETTING_GROUP_ID                 =>  v.NETTING_GROUP_ID         
     , UNDERLYING_COMMODITY             =>  v.UNDERLYING_COMMODITY     
     , COMMODITY                        =>  v.COMMODITY        
     , LOCATION                         =>  v.LOCATION        
     , PRICE_VOL_INDICATOR              =>  v.PRICE_VOL_INDICATOR     
     , CONTRACTYEAR_MONTH               =>  v.CONTRACTYEAR_MONTH      
     , CONTRACT_YEAR                    => v.CONTRACT_YEAR      
     , CONTRACT_MONTH                   => v.CONTRACT_MONTH      
     , LOCATION_DELTA_POSITION          => v.LOCATION_DELTA_POSITION  
     , COMMODITY_ABSOLUTE_POSITION      => v.COMMODITY_ABSOLUTE_POSITION
     , M2M_VALUE                        => v.M2M_VALUE        
     , LEGGED_M2M_VALUE                 =>  v.LEGGED_M2M_VALUE       
     , ORIGINAL_FAS157_LEVEL            => v.ORIGINAL_FAS157_LEVEL      
     , ORIGINAL_FAS157_PERCENT          =>  v.ORIGINAL_FAS157_PERCENT  
     , FINAL_FAS157_VALUE               =>   v.FINAL_FAS157_VALUE    
     , FINAL_FAS157_TYPE                =>   v.FINAL_FAS157_TYPE      
     , PROJ_LOC_AMT                     =>   v.PROJ_LOC_AMT     
     , ON_OFF_PEAK_INDICATOR            =>   v.ON_OFF_PEAK_INDICATOR    
     , HOUR_TYPE                        =>   v.HOUR_TYPE     
     , TOT_HOURS_PER_CONTRACT_MONTH     => v.TOT_HOURS_PER_CONTRACT_MONTH
     , CAL_DAYS_IN_CONTRACT_MONTH       => v.CAL_DAYS_IN_CONTRACT_MONTH 
     , INITIAL_RANK                     => v.INITIAL_RANK 
     , BASISPOINT_OVERRIDE_ID           => v.BASISPOINT_OVERRIDE_ID 
     , BASISPOINT_OVERRIDE_INDICATOR    => v.BASISPOINT_OVERRIDE_INDICATOR
     , FAS157_OVERRIDE_INDICATOR        => v.FAS157_OVERRIDE_INDICATOR 
     , BASIS_CURVE_INDICATOR            => v.BASIS_CURVE_INDICATOR 
     , PHOENIX_BASIS_NAME               => v.PHOENIX_BASIS_NAME
     , BASIS_QUOTE_DATE                 => v.BASIS_QUOTE_DATE 
     , BASIS_QUOTE_AGE                  => v.BASIS_QUOTE_AGE
     , TRADER_BASIS_QUOTE_PRICE         => v.TRADER_BASIS_QUOTE_PRICE
     , USING_100PRCNT_HIST_METHOD       => v.USING_100PRCNT_HIST_METHOD
     , EXT_PROVIDER_ID                  => v.EXT_PROVIDER_ID    
     , DATASOURCE_CATEGORY              => v.EXT_DATA_SOURCE_CATEGORY            
     , EXT_PROFILE_ID                   => v.EXT_PROFILE_ID   
     , EXT_LOCATION                     => v.EXTERNAL_COLUMN_VALUE
     , PRICE_VOL_PROFILE                => v.PRICE_VOL_PROFILE 
     , ZEMA_PROFILE_ID                  => v.ZEMA_PROFILE_ID   
     , ZEMA_PROFILE_DISPLAY_NAME        => v.ZEMA_PROFILE_DISPLAY_NAME
     , FULLY_VALIDATED_FLAG             => v.FULLY_VALIDATED_FLAG  
     , TERM_START                       =>  V.TERM_START
     , TERM_END                         =>  V.TERM_END
     , BASIS_ASK                        =>  V.BASIS_ASK
     , BASIS_BID                        =>  V.BASIS_BID
     , BROKER                           =>  V.BROKER 
     , MONTHLY_VOL                      =>  V.MONTHLY_VOL    
     , PROJ_LOCATION_AMT                =>  V.PROJ_LOCATION_AMT
     , PROJ_PREMIUM_AMT                 =>  V.PROJ_PREMIUM_AMT
     , PROJ_BASIS_AMT                   =>  V.PROJ_BASIS_AMT
     , MAX_CONTRACTYEAR_MONTH           =>  V.MAX_CONTRACTYEAR_MONTH
     , MIN_CONTRACTYEAR_MONTH           =>  V.MIN_CONTRACTYEAR_MONTH
     , TOT_CONTRACTYEAR_MONTH           =>  V.TOT_CONTRACTYEAR_MONTH 
     , Backbone_flag                    =>  V.BACKBONE_FLAG
     , Internal_price                   =>  V.INTERNAL_PRICE
     , ABSOLUTE_M2M_VALUE               => v.ABS_M2M_VALUE_AMT 
     , ABSOLUTE_LEGGED_M2M_VALUE        => v.ABS_M2M_LEG_VALUE_AMT
     , ABSOLUTE_M2M_VALUE_VOL           => v.ABS_M2M_VALUE_AMT_VOL
     , ABSOLUTE_LEGGED_M2M_VALUE_VOL    => v.ABS_M2M_LEG_VALUE_AMT_VOL
     , EXCEPTION_PRICE_FLAG             => 'N'
     , OVERRIDE_BASISPOINT              => pOverridelocation
     , OVERRIDE_COMMODITY              =>  v.OVERRIDE_COMMODITY
     , OVERRIDE_HOUR_TYPE               => v.OVERRIDE_HOUR_TYPE
     , OVERRIDE_HOURS                   => v.OVERRIDE_HOURS
     , OVERRIDE_PRICE                   => v.OVERRIDE_PRICE
     , OVERRIDE_BACKBONE_FLAG           => v.OVERRIDE_BACKBONE_FLAG
     , OVERRIDE_ORIG_FAS_LEVEL          => v.OVERRIDE_ORIG_FAS_LEVEL
    ) BULK COLLECT INTO  pmonthlym2m
    FROM 
    ( 
WITH
valuemap as (
 SELECT 
  ep.EXT_PROVIDER_ID                  
 , ep.EXT_DATA_SOURCE_CATEGORY                 
 , epr.EXT_PROFILE_ID 
 , NV.INTERNAL_COLUMN_VALUE                
 , nv.EXTERNAL_COLUMN_VALUE                   
 , epr.PRICE_VOL_INDICATOR  PRICE_VOL_PROFILE        
 , epr.ZEMA_PROFILE_ID              
 , epr.ZEMA_PROFILE_DISPLAY_NAME 
 from 
 RISKDB.QP_EXT_NEER_VALUE_MAPPINGS nv
 , RISKDB.QP_EXT_PROFILES epr
 , RISKDB.QP_EXT_PROVIDERS ep
 where
 nv.EXT_PROFILE_ID = epr.ext_profile_id
 and NV.INTERNAL_COLUMN_VALUE =  NVL(PoverrideLocation, pOriginalLocation)
 --and epr.zema_profile_id = pzemaid --180
 and epr.EXT_PROVIDER_ID = ep.EXT_PROVIDER_ID
 and nv.active_flag = 'Y'
 and epr.active_flag = 'Y'
 and ep.ACTIVE_FLAG = 'Y'
 AND epr.SOURCE_SYSTEM = 'QVAR'
-- and TO_DATE('04-MAY-2015','DD-MON-YYYY') Between nv.Effective_start_date and  NVL(nv.Effective_end_date, TO_DATE('04-MAY-2015','DD-MON-YYYY'))
-- and TO_DATE('04-MAY-2015','DD-MON-YYYY') Between epr.Effective_start_date and  NVL(epr.Effective_end_date, TO_DATE('04-MAY-2015','DD-MON-YYYY'))
-- and TO_DATE('04-MAY-2015','DD-MON-YYYY') Between ep.Effective_start_date and  NVL(ep.Effective_end_date, TO_DATE('04-MAY-2015','DD-MON-YYYY'))
 and pcobdate Between nv.Effective_start_date and  NVL(nv.Effective_end_date, pcobdate)
 and pcobdate Between epr.Effective_start_date and  NVL(epr.Effective_end_date, pcobdate)
 and pcobdate Between ep.Effective_start_date and  NVL(ep.Effective_end_date, pcobdate)
)
SELECT 
      rd.COB_DATE
    , rd.netting_group_id                         
    , rd.UNDERLYING_COMMODITY           
    , rd.COMMODITY                      
    , rd.LOCATION         
    , rd.PRICE_VOL_INDICATOR
    , rd.CONTRACTYEAR_MONTH             
    , rd.CONTRACT_YEAR                  
    , rd.CONTRACT_MONTH                
    , rd.LOCATION_DELTA_POSITION     
    , rd.COMMODITY_ABSOLUTE_POSITION
    , rd.M2M_VALUE                   
    , rd.LEGGED_M2M_VALUE                      
    , rd.ORIGINAL_FAS157_LEVEL          
    , rd.ORIGINAL_FAS157_PERCENT        
    , rd.FINAL_FAS157_VALUE             
    , rd.FINAL_FAS157_TYPE
    , rd.PROJ_LOC_AMT                    
    , rd.ON_OFF_PEAK_INDICATOR                       
    , rd.HOUR_TYPE                   
    , rd.TOT_HOURS_PER_CONTRACT_MONTH
    , rd.CAL_DAYS_IN_CONTRACT_MONTH   
    , rd.INITIAL_RANK   
     , rd.BASISPOINT_OVERRIDE_ID         
     , rd.BASISPOINT_OVERRIDE_INDICATOR  
     , rd.FAS157_OVERRIDE_INDICATOR      
     , rd.BASIS_CURVE_INDICATOR           
     , rd.PHOENIX_BASIS_NAME             
     , rd.BASIS_QUOTE_DATE               
     , rd.BASIS_QUOTE_AGE                
     , rd.TRADER_BASIS_QUOTE_PRICE       
     , rd.USING_100PRCNT_HIST_METHOD                     
     , valuemap.EXT_PROVIDER_ID                  
     , valuemap.EXT_DATA_SOURCE_CATEGORY                 
     , valuemap.EXT_PROFILE_ID                 
     , valuemap.EXTERNAL_COLUMN_VALUE
     , valuemap.PRICE_VOL_PROFILE        
     , valuemap.ZEMA_PROFILE_ID              
     , valuemap.ZEMA_PROFILE_DISPLAY_NAME    
     , 'N' FULLY_VALIDATED_FLAG
     , rd.TERM_START                 
     , rd.TERM_END                  
     , rd.BASIS_ASK                 
     , rd.BASIS_BID                 
     , rd.BROKER   
     , rd.MONTHLY_VOL    
     , rd.PROJ_LOCATION_AMT
     , rd.PROJ_PREMIUM_AMT
     , rd.PROJ_BASIS_AMT
     , rd.MAX_CONTRACTYEAR_MONTH
     , rd.MIN_CONTRACTYEAR_MONTH
     , rd.TOT_CONTRACTYEAR_MONTH 
     , pLocbbflag Backbone_flag     
     , rd.Internal_price    
     , rd.ABS_M2M_VALUE_AMT              
     , rd.ABS_M2M_LEG_VALUE_AMT       
     , rd.ABS_M2M_VALUE_AMT_VOL              
     , rd.ABS_M2M_LEG_VALUE_AMT_VOL   
     , pOverridelocation OVERRIDE_BASISPOINT
     , bpo.OVERRIDE_COMMODITY
     , NULL Override_Hour_Type    
     ,( CASE
       WHEN rd.underlying_commodity = 'ELECTRICITY'  THEN 
          RISKDB.qp_zema_extract_pkg.Get_Hours (      
                         pLocation          => pOverrideLocation 
                       , PHourType          => NULL -- process will derive Hour Type 
                       , pcontractYearMonth => rd.CONTRACTYEAR_MONTH
              )     
       ELSE
         NULL
       END
     ) Override_Hours 
    , ( CASE
      WHEN  ppricevol = 'PRICE' THEN 
          ( SELECT 
           (
            CASE
            WHEN pOverrideLocation is NOT NULL and poverridebbflag = 'Y' THEN PROJ_LOCATION_AMT
            WHEN pOverrideLocation IS NULL and pLocbbflag = 'Y' THEN PROJ_LOCATION_AMT 
            ELSE
               PROJ_BASIS_AMT
            END
           ) 
           FROM RISKDB.PROJECTION_CURVES
           WHERE   
           EFFECTIVE_DATE =  pcobdate 
           AND PARTITION_BIT =2
           AND BASIS_POINT = pOverrideLocation
           and commodity = bpo.OVERRIDE_COMMODITY
           AND contract_month = rd.CONTRACTYEAR_MONTH 
          ) 
      WHEN  ppricevol = 'VOL' THEN
      (
       SELECT DAILY_VOL
       FROM
       RISKDB.PROJ_LOCATION_VOL_CURVE
       WHERE
       Partition_bit = 2
       and EFFECTIVE_DATE =  pcobdate
       and BASIS_POINT = pOverrideLocation
       and commodity = bpo.override_commodity
       and contract_month = rd.CONTRACTYEAR_MONTH 
      )  
      END
     ) Override_price
    ,  poverridebbflag OVERRIDE_BACKBONE_FLAG
    ,  RISKDB.qp_zema_extract_pkg.Get_Monthly_Original_FAS_level (


          pLocation          => pOverrideLocation 
        , pcommodity        =>  bpo.Override_commodity  
        , Pcobdate          =>  pcobdate -- process will derive Hour Type 
        , pcontractYearMonth => rd.CONTRACTYEAR_MONTH




         ) OVERRIDE_ORIG_FAS_LEVEL
    FROM
        RISKDB.QP_INT_FWD_CURVES_RAW_DATA rd
        LEFT OUTER join RISKDB.qp_basis_point_override bpo
        ON (
            bpo.Basis_point = rd.Location
            and pcobdate Between bpo.Effective_start_date and  NVL(bpo.Effective_end_date, pcobdate)
            and bpo.active_flag = 'Y'
            AND bpo.PRICE_VOL_INDICATOR =rd.PRICE_VOL_INDICATOR
        ) 
        LEFT OUTER join valuemap
        ON 
        (
            (  
            CASE 
            WHEN  bpo.override_basis_point IS NULL THEN rd.Location
            ELSE
                 bpo.override_basis_point
            END
            ) = valuemap.INTERNAL_COLUMN_VALUE
            and rd.PRICE_VOL_INDICATOR = valuemap.PRICE_VOL_PROFILE
         )
        WHERE
        rd.cob_date = pcobdate --to_date('01/07/2015', 'MM/DD/YYYY')--
        and to_number(rd.contractYear_Month) > to_number(to_char(pcobdate, 'YYYYMM'))
        and rd.Netting_group_id = pNettingGroup
        and rd.underlying_commodity = pUnderlyingCommodity
        and rd.commodity = pCommodity
        and rd.LOCATION =  pOriginallocation
        and rd.PRICE_VOL_INDICATOR = ppricevol
--        and rd.contractYear_Month =  '201601'
--        and valuemap.EXT_PROFILE_ID = 11
        and rd.active_flag = 'Y'  
        )   v 
    ;

    Select count(*) into vn_reccount
    from 
    TABLE(pmonthlym2m)
    ;

    vc_message := vc_cobdate||'-total_recs->'||to_CHAR(vn_reccount);

--       DMR.PROCESS_LOG_PKG.WRITE_LOG(
--                P_PROCESS_NAME => PROCESS_LOG_REC.PROCESS_NAME,
--                P_STAGE=> PROCESS_LOG_REC.STAGE,
--                P_STATUS=>PROCESS_LOG_PKG.C_STATUS_INFO,
--                P_MESSAGE=>vc_message
--        );
        
     dbms_output.PUT_LINE ( vc_message);
 
           pout_rtn_code := c_success;
           pout_rtn_msg := ''; --Null means Success
                            
    EXCEPTION
     WHEN NO_DATA_FOUND THEN 
     
        pout_rtn_code := c_success;
        pout_rtn_msg := ''; --Null means Success
           
        vc_message := 'There are no Records available for CobDAte = '||vc_cobdate
       ;
       
       --dbms_output.PUT_LINE ( vc_message); 
       
       DMR.PROCESS_LOG_PKG.WRITE_LOG(
                P_PROCESS_NAME => PROCESS_LOG_REC.PROCESS_NAME,
                P_STAGE=> PROCESS_LOG_REC.STAGE,
                P_STATUS=>PROCESS_LOG_PKG.C_STATUS_INFO,
                P_MESSAGE=>vc_message
        );
       
              
    END;

   
Exception 
 WHEN OTHERS THEN 
 
       vc_message := SUBSTR(SQLERRM,1,1000);
      
       pout_rtn_code := c_failure;
       pout_rtn_msg := vc_message; --Null means Success
   
   
       DMR.PROCESS_LOG_PKG.WRITE_LOG(
            P_PROCESS_NAME => PROCESS_LOG_REC.PROCESS_NAME,
            P_STAGE=> PROCESS_LOG_REC.STAGE,
            P_STATUS=>PROCESS_LOG_PKG.C_STATUS_ERRORS,
            P_MESSAGE=>vc_message
       );      
         
end Construct_Locm2m_object;



Procedure  Construct_Locquotes_object(
                             pcobdate       IN DATE
                            , pProviderId    IN NUMBER 
                            , pProfileId    IN NUMBER
                            , pextLocation  IN VARCHAR2  
                            , pextquotes     IN OUT NOCOPY ext_quotes_tab 
                            , pout_rtn_code  OUT  NOCOPY NUMBER    
                            , pout_rtn_msg   OUT  NOCOPY VARCHAR2
                            ) 
is
            
vc_cobdate                      VARCHAR2(12) := TO_CHAR(pCOBDATE, 'DD-MON-YYYY');

Begin

    PROCESS_LOG_REC.STAGE        := 'Construct_Ext_quotes_object, '||' cobdate='||vc_cobdate;
    
BEGIN

    --- Limit Object construction only to a mapped Locations 
    -- Data 

    Select 
    ext_quotes_T (
    EXT_PROVIDER_QUOTE_ID => eq.EXT_PROVIDER_QUOTE_ID
    ,EXT_PROVIDER_ID      => eq.EXT_PROVIDER_ID
    ,EXT_PROFILE_ID       => eq.EXT_PROFILE_ID
    ,UNDERLYING_COMMODITY => eq.UNDERLYING_COMMODITY
    ,EXT_EFF_DATE         => eq.EXT_EFF_DATE 
    ,EXT_RAW_CONTRACT     => eq.EXT_RAW_CONTRACT
    ,MID                  => eq.MID
    ,BID                  => eq.BID
    ,ASK                  => eq.ASK
    ,STRIP_TENOR          => eq.STRIP_TENOR
    ,CONTRACT_MONTH       => eq.CONTRACT_MONTH
    ,CONTRACT_YEAR        => eq.CONTRACT_YEAR
    ,EXT_CATEGORY         => eq.EXT_CATEGORY
    ,EXT_PROVIDER         => eq.EXT_PROVIDER
    ,EXT_LOCATION         => eq.EXT_LOCATION
    ,NEER_HUB             => eq.NEER_HUB
    ,NEER_LOCATION        => eq.NEER_LOCATION
    ,PEAK_TYPE            => eq.PEAK_TYPE
    ,PROCESS_LEVEL        => eq.PROCESS_LEVEL
    ,EXT_HUB_MAPPED_FLAG  => eq.EXT_HUB_MAPPED_FLAG
    ,EXT_LOC_MAPPED_FLAG  => eq.EXT_LOC_MAPPED_FLAG
    ,NEER_LOC_MAPPED_FLAG => eq.NEER_LOC_MAPPED_FLAG
    ,NEER_HUB_MAPPED_FLAG => eq.NEER_HUB_MAPPED_FLAG
    ,STRIP_TENOR_MAPPED_FLAG => eq.STRIP_TENOR_MAPPED_FLAG    
    ,SECONDARY_CALCULATION_FLAG => eq.SECONDARY_CALCULATION_FLAG 
    ,ON_MID               => eq.ON_MID
    ,OFF_MID              => eq.OFF_MID
    ,ON_BID               => eq.ON_BID
    ,OFF_BID              => eq.OFF_BID    
    ,ON_ASK               => eq.ON_ASK    
    ,OFF_ASK              => eq.OFF_ASK
    ,PRICE_TYPE          =>  eq.PRICE_TYPE
    ,VALUE               =>  eq.VALUE
    ,HOUR_TYPE           => eq.HOUR_TYPE
    ) BULK COLLECT INTO  pextquotes
    FROM 
    (
       SELECT DISTINCT
        ext_q.EXT_PROVIDER_QUOTE_ID
        , epr.EXT_PROVIDER_ID
        , epr.EXT_PROFILE_ID
        , ext_q.UNDERLYING_COMMODITY
        , ext_q.EXT_EFF_DATE
        , ext_q.EXT_RAW_CONTRACT
        , ext_q.MID
        , ext_q.BID
        , ext_q.ASK
        , ext_q.STRIP_TENOR
        , ext_q.CONTRACT_MONTH
        , ext_q.CONTRACT_YEAR
        , ep.EXT_DATA_SOURCE_CATEGORY  EXT_CATEGORY
        , ep.EXT_PROVIDER  EXT_PROVIDER
        , ext_q.EXT_LOCATION
        , NULL            NEER_HUB
        , NULL            NEER_LOCATION
        , NULL peaK_Type
        , ext_q.PROCESS_LEVEL
        , ext_q.EXT_HUB_MAPPED_FLAG
        , 'Y' EXT_LOC_MAPPED_FLAG
        , 'Y' NEER_LOC_MAPPED_FLAG
        , ext_q.NEER_HUB_MAPPED_FLAG
        , ext_q.STRIP_TENOR_MAPPED_FLAG
        , ext_q.SECONDARY_CALCULATION_FLAG
        , ext_q.ON_MID
        , ext_q.OFF_MID
        , ext_q.ON_BID
        , ext_q.OFF_BID
        , ext_q.ON_ASK
        , ext_q.OFF_ASK
        , ext_q.PRICE_TYPE
        , ext_q.VALUE
        , ext_q.DATA_STATUS
        , ext_q.COMMENTS
        , ext_q.ACTIVE_FLAG
        , ext_q.CREATE_USER
        , ext_q.CREATE_DATE
        , ext_q.MODIFY_USER
        , ext_q.MODIFY_DATE
        , ext_q.VERSION
        , NULL HOur_type
      FROM  
        RISKDB.QP_EXT_PROVIDER_QUOTES ext_q
        INNER join RISKDB.QP_EXT_NEER_VALUE_MAPPINGS nv
        ON ( Upper(ext_q.EXT_Location) = Upper(nv.EXTERNAL_COLUMN_VALUE) )
        INNER Join RISKDB.QP_EXT_PROFILES epr
        ON (
        NV.ext_profile_id =  epr.EXT_PROFILE_ID
        and epr.EXT_PROFILE_ID = ext_q.ext_profile_id
       -- and rd.PRICE_VOL_INDICATOR = epr.PRICE_VOL_INDICATOR
        )
        INNER Join RISKDB.QP_EXT_PROVIDERS ep
        ON ( epr.EXT_PROVIDER_ID = ep.EXT_PROVIDER_ID ) 
       WHERE
        ext_q.EXT_EFF_date = pcobdate --to_date('06/30/2015', 'MM/DD/YYYY')
        and epr.SOURCE_SYSTEM ='QVAR' 
        and ext_q.ext_provider_id = pproviderId
        and ext_q.ext_profile_id = pprofileId
        and ext_q.Ext_Location = pextLocation
    --    and epr.Zema_profile_id = pzemaid
    --    and rd.LOCATION = 'ERCOT-HB_HOUSTON-5x16'
        and nv.External_column_value is not  NULL
    --    and epr.zema_profile_id = 181 -- limiting to one first to validate 
    --    and Upper(rd.location) Like Upper('NEPOOL-.H.INTERNAL_HUB-5x16%')
        and nv.active_flag = 'Y'
        and epr.active_flag = 'Y'
        and ep.active_flag = 'Y'
        and pcobdate Between nv.Effective_start_date and  NVL(nv.Effective_end_date, pcobdate)
        and pcobdate Between epr.Effective_start_date and  NVL(epr.Effective_end_date, pcobdate)
        and pcobdate Between ep.Effective_start_date and  NVL(ep.Effective_end_date, pcobdate)
--        and to_date('06/30/2015', 'MM/DD/YYYY') Between nv.Effective_start_date and  NVL(nv.Effective_end_date, to_date('06/30/2015', 'MM/DD/YYYY'))
--        and to_date('06/30/2015', 'MM/DD/YYYY') Between epr.Effective_start_date and  NVL(epr.Effective_end_date, to_date('06/30/2015', 'MM/DD/YYYY'))
--        and to_date('06/30/2015', 'MM/DD/YYYY') Between ep.Effective_start_date and  NVL(ep.Effective_end_date, to_date('06/30/2015', 'MM/DD/YYYY'))
        order by 
        ext_q.EXT_LOCATION
        , TO_NUMBER(ext_q.CONTRACT_YEAR)      
    ) eq
    ;

EXCEPTION
 WHEN NO_DATA_FOUND THEN 
   pout_rtn_code := c_failure;
   pout_rtn_msg := 'There are no External Quotes available for CobDAte = '||to_CHAR(pcobdate, 'DD-MON-YYYY')
   ;
   raise NO_DATA_FOUND;
END;


 
   pout_rtn_code := c_success;
   pout_rtn_msg := ''; --Null means Success


   
Exception 
 WHEN OTHERS THEN 

       pout_rtn_code := c_failure;
       pout_rtn_msg := NVL(pout_rtn_msg , SUBSTR(SQLERRM,1,200));
         
end Construct_Locquotes_object;

 --added by siri
PROCEDURE Get_proj_prices (
         pcobdate              IN             DATE,
         pOriginalLocation     IN            VARCHAR2,
         pOverrideLocation     IN            VARCHAR2 , 
         pnettingGroup         IN            NUMBER,
         ppricevol             IN            VARCHAR2,
         pstartYearMonth       IN            NUMBER,
         PEndYearMOnth         IN            NUMBER,
         pbackbone_flag        IN            VARCHAR2,
         pOverridebb_flag      IN            VARCHAR2 , 
         pmonthlym2m           IN OUT NOCOPY m2m_by_Loc_tab,
         pout_rtn_code            OUT NOCOPY NUMBER,
         pout_rtn_msg             OUT NOCOPY VARCHAR2
         )
 IS
 vn_rtn_code            NUMBER;
 vc_rtn_msg             VARCHAR2 (500);
 vn_hours               NUMBER;
 vn_OverrideHours       NUMBER;
 vc_obp_hourtype        VARCHAR2(100); -- HourType associated with Override Basis Point 
 vn_FASLevel            NUMBER;
 vn_OverrideFASLevel    NUMBER;
 Vn_startYearMonth      NUMBER;
 vn_endYearMOnth        NUMBER;        
 vl_recCount            NUMBER;
 li_rec_Index           NUMBER;
 vn_internal_price      NUMBER;
 vn_Override_price      NUMBER;
 vn_proj_loc_amt        NUMBER;
 vn_Proj_Location_amt   NUMBER;
 vn_proj_basis_amt      NUMBER;
 vn_proj_premium_amt    NUMBER;
 vn_cal_Days             NUMBER;
 Vc_LOCATION             VARCHAR2 (500);
 Location_sample_obj    m2m_by_Loc_tab;
         
                                                      
BEGIN
   
 vl_recCount := pmonthlym2m.COUNT;
-- Only need Location specific attribute Values , (remaining all attributes set to NULL to avoid duplicate entries )
-- so we can copy these values to any new elements we are adding to a data structure 


       SELECT m2m_by_Loc_T (
                   COB_DATE                        => v.COB_DATE,
                   NETTING_GROUP_ID                => v.NETTING_GROUP_ID,
                   UNDERLYING_COMMODITY            => v.UNDERLYING_COMMODITY,
                   COMMODITY                       => v.COMMODITY,
                   LOCATION                        => v.LOCATION,
                   PRICE_VOL_INDICATOR             => ppricevol,
                   CONTRACTYEAR_MONTH              => NULL,
                   CONTRACT_YEAR                   => NULL,
                   CONTRACT_MONTH                  => NULL,
                   LOCATION_DELTA_POSITION         => NULL,
                   COMMODITY_ABSOLUTE_POSITION     => NULL,
                   M2M_VALUE                       => NULL,
                   LEGGED_M2M_VALUE                => NULL,
                   ORIGINAL_FAS157_LEVEL           => NULL,
                   ORIGINAL_FAS157_PERCENT         => NULL,
                   FINAL_FAS157_VALUE              => NULL,
                   FINAL_FAS157_TYPE               => NULL,
                   PROJ_LOC_AMT                    => NULL,
                   ON_OFF_PEAK_INDICATOR           => v.ON_OFF_PEAK_INDICATOR,
                   HOUR_TYPE                       => v.HOUR_TYPE,
                   TOT_HOURS_PER_CONTRACT_MONTH    => NULL,
                   CAL_DAYS_IN_CONTRACT_MONTH      => NULL,
                   INITIAL_RANK                    => NULL,
                   BASISPOINT_OVERRIDE_ID          => NULL,
                   BASISPOINT_OVERRIDE_INDICATOR   => NULL,
                   FAS157_OVERRIDE_INDICATOR       => NULL,
                   BASIS_CURVE_INDICATOR           => NULL,
                   PHOENIX_BASIS_NAME              => NULL,
                   BASIS_QUOTE_DATE                => NULL,
                   BASIS_QUOTE_AGE                 => NULL,
                   TRADER_BASIS_QUOTE_PRICE        => NULL,
                   USING_100PRCNT_HIST_METHOD      => NULL,
                   EXT_PROVIDER_ID                 => v.EXT_PROVIDER_ID,
                   DATASOURCE_CATEGORY             => v.DATASOURCE_CATEGORY ,
                   EXT_PROFILE_ID                  => v.EXT_PROFILE_ID,
                   EXT_LOCATION                    => v.EXT_LOCATION ,
                   PRICE_VOL_PROFILE               => v.PRICE_VOL_PROFILE,
                   ZEMA_PROFILE_ID                 => v.ZEMA_PROFILE_ID,
                   ZEMA_PROFILE_DISPLAY_NAME       => v.ZEMA_PROFILE_DISPLAY_NAME,
                   FULLY_VALIDATED_FLAG            => NULL,
                   TERM_START                      => NULL,
                   TERM_END                        => NULL,
                   BASIS_ASK                       => NULL,
                   BASIS_BID                       => NULL,
                   BROKER                          => NULL,
                   MONTHLY_VOL                     => NULL,
                   PROJ_LOCATION_AMT               => NULL,
                   PROJ_PREMIUM_AMT                => NULL,
                   PROJ_BASIS_AMT                  => NULL,
                   MAX_CONTRACTYEAR_MONTH          => V.MAX_CONTRACTYEAR_MONTH,
                   MIN_CONTRACTYEAR_MONTH          => V.MIN_CONTRACTYEAR_MONTH,
                   TOT_CONTRACTYEAR_MONTH          => V.TOT_CONTRACTYEAR_MONTH,
                   Backbone_flag                   => NULL,
                   Internal_price                  => NULL,
                   ABSOLUTE_M2M_VALUE              => NULL,
                   ABSOLUTE_LEGGED_M2M_VALUE       => NULL,
                   ABSOLUTE_M2M_VALUE_VOL          => NULL,
                   ABSOLUTE_LEGGED_M2M_VALUE_VOL   => NULL,
                   EXCEPTION_PRICE_FLAG            => 'N' ,
                   OVERRIDE_BASISPOINT             =>  v.OVERRIDE_BASISPOINT ,
                   OVERRIDE_COMMODITY              => v.OVERRIDE_COMMODITY , 
                   OVERRIDE_HOUR_TYPE               => NULL ,
                   OVERRIDE_HOURS                   => NULL     ,
                   OVERRIDE_PRICE                   => NULL     ,
                   OVERRIDE_BACKBONE_FLAG           => v.OVERRIDE_BACKBONE_FLAG ,
                   OVERRIDE_ORIG_FAS_LEVEL          => NULL
    ) BULK COLLECT INTO  Location_sample_obj
    FROM  
    (
     SELECT DISTINCT
       COB_DATE                        ,
       NETTING_GROUP_ID                ,
       UNDERLYING_COMMODITY            ,
       COMMODITY                       ,
       LOCATION                        ,
       HOUR_TYPE                       ,
       EXT_PROVIDER_ID                 ,
       DATASOURCE_CATEGORY             ,
       EXT_PROFILE_ID                  ,
       EXT_LOCATION                    ,
       PRICE_VOL_PROFILE               ,
       ZEMA_PROFILE_ID                 ,
       ZEMA_PROFILE_DISPLAY_NAME       ,
       MAX_CONTRACTYEAR_MONTH          ,
       MIN_CONTRACTYEAR_MONTH          ,
       TOT_CONTRACTYEAR_MONTH          ,
       OVERRIDE_BASISPOINT             ,
       OVERRIDE_COMMODITY              ,
       OVERRIDE_BACKBONE_FLAG          ,
       ON_OFF_PEAK_INDICATOR
     FROM
     Table (pmonthlym2m) m
     where  
     m.LOCATION                 =    pOriginalLocation
     AND m.COB_DATE             =    pcobdate
     and m.netting_group_id     =    pNettingGroup
     and m.price_vol_indicator  =   ppricevol
     and to_number(m.CONTRACTYEAR_MONTH) Between   pstartyearMonth and PEndYearMOnth
     ) v
--     where  
--     v.LOCATION     =    pOriginalLocation
--     AND V.COB_DATE    =    pcobdate
--     and v.netting_group_id = pNettingGroup
--     and v.price_vol_indicator =   ppricevol
--     and to_number(v.CONTRACTYEAR_MONTH) Between   pstartyearMonth and PEndYearMOnth
   --  and rownum < 2
     ; 
 
--DBMS_OUTPUT.Put_line ('sampleobj count := '|| Location_sample_obj.COUNT 
--||'overLoc='||Location_sample_obj(1).OVERRIDE_BASISPOINT
--||'overcom='||Location_sample_obj(1).OVERRIDE_COMMODITY
--||'Loc='||Location_sample_obj(1).Location
--||'com='||Location_sample_obj(1).Commodity
--);


vn_startYearMonth := pstartYearMonth;
vn_EndYearMonth   := pEndYearMonth;
          
               
IF ppricevol ='PRICE' THEN 
 

LOOP

   EXIT WHEN  vn_startYearMonth > vn_EndYearMonth;  
  
   vn_hours                     := NULL;
   Vn_Overridehours             := NULL;
   vn_FASLevel                  := NULL; 
   vn_OverrideFASLevel          := NULL;
   vn_proj_loc_amt              := NULL;
   vn_Proj_Location_amt         := NULL;
   vn_proj_basis_amt            := NULL;
   vn_proj_premium_amt          := NULL;
   vn_cal_days                  := NULL;
   
 BEGIN
   
 
    SELECT 
    PROJ_LOC_AMT
    , PROJ_LOCATION_AMT
    , PROJ_BASIS_AMT
    , PROJ_PREMIUM_AMT
     ,TO_NUMBER (
                TO_CHAR (
                         LAST_DAY ( TO_DATE (to_char(vn_startYearMonth) || '01', 'YYYYMMDD') )
                         , 'DD'
                         )
              )   cal_days
    INTO 
       vn_proj_loc_amt
     , vn_Proj_Location_amt
     , vn_proj_basis_amt
     , vn_proj_premium_amt 
     , vn_cal_Days         
   FROM PROJECTION_CURVES
   WHERE   
   EFFECTIVE_DATE =  pcobdate 
   AND PARTITION_BIT =2
   AND COMMODITY = NVL(Location_sample_obj(1).OVERRIDE_COMMODITY,Location_sample_obj(1).COMMODITY)
   AND BASIS_POINT = NVL(pOverrideLocation, pOriginalLocation)
   AND contract_month = vn_startyearMonth ;
 
 EXCEPTION
 WHEN NO_DATA_FOUND THEN 
    
  pout_rtn_code := c_failure;
  pout_rtn_msg := 'STOP';
  raise; --- exit unconditionally 
 END;
           
           
  CASE
  WHEN pOverrideLocation IS NOT NULL AND pOverridebb_flag = 'Y' THEN 
     vn_internal_price := NULL; 
     vn_override_price := vn_PROJ_LOCATION_AMT;
     
  WHEN pOverrideLocation IS NULL AND pbackbone_flag  = 'Y' THEN
    vn_internal_price := vn_PROJ_LOCATION_AMT;
    vn_override_price := NULL;
  ELSE
    IF  pOverrideLocation IS NOT NULL THEN 
      vn_Override_price := vn_PROJ_BASIS_AMT;
      vn_internal_price := NULL;
    ELSE
      vn_Override_price := NULL ;
      vn_internal_price := vn_PROJ_BASIS_AMT;    
    END IF;
       
  END CASE;
 

 
  BEGIN 

    --- Check to confirm Monthly price is available for current Location
    -- in the collection 
    
     SELECT DISTINCT LOCATION INTO VC_LOCATION
     FROM TABLE(pmonthlym2m ) M2M
     WHERE 
     m2m.LOCATION =    NVL(pOverrideLocation, pOriginalLocation) 
     and m2m.netting_group_id = pNettingGroup
     and m2m.price_vol_indicator =   ppricevol
     and to_number(m2m.CONTRACTYEAR_MONTH) = vn_startyearMonth 
     and M2M.INTERNAL_PRICE = NVL(vn_Override_price , vn_internal_price)
      ;
      
--      DBMS_OUTPUT.PUT_LINE ( 'element found for Month='|| vn_startyearMonth
--      ||' and Location = '||pOriginalLocation||' pricevol='||ppricevol
--      ||' NG='||pNettingGroup
--      );
      
      EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
       
      DBMS_OUTPUT.PUT_LINE ( 'No element found for Month='|| vn_startyearMonth);
       Vn_hours := NULL;
       vn_OverrideHours := NULL;
       
       vn_FASLevel := NULL;
       vn_OverrideFASLevel := NULL; 
       
         IF   pOverrideLocation IS NOT NULL AND Location_sample_obj(1).UNDERLYING_COMMODITY = 'ELECTRICITY' THEN 
         
              Vn_Overridehours := Get_Hours (       
                 pLocation          => pOverrideLocation 
               , PHourType          => NULL
               , pcontractYearMonth => vn_startyearMonth
               ) ;
               
 

         ELSE
         
          IF Location_sample_obj(1).UNDERLYING_COMMODITY = 'ELECTRICITY' THEN 
             Vn_hours := Get_Hours (       
                 pLocation          => POriginalLocation
               , PHourType          => Location_sample_obj(1).HOUR_TYPE
               , pcontractYearMonth => vn_startyearMonth
               ) ;

 
          END IF;
                     
         END IF;      
       
    
      -- Get Current FAS Level for Original Location for the  current Month     
        vn_FASLevel :=    RISKDB.qp_zema_extract_pkg.Get_Monthly_Original_FAS_level (
             pLocation             =>  POriginalLocation
            , pcommodity           => Location_sample_obj(1).COMMODITY
            ,pcobdate              => pcobdate
             ,pContractYearMonth    => vn_startyearMonth
           ) ;
         
--        dbms_output.put_line('FAS level'||vn_FASlevel||' for month'||vn_startyearMonth);
         -- end of VN_FASLevel is NULL check 
       IF  pOverrideLocation IS NOT NULL THEN      
        
        vn_OverrideFASLevel :=  RISKDB.qp_zema_extract_pkg.Get_Monthly_Original_FAS_level (
             pLocation              => pOverrideLocation
             , pcommodity           => Location_sample_obj(1).OVERRIDE_COMMODITY
             ,pcobdate              => pcobdate
             ,pContractYearMonth    => vn_startyearMonth
           ) ;
       ELSE
        vn_OverrideFASLevel := NULL; 
       
       END IF;
       
        
        li_rec_Index :=pmonthlym2m.COUNT;

--       DBMS_OUTPUT.Put_line ('after no of elements :='||to_char(li_rec_index)||'-'||pMonthlym2m.COUNT);
       
       FOR Recs in Location_sample_obj.FIRST ..Location_sample_obj.COUNT 
       LOOP
      
           pmonthlym2m.EXTEND;
           li_rec_index := li_rec_index + 1;

--            DBMS_OUTPUT.Put_line ('after no of elements :='||to_char(li_rec_index)||'-'||pMonthlym2m.COUNT);
       
--            DBMS_OUTPUT.Put_line ('constructing element'||li_rec_index);
--            DBMS_OUTPUT.Put_line ('internal_price:='||vn_internal_price
--                                 ||'Zema Profile='||Location_sample_obj(recs).ext_PROFILE_ID 
--                                 ||'HourType='||Location_sample_obj(recs).HOUR_TYPE 
--                                 ||'Hours = '||Vn_hours||'Override Hours = '||Vn_Overridehours
--                                 );
          
          pmonthlym2m(li_rec_index) :=  m2m_by_Loc_T (
                       COB_DATE                        => Location_sample_obj(recs).COB_DATE
                       , NETTING_GROUP_ID                => Location_sample_obj(recs).NETTING_GROUP_ID
                       , UNDERLYING_COMMODITY            => Location_sample_obj(recs).UNDERLYING_COMMODITY
                       , COMMODITY                       => Location_sample_obj(recs).COMMODITY
                       , LOCATION                        => Location_sample_obj(recs).LOCATION
                       , PRICE_VOL_INDICATOR             => Location_sample_obj(recs).PRICE_VOL_INDICATOR
                       ,CONTRACTYEAR_MONTH              => vn_startyearMonth
                       ,CONTRACT_YEAR                   => SUBSTR(to_char(vn_startyearMonth), 1,4)
                       ,CONTRACT_MONTH                  => SUBSTR(to_char(vn_startyearMonth), 5,2)
                       ,LOCATION_DELTA_POSITION         => NULL
                       ,COMMODITY_ABSOLUTE_POSITION     => NULL
                       ,M2M_VALUE                       => NULL
                       ,LEGGED_M2M_VALUE                => NULL
                       ,ORIGINAL_FAS157_LEVEL           => vn_FASLevel
                       ,ORIGINAL_FAS157_PERCENT         => NULL
                       ,FINAL_FAS157_VALUE              => NULL
                       ,FINAL_FAS157_TYPE               => NULL
                       ,PROJ_LOC_AMT                    => vn_PROJ_LOC_AMT
                       ,ON_OFF_PEAK_INDICATOR           => Location_sample_obj(recs).ON_OFF_PEAK_INDICATOR
                       ,HOUR_TYPE                       => Location_sample_obj(recs).HOUR_TYPE
                       ,TOT_HOURS_PER_CONTRACT_MONTH    => Vn_hours
                       ,CAL_DAYS_IN_CONTRACT_MONTH      => vn_cal_days
                       ,INITIAL_RANK                    => NULL
                       ,BASISPOINT_OVERRIDE_ID          => Location_sample_obj(recs).BASISPOINT_OVERRIDE_ID
                       ,BASISPOINT_OVERRIDE_INDICATOR   => Location_sample_obj(recs).BASISPOINT_OVERRIDE_INDICATOR
                       ,FAS157_OVERRIDE_INDICATOR       => NULL
                       ,BASIS_CURVE_INDICATOR           => NULL
                       ,PHOENIX_BASIS_NAME              => NULL
                       ,BASIS_QUOTE_DATE                => NULL
                       ,BASIS_QUOTE_AGE                 => NULL
                       ,TRADER_BASIS_QUOTE_PRICE        => NULL
                       ,USING_100PRCNT_HIST_METHOD      => NULL
                       ,EXT_PROVIDER_ID                 => Location_sample_obj(recs).EXT_PROVIDER_ID
                       ,DATASOURCE_CATEGORY             => Location_sample_obj(recs).DATASOURCE_CATEGORY 
                       ,EXT_PROFILE_ID                  => Location_sample_obj(recs).EXT_PROFILE_ID
                       ,EXT_LOCATION                    => Location_sample_obj(recs).EXT_LOCATION 
                       ,PRICE_VOL_PROFILE               => Location_sample_obj(recs).PRICE_VOL_PROFILE
                       ,ZEMA_PROFILE_ID                 => Location_sample_obj(recs).ZEMA_PROFILE_ID
                       ,ZEMA_PROFILE_DISPLAY_NAME       => Location_sample_obj(recs).ZEMA_PROFILE_DISPLAY_NAME
                       ,FULLY_VALIDATED_FLAG            => NULL
                       ,TERM_START                      => NULL
                       ,TERM_END                        => NULL
                       ,BASIS_ASK                       => NULL
                       ,BASIS_BID                       => NULL
                       ,BROKER                          => NULL
                       ,MONTHLY_VOL                     => NULL
                       ,PROJ_LOCATION_AMT               => vn_proj_location_amt
                       ,PROJ_PREMIUM_AMT                => vn_proj_PREMIUM_amt
                       ,PROJ_BASIS_AMT                  => vn_proj_BASIS_amt
                       ,MAX_CONTRACTYEAR_MONTH          => Location_sample_obj(recs).MAX_CONTRACTYEAR_MONTH
                       ,MIN_CONTRACTYEAR_MONTH          => Location_sample_obj(recs).MIN_CONTRACTYEAR_MONTH
                       ,TOT_CONTRACTYEAR_MONTH          => Location_sample_obj(recs).TOT_CONTRACTYEAR_MONTH
                       ,Backbone_flag                   => pbackbone_flag
                       ,Internal_price                  => vn_internal_price
                       ,ABSOLUTE_M2M_VALUE              => NULL
                       ,ABSOLUTE_LEGGED_M2M_VALUE       => NULL
                       ,ABSOLUTE_M2M_VALUE_VOL          => NULL
                       ,ABSOLUTE_LEGGED_M2M_VALUE_VOL   => NULL
                       ,EXCEPTION_PRICE_FLAG            => 'Y'
                       , OVERRIDE_BASISPOINT            => Location_sample_obj(1).OVERRIDE_BASISPOINT
                       , OVERRIDE_COMMODITY             => Location_sample_obj(1).OVERRIDE_COMMODITY
                       , Override_hour_Type             => NULL
                       , Override_Hours                 => vn_OverrideHours
                       , OVERRIDE_PRICE                 => vn_Override_price  
                       , OVERRIDE_BACKBONE_FLAG         => Location_sample_obj(1).OVERRIDE_BACKBONE_FLAG 
                       , OVERRIDE_ORIG_FAS_LEVEL        => vn_overrideFASLevel   
                     );
            
      
       
       END LOOP;
       -- end of Location_Sample_Obj Loop

            
        END;
  
  vn_startyearMonth := TO_NUMBER(TO_CHAR(ADD_MONTHS ( TO_DATE(to_CHAr(vn_startyearMonth)||'01', 'YYYYMMDD'),1),'YYYYMM'));         
  

 END LOOP;
    -- End of Price LOOP 

ELSIF ppricevol ='VOL' THEN 

LOOP

   EXIT WHEN  vn_startYearMonth > vn_EndYearMonth;  
  
   vn_hours                     := NULL;
   Vn_Overridehours             := NULL;
   vn_FASLevel                  := NULL; 
   vn_OverrideFASLevel          := NULL;
   vn_proj_loc_amt              := NULL;
   vn_Proj_Location_amt         := NULL;
   vn_proj_basis_amt            := NULL;
   vn_proj_premium_amt          := NULL;
   vn_cal_days                  := NULL; 
 

       
         IF   pOverrideLocation IS NOT NULL AND Location_sample_obj(1).UNDERLYING_COMMODITY = 'ELECTRICITY' THEN 
         
              Vn_Overridehours := Get_Hours (       
                 pLocation          => pOverrideLocation 
               , PHourType          => NULL
               , pcontractYearMonth => vn_startyearMonth
               ) ;
      
         ELSE
         
           IF Location_sample_obj(1).UNDERLYING_COMMODITY = 'ELECTRICITY' THEN 
             Vn_hours := Get_Hours (       
                 pLocation          => POriginalLocation
               , PHourType          => Location_sample_obj(1).HOUR_TYPE
               , pcontractYearMonth => vn_startyearMonth
               ) ;

   

           END IF;
                      
         END IF;      
       
       
      -- Get Current FAS Level for Original Location for the  current Month     
        vn_FASLevel :=    RISKDB.qp_zema_extract_pkg.Get_Monthly_Original_FAS_level (
             pLocation              =>  POriginalLocation
              , pcommodity           => Location_sample_obj(1).COMMODITY
             ,pcobdate              => pcobdate
             ,pContractYearMonth    => vn_startyearMonth
           ) ;
         
--        dbms_output.put_line('FAS level'||vn_FASlevel||' for month'||vn_startyearMonth);
         -- end of VN_FASLevel is NULL check 
           
 IF  pOverrideLocation IS NOT NULL THEN 
    
   vn_OverrideFASLevel :=  RISKDB.qp_zema_extract_pkg.Get_Monthly_Original_FAS_level (
             pLocation              => pOverrideLocation
              , pcommodity           => Location_sample_obj(1).OVERRIDE_COMMODITY
             ,pcobdate              => pcobdate
             ,pContractYearMonth    => vn_startyearMonth
           ) ;
 ELSE
   vn_OverrideFASLevel := NULL;
 END IF;
 
   
     BEGIN
       
            SELECT 
            DAILY_VOL 
           ,TO_NUMBER (
            TO_CHAR (
                     LAST_DAY ( TO_DATE (contract_MOnth || '01', 'YYYYMMDD') )
                     , 'DD'
                     )
             )   cal_days
             INTO
             vn_internal_price
             , vn_cal_Days
             FROM PROJ_LOCATION_VOL_CURVE
             WHERE    
             EFFECTIVE_DATE =  pcobdate 
             AND PARTITION_BIT =2 
             AND BASIS_POINT = NVL(pOverrideLocation, pOriginalLocation)
             AND COMMODITY = NVL(Location_sample_obj(1).OVERRIDE_COMMODITY,Location_sample_obj(1).COMMODITY)
             AND contract_month = vn_startYearMOnth;
     
     IF pOverrideLocation IS NOT NULL THEN 
        vn_override_price := vn_internal_price;
        vn_internal_price := NULL;
     ELSE
        vn_override_price := NULL;
        
     END IF ;
     
     EXCEPTION
     WHEN NO_DATA_FOUND THEN 
        
      pout_rtn_code := c_failure;
      pout_rtn_msg := 'STOP';
      raise; --- exit unconditionally 
     END;
 

    BEGIN 

     SELECT DISTINCT LOCATION INTO VC_LOCATION
     FROM TABLE(pmonthlym2m ) M2M
     WHERE m2m.LOCATION =     NVL(pOverrideLocation, pOriginalLocation)
             and m2m.netting_group_id = pNettingGroup
             and m2m.price_vol_indicator =   ppricevol
             and to_number(m2m.CONTRACTYEAR_MONTH) = vn_startYearMOnth
             and M2M.INTERNAL_PRICE = NVL(vn_Override_price, vn_internal_price )
     ;
      
      
    EXCEPTION
    WHEN NO_DATA_FOUND THEN


        li_rec_Index :=pmonthlym2m.COUNT;
 

              
       FOR Recs in Location_sample_obj.FIRST ..Location_sample_obj.COUNT 
       LOOP
      
           pmonthlym2m.EXTEND;
           li_rec_index := li_rec_index + 1;

  
          pmonthlym2m(li_rec_index) :=  m2m_by_Loc_T (
                       COB_DATE                        => Location_sample_obj(recs).COB_DATE
                       , NETTING_GROUP_ID                => Location_sample_obj(recs).NETTING_GROUP_ID
                       , UNDERLYING_COMMODITY            => Location_sample_obj(recs).UNDERLYING_COMMODITY
                       , COMMODITY                       => Location_sample_obj(recs).COMMODITY
                       , LOCATION                        => Location_sample_obj(recs).LOCATION
                       , PRICE_VOL_INDICATOR             => Location_sample_obj(recs).PRICE_VOL_INDICATOR
                       ,CONTRACTYEAR_MONTH              => vn_startYearMOnth
                       ,CONTRACT_YEAR                   => SUBSTR(to_Char(vn_startYearMOnth), 1,4)
                       ,CONTRACT_MONTH                  => SUBSTR(to_char(vn_startYearMOnth), 5,2)
                       ,LOCATION_DELTA_POSITION         => NULL
                       ,COMMODITY_ABSOLUTE_POSITION     => NULL
                       ,M2M_VALUE                       => NULL
                       ,LEGGED_M2M_VALUE                => NULL
                       ,ORIGINAL_FAS157_LEVEL           => vn_FASLevel
                       ,ORIGINAL_FAS157_PERCENT         => NULL
                       ,FINAL_FAS157_VALUE              => NULL
                       ,FINAL_FAS157_TYPE               => NULL
                       ,PROJ_LOC_AMT                    => NULL
                       ,ON_OFF_PEAK_INDICATOR           => Location_sample_obj(recs).ON_OFF_PEAK_INDICATOR
                       ,HOUR_TYPE                       => Location_sample_obj(recs).HOUR_TYPE
                       ,TOT_HOURS_PER_CONTRACT_MONTH    => Vn_hours
                       ,CAL_DAYS_IN_CONTRACT_MONTH      => vn_cal_days
                       ,INITIAL_RANK                    => NULL
                       ,BASISPOINT_OVERRIDE_ID          => Location_sample_obj(recs).BASISPOINT_OVERRIDE_ID
                       ,BASISPOINT_OVERRIDE_INDICATOR   => Location_sample_obj(recs).BASISPOINT_OVERRIDE_INDICATOR
                       ,FAS157_OVERRIDE_INDICATOR       => NULL
                       ,BASIS_CURVE_INDICATOR           => Location_sample_obj(recs).BASIS_CURVE_INDICATOR
                       ,PHOENIX_BASIS_NAME              => Location_sample_obj(recs).PHOENIX_BASIS_NAME
                       ,BASIS_QUOTE_DATE                => Location_sample_obj(recs).BASIS_QUOTE_DATE
                       ,BASIS_QUOTE_AGE                 => Location_sample_obj(recs).BASIS_QUOTE_AGE
                       ,TRADER_BASIS_QUOTE_PRICE        => Location_sample_obj(recs).TRADER_BASIS_QUOTE_PRICE
                       ,USING_100PRCNT_HIST_METHOD      => Location_sample_obj(recs).USING_100PRCNT_HIST_METHOD
                       ,EXT_PROVIDER_ID                 => Location_sample_obj(recs).EXT_PROVIDER_ID
                       ,DATASOURCE_CATEGORY             => Location_sample_obj(recs).DATASOURCE_CATEGORY 
                       ,EXT_PROFILE_ID                  => Location_sample_obj(recs).EXT_PROFILE_ID
                       ,EXT_LOCATION                    => Location_sample_obj(recs).EXT_LOCATION 
                       ,PRICE_VOL_PROFILE               => Location_sample_obj(recs).PRICE_VOL_PROFILE
                       ,ZEMA_PROFILE_ID                 => Location_sample_obj(recs).ZEMA_PROFILE_ID
                       ,ZEMA_PROFILE_DISPLAY_NAME       => Location_sample_obj(recs).ZEMA_PROFILE_DISPLAY_NAME
                       ,FULLY_VALIDATED_FLAG            => Location_sample_obj(recs).FULLY_VALIDATED_FLAG
                       ,TERM_START                      => Location_sample_obj(recs).TERM_START
                       ,TERM_END                        => Location_sample_obj(recs).TERM_END
                       ,BASIS_ASK                       => Location_sample_obj(recs).BASIS_ASK
                       ,BASIS_BID                       => Location_sample_obj(recs).BASIS_BID
                       ,BROKER                          => Location_sample_obj(recs).BROKER
                       ,MONTHLY_VOL                     => Location_sample_obj(recs).MONTHLY_VOL
                       ,PROJ_LOCATION_AMT               => NULL
                       ,PROJ_PREMIUM_AMT                => NULL
                       ,PROJ_BASIS_AMT                  => NULL
                       ,MAX_CONTRACTYEAR_MONTH          => Location_sample_obj(recs).MAX_CONTRACTYEAR_MONTH
                       ,MIN_CONTRACTYEAR_MONTH          => Location_sample_obj(recs).MIN_CONTRACTYEAR_MONTH
                       ,TOT_CONTRACTYEAR_MONTH          => Location_sample_obj(recs).TOT_CONTRACTYEAR_MONTH
                       ,Backbone_flag                   => pbackbone_flag
                       ,Internal_price                  => vn_internal_price
                       ,ABSOLUTE_M2M_VALUE              => NULL
                       ,ABSOLUTE_LEGGED_M2M_VALUE       => NULL
                       ,ABSOLUTE_M2M_VALUE_VOL          => NULL
                       ,ABSOLUTE_LEGGED_M2M_VALUE_VOL   => NULL
                       ,EXCEPTION_PRICE_FLAG            => 'Y'
                       , OVERRIDE_BASISPOINT            => Location_sample_obj(1).OVERRIDE_BASISPOINT
                       , OVERRIDE_COMMODITY            => Location_sample_obj(1).OVERRIDE_COMMODITY
                       , Override_hour_Type             => Location_sample_obj(1).OVERRIDE_HOUR_TYPE
                       , Override_Hours                 => vn_OverrideHours
                       , OVERRIDE_PRICE                 => vn_Override_price  
                       , OVERRIDE_BACKBONE_FLAG         => Location_sample_obj(1).OVERRIDE_BACKBONE_FLAG
                       , OVERRIDE_ORIG_FAS_LEVEL         => vn_overrideFASLevel
                      ); 
            
       END LOOP;
       -- end of Location_Sample_Obj Loop
      
    END;
    
     vn_startyearMonth := TO_NUMBER(TO_CHAR(ADD_MONTHS ( TO_DATE(to_CHAr(vn_startyearMonth)||'01', 'YYYYMMDD'),1),'YYYYMM'));
 
  END LOOP;
-- End of VOLrecs LOOP 

END IF ;
-- end of PRICE or VOL Check 

--    DBMS_OUTPUT.put_line ('BACKBONE_FLAG at function := ' || pbackbone_flag);
     pout_rtn_code := c_success;
     pout_rtn_msg := '';                              --Null means Success
     
EXCEPTION
WHEN NO_DATA_FOUND THEN 
  pout_rtn_code := c_failure;
  pout_rtn_msg := 'STOP';
WHEN OTHERS THEN 
  pout_rtn_code := c_failure;
  pout_rtn_msg := 'STOP'||' '||SUBSTR(SQLERRM,1,100);  
END Get_proj_prices;


Procedure  Prepare_qvar_strips(
                              pcobdate                  IN DATE
                            , pCurrentMOnth             IN VARCHAR2
                            , ppromptMonth              IN VARCHAR2
                            , punderlyingCommodity      IN VARCHAR2
                            , pNettingGroupId           IN NUMBER
                            , pLocation                 IN VARCHAR2
                            , pOverrideLocation         IN VARCHAR2 
                            , pOverrideID               IN NUMBER
                            , ppricevol                 IN VARCHAR2
                            , pstart_YearMonth          IN NUMBER
                            , pEnd_YearMonth            IN NUMBER  
                            , pmonthlym2m               IN OUT NOCOPY m2m_by_Loc_tab 
                            , pcollectn                 IN OUT NOCOPY strips_collectn_t
                            , pmonthlyflag              IN VARCHAR2
                            , pbalancedFlag              IN VARCHAR2
                            , pnonBalancedFlag          IN VARCHAR2 
                            , pStripName                IN VARCHAR2
                            , pmaxYear                  IN NUMBER
                            , pMINYear                  IN NUMBER
                            , ptotalMonths              IN NUMBER  
                            , prank                     IN NUMBER
                            , pflatposition             IN VARCHAR2    
                            , pbackbone_flag            IN  VARCHAR2  
                            , pOverridebb_flag          IN VARCHAR2                                                                                  
                            , pbasis_indicator          IN  VARCHAR2  
                            , pout_rtn_code      OUT  NOCOPY NUMBER    
                            , pout_rtn_msg       OUT  NOCOPY VARCHAR2
                            ) 
is

vn_rtn_code             NUMBER;
vc_rtn_msg              VARCHAR2(1000);
vc_message              VARCHAR2(1000);
vn_reccount             NUMBER;


-- for m2m% caclculations 
--m2m_tab_obj               riskdb.qp_strip_tabtype_Obj; -- automaically NULL Object
--null_m2m_tabObj           riskdb.qp_strip_tabtype_Obj; -- to reset when required 
-- 
--vn_objects_count               NUMBER := 0; --total elements to be inserted in m2m%Obj
vn_Objects_START_indx          NUMBER := 0; --start Bound
--Vn_Objects_End_indx            NUMBER := 0; -- end Bound of m2m%Obj to be inserted in current call
--Vn_objindx                     NUMBER := 0; -- m2m%Obj index  

vc_return_val           VARCHAR2(2);
v_delta_price_deno      NUMBER;
v_backbone_flag         VARCHAR2 (1);
v_basis_indicator         VARCHAR2 (1);

li_rec_index            NUMBER; 

ext_Quotes_obj            ext_quotes_T;  -- TYPE Object
mapped_ext_quotes         ext_quotes_tab; -- Table of TYPE Objects
null_ext_quotes           ext_quotes_tab;     
   

  
e_Invalid_Strip_startMonth         EXCEPTION;
PRAGMA EXCEPTION_INIT( e_Invalid_Strip_startMonth , -20040);

e_Invalid_Strip_EndMonth          EXCEPTION;
PRAGMA EXCEPTION_INIT( e_Invalid_Strip_EndMonth , -20041);


Cursor cur_Monthlym2m  is
SELECT
  m2m.COB_DATE                                   
 , m2m.NETTING_GROUP_ID                 
 , m2m.UNDERLYING_COMMODITY            
 , m2m.COMMODITY                       
 , m2m.LOCATION                      
 , m2m.PRICE_VOL_INDICATOR            
 , m2m.CONTRACTYEAR_MONTH              
 , m2m.CONTRACT_YEAR                  
 , m2m.CONTRACT_MONTH                 
 , m2m.LOCATION_DELTA_POSITION        
 , m2m.COMMODITY_ABSOLUTE_POSITION    
 , m2m.M2M_VALUE                      
 , m2m.ABSOLUTE_M2M_Value
 , m2m.LEGGED_M2M_VALUE
 , m2m.ABSOLUTE_legged_m2m_value               
 , m2m.ORIGINAL_FAS157_LEVEL          
 , m2m.ORIGINAL_FAS157_PERCENT        
 , m2m.FINAL_FAS157_VALUE            
 , m2m.FINAL_FAS157_TYPE              
 , m2m.internal_price PROJ_LOC_AMT   /* internal Price projected as Proj_loc_amt */              
 , m2m.ON_OFF_PEAK_INDICATOR          
 , m2m.HOUR_TYPE                     
 , m2m.TOT_HOURS_PER_CONTRACT_MONTH   
 , m2m.CAL_DAYS_IN_CONTRACT_MONTH     
 , m2m.INITIAL_RANK                   
 , m2m.BASISPOINT_OVERRIDE_ID         
 , m2m.BASISPOINT_OVERRIDE_INDICATOR  
 , m2m.FAS157_OVERRIDE_INDICATOR      
 ----need to check
 --, m2m.BASIS_CURVE_INDICATOR           
 , m2m.PHOENIX_BASIS_NAME             
 , m2m.BASIS_QUOTE_DATE               
 , m2m.BASIS_QUOTE_AGE                
 , m2m.TRADER_BASIS_QUOTE_PRICE       
 , m2m.USING_100PRCNT_HIST_METHOD     
 , m2m.EXT_PROVIDER_ID                  
 , m2m.DATASOURCE_CATEGORY                         
 , m2m.EXT_PROFILE_ID                     
 , m2m.EXT_LOCATION                   
 , m2m.PRICE_VOL_PROFILE               
 , m2m.ZEMA_PROFILE_ID                   
 , m2m.ZEMA_PROFILE_DISPLAY_NAME    
 , m2m.term_start                    
 , m2m.term_end                      
 , m2m.basis_ask                     
 , m2m.Basis_bid                     
 , m2m.Broker                         
 , m2m.MONTHLY_VOL                  
 , m2m.PROJ_LOCATION_AMT            
 , m2m.PROJ_PREMIUM_AMT              
 , m2m.PROJ_BASIS_AMT               
 , pmaxYear MAX_CONTRACTYEAR_MONTH      
 , pminYear MIN_CONTRACTYEAR_MONTH       
 , ptotalMonths TOT_CONTRACTYEAR_MONTH  
 , m2m.ABSOLUTE_M2M_VALUE_VOL        
 , m2m.ABSOLUTE_LEGGED_M2M_VALUE_VOL  
 , m2m.OVERRIDE_BASISPOINT    
 , m2m.OVERRIDE_HOURS     
 , m2m.OVERRIDE_PRICE         
FROM 
TABLE(pmonthlym2m) m2m
--RISKDB.QP_INT_FWD_Curves_raw_data m2m
where
 m2m.Underlying_commodity = punderlyingCommodity
and m2m.price_vol_indicator = ppricevol
and m2m.Netting_Group_id    = pNettingGroupId
and m2m.Location = pLocation
--and m2m.LOCATION = 'CAISO-TH_NP15_GEN-APND-7x8'  
--and to_number(m2m.CONTRACTYEAR_MONTH)  > pCurrentMOnth
and to_number(m2m.contractYear_month) Between  pstart_YearMonth AND pEnd_YearMonth
and m2m.EXCEPTION_PRICE_FLAG = 'N' --- only pull original Months from work area not derived extra Months 
--and ( NVL(m2m.LOCATION_DELTA_POSITION , 0)  <> 0
--      OR NVL(m2m.LEGGED_M2M_VALUE, 0) <> 0
--    )
order by m2m.LOCATION 
, m2m.ext_profile_id 
, m2m.CONTRACTYEAR_MONTH
;

Cursor cur_NonMOnthlym2m  is
SELECT DISTINCT
  m2m.COB_DATE                                   
 , m2m.NETTING_GROUP_ID                 
 , m2m.UNDERLYING_COMMODITY            
 , m2m.COMMODITY                       
 , m2m.LOCATION LOCATION                      
 , m2m.PRICE_VOL_INDICATOR            
-- , m2m.CONTRACTYEAR_MONTH              
, LEAST(to_number(substr(pstart_YearMonth,1,4)), to_number(substr(pEnd_YearMonth,1,4))) CONTRACT_YEAR                   
 , m2m.ON_OFF_PEAK_INDICATOR          
 , m2m.HOUR_TYPE                     
 , m2m.INITIAL_RANK                   
 , m2m.BASISPOINT_OVERRIDE_ID         
 , m2m.BASISPOINT_OVERRIDE_INDICATOR  
 , m2m.EXT_PROVIDER_ID                  
 , m2m.DATASOURCE_CATEGORY                         
 , m2m.EXT_PROFILE_ID                     
 , m2m.EXT_LOCATION                   
 , m2m.PRICE_VOL_PROFILE               
 , m2m.ZEMA_PROFILE_ID                   
 , m2m.ZEMA_PROFILE_DISPLAY_NAME      
 , pmaxYear     MAX_CONTRACTYEAR_MONTH      
 , pMinYear     MIN_CONTRACTYEAR_MONTH       
 , ptotalMonths TOT_CONTRACTYEAR_MONTH 
 , m2m.OVERRIDE_BASISPOINT 
 , m2m.OVERRIDE_HOURS    
FROM 
TABLE(pmonthlym2m) m2m
where
 m2m.Underlying_commodity = punderlyingCommodity
and m2m.price_vol_indicator = ppricevol
and m2m.Netting_Group_id    = pNettingGroupId
and m2m.LOCATION = pLOcation
--and m2m.LOCATION = 'CAISO-TH_NP15_GEN-APND-7x8'  
and to_number(m2m.CONTRACTYEAR_MONTH)  > pCurrentMOnth
and to_number(m2m.contractYear_month) Between  pstart_YearMonth AND pEnd_YearMonth
order by m2m.LOCATION
, LEAST(to_number(substr(pstart_YearMonth,1,4)), to_number(substr(pEnd_YearMonth,1,4)))
;


lb_quotes_rec Boolean := FALSE;

Cursor cur_ext_quotes (
pextProviderid      NUMBER
,pextprofileid      NUMBER
,pextlocation       VARCHAR2
, pstriptenor       VARCHAR2
, pcontractYear     NUMBER
) is
Select 
quotes.EXT_PROVIDER_QUOTE_ID            
,quotes.EXT_PROVIDER_ID                 
,quotes.EXT_PROFILE_ID                  
,quotes.UNDERLYING_COMMODITY            
,quotes.EXT_EFF_DATE                    
,quotes.EXT_RAW_CONTRACT                
,quotes.MID                             
,quotes.BID                             
,quotes.ASK                             
,quotes.STRIP_TENOR                     
,quotes.CONTRACT_MONTH                  
,quotes.CONTRACT_YEAR                   
,quotes.EXT_CATEGORY     
, quotes.EXT_PROVIDER               
,quotes.EXT_LOCATION                    
,quotes.NEER_HUB                        
,quotes.NEER_LOCATION                   
,quotes.PEAK_TYPE                       
,quotes.PROCESS_LEVEL                   
,quotes.EXT_HUB_MAPPED_FLAG             
,quotes.EXT_LOC_MAPPED_FLAG             
,quotes.NEER_LOC_MAPPED_FLAG            
,quotes.NEER_HUB_MAPPED_FLAG            
,quotes.STRIP_TENOR_MAPPED_FLAG         
,quotes.SECONDARY_CALCULATION_FLAG      
,quotes.ON_MID                          
,quotes.OFF_MID                         
,quotes.ON_BID                          
,quotes.OFF_BID                         
,quotes.ON_ASK                          
,quotes.OFF_ASK                         
,quotes.PRICE_TYPE                      
,quotes.VALUE                           
,quotes.HOUR_TYPE                       
FROM 
TABLE (mapped_ext_quotes ) quotes
where 
--quotes.Underlying_commodity =  v_underlying_commodity
quotes.ext_Provider_id  =  pextProviderid
and quotes.ext_profile_id   =  pextprofileid 
--and quotes.NEER_HUB         =  vc_hub
--and quotes.NEER_LOCATION    =  vc_location  
and quotes.ext_Location     =  pextlocation
and Upper(quotes.STRIP_TENOR) = Upper(pstripTenor)
and quotes.contract_year    =  pcontractYear
-- this query below is to ignore any duplicate quotes by tenor n location
and 1 = ( Select count(*)
      FROM 
      TABLE (mapped_ext_quotes) dups
      where
      --dups.Underlying_commodity =  v_underlying_commodity
       dups.ext_Provider_id  =  quotes.ext_Provider_id
      and dups.ext_profile_id   =  quotes.ext_profile_id
--      and dups.NEER_HUB         =  v_hub
--      and dups.NEER_LOCATION    =  v_location  
      and dups.ext_Location     =  quotes.ext_Location
      and Upper(dups.STRIP_TENOR) = Upper(quotes.STRIP_TENOR)
      and dups.contract_year    =  quotes.contract_year
      and NVL(dups.PRICE_TYPE, '1') =  NVL(quotes.PRICE_TYPE, '1')
      group by dups.ext_Location , dups.STRIP_TENOR, dups.contract_year , NVL(dups.PRICE_TYPE, '1')
     ) 
;


---- Intialize montly strips collection
--Procedure INIT_collection IS
--BEGIN
--   m2m_tab_obj := qp_strip_tabtype_Obj(); --intialize TYPE Object collection
--END INIT_collection;

   
BEGIN

--    INIT_collection;

   PROCESS_LOG_REC.STAGE        := 'Prepare_qvar_strips';

   
-- Based on Given COB DATE DERIVE 
-- Current Year vc_CYYYY
-- Prompt Year  V_PYYYY
-- Quote Year   V_QYYYY
-- ALL YEARs is Based on CONTRACT YEARS 
 
 IF pstart_YearMonth = pEnd_YearMonth THEN 
 
 
 /************************************************************************
 **
 **               MONTHLY SECTION 
 ***********************************************************************/
 
 -- Monthly cursor 
   For m2m_recs in cur_Monthlym2m LOOP
   
 
   
   
    IF pcollectn.COUNT = 0 THEN 
     li_rec_Index :=  0 ;
    ELSE
    li_rec_Index :=pcollectn.COUNT;
    END IF;
    
--    vn_Objects_START_indx  := pcollectn.COUNT;

                   
    pcollectn.EXTEND;
    
    li_rec_index := li_rec_index + 1 ;
    
                            
    pcollectn(li_rec_Index).MONTHLY_STRIP_FLAG := pmonthlyflag; 
    pcollectn(li_rec_Index).BALANCED_STRIP_FLAG := 'N';
    pcollectn(li_rec_Index).NONBALANCED_STRIP_FLAG := 'N';
    pcollectn(li_rec_Index).COMMODITY_STRIP_FLAG := 'N';                 
    pcollectn(li_rec_Index).SECONDARY_CALCULATION_FLAG := 'N'; 
    pcollectn(li_rec_Index).MANUAL_QUOTE_FLAG := 'N'; 
    pcollectn(li_rec_Index).VALIDATED_FLAG := 'N';
    pcollectn(li_rec_Index).FULLY_VALIDATED_FLAG := 'N';
                
    pcollectn(li_rec_Index).PROCESS_LEVEL          
                                := '3';
                                            
    pcollectn(li_rec_Index).PARTITION_BIT      :=  2; 
               
    pcollectn(li_rec_Index).EXT_MANUAL_QUOTE_ID       
                                := NULL;
                                    
   pcollectn(li_rec_Index).ACTIVE_FLAG := 'Y';
   
   pcollectn(li_rec_Index).MAX_CONTRACTYEAR_MONTH := pMaxYear;                 
   pcollectn(li_rec_Index).MIN_CONTRACTYEAR_MONTH := pMinYear;                 
   pcollectn(li_rec_Index).TOT_CONTRACTYEAR_MONTH  :=pTotalMonths ;

   pcollectn(li_rec_index).Curve_Rank := prank;
 
   pcollectn(li_rec_index).FLAT_POSITION_FLAG := pflatposition;

   -- SK 02/19/2016 , when overridden show overridden backbone flag 
   IF  pOverrideLocation IS NOT NULL THEN 
   
   pcollectn (li_rec_index).BACKBONE_FLAG := pOverridebb_flag ;
   
   ELSE
   
   pcollectn (li_rec_index).BACKBONE_FLAG := pbackbone_flag;
   
   END IF;
   --SK until here on 02/19/2016
   
    pcollectn (li_rec_index).BASIS_CURVE_INDICATOR  := pbasis_indicator;  
   --- collect m2m information 
    pcollectn(li_rec_Index).COB_DATE 
                            := m2m_recs.COB_DATE;       
                                                 
    pcollectn(li_rec_Index).NETTING_GROUP_ID  
                            := m2m_recs.NETTING_GROUP_ID ;
                                        
    pcollectn(li_rec_Index).UNDERLYING_COMMODITY
                            := m2m_recs.UNDERLYING_COMMODITY;      
        
    pcollectn(li_rec_Index).NEER_COMMODITY  
                            := m2m_recs.COMMODITY; 
                                                 
    pcollectn(li_rec_Index).NEER_LOCATION   
                            := m2m_recs.LOcation  ;


     -- Derive these for Original Location  
   pcollectn(li_rec_index).BASISPOINT_OVERRIDE_ID    
                        := pOverrideID ; --m2m_recs.BASISPOINT_OVERRIDE_ID ;
                        
   IF pOverrideLocation IS NOT NULL THEN 
    pcollectn(li_rec_index).BASISPOINT_OVERRIDE_INDICATOR := 'Y' ;      
   ELSE
     pcollectn(li_rec_index).BASISPOINT_OVERRIDE_INDICATOR := 'N' ;      
   END IF;
                        
    pcollectn(li_rec_Index).PRICE_VOL_INDICATOR  
                    := m2m_recs.PRICE_VOL_INDICATOR ; 
  
   pcollectn(li_rec_Index).STRIP_TENOR  
                                :=  pstripName;  
   
                                                            
   pcollectn(li_rec_Index).STRIP_START_YEARMONTH := 
                                 pstart_YearMonth;             
   pcollectn(li_rec_Index).STRIP_END_YEARMONTH   :=    
                                 pstart_YearMonth;         
    pcollectn(li_rec_Index).CONTRACTYEAR_MONTH    :=  
                                 pstart_YearMonth;              
    pcollectn(li_rec_Index).CONTRACT_YEAR        :=  
                                 m2m_recs.contract_Year;   
    pcollectn(li_rec_Index).CONTRACT_MONTH        := 
                                  M2M_RECS.CONTRACT_mONTH; 

    pcollectn(li_rec_Index).TOTAL_TENOR_MONTHS    
                    := 1;   

                            
---- Basis Point attributes                                                    
--need to check
--    pcollectn(li_rec_Index).BASIS_CURVE_INDICATOR
--                    := m2m_recs.BASIS_CURVE_INDICATOR; 
                                         
    pcollectn(li_rec_Index).PHOENIX_BASIS_NAME 
                    := m2m_recs.PHOENIX_BASIS_NAME;    
                                        
    pcollectn(li_rec_Index).BASIS_QUOTE_DATE  
                    := m2m_recs.BASIS_QUOTE_DATE;    
                                         
    pcollectn(li_rec_Index).BASIS_QUOTE_AGE 
                    := m2m_recs.BASIS_QUOTE_AGE;  
                                                 
    pcollectn(li_rec_Index).TRADER_BASIS_QUOTE_PRICE  
                    := m2m_recs.TRADER_BASIS_QUOTE_PRICE;  
                                    
    pcollectn(li_rec_Index).USING_100PRCNT_HIST_METHOD 
                    := m2m_recs.USING_100PRCNT_HIST_METHOD; 
                  
   pcollectn(li_rec_Index).TERM_START := m2m_recs.term_start; 
                              
   pcollectn(li_rec_Index).TERM_END := m2m_recs.term_end;                 
   pcollectn(li_rec_Index).BASIS_ASK := m2m_recs.basis_ask;                 
   pcollectn(li_rec_Index).BASIS_BID := m2m_recs.Basis_bid;                 
   pcollectn(li_rec_Index).BROKER  := m2m_recs.Broker ;

                                    
    pcollectn(li_rec_Index).FAS157_OVERRIDE_INDICATOR   
                       := m2m_recs.FAS157_OVERRIDE_INDICATOR ;

---- until here basis properties 
                                       
    pcollectn(li_rec_Index).ON_OFF_PEAK_INDICATOR   
                       := m2m_recs.ON_OFF_PEAK_INDICATOR;  
                                         
    pcollectn(li_rec_Index).HOUR_TYPE 
                       := m2m_recs.HOUR_TYPE;      
        
    pcollectn(li_rec_Index).M2M_VALUE            :=  m2m_recs.m2m_value;
    pcollectn(li_rec_Index).ABSOLUTE_M2M_VALUE   :=  m2m_recs.Absolute_m2m_value;
    pcollectn(li_rec_Index).LEGGED_M2M_VALUE     :=  m2m_recs.Legged_m2m_value;
    pcollectn(li_rec_Index).ABSOLUTE_LEGGED_M2M_VALUE := m2m_recs.ABSOLUTE_LEGGED_M2M_VALUE;

-- adding VOL amounts to Monthly curves SK 12/17/2015   
    pcollectn(li_rec_Index).ABS_M2M_AMT_VOL      :=  m2m_recs.ABSOLUTE_M2M_VALUE_VOL;
    pcollectn(li_rec_Index).ABS_LEGGED_M2M_VOL   :=  m2m_recs.ABSOLUTE_LEGGED_M2M_VALUE_VOL;

    
    pcollectn(li_rec_Index).LOCATION_DELTA_POSITION := m2m_recs.LOCATION_DELTA_POSITION;
    pcollectn(li_rec_Index).COMMODITY_ABS_POSITION := m2m_recs.COMMODITY_ABSOLUTE_POSITION;
    
   pcollectn(li_rec_Index).ORIGINAL_FAS157_LEVEL 
                    := m2m_recs.ORIGINAL_FAS157_LEVEL;  
                                        
   pcollectn(li_rec_Index).ORIGINAL_FAS157_PERCENT
                    := m2m_recs.ORIGINAL_FAS157_PERCENT;

    -- Internal Price is Projected as PROJ_LOC_AMAT in cursor to minimize impact on the code 
    CASE 
    WHEN m2m_RECS.OVERRIDE_BASISPOINT   IS NULL THEN 
        pcollectn(li_rec_Index).Proj_Loc_amt           := m2m_recs.proj_loc_amt;
        pcollectn(li_rec_Index).TOTAL_HOURS_PER_STRIP  := m2m_recs.TOT_HOURS_PER_CONTRACT_MONTH;
    
    
    ELSE 
    
    
        pcollectn(li_rec_Index).Proj_Loc_amt           := m2m_recs.OVERRIDE_PRICE ;
        pcollectn(li_rec_Index).TOTAL_HOURS_PER_STRIP  := m2m_recs.OVERRIDE_HOURS;
        
    END CASE;
    
       
    
--  dbms_output.Put_line('proj_loc_amt = '||m2m_recs.proj_loc_amt);

    pcollectn(li_rec_Index).CALENDAR_DAYS_in_STRIP := m2m_recs.CAL_DAYS_IN_CONTRACT_MONTH;
       

   pcollectn(li_rec_Index).MONTHLY_VOL  := m2m_recs.MONTHLY_VOL ;                 
--   pcollectn(li_rec_Index).PROJ_LOCATION_AMT := m2m_recs.PROJ_LOCATION_AMT;
--   pcollectn(li_rec_Index).PROJ_PREMIUM_AMT  := m2m_recs.PROJ_PREMIUM_AMT ; 
--   pcollectn(li_rec_Index).PROJ_BASIS_AMT  := m2m_recs.PROJ_BASIS_AMT ;                
                              
                                         
 
                                          
--   pcollectn(li_rec_Index).FINAL_FAS157_TYPE             
--                    := m2m_recs.FINAL_FAS157_TYPE; 


          --assign Override curves FAS 
           Derive_StripLevel_Measures(
                 pindex             =>  li_rec_Index
               , pOriginalLocation  =>  pcollectn(li_rec_index).NEER_LOCATION 
               , pOverrideLocation  =>  pOverrideLocation
               , pbackbone_flag     =>  pbackbone_flag
               , pOverridebb_flag   =>  pOverridebb_flag       
               , pcUrrentMOnth      =>  TO_NUMBER(pCurrentMonth)
               , pstartYearMonth    =>  TO_NUMBER(pstart_yearMonth)
               , PEndYearMOnth      =>  TO_NUMBER(pend_yearMonth)
               , pm2mData           =>  pmonthlym2m
               , pcollection        =>  pcollectn
               , pcalcType          =>  'TENOR_LEVEL_FAS'
               , pout_rtn_code      =>  vn_rtn_code    
               , pout_rtn_msg       =>  vc_rtn_msg 
               );  
               
               
--    dbms_output.put_line('before granularity call ');
   vc_return_val := Get_granularity (
                    pcobdate  => pcobdate
                  , pUnderlying_commodity => punderlyingCommodity
                  , pPrice_vol_Indicator => ppricevol
                  , ptenor_Months      => pcollectn(li_rec_Index).TOTAL_TENOR_MONTHS
                  ) ;
                          
                
        IF    vc_return_val = 'G' THEN
           pcollectn(li_rec_Index).Granular_quote_indicator  := 'Y'  ;     
        ELSE
          pcollectn(li_rec_Index).Granular_quote_indicator  := 'N'  ;
        END IF;
        
 

                
    ---  collect external quotes data 
    
        IF m2m_recs.EXT_LOCATION iS NOT NULL THEN 
            

                
               pcollectn(li_rec_Index).EXT_LOCATION 
                        := m2m_recs.EXT_LOCATION ;
              
               pcollectn(li_rec_Index).EXT_PROFILE_ID  :=
                           m2m_recs.ext_profile_id;
               
               pcollectn(li_rec_Index).PRICE_VOL_PROFILE    :=
                           m2m_recs.PRICE_VOL_PROFILE;
               pcollectn(li_rec_Index).ZEMA_PROFILE_ID :=
                           m2m_recs.ZEMA_PROFILE_ID;
               pcollectn(li_rec_Index).ZEMA_PROFILE_DISPLAY_NAME :=
                           m2m_recs.ZEMA_PROFILE_DISPLAY_NAME;
               pcollectn(li_rec_Index).EXT_EFF_DATE  :=
                           m2m_recs.COB_DATE;            
            
               pcollectn(li_rec_Index).ext_Provider_id :=
                           m2m_recs.ext_provider_id;   
               
               
               
                    Select count(*) into vn_reccount
                    from 
                    TABLE(mapped_ext_quotes) ext
                    where
                    ext.ext_Provider_id = pcollectn(li_rec_Index).ext_Provider_id
                    and ext.ext_Profile_id = pcollectn(li_rec_Index).ext_Profile_id
                    and ext_Location = pcollectn(li_rec_Index).ext_Location
                    ;
            
                 IF vn_reccount > 0 THEN 
                   -- lets not construct
                    null; 
                 ELSE
                   -- reset Local ext Object                
                     mapped_ext_quotes := null_ext_quotes; 
                   --lets construct Object 
                     Construct_Locquotes_object(
                             pcobdate               => pcobdate
                            , pProviderId           => m2m_recs.ext_provider_id 
                            , pProfileId            => m2m_recs.ext_profile_id 
                            , pextLocation          => m2m_recs.EXT_LOCATION  
                            , pextquotes            => mapped_ext_quotes 
                            , pout_rtn_code         => vn_rtn_code  
                            , pout_rtn_msg          => vc_rtn_msg
                            );
                  END IF;
                     
                            
            Select count(*) into vn_reccount
            from 
            TABLE(mapped_ext_quotes)
            ;

            vc_message := 'Ext-total_recs->'||to_CHAR(vn_reccount)
            ||' profid='||m2m_recs.ext_profile_id;  
 
           dbms_output.Put_line(vc_message);
                                      
                            
               BEGIN
                    SELECT 
                    ext.EXT_CATEGORY
                    , ext.EXT_PROVIDER
                    INTO 
                      pcollectn(li_rec_Index).DATASOURCE_CATEGORY 
                    , pcollectn(li_rec_Index).DATA_SOURCE_NAME 
                    FROM
                    TABLE (mapped_ext_quotes ) ext
                    where
                    EXT_PROVIDER_ID = m2m_recs.ext_provider_id
                    and rownum < 2
                    ;
               EXCEPTION
               WHEN NO_DATA_FOUND THEN 
                 dbms_output.Put_line ('inside EXT_LOCATION NO Provider found'||m2m_recs.ext_provider_id );
                 null;
               END;                    

--         dbms_output.Put_line('after Data source category');        


             lb_quotes_rec := FALSE;
             
             For quotes_rec IN cur_ext_quotes (
                                                  m2m_recs.ext_provider_id
                                                , m2m_recs.ext_profile_id
                                                , m2m_recs.EXT_LOCATION
                                                , pstripName
                                                , m2m_recs.CONTRACT_YEAR  
                                              ) LOOP
                

                -- Quotes available   
                lb_quotes_rec := TRUE;      
                
           
               --- check adn assign MID/BID/ASK                  
                IF  ( quotes_rec.MID is not null OR
                        quotes_rec.BID is not null OR
                        quotes_rec.ASK is not null
                       ) THEN                 
                    pcollectn(li_rec_Index).MID :=
                                quotes_rec.MID;
                    pcollectn(li_rec_Index).BID :=
                                quotes_rec.BID;
                    pcollectn(li_rec_Index).ASK :=
                            quotes_rec.ASK;
                END IF;
                
                IF (  ( quotes_rec.ON_BID is not null  OR
                          quotes_rec.OFF_BID is not null 
                          )
                      ) THEN 
                    IF m2m_recs.ON_OFF_PEAK_INDICATOR = 'ON' THEN
                        pcollectn(li_rec_Index).BID :=
                            quotes_rec.ON_BID;  
                    ELSE
                       pcollectn(li_rec_Index).BID :=
                            quotes_rec.OFF_BID;   
                    END IF;
                END IF;
                
                IF (  ( quotes_rec.ON_MID is not null  OR
                          quotes_rec.OFF_MID is not null 
                          )
                      ) THEN 
                    IF m2m_recs.ON_OFF_PEAK_INDICATOR = 'ON' THEN
                        pcollectn(li_rec_Index).MID :=
                            quotes_rec.ON_MID;  
                    ELSE
                       pcollectn(li_rec_Index).MID :=
                            quotes_rec.OFF_MID;   
                    END IF;                            
               END IF;
               
               IF (  ( quotes_rec.ON_ASK is not null  OR
                          quotes_rec.OFF_ASK is not null 
                          )
                      ) THEN 
                    IF m2m_recs.ON_OFF_PEAK_INDICATOR = 'ON' THEN
                        pcollectn(li_rec_Index).ASK :=
                            quotes_rec.ON_ASK  ;
                    ELSE
                       pcollectn(li_rec_Index).ASK :=
                            quotes_rec.OFF_ASK;   
                    END IF;
               END IF;
               
               CASE  
                WHEN ( Upper(quotes_rec.PRICE_TYPE) = 'MID' ) THEN 
                       pcollectn(li_rec_Index).MID :=
                            quotes_rec.VALUE;
                WHEN ( Upper(quotes_rec.PRICE_TYPE) = 'BID' ) THEN 
                       pcollectn(li_rec_Index).BID :=
                            quotes_rec.VALUE;                                
                WHEN ( Upper(quotes_rec.PRICE_TYPE) = 'ASK' ) THEN 
                       pcollectn(li_rec_Index).ASK :=
                            quotes_rec.VALUE;    
                ELSE
                   null;                    
               END CASE;
                                            
                  
              pcollectn(li_rec_Index).ext_CONTRACT_YEAR         :=   
                                         quotes_rec.COntract_year;         
 
              pcollectn(li_rec_Index).EXT_LOCATION           :=  
                                            quotes_rec.EXT_LOCATION;
                                                          
                -- Rowise as PRICE_TYPE OR VALUE 
               
               END LOOP; -- end of quotes_rec Loop
                 /***********************/
               
           
            
               -- If No External Quotes found for a m2m rec 
               -- then set NULL to all MID/BID/ASK and EXT_COntract_YEAR
               -- these are exclusive  attributes , only available if QUOTES 
               --exists
               -- Even though EXT_LOCATION is Important, we get this value from
               -- Value mapping column for each mapped internal Location.

               IF NOT lb_quotes_rec THEN 
                

                pcollectn(li_rec_Index).EXT_EFF_DATE  :=
                           NULL;
               
               --- check and assign MID/BID/ASK                  
                                
                pcollectn(li_rec_Index).MID := NULL;
                           
                pcollectn(li_rec_Index).BID := NULL;
                 
                pcollectn(li_rec_Index).ASK := NULL;
 
                pcollectn(li_rec_Index).ext_CONTRACT_YEAR         
                                            := NULL;    
                                                
               END IF;
               
                      
                
        ELSE
                
--                --dbms_output.Put_line ('Else clause EXT_LOCATION ');
                

                pcollectn(li_rec_Index).EXT_LOCATION 
                                        := NULL;
 
                pcollectn(li_rec_Index).EXT_PROFILE_ID  
                                        := NULL;
 
                pcollectn(li_rec_Index).ZEMA_PROFILE_ID 
                                        := NULL;
                pcollectn(li_rec_Index).ZEMA_PROFILE_DISPLAY_NAME 
                                        := NULL;            


                pcollectn(li_rec_Index).ext_Provider_id 
                                        := NULL; 

                pcollectn(li_rec_Index).EXT_EFF_DATE  
                                        :=NULL; 
                                        
                pcollectn(li_rec_Index).MID := NULL;
                           
                pcollectn(li_rec_Index).BID := NULL;
                 
                pcollectn(li_rec_Index).ASK := NULL;
 
                pcollectn(li_rec_Index).ext_CONTRACT_YEAR         
                                            := NULL;
                                                                                                             
        END IF;
           -- end of ext_location null check 
           
            
         /**MID/BID/ASK PROCESS for Monthly strips***/
--       dbms_output.Put_line('Calculating mid bid ask');          
         --calculate missing MID/BID/ASk
                 
         CASE
         WHEN ( pcollectn(li_rec_Index).MID is not null AND
                pcollectn(li_rec_Index).BID is not null AND
                pcollectn(li_rec_Index).ASK is not null
               )  THEN                           --All of them given
             null;   -- do nothing all r given
         WHEN ( pcollectn(li_rec_Index).MID is null AND
                pcollectn(li_rec_Index).BID is null AND
                pcollectn(li_rec_Index).ASK is null
               )  THEN          
             null;                       --none of them given
                    
         else
                  
           -- when any single value of MID/BID/ASK is given
           -- calculate the remaining 
                   
           case 
           WHEN
                    ( 
                    -- BID is given 
                    pcollectn(li_rec_Index).BID is not null   AND
                    pcollectn(li_rec_Index).MID is null       AND
                    pcollectn(li_rec_Index).ASK is null
                            
                    ) THEN
                    
              IF pcollectn(li_rec_Index).FINAL_FAS157_TYPE  = 'Price Override' THEN 
                
                pcollectn(li_rec_Index).MID := pcollectn(li_rec_Index).BID + pcollectn(li_rec_Index).FINAL_FAS157_VALUE; 
                pcollectn(li_rec_Index).ASK := pcollectn(li_rec_Index).MID + (pcollectn(li_rec_Index).MID - pcollectn(li_rec_Index).BID); 
                
              ELSE 
                      
                pcollectn(li_rec_Index).MID := pcollectn(li_rec_Index).BID * (1 + (pcollectn(li_rec_Index).FINAL_FAS157_VALUE /100)) ;
                pcollectn(li_rec_Index).ASK := pcollectn(li_rec_Index).MID + (pcollectn(li_rec_Index).MID - pcollectn(li_rec_Index).BID);
                
              END IF;
                 
          
           WHEN
                    ( -- MID is given 
                      pcollectn(li_rec_Index).MID is not null AND
                      pcollectn(li_rec_Index).BID is null     AND
                      pcollectn(li_rec_Index).ASK is null
                    ) THEN
                    
              IF pcollectn(li_rec_Index).FINAL_FAS157_TYPE  = 'Price Override' THEN 
                
                pcollectn(li_rec_Index).ASK := pcollectn(li_rec_Index).MID + pcollectn(li_rec_Index).FINAL_FAS157_VALUE ;                
                pcollectn(li_rec_Index).BID := pcollectn(li_rec_Index).MID + (pcollectn(li_rec_Index).MID - pcollectn(li_rec_Index).ASK) ; 
                 
                
              ELSE      
                
                pcollectn(li_rec_Index).ASK := pcollectn(li_rec_Index).MID * (1 + (pcollectn(li_rec_Index).FINAL_FAS157_VALUE /100)) ;
                pcollectn(li_rec_Index).BID := pcollectn(li_rec_Index).MID +  pcollectn(li_rec_Index).MID - pcollectn(li_rec_Index).ASK ;
               
              
              END IF;
              
           WHEN  
                    ( -- ask is given 
                     pcollectn(li_rec_Index).ASK is not null AND 
                     pcollectn(li_rec_Index).MID is null     AND
                      pcollectn(li_rec_Index).BID is null
                              
                    ) THEN
                    
             IF pcollectn(li_rec_Index).FINAL_FAS157_TYPE  = 'Price Override' THEN 
                
              pcollectn(li_rec_Index).MID := pcollectn(li_rec_Index).ASK - pcollectn(li_rec_Index).FINAL_FAS157_VALUE;
              pcollectn(li_rec_Index).BID := pcollectn(li_rec_Index).MID - ( pcollectn(li_rec_Index).ASK -  pcollectn(li_rec_Index).MID );    
               
                
                
             ELSE             
              pcollectn(li_rec_Index).MID := pcollectn(li_rec_Index).ASK/(1 + (pcollectn(li_rec_Index).FINAL_FAS157_VALUE/100)) ;   
              pcollectn(li_rec_Index).BID := pcollectn(li_rec_Index).MID - (pcollectn(li_rec_Index).ASK - pcollectn(li_rec_Index).MID);  
           
             END IF;
             

          WHEN
                    ( -- BID and MID given 
                      pcollectn(li_rec_Index).BID is not null AND
                      pcollectn(li_rec_Index).MID is not null AND
                      pcollectn(li_rec_Index).ASK is null
                    ) THEN
                    
             IF pcollectn(li_rec_Index).FINAL_FAS157_TYPE  = 'Price Override' THEN 
                
                pcollectn(li_rec_Index).ASK :=  pcollectn(li_rec_Index).MID +   ( pcollectn(li_rec_Index).MID - pcollectn(li_rec_Index).BID ); 
             
                
             ELSE                      
                     pcollectn(li_rec_Index).ASK  := pcollectn(li_rec_Index).MID + (pcollectn(li_rec_Index).MID - pcollectn(li_rec_Index).BID);              
             END IF ;
                       
          WHEN
                    ( 
                    -- BID and ASK are given 
                    pcollectn(li_rec_Index).BID is not null AND
                    pcollectn(li_rec_Index).ASK is not null AND 
                    pcollectn(li_rec_Index).MID is null 

                    ) THEN
  
                    pcollectn(li_rec_Index).MID := (pcollectn(li_rec_Index).BID + pcollectn(li_rec_Index).ASK)/2;
 
          WHEN
                    ( -- MID AND ASK given
                      pcollectn(li_rec_Index).MID is not null AND
                      pcollectn(li_rec_Index).ASK is not null AND 
                      pcollectn(li_rec_Index).BID is null 
                          
                    ) THEN      
           
                
                 pcollectn(li_rec_Index).BID  := pcollectn(li_rec_Index).MID - (pcollectn(li_rec_Index).ASK - pcollectn(li_rec_Index).MID); 
             
           end case ;
                  
         end CASE;

        IF pcollectn(li_rec_Index).MID IS NULL THEN
                   
            v_delta_price_deno := NULL;
            
        ELSIF pcollectn(li_rec_Index).MID= 0 THEN
            v_delta_price_deno := 1; 
        ELSE
             v_delta_price_deno := ABS(pcollectn(li_rec_Index).MID ) ;    
        END IF;
                
        pcollectn(li_rec_Index).DELTA_PRICE_VALUE 
        := abs(
              pcollectn(li_rec_Index).PROJ_LOC_AMT - 
               pcollectn(li_rec_Index).MID
              );
                
  
        pcollectn(li_rec_Index).DELTA_PRICE_PRCNT 
        := pcollectn(li_rec_Index).DELTA_PRICE_VALUE /v_delta_price_deno;
                
 
       /*******
           Validate the Curve 
       ******/
--         dbms_output.Put_line('intial validation');    
               
        --- compute level 1 and level 2 validation 
             CASE
               WHEN pcollectn (li_rec_Index).Curve_Rank = 1
               THEN
--                  DBMS_OUTPUT.Put_line ('Getting validation description');
                  pcollectn (li_rec_Index).VALIDATED_FLAG := 'Y';
                  pcollectn (li_rec_Index).FULLY_VALIDATED_FLAG := 'Y';
                  pcollectn (li_rec_Index).Curve_Validation_Description :=
                     'FULLY VALIDATED, POSITION = 0/M2M <>0';
               WHEN pcollectn (li_rec_Index).Curve_Rank = 2
               THEN
                  -- If not already ranked as 2 , thn rank it as 1
                  pcollectn (li_rec_Index).Curve_Validation_Description :=
                     'FULLY VALIDATED, APPROVED SYSTEM METHODOLOGY'; --skip it , it could be basis curve ranking/ISOCON deals ranking
                  pcollectn (li_rec_Index).VALIDATED_FLAG := 'Y';
                  pcollectn (li_rec_Index).FULLY_VALIDATED_FLAG := 'Y';
               
               WHEN pcollectn(li_rec_Index).PROJ_LOC_AMT is NULL THEN
                    
                  pcollectn(li_rec_Index).Curve_Validation_Description := 'NO INTERNAL PRICE';    
                          
                  pcollectn(li_rec_Index).VALIDATED_FLAG := 'N';
                  pcollectn(li_rec_Index).FULLY_VALIDATED_FLAG := 'N';


               WHEN  ( pcollectn(li_rec_Index).MID is NULL 
                        AND pcollectn(li_rec_Index).BID is NULL
                        AND pcollectn(li_rec_Index).ASK IS NULL ) THEN

                  pcollectn(li_rec_Index).Curve_Validation_Description := 'NO QUOTE';
                           
                  pcollectn(li_rec_Index).VALIDATED_FLAG := 'N';
                  pcollectn(li_rec_Index).FULLY_VALIDATED_FLAG := 'N';

               ELSE
                
                 
                   Derive_StripLevel_Measures(
                             pindex             =>   li_rec_Index
                            , pOriginalLocation  =>  pcollectn(li_rec_index).NEER_LOCATION 
                            , pOverrideLocation  =>  pOverrideLocation
                            , pbackbone_flag     =>  pbackbone_flag
                            , pOverridebb_flag   =>  pOverridebb_flag                                   
                            , pCurrentMonth      =>  pCurrentMOnth
                            , pstartYearMonth    =>  pstart_YearMonth
                            , PEndYearMOnth      =>  pEnd_YearMonth  
                            , pm2mData           =>  pmonthlym2m
                            , pcollection        =>  pcollectn
                            , pcalcType          =>  'VALIDATE_A_CURVE' 
                            , pout_rtn_code      =>  vn_rtn_code    
                            , pout_rtn_msg       =>  vc_rtn_msg 
                         );

               END CASE;
                
            -- Get the Indicator ID and  
               BEGIN
                    
                SELECT 
                   VALIDATION_INDICATOR_ID
                 INTO 
                   pcollectn(li_rec_Index).VALIDATION_INDICATOR_ID
                 FROM
                  RISKDB.QP_CURVE_VALIDATION v
                 where
                  Upper(VALIDATION_DESCRIPTION)  = (pcollectn(li_rec_Index).Curve_Validation_Description)
                  AND ACTIVE_FLAG = 'Y'
                  AND pcollectn(li_rec_Index).COB_DATE BETWEEN  
                        TRUNC(EFFECTIVE_START_DATE) AND 
                     pcollectn(li_rec_Index).COB_DATE
                  ;
                      
               EXCEPTION
               WHEN NO_DATA_FOUND THEN 
                 pcollectn(li_rec_Index).VALIDATION_INDICATOR_ID := NULL;
               END;
               



       IF pcollectn(li_rec_Index).NEER_COMMODITY =
          pcollectn(li_rec_Index).NEER_LOCATION  
          AND pcollectn(li_rec_Index).UNDERLYING_COMMODITY = 'ELECTRICITY' 
          AND pcollectn(li_rec_Index).ACTIVE_FLAG = 'Y' THEN
                 -- we have to add additional comodity record 
                 -- only change is m2m, delta position , legged m2m
                 -- and abolute delta would be sum @commdity level
                 --(all BAsis atributes same as LOC level )
                 -- FAS need to be recalculated at commodity
 


--            dbms_output.Put_line('Inside commodity strip');

                   PROCESS_LOG_REC.STAGE        := 'COMPUTE_COMMODITY_STRIP';
                    
                    Compute_commodity_strip(
                     pindex             =>  li_rec_Index
                    , pOriginalLocation  =>  pcollectn(li_rec_index).NEER_LOCATION 
                    , pOverrideLocation  =>  pOverrideLocation
                    , pbackbone_flag     =>  pbackbone_flag
                    , pOverridebb_flag   =>  pOverridebb_flag                             
                    , pCurrentMonth      =>  TO_NUMBER(pCurrentMOnth)
                    , pstartYearMonth    =>  pstart_YearMonth
                    , PEndYearMOnth      =>  pEnd_YearMonth  
                    , pm2mData           =>  pmonthlym2m
                    , pcollection        =>  pcollectn
                    , pcalcType          =>  'COMMODITY_LEVEL_SUM' 
                    , pout_rtn_code      =>  vn_rtn_code    
                    , pout_rtn_msg       =>  vc_rtn_msg 
                    );


       
                    select 
                    MIN(TO_NUMBER(v.contractYear_month))
                    , MAX (TO_NUMBER(v.contractYear_month))
                    INTO
                     pcollectn(li_rec_Index).min_ContractYEAR_Month
                     ,  pcollectn(li_rec_Index).max_ContractYEAR_Month
                    from 
                    RISKDB.QP_INT_FWD_CURVES_RAW_DATA v
                    where
                    v.cob_date = pcobdate
                    and v.underlying_commodity =  pcollectn(li_rec_Index).UNDERLYING_COMMODITY 
                    and v.commodity =  pcollectn(li_rec_Index).NEER_COMMODITY
                    --AND V.COMMODITY_ABSOLUTE_POSITION <> 0
                    AND ACTIVE_FLAG = 'Y'
                    ;
                    
   
                 IF   pcollectn(li_rec_Index).max_ContractYEAR_Month IS NOT NULL 
                     AND   pcollectn(li_rec_Index).min_ContractYEAR_Month IS NOT NULL 
                  THEN  
                     
                      pcollectn(li_rec_Index).tot_ContractYEAR_Month := 
                        MOnths_between( to_date(  pcollectn(li_rec_Index).max_ContractYEAR_Month||'01', 'YYYYMMDD') 
                                       ,  to_date(  pcollectn(li_rec_Index).min_ContractYEAR_Month||'01', 'YYYYMMDD')
                                      ) + 1;                    
                 ELSE
                      pcollectn(li_rec_Index).tot_ContractYEAR_Month := NULL;
                 END IF ;                   
                   
              
               --ADD RANK SIRI FOR MONTHLY COMMODITY
                 IF pcollectn (li_rec_Index).PRICE_VOL_INDICATOR ='PRICE' AND 
                     ( pcollectn (li_rec_Index).ABSOLUTE_M2M_VALUE <> 0 AND  
                     pcollectn (li_rec_Index).COMMODITY_ABS_POSITION =0)
                 THEN 
                     pcollectn (li_rec_Index).CURVE_RANK := 1;
                 END IF;
                 
                 
                 IF pcollectn (li_rec_Index).PRICE_VOL_INDICATOR ='VOL' AND 
                  ( pcollectn (li_rec_Index).ABS_LEGGED_M2M_VOL <> 0 AND  
                     pcollectn (li_rec_Index).COMMODITY_ABS_POSITION =0)
                 THEN 
                    pcollectn (li_rec_Index).CURVE_RANK := 1;
                 END IF;


                  PROCESS_LOG_REC.STAGE        := 'DERIVE_STRIPLEVEL_MEASURES';
                   

                                   
                   Derive_StripLevel_Measures(
                     pindex             =>   li_rec_Index
                    , pOriginalLocation  =>  pcollectn(li_rec_index).NEER_LOCATION 
                    , pOverrideLocation  =>  pOverrideLocation
                    , pbackbone_flag     =>  pbackbone_flag
                    , pOverridebb_flag   =>  pOverridebb_flag       
                    , pCurrentMonth      =>  pCurrentMOnth
                    , pstartYearMonth    =>  pstart_YearMonth
                    , PEndYearMOnth      =>  pEnd_YearMonth  
                    , pm2mData           =>  pmonthlym2m
                    , pcollection        =>  pcollectn
                    , pcalcType          =>  'VALIDATE_A_CURVE' 
                    , pout_rtn_code      =>  vn_rtn_code    
                    , pout_rtn_msg       =>  vc_rtn_msg 
                 );
                            
                   
                  PROCESS_LOG_REC.STAGE        := 'PREPARE_INTANDEXT_MONTHLY_QTS';  
        
                            
        END IF;
                               
     
                                                          
   END LOOP;
 -- End of Monthly Cursor   
   

 ELSE
 
 
 /************************************************************************
 **
 **              NON MONTHLY SECTION 
 ***********************************************************************/
-- Open Non Monthly Cursor 
   For m2m_recs in cur_NonMonthlym2m LOOP
   
    IF pcollectn.COUNT = 0 THEN 
     li_rec_Index :=  0 ;
    ELSE
    li_rec_Index :=pcollectn.COUNT;
    END IF;
    
    
    vn_Objects_START_indx  := pcollectn.COUNT;


--   dbms_output.Put_line ('vn_Objects_START_indx='|| vn_Objects_START_indx||'increment by 1 ') ;  

                   
    pcollectn.EXTEND;
    
    li_rec_index := li_rec_index + 1 ;
    
    
--     dbms_output.Put_line ('vn_Objects_START_indx='|| vn_Objects_START_indx||'rec Index='||li_rec_Index) ;
  
                              
                            
    pcollectn(li_rec_Index).MONTHLY_STRIP_FLAG := 'N'; 
    pcollectn(li_rec_Index).BALANCED_STRIP_FLAG := PBalancedFlag;
    pcollectn(li_rec_Index).NONBALANCED_STRIP_FLAG := PnonBalancedFlag;
    pcollectn(li_rec_Index).COMMODITY_STRIP_FLAG := 'N';                 
    pcollectn(li_rec_Index).SECONDARY_CALCULATION_FLAG := 'N'; 
    pcollectn(li_rec_Index).MANUAL_QUOTE_FLAG := 'N'; 
    pcollectn(li_rec_Index).VALIDATED_FLAG := 'N';
    pcollectn(li_rec_Index).FULLY_VALIDATED_FLAG := 'N';
                
    pcollectn(li_rec_Index).ACTIVE_FLAG := 'Y';  
    
    pcollectn(li_rec_Index).PROCESS_LEVEL          
                                := '3';
                                            
    pcollectn(li_rec_Index).PARTITION_BIT      :=  2; 
               
    pcollectn(li_rec_Index).EXT_MANUAL_QUOTE_ID       
                                := NULL;


    pcollectn(li_rec_Index).NETTING_GROUP_ID  
                            := m2m_recs.NETTING_GROUP_ID ;
                                        
    pcollectn(li_rec_index).UNDERLYING_COMMODITY
                            := m2m_recs.UNDERLYING_COMMODITY;      
        
    pcollectn(li_rec_index).NEER_COMMODITY  
                            := m2m_recs.COMMODITY; 
                                                 
    pcollectn(li_rec_index).NEER_LOCATION   
                            := m2m_recs.LOcation  ;
                            
   --- collect m2m information 
   pcollectn(li_rec_Index).COB_DATE 
                    := m2m_recs.COB_DATE;       
                 
                
   pcollectn(li_rec_Index).STRIP_START_YEARMONTH := 
                                 pstart_YearMonth;             
   pcollectn(li_rec_Index).STRIP_END_YEARMONTH   :=    
                                 pEnd_YearMonth; 
               
   pcollectn(li_rec_Index).TOTAL_TENOR_MONTHS    := 
     MOnths_between( to_date(pEnd_YearMonth||'01', 'YYYYMMDD') 
                     ,  to_date(pstart_YearMonth||'01', 'YYYYMMDD')
                    ) + 1;    



    IF pcollectn(li_rec_Index).BALANCED_STRIP_FLAG = 'Y' 
    THEN 
            IF SUBSTR(pstart_YearMonth, 5,6) between 01 and 05 THEN 
             pcollectn(li_rec_Index).CONTRACT_YEAR := (SUBSTR(pstart_YearMonth, 1,4) -1);
             --dbms_output.Put_line ('CONTRACT_YEAR :='|| pcollectn(li_rec_Index).CONTRACT_YEAR );
             
            ELSIF
             SUBSTR(pstart_YearMonth, 5,6) between 06 and 12 THEN
            pcollectn(li_rec_Index).CONTRACT_YEAR := m2m_recs.contract_year;
             --dbms_output.Put_line ('I MA HERE');
            END IF;
  
    ELSE
     
    pcollectn(li_rec_Index).CONTRACT_YEAR 
                            := m2m_recs.contract_year;
    END IF;



   pcollectn(li_rec_Index).MAX_CONTRACTYEAR_MONTH := pMaxYear;                 
   pcollectn(li_rec_Index).MIN_CONTRACTYEAR_MONTH := pMinYear;                 
   pcollectn(li_rec_Index).TOT_CONTRACTYEAR_MONTH  :=pTotalMonths ;

   pcollectn(li_rec_index).Curve_Rank := prank;
 
   pcollectn(li_rec_index).FLAT_POSITION_FLAG := pflatposition;
   
   
      -- SK 02/19/2016 , when overridden show overridden backbone flag  
   IF  pOverrideLocation IS NOT NULL THEN 
   
   pcollectn (li_rec_index).BACKBONE_FLAG := pOverridebb_flag ;
   
   ELSE
   
   pcollectn (li_rec_index).BACKBONE_FLAG := pbackbone_flag;
   
   END IF;
   --SK until here on 02/19/2016
   
   
   pcollectn (li_rec_index).BASIS_CURVE_INDICATOR  := pbasis_indicator;                                     
   pcollectn(li_rec_index).STRIP_TENOR  
                                :=  pstripName;  
 
                                                                                            
-- Derive these for Original Location  
   pcollectn(li_rec_index).BASISPOINT_OVERRIDE_ID    
                        := pOverrideID ; --m2m_recs.BASISPOINT_OVERRIDE_ID ;
  
                         
                                   

   IF pOverrideLocation IS NOT NULL THEN 
    pcollectn(li_rec_index).BASISPOINT_OVERRIDE_INDICATOR := 'Y' ;      
   ELSE
     pcollectn(li_rec_index).BASISPOINT_OVERRIDE_INDICATOR := 'N' ;      
   END IF;
   
                        
    

--    Get_hours 
                        
-- Until here Derive these for Original Location 
                                    
    pcollectn(li_rec_index).ON_OFF_PEAK_INDICATOR   
                       := m2m_recs.ON_OFF_PEAK_INDICATOR;  
                                         
    pcollectn(li_rec_index).HOUR_TYPE 
                       := m2m_recs.HOUR_TYPE;      
                                                   
       
    pcollectn(li_rec_index).PRICE_VOL_INDICATOR  
                    := m2m_recs.PRICE_VOL_INDICATOR ; 

 
--    dbms_output.Put_line ('calling Get Granularity '); 
    vc_return_val := Get_granularity (
                    pcobdate  => pcobdate
                  , pUnderlying_commodity => punderlyingCommodity
                  , pPrice_vol_Indicator => ppricevol
                  , ptenor_Months      => pcollectn(li_rec_Index).TOTAL_TENOR_MONTHS
                  ) ;
    
--    dbms_output.Put_line ('after calling Get Granularity '); 
                          
    PROCESS_LOG_REC.STAGE        := 'PREPARE_QVAR_STRIPS';
                

                
        IF    vc_return_val = 'G' THEN
           pcollectn(li_rec_Index).Granular_quote_indicator  := 'Y'  ;     
        ELSE
           pcollectn(li_rec_Index).Granular_quote_indicator  := 'N'  ;
        END IF;

  
    /*
      Basis Quotes are given at monthly Level
      , there are no strip level Quotes and hence 
      all basis information at this level 
      are explicitly set to NULL
    */                          
  --need to check                                                     
--    pcollectn(li_rec_index).BASIS_CURVE_INDICATOR
--                    := 'N'; --m2m_recs.BASIS_CURVE_INDICATOR; 
                                         
    pcollectn(li_rec_index).PHOENIX_BASIS_NAME 
                    := NULL; --m2m_recs.PHOENIX_BASIS_NAME;    
                                        
    pcollectn(li_rec_index).BASIS_QUOTE_DATE  
                    := NULL; --m2m_recs.BASIS_QUOTE_DATE;    
                                         
    pcollectn(li_rec_index).BASIS_QUOTE_AGE 
                    := NULL; --m2m_recs.BASIS_QUOTE_AGE;  
                                                 
    pcollectn(li_rec_index).TRADER_BASIS_QUOTE_PRICE  
                    := NULL; --m2m_recs.TRADER_BASIS_QUOTE_PRICE;  
                                    
    pcollectn(li_rec_index).USING_100PRCNT_HIST_METHOD 
                    := 'N'; --m2m_recs.USING_100PRCNT_HIST_METHOD; 


       
    vn_rtn_code := NULL;
    vc_rtn_msg := '';     
    

--dbms_output.Put_line ('calling  Derive_StripLevel_Measures '); 

           Derive_StripLevel_Measures(
                 pindex             =>  li_rec_Index
               , pOriginalLocation  =>  pcollectn(li_rec_index).NEER_LOCATION 
               , pOverrideLocation  =>  pOverrideLocation
               , pbackbone_flag     =>  pbackbone_flag
               , pOverridebb_flag   =>  pOverridebb_flag
               , pcUrrentMOnth      =>  TO_NUMBER(pCurrentMonth)
               , pstartYearMonth    =>  TO_NUMBER(pstart_yearMonth)
               , PEndYearMOnth      =>  TO_NUMBER(pend_yearMonth)
               , pm2mData           =>  pmonthlym2m
               , pcollection        =>  pcollectn
               , pcalcType          =>  'LOCATION_LEVEL_M2M' 
               , pout_rtn_code      =>  vn_rtn_code    
               , pout_rtn_msg       =>  vc_rtn_msg 
               );
 
--dbms_output.Put_line ('calling  Derive_StripLevel_Measures after LOCATION_LEVEL_M2M'); 
           
          Derive_StripLevel_Measures(
                 pindex             =>  li_rec_Index
               , pOriginalLocation  =>  pcollectn(li_rec_index).NEER_LOCATION 
               , pOverrideLocation  =>  pOverrideLocation
               , pbackbone_flag     =>  pbackbone_flag
               , pOverridebb_flag   =>  pOverridebb_flag
               , pcUrrentMOnth      =>  TO_NUMBER(pCurrentMonth)
               , pstartYearMonth    =>  TO_NUMBER(pstart_yearMonth)
               , PEndYearMOnth      =>  TO_NUMBER(pend_yearMonth)
               , pm2mData           =>  pmonthlym2m
               , pcollection        =>  pcollectn
               , pcalcType          =>  'LOCATION_PRICE' 
               , pout_rtn_code      =>  vn_rtn_code    
               , pout_rtn_msg       =>  vc_rtn_msg 
               );
 
--           dbms_output.Put_line (
--           'Derive_StripLevel_Measures '
--           ||'Price ='||pcollectn(li_rec_index).Proj_loc_amt
--           ||' Hours ='||NVL(pcollectn(li_rec_index).TOTAL_HOURS_PER_STRIP,1)
--           ||' Days='||NVL(pcollectn(li_rec_index).CALENDAR_DAYS_in_STRIP,1)
--           ||' Months='||NVL(pcollectn(li_rec_index).TOTAL_TENOR_MONTHS,1)
--           ); 

           -- Calculate strip Level PRICE
 
           CASE
            WHEN trim(pcollectn(li_rec_index).UNDERLYING_COMMODITY) = 'ELECTRICITY' THEN 
             
            --HOur weighted Price 
            -- total Strip Price * totoal Hours/total Hours 
            
 --          DBMS_OUTPUT.PUT_LINE ('tot Hours='||pcollectn(li_rec_index).TOTAL_HOURS_PER_STRIP);
            pcollectn(li_rec_index).Proj_loc_amt := 
               ( pcollectn(li_rec_index).Proj_loc_amt)/NVL(pcollectn(li_rec_index).TOTAL_HOURS_PER_STRIP,1);   


            WHEN trim(pcollectn(li_rec_index).UNDERLYING_COMMODITY) = 'OIL' THEN
           
--           DBMS_OUTPUT.PUT_LINE ('OIL tot MOnths='||pcollectn(li_rec_index).TOTAL_TENOR_MONTHS);
           
              pcollectn(li_rec_index).Proj_loc_amt := 
                 pcollectn(li_rec_index).Proj_loc_amt/NVL(pcollectn(li_rec_index).TOTAL_TENOR_MONTHS,1);   
           
            WHEN trim(pcollectn(li_rec_index).UNDERLYING_COMMODITY) IN ( 'NATURAL GAS','GAS', 'NG 6 MOAVG')        THEN
              -- Day weighted Price  


--            DBMS_OUTPUT.PUT_LINE ('NG tot DAYS='||pcollectn(li_rec_index).CALENDAR_DAYS_in_STRIP);
              pcollectn(li_rec_index).Proj_loc_amt := 
                  ( pcollectn(li_rec_index).Proj_loc_amt) /NVL(pcollectn(li_rec_index).CALENDAR_DAYS_in_STRIP,1);   

            WHEN trim(pcollectn(li_rec_index).UNDERLYING_COMMODITY) = 'CAPACITY'    THEN
              
--             DBMS_OUTPUT.PUT_LINE ('CAPACITY tot DAYS='||pcollectn(li_rec_index).CALENDAR_DAYS_in_STRIP);
              
              pcollectn(li_rec_index).Proj_loc_amt := 
                    pcollectn(li_rec_index).Proj_loc_amt/NVL(pcollectn(li_rec_index).TOTAL_TENOR_MONTHS,1);   
          
            WHEN trim(pcollectn(li_rec_index).UNDERLYING_COMMODITY) = 'EMISSIONS'   THEN     

--             DBMS_OUTPUT.PUT_LINE ('Emissionstot Months='||pcollectn(li_rec_index).TOTAL_TENOR_MONTHS);            
             
              pcollectn(li_rec_index).Proj_loc_amt := 
                    pcollectn(li_rec_index).Proj_loc_amt/NVL(pcollectn(li_rec_index).TOTAL_TENOR_MONTHS,1);   

            ELSE
              -- ----dbms_output.PUT_LINE('HELL NOOOO'||pcollectn(li_rec_index).UNDERLYING_COMMODITY);
              null;
            END CASE;
            
--          dbms_output.Put_line ('proj_loc_amt = '||pcollectn(li_rec_index).Proj_loc_amt);
--          dbms_output.Put_line ('calling  Derive_StripLevel_Measures TENOR_LEVEL_FAS'); 

                Derive_StripLevel_Measures(
                 pindex             =>  li_rec_Index
               , pOriginalLocation  =>  pcollectn(li_rec_index).NEER_LOCATION 
               , pOverrideLocation  =>  pOverrideLocation
               , pbackbone_flag     =>  pbackbone_flag
               , pOverridebb_flag   =>  pOverridebb_flag       
               , pcUrrentMOnth      =>  TO_NUMBER(pCurrentMonth)
               , pstartYearMonth    =>  TO_NUMBER(pstart_yearMonth)
               , PEndYearMOnth      =>  TO_NUMBER(pend_yearMonth)
               , pm2mData           =>  pmonthlym2m
               , pcollection        =>  pcollectn
               , pcalcType          =>  'TENOR_LEVEL_FAS'
               , pout_rtn_code      =>  vn_rtn_code    
               , pout_rtn_msg       =>  vc_rtn_msg 
               );        
               
--     dbms_output.Put_line ('calling  Derive_StripLevel_Measures after TENOR_LEVEL_FAS');
     
--     dbms_output.put_line ('m2mset.original_FAS157_Level->'
--      ||'origFAS='||pcollectn(li_rec_index).original_FAS157_Level
--      ||'FinalFAS='||pcollectn(li_rec_index).FINAL_FAS157_VALUE 
--      ||'finatype='||pcollectn(li_rec_index).FINAL_FAS157_TYPE
--      );
        
                      
    ---  collect external quotes data 
    
        IF m2m_recs.EXT_LOCATION iS NOT NULL THEN 
            
                
               pcollectn(li_rec_Index).EXT_LOCATION 
                        := m2m_recs.EXT_LOCATION ;
              
               pcollectn(li_rec_Index).EXT_PROFILE_ID  :=
                           m2m_recs.ext_profile_id;
               
               pcollectn(li_rec_Index).PRICE_VOL_PROFILE    :=
                           m2m_recs.PRICE_VOL_PROFILE;
               pcollectn(li_rec_Index).ZEMA_PROFILE_ID :=
                           m2m_recs.ZEMA_PROFILE_ID;
               pcollectn(li_rec_Index).ZEMA_PROFILE_DISPLAY_NAME :=
                           m2m_recs.ZEMA_PROFILE_DISPLAY_NAME;
               pcollectn(li_rec_Index).EXT_EFF_DATE  :=
                           m2m_recs.COB_DATE;            
               
               pcollectn(li_rec_Index).ext_Provider_id :=
                           m2m_recs.ext_provider_id;   
               
            --  IF  PBalancedFlag = 'Y' THEN 
                 pcollectn(li_rec_index).ext_contract_Year :=  pcollectn(li_rec_Index).CONTRACT_YEAR ;
               
               
               
               -- Construct external Object for this EXT Location 
               --Provider and Profile 
               
               
                    Select count(*) into vn_reccount
                    from 
                    TABLE(mapped_ext_quotes) ext
                    where
                    ext.ext_Provider_id = pcollectn(li_rec_Index).ext_Provider_id
                    and ext.ext_Profile_id = pcollectn(li_rec_Index).ext_Profile_id
                    and ext.ext_Location = pcollectn(li_rec_Index).ext_Location
                    ;
            
                 IF vn_reccount > 0 THEN 
                   -- lets not construct
                    null; 
                 ELSE
                   -- reset Local ext Object                
                     mapped_ext_quotes := null_ext_quotes; 
                   --lets construct Object 
                     Construct_Locquotes_object(
                             pcobdate               => pcobdate
                            , pProviderId           => m2m_recs.ext_provider_id 
                            , pProfileId            => m2m_recs.ext_profile_id 
                            , pextLocation          => m2m_recs.EXT_LOCATION  
                            , pextquotes            => mapped_ext_quotes 
                            , pout_rtn_code         => vn_rtn_code  
                            , pout_rtn_msg          => vc_rtn_msg
                            );
                  END IF;
                     
                            
            Select count(*) into vn_reccount
            from 
            TABLE(mapped_ext_quotes)
            ;

            vc_message := 'Ext-total_recs->'||to_CHAR(vn_reccount)
            ||' profid='||m2m_recs.ext_profile_id
            ||'Extloc ='||m2m_recs.EXT_LOCATION ;
 
           dbms_output.Put_line(vc_message);
           
                                      
                            
               BEGIN
                    SELECT 
                    ext.EXT_CATEGORY
                    , ext.EXT_PROVIDER
                    INTO 
                      pcollectn(li_rec_Index).DATASOURCE_CATEGORY 
                    , pcollectn(li_rec_Index).DATA_SOURCE_NAME 
                    FROM
                    TABLE (mapped_ext_quotes ) ext
                    where
                    EXT_PROVIDER_ID = m2m_recs.ext_provider_id
                    and rownum < 2
                    ;
               EXCEPTION
               WHEN NO_DATA_FOUND THEN 
                 dbms_output.Put_line ('inside EXT_LOCATION NO Provider found'||m2m_recs.ext_provider_id );
                 null;
               END; 
               
               
             lb_quotes_rec := FALSE;
             
             For quotes_rec IN cur_ext_quotes (
                                                  m2m_recs.ext_provider_id
                                                , m2m_recs.ext_profile_id
                                                , m2m_recs.EXT_LOCATION
                                                , pstripName
                                                , pcollectn(li_rec_Index).CONTRACT_YEAR   
                                              ) LOOP
                

                -- Quotes available   
                lb_quotes_rec := TRUE;      
                
--                dbms_output.Put_line ('entered quotes loop'
--                ||'extloc='||m2m_recs.EXT_LOCATION
--                ||'extcontractYR = '||m2m_recs.CONTRACT_YEAR
--                ||'profile='||m2m_recs.ext_profile_id
--                ||'MID/BID/ASK/ON_BID/OFF_BID/ON_MID/OFF_MID/ON_ASK/OFF_ASK/PRICE_TYPE/VAlue ='
--                ||quotes_rec.MID
--                ||'/'||quotes_rec.BID
--                ||'/'||quotes_rec.ASK
--                ||'/'||quotes_rec.ON_BID
--                ||'/'||quotes_rec.OFF_BID
--                ||'/'||quotes_rec.ON_MID
--                ||'/'||quotes_rec.OFF_MID
--                ||'/'||quotes_rec.ON_ASK
--                ||'/'||quotes_rec.OFF_ASK
--                ||'/'||quotes_rec.PRICE_TYPE
--                ||'/'||quotes_rec.VALUE
--                 ) ;
                 
               --- check adn assign MID/BID/ASK                  
                IF  ( quotes_rec.MID is not null OR
                        quotes_rec.BID is not null OR
                        quotes_rec.ASK is not null
                       ) THEN                 
                    pcollectn(li_rec_Index).MID :=
                                quotes_rec.MID;
                    pcollectn(li_rec_Index).BID :=
                                quotes_rec.BID;
                    pcollectn(li_rec_Index).ASK :=
                            quotes_rec.ASK;
                END IF;
                
                IF (  ( quotes_rec.ON_BID is not null  OR
                          quotes_rec.OFF_BID is not null 
                          )
                      ) THEN 
                    IF m2m_recs.ON_OFF_PEAK_INDICATOR = 'ON' THEN
                        pcollectn(li_rec_Index).BID :=
                            quotes_rec.ON_BID;  
                    ELSE
                       pcollectn(li_rec_Index).BID :=
                            quotes_rec.OFF_BID;   
                    END IF;
                END IF;
                
                IF (  ( quotes_rec.ON_MID is not null  OR
                          quotes_rec.OFF_MID is not null 
                          )
                      ) THEN 
                    IF m2m_recs.ON_OFF_PEAK_INDICATOR = 'ON' THEN
                        pcollectn(li_rec_Index).MID :=
                            quotes_rec.ON_MID;  
                    ELSE
                       pcollectn(li_rec_Index).MID :=
                            quotes_rec.OFF_MID;   
                    END IF;                            
               END IF;
               
               IF (  ( quotes_rec.ON_ASK is not null  OR
                          quotes_rec.OFF_ASK is not null 
                          )
                      ) THEN 
                    IF m2m_recs.ON_OFF_PEAK_INDICATOR = 'ON' THEN
                        pcollectn(li_rec_Index).ASK :=
                            quotes_rec.ON_ASK  ;
                    ELSE
                       pcollectn(li_rec_Index).ASK :=
                            quotes_rec.OFF_ASK;   
                    END IF;
               END IF;
               
                   CASE  
                    WHEN ( Upper(quotes_rec.PRICE_TYPE) = 'MID' ) THEN 
                           pcollectn(li_rec_Index).MID :=
                                quotes_rec.VALUE;
                    WHEN ( Upper(quotes_rec.PRICE_TYPE) = 'BID' ) THEN 
                           pcollectn(li_rec_Index).BID :=
                                quotes_rec.VALUE;                                
                    WHEN ( Upper(quotes_rec.PRICE_TYPE) = 'ASK' ) THEN 
                           pcollectn(li_rec_Index).ASK :=
                                quotes_rec.VALUE;    
                    ELSE
                       null;                    
                   END CASE;
                                            
                  
                  pcollectn(li_rec_Index).ext_CONTRACT_YEAR         :=   
                                             quotes_rec.COntract_year;         
 
                  pcollectn(li_rec_Index).EXT_LOCATION           :=  
                                                quotes_rec.EXT_LOCATION;
                                                          
                -- Rowise as PRICE_TYPE OR VALUE 
               
               END LOOP; -- end of quotes_rec Loop
                 /***********************/
               
           
            
               -- If No External Quotes found for a m2m rec 
               -- then set NULL to all MID/BID/ASK and EXT_COntract_YEAR
               -- these are exclusive  attributes , only available if QUOTES 
               --exists
               -- Even though EXT_LOCATION is Important, we get this value from
               -- Value mapping column for each mapped internal Location.

               IF NOT lb_quotes_rec THEN 
                


                    vn_rtn_code := NULL;
                    vc_rtn_msg := '';                           
               
              
--               dbms_output.Put_line ( 'calling secondary'
--               ||'profile id='||pcollectn(li_rec_Index).EXT_PROFILE_ID
--               ||' ext Loc='||pcollectn(li_rec_Index).EXT_LOCATION 
--               );
                                            
               Derive_secondary_Calcs(
                 pindex             => li_rec_Index
                , pstartYearMonth    => TO_NUMBER(pstart_YearMonth)
                , PEndYearMOnth      => TO_NUMBER(pEnd_YearMonth)
                , pextquotes         => mapped_ext_quotes 
                , pcollection        => pcollectn
                , pm2mData           => pmonthlym2m
                , pout_rtn_code      => vn_rtn_code    
                , pout_rtn_msg       => vc_rtn_msg                 
                );
                
--                 dbms_output.Put_line ( 'After calling secondary');
                            
                pcollectn(li_rec_Index).EXT_EFF_DATE  := pcobdate;
                
                      IF pout_rtn_msg   = 'Duplicate Record found' 
                       OR  pout_rtn_msg   = 'Missing months'   THEN 
           

 --                      dbms_output.Put_line ( 'Duplicate quotes found/Missing Month');
                        
                        pcollectn(li_rec_Index).MID := NULL;
                        pcollectn(li_rec_Index).BID := NULL;
                        pcollectn(li_rec_Index).ASK := NULL;
                      
                      END IF;  --END IF OF C_FAIURE      
                             
               END IF;
                 -- End of NOT lb_quotes_rec   check 
                                  
             
               
        ELSE
                
--                --dbms_output.Put_line ('Else clause EXT_LOCATION ');
                

                pcollectn(li_rec_Index).EXT_LOCATION 
                                        := NULL;
 
                pcollectn(li_rec_Index).EXT_PROFILE_ID  
                                        := NULL;
               

                pcollectn(li_rec_Index).ZEMA_PROFILE_ID 
                                        := NULL;
                pcollectn(li_rec_Index).ZEMA_PROFILE_DISPLAY_NAME 
                                        := NULL;            


                pcollectn(li_rec_Index).ext_Provider_id 
                                        := NULL; 

                pcollectn(li_rec_Index).EXT_EFF_DATE  
                                        := NULL; 
                                        
                pcollectn(li_rec_Index).MID := NULL;
                           
                pcollectn(li_rec_Index).BID := NULL;
                 
                pcollectn(li_rec_Index).ASK := NULL;
 
                pcollectn(li_rec_Index).ext_CONTRACT_YEAR         
                                            := NULL;
                                                                                                             
        END IF;
           -- end of ext_location null check 
           
           
         /**MID/BID/ASK PROCESS for Monthly strips***/
                 
         --calculate missing MID/BID/ASk
         
--        dbms_output.put_line('before calculating mid/bid/ask');         
                 
         /**MID/BID/ASK PROCESS for Monthly strips***/
--       dbms_output.Put_line('Calculating mid bid ask');          
         --calculate missing MID/BID/ASk
                 
         CASE
         WHEN ( pcollectn(li_rec_Index).MID is not null AND
                pcollectn(li_rec_Index).BID is not null AND
                pcollectn(li_rec_Index).ASK is not null
               )  THEN                           --All of them given
             null;   -- do nothing all r given
         WHEN ( pcollectn(li_rec_Index).MID is null AND
                pcollectn(li_rec_Index).BID is null AND
                pcollectn(li_rec_Index).ASK is null
               )  THEN          
             null;                       --none of them given
                    
         else
                  
           -- when any single value of MID/BID/ASK is given
           -- calculate the remaining 
                   
           case 
           WHEN
                    ( 
                    -- BID is given 
                    pcollectn(li_rec_Index).BID is not null   AND
                    pcollectn(li_rec_Index).MID is null       AND
                    pcollectn(li_rec_Index).ASK is null
                            
                    ) THEN
                    
              IF pcollectn(li_rec_Index).FINAL_FAS157_TYPE  = 'Price Override' THEN 
                
                pcollectn(li_rec_Index).MID := pcollectn(li_rec_Index).BID + pcollectn(li_rec_Index).FINAL_FAS157_VALUE; 
                pcollectn(li_rec_Index).ASK := pcollectn(li_rec_Index).MID + (pcollectn(li_rec_Index).MID - pcollectn(li_rec_Index).BID); 
                
              ELSE 
                      
                pcollectn(li_rec_Index).MID := pcollectn(li_rec_Index).BID * (1 + (pcollectn(li_rec_Index).FINAL_FAS157_VALUE /100)) ;
                pcollectn(li_rec_Index).ASK := pcollectn(li_rec_Index).MID + (pcollectn(li_rec_Index).MID - pcollectn(li_rec_Index).BID);
                
              END IF;
                 
          
           WHEN
                    ( -- MID is given 
                      pcollectn(li_rec_Index).MID is not null AND
                      pcollectn(li_rec_Index).BID is null     AND
                      pcollectn(li_rec_Index).ASK is null
                    ) THEN
                    
              IF pcollectn(li_rec_Index).FINAL_FAS157_TYPE  = 'Price Override' THEN 
                
                pcollectn(li_rec_Index).ASK := pcollectn(li_rec_Index).MID + pcollectn(li_rec_Index).FINAL_FAS157_VALUE ;                
                pcollectn(li_rec_Index).BID := pcollectn(li_rec_Index).MID + (pcollectn(li_rec_Index).MID - pcollectn(li_rec_Index).ASK) ; 
                 
                
              ELSE      
                
                pcollectn(li_rec_Index).ASK := pcollectn(li_rec_Index).MID * (1 + (pcollectn(li_rec_Index).FINAL_FAS157_VALUE /100)) ;
                pcollectn(li_rec_Index).BID := pcollectn(li_rec_Index).MID +  pcollectn(li_rec_Index).MID - pcollectn(li_rec_Index).ASK ;
               
              
              END IF;
              
           WHEN  
                    ( -- ask is given 
                     pcollectn(li_rec_Index).ASK is not null AND 
                     pcollectn(li_rec_Index).MID is null     AND
                      pcollectn(li_rec_Index).BID is null
                              
                    ) THEN
                    
             IF pcollectn(li_rec_Index).FINAL_FAS157_TYPE  = 'Price Override' THEN 
                
              pcollectn(li_rec_Index).MID := pcollectn(li_rec_Index).ASK - pcollectn(li_rec_Index).FINAL_FAS157_VALUE;
              pcollectn(li_rec_Index).BID := pcollectn(li_rec_Index).MID - ( pcollectn(li_rec_Index).ASK -  pcollectn(li_rec_Index).MID );    
               
                
                
             ELSE             
              pcollectn(li_rec_Index).MID := pcollectn(li_rec_Index).ASK/(1 + (pcollectn(li_rec_Index).FINAL_FAS157_VALUE/100)) ;   
              pcollectn(li_rec_Index).BID := pcollectn(li_rec_Index).MID - (pcollectn(li_rec_Index).ASK - pcollectn(li_rec_Index).MID);  
           
             END IF;
             

          WHEN
                    ( -- BID and MID given 
                      pcollectn(li_rec_Index).BID is not null AND
                      pcollectn(li_rec_Index).MID is not null AND
                      pcollectn(li_rec_Index).ASK is null
                    ) THEN
                    
             IF pcollectn(li_rec_Index).FINAL_FAS157_TYPE  = 'Price Override' THEN 
                
                pcollectn(li_rec_Index).ASK :=  pcollectn(li_rec_Index).MID +   ( pcollectn(li_rec_Index).MID - pcollectn(li_rec_Index).BID ); 
             
                
             ELSE                      
                     pcollectn(li_rec_Index).ASK  := pcollectn(li_rec_Index).MID + (pcollectn(li_rec_Index).MID - pcollectn(li_rec_Index).BID);              
             END IF ;
                       
          WHEN
                    ( 
                    -- BID and ASK are given 
                    pcollectn(li_rec_Index).BID is not null AND
                    pcollectn(li_rec_Index).ASK is not null AND 
                    pcollectn(li_rec_Index).MID is null 

                    ) THEN
  
                    pcollectn(li_rec_Index).MID := (pcollectn(li_rec_Index).BID + pcollectn(li_rec_Index).ASK)/2;
 
          WHEN
                    ( -- MID AND ASK given
                      pcollectn(li_rec_Index).MID is not null AND
                      pcollectn(li_rec_Index).ASK is not null AND 
                      pcollectn(li_rec_Index).BID is null 
                          
                    ) THEN      
           
                
                 pcollectn(li_rec_Index).BID  := pcollectn(li_rec_Index).MID - (pcollectn(li_rec_Index).ASK - pcollectn(li_rec_Index).MID); 
             
           end case ;
                  
         end CASE;      


        IF pcollectn(li_rec_Index).MID IS NULL THEN
                   
            v_delta_price_deno := NULL;
            
        ELSIF pcollectn(li_rec_Index).MID= 0 THEN
            v_delta_price_deno := 1; 
        ELSE
             v_delta_price_deno := ABS(pcollectn(li_rec_Index).MID ) ;    
        END IF;
       

    --- calculate delta price n price % 
        pcollectn(li_rec_Index).DELTA_PRICE_VALUE 
        := abs(
              pcollectn(li_rec_Index).PROJ_LOC_AMT - 
               pcollectn(li_rec_Index).MID
              );
                
  
        pcollectn(li_rec_Index).DELTA_PRICE_PRCNT 
        := pcollectn(li_rec_Index).DELTA_PRICE_VALUE /v_delta_price_deno;



       /*******
           Validate the Curve 
       ******/
--         dbms_output.Put_line('intial validation');    
               
        --- compute level 1 and level 2 validation 
        CASE
        WHEN pcollectn (li_rec_Index).Curve_Rank = 1
        THEN
--                  DBMS_OUTPUT.Put_line ('Getting validation description');
                  pcollectn (li_rec_Index).VALIDATED_FLAG := 'Y';
                  pcollectn (li_rec_Index).FULLY_VALIDATED_FLAG := 'Y';
                  pcollectn (li_rec_Index).Curve_Validation_Description :=
                     'FULLY VALIDATED, POSITION = 0/M2M <>0';
        WHEN pcollectn (li_rec_Index).Curve_Rank = 2
        THEN
                  -- If not already ranked as 2 , thn rank it as 1
                  pcollectn (li_rec_Index).Curve_Validation_Description :=
                     'FULLY VALIDATED, APPROVED SYSTEM METHODOLOGY'; --skip it , it could be basis curve ranking/ISOCON deals ranking
                  pcollectn (li_rec_Index).VALIDATED_FLAG := 'Y';
                  pcollectn (li_rec_Index).FULLY_VALIDATED_FLAG := 'Y';
            
        WHEN pcollectn(li_rec_Index).PROJ_LOC_AMT is NULL THEN
            
          pcollectn(li_rec_Index).Curve_Validation_Description := 'NO INTERNAL PRICE';    
                  
          pcollectn(li_rec_Index).VALIDATED_FLAG := 'N';
          pcollectn(li_rec_Index).FULLY_VALIDATED_FLAG := 'N';


        WHEN  ( pcollectn(li_rec_Index).MID is NULL 
                AND pcollectn(li_rec_Index).BID is NULL
                AND pcollectn(li_rec_Index).ASK IS NULL ) THEN

          pcollectn(li_rec_Index).Curve_Validation_Description := 'NO QUOTE';
                   
          pcollectn(li_rec_Index).VALIDATED_FLAG := 'N';
          pcollectn(li_rec_Index).FULLY_VALIDATED_FLAG := 'N';

        ELSE
        
--          dbms_output.put_line('call Validate curve');   
           Derive_StripLevel_Measures(
                     pindex             =>   li_rec_Index
                    , pOriginalLocation  =>  pcollectn(li_rec_index).NEER_LOCATION 
                    , pOverrideLocation  =>  pOverrideLocation
                    , pbackbone_flag     =>  pbackbone_flag
                    , pOverridebb_flag   =>  pOverridebb_flag       
                    , pCurrentMonth      =>  pCurrentMOnth
                    , pstartYearMonth    =>  pstart_YearMonth
                    , PEndYearMOnth      =>  pEnd_YearMonth  
                    , pm2mData           =>  pmonthlym2m
                    , pcollection        =>  pcollectn
                    , pcalcType          =>  'VALIDATE_A_CURVE' 
                    , pout_rtn_code      =>  vn_rtn_code    
                    , pout_rtn_msg       =>  vc_rtn_msg 
                 );
        END CASE;
      
--      dbms_output.put_line('after Validate a curve ');
     -- Get the Indicator ID and  
       BEGIN
                    
        SELECT 
           VALIDATION_INDICATOR_ID
         INTO 
           pcollectn(li_rec_Index).VALIDATION_INDICATOR_ID
         FROM
          RISKDB.QP_CURVE_VALIDATION v
         where
          Upper(VALIDATION_DESCRIPTION)  = (pcollectn(li_rec_Index).Curve_Validation_Description)
          AND ACTIVE_FLAG = 'Y'
          AND pcollectn(li_rec_Index).COB_DATE BETWEEN  
                TRUNC(EFFECTIVE_START_DATE) AND 
             pcollectn(li_rec_Index).COB_DATE
          ;
                      
       EXCEPTION
       WHEN NO_DATA_FOUND THEN 
         pcollectn(li_rec_Index).VALIDATION_INDICATOR_ID := NULL;
       END;
               
--      dbms_output.put_line('after Validate a curve ID ');

       IF pcollectn(li_rec_Index).NEER_COMMODITY =
          pcollectn(li_rec_Index).NEER_LOCATION  
          AND pcollectn(li_rec_Index).UNDERLYING_COMMODITY = 'ELECTRICITY' 
          AND pcollectn(li_rec_Index).ACTIVE_FLAG = 'Y' THEN
                 -- we have to add additional comodity record 
                 -- only change is m2m, delta position , legged m2m
                 -- and abolute delta would be sum @commdity level
                 --(all BAsis atributes same as LOC level )
                 -- FAS need to be recalculated at commodity
 

--      dbms_output.put_line(' non monthly commodity section  ');


                   PROCESS_LOG_REC.STAGE        := 'COMPUTE_COMMODITY_STRIP';
                    
                    Compute_commodity_strip(
                     pindex             =>  li_rec_Index
                    , pOriginalLocation  =>  pcollectn(li_rec_index).NEER_LOCATION 
                    , pOverrideLocation  =>  pOverrideLocation
                    , pbackbone_flag     =>  pbackbone_flag
                    , pOverridebb_flag   =>  pOverridebb_flag       
                    , pCurrentMonth      =>  TO_NUMBER(pCurrentMOnth)
                    , pstartYearMonth    =>  pstart_YearMonth
                    , PEndYearMOnth      =>  pEnd_YearMonth  
                    , pm2mData           =>  pmonthlym2m
                    , pcollection        =>  pcollectn
                    , pcalcType          =>  'COMMODITY_LEVEL_SUM' 
                    , pout_rtn_code      =>  vn_rtn_code    
                    , pout_rtn_msg       =>  vc_rtn_msg 
                    );


--                  ----dbms_output.Put_line('commdty min max contract yrs');
                  
                    select 
                    MIN(TO_NUMBER(v.contractYear_month))
                    , MAX (TO_NUMBER(v.contractYear_month))
                    INTO
                     pcollectn(li_rec_Index).min_ContractYEAR_Month
                     ,  pcollectn(li_rec_Index).max_ContractYEAR_Month
                    from 
                    RISKDB.QP_INT_FWD_CURVES_RAW_DATA v
                    where
                    v.cob_date = pcobdate
                    and v.underlying_commodity =  pcollectn(li_rec_Index).UNDERLYING_COMMODITY 
                    and v.commodity =  pcollectn(li_rec_Index).NEER_COMMODITY
                    --AND V.COMMODITY_ABSOLUTE_POSITION <> 0
                    AND ACTIVE_FLAG = 'Y'
                    ;
                    
   
                 IF   pcollectn(li_rec_Index).max_ContractYEAR_Month IS NOT NULL 
                     AND   pcollectn(li_rec_Index).min_ContractYEAR_Month IS NOT NULL 
                  THEN  
                     
                      pcollectn(li_rec_Index).tot_ContractYEAR_Month := 
                        MOnths_between( to_date(  pcollectn(li_rec_Index).max_ContractYEAR_Month||'01', 'YYYYMMDD') 
                                       ,  to_date(  pcollectn(li_rec_Index).min_ContractYEAR_Month||'01', 'YYYYMMDD')
                                      ) + 1;                    
                 ELSE
                      pcollectn(li_rec_Index).tot_ContractYEAR_Month := NULL;
                 END IF ;                   
                   
                   PROCESS_LOG_REC.STAGE        := 'DERIVE_STRIPLEVEL_MEASURES';
                   
               --ADD RANK SIRI FOR MONTHLY COMMODITY
                 IF pcollectn (li_rec_Index).PRICE_VOL_INDICATOR ='PRICE' AND 
                     ( pcollectn (li_rec_Index).ABSOLUTE_M2M_VALUE <> 0 AND  
                     pcollectn (li_rec_Index).COMMODITY_ABS_POSITION =0)
                 THEN 
                     pcollectn (li_rec_Index).CURVE_RANK := 1;
                 END IF;
                 
                 
                 IF pcollectn (li_rec_Index).PRICE_VOL_INDICATOR ='VOL' AND 
                  ( pcollectn (li_rec_Index).ABS_LEGGED_M2M_VOL <> 0 AND  
                     pcollectn (li_rec_Index).COMMODITY_ABS_POSITION =0)
                 THEN 
                    pcollectn (li_rec_Index).CURVE_RANK := 1;
                 END IF;
                                   
                   Derive_StripLevel_Measures(
                     pindex             =>   li_rec_Index
                    , pOriginalLocation  =>  pcollectn(li_rec_index).NEER_LOCATION 
                    , pOverrideLocation  =>  pOverrideLocation
                    , pbackbone_flag     =>  pbackbone_flag
                    , pOverridebb_flag   =>  pOverridebb_flag       
                    , pCurrentMonth      =>  pCurrentMOnth
                    , pstartYearMonth    =>  pstart_YearMonth
                    , PEndYearMOnth      =>  pEnd_YearMonth  
                    , pm2mData           =>  pmonthlym2m
                    , pcollection        =>  pcollectn
                    , pcalcType          =>  'VALIDATE_A_CURVE' 
                    , pout_rtn_code      =>  vn_rtn_code    
                    , pout_rtn_msg       =>  vc_rtn_msg 
                 );
                            
                   


                  PROCESS_LOG_REC.STAGE        := 'PREPARE_INTANDEXT_MONTHLY_QTS';  
        
                            
        END IF; 
                
                                    
                                             
   END LOOP;
 -- End of NonMonthly Cursor 
 END IF;
  --End of pstart_YearMonth = pEnd_YearMonth  
      
   
     
     
     
EXCEPTION
WHEN OTHERS THEN 
       
  --dbms_output.PUT_LINE('EXCEPTION-'||pstartYearMonth||'-'||PEndYearMOnth||'-'||pcalcType);
       
   pout_rtn_code := c_failure;
   pout_rtn_msg := SUBSTR(SQLERRM,1,1000); --Null means Success
   
   
   DMR.PROCESS_LOG_PKG.WRITE_LOG(
        P_PROCESS_NAME => PROCESS_LOG_REC.PROCESS_NAME,
        P_STAGE=> PROCESS_LOG_REC.STAGE,
        P_STATUS=>PROCESS_LOG_PKG.C_STATUS_ERRORS,
        P_MESSAGE=>pout_rtn_msg
   );     
    
END  Prepare_qvar_strips;



PROCEDURE Get_backbone_flag ( PUNDERLYING_COMMODITY IN         VARCHAR2,
                              pLocation             IN         VARCHAR2,
                              pbackbone_flag        OUT        VARCHAR2,
                              pout_rtn_code         OUT NOCOPY NUMBER,
                              pout_rtn_msg          OUT NOCOPY VARCHAR2)
IS
   vc_section        VARCHAR2 (1000) := 'Get_backbone_flag';
   v_backbone_flag   RISKDB.QP_INT_FWD_CURVES_RAW_DATA.backbone_flag%TYPE;
   
BEGIN

CASE 
 WHEN  PUNDERLYING_COMMODITY = 'CAPACITY'  THEN
        Pbackbone_flag := 'Y';
  
 WHEN  PUNDERLYING_COMMODITY = 'ELECTRICITY' THEN
        -- THIS QUERY RETURNS A SET OF LOCATIONS WITH A SET BACKBONE FLAG THAT COMES FROM THE POWER TRANSFER POINTS TABLE
        --ELECTRICITY
     BEGIN
            SELECT DISTINCT 
            ptp.BACKBONE_FLAG
              INTO v_backbone_flag
              FROM PHOENIX.POWER_TRANSFER_POINTS ptp
                   INNER JOIN RISKDB.PRICE_CURVE_MAPPING_MV m
                      ON ( ptp.ISO_REF = m.BASIS_NODEID -- THE BASIS_NODEID AND ISO_REF ARE THE JOIN FIELDS FOR THESE TWO TABLES
                         and  m.ISO_ID = ptp.ISO_ID 
                         )
             WHERE m.BASIS_NAME = PLocation --pLocation
          GROUP BY m.BASIS_NAME, ptp.BACKBONE_FLAG;
          
          pbackbone_flag :=v_backbone_flag;
          

--          IF v_backbone_flag = 'Y'
--          THEN
--             pbackbone_flag := 'Y';
--          ELSIF v_backbone_flag  = 'N' THEN 
--            pbackbone_flag := 'N';
--          ELSE -- Price defaulted to PROJ_BASIS_AMT 
--             pInternal_curves (ptargetIndex).backbone_flag := v_backbone_flag;
--                
         -- END IF;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN 
         v_backbone_flag := NULL ;
        WHEN TOO_MANY_ROWS THEN 
         v_backbone_flag := NULL ;
        WHEN OTHERS THEN 
         v_backbone_flag := NULL ;
    END; 
   
  --NON ELECTRICITY
  WHEN  pUNDERLYING_COMMODITY <>
            'ELECTRICITY' AND pUNDERLYING_COMMODITY <> 'CAPACITY'
   THEN
       BEGIN 
       
         Select 
            CASE 
            WHEN nc.price_id = nc.bb_price_id THEN 'Y' ELSE 'N' 
            END   INTO v_backbone_flag
         FROM 
            nucdba.commodities cm
            , riskdb.nuc_curve nc
         WHERE
            nc.cm_commodity = cm.commodity
            AND cm.unc_underlying_commodity =
                                       pUnderlying_commodity 
            and nc.bp_basis_point = pLOCATION
         ;

        EXCEPTION
        WHEN NO_DATA_FOUND THEN 
         v_backbone_flag := NULL ;
        WHEN TOO_MANY_ROWS THEN 
         v_backbone_flag := NULL ;
        WHEN OTHERS THEN 
         v_backbone_flag := 'N' ;
        END;

      IF v_backbone_flag IS NOT NULL AND v_backbone_flag = 'Y'THEN 
      
        pbackbone_flag := 'Y';
      ELSIF v_backbone_flag IS NOT NULL AND v_backbone_flag = 'N' THEN
         pbackbone_flag  := 'N';
      ELSE
         pbackbone_flag  := NULL;
      END IF;
ELSE
  NULL;
END CASE; 

   --- Return Success info
   pout_rtn_code := c_success;
   pout_rtn_msg := '';                                    --Null means Success

END Get_backbone_flag;


Procedure Build_strips      (   PCOBDATE         IN  DATE DEFAULT NULL
                                , PSTATUSCODE    OUT VARCHAR2
                                , PSTATUSMSG     OUT VARCHAR2 
                                )
is     


Monthly_m2m_tab           m2m_by_Loc_tab; -- Table of TYPE Objects
Null_m2m_tab              m2m_by_Loc_tab; -- automaically NULL Object      
      

-- adding a Price Object to store Prices
Price_tab                m2m_by_Loc_tab; -- Table of TYPE Objects
Null_price_tab           m2m_by_Loc_tab; -- automaically NULL Object 


-- for m2m% caclculations 
m2m_tab_obj               riskdb.qp_strip_tabtype_Obj; -- automaically NULL Object
null_m2m_tabObj           riskdb.qp_strip_tabtype_Obj; -- to reset when required  


v_QvarStrips                   strips_collectn_t; 
v_locQvarstrips                strips_collectn_t; 
 
vn_records                     NUMBER := 0;
vn_recCount                    NUMBER := 0; 
 
vn_objects_count               NUMBER := 0; --total elements to be inserted in m2m%Obj
vn_Objects_START_indx          NUMBER := 0; --start Bound
Vn_Objects_End_indx            NUMBER := 0; -- end Bound of m2m%Obj to be inserted in current call
Vn_objindx                     NUMBER := 0; -- m2m%Obj index 

  
vc_cobdate       VARCHAR2(12) ;
vd_cobdate       date ;
  
vc_CurrentMonth    VARCHAR2(6) ;
vc_promptMonth     VARCHAR2(6) ;
vc_SQL            VARCHAR2(1000);   

vc_Quarterstart          VARCHAR2(10);
vc_QuarterEnd            VARCHAR2(10);

vc_contractYear          NUMBER(4);

  
li_ERRORS                       NUMBER;
  

vc_section                     VARCHAR2(100);
vn_rtn_code                    NUMBER;
vc_rtn_msg                     VARCHAR2(2000);
vc_sql_stmt                    VARCHAR2(1000);
vc_singleQuote                 VARCHAR2(1) := CHR(39);
 
vn_starttimeStamp             DATE := SYSDATE ;

vc_StartAYYYYMM          VARCHAR2(10); 
vc_endAYYYYMM            VARCHAR2(10);
vc_stripName             VARCHAR2(100);

  e_Invalid_Strip_startMonth         EXCEPTION;
   PRAGMA EXCEPTION_INIT( e_Invalid_Strip_startMonth , -20040);

  e_Invalid_Strip_EndMonth          EXCEPTION;
   PRAGMA EXCEPTION_INIT( e_Invalid_Strip_EndMonth , -20041);
   
   e_Appl_error                     EXCEPTION;
   PRAGMA EXCEPTION_INIT( e_Appl_error , -20042);
   
vc_monthly_flag           VARCHAR2(1) := 'N';
vc_Balanced_flag          VARCHAR2(1) := 'N';
vc_NonBalanced_flag       VARCHAR2(1) := 'N';
  v_b_char            VARCHAR2 (1);
  v_b_digits          VARCHAR2 (3);
  v_cob_date_yr       VARCHAR2 (20);
vn_maxYear              PLS_INTEGER; 
vn_MinYear              PLS_INTEGER;
vn_TotalMonths          PLS_INTEGER;  
vn_LocationRank         PLS_INTEGER;
vc_flat_position        VARCHAR2(1);
vc_backbone_flag         VARCHAR2 (1);
vc_Override_bb_flag       Varchar2(1);  
v_basis_indicator   RISKDB.QP_INT_FWD_CURVES_RAW_DATA.BASIS_CURVE_INDICATOR%TYPE;
vn_NettingGroup         RISKDB.QP_STRIP_LVL_NEER_EXT_DATA.netting_group_id%TYPE;
vc_commodity            RISKDB.QP_STRIP_LVL_NEER_EXT_DATA.NEER_COMMODITY%TYPE;
vc_overcommodity        RISKDB.QP_STRIP_LVL_NEER_EXT_DATA.NEER_COMMODITY%TYPE;
vc_uc_commodity         RISKDB.QP_STRIP_LVL_NEER_EXT_DATA.UNDERLYING_COMMODITY%TYPE;
vc_pricevol             RISKDB.QP_STRIP_LVL_NEER_EXT_DATA.PRICE_VOL_INDICATOR%TYPE;
  v_loop_flag              BOOLEAN := FALSE;
v_excp_price_flag            VARCHAR2 (1);

v_OverrideID                NUMBER;
v_denom                     NUMBER;
v_Granularm2m               NUMBER;
v_granular_m2mPercent       NUMBER;
v_nonGranularm2m            NUMBER;
v_nongranular_m2mPercent    NUMBER;
v_m2mbasis                  NUMBER;
v_m2mbasis_m2mPercent       NUMBER;

vc_location                 RISKDB.QP_STRIP_LVL_NEER_EXT_DATA.NEER_LOCATION%TYPE;
vc_originalLocation         RISKDB.QP_STRIP_LVL_NEER_EXT_DATA.NEER_LOCATION%TYPE;
vc_overrideLocation         RISKDB.QP_STRIP_LVL_NEER_EXT_DATA.NEER_LOCATION%TYPE;
v_overrideHours             NUMBER;
v_Hours                     NUMBER;

Cursor Cur_location IS 
SELECT DISTINCT
      rd.COB_DATE
    , rd.netting_group_id                         
    , rd.UNDERLYING_COMMODITY           
    , rd.COMMODITY                      
    , rd.LOCATION           
    , rd.PRICE_VOL_INDICATOR
--    , rd.CONTRACTYEAR_MONTH             
--    , rd.CONTRACT_YEAR                  
FROM
    RISKDB.QP_INT_FWD_CURVES_RAW_DATA rd 
WHERE
rd.cob_date = vd_cobdate --to_date('01/07/2015', 'MM/DD/YYYY')--
and to_number(rd.contractYear_Month) > to_number(vc_currentMonth)
--and rd.Underlying_commodity = 'ELECTRICITY'
--and rd.commodity = 'NEPOOL-.H.INTERNAL_HUB-5x16'
--and rd.LOCATION   = 'ALGONQUIN'
--IN ( 
--'MISO-INDIANA.HUB-WEC.PTBHGB2-5x16'
--,'MISO-INDIANA.HUB-5x16'
--,'MISO-INDIANA.HUB-7x8'
--,'MISO-INDIANA.HUB-DA-2x16'
--,'MISO-INDIANA.HUB-DA-5x16'
--,'MISO-INDIANA.HUB-WEC.PTBHGB2-Off'
--,'MISO-INDIANA.HUB-2x16'
--,'MISO-INDIANA.HUB-DA-7x8'
--)
--                    , '1% NYH Fuel Oil'
--                    , 'ALGONQUIN'
--                    , 'MISO-MISO_ZPRC_Z7-M'
--                   )
--AND rd.Legged_M2M_VALUE <> 0
and rd.active_flag = 'Y'
order by rd.Location
;

Cursor cur_tenors (
pUnderlying_commodity VARCHAR2
, PpriceVol  VARCHAR2 
) 
is
Select 
tenor.TENOR_ID
,tenor.UNDERLYING_COMMODITY
,tenor.STRIP_NAME
,tenor.START_MONTH
,tenor.END_MONTH
,tenor.ADD_MONTHS
,tenor.PRICE_VOL_INDICATOR
,tenor.AGGREGATION_TYPE
,tenor.BALANCE_FLAG
,tenor.LITERAL_DISPLAY
,tenor.EFFECTIVE_START_DATE
,tenor.EFFECTIVE_END_DATE
,tenor.ACTIVE_FLAG
from 
RISKDB.QP_TENOR_MASTER tenor
where
tenor.underlying_commodity =    punderlying_commodity
and tenor.price_vol_indicator = ppricevol
--and strip_name like 'Balance of Winter'
--IN ( 
--'Planning Year'
--, 'Power Summer (July to August)'
--,'Quarter 2'
--,'Quarter 3'
--,'Quarter 4'
--,'Balance of Planning Year'
--)
and active_flag = 'Y' 
--and Balance_flag = 'N'
and trunc(vd_cobdate) between 
            trunc(Effective_start_date) and  NVL(effective_end_date , vd_cobdate )
order by 
  tenor.underlying_commodity
 , tenor.price_vol_indicator
;

Cursor  cur_contract_years ( 
pLocation     VARCHAR2
, pnettingGroup VARCHAR2
,punderlying_commodity VARCHAR2
, PpriceVol  VARCHAR2
)
IS
SELECT DISTINCT
        strip.CONTRACT_YEAR          
FROM
     TABLE(Monthly_m2m_tab) strip
--RISKDB.QP_INT_FWD_CURVES_RAW_DATA strip
where
 To_Number(strip.CONTRACTYEAR_MONTH) > to_number(vc_CurrentMonth) 
 and strip.UNDERLYING_COMMODITY = pUnderlying_commodity
 and strip.NETTING_GROUP_ID = pnettingGroup
 and strip.PRICE_VOL_INDICATOR  = PpriceVol
 and strip.LOCATION = plocation 
 order by to_number(contract_year)
;  

-- Intialize montly strips collection
Procedure INIT_collection IS
BEGIN
   v_QvarStrips   := strips_collectn_t();
END INIT_collection;

Procedure INIT_m2mtabObj IS
BEGIN
   m2m_tab_obj := qp_strip_tabtype_Obj(); --intialize TYPE Object collection
END INIT_m2mtabObj;



PROCEDURE Get_MAX_MIN_CONTRACTYR ( 
                                  pLocation          IN VARCHAR2 
                                , pnettingGroup      IN NUMBER
                                , ppricevol          IN VARCHAR2  
                                , pmaxYear           OUT NUMBER
                                , pMINYear           OUT NUMBER
                                , ptotalMonths       OUT NUMBER
                                , pout_rtn_code      OUT  NOCOPY NUMBER  
                                , pout_rtn_msg       OUT  NOCOPY VARCHAR2
                               )
IS
 vn_rtn_code                    NUMBER;
 vc_rtn_msg                     VARCHAR2(500);
 vn_maxYear                      PLS_INTEGER;
 vn_minYear                      PLS_INTEGER;
 vn_totalContractMonths          PLS_INTEGER;
BEGIN


IF ppricevol = 'PRICE' THEN 

   SELECT 
   MAX(TO_NUMBER(contractYear_month))
   , MIN(TO_NUMBER(contractYear_month))
   INTO 
    vn_maxYear
   , vn_minYear
   FROM 
   TABLE(Monthly_m2m_tab) m2m
   WHERE
   LOCATION = pLocation 
   AND NETTING_GROUP_ID = pnettingGroup
   and price_vol_indicator = ppricevol
   and ABSOLUTE_LEGGED_M2M_VALUE IS NOT NULL
   ;
   
END IF;

 
IF ppricevol = 'VOL' THEN 

   SELECT 
   MAX(TO_NUMBER(contractYear_month))
   , MIN(TO_NUMBER(contractYear_month))
   INTO 
    vn_maxYear
   , vn_minYear
   FROM 
   TABLE(Monthly_m2m_tab) m2m
   WHERE
   LOCATION = pLocation 
   AND NETTING_GROUP_ID = pnettingGroup
   and price_vol_indicator = ppricevol
   and ABSOLUTE_LEGGED_M2M_VALUE_VOL IS NOT NULL
   ;
   
END IF;
 
 
 --       DBMS_OUTPUT.put_line ('maxYear := ' ||  vn_maxYear||'vn_minYear:='||vn_minYear);

  IF   vn_maxYear IS NOT NULL AND  vn_minYear IS NOT NULL 
  THEN  
                     
     vn_totalContractMonths := 
        MOnths_between( to_date( vn_maxYear||'01', 'YYYYMMDD') 
                       ,  to_date( vn_minYear||'01', 'YYYYMMDD')
                      ) + 1;                    
 ELSE
     vn_totalContractMonths := NULL;
 END IF ;
   
   pmaxYear := vn_maxYear;
   pminYear := vn_minYear;           
   ptotalMonths := vn_totalContractMonths;       
   pout_rtn_code := c_success;
   pout_rtn_msg := ''; --Null means Success
   

EXCEPTION 
WHEN NO_DATA_FOUND THEN 
  pmaxYear  := NULL;
  pminYear :=  NULL;
  ptotalMonths :=NULL;
WHEN OTHERS THEN 
  pmaxYear  := NULL;
  pminYear :=  NULL;
  ptotalMonths :=NULL;
END Get_MAX_MIN_CONTRACTYR;


PROCEDURE Get_LocationRank ( 
                                  pLocation          IN VARCHAR2 
                                , pnettingGroup      IN NUMBER
                                , ppricevol          IN VARCHAR2  
                                , prank              OUT NUMBER
                                , pout_rtn_code      OUT  NOCOPY NUMBER  
                                , pout_rtn_msg       OUT  NOCOPY VARCHAR2
                               )
IS
 vn_rtn_code                        NUMBER;
 vc_rtn_msg                         VARCHAR2(500);
 vn_rank                            PLS_INTEGER;
         V_LOCATION_DELTA_POSITION   RISKDB.QP_STRIP_LVL_NEER_EXT_DATA.LOCATION_DELTA_POSITION%TYPE;
         V_LEGGED_M2M_VALUE          RISKDB.QP_STRIP_LVL_NEER_EXT_DATA.LEGGED_M2M_VALUE%TYPE;
         V_LOCATION                  RISKDB.QP_STRIP_LVL_NEER_EXT_DATA.neer_LOCATION%TYPE;
         V_NETTING_GROUP_ID          RISKDB.QP_INT_FWD_CURVES_RAW_DATA.NETTING_GROUP_ID%TYPE;
BEGIN

         SELECT LOCATION,
                  NETTING_GROUP_ID,
                  MAX (INITIAL_RANK),
                  SUM (LOCATION_DELTA_POSITION),
                  SUM (LEGGED_M2M_VALUE)
             INTO V_LOCATION,
                  V_NETTING_GROUP_ID,
                  vn_rank,
                  V_LOCATION_DELTA_POSITION,
                  V_LEGGED_M2M_VALUE
             FROM TABLE (Monthly_m2m_tab) m2m
            WHERE     LOCATION = pLocation
                  AND NETTING_GROUP_ID = pnettingGroup
                  AND price_vol_indicator = ppricevol
         GROUP BY LOCATION, NETTING_GROUP_ID;
   
 

 IF   vn_rank IS NOT NULL AND  vn_rank > 0 THEN  
  
    prank := vn_rank ;                    
 ELSIF 
               -- Rank 1 if position = 0 and m2m <> 0     -- FOr a Location Level
               -- or position <> 0 and m2m = 0 --sharana 11/30/2015
               (V_LOCATION_DELTA_POSITION = 0 AND V_LEGGED_M2M_VALUE <> 0) OR
               (V_LOCATION_DELTA_POSITION <> 0 AND V_LEGGED_M2M_VALUE = 0)
 THEN
            prank := 1;
  --          DBMS_OUTPUT.put_line ('Curve_Rank := ' || prank);
 ELSE
     prank := NULL;
     
 END IF ;

   pout_rtn_code := c_success;
   pout_rtn_msg := ''; --Null means Success
   

EXCEPTION 
WHEN NO_DATA_FOUND THEN 
   prank := NULL;
WHEN OTHERS THEN 
   prank := NULL;
END Get_LocationRank;


      
PROCEDURE Get_FlatPosition ( 
                                  pLocation          IN VARCHAR2 
                                , pnettingGroup      IN NUMBER
                                , ppricevol          IN VARCHAR2  
                                , pflatpos           OUT VARCHAR2
                                , pout_rtn_code      OUT  NOCOPY NUMBER  
                                , pout_rtn_msg       OUT  NOCOPY VARCHAR2
                               )
IS
 vn_rtn_code                        NUMBER;
 vc_rtn_msg                         VARCHAR2(500);
 vc_flat_position                   VARCHAR2(1);

BEGIN

    BEGIN
       
    Select  DISTINCT
        ( select 
            CASE WHEN COUNT ( DISTINCT ( Location_delta_position/( case 
                                           when Underlying_commodity = 'ELECTRICITY' 
                                                then TOT_HOURS_PER_CONTRACT_MONTH
                                           When Underlying_commodity IN ( 'NATURAL GAS','GAS', 'NG 6 MOAVG')
                                                Then CAL_DAYS_IN_CONTRACT_MONTH
                                           else
                                                1
                                          end )
                                    )  
                         ) = 1 THEN 'Y' ELSE 'N' END 
             FROM   
              TABLE(Monthly_m2m_tab)  v
             where
             v.NETTING_GROUP_ID = m2m.Netting_group_id
             and v.COMMODITY = m2m.commodity
             and v.price_vol_indicator = m2m.price_vol_indicator
             and v.LOCATION = m2m.Location
             --and CONTRACTYEAR_MONTH  = m2m.contractYear_month
          )
        into
        --pcollection(pIndex).FLAT_POSITION_FLAG
        vc_flat_position  
        from  TABLE(Monthly_m2m_tab)   m2m
        where  
            m2m.LOCATION =    pLocation
         and m2m.netting_group_id = pNettingGroup   
         and m2m.price_vol_indicator =   ppricevol
         and to_number(m2m.contractYear_month) > To_number(vc_CurrentMonth)
        ;


    EXCEPTION
    WHEN NO_DATA_FOUND THEN
     vc_flat_position := 'N';
    WHEN TOO_MANY_ROWS THEN 
      vc_flat_position := 'N'; 
    WHEN OTHERS THEN 
     vc_flat_position := 'N'; 
    END;


   pout_rtn_code := c_success;
   pout_rtn_msg := ''; --Null means Success
   pflatpos := vc_flat_position ;
   
END Get_FlatPosition;
   
PROCEDURE Get_Basis_indicator     (pLocation           IN         VARCHAR2,
                                   pnettingGroup       IN         NUMBER,
                                   ppricevol           IN         VARCHAR2,
                                   pbasis_indicator    OUT        VARCHAR2,
                                   pout_rtn_code       OUT NOCOPY NUMBER,
                                   pout_rtn_msg        OUT NOCOPY VARCHAR2
                                  )
IS
 vn_rtn_code       NUMBER;
 vc_rtn_msg        VARCHAR2 (500);
 v_count           NUMBER;
 v_basis_indicator   RISKDB.QP_INT_FWD_CURVES_RAW_DATA.BASIS_CURVE_INDICATOR%TYPE;
--         V_LOCATION_DELTA_POSITION   RISKDB.QP_STRIP_LVL_NEER_EXT_DATA.LOCATION_DELTA_POSITION%TYPE;
--         V_LEGGED_M2M_VALUE          RISKDB.QP_STRIP_LVL_NEER_EXT_DATA.LEGGED_M2M_VALUE%TYPE;
--         V_LOCATION                  RISKDB.QP_STRIP_LVL_NEER_EXT_DATA.neer_LOCATION%TYPE;
--         V_NETTING_GROUP_ID          RISKDB.QP_INT_FWD_CURVES_RAW_DATA.NETTING_GROUP_ID%TYPE;
BEGIN

SELECT COUNT (BASIS_CURVE_INDICATOR)
  INTO v_count
  FROM QP_INT_FWD_Curves_raw_data
 WHERE     
 COB_DATE = vd_cobdate
 AND LOCATION = pLocation
 AND NETTING_GROUP_ID = pnettingGroup
 AND price_vol_indicator = ppricevol 
 AND ACTIVE_FLAG = 'Y'
 AND basis_curve_indicator = 'Y';
 
    IF v_count > 0 THEN
     v_basis_indicator:=  'Y';
     ELSE
     v_basis_indicator :=  'N';
     END IF;

     pbasis_indicator := v_basis_indicator;
     pout_rtn_code := c_success;
     pout_rtn_msg := '';                              --Null means Success
EXCEPTION
 WHEN NO_DATA_FOUND
 THEN
    pbasis_indicator := 'N';
 WHEN OTHERS
 THEN
    pbasis_indicator := NULL;
END Get_Basis_indicator ;

PROCEDURE Derive_final_ranking (  pcobdate               IN DATE
                                , pUnderlyingCommodity  IN VARCHAR2
                                , ppriceVOlind          IN VARCHAR2
                                ,  pgranularm2m         IN NUMBER 
                                , pnongranularm2m       IN NUMBER
                                , pbasism2m             IN NUMBER
                                , pflatpos              IN VARCHAR2 
                                , prank              OUT NUMBER
                                , pout_rtn_code      OUT  NOCOPY NUMBER  
                                , pout_rtn_msg       OUT  NOCOPY VARCHAR2
                               )
IS

 vn_rtn_code                        NUMBER;
 vc_rtn_msg                         VARCHAR2(500);
 vn_rank                            PLS_INTEGER;
 vn_condition                       VARCHAR2(10);
 vn_m2mThreshold                    NUMBER;
 

 
BEGIN 

        --Get m2m Threshold 
        BEGIN 

        Select 
        MIN_M2M_PRCNT_THRESHOLD
        , MIN_M2M_PRCNT_VALUE
        INTO 
        vn_condition
        , vn_m2mthreshold
        FROM 
        RISKDB.QP_CURVE_VALDTN_M2M_THRESHOLD threshold
        where
        UNDERLYING_COMMODITY = pUnderlyingCommodity
        and CURVE_TYPE = ppriceVOlind
        and ACTIVE_FLAG = 'Y'
        and trunc(pcobdate) between EFFECTIVE_START_DATE and NVL(effective_end_date , pcobdate)
        ;

        vn_m2mthreshold := vn_m2mthreshold/100; -- convert percent value to Decimal ratio



        EXCEPTION
        WHEN NO_DATA_FOUND THEN 
        vn_condition :=  NULL ;
         vn_m2mthreshold := NULL;
        WHEN TOO_MANY_ROWS THEN 
         
         vn_condition :=  '>' ;
         vn_m2mthreshold := 1000; --set to high so someone alerts about failures
        WHEN OTHERS THEN 
         
         vn_condition :=  '>' ;
         vn_m2mthreshold := 1000;  --set to high so someone alerts about failures
        END;
        --end of Get m2m Threshold Block 


-- NOw check which relation Operator to apply
CASE 
WHEN vn_condition IS NULL THEN 

      vn_rank:= 4;

WHEN vn_condition = '=' THEN   


  CASE 
  WHEN pgranularm2m = vn_m2mthreshold THEN 
      vn_rank:= 2;
  WHEN pbasism2m = vn_m2mthreshold THEN 
      CASE
       WHEN pflatpos = 'Y' THEN 
         vn_rank:= 2;
      ELSE
           vn_rank := 3;
      END CASE;
      
          
  ELSE
     vn_rank:= 4;
  END CASE;
  

WHEN vn_condition = '>' THEN

  CASE 
  WHEN pgranularm2m > vn_m2mthreshold THEN 
      vn_rank:= 2;
  WHEN pbasism2m > vn_m2mthreshold THEN 
      CASE
       WHEN pflatpos = 'Y' THEN 
         vn_rank:= 2;
      ELSE
           vn_rank := 3;
      END CASE;
      
          
  ELSE
     vn_rank:= 4;
  END CASE;
  
WHEN vn_condition = '>=' THEN

  CASE 
  WHEN pgranularm2m >= vn_m2mthreshold THEN 
      vn_rank:= 2;
  WHEN pbasism2m >= vn_m2mthreshold THEN 
      CASE
       WHEN pflatpos = 'Y' THEN 
         vn_rank:= 2;
      ELSE
           vn_rank := 3;
      END CASE;
      
          
  ELSE
     vn_rank:= 4;
  END CASE;
  
WHEN vn_condition = '<' THEN

  CASE 
  WHEN pgranularm2m < vn_m2mthreshold THEN 
      vn_rank:= 2;
  WHEN pbasism2m < vn_m2mthreshold THEN 
      CASE
       WHEN pflatpos = 'Y' THEN 
         vn_rank:= 2;
      ELSE
           vn_rank := 3;
      END CASE;
      
          
  ELSE
     vn_rank:= 4;
  END CASE;
  
WHEN vn_condition = '<=' THEN

  CASE 
  WHEN pgranularm2m <= vn_m2mthreshold THEN 
      vn_rank:= 2;
  WHEN pbasism2m <= vn_m2mthreshold THEN 
      CASE
       WHEN pflatpos = 'Y' THEN 
         vn_rank:= 2;
      ELSE
           vn_rank := 3;
      END CASE;
      
          
  ELSE
     vn_rank:= 4;
  END CASE;
  
 
WHEN vn_condition = '<>' THEN

  CASE 
  WHEN pgranularm2m <> vn_m2mthreshold THEN 
      vn_rank:= 2;
  WHEN pbasism2m <> vn_m2mthreshold THEN 
      CASE
       WHEN pflatpos = 'Y' THEN 
         vn_rank:= 2;
      ELSE
           vn_rank := 3;
      END CASE;
      
          
  ELSE
     vn_rank:= 4;
  END CASE;
  
Else
  null; -- do nothing 
END CASE;

   prank := vn_rank;
   
   pout_rtn_code := c_success;
   pout_rtn_msg := ''; --Null means Success
   

EXCEPTION 
WHEN NO_DATA_FOUND THEN 
   prank := NULL;
WHEN OTHERS THEN 
   prank := NULL;
END Derive_final_ranking;


BEGIN

   IF NVL(g_minlogid , 0) = 0 THEN
   
    --stage 2 not involked , so find out minimum number logged for this stage 
    SELECT MAX(l.log_id) 
    INTO g_minlogid
    FROM 
    DMR.PROCESS_LOG l
    WHERE
    application_Name = 'QVAR_APP'
    and process_name = 'BUILD_STRIPS' 
    and parent_name = 'QP_ZEMA_EXTRACT_PKG'
    ;

   END IF ;
   

 
    IF pcobdate is NULL THEN 

     Get_current_COBDATE (
      pcobdate =>   vd_cobdate  
     ,pout_rtn_code     => vn_rtn_code
     ,pout_rtn_msg      => vc_rtn_msg 
     );
     
     vc_cobdate       := to_CHAR( vd_cobdate, 'DD-MON-YYYY');
     vd_cobdate       := to_date( vc_cobdate, 'DD-MON-YYYY');
     
    ELSE


    vc_cobdate       := to_CHAR(pcobdate, 'DD-MON-YYYY');
    vd_cobdate       := to_date(vc_cobdate, 'DD-MON-YYYY');

    --vd_default_effective_Date := vd_cobdate;

    END IF;


    vc_CurrentMonth    := to_CHAR(vd_cobdate, 'YYYYMM');
    vc_promptMonth     := TO_CHAR(ADD_MONTHS(vd_cobdate,1), 'YYYYMM');

     --Set application Name for the Log Process
    DMR.PROCESS_LOG_PKG.CONSTRUCTOR ('QVAR_APP' );
    
     --Set Process Name and status for the Log Process                   
    PROCESS_LOG_REC.PROCESS_NAME :=  'BUILD_STRIPS';
    PROCESS_LOG_REC.PARENT_NAME  := 'QP_ZEMA_EXTRACT_PKG';
    PROCESS_LOG_REC.STAGE        := 'BUILD_STRIPS';
    PROCESS_LOG_REC.STATUS       := PROCESS_LOG_PKG.C_STATUS_STARTED;
    PROCESS_LOG_REC.MESSAGE      := 'Build_Strips process Started for cobdate = '|| vc_cobdate;
    
    DMR.PROCESS_LOG_PKG.SET_PROCESS_NAME(
    P_PROCESS_NAME =>PROCESS_LOG_REC.PROCESS_NAME
    , P_PARENT_NAME => PROCESS_LOG_REC.PARENT_NAME
    );

   -- Log Message first
   write_log;
   
   
   INIT_collection; --intialize target collection
   
   for Location In Cur_location LOOP
   
       vc_originalLocation :=   Location.LOCATION;
       vn_NettingGroup :=   Location.netting_group_id;
       vc_commodity    :=   Location.Commodity;
       vc_uc_commodity :=   Location.UNDERLYING_COMMODITY;
       vc_pricevol     :=   Location.PRICE_VOL_INDICATOR;

       v_denom := 0; 
       v_granular_m2mPercent   := 0;

       v_nongranular_m2mPercent   := 0;

       v_m2mbasis_m2mPercent    := 0;
       vn_LocationRank := NULL;         --reset for every single location          
       v_loop_flag := FALSE ;           --reset flag for balance of planning year calculation 
                
        --  reset the montly location set of values to NULL Value 
        Monthly_m2m_tab := Null_m2m_tab;
            
        INIT_m2mtabObj; -- reset m2m% caculation data structure ( collection)
        
        --capture  starting index of total strips  available for a last Location
        vn_Objects_START_indx  := v_QvarStrips.COUNT;
        
--        Dbms_output.Put_line ('start indx'||vn_Objects_START_indx );
        
        Select count(*) into vn_recCount
        from 
        TABLE(Monthly_m2m_tab)
        ;


    -- get backbone of Original Location 
      Get_backbone_flag ( PUNDERLYING_COMMODITY => Location.Underlying_commodity
                          , pLocation          => vc_originalLocation
                          , pbackbone_flag     => vc_backbone_flag
                          , pout_rtn_code      => vn_rtn_code  
                          , pout_rtn_msg       => vc_rtn_msg
                          );
                          


    --- Find out do we have override Location 
   BEGIN 
    
    SELECT 
    OVERRIDE_COMMODITY 
    , OVERRIDE_BASIS_POINT 
    , BASIS_POINT_OVERRIDE_ID
     INTO vc_OverCommodity , vc_overrideLocation , v_overrideID
     FROM
      RISKDB.QP_BASIS_POINT_OVERRIDE m 
     WHERE
     m.Basis_point = vc_originallocation
     and m.commodity = Location.Commodity
     and m.price_vol_Indicator = Location.Price_vol_indicator
     and m.active_flag = 'Y'
     and pcobdate between EFFECTIVE_START_DATE and NVL(EFFECTIVE_END_DATE , pCOBDATE)
     ;
 
      
   EXCEPTION
   WHEN NO_DATA_FOUND THEN 
    vc_overCOmmodity := NULL;
    vc_overrideLocation := NULL ;
    v_overrideID := NULL;
    
   WHEN OTHERS THEN 
     vc_overCommodity := NULL;
     vc_overrideLocation := NULL ;
     v_overrideID := NULL;
   END;
   
   
    -- get backbone of Original Location 
    IF  vc_overrideLocation IS NOT NULL THEN 
      Get_backbone_flag ( PUNDERLYING_COMMODITY => Location.Underlying_commodity
                          , pLocation          => vc_overrideLocation
                          , pbackbone_flag     => vc_Override_bb_flag
                          , pout_rtn_code      => vn_rtn_code  
                          , pout_rtn_msg       => vc_rtn_msg
                          );
    END IF;
                            
--   DBMS_OUTPUT.PUT_LINE( 'Location = '||vc_originalLocation
--                    ||'OverLoc='||vc_overrideLocation
--                    ||'NettingGroup='||Location.Netting_Group_id
--                    ||'Pricevol='||Location.Price_vol_indicator
--                    ||' orig BB flag='||vc_backbone_flag
--                    ||' override BB flag='||vc_override_bb_flag
--                   ); 

  
                        
      
           /*************************************************************************
               Get All forward Monthly level Data for the given COBDATE 
           *************************************************************************/                           
  
             Construct_Locm2m_object(
                     pcobdate                      => Location.COB_DATE
                    , pNettingGroup                => Location.Netting_Group_id
                    , pUnderlyingCommodity         => Location.Underlying_commodity
                    , pCommodity                   => Location.COMMODITY
                    , pOverridelocation            => vc_overrideLocation
                    , POriginalLocation            => vc_originalLocation
                    , ppricevol                    => Location.price_vol_Indicator 
                    , pLocbbflag                   => vc_backbone_flag
                    , poverridebbflag              => vc_override_bb_flag                     
                    , pmonthlym2m                  => Monthly_m2m_tab  
                    , pout_rtn_code     => vn_rtn_code 
                    , pout_rtn_msg      =>  vc_rtn_msg
                    ) ;
 
  
            Select count(*) into vn_recCount
            from 
            TABLE(Monthly_m2m_tab)
            ;

            Dbms_output.Put_line (vc_Location||'-total_recs->'||to_CHAR(vn_reccount));
  

    
 --- For Location and Netting group compute MAX /MIN/ an total Contract MOnths 
    Get_MAX_MIN_CONTRACTYR ( 
                              pLocation          => vc_OriginalLocation 
                            , pnettingGroup      => Location.Netting_Group_id
                            , ppricevol          => Location.price_vol_Indicator  
                            , pmaxYear           => vn_MAXYear
                            , pMINYear           => vn_minYear
                            , ptotalMonths       => vn_totalMonths
                            , pout_rtn_code      => vn_rtn_code  
                            , pout_rtn_msg       => vc_rtn_msg
                           );
          
 
--    DBMS_output.Put_line('max contrYR='||vn_MAXYear||'; minYr='||vn_minYear);                     

--- Get Location level Ranking if any     
     Get_LocationRank ( 
                              pLocation          => vc_originalLocation 
                            , pnettingGroup      => Location.Netting_Group_id
                            , ppricevol          => Location.price_vol_Indicator
                            , prank              => vn_LocationRank
                            , pout_rtn_code      => vn_rtn_code  
                            , pout_rtn_msg       => vc_rtn_msg
                           );

                          
--- Check is Location a Flat position 
         Get_FlatPosition ( 
                            pLocation            => vc_originalLocation 
                            , pnettingGroup      => Location.Netting_Group_id
                            , ppricevol          => Location.price_vol_Indicator 
                            , pflatpos           => vc_flat_position
                            , pout_rtn_code      => vn_rtn_code  
                            , pout_rtn_msg       => vc_rtn_msg
                               );

--         DBMS_output.Put_line('Flat position='||vc_flat_position );

      Get_Basis_indicator     (pLocation           => NVL(vc_OverrideLocation , Vc_OriginalLocation)
                                   ,pnettingGroup       => Location.Netting_Group_id
                                   ,ppricevol           => Location.price_vol_Indicator 
                                   ,pbasis_indicator    => v_basis_indicator
                                   ,pout_rtn_code       => vn_rtn_code  
                                   ,pout_rtn_msg        => vc_rtn_msg
                                  );
       
                                                           
      for tenor in cur_tenors ( Location.UNDERLYING_COMMODITY
                                , Location.PRICE_VOL_INDICATOR
                              )
      loop
        --- derive date range
       
      --reset all Local Variables 
       vc_stripName := Tenor.strip_Name;
       
       
       vc_Monthly_flag := 'N';
       vc_Balanced_flag     := 'N';
       vc_NonBalanced_flag     := 'N'; 
       
       
       vc_StartAYYYYMM :=  NULL;
       vc_endAYYYYMM := NULL; 
        
       
--         DBMS_OUTPUT.PUT_LINE ( Tenor.strip_Name          
--                               ||'Start_month= '||Tenor.Start_Month
--                               ||',endMOnth =  '||Tenor.End_month
--                               ||', Add_months = '||Tenor.ADD_MONTHS
--                               ||', '||Tenor.Balance_flag
--                             );
--                              
--       For contract_yr in cur_contract_years(
--                            Location.LOcation
--                            , Location.Netting_Group_id
--                            , Location.UNDERLYING_COMMODITY
--                            , Location.PRICE_VOL_INDICATOR
--       )  


       For contract_yr in TO_NUMBER(SUBSTR(vc_CurrentMonth,1,4)) ..  TO_NUMBER(SUBSTR(vn_maxYear,1,4))
  
       LOOP
        

       
        vc_contractyear := to_char(Contract_Yr);
          
        CASE 
        WHEN tenor.start_month IS NOT NULL 
           AND tenor.END_MONTH IS NOT NULL 
           and tenor.Balance_flag = 'N' 
           AND Tenor.start_MOnth = Tenor.END_MONTH then 
         
          --dealing with Monthly strips 
         vc_Monthly_flag := 'Y';

         vc_StartAYYYYMM := REPLACE(tenor.start_Month,'AYYYYF', vc_contractyear );
                         
         
         vc_endAYYYYMM := vc_StartAYYYYMM;
        
        
          IF to_Number(vc_StartAYYYYMM) >= to_Number(vc_promptMonth) THEN 
             null; -- we have a valid monthly strip
          ELSE
             CONTINUE; -- go to next contract year strip
          END IF;
          


         /*********************BALANCE STRIPS CHECK ********/ 
                                             
        WHEN tenor.Balance_flag = 'Y' and (   
           ( INSTR(Upper(tenor.Start_month), 'MAX(CYYYYF',1,1) > 0 )
           AND ( INSTR(Upper(tenor.END_month), 'CYYYYF',1,1) > 0 )
          ) 
        THEN 

          vc_Balanced_flag     := 'Y';
        
         IF vc_contractyear = to_CHAR(vd_cobdate, 'YYYY') THEN 
        
                  parse_strip (
                        PCOBDATE         => vd_cobdate
                       , ppromptMonth    => vc_promptMonth
                       , pstartFormula   => Upper(tenor.Start_month)
                       , pendFormula     => Upper(tenor.END_month)
                       , pAddMonths      => tenor.ADD_MONTHS
                       , pcontractYear   => vc_contractyear
                       , pstartYRPattern => 'CYYYYF'
                       , pendYRPattern   => 'CYYYYF'
                       , pstartValue     => vc_StartAYYYYMM                       
                       , pendValue       => vc_endAYYYYMM   
                       , pout_rtn_code   => vn_rtn_code  
                       , pout_rtn_msg    => vc_rtn_msg 
                      );
          

  
         ELSE
             CONTINUE; -- go for next contract year                      
         END IF;
         -- End of IF vc_contractyear = to_CHAR(pcobdate, 'YYYY') THEN 
         
           --       WHEN Start MOnths and ADD_MONTHS  of Balance strips are given and 
           --  END_MONTH IS NULL 
           
        WHEN tenor.Balance_flag = 'Y' and    
           INSTR(Upper(tenor.Start_month), 'MAX(CYYYYF',1,1) > 0 
           AND  trim(tenor.END_month) IS NULL 
           AND  trim(tenor.ADD_MONTHS) IS NOT NULL    
           
        THEN 
          vc_Balanced_flag     := 'Y';
           IF vc_contractyear = to_CHAR(vd_cobdate, 'YYYY') THEN 
        
                 parse_strip (
                        PCOBDATE         => vd_cobdate
                       , ppromptMonth    => vc_promptMonth
                       , pstartFormula   => Upper(tenor.Start_month)
                       , pendFormula     => Upper(tenor.END_month)
                       , pAddMonths      => tenor.ADD_MONTHS
                       , pcontractYear   => vc_contractyear
                       , pstartYRPattern => 'CYYYYF'
                       , pendYRPattern   => 'CYYYYF'
                       , pstartValue     => vc_StartAYYYYMM                       
                       , pendValue       => vc_endAYYYYMM   
                       , pout_rtn_code   => vn_rtn_code  
                       , pout_rtn_msg    => vc_rtn_msg 
                      );
          


         ELSE
             CONTINUE; -- go for next contract year                      
         END IF;
         -- End of IF vc_contractyear = to_CHAR(pcobdate, 'YYYY') THEN 
          /********BAL OF YR AND BAL OF WINTER CALCULATION  ********/
 
 
        WHEN tenor.Balance_flag = 'Y' 
             AND INSTR (UPPER (tenor.end_month), 'N', 1, 1) > 0 
        THEN
        
        
         IF vc_contractyear = to_CHAR(vd_cobdate, 'YYYY') THEN 
         
          v_b_char             := SUBSTR (tenor.END_MONTH, 1, 1);
 
          IF v_b_char = 'N' AND NOT v_loop_flag THEN
            
            vc_Balanced_flag := 'Y';

            v_b_digits := TRIM (SUBSTR (tenor.END_MONTH, 2));

            v_cob_date_yr := SUBSTR (vc_CurrentMonth, 1, 4);

            IF TO_NUMBER (vc_CurrentMonth) < TO_NUMBER (v_cob_date_yr) || v_b_digits THEN
              vc_endAYYYYMM               := v_cob_date_yr || v_b_digits;

            ELSE
              vc_endAYYYYMM := TO_CHAR (TO_NUMBER (v_cob_date_yr) + 1) || v_b_digits;
            END IF;
 
           vc_StartAYYYYMM    := TO_CHAR ( ADD_MONTHS ( TO_DATE (vc_endAYYYYMM || '01', 'YYYYMMDD'), tenor.ADD_MONTHS ), 'YYYYMM');
             IF to_number(vc_promptmonth) > to_number(vc_StartAYYYYMM)
           THEN 
           vc_StartAYYYYMM :=vc_promptmonth;
           ELSE
           NULL ;      --DO NOTHING
           END IF;

              
            IF vc_StartAYYYYMM IS NOT NULL AND vc_endAYYYYMM IS NOT NULL THEN
              v_loop_flag      := TRUE;
            END IF;
            
          END IF;
        
        ELSE
          CONTINUE; 
        END IF;
        
        
          --- end of 'N' check
        /*********************NON BALANCE STRIPS CHECK ********/ 
        WHEN  tenor.Balance_flag = 'N' and 
              INSTR(Upper(Tenor.Start_month), 'AYYYYF',1,1) > 0 
              and INSTR(Upper(Tenor.End_month), 'AYYYYF',1,1) > 0 
              and trim(tenor.ADD_MONTHS) IS NULL 
         THEN
         
           vc_NonBalanced_flag     := 'Y';
                   
           VC_STARTAYYYYMM := REPLACE (Upper(Tenor.Start_month), 'AYYYYF', vc_contractyear );
           VC_ENDAYYYYMM := REPLACE (Upper(Tenor.End_month), 'AYYYYF', vc_contractyear );
           
           -- means strip not yet started as of the Given cobdate 
           -- and starting month is either equal to Prompt m0nth or greater 
           -- then we have valid strip 
           
          IF   TO_NUMBER( VC_STARTAYYYYMM ) > to_Number(vc_CurrentMonth) 
               AND ( TO_NUMBER( VC_STARTAYYYYMM ) >= to_Number(vc_promptMonth) )THEN 
           
             -- means strip not yet started 
              VC_STARTAYYYYMM := REPLACE (Upper(Tenor.Start_month), 'AYYYYF', vc_contractyear );
              VC_ENDAYYYYMM := REPLACE (Upper(Tenor.End_month), 'AYYYYF', vc_contractyear);
                     

                     
          ELSE
              

              CONTINUE; -- go for next conract year      
                    
          END IF; 
        
        WHEN tenor.Balance_flag = 'N' and 
            INSTR(Upper(tenor.Start_month), 'AYYYYF',1,1) > 0 
            AND Tenor.END_month IS NULL 
            AND Tenor.ADD_MONTHS IS NOT NULL THEN 
      
         vc_NonBalanced_flag     := 'Y';

        VC_STARTAYYYYMM := REPLACE (Upper(Tenor.Start_month), 'AYYYYF', vc_contractyear );
                      
         IF   TO_NUMBER( VC_STARTAYYYYMM ) > to_Number(vc_CurrentMonth) 
               AND ( TO_NUMBER( VC_STARTAYYYYMM ) >= to_Number(vc_promptMonth) )THEN 
           
             -- means strip not yet started 
              VC_STARTAYYYYMM := REPLACE (Upper(Tenor.Start_month), 'AYYYYF', vc_contractyear );
                                
              VC_ENDAYYYYMM := TO_CHAR(ADD_MONTHS(TO_DATE(VC_STARTAYYYYMM||'01', 'YYYYMMDD') 
                                            ,  Tenor.ADD_MONTHS 
                                            )
                                         , 'YYYYMM'
                                      ) ;
                  
         ELSE
                  ---   strip starting time could be between this year or prompt Year
                                
                CONTINUE ;
                
         END IF;           
        
        WHEN  INSTR(Upper(Tenor.Start_month), 'Q1YYYYF',1,1) > 0 THEN
      
          vc_NonBalanced_flag     := 'Y';
          
         VC_STARTAYYYYMM := REPLACE (Upper(Tenor.Start_month), 'Q1YYYYF', vc_contractyear);
         VC_ENDAYYYYMM :=   REPLACE (Upper(Tenor.End_month), 'Q1YYYYF', vc_contractyear);
      

           
         vc_Quarterstart := 'Q'||TO_CHAR(TO_DATE(REPLACE ( Upper(Tenor.Start_month)||'01', 'Q1YYYYF',  vc_contractyear), 'YYYYMMDD'),'Q');
         vc_Quarterend   := 'Q'||TO_CHAR(TO_DATE(REPLACE ( Upper(Tenor.End_month)||'01', 'Q1YYYYF',  vc_contractyear), 'YYYYMMDD'),'Q');
           
                
           -- as of the Given cobdate 
           -- Both start and end tenor Months Must derive as Q1 
           
          IF   vc_Quarterstart = 'Q1' AND vc_Quarterend = 'Q1' THEN 
          

               -- Q1 Must not be started as of given cobdate 
               
               IF  TO_NUMBER( VC_STARTAYYYYMM ) > to_Number(vc_CurrentMonth) 
                   AND ( TO_NUMBER( VC_STARTAYYYYMM ) >= to_Number(vc_promptMonth) 
                         AND TO_NUMBER( VC_STARTAYYYYMM ) < TO_NUMBER( VC_ENDAYYYYMM ) )THEN 
                 

                     -- means we have a valid strip
                     null; 
               ELSE
      
                CONTINUE; --no partial strips if startmonth <= current Month or prompt Month
               END IF;
                      
          ELSE
          
   
              CONTINUE; -- go for next conract year  , if currentMonth is >= startMonth    
                    
          END IF;
        
           
        WHEN  INSTR(Upper(Tenor.Start_month), 'Q2YYYYF',1,1) > 0 THEN
      
         vc_NonBalanced_flag     := 'Y';
         
         VC_STARTAYYYYMM := REPLACE (Upper(Tenor.Start_month), 'Q2YYYYF', vc_contractyear);
         VC_ENDAYYYYMM :=   REPLACE (Upper(Tenor.End_month), 'Q2YYYYF', vc_contractyear);
      

           
         vc_Quarterstart := 'Q'||TO_CHAR(TO_DATE(REPLACE ( Upper(Tenor.Start_month)||'01', 'Q2YYYYF',  vc_contractyear), 'YYYYMMDD'),'Q');
         vc_Quarterend   := 'Q'||TO_CHAR(TO_DATE(REPLACE ( Upper(Tenor.End_month)||'01', 'Q2YYYYF',  vc_contractyear), 'YYYYMMDD'),'Q');
           
                
           -- as of the Given cobdate 
           -- Both start and end tenor Months Must derive as Q1 
           
          IF   vc_Quarterstart = 'Q2' AND vc_Quarterend = 'Q2' THEN 
          
             -- dbms_output.Put_line('Q2-'||VC_STARTAYYYYMM||'-'||VC_EndAYYYYMM);
               -- Q1 Must not be started as of given cobdate 
               
               IF  TO_NUMBER( VC_STARTAYYYYMM ) > to_Number(vc_CurrentMonth) 
                   AND ( TO_NUMBER( VC_STARTAYYYYMM ) >= to_Number(vc_promptMonth) 
                         AND TO_NUMBER( VC_STARTAYYYYMM ) < TO_NUMBER( VC_ENDAYYYYMM ) )THEN 
                 

                     -- means we have a valid strip
                     null; 
               ELSE
    
                CONTINUE; --no partial strips if startmonth <= current Month or prompt Month
               END IF;
                      
          ELSE
          
  
              CONTINUE; -- go for next conract year  , if currentMonth is >= startMonth    
                    
          END IF;       
          
        WHEN  INSTR(Upper(Tenor.Start_month), 'Q3YYYYF',1,1) > 0 THEN
      
         vc_NonBalanced_flag     := 'Y';
         
         VC_STARTAYYYYMM := REPLACE (Upper(Tenor.Start_month), 'Q3YYYYF', vc_contractyear);
         VC_ENDAYYYYMM :=   REPLACE (Upper(Tenor.End_month), 'Q3YYYYF', vc_contractyear);
      

           
         vc_Quarterstart := 'Q'||TO_CHAR(TO_DATE(REPLACE ( Upper(Tenor.Start_month)||'01', 'Q3YYYYF',  vc_contractyear), 'YYYYMMDD'),'Q');
         vc_Quarterend   := 'Q'||TO_CHAR(TO_DATE(REPLACE ( Upper(Tenor.End_month)||'01', 'Q3YYYYF',  vc_contractyear), 'YYYYMMDD'),'Q');
           
                
           -- as of the Given cobdate 
           -- Both start and end tenor Months Must derive as Q1 
           
          IF   vc_Quarterstart = 'Q3' AND vc_Quarterend = 'Q3' THEN 
          

               -- Q1 Must not be started as of given cobdate 
               
               IF  TO_NUMBER( VC_STARTAYYYYMM ) > to_Number(vc_CurrentMonth) 
                   AND ( TO_NUMBER( VC_STARTAYYYYMM ) >= to_Number(vc_promptMonth) 
                         AND TO_NUMBER( VC_STARTAYYYYMM ) < TO_NUMBER( VC_ENDAYYYYMM ) )THEN 
                 

                     -- means we have a valid strip
                     null; 
               ELSE
--                --dbms_output.put_line('else Q1 continue ');      
                CONTINUE; --no partial strips if startmonth <= current Month or prompt Month
               END IF;
                      
          ELSE
          
  
              CONTINUE; -- go for next conract year  , if currentMonth is >= startMonth    
                    
          END IF;             
          
        WHEN  INSTR(Upper(Tenor.Start_month), 'Q4YYYYF',1,1) > 0 THEN
      
          vc_NonBalanced_flag     := 'Y';
          
         VC_STARTAYYYYMM := REPLACE (Upper(Tenor.Start_month), 'Q4YYYYF', vc_contractyear);
         VC_ENDAYYYYMM :=   REPLACE (Upper(Tenor.End_month), 'Q4YYYYF', vc_contractyear);
      

           
         vc_Quarterstart := 'Q'||TO_CHAR(TO_DATE(REPLACE ( Upper(Tenor.Start_month)||'01', 'Q4YYYYF',  vc_contractyear), 'YYYYMMDD'),'Q');
         vc_Quarterend   := 'Q'||TO_CHAR(TO_DATE(REPLACE ( Upper(Tenor.End_month)||'01', 'Q4YYYYF',  vc_contractyear), 'YYYYMMDD'),'Q');
           
                
           -- as of the Given cobdate 
           -- Both start and end tenor Months Must derive as Q1 
           
          IF   vc_Quarterstart = 'Q4' AND vc_Quarterend = 'Q4' THEN 
          

               -- Q1 Must not be started as of given cobdate 
               
               IF  TO_NUMBER( VC_STARTAYYYYMM ) > to_Number(vc_CurrentMonth) 
                   AND ( TO_NUMBER( VC_STARTAYYYYMM ) >= to_Number(vc_promptMonth) 
                         AND TO_NUMBER( VC_STARTAYYYYMM ) < TO_NUMBER( VC_ENDAYYYYMM ) )THEN 
                 

                     -- means we have a valid strip
                     null; 
               ELSE
      
                CONTINUE; --no partial strips if startmonth <= current Month or prompt Month
               END IF;
                      
          ELSE
          
  
              CONTINUE; -- go for next conract year  , if currentMonth is >= startMonth    
                    
          END IF;
                    
        ELSE /* END CASE ELSE */
          
         NULL; -- DO nothing  
             /************/
        END CASE;
        -- end case for  tenor strips 
        
 
   

        IF    TO_NUMBER(VC_EndAYYYYMM) <  TO_NUMBER(VC_STARTAYYYYMM) THEN
          CONTINUE; -- go to next contract Year 
        END IF;

  --      Validate do we have a valid internal Strip ?
      
--        dbms_output.Put_line('Tenor start='||VC_STARTAYYYYMM||'-end='||VC_EndAYYYYMM);
         
                                
            
            Validate_Tenorstrip (
                 pcobdate                  => Location.COB_DATE
               ,  porigCommodity           => Location.Commodity
               ,  poverCommodity           =>  vc_overCommodity
               , pOrigLocation             => vc_OriginalLocation
               , pOverrideLoc              => vc_OverrideLocation 
               , pNettingGroup             => Location.Netting_Group_id
               , pstartYearMonth           => VC_STARTAYYYYMM
               , pendYearMonth             => VC_EndAYYYYMM
               , PpricevolInd              => Location.price_vol_Indicator
               , pbackbone_flag            => vc_backbone_flag
               , poverride_BB_flag          => vc_override_bb_flag
               , pmonthlym2m               => Monthly_m2m_tab
               , pexcp_price_flag          => v_excp_price_flag 
               , pout_rtn_code   => vn_rtn_code
               , pout_rtn_msg    => vc_rtn_msg
            );
          


               /****************************/
        
--             start and END Duration is KNOWN
--             NOW 
--             1) Validate is this a Good strip 
--             2) pull external data for this Duration
--             3) find if Manual Quote exists for the same duration? IF YES  Treat as NO QUOTE
--             4) TRy if we can construct Secondary 
--             5) Compute Delta price and delta price %
--             6) validate level 1 and Validate level 2 
            
            
         IF vn_rtn_code = c_success AND vc_rtn_msg = 'STOP' THEN 
         
          Dbms_output.Put_line ('Strip Invalidated ');
         
          CONTINUE ; -- go to NEXT strip 
         END IF;

         --added by siri           
         --    /****************************/ --INVOKING SPECIAL PRICES 

          IF v_excp_price_flag  ='Y' THEN 
         
             DBMS_OUTPUT.Put_line ('special price ');
             
             -- for monthly strip do not call add elements logic 
             IF   VC_STARTAYYYYMM <> VC_EndAYYYYMM THEN  
--              dbms_output.put_line ('calling get proj prices');
              

             
               
                    Get_proj_prices (
                                   pcobdate                  => Location.COB_DATE 
                                 , pOriginallocation        =>  vc_OriginalLocation 
                                 , pOverrideLocation        =>  vc_OverrideLocation
                                 , pNettingGroup             => Location.Netting_Group_id
                                 , ppricevol                 => Location.price_vol_Indicator
                                 , pstartYearMonth           => VC_STARTAYYYYMM
                                 , pendYearMonth             => VC_EndAYYYYMM
                                 , pbackbone_flag            => vc_backbone_flag
                                 , pOverridebb_flag          => vc_override_bb_flag
                                 , pmonthlym2m               => Monthly_m2m_tab
                                 , pout_rtn_code             => vn_rtn_code
                                 , pout_rtn_msg              => vc_rtn_msg
                                 );
                                 
                 IF SUBSTR(vc_rtn_msg , 1,4) = 'STOP' THEN 
                   DBMS_OUTPUT.PUT_LINE ('SKipping this strip for missing Prices');
                   CONTINUE;
                 END IF;
                 
                 

              
             END IF;
             -- end of VC_STARTAYYYYMM <> VC_EndAYYYYMM check 
          ELSE 
             NULL;
          END IF;
                   
        /****************************/            


         /****************************/

        Prepare_qvar_strips  (
                           pcobdate             => Location.COB_DATE
                         , pCurrentMOnth        => vc_CurrentMonth
                         , ppromptMonth         => vc_promptMonth
                         , punderlyingCommodity => Location.UNDERLYING_COMMODITY
                         , pNettingGroupId      => Location.netting_group_id 
                         , plocation            => vc_OriginalLocation
                         , pOverrideLocation    => vc_OverrideLocation
                         , pOverrideID          => v_overrideID                             
                         , ppricevol            => Location.PRICE_VOL_INDICATOR
                         , pstart_YearMonth   => VC_STARTAYYYYMM
                         , pEnd_YearMonth     => vc_endAYYYYMM 
                         , pmonthlym2m        => Monthly_m2m_tab
                         , pcollectn          => v_QvarStrips  
                         , pmonthlyflag       => vc_Monthly_flag
                         , pbalancedFlag      => vc_Balanced_flag
                         , pnonBalancedFlag   => vc_NonBalanced_flag
                         , pStripName         => Tenor.Strip_Name
                         , pmaxYear           => vn_MAXYear
                         , pMINYear           => vn_minYear
                         , ptotalMonths       => vn_totalMonths
                         , prank              => vn_LocationRank
                         , pflatposition      => vc_flat_position
                         , pbackbone_flag     => vc_backbone_flag   
                         , pOverridebb_flag   => vc_override_bb_flag   
                         , pbasis_indicator   => v_basis_indicator
                       , pout_rtn_code   => vn_rtn_code  
                       , pout_rtn_msg    => vc_rtn_msg 
                        );
  
                             
         IF vn_rtn_code <> c_success THEN 
         
          Dbms_output.Put_line ('Prepare external Quotes resulted in Error');
         
           PROCESS_LOG_REC.STAGE        := 'BUILD_Strips';

           PROCESS_LOG_REC.MESSAGE      := 'Location='||vc_Location
           ||'NettingGroup='||Location.Netting_group_id 
           ||'pricevol='||Location.PRICE_VOL_INDICATOR
           ||' Tenor='||vc_StripName
           ||' Prepare external Quotes resulted in Error';
          
   
   
            DMR.PROCESS_LOG_PKG.WRITE_LOG(
                        P_PROCESS_NAME => PROCESS_LOG_REC.PROCESS_NAME,
                        P_STAGE=> PROCESS_LOG_REC.STAGE,
                        P_STATUS=>PROCESS_LOG_PKG.C_STATUS_ERRORS,
                        P_MESSAGE=>PROCESS_LOG_REC.MESSAGE ||' '||SUBSTR(SQLERRM,1,100) 
                        );
            RAISE e_appl_error;
         END IF;

     
       END LOOP;
       -- end of contract years loop 
        
      end loop;
      -- end of tenors 
         
       -- at this stage we have all Required strips/Tenors for all Contract years
       -- AT a LOCATION ( total contract years * Total valid tenors )
 
        
          --- check weather last strip added any elements 
          -- so the current set =  total strips in target collection MINUS
          -- Vn_Objects_START_indx , which represents Number of elements before processing
          -- current Location
   
           IF v_QvarStrips.COUNT > 0 THEN 
                vn_Objects_count := v_QvarStrips.COUNT -  Vn_Objects_START_indx  ;
           END IF;
       
--        dbms_output.Put_line('vn_Objects_count='|| v_QvarStrips.COUNT ||' - '||Vn_Objects_START_indx );


     --if last strip added any elements then we need append
     --these elements to a Location m2m % calculation Datastructure /collection   
      IF vn_Objects_count > 0 THEN 
         
         vn_objindx := 0;
          
         --- from end of previous strip element = start of current strip index
         --- where as vqvarstrips.count = total elements until current strip 
         -- example if last strip addded 10 elements and current strip added 6 more
         -- then we need to append these 6 more to m2m calculation 
         -- i.e from 11 to 16 
         
         For objindx in Vn_Objects_START_indx+1 .. v_QvarStrips.COUNT 
         LOOP         
         

         
           -- intialize m2m Objects with m2m values for m2m% calculation
           IF v_QVarStrips(objindx).PRICE_VOL_INDICATOR = 'PRICE'  THEN 
  
--           dbms_output.Put_line('before='|| m2m_tab_obj.COUNT);
           
                   m2m_tab_obj.EXTEND;
                
                   vn_objindx := vn_objindx + 1 ;
                 
                   m2m_tab_obj(vn_objindx) := qp_strip_Obj(
                      COB_DATE                 =>  v_QVarStrips(objindx).COB_DATE 
                    ,  NETTING_GROUP_ID         => v_QVarStrips(objindx).NETTING_GROUP_ID   
                    ,  NEER_COMMODITY           => v_QVarStrips(objindx).NEER_COMMODITY 
                    ,  NEER_LOCATION            =>v_QVarStrips(objindx).NEER_LOCATION
                   , PRICE_VOL_INDICATOR       => v_QVarStrips(objindx).PRICE_VOL_INDICATOR                    
                    ,  STRIP_TENOR              =>v_QVarStrips(objindx).STRIP_TENOR
                    ,  STRIP_START_YEARMONTH    =>v_QVarStrips(objindx).STRIP_START_YEARMONTH
                    ,  STRIP_END_YEARMONTH      =>v_QVarStrips(objindx).STRIP_END_YEARMONTH
                    ,  TOTAL_TENOR_MONTHS       => v_QVarStrips(objindx).TOTAL_TENOR_MONTHS
                    ,  CONTRACTYEAR_MONTH       => v_QVarStrips(objindx).CONTRACTYEAR_MONTH
                    ,  CONTRACT_YEAR            => v_QVarStrips(objindx).CONTRACT_YEAR
                    ,  LOCATION_DELTA_POSITION  => v_QVarStrips(objindx).LOCATION_DELTA_POSITION
                    ,  COMMODITY_ABS_POSITION   => v_QVarStrips(objindx).COMMODITY_ABS_POSITION
                    ,  CURVE_RANK               => NULL 
                    ,  GRANULAR_QUOTE_INDICATOR => v_QVarStrips(objindx).GRANULAR_QUOTE_INDICATOR
                    ,  COMMODITY_STRIP_FLAG     => v_QVarStrips(objindx).COMMODITY_STRIP_FLAG
                    ,  ABSOLUTE_LEGGED_M2M_VALUE  => v_QVarStrips(objindx).ABSOLUTE_LEGGED_M2M_VALUE
                    ,  ABS_LEGGED_M2M_VOL   => NULL
                    ,  FULLY_VALIDATED_FLAG       => v_QVarStrips(objindx).FULLY_VALIDATED_FLAG
                    ,  FLAT_POSITION_FLAG         => v_QVarStrips(objindx).FLAT_POSITION_FLAG
                    ,  Granular_m2m_Percent       => NULL
                    ,  NonGranular_m2m_percent    => NULL
                    ,  M2M_PERCENT                => NULL
                   );  
                   
             ELSIF  v_QVarStrips(objindx).PRICE_VOL_INDICATOR = 'VOL'  THEN 
             
                   m2m_tab_obj.EXTEND;
                
                   vn_objindx := vn_objindx + 1 ;
                 
                   m2m_tab_obj(vn_objindx) := qp_strip_Obj(
                      COB_DATE                 =>  v_QVarStrips(objindx).COB_DATE 
                    ,  NETTING_GROUP_ID         => v_QVarStrips(objindx).NETTING_GROUP_ID   
                    ,  NEER_COMMODITY           => v_QVarStrips(objindx).NEER_COMMODITY 
                    ,  NEER_LOCATION            =>v_QVarStrips(objindx).NEER_LOCATION
                    , PRICE_VOL_INDICATOR       => v_QVarStrips(objindx).PRICE_VOL_INDICATOR
                    ,  STRIP_TENOR              =>v_QVarStrips(objindx).STRIP_TENOR
                    ,  STRIP_START_YEARMONTH    =>v_QVarStrips(objindx).STRIP_START_YEARMONTH
                    ,  STRIP_END_YEARMONTH      =>v_QVarStrips(objindx).STRIP_END_YEARMONTH
                    ,  TOTAL_TENOR_MONTHS       => v_QVarStrips(objindx).TOTAL_TENOR_MONTHS
                    ,  CONTRACTYEAR_MONTH       => v_QVarStrips(objindx).CONTRACTYEAR_MONTH
                    ,  CONTRACT_YEAR            => v_QVarStrips(objindx).CONTRACT_YEAR
                    ,  LOCATION_DELTA_POSITION  => v_QVarStrips(objindx).LOCATION_DELTA_POSITION
                    ,  COMMODITY_ABS_POSITION   => v_QVarStrips(objindx).COMMODITY_ABS_POSITION
                    ,  CURVE_RANK               => NULL 
                    ,  GRANULAR_QUOTE_INDICATOR => v_QVarStrips(objindx).GRANULAR_QUOTE_INDICATOR
                    ,  COMMODITY_STRIP_FLAG     => v_QVarStrips(objindx).COMMODITY_STRIP_FLAG
                    ,  ABSOLUTE_LEGGED_M2M_VALUE => NULL
                    ,  ABS_LEGGED_M2M_VOL   => v_QVarStrips(objindx).ABS_LEGGED_M2M_VOL
                    ,  FULLY_VALIDATED_FLAG       => v_QVarStrips(objindx).FULLY_VALIDATED_FLAG
                    ,  FLAT_POSITION_FLAG         => v_QVarStrips(objindx).FLAT_POSITION_FLAG
                    ,  Granular_m2m_Percent       => NULL
                    ,  NonGranular_m2m_percent    => NULL
                    ,  M2M_PERCENT                => NULL
                   );
                   

             END IF;

         END LOOP;
         
         --dump to verify data is correct

  
 
     --- Calculate Granular m2m% for a Location  
         BEGIN
     
           --calculate DISTINCT Months ABS m2m For a Location 
           -- where  Position <> 0
     
            BEGIN
            
                   vc_location := Location.LOCATION;
                   vn_NettingGroup :=        Location.netting_group_id;
                   vc_commodity    :=        Location.Commodity;
                   vc_uc_commodity :=        Location.UNDERLYING_COMMODITY;
                   vc_pricevol     :=        Location.PRICE_VOL_INDICATOR;
       
       
            WITH
            m2mValues as (         
             SELECT
              a.NEER_LOCATION              ,
              a.netting_group_id           ,
              a.strip_tenor                ,
              b.CONTRACTYEAR_MONTH         ,
              b.Contract_Year              ,
              a.STRIP_START_YEARMONTH , 
              a.STRIP_END_YEARMONTH   ,
              a.GRANULAR_QUOTE_INDICATOR   ,
              ( CASE 
              WHEN a.price_vol_indicator = 'PRICE' THEN b.ABSOLUTE_LEGGED_M2M_VALUE 
              WHEN a.price_vol_indicator = 'VOL' THEN b.ABSOLUTE_LEGGED_M2M_VALUE_VOL 
              END
              ) ABSOLUTE_LEGGED_M2M_VALUE  ,
              a.FULLY_VALIDATED_FLAG       ,
              a.FLAT_POSITION_FLAG         
             FROM  
             TABLE( m2m_tab_obj ) a
             ,TABLE( Monthly_m2m_tab) b
             where
                 a.cob_date = b.cob_date
                 and a.NEER_Location = b.LOCATION
                 and a.Netting_group_id = b.Netting_group_id
                 and a.PRICE_VOL_INDICATOR = b.PRICE_VOL_INDICATOR
                 and to_Number(b.CONTRACTYEAR_MONTH) >= to_NUMBER(a.STRIP_START_YEARMONTH) 
                 and  to_Number(b.CONTRACTYEAR_MONTH) <= TO_NUMBER(a.STRIP_END_YEARMONTH)
                 and a.commodity_strip_flag = 'N'
                 and a.NEER_LOCATION = vc_location --'PJM-AECO-2x16'
                 and a.Netting_group_id = vn_NettingGroup
                 and a.Price_vol_indicator = vc_pricevol
                 and b.Location_delta_position <> 0
             )
            , Denom_sum as (
                 SELECT SUM(m1.ABSOLUTE_LEGGED_M2M_VALUE) m2m
                  FROM
                  (
                   SELECT DISTINCT 
                   CONTRACTYEAR_MONTH
                   ,ABSOLUTE_LEGGED_M2M_VALUE
                   FROM 
                    m2mValues 
                  ) m1
            ) 
            , Granularm2m as (
                          SELECT SUM(g1.ABSOLUTE_LEGGED_M2M_VALUE) m2m
                          FROM
                          (
                           SELECT DISTINCT 
                           CONTRACTYEAR_MONTH
                           ,ABSOLUTE_LEGGED_M2M_VALUE
                           FROM 
                            m2mValues m
                           where
                           m.GRANULAR_QUOTE_INDICATOR = 'Y'
                           and  FULLY_VALIDATED_FLAG = 'Y'
                          )g1
            ) 
            , NonGranularm2m as (
                          SELECT SUM(g1.ABSOLUTE_LEGGED_M2M_VALUE) m2m
                          FROM
                          (
                           SELECT DISTINCT 
                           CONTRACTYEAR_MONTH
                           ,ABSOLUTE_LEGGED_M2M_VALUE
                           FROM 
                            m2mValues m
                           where
                           m.GRANULAR_QUOTE_INDICATOR = 'N'
                           and  FULLY_VALIDATED_FLAG = 'Y'
                           MINUS
                           SELECT DISTINCT 
                           CONTRACTYEAR_MONTH
                           ,ABSOLUTE_LEGGED_M2M_VALUE
                           FROM 
                            m2mValues m
                           where
                           m.GRANULAR_QUOTE_INDICATOR = 'Y'
                           and  FULLY_VALIDATED_FLAG = 'Y'
                          )g1
            )   
            , Basism2m as (
                          SELECT SUM(b1.ABSOLUTE_LEGGED_M2M_VALUE) m2m
                          FROM
                          (
                           SELECT DISTINCT 
                           CONTRACTYEAR_MONTH
                           ,ABSOLUTE_LEGGED_M2M_VALUE
                           FROM 
                            m2mValues m
                           where
                            FULLY_VALIDATED_FLAG = 'Y'
                          )b1
            )                        
             SELECT 
             Denom_sum.m2m
             , Granularm2m.m2m
             ,  NonGranularm2m.m2m
             , Basism2m.m2m
             INTO v_denom , v_Granularm2m , v_nonGranularm2m, v_m2mbasis                              
             FROM 
             Denom_sum 
            , Granularm2m
            , NonGranularm2m
            , Basism2m  
            ;
            

             
           
           
           
             IF v_denom =0 THEN v_denom := 1 ; END IF;
 
                v_granular_m2mPercent   := (NVL(v_Granularm2m,0)/v_denom);

                v_nongranular_m2mPercent   := (NVL(v_nonGranularm2m, 0)/v_denom);

                v_m2mbasis_m2mPercent    := (NVL(v_m2mbasis,0)/v_denom);


              --Derive rank for this location if not already have rank assigned
              
              IF vn_LocationRank IS NULL THEN 
                Derive_final_ranking (       
                              pcobdate              => pCOBDATE
                            , pUnderlyingCommodity  => vc_uc_commodity
                            , ppriceVOlind          => vc_pricevol
                            ,  pgranularm2m         => v_granular_m2mPercent 
                            , pnongranularm2m       => v_nongranular_m2mPercent
                            , pbasism2m             => v_m2mbasis_m2mPercent
                            , pflatpos              => vc_flat_position 
                            , prank             => vn_LocationRank
                            , pout_rtn_code     => vn_rtn_code 
                            , pout_rtn_msg      => vc_rtn_msg
                           );
              ELSE 
               --otherwise rank is already derived no need to derive one
                null;             
              END IF;
                               
                               

             -- Now assign entire collection with current m2m% values to every strip in the target 
            For objindx in Vn_Objects_START_indx+1 .. v_QvarStrips.COUNT LOOP 
              v_QVarStrips(objindx).Granular_m2m_percent := v_granular_m2mPercent ;
              v_QVarStrips(objindx).NonGranular_m2m_percent := v_nongranular_m2mPercent; 
              v_QVarStrips(objindx).Basis_m2mPercent := v_m2mbasis_m2mPercent; 
              v_QVarStrips(objindx).m2m_Percent := v_m2mbasis_m2mPercent; 
              v_QVarStrips(objindx).Curve_rank := vn_LocationRank;
              
            END LOOP;
            
            
            EXCEPTION
            WHEN NO_DATA_FOUND THEN 
            null;
            
            END ;
          
 --            INSERT  /*+ APPEND */ INTO riskdb.qp_m2mTab 

--              Insert_m2m(
--                m2m_tab_obj       => m2m_tab_obj
--              , pout_rtn_code   => vn_rtn_code  
--              , pout_rtn_msg    => vc_rtn_msg 
--              );
      
            --reset m2m_tab_Obj
            m2m_tab_obj:= null_m2m_tabObj ; 
             
            --Construct new m2mtabObj based on current locatio results
            --- all are in collection 
             
                 

           -- clear m2m data Object , External Data Object 
           -- Go back to new Location 
           
           
         END;  -- End of m2m% calculation Block  
         
      ELSE
 
            null;
      END IF;
          
          
   END LOOP;
   -- end of locations loop 
   
  
       -- Delete Location 
       -- Load Location data as BULK data 

         --- Load the Collection
       PROCESS_LOG_REC.MESSAGE      := 'Clean up records from '
                                      ||'RISKDB.QP_STRIP_LVL_NEER_EXT_DATA'
                                      ||'COB_DATE = TO_DATE('
                                      ||vc_singlequote||vc_cobdate ||vc_singlequote||', '
                                      ||vc_singlequote|| 'DD-MON-YYYY'||vc_singlequote
                                      ||')'
                                      ;
      --vc_section := 'Clean up records from RISKDB.QP_STRIP_LVL_NEER_EXT_DATA '||PCOBDATE;
      
      
      /* This COB_DATE data is not finalized , so clean up RISKDB.QP_INT_FWD_CURVES_RAW_DATA  */
         vc_sql_stmt := 'DELETE FROM RISKDB.QP_STRIP_LVL_NEER_EXT_DATA '
                     ||' WHERE COB_DATE = TO_DATE('
                     ||vc_singlequote||vc_cobdate ||vc_singlequote||', '
                     ||vc_singlequote|| 'DD-MON-YYYY'||vc_singlequote
                     ||')'
--                     ||' AND Underlying_commodity ='||vc_singlequote||'ELECTRICITY'||vc_singlequote
--                   ||' AND MONTHLY_STRIP_FLAG = '||vc_singlequote||'Y'||vc_singlequote
--                   || ' AND NEER_COMMODITY LIKE '||vc_singlequote||'NEPOOL-.H.INTERNAL_HUB-5x16'||vc_singlequote
--                     ||' AND NEER_LOCATION LIKE ' ||vc_singlequote||'ALGONQUIN'||vc_singlequote
--                     ||' AND NEER_LOCATION IN ( '
--||vc_singlequote||'MISO-INDIANA.HUB-WEC.PTBHGB2-5x16'||vc_singlequote
--||', '||vc_singlequote||'MISO-INDIANA.HUB-5x16'||vc_singlequote
--||', '||vc_singlequote|| 'MISO-INDIANA.HUB-7x8'||vc_singlequote
--||', '||vc_singlequote||  'MISO-INDIANA.HUB-DA-2x16'||vc_singlequote
--||', '||vc_singlequote|| 'MISO-INDIANA.HUB-DA-5x16'||vc_singlequote
--||', '||vc_singlequote|| 'MISO-INDIANA.HUB-WEC.PTBHGB2-Off'||vc_singlequote
--||', '||vc_singlequote|| 'MISO-INDIANA.HUB-2x16'||vc_singlequote
--||', '||vc_singlequote|| 'MISO-INDIANA.HUB-DA-7x8'||vc_singlequote
--||' ) '
--                     ||' AND Netting_Group_id = '||Location.Netting_Group_id
 ;

              vn_rtn_code := NULL;
              vc_rtn_msg := '';
              
        --     ----dbms_output.Put_line(vc_section);
              
             DELETE_RECORDS ( pDelete_statement => vc_sql_stmt 
                              ,pout_rtn_code => vn_rtn_code      
                              ,pout_rtn_msg => vc_rtn_msg  
                              );


             
             IF vn_rtn_code <> c_success THEN 
               RAISE e_appl_error;
             END IF;
              
             Load_qvar_target(
               pcollectn   => v_QvarStrips 
              , pout_rtn_code    => vn_rtn_code    
              , pout_rtn_msg     => vc_rtn_msg 
             );
             
             IF vn_rtn_code <> c_success THEN 
                RAISE e_appl_error;
             END IF; 
                
   PROCESS_LOG_REC.STAGE        := 'BUILD_Strips';
   PROCESS_LOG_REC.STATUS       := PROCESS_LOG_PKG.C_STATUS_COMPLETED;
   PROCESS_LOG_REC.MESSAGE      := 'BUILD_Strips Process Completed';
     
   write_log;
  
    SELECT count(*) 
    INTO li_ERRORS
    FROM 
    DMR.PROCESS_LOG l
    WHERE
    application_Name = 'QVAR_APP'
    and process_name = 'BUILD_STRIPS' 
    and parent_name = 'QP_ZEMA_EXTRACT_PKG'
    and STATUS = 'ERRORS'
    and TIMESTAMP  > vn_starttimeStamp 
    and l.log_id > g_minlogid
   ;
    
   IF li_ERRORS > 0 THEN 
    
    PSTATUSCODE := 'SUCCESS';
    PSTATUSMSG := 'Process Completed with Errors, Please check Error Log DMR.PROCESS_LOG for details';
   
   ELSE
   
    PSTATUSCODE := 'SUCCESS';
    PSTATUSMSG := 'Process Completed Successfully';      
   
   END IF;
          
  EXCEPTION
  WHEN e_appl_error THEN 
    
    DMR.PROCESS_LOG_PKG.WRITE_LOG(
                        P_PROCESS_NAME => PROCESS_LOG_REC.PROCESS_NAME,
                        P_STAGE=> PROCESS_LOG_REC.STAGE,
                        P_STATUS=>PROCESS_LOG_PKG.C_STATUS_ERRORS,
                        P_MESSAGE=>NVL( vc_rtn_msg, SQLERRM) 
                        );  
  WHEN OTHERS THEN 
      
        DMR.PROCESS_LOG_PKG.WRITE_LOG(
            P_PROCESS_NAME => PROCESS_LOG_REC.PROCESS_NAME,
            P_STAGE=> PROCESS_LOG_REC.STAGE,
            P_STATUS=>PROCESS_LOG_PKG.C_STATUS_ERRORS,
            P_MESSAGE=>substr(PROCESS_LOG_REC.MESSAGE||SQLERRM , 1,250)
            );
                         

END Build_strips;
                                                  
END QP_ZEMA_Extract_PKG;
/

