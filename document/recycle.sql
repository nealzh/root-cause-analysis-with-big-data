set nocount on
declare
@WaferStartOID OID,
@VendorCorrItemOID OID,
@ProdNoCorrItemOID OID,
@EmployeeOutOID OID,
@MonitorLoadOID OID,
@RecipeOID OID
select
@WaferStartOID = (select step_OID from traveler..step where step_name = '0000-00 START LOT START')
select
@VendorCorrItemOID = (select corr_item_OID from traveler..corr_item where corr_item_no = 3820) 
select
@ProdNoCorrItemOID = (select corr_item_OID from traveler..corr_item where corr_item_no = 9035) 
select
@EmployeeOutOID = (select corr_item_OID from traveler..corr_item where corr_item_no = 4672) 
select
@MonitorLoadOID = (select step_OID from traveler..step where step_name = '3010-15 NTUB PHOTO')
select
@RecipeOID = (select corr_item_OID from traveler..corr_item where corr_item_no = 424) 
create table #tmp_from_lot
(
lot_id MT_name not null
)
insert into #tmp_from_lot
select from_lot_id from fab_lot_extraction..wafer_genealogy WFR where WFR.wafer_scribe in (
select wafer_scribe from fab_lot_extraction..wafer_genealogy where to_lot_id = '0716117.007')
/*
create table #tmp_to_lot
(
lot_id MT_name not null
)
insert into #tmp_to_lot
select to_lot_id from fab_lot_extraction..wafer_genealogy WFR where WFR.wafer_scribe in (
select wafer_scribe from fab_lot_extraction..wafer_genealogy where to_lot_id = '0716117.007')
*/
select FLH.fab_lot_hist_OID, FLH.lot_id
into #tmp_from_flh
from fab_lot_extraction..fab_lot_hist FLH, traveler..trav_step TS, traveler..step STEP
where FLH.lot_id in (select lot_id from #tmp_from_lot)
and TS.trav_step_OID = FLH.trav_step_OID
and TS.step_OID = STEP.step_OID
and STEP.step_OID = @WaferStartOID
select FLH.fab_lot_hist_OID, FLH.lot_id
into #tmp_from_mon_load
from fab_lot_extraction..fab_lot_hist FLH, traveler..trav_step TS, traveler..step STEP
where FLH.lot_id in (select lot_id from #tmp_from_lot)
and TS.trav_step_OID = FLH.trav_step_OID
and TS.step_OID = STEP.step_OID
and STEP.step_OID = @MonitorLoadOID
/*
select FLH.fab_lot_hist_OID, FLH.lot_id
into #tmp_to_flh
from fab_lot_hist FLH, traveler..trav_step TS, traveler..step STEP
where FLH.lot_id in (select lot_id from #tmp_to_lot)
and TS.trav_step_OID = FLH.trav_step_OID
and TS.step_OID = STEP.step_OID
and STEP.step_OID = @WaferStartOID
*/
select
WFR.wafer_scribe,
datediff(dd,'12/30/1899',WFR.move_datetime),substring(convert(char(8),WFR.move_datetime,108),1,5),
rtrim(WFR.from_lot_id),
rtrim(WFR.to_lot_id),
rtrim(TRAV.trav_id),
(select distinct rtrim(fab_corr_attr_value) from fab_lot_extraction..fab_lot_corr_hist FLCH where FLCH.fab_lot_hist_OID = FFLH.fab_lot_hist_OID and FLCH.corr_item_OID = @VendorCorrItemOID),
(select distinct rtrim(fab_corr_attr_value) from fab_lot_extraction..fab_lot_corr_hist FLCH where FLCH.fab_lot_hist_OID = FFLH.fab_lot_hist_OID and FLCH.corr_item_OID = @ProdNoCorrItemOID),
(select distinct rtrim(SAP.micron_username) from fab_lot_extraction..fab_lot_corr_hist FLCH, reference..SAP_worker SAP
where FLCH.fab_lot_hist_OID = FFLH.fab_lot_hist_OID
and FLCH.corr_item_OID = @EmployeeOutOID
and FLCH.fab_corr_attr_value = convert(char(10),SAP.worker_no)),
(select rtrim(STEP.step_name)
from
fab_lot_extraction..fab_lot_status FLS,
traveler..step_data_for_fab SDFAB,
traveler..traveler TRAV,
traveler..trav_step TS,
traveler..step STEP,
reference..mfg_area AREA
where
WFR.from_lot_id = FLS.lot_id and
FLS.trav_step_OID = TS.trav_step_OID and
TS.trav_OID = TRAV.trav_OID and
TS.step_OID = STEP.step_OID and
TS.step_OID = SDFAB.step_OID and
SDFAB.mfg_facility_OID = 0x7f5156e2400a9854 and
SDFAB.mfg_area_OID = AREA.mfg_area_OID),
rtrim(AREA.mfg_area_id),
rtrim(STEP.step_name)
from
fab_lot_extraction..wafer_genealogy WFR,
traveler..step_data_for_fab SDFAB,
traveler..traveler TRAV,
traveler..trav_step TS,
traveler..step STEP,
reference..mfg_area AREA,
#tmp_from_flh FFLH,
#tmp_from_mon_load FML
where
WFR.wafer_scribe in (select wafer_scribe from fab_lot_extraction..wafer_genealogy where to_lot_id = '0716117.007') and
WFR.trav_step_OID = TS.trav_step_OID and
TS.trav_OID = TRAV.trav_OID and
TS.step_OID = STEP.step_OID and
TS.step_OID = SDFAB.step_OID and
SDFAB.mfg_facility_OID = 0x7f5156e2400a9854 and
SDFAB.mfg_area_OID = AREA.mfg_area_OID and
WFR.from_lot_id = FFLH.lot_id and
WFR.from_lot_id = FML.lot_id  
order by
WFR.wafer_scribe asc, WFR.move_datetime desc
drop table #tmp_from_lot, #tmp_from_flh, #tmp_from_mon_load  
