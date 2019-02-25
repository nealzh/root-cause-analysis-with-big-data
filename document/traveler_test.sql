SELECT * FROM(
(SELECT wafer_scribe, from_lot_id, to_lot_id FROM FAB_7_FT_DM.WAFER_GENEALOGY
where wafer_scribe = 'CZ0NQ161SED4') WG

LEFT JOIN

(SELECT wafer_id, lot_id FROM FAB_7_FT_DM.wafer_status
--lot_id is to_lot_id
where wafer_scribe = 'CZ0NQ161SED4') WS
ON WG.to_lot_id = WS.lot_id);
SELECT * FROM(
(SELECT wafer_scribe, from_lot_id, to_lot_id FROM FAB_7_FT_DM.WAFER_GENEALOGY
where wafer_scribe = 'CZ0XH041SEH2') WG

LEFT JOIN

(SELECT wafer_id, lot_id FROM FAB_7_FT_DM.wafer_status
--lot_id is to_lot_id
where wafer_scribe = 'CZ0XH041SEH2') WS
ON WG.to_lot_id = WS.lot_id);
SELECT * FROM(
(SELECT wafer_scribe, from_lot_id, to_lot_id FROM FAB_7_FT_DM.WAFER_GENEALOGY
where wafer_scribe = 'CZ0XH031SEC6') WG

LEFT JOIN

(SELECT wafer_id, lot_id FROM FAB_7_FT_DM.wafer_status
--lot_id is to_lot_id
where wafer_scribe = 'CZ0XH031SEC6') WS
ON WG.to_lot_id = WS.lot_id);
SELECT * FROM(
(SELECT wafer_scribe, from_lot_id, to_lot_id FROM FAB_7_FT_DM.WAFER_GENEALOGY
where wafer_scribe = 'CZ3DN268SEG7') WG

LEFT JOIN

(SELECT wafer_id, lot_id FROM FAB_7_FT_DM.wafer_status
--lot_id is to_lot_id
where wafer_scribe = 'CZ3DN268SEG7') WS
ON WG.to_lot_id = WS.lot_id);
SELECT * FROM(
(SELECT wafer_scribe, from_lot_id, to_lot_id FROM FAB_7_FT_DM.WAFER_GENEALOGY
where wafer_scribe = 'CZ0NP142SEF6') WG

LEFT JOIN

(SELECT wafer_id, lot_id FROM FAB_7_FT_DM.wafer_status
--lot_id is to_lot_id
where wafer_scribe = 'CZ0NP142SEF6') WS
ON WG.to_lot_id = WS.lot_id);
SELECT * FROM(
(SELECT wafer_scribe, from_lot_id, to_lot_id FROM FAB_7_FT_DM.WAFER_GENEALOGY
where wafer_scribe = 'CZ3PH063SEG3') WG

LEFT JOIN

(SELECT wafer_id, lot_id FROM FAB_7_FT_DM.wafer_status
--lot_id is to_lot_id
where wafer_scribe = 'CZ3PH063SEG3') WS
ON WG.to_lot_id = WS.lot_id);
SELECT * FROM(
(SELECT wafer_scribe, from_lot_id, to_lot_id FROM FAB_7_FT_DM.WAFER_GENEALOGY
where wafer_scribe = 'CZ2CX055SEF0') WG

LEFT JOIN

(SELECT wafer_id, lot_id FROM FAB_7_FT_DM.wafer_status
--lot_id is to_lot_id
where wafer_scribe = 'CZ2CX055SEF0') WS
ON WG.to_lot_id = WS.lot_id);
SELECT * FROM(
(SELECT wafer_scribe, from_lot_id, to_lot_id FROM FAB_7_FT_DM.WAFER_GENEALOGY
where wafer_scribe = 'CZ2CX075SEG5') WG

LEFT JOIN

(SELECT wafer_id, lot_id FROM FAB_7_FT_DM.wafer_status
--lot_id is to_lot_id
where wafer_scribe = 'CZ2CX075SEG5') WS
ON WG.to_lot_id = WS.lot_id);
SELECT * FROM(
(SELECT wafer_scribe, from_lot_id, to_lot_id FROM FAB_7_FT_DM.WAFER_GENEALOGY
where wafer_scribe = 'CZ0XH343SEF6') WG

LEFT JOIN

(SELECT wafer_id, lot_id FROM FAB_7_FT_DM.wafer_status
--lot_id is to_lot_id
where wafer_scribe = 'CZ0XH343SEF6') WS
ON WG.to_lot_id = WS.lot_id);
SELECT * FROM(
(SELECT wafer_scribe, from_lot_id, to_lot_id FROM FAB_7_FT_DM.WAFER_GENEALOGY
where wafer_scribe = 'CZ0XH344SEC5') WG

LEFT JOIN

(SELECT wafer_id, lot_id FROM FAB_7_FT_DM.wafer_status
--lot_id is to_lot_id
where wafer_scribe = 'CZ0XH344SEC5') WS
ON WG.to_lot_id = WS.lot_id);