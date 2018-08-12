BEGIN;
select lw_initialise('kermit',3448);

select lw_addedgeparticipant('kermit','{
	  "schemaname":"data",						 
      "tablename": "ugprimaryline",
      "primarykey":"objectid",
      "geomcolumn": "wkb_geometry",
      "labelcolumn": "dataid",
      "phasecolumn": "phasingcode",
      "phasemap":{"ABC":"7","AB":"6","AC":"5","BC":"3","A":"4","B":"2","C":"1"}
    }')	;

select lw_addedgeparticipant('kermit','{
	  "schemaname":"data",						 
      "tablename": "ohprimaryline",
      "primarykey":"objectid",
      "geomcolumn": "wkb_geometry",
      "labelcolumn": "dataid",
      "phasecolumn": "phasingcode",
      "phasemap":{"ABC":"7","AB":"6","AC":"5","BC":"3","A":"4","B":"2","C":"1"}
    }')	;

select lw_addnodeparticipant('kermit', $${
	  "schemaname":"data",						 
      "tablename": "switchdevicebank",
      "primarykey":"facilityid",
      "geomcolumn": "wkb_geometry",
      "sourcequery": "\"devicetype\" = 'FDR'",
      "blockquery": "0 =ANY (array[\"enabled\",\"normalposa\",\"normalposb\",\"normalposc\"])",
      "phasecolumn": "phasingcode",
      "phasemap":{"ABC":"7","AB":"6","AC":"5","BC":"3","A":"4","B":"2","C":"1"}
    }$$);
	
select lw_addnodeparticipant('kermit', $${
	  "schemaname":"data",						 
      "tablename": "overcurrentdevicebank",
      "primarykey":"facilityid",
      "geomcolumn": "wkb_geometry",
      "sourcequery": "\"devicetype\" = 'FDR'",
      "blockquery": "0 =ANY (array[\"enabled\",\"normalposa\",\"normalposb\",\"normalposc\"])",
      "phasecolumn": "phasingcode",
      "phasemap":{"ABC":"7","AB":"6","AC":"5","BC":"3","A":"4","B":"2","C":"1"}
    }$$);

select lw_addnodeparticipant('kermit', $${
	  "schemaname":"data",						 
      "tablename": "transformerbank",
      "primarykey":"facilityid",
      "geomcolumn": "wkb_geometry",
      "sourcequery": "1 = 2",
      "blockquery": "\"enabled\" = 0",
      "phasecolumn": "phasingcode",
      "phasemap":{"ABC":"7","AB":"6","AC":"5","BC":"3","A":"4","B":"2","C":"1"}
    }$$);
	
select lw_generate('kermit');

commit;
