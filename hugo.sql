BEGIN;
select lw_initialise('hugo',3448);

select lw_addedgeparticipant('hugo','{
	  "schemaname":"hugo_",						 
      "tablename": "ugprimaryline",
      "primarykey":"objectid",
      "geomcolumn": "wkb_geometry",
      "labelcolumn": "hugo_id",
      "phasecolumn": "phasingcode",
      "phasemap":{"ABC":"7","AB":"6","AC":"5","BC":"3","A":"4","B":"2","C":"1"}
    }')	;

select lw_addedgeparticipant('hugo','{
	  "schemaname":"hugo_",						 
      "tablename": "ohprimaryline",
      "primarykey":"objectid",
      "geomcolumn": "wkb_geometry",
      "labelcolumn": "hugo_id",
      "phasecolumn": "phasingcode",
      "phasemap":{"ABC":"7","AB":"6","AC":"5","BC":"3","A":"4","B":"2","C":"1"}
    }')	;

select lw_addnodeparticipant('hugo', $${
	  "schemaname":"hugo_",						 
      "tablename": "switchdevicebank",
      "primarykey":"facilityid",
      "geomcolumn": "wkb_geometry",
      "sourcequery": "\"devicetype\" = 'FDR'",
      "blockquery": "0 =ANY (array[\"enabled\",\"normalposa\",\"normalposb\",\"normalposc\"])",
      "phasecolumn": "phasingcode",
      "phasemap":{"ABC":"7","AB":"6","AC":"5","BC":"3","A":"4","B":"2","C":"1"}
    }$$);
	
select lw_addnodeparticipant('hugo', $${
	  "schemaname":"hugo_",						 
      "tablename": "overcurrentdevicebank",
      "primarykey":"facilityid",
      "geomcolumn": "wkb_geometry",
      "sourcequery": "\"devicetype\" = 'FDR'",
      "blockquery": "0 =ANY (array[\"enabled\",\"normalposa\",\"normalposb\",\"normalposc\"])",
      "phasecolumn": "phasingcode",
      "phasemap":{"ABC":"7","AB":"6","AC":"5","BC":"3","A":"4","B":"2","C":"1"}
    }$$);

select lw_addnodeparticipant('hugo', $${
	  "schemaname":"hugo_",						 
      "tablename": "transformerbank",
      "primarykey":"facilityid",
      "geomcolumn": "wkb_geometry",
      "sourcequery": "1 = 2",
      "blockquery": "\"enabled\" = 0",
      "phasecolumn": "phasingcode",
      "phasemap":{"ABC":"7","AB":"6","AC":"5","BC":"3","A":"4","B":"2","C":"1"}
    }$$);
	
select lw_generate('hugo');

commit;
