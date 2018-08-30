BEGIN;
select lw_initialise('powerflow',3448);

select lw_addedgeparticipant('powerflow','{
	  "schemaname":"data",						 
      "tablename": "ugprimaryline",
      "primarykey":"objectid",
      "geomcolumn": "g",
      "labelcolumn": "feeerid",
      "phasecolumn": "phasingcode",
      "phasemap":{"ABC":"7","AB":"6","AC":"5","BC":"3","A":"4","B":"2","C":"1"}
    }')	;

select lw_addedgeparticipant('powerflow','{
	  "schemaname":"data",						 
      "tablename": "ohprimaryline",
      "primarykey":"objectid",
      "geomcolumn": "g",
      "labelcolumn": "dataid",
      "phasecolumn": "phasingcode",
      "phasemap":{"ABC":"7","AB":"6","AC":"5","BC":"3","A":"4","B":"2","C":"1"}
    }')	;

select lw_addnodeparticipant('powerflow', $${
	  "schemaname":"data",						 
      "tablename": "switchdevicebank",
      "primarykey":"facilityid",
      "geomcolumn": "g",
      "sourcequery": "\"devicetype\" = 'FDR'",
      "blockquery": "enabled=0",
      "phasecolumn": "phasingcode",
      "phasemap":{"ABC":"7","AB":"6","AC":"5","BC":"3","A":"4","B":"2","C":"1"}
    }$$);
	
select lw_addnodeparticipant('powerflow', $${
	  "schemaname":"data",						 
      "tablename": "overcurrentdevicebank",
      "primarykey":"facilityid",
      "geomcolumn": "g",
      "sourcequery": "\"devicetype\" = 'FDR'",
      "blockquery": "0 =ANY (array[\"enabled\",\"normalposa\",\"normalposb\",\"normalposc\"])",
      "phasecolumn": "phasingcode",
      "phasemap":{"ABC":"7","AB":"6","AC":"5","BC":"3","A":"4","B":"2","C":"1"}
    }$$);

select lw_addnodeparticipant('powerflow', $${
	  "schemaname":"data",						 
      "tablename": "transformerbank",
      "primarykey":"facilityid",
      "geomcolumn": "g",
      "sourcequery": "1 = 2",
      "blockquery": "\"enabled\" = 0",
      "phasecolumn": "phasingcode",
      "phasemap":{"ABC":"7","AB":"6","AC":"5","BC":"3","A":"4","B":"2","C":"1"}
    }$$);
	
select lw_generate('powerflow');

commit;
