select lw_initialise('kermit',3448);

select lw_addedgeparticipant('kermit','{
	  "schemaname":"data",						 
      "tablename": "ugprimaryline",
      "primarykey":"objectid",
      "geomcolumn": "geom",
      "labelcolumn": "dataid",
      "phasecolumn": "phasingcode",
      "phasemap":{"ABC":"7","AB":"6","AC":"5","BC":"3","A":"4","B":"2","C":"1"}
    }')	;

select lw_addedgeparticipant('kermit','{
	  "schemaname":"data",						 
      "tablename": "ohprimaryline",
      "primarykey":"objectid",
      "geomcolumn": "geom",
      "labelcolumn": "dataid",
      "phasecolumn": "phasingcode",
      "phasemap":{"ABC":"7","AB":"6","AC":"5","BC":"3","A":"4","B":"2","C":"1"}
    }')	;

select lw_addnodeparticipant('kermit', $${
	  "schemaname":"data",						 
      "tablename": "switchdevicebank",
      "primarykey":"objectid",
      "geomcolumn": "geom",
      "sourcequery": "\"DeviceType\" = 'FDR'",
      "blockquery": "\"Enabled\" = 0",
      "phasecolumn": "phasingcode",
      "phasemap":{"ABC":"7","AB":"6","AC":"5","BC":"3","A":"4","B":"2","C":"1"}
    }$$);
	
select lw_addnodeparticipant('kermit', $${
	  "schemaname":"data",						 
      "tablename": "overcurrentdevicebank",
      "primarykey":"objectid",
      "geomcolumn": "geom",
      "sourcequery": "\"DeviceType\" = 'FDR'",
      "blockquery": "\"Enabled\" = 0",
      "phasecolumn": "phasingcode",
      "phasemap":{"ABC":"7","AB":"6","AC":"5","BC":"3","A":"4","B":"2","C":"1"}
    }$$);

select lw_addnodeparticipant('kermit', $${
	  "schemaname":"data",						 
      "tablename": "transformerbank",
      "primarykey":"objectid",
      "geomcolumn": "geom",
      "sourcequery": "1 = 2",
      "blockquery": "\"Enabled\" = 0",
      "phasecolumn": "phasingcode",
      "phasemap":{"ABC":"7","AB":"6","AC":"5","BC":"3","A":"4","B":"2","C":"1"}
    }$$);
	
select lw_generate('kermit');