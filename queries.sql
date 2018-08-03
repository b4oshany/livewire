select lw_initialise('gratis',3448);

select lw_addedgeparticipant('gratis','{
	  "schemaname":"_arcfm",						 
      "tablename": "arcfm.UGPRIMARYLINE_EVW",
      "primarykey":"OBJECTID",
      "geomcolumn": "geom",
      "labelcolumn": "dataid",
      "phasecolumn": "PhasingCode",
      "phasemap":{"ABC":"7","AB":"6","AC":"5","BC":"3","A":"4","B":"2","C":"1"}
    }')	;

select lw_addedgeparticipant('gratis','{
	  "schemaname":"_arcfm",						 
      "tablename": "arcfm.OHPRIMARYLINE_EVW",
      "primarykey":"OBJECTID",
      "geomcolumn": "geom",
      "labelcolumn": "dataid",
      "phasecolumn": "PhasingCode",
      "phasemap":{"ABC":"7","AB":"6","AC":"5","BC":"3","A":"4","B":"2","C":"1"}
    }')	;

select lw_addnodeparticipant('gratis', $${
	  "schemaname":"_arcfm",						 
      "tablename": "arcfm.SWITCHDEVICEBANK_EVW",
      "primarykey":"FacilityID",
      "geomcolumn": "geom",
      "sourcequery": "\"DeviceType\" = 'FDR'",
      "blockquery": "0 =ANY (array[\"Enabled\",\"NormalPosA\",\"NormalPosB\",\"NormalPosC\"])",
      "phasecolumn": "PhasingCode",
      "phasemap":{"ABC":"7","AB":"6","AC":"5","BC":"3","A":"4","B":"2","C":"1"}
    }$$);
	
select lw_addnodeparticipant('gratis', $${
	  "schemaname":"_arcfm",						 
      "tablename": "arcfm.OVERCURRENTDEVICEBANK_EVW",
      "primarykey":"FacilityID",
      "geomcolumn": "geom",
      "sourcequery": "\"DeviceType\" = 'FDR'",
      "blockquery": "0 =ANY (array[\"Enabled\",\"NormalPosA\",\"NormalPosB\",\"NormalPosC\"])",
      "phasecolumn": "PhasingCode",
      "phasemap":{"ABC":"7","AB":"6","AC":"5","BC":"3","A":"4","B":"2","C":"1"}
    }$$);

select lw_addnodeparticipant('gratis', $${
	  "schemaname":"_arcfm",						 
      "tablename": "arcfm.TRANSFORMERBANK_EVW",
      "primarykey":"FacilityID",
      "geomcolumn": "geom",
      "sourcequery": "1 = 2",
      "blockquery": "\"Enabled\" = 0",
      "phasecolumn": "PhasingCode",
      "phasemap":{"ABC":"7","AB":"6","AC":"5","BC":"3","A":"4","B":"2","C":"1"}
    }$$);
	
select lw_generate('gratis');