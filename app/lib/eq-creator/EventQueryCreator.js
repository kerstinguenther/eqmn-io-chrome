'use strict'

/**
 * this file contains all functions for general creation of queries 
 * (includes functions that are used for more than one query language)
 */

var abbr;

var getModel = require('./EventQueryModeler').getModel;
var createEsperQuery = require('./EsperQueryCreator').createEsperQuery;
var createDroolsQuery = require('./DroolsQueryCreator').createDroolsQuery;
var validateDroolsModel = require('./DroolsQueryCreator').validateDroolsModel;

module.exports.createQuery = function(language) {
	var model = getModel();
	
	var query = {};
	abbr = null;
	
	if(!model.errorMessage) {
		
		switch(language) {
		case 'ESPER': 
			query = createEsperQuery(model);
			break;
		case 'DROOLS':
			// note: in drools not all constructs can be modeled
			var validationError = validateDroolsModel(model);
			if(!validationError) {
				query = createDroolsQuery(model);
			} else {
				query.error = validationError;
			}
			break;
		default:
			console.error(language + " is not a supported query language")
		}	
	} else {
		query.errorMessage = model.errorMessage;
	}
	
	if(model.warningMessage) {
		query.warningMessag = "__________________________________________________________________________________________";		// for new line ...
		query.warningMessage = model.warningMessage;
	}

	return query;
};

//module.exports.getNextAbbreviation = function() {
//	if(!abbr) {
//		abbr = 'a';
//		return abbr;
//	}
//	if (/^z+$/.test(abbr)) {
//		// all z's -> replace all with a's and add one a
//		abbr = abbr.replace(/z/g, 'a') + 'a';
//	} else {
//		// increment last char
//		abbr = abbr.slice(0, -1) + String.fromCharCode(abbr.slice(-1).charCodeAt() + 1);
//	}
//	return abbr;
//};

