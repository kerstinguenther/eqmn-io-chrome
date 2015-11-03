'use strict'

//var getNextAbbreviation = require('./EventQueryCreator').getNextAbbreviation;

/**
 * this file contains only functions relevant for creation of drools queries
 */

var usedAbbreviations, abbr;

module.exports.validateDroolsModel = function(model) {
	if(model.input.pattern && !model.input.pattern.interval) {
		if(hasProperty(model.input.pattern, "interval")) {
			return "VALIDATION ERROR: interval can only be the only input element or be part of a sequence (for DRL)";
		}
	} else if(model.input.sequence) {
		// start of sequence must be input event
		if(!model.input.sequence.start.name) {
			return "VALIDATION ERROR: start of sequence must be an input event (for DRL)";
		}
		var next = model.input.sequence.end;
		var val;
		while(next.sequence) {
			val = validateElementInSequence(next.sequence.start);
			if(val) {
				return val;
			}
			next = next.sequence.end;
		}
		val = validateElementInSequence(next);
	}
	return val;
}

function validateElementInSequence(element) {
	if(!(element.name || element.interval || element.negation)) {
		return "VALIDATION ERROR: sequence must only contain input events, negations and simple intervals (for DRL)"; 
	} else if(element.interval && !validateInterval) {
		return "VALIDATION ERROR: interval must only contain one (optionally negated) input event(for DRL)"; 
	}
	return null;
}

function validateInterval(interval) {
	if(!interval.pattern.name | interval.pattern.negation) {
		return false;
	} else {
		if(interval.pattern.negation) {
			if(!interval.pattern.negation.name) {
				return false;
			}
		}
	}
	return true;
}

function hasProperty(element, property) {
	if(JSON.stringify(element).indexOf(property) > -1) {
		return true;
	}
	return false;
}

module.exports.createDroolsQuery = function(model) {
	usedAbbreviations = {};
	abbr = null;

	var query = {};
	var subquery = "";

	/* RULE */
	query.rule = "rule '...'";

	if(model.input.interval) {
		query.rule1 = "timer (int:0 " + getIntervalTime(model.input.interval.value) + ")";
	}

	/* WHEN */
	query.when = "when";

	// (a) several inputs (events or windows)
	if(model.input.from) {
		for(var i=0; i<model.input.from.length; i++) {
			if(model.input.from[i].event) {
				subquery = getSingleEventPattern(model.input.from[i].event, model.output.select);
			} else {
				subquery += getWindowPattern(model.input.from[i].window, model.output.select);
			}
			query["when"+(i+1)] = subquery;
		}
	}

	// (b) single input event
	else if(model.input.name) {
		query.when1 = getSingleEventPattern(model.input, model.output.select);
	}

	// (c) window
	else if(model.input.window) {
		query.when1 = getWindowPattern(model.input.from[i].window, model.output.select);
	}

	// (d) interval or pattern
	// note: timer of interval is handled separately
	else {
		var pattern = (model.input.interval) ? model.input.interval.pattern : model.input;
		var subqueries = getComplexEventPattern(pattern, model.output.select);
		for(var i=0; i<subqueries.length; i++) {
			query["when"+(i+1)] = subqueries[i];
		}
	}

	/* THEN */
	query.then = "then";
	if(model.output.name) {
		var variable = model.output.name.toLowerCase().charAt(0);
		query.then1 = model.output.name + " " + variable + " = new " + model.output.name + "();";
		var n = 2;
		if(model.output.select) {
			var selects = getSingleSelects(model.output.select);
			var select;
			for(var i=0; i<selects.length; i++) {
				select = selects[i];
				query["then"+n] = variable + ".set" + select.as.capitalize() + "(" + getPatternBinding(selects[i].value) + ");";
				n++;
			}
		}
		query["then"+n] = "\t insert(" + variable + ");";
	} else {
		query.then1 = "// do something with selected variables";
	}

	query.end = "end";

	return query;
}

function getIntervalTime(time) {
	return time.substring(0, time.indexOf(" ")+2).replaceAll(" ", "");
}

function getComplexEventPattern(pattern, select) {
	var subqueries = [];

	if(pattern.conjunction) {
		subqueries.push("(");
		for(var i=0; i<pattern.conjunction.length; i++) {
			subqueries.pushArray(getComplexEventPattern(pattern.conjunction[i], select));
			if(i < pattern.conjunction.length-1) {
				subqueries.push("and");
			}
		}
		subqueries.push(")");
	} else if(pattern.disjunction) {
		subqueries.push("(");
		for(var i=0; i<pattern.disjunction.length; i++) {
			subqueries.pushArray(getComplexEventPattern(pattern.disjunction[i], select));
			if(i < pattern.disjunction.length-1) {
				subqueries.push("or");
			}
		}
		subqueries.push(")");
	} else if(pattern.negation) {
		subqueries.push("not (");
		subqueries.pushArray(getComplexEventPattern(pattern.negation, select));
		subqueries.push(")");
	} else if(pattern.sequence) {
		subqueries.pushArray(getSequencePattern(pattern.sequence, select));
	} else if(pattern.name){
		// simple event
		subqueries.push(getSingleEventPattern(pattern, select));
	}

	return subqueries;
}

function getSequencePattern(sequence, select, start) {
	var subqueries = [];
	// conditions for sequences to be mapped to Drools:
	// 1) first element of sequence must be simple input event
	// 2) sequence must not contain disjunction or conjunction operators
	// 3) interval in sequence must contain only an input event and optionally a negation
	var start;
	if(!start) {
		if(sequence.start.name) {
			subqueries.push(getSingleEventPattern(sequence.start, select));
			subqueries.pushArray(getSequencePattern(sequence.end, select, getPatternBinding(sequence.start.name)));
		}
	} else {
		if(sequence.negation) {
			subqueries.push("not " + getSingleEventPattern(sequence.negation, select, start));
		} else if(sequence.name) {
			subqueries.push(getSingleEventPattern(sequence, select, start));
		} else if(sequence.interval) {
			if(sequence.interval.pattern.negation) {
				subqueries.push("not " + getSingleEventPattern(sequence.interval.pattern.negation, select, start, sequence.interval.value));
			} else if(sequence.interval.pattern.name){
				subqueries.push(getSingleEventPattern(sequence.interval.pattern, select, start, sequence.interval.value));
			}
		} else if(sequence.sequence) {
			if(sequence.sequence.start.interval) {
				if(sequence.sequence.start.interval.pattern.negation) {
					if(sequence.sequence.start.interval.pattern.negation.name) {
						subqueries.push("not " + getSingleEventPattern(sequence.sequence.start.interval.pattern.negation, select, start, sequence.sequence.start.interval.value));
						start = sequence.sequence.start.interval.pattern.negation.name;
					}
				} else if(sequence.sequence.start.interval.pattern.name){
					subqueries.push(getSingleEventPattern(sequence.sequence.start.interval.pattern, select, start, sequence.sequence.start.interval.value));
					start = sequence.sequence.start.interval.pattern.name;
				}
			} else if(sequence.sequence.start.negation) {
				if(sequence.sequence.start.negation.name) {
					subqueries.push("not " + getSingleEventPattern(sequence.sequence.start.negation, select, start));
					start = sequence.sequence.start.negation.name;
				}
			} else if(sequence.sequence.start.name) {
				subqueries.push(getSingleEventPattern(sequence.sequence.start, select, start));
				start = sequence.sequence.start.name;
			}
			subqueries.pushArray(getSequencePattern(sequence.sequence.end, select, getPatternBinding(start)));
		}
	}
	return subqueries;
}

function getSingleEventPattern(event, select, start, interval) {
	var subquery = getPatternBinding(event.name) + " : " + event.name + "(";
	if(event.condition) {
		subquery += replaceEventTypes(event.condition);
		if(select) {
			subquery += ", ";
		}
	}
	/* SELECT */
	if(select) {
		var selects = getSingleSelects(select);
		var type;
		for(var i=0; i<selects.length; i++) {
			type = selects[i].value.split(".")[0];
			if(event.name == type) {
				subquery += "$" + selects[i].as + ":" + selects[i].value.split(".")[1];
				if(i != selects.length-1 || start) {
					subquery += ", ";
				}
			}
		}
	}

	// selection of input event cannot be mapped to Drools Query
	// "first" cannot be defined in Drools
	// "last" event is always taken

	if(start) {
		if(interval) {
			subquery += "this after[0s, " + getIntervalTime(interval) + "] " + start;
		} else {
			subquery += "this after " + start;
		}
	}

	subquery += ")";

	return subquery;
}

function getWindowPattern(window, select) {
	var subquery = getSingleEventPattern(window.event, select);

	subquery += " over window:";

	if(window.type.indexOf("time")) {
		subquery += "time";
	} else if(window.type.indexOf("length")) {
		subquery += "length";
	}
	// keepAll (empty/default window) cannot be modeled with Drools

	subquery += "(" + window.value + ")";

	return subquery;
}

function getSingleSelects(select) {
	var result = [];
	var selects = select.split(",");
	var value, as;
	for(var i=0; i<selects.length; i++) {
		value = selects[i].trim().match(new RegExp("[A-Za-z]+\\.[A-Za-z]+(\\s)+(AS|as)"))[0].match(/\S+/g)[0];
		as = selects[i].trim().match(new RegExp("(AS|as)(\\s)+[A-Za-z]+"))[0].match(/\S+/g)[1];
		result.push({
			"value": value,
			"as": as
		});
	}
	return result;
}

function getPatternBinding(type) {
	var a;
	if(!usedAbbreviations[type]) {
		a = "$" + getNextAbbreviation();
		usedAbbreviations[type] = a;
	} else {
		a = usedAbbreviations[type];
	}
	return a
}

function replaceEventTypes(string) {
	var eventTypes = string.match(new RegExp("[A-Za-z]*\\.", "g"));
	if(eventTypes) {
		var type;
		// replace each event type with unique abbreviation
		for(var i=0; i<eventTypes.length; i++) {
			type = eventTypes[i].replace(".", "");
			string = string.replaceAll(type, getAbbrForEventType(type));
		}
	}
	// DROOLS uses == instead of =
	return string.replace(new RegExp("=+"), "==");
}

function getNextAbbreviation() {
	if(!abbr) {
		abbr = 'a';
		return abbr;
	}
	if (/^z+$/.test(abbr)) {
		// all z's -> replace all with a's and add one a
		abbr = abbr.replace(/z/g, 'a') + 'a';
	} else {
		// increment last char
		abbr = abbr.slice(0, -1) + String.fromCharCode(abbr.slice(-1).charCodeAt() + 1);
	}
	return abbr;
};

String.prototype.capitalize = function() {
	return this.replace(/^./, this[0].toUpperCase());
}

String.prototype.replaceAll = function(from, to) {
	return this.replace(new RegExp(from, "g"), to);
};

Array.prototype.pushArray = function(array) {
	this.push.apply(this, array);
};