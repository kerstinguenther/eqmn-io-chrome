'use strict'

// TODO (nice to have): validate against XSD Schema (Unicorn event types)
// ... maybe during editing of labels as well

// TODO: validate Event Types in conditions and labels

module.exports.validateModel = function(model, error) {
	var output = 0, input = false, element;
	if(model.length == 0) {
		error.errorMessage = "VALIDATION ERROR: no model";
		return false;
	}
	if(!window.eventTypes) {
		error.warningMessage = "WARNING: no XSD schema was loaded (event types and attributes could not be validated)"
	}
	for(var i=0; i<model.length; i++) {
		element = model[i];
		switch(element.type || element.$type) {
			case "eqmn:OutputEvent":
				if(element.incoming.length == 0) {
					error.errorMessage = "VALIDATION ERROR: output event must be the target of a sequence";
					return false;
				}
				if(window.eventTypes && element.name && !isValidEventType(element.name)) {
					error.errorMessage = "VALIDATION ERROR: " + element.name + " is no valid event type";
					return false;
				}
				output++;
				break;
			case "eqmn:InputEvent":
				var name = element.businessObject ? element.bussinessObject.name : element.name;
				if(!name || name == "") {
					error.errorMessage = "VALIDATION ERROR: input event needs a label";
					return false;
				}
				if(window.eventTypes && !isValidEventType(name)) {
					error.errorMessage = "VALIDATION ERROR: " + name + " is no valid event type";
					return false;
				}
				if(element.parent && element.parent.type.indexOf("Window")==-1 && element.outgoing.length == 0) {
					error.errorMessage = "VALIDATION ERROR: input event outside window must be the source of a sequence";
					return false;
				}
				if(hasCondition(element) && !isSingleInputEvent(element) && hasSelectingCondition(element)) {
					error.errorMessage = "VALIDATION ERROR: only single input events can have a selecting condition (first/last)";
					return false;
				}
				input = true;
				break;
			case "bpmn:TextAnnotation":
				if(!element.text || element.text == "") {
					error.errorMessage = "VALIDATION ERROR: conditions need a text";
					return false;
				}
				var invalidAttributeAndType = {};
				var source = element.incoming[0].source;
				var name = source.businessObject ? source.businessObject.name : source.name;
				if(window.eventTypes && !isValidCondition(element.text, name, invalidAttributeAndType)) {
					if(invalidAttributeAndType.attribute) {
						error.errorMessage = "VALIDATION ERROR: '" + invalidAttributeAndType.attribute + "' if no valid attribute for event type '" + invalidAttributeAndType.type + "'";
					} else {
						error.errorMessage = "VALIDATION ERROR: '" + invalidAttributeAndType.type + "' is no valid event type";
					}
					return false;
					
				}
				break;
			case "eqmn:Window":
			case "eqmn:TimeWindow":
			case "eqmn:LengthWindow":
			case "eqmn:SlidingTimeWindow":
			case "eqmn:SlidingLengthWindow":
			case "eqmn:SlidingBatchTimeWindow":
			case "eqmn:SlidingBatchLengthWindow":
				if(element.children.length == 0) {
					error.errorMessage = "VALIDATION ERROR: window must contain an input event";
					return false;
				}
				if(element.outgoing.length == 0) {
					error.errorMessage = "VALIDATION ERROR: window must be the source of a sequence";
					return false;
				}
				if(element.type != "eqmn:Window") {
					if(!element.name || element.name == "") {
						error.errorMessage = "VALIDATION ERROR: time/length window needs a label specifying the time/length";
						return false;
					}
				}
				if(childrenHasSelectingCondition(element.children)) {
					error.errorMessage = "VALIDATION ERROR: only single input events outside windows can have a selecting condition (first/last)";
					return false;
				}
				break;
			case "eqmn:Interval":
				if(getNumberOfElements(element.children) < 1) {
					error.errorMessage = "VALIDATION ERROR: interval must contain at least one input event";
					return false;
				}
				if(element.outgoing.length == 0) {
					error.errorMessage = "VALIDATION ERROR: interval must be the source of a sequence";
					return false;
				}
				if(!element.name || element.name == "") {
					error.errorMessage = "VALIDATION ERROR: interval needs a label specifying the time";
					return false;
				}
				break;
			case "eqmn:NegationOperator":
				if((element.parent && element.parent.type != "eqmn:Interval") && element.outgoing.length == 0) {
					error.errorMessage = "VALIDATION ERROR: negation operator outside window/intervall must be the source of a sequence";
					return false;
				}
				if(getIncomingSequences(element) != 1) {
					error.errorMessage = "VALIDATION ERROR: negation operator must be the target of only one sequence";
					return false;
				}
				break;
			case "eqmn:DisjunctionOperator":
			case "eqmn:ConjunctionOperator":
				if((element.parent && element.parent.type != "eqmn:Interval") && element.outgoing.length == 0) {
					error.errorMessage = "VALIDATION ERROR: conjunction/disjunction operator outside window/intervall must be the source of a sequence";
					return false;
				}
				if(element.incoming.length < 2) {
					error.errorMessage = "VALIDATION ERROR: conjunction/disjunction operator must be the target of at least two sequences";
					return false;
				}
				break;
			case "eqmn:ListOperator":
				if(element.outgoing.length == 0) {
					error.errorMessage = "VALIDATION ERROR: list operator must be the source of a sequence";
					return false;
				}
				if(element.incoming.length == 0) {
					error.errorMessage = "VALIDATION ERROR: list operator must be the target of at least one sequence";
					return false;
				}
				break;
		}
	}
	if(output==1 && input) {
		return true;
	} else {
		error.errorMessage = "VALIDATION ERROR: model must have exactly one output event and at least one input event";
		return false;
	}
};

module.exports.isSingleInputEvent = isSingleInputEvent
module.exports.isValidEventType = isValidEventType
module.exports.isValidCondition = isValidCondition

function isSingleInputEvent(element) {
	if(element.incoming.length != 0) {
		return false;
	}
	if(element.outgoing.length == 0) {
		return true;
	}
	var element;
	for(var i=0; i<element.outgoing.length; i++) {
		element = element.outgoing[i].target;
		if(element.type != "bpmn:TextAnnotation" && element.type != "eqmn:OutputEvent") {
			return false;
		}
	}
	return true;
}

function isValidEventType(type) {
	if(window.eventTypes[type]) {
		return true;
	}
	return false;
}

function isValidAttribute(type, attribute) {
	// if no type is given, each attribute is valid
	if(!type) {
		return true;
	}
	var attributes = window.eventTypes[type];
	for(var i=0; i<attributes.length; i++) {
		if(attributes[i].split(":")[0] == attribute) {
			return true;
		}
	}
	return false;
}

function isValidCondition(condition, thisType, invalidAttributeAndType) {
	if(condition == "first" || condition == "last") {
		return true;
	}
	
	// TODO (a) check general syntax
	// (a.1) for output events: EventType.attribute as|AS attribute (several times separated by comma)
	// (a.2) for input events: attribute = EventType.attribute | attribute >|<|=|!= number/string (multiple times)
	// (a.3) for WHERE: like input events + functions like sum(attribute)>10
	
	// (b) check attributes of form EventType.attribute
	var attributesWithType = condition.match(new RegExp("[A-Za-z]+\\.[A-Za-z]+", "g"));
	if(attributesWithType) {
		var splitted, type, attribute;
		for(var i=0; i<attributesWithType.length; i++) {
			splitted = attributesWithType[i].split(".");
			type = splitted[0];
			attribute = splitted[1];
			if(!isValidEventType(type)) {
				if(invalidAttributeAndType) {
					invalidAttributeAndType.type = type;
				}
				return false;
			}
			if(!isValidAttribute(type, attribute)) {
				if(invalidAttributeAndType) {
					invalidAttributeAndType.type = type;
					invalidAttributeAndType.attribute = attribute;
				}
				return false;
			}
		}
	}
	
	// (c) check attributes without event type (like 'attribute > 0' )
	// is always followed by a sign like =, >, < or !
	var attributes = condition.match(new RegExp("[A-Za-z]+(\\s)?[=><!]", "g"));
	if(attributes) {
		for(var i=0; i<attributes.length; i++) {
			attribute = attributes[i].match(new RegExp("[A-Za-z]+", "g"))[0];
			if(!isValidAttribute(thisType, attribute)) {
				if(invalidAttributeAndType) {
					invalidAttributeAndType.type = thisType;
					invalidAttributeAndType.attribute = attribute;
				}
				return false;
			}
		}
	}
	
	// (d) check attributes of form "AS|as attribute"
	var attributes = condition.match(new RegExp("(AS|as)\\s[A-Za-z]+", "g"));
	if(attributes) {
		for(var i=0; i<attributes.length; i++) {
			attribute = attributes[i].replace(new RegExp("(AS|as)\\s", "g"), "");
			if(!isValidAttribute(thisType, attribute)) {
				if(invalidAttributeAndType) {
					invalidAttributeAndType.type = thisType;
					invalidAttributeAndType.attribute = attribute;
				}
				return false;
			}
		}
	}
	
	return true;
}

function childrenHasSelectingCondition(elements) {
	var text;
	for(var i=0; i<elements.length; i++) {
		if(elements[i].type == "bpmn:TextAnnotation") {
			text = elements[i].text || elements[i].businessObject.text;
			if(text == "first" || text == "last") {
				return true;
			}
		}
	}
	return false;
}

function hasSelectingCondition(element) {
	var text;
	for(var i=0; i<element.outgoing.length; i++) {
		if(element.outgoing[i].type == "bpmn:Association") {
			text = element.outgoing[i].target.text;
			if(text == "first" || text == "last") {
				return true;
			}
		}
	}
	return false;
}

function getNumberOfElements(elements) {
	var n = 0;
	for(var i=0; i<elements.length; i++) {
		if(elements[i].type == "eqmn:InputEvent") {
			n++;
		}
	}
	return n;
}

function hasCondition(element) {
	for(var i=0; i<element.outgoing.length; i++) {
		if(element.outgoing[i].type == "bpmn:Association") {
			return true;
		}
	}
	return false;
}

function getIncomingSequences(element) {
	var count = 0;
	for(var i=0; i<element.incoming.length; i++) {
		if(element.incoming[i].type == "eqmn:Sequence") {
			count++;
		}
	}
	return count;
}