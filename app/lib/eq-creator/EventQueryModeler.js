'use strict'

var validateModel = require('./EventQueryValidator').validateModel;
var isSingleInputEvent = require('./EventQueryValidator').isSingleInputEvent;

module.exports.getModel = function() {
	var model = window.bpmnjs.getEqmnElements();
	var error = {};
	if(!validateModel(model, error)) {
		return error;
	}
	// store ids of already processed input events to handle them only once
	var queryModel = {}, element, processed = [], children, child;
	queryModel.warningMessage = error.warningMessage;
	
	for(var i=0; i<model.length; i++) {
		if(isConnection(model[i]) || !isUnprocessed(model[i], processed)) {
			continue;
		} else if(model[i].$type == "bpmn:TextAnnotation" || model[i].type == "eqmn:OutputEvent") {
			element = model[i];
		} else {
			element = getStartElement(model[i]);
		}
		
		if(hasIncomingLooseSequence(element)) {
			var first = getFirstElementOfSequence(element);
			queryModel.input = getSequence(first, processed);
		} else {
			switch(element.type || element.$type) {
				case "bpmn:TextAnnotation":
					if(element.incoming[0].source.type == "eqmn:Sequence"
					   && element.incoming[0].source.target.type == "eqmn:OutputEvent") {
						queryModel.condition = getConditionProperties(element);
	
					}
					break;
				case "eqmn:OutputEvent": 									
					queryModel.output = getOutputProperties(element);
					break;
				case "eqmn:Window":											
					queryModel.input = getWindowProperties(element, processed);
					break;
				case "eqmn:TimeWindow":
					queryModel.input = getWindowProperties(element, processed);
					break;
				case "eqmn:SlidingTimeWindow":
					queryModel.input = getWindowProperties(element, processed);
					break;
				case "eqmn:SlidingBatchTimeWindow":
					queryModel.input = getWindowProperties(element, processed);
					break;
				case "eqmn:LengthWindow":
					queryModel.input = getWindowProperties(element, processed);
					break;
				case "eqmn:SlidingLengthWindow":
					queryModel.input = getWindowProperties(element, processed);
					break;
				case "eqmn:SlidingBatchLengthWindow":
					queryModel.input = getWindowProperties(element, processed);
					break;
				case "eqmn:ConjunctionOperator":
				case "eqmn:DisjunctionOperator":
				case "eqmn:NegationOperator":
					if(isUnprocessed(element, processed)) {
						queryModel.input = getComplexInputProperties(element, processed);
					}
					break;
				case "eqmn:ListOperator":
					if(isUnprocessed(element, processed)) {
						queryModel.input = getInputs(element, processed);
					}
					break;
				case "eqmn:InputEvent":
				case "eqmn:Interval":
					if(isUnprocessed(element, processed)) {
						if(isSingleInputEvent(element)) {
							queryModel.input = (element.type == "eqmn:Interval") ? getIntervalProperties(element, processed) : getInputProperties(element, processed);
						} else {
							// sequence
							var first = getFirstElementOfSequence(element);
							queryModel.input = getSequence(first, processed);
						}
					}
					break;
			}
		}
	}
	return queryModel;
};

function hasIncomingLooseSequence(element) {
	for(var i=0; i<element.incoming.length; i++) {
		if(element.incoming[i].type == "eqmn:LooseSequence") {
			return true;
		}
	}
	return false;
}

function isConnection(element) {
	if(element.type == "bpmn:Association" ||
	   element.type == "eqmn:Sequence" ||
	   element.type == "eqmn:LooseSequence") {
		return true;
	}
	return false;
}

function isAnnotation(element) {
	var type = element.type | element.$type;
	if(type == "bpmn:TextAnnotation") {
		return true;
	}
	return false;
}

function isUnprocessed(element, processed) {
	if (processed.indexOf(element.id) > -1) {
		return false;
	}
	return true;
}


function getRootElement(elements, processed) {
	// only one element? (= no array) --> root element
	if(!elements.length) return elements;
	
	// look for element that has NO outgoing connection and is no text annotation
	var element;
	for(var i=0; i<elements.length; i++) {
		element = elements[i];
		if(element.outgoing.length == 0 && !isConnection(element) && !isAnnotation(element)) {
			return element;
		}
	}
}

function getStartElement(element) {
	var start = element;
	while(!isLastElement(start)) {
		start = getNextElement(start);
	}
	start = getParentWindowIfAvailable(start);
	return (isLastElement(start)) ? start : getStartElement(start);
}

function getParentWindowIfAvailable(element) {
	if(!element.parent) {
		return element;
	}
	var parent = element.parent;
	if(parent.type.indexOf("Window") != -1 || parent.type == "eqmn:Interval") {
		return parent;
	}
	return element;
}

function getNextElement(element) {
	var target;
	for(var i=0; i<element.outgoing.length; i++) {
		target = element.outgoing[i].target;
		if(target.type != "eqmn:OutputEvent" && target.type != "bpmn:TextAnnotation") {
			return target;
		}
	}	
}

function isLastElement(element) {
	if(element.outgoing.length == 0 && element.type != "eqmn:OutputEvent") {
		return true;
	}
	var target;
	for(var i=0; i<element.outgoing.length; i++) {
		if(element.outgoing[i].type == "eqmn:Sequence" && element.outgoing[i].target.type == "eqmn:OutputEvent") {
			return true;
		}
	}
	return false;
}

function getSequence(element, processed) {
	var properties = {};
	properties.sequence = getNextSequence(element, processed);
	return properties;
}

function getNextSequence(element, processed) {
	var properties = {};
	var next = getNextElementInSequence(element);
	properties.start = getIncomingPattern(element, processed);
	if(isLastElementInSequence(next)) {
		properties.end = getIncomingPattern(next, processed);
	} else {
		properties.end = getSequence(next, processed);
	}
	return properties;
}

function getNextElementInSequence(element) {
	for(var i=0; i<element.outgoing.length; i++) {
		if(element.outgoing[i].type == "eqmn:LooseSequence") {
			return element.outgoing[i].target;
		}
	}	
}

function isLastElementInSequence(element) {
	if(element.outgoing.length == 0) {
		return true;
	}
	for(var i=0; i<element.outgoing.length; i++) {
		if(element.outgoing[i].type == "eqmn:LooseSequence") {
			return false;
		}
	}
	return true;
}

function getFirstElementOfSequence(element) {
	var first = element;
	outer:
	while(first.incoming.length > 0) {
		for(var i=0; i<first.incoming.length; i++) {
			if(first.incoming[i].type == "eqmn:LooseSequence") {
				first = first.incoming[i].source;
				continue outer;
			}
		}
		break;
	}
	return first;
}


function getIncomingPattern(rootElement, processed) {
	var properties = {};
	switch(rootElement.type) {
		case "eqmn:Interval":
			if(rootElement.incoming.length > 0 && isUnprocessed(rootElement.incoming[0].source, processed)) {
				// end of sequence
				var first = getFirstElementOfSequence(rootElement);
				properties = getSequence(first, processed);
			} else {
				// single interval
				properties = getIntervalProperties(rootElement, processed);
			}
			break;
		case "eqmn:ConjunctionOperator":
			properties.conjunction = [];
			var element;
			for(var i=0; i<rootElement.incoming.length; i++) {
				// look only for incoming sequence flows (because loose sequences are indicators for a sequence and not a conjunction) 
				if(rootElement.incoming[i].type == "eqmn:Sequence") {
					element = rootElement.incoming[i].source;
					properties.conjunction.push(getIncomingPattern(element, processed));
				}
			}
			processed.push(rootElement.id);
			break;
		case "eqmn:DisjunctionOperator":
			properties.disjunction = [];
			var element;
			for(var i=0; i<rootElement.incoming.length; i++) {
				// look only for incoming sequence flows (because loose sequences are indicators for a sequence and not a disjunction) 
				if(rootElement.incoming[i].type == "eqmn:Sequence") {
					element = rootElement.incoming[i].source;
					properties.disjunction.push(getIncomingPattern(element, processed));
				}
			}
			processed.push(rootElement.id);
			break;
		case "eqmn:NegationOperator":
			for(var i=0; i<rootElement.incoming.length; i++) {
				// look only for incoming sequence flows (because loose sequences are indicators for a sequence and not a negation) 
				if(rootElement.incoming[i].type == "eqmn:Sequence") {
					element = rootElement.incoming[i].source;
					properties.negation = getIncomingPattern(element, processed);
					break; // only one incoming sequence flow for negations
				}
			}
			processed.push(rootElement.id);
			break;
		case "eqmn:InputEvent":
			if(rootElement.incoming.length > 0 && isUnprocessed(rootElement.incoming[0].source, processed)) {
				// end of sequence
				var first = getFirstElementOfSequence(rootElement);
				properties = getSequence(first, processed);
			} else {
				// single input event
				properties = getInputProperties(rootElement, processed);
			}
			
			break;
	}
	return properties;
}

function getComplexInputProperties(elements, processed) {
	var rootElement = getRootElement(elements, processed); 	// get upmost element like root of tree
	// walk down the tree and look for connected elements (depth-first)
	return getIncomingPattern(rootElement, processed);
}

function getInputProperties(element, processed) {
	var properties = {}, connection, text;
	properties.name = element.name || element.businessObject.name;		// type
	if(element.outgoing.length > 0) {								
		for(var i=0; i<element.outgoing.length; i++) {
			connection = element.outgoing[i];
			if(connection.type == "bpmn:Association") {		
				text = connection.target.businessObject.text;
				if(text.toLowerCase() == "first" || text.toLowerCase() == "last") {
					properties.selection = text;
				} else {
					properties.condition = text;
				}
			}				
		}
	}
	processed.push(element.id);
	return properties;
}

function getOutputProperties(element) {
	var properties = {};
	properties.name = element.name;										// type
	if(element.outgoing.length > 0) {									// condition
		properties.select = element.outgoing[0].target.businessObject.text; 	
	}
	return properties;
}

function getConditionProperties(element) {
	return element.text;
}

function getIntervalProperties(element, processed) {
	var properties = {};
	properties.interval = {};
	properties.interval.value = element.name || element.businessObject.name;
	properties.interval.pattern = getComplexInputProperties(element.children, processed);
	processed.push(element.id);
	return properties;
}

function getWindowProperties(element, processed) {
	var properties = {}, child;
	properties.window = {};
	var children = element.children;
	properties.window.event = {};
	for(var j=0; j<children.length; j++) {
		child = children[j];					
		if(child.type == "eqmn:InputEvent") {				// event type
			properties.window.event.name = child.name || child.businessObject.name;	
			processed.push(child.id);
		} else if(child.type == "bpmn:TextAnnotation") {	// event condition
			properties.window.event.condition = child.businessObject.text;
		}
	}	
	var type = getWindowType(element);
	properties.window.type = type;							// type [default, time, time_sliding, ...]
	if(type != 'default') {									
		properties.window.value = element.name || element.businessObject.name;				// value (length/time)
	}
	processed.push(element.id);
	return properties;
}

function getWindowType(element) {
	var type;
	switch(element.type) {
		case "eqmn:Window":											
			type = 'default';
			break;
		case "eqmn:TimeWindow":
			type = 'time';
			break;
		case "eqmn:SlidingTimeWindow":
			type = 'sliding_time';
			break;
		case "eqmn:SlidingBatchTimeWindow":
			type = 'sliding_batch_time';
			break;
		case "eqmn:LengthWindow":
			type = 'length';
			break;
		case "eqmn:SlidingLengthWindow":
			type = 'sliding_length';
			break;
		case "eqmn:SlidingBatchLengthWindow":
			type = 'sliding_batch_length';
			break;
	}
	return type;
}

function getInputs(element, processed) {
	var properties = {};
	properties.from = [];
	var child;
	for(var i=0; i<element.incoming.length; i++) {
		if(element.incoming[i].source.type == "eqmn:InputEvent") {
			child = { "event": getInputProperties(element.incoming[i].source, processed) };
		} else {
			// must be window due to EQMN rules
			child = getWindowProperties(element.incoming[i].source, processed);
		}
		properties.from.push(child);
	}
	processed.push(element.id);
	return properties;
}