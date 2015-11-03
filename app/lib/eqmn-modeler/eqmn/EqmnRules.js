'use strict';

var reduce = require('lodash/collection/reduce'),
inherits = require('inherits');

var is = require('bpmn-js/lib/util/ModelUtil').is;

var RuleProvider = require('diagram-js/lib/features/rules/RuleProvider');

var HIGH_PRIORITY = 150000;


function isEqmn(element) {
	return element && /^eqmn\:/.test(element.type);
}

/**
 * Specific rules for eqmn elements
 */
function EqmnRules(eventBus) {
	RuleProvider.call(this, eventBus);
}

inherits(EqmnRules, RuleProvider);

EqmnRules.$inject = [ 'eventBus' ];

module.exports = EqmnRules;


EqmnRules.prototype.init = function() {

	this.addRule('elements.move', HIGH_PRIORITY, function(context) {

		var target = context.target,
		shapes = context.shapes;

		// do not allow mixed movements of eqmn / BPMN shapes
		// if any shape cannot be moved, the group cannot be moved, too
		var allowed = reduce(shapes, function(result, s) {

			if (result === false) {
				return false;
			}

			return canCreate(s, target);
		}, undefined);

		// reject, if we have at least one
		// eqmn element that cannot be moved
		return allowed;
	});

	this.addRule('shape.create', HIGH_PRIORITY, function(context) {
		var target = context.target,
		shape = context.shape;

		return canCreate(shape, target);
	});

	this.addRule('shape.resize', HIGH_PRIORITY, function(context) {
		var shape = context.shape;

		if (shape.type.indexOf('Window') != -1 || shape.type.indexOf('Interval') != -1 || shape.type == "bpmn:TextAnnotation") {
			return true;
		} else {
			return false;
		}
	});

	this.addRule('connection.create', function(context) {
		var source = context.source,
		target = context.target,
		connection = context.connection;

		return canConnect(source, target, connection);
	});

	this.addRule('connection.reconnectStart', function(context) {

		var connection = context.connection,
		source = context.hover || context.source,
		target = connection.target;

		return canConnect(source, target, connection);
	});

	this.addRule('connection.reconnectEnd', function(context) {

		var connection = context.connection,
		source = connection.source,
		target = context.hover || context.target;

		return canConnect(source, target, connection);
	});

	this.addRule('connection.updateWaypoints', function(context) {
		// OK! but visually ignore
		return null;
	});

};

EqmnRules.prototype.canAttach = canAttach;

EqmnRules.prototype.canCreate = canCreate;

EqmnRules.prototype.canConnect = canConnect;

EqmnRules.prototype.canInsert = canInsert;

EqmnRules.prototype.canDrop = canDrop;

EqmnRules.prototype.canReplace = canReplace;

EqmnRules.prototype.canConnectSequence = canConnectSequence;

EqmnRules.prototype.canConnectLooseSequence = canConnectLooseSequence;

EqmnRules.prototype.canConnectAssociation = canConnectAssociation;

/**
 * Can shape be created on target container?
 */
function canCreate(shape, target) {

	// allow annotations everywhere
	if(shape.type == "bpmn:TextAnnotation") {
		return true;
	}
	
	if(target && target.children) {
		
		// allow if shape is already contained in target (= move operation)
		var child;
		for(var i=0; i<target.children.length; i++) {
			child = target.children[i];
			if(shape.id == child.id) {
				return true;
			}
		}
		
		// allow one input event in windows
		if(shape.type.indexOf('InputEvent') != -1 && target.type.indexOf('Window') != -1 && target.children.length == 0) {
			return true;
		}

		// allow multiple input events and operators in intervals
		if(target.type.indexOf('Interval') != -1 && (shape.type.indexOf('InputEvent') != -1 || (shape.type.indexOf('Operator') != -1 && shape.type != "eqmn:Operator"))) {
			return true;
		}
		
	}

	// allow creation on processes
	return is(target, 'bpmn:Process') || is(target, 'bpmn:Participant') || is(target, 'bpmn:Collaboration');
}

function canConnect(source, target, connection) {

	if (isSame(source, target)) {
		return false;
	}

	if(connection && connection.type == "bpmn:Association") {
		if(isConnection(source)) {
			if(source.target.type == "eqmn:OutputEvent") {
				return true;
			} else {
				return false;
			}
		}
		return canConnectAssociation(source, target);
	}
	
	// do not connect connections
	if (isConnection(source) || isConnection(target)) {
		return false;
	}
	
	if(connection && (connection == "eqmn:LooseSequence" || connection.type == "eqmn:LooseSequence")) {
		return canConnectLooseSequence(source, target);
	}
	
	return canConnectSequence(source, target);	
}

function canAttach(elements, target, source, position) {

	// disallow appending as boundary event
	if (source) {
		return false;
	}

	// only (re-)attach one element at a time
	if (elements.length !== 1) {
		return false;
	}

	var element = elements[0];

	// do not attach labels
	if (isLabel(element)) {
		return false;
	}

	// allow default move operation
	if (!target) {
		return true;
	}

	return 'attach';
}

function canInsert(shape, flow, position) {

	canDrop(shape, flow.parent, position);
}

/**
 * Can an element be dropped into the target element
 *
 * @return {Boolean}
 */
function canDrop(element, target) {

	// allow annotations everywhere
	if(element.type == "bpmn:TextAnnotation") {
		return true;
	}
	
	// can move labels everywhere
	if (isLabel(element) && !isConnection(target)) {
		return true;
	}

	return false;
}

function canReplace(elements, target) {

	if (!target) {
		return false;
	}

	var canExecute = {
			replacements: []
	};

	return true;
}

// to allow movement of connections
function isAlreadyConnectedWithAssociation(source, target) {
	for(var i=0; i<source.outgoing.length; i++) {
		if(source.outgoing[i].type == "bpmn:Association") {
			if(source.outgoing[i].target == target) {
				return true;
			}
		}
	}
	return false;
}

function canConnectAssociation(source, target) {

	// can connect only text annotations (bpmn element)
	if(!isEqmn(target) && (!hasIncomingAssociation(target) || isAlreadyConnectedWithAssociation(source, target))) {
		return true;
	}
	
	return false;
}


function hasIncomingAssociation(element) {
	for(var i=0; i<element.incoming.length; i++) {
		if(element.incoming[i].type == "bpmn:Association") {
			return true;
		}
	}
	return false;
}

function hasIncomingSequence(element) {
	for(var i=0; i<element.incoming.length; i++) {
		if(element.incoming[i].type == "eqmn:Sequence") {
			return true;
		}
	}
	return false;
}

function hasIncomingLooseSequence(element) {
	for(var i=0; i<element.incoming.length; i++) {
		if(element.incoming[i].type == "eqmn:LooseSequence") {
			return true;
		}
	}
	return false;
}

/**
 * defines which elements can be connected with a simple sequence flow
 * (defining the flow direction of the query)
 */
function canConnectSequence(source, target) {

	var possibleTargets;
	
	// check number of already incoming sequences
	switch(target.type) {
		case "eqmn:ConjunctionOperator":
		case "eqmn:DisjunctionOperator":
		case "eqmn:ListOperator":
			// conjunction, disjunction and list operator can have multiple incoming sequence flows
			break;
		default:
			// all other elements can have at most one incoming sequence flow
			if(hasIncomingSequence(target)) {
				return false;
			}
			break;
	}
	
	switch(source.type) {
		case "eqmn:InputEvent":
			// input events can be connected to output events and operators
			possibleTargets = ["eqmn:OutputEvent",
			                   "eqmn:ConjunctionOperator",
			                   "eqmn:DisjunctionOperator",
			                   "eqmn:NegationOperator",
			                   "eqmn:ListOperator"];
			if(possibleTargets.indexOf(target.type) > -1) {
				return true;
			}
			break;
		case "eqmn:ListOperator":
			// list operators can be connected to output events
			possibleTargets = ["eqmn:OutputEvent"];
			if(possibleTargets.indexOf(target.type) > -1) {
				return true;
			}
			break;
		case "eqmn:ConjunctionOperator":
		case "eqmn:DisjunctionOperator":
		case "eqmn:NegationOperator":
		case "eqmn:Interval":
			// operators (except list operator) and intervals can be connected to output events and other operators (except list operator)
			possibleTargets = ["eqmn:OutputEvent",
			                   "eqmn:ConjunctionOperator",
			                   "eqmn:DisjunctionOperator",
			                   "eqmn:NegationOperator"];
			if(possibleTargets.indexOf(target.type) > -1) {
				return true;
			}
			break;
		case "eqmn:Window":
		case "eqmn:TimeWindow":
		case "eqmn:LengthWindow":
		case "eqmn:TimeSlidingWindow":
		case "eqmn:LengthSlidingWindow":
		case "eqmn:TimeSlidingBatchWindow":
		case "eqmn:LengthSlidingBatchWindow":
			// each kind of window can be connected to list operators and output events
			possibleTargets = ["eqmn:ListOperator",
			                   "eqmn:OutputEvent"];
			if(possibleTargets.indexOf(target.type) > -1) {
				return true;
			}
			break;
	}

	return false;
}

/**
 * defines which elements can be connected with a loose sequence
 * (defining the temporal sequence of events)
 */
function canConnectLooseSequence(source, target) {
	
	// all elements can be loosely connected to input events, operators (except list operator) and intervals
	var possibleTargets = ["eqmn:InputEvent",
	                       "eqmn:Interval",
	                       "eqmn:ConjunctionOperator",
	                       "eqmn:DisjunctionOperator",
	                       "eqmn:NegationOperator"];
	if(possibleTargets.indexOf(target.type) < 0) {
		return false;
	}
	
	// check number of already incoming sequences
	switch(target.type) {
		case "eqmn:ConjunctionOperator":
		case "eqmn:DisjunctionOperator":
			// conjunction and disjunction operator can have only one incoming loose sequence
			if(hasIncomingLooseSequence(target)) {
				return false;
			}
			break;
		default:
			// all other elements can have at most one incoming loose sequence
			if(hasIncomingLooseSequence(target)) {
				return false;
			}
			break;
	}

	return true;
}

function isLabel(element) {
	return element.labelTarget;
}

function isConnection(element) {
	return element.waypoints;
}

function isSame(a, b) {
	return a === b;
}


