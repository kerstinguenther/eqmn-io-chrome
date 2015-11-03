'use strict';

var inherits = require('inherits');

var pick = require('lodash/object/pick'),
assign = require('lodash/object/assign');

var CommandInterceptor = require('diagram-js/lib/command/CommandInterceptor');

var Collections = require('diagram-js/lib/util/Collections');


function isEqmn(element) {
	return element && /eqmn\:/.test(element.type);
}

function ifEqmnElement(fn) {
	return function(event) {
		var context = event.context,
		element = context.shape || context.connection;

		if (isEqmn(element) || element.type == "bpmn:TextAnnotation") {
			fn(event);
		}
	};
}

/**
 * A handler responsible for updating the eqmn element's businessObject
 * once changes on the diagram happen.
 */
function EqmnUpdater(eventBus, bpmnjs) {

	CommandInterceptor.call(this, eventBus);

	function updateEqmnShape(shape) {
		var businessObject = shape.businessObject;

		var parent = shape.parent;

		var eqmnElements = window.bpmnjs.getEqmnElements();

		// make sure element is added / removed from bpmnjs.eqmnElements
		if (!parent) {
			Collections.remove(eqmnElements, businessObject);
		} else {
			Collections.add(eqmnElements, businessObject);
		}

		// save eqmn element
		assign(businessObject, pick(shape, [ 'id', 'x', 'y', 'width', 'height', 'outgoing', 'incoming', 'label', 'children' ]));

		console.log(eqmnElements);
	}

	function updateEqmnConnection(connection) {

		var businessObject = connection.businessObject;

		var parent = connection.parent;

		var eqmnElements = window.bpmnjs.getEqmnElements();

		// make sure element is added / removed from bpmnjs.eqmnElements
		if (!parent) {
			Collections.remove(eqmnElements, businessObject);
		} else {
			Collections.add(eqmnElements, businessObject);
		}

		// save eqmn element position
		assign(businessObject, pick(connection, [ 'id', 'waypoints', 'source', 'target' ]));

		console.log(eqmnElements);
	}

	function updateEqmnElement(e) {
		var context = e.context;
		if(context.shape) {
			updateEqmnShape(context.shape);
		} else if(context.connection) {
			updateEqmnConnection(context.connection);
			updateEqmnShape(context.source);
			updateEqmnShape(context.target);
		}
	}

	this.executed([
	               'connection.create',
	               'connection.delete',
	               'shape.create',
	               'shape.move',
	               'shape.delete'
	               ], ifEqmnElement(updateEqmnElement));

	this.reverted([
	               'connection.create',
	               'connection.delete',           
	               'shape.create',
	               'shape.move',
	               'shape.delete'
	               ], ifEqmnElement(updateEqmnElement));

}

inherits(EqmnUpdater, CommandInterceptor);

module.exports = EqmnUpdater;

EqmnUpdater.$inject = [ 'eventBus', 'bpmnjs' ];
