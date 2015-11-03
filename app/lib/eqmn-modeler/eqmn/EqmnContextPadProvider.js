var assign = require('lodash/object/assign');


function EqmnContextPadProvider(contextPad, modeling, elementFactory,
		connect, create, bpmnReplace,
		canvas) {

	contextPad.registerProvider(this);

	function removeElement(event, element) {
		if (element.waypoints) {
			modeling.removeConnection(element);
		} else {
			modeling.removeShape(element);
		}
	}

	function startConnect(event, element, autoActivate) {
		connect.start(event, element, autoActivate);
	}

	function startLooseConnect(event, element, autoActivate) {
		connect.start(event, element, autoActivate, 'connect_loose');
	}

	function appendAction(type, className, options) {

		function appendListener(event, element) {

			var shape = elementFactory.createShape(assign({ type: type }, options));
			create.start(event, shape, element);
		}

		var shortType = type.replace(/^eqmn\:/, '');

		return {
			group: 'model',
			className: className,
			title: 'Append ' + shortType,
			action: {
				dragstart: appendListener,
				click: appendListener
			}
		};
	}

	function getReplaceMenuPosition(element) {

		var Y_OFFSET = 5;

		var diagramContainer = canvas.getContainer(),
		pad = contextPad.getPad(element).html;

		var diagramRect = diagramContainer.getBoundingClientRect(),
		padRect = pad.getBoundingClientRect();

		var top = padRect.top - diagramRect.top;
		var left = padRect.left - diagramRect.left;

		var pos = {
				x: left,
				y: top + padRect.height + Y_OFFSET
		};

		return pos;
	}

	function hasCondition(element) {
		for(var i=0; i<element.outgoing.length; i++) {
			if(element.outgoing[i].type == "bpmn:Association"){
				return true;
			}
		}
		return false;
	}

	function hasOutgoingSequence(element) {
		for(var i=0; i<element.outgoing.length; i++) {
			if(element.outgoing[i].type.indexOf("Sequence") != -1){
				return true;
			}
		}
		return false;
	}

	this.getContextPadEntries = function(element) {

		var entries = {
				// all elements have an option for deleting
				'delete': {
					group: 'edit',
					className: 'icon-trash',
					title: 'Remove',
					action: {
						click: removeElement,
						dragstart: removeElement
					}
				}
		};

		switch(element.type) {
			// input events have the options for sequence flow and loose sequence (if not containting an outgoing sequence yet)
			// and the option for a condition (if not already having one)
			case "eqmn:InputEvent":
				if(!hasOutgoingSequence(element)) {
					entries['connect'] = {
							group: 'connect',
							className: 'icon-sequence-strict',
							title: 'Connect using Sequence',
							action: {
								click: startConnect,
								dragstart: startConnect
							}
					};
	
					entries['connect_loose'] = {
							group: 'connect',
							className: 'icon-sequence-loose',
							title: 'Connect using LooseSequence',
							action: {
								click: startLooseConnect,
								dragstart: startLooseConnect
							}
					};
				}
				if(!hasCondition(element)) {
					entries['append.text-annotation'] = appendAction('bpmn:TextAnnotation', 'icon-text-annotation');
				}
				break;
			case "eqmn:OutputEvent":
				// output events have the option for a condition (if not already having one)
				if(!hasCondition(element)) {
					entries['append.text-annotation'] = appendAction('bpmn:TextAnnotation', 'icon-text-annotation');
				}
				break;
			case "eqmn:Window":
			case "eqmn:TimeWindow":
			case "eqmn:LengthWindow":
			case "eqmn:TimeSlidingWindow":
			case "eqmn:LengthSlidingWindow":
			case "eqmn:TimeSlidingBatchWindow":
			case "eqmn:LengthSlidingBatchWindow":
			case "eqmn:ListOperator":
				// all kinds of window and the list operator have the options for sequence flow (if not containting an outgoing sequence yet)
				if(!hasOutgoingSequence(element)) {
					entries['connect'] = {
							group: 'connect',
							className: 'icon-sequence-strict',
							title: 'Connect using Sequence',
							action: {
								click: startConnect,
								dragstart: startConnect
							}
					};
				}
				break;
			case "eqmn:ConjunctionOperator":
			case "eqmn:DisjunctionOperator":
			case "eqmn:NegationOperator":
				// operators (except list operators) have the options for sequence flow and loose sequence (if not containting an outgoing sequence yet)
				if(!hasOutgoingSequence(element)) {
					entries['connect'] = {
							group: 'connect',
							className: 'icon-sequence-strict',
							title: 'Connect using Sequence',
							action: {
								click: startConnect,
								dragstart: startConnect
							}
					};
					entries['connect_loose'] = {
							group: 'connect',
							className: 'icon-sequence-loose',
							title: 'Connect using LooseSequence',
							action: {
								click: startLooseConnect,
								dragstart: startLooseConnect
							}
					};
				}
				break;
			case "eqmn:Interval":
				// intervals have the options for sequence flow and loose sequence (if not containing an outgoing sequence yet)
				if(!hasOutgoingSequence(element)) {
					entries['connect'] = {
							group: 'connect',
							className: 'icon-sequence-strict',
							title: 'Connect using Sequence',
							action: {
								click: startConnect,
								dragstart: startConnect
							}
					};
	
					entries['connect_loose'] = {
							group: 'connect',
							className: 'icon-sequence-loose',
							title: 'Connect using LooseSequence',
							action: {
								click: startLooseConnect,
								dragstart: startLooseConnect
							}
					};
				}
				break;
			case "eqmn:Sequence":
				// flow sequences between an element and the output event have the option for a condition (if not already having one)
				if(element.target.type == "eqmn:OutputEvent" && !hasCondition(element)) {
					entries['append.text-annotation'] = appendAction('bpmn:TextAnnotation', 'icon-text-annotation');
				}
				break;
		}
		return entries;
	};
}

EqmnContextPadProvider.$inject = [
                                  'contextPad',
                                  'modeling',
                                  'elementFactory',
                                  'connect',
                                  'create',
                                  'bpmnReplace',
                                  'canvas'
                                  ];

module.exports = EqmnContextPadProvider;
