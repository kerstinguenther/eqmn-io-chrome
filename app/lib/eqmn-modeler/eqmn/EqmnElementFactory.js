'use strict';

var assign = require('lodash/object/assign'),
inherits = require('inherits');

var BpmnElementFactory = require('bpmn-js/lib/features/modeling/ElementFactory'),
LabelUtil = require('bpmn-js/lib/util/LabelUtil');


/**
 * A eqmn factory that knows how to create BPMN _and_ eqmn elements.
 */
function EqmnElementFactory(bpmnFactory, moddle) {
	BpmnElementFactory.call(this, bpmnFactory, moddle);

	var self = this;

	/**
	 * Create a diagram-js element with the given type (any of shape, connection, label).
	 *
	 * @param  {String} elementType
	 * @param  {Object} attrs
	 *
	 * @return {djs.model.Base}
	 */
	this.create = function(elementType, attrs) {
		var type = attrs.type,
		size;

		if (elementType === 'label') {
			return self.baseCreate(elementType, assign({ type: 'label' }, LabelUtil.DEFAULT_LABEL_SIZE, attrs));
		}

		attrs.label = "";

		if (/^eqmn\:/.test(type)) {
			if (!attrs.businessObject) {
				attrs.businessObject = {
						type: type

				};
			}

			size = self._getEqmnElementSize(type);

			var element = self.baseCreate(elementType, assign(attrs, size));

			return element;
		}

		return self.createBpmnElement(elementType, attrs);
	};
	
}



inherits(EqmnElementFactory, BpmnElementFactory);

module.exports = EqmnElementFactory;

EqmnElementFactory.$inject = [ 'bpmnFactory', 'moddle'];


/**
 * Returns the default size of eqmn shapes.
 *
 * The following example shows an interface on how
 * to setup the eqmn shapes's dimensions.
 *
 * @example
 *
 * var shapes = {
 *   triangle: { width: 40, height: 40 },
 *   rectangle: { width: 100, height: 20 }
 * };
 *
 * return shapes[type];
 *
 *
 * @param {String} type
 *
 * @return {Dimensions} a {width, height} object representing the size of the element
 */
EqmnElementFactory.prototype._getEqmnElementSize = function (type) {
	var shapes = {
			__default: { width: 100, height: 80 },
			'eqmn:triangle': { width: 40, height: 40 },
			'eqmn:circle': { width: 140, height: 140 },
			'eqmn:InputEvent': { width: 50, height: 50 },
			'eqmn:OutputEvent': { width: 50, height: 50 },
			'eqmn:ListOperator': { width: 50, height: 50 },
			'eqmn:ConjunctionOperator': { width: 50, height: 50 },
			'eqmn:DisjunctionOperator': { width: 50, height: 50 },
			'eqmn:NegationOperator': { width: 50, height: 50 },
			'eqmn:LooseSequence': { width: 100, height: 30 },
			'eqmn:StrictSequence': { width: 100, height: 30 },
			'eqmn:Condition': { width: 50, height: 50 },
			'eqmn:Interval': { width: 150, height: 100 },
			'eqmn:Window': { width: 150, height: 100 },
			'eqmn:TimeWindow': { width: 150, height: 100 },
			'eqmn:LengthWindow': { width: 150, height: 100 },
			'eqmn:SlidingTimeWindow': { width: 150, height: 100 },
			'eqmn:SlidingLengthWindow': { width: 150, height: 100 },
			'eqmn:SlidingBatchTimeWindow': { width: 150, height: 100 },
			'eqmn:SlidingBatchLengthWindow': { width: 150, height: 100 },
	};

	return shapes[type] || shapes.__default;
};
