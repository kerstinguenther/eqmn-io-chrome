var Modeler = require('bpmn-js/lib/Modeler');

var assign = require('lodash/object/assign');

var inherits = require('inherits');

var domify = require('min-dom/lib/domify');

function EqmnModeler(options) {
  Modeler.call(this, options);
  
  this._eqmnElements = [];
}

inherits(EqmnModeler, Modeler);

EqmnModeler.prototype._modules = [].concat(
  EqmnModeler.prototype._modules,
  [
    require('./eqmn')
  ]
);

/**
 * Add a single eqmn element to the underlying diagram
 *
 * @param {Object} eqmnElement
 */
EqmnModeler.prototype.addEqmnElement = function(eqmnElement) {

  var canvas = this.get('canvas');
  var elementFactory = this.get('elementFactory');

  var eqmnShapeAttrs = assign({ businessObject: eqmnElement }, eqmnElement);

  if(eqmnElement.waypoints) {
	  var eqmnConnection = elementFactory.create('connection', eqmnShapeAttrs);
	  canvas.addConnection(eqmnConnection);
  } else {
	 var eqmnShape = elementFactory.create('shape', eqmnShapeAttrs);
	 canvas.addShape(eqmnShape);
  }

//  EqmnRenderer.drawShape(eqmnShape);
};

/**
 * Add a number of eqmn elements to the underlying diagram.
 *
 * @param {Array<Object>} eqmnElements
 */
EqmnModeler.prototype.setEqmnElements = function(eqmnElements) {

//  if (!this.diagram) {
//    throw new Error('load a diagram first');
//  }

  this._eqmnElements = eqmnElements;

  eqmnElements.forEach(this.addEqmnElement.bind(this));
};

/**
 * Get eqmn elements with their current status.
 *
 * @return {Array<Object>} eqmn elements on the diagram
 */
EqmnModeler.prototype.getEqmnElements = function() {
  return this._eqmnElements;
};

module.exports = EqmnModeler;
