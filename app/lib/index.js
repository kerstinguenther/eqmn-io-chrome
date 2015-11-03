// expose jquery to window
window.jQuery = require('jquery');

var domify = require('min-dom/lib/domify');

var angular = require('angular');

var ngModule = module.exports = angular.module('app', [
  require('./dialog').name,
  require('./editor').name
]);

// custom elements JSON; load it from somewhere else if you like
var customElements = require('./eqmn-elements.json');

// our custom modeler
var EqmnModeler = require('./eqmn-modeler');

var overrideModule = {
    contextPadProvider: [ 'type', require('./eqmn-modeler/eqmn/EqmnContextPadProvider') ],
    bpmnRules: [ 'type', require('./eqmn-modeler/eqmn/EqmnRules') ],
    bpmnReplace: [ 'type', require('./eqmn-modeler/eqmn/EqmnReplace') ],
    //bpmnFactory: [ 'type', require('./eqmn-modeler/eqmn/EqmnElementFactory') ],
    //modeling: [ 'type', require('./custom-modeler/custom/CustomModeling') ]
};

var modeler = new EqmnModeler({ 
    container: domify('<div>'), 
    keyboard: { bindTo: document },
    overrideModule
});

// expose bpmnjs to window for debugging purposes
window.bpmnjs = modeler;
