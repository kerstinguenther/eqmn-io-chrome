'use strict';

/**
 * A replace menu provider that gives users the controls to choose
 * and replace EQMN elements with each other.
 *
 * @param {BpmnFactory} bpmnFactory
 * @param {Moddle} moddle
 * @param {PopupMenu} popupMenu
 * @param {Replace} replace
 */
function EqmnReplace(bpmnFactory, moddle, popupMenu, replace, selection, modeling, eventBus) {

	/**
	 * Prepares a new business object for the replacement element
	 * and triggers the replace operation.
	 *
	 * @param  {djs.model.Base} element
	 * @param  {Object} target
	 * @param  {Object} [hints]
	 * @return {djs.model.Base} the newly created element
	 */
	function replaceElement(element, type) {

		var oldBusinessObject = element.businessObject,
		businessObject = bpmnFactory.create(type);

		var newElement = {
				type: type,
				businessObject: businessObject
		};

		newElement = replace.replaceElement(element, newElement);

		return newElement;
	}

	this.replaceElement = replaceElement;
}

EqmnReplace.$inject = [ 'bpmnFactory', 'moddle', 'popupMenu', 'replace', 'selection', 'modeling', 'eventBus' ];

module.exports = EqmnReplace;
