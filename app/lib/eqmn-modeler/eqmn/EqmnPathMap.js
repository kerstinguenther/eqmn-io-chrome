'use strict';

/*
 * adpation of bpmn-js' PathMap
 */

function EqmnPathMap(pathMap) {
	pathMap.pathMap.OPERATOR_CONJUNCTION = {
			d: 'm {mx},{my} l {e.x0}, -{e.y1} l {e.x0}, {e.y1}',
			height: 40,
			width:  40,
			heightElements: [0, 40],
			widthElements: [20, 40]
	};
	pathMap.pathMap.OPERATOR_DISJUNCTION = {
			d: 'm {mx},{my} l {e.x0}, {e.y1} l {e.x0}, -{e.y1}',
			height: 40,
			width:  40,
			heightElements: [0, 40],
			widthElements: [20, 40]
	};
	pathMap.pathMap.OPERATOR_NEGATION = {
			d: 'm {mx},{my} l {e.x1},0 l 0,{e.y1}',
			height: 20,
			width:  40,
			heightElements: [0, 20],
			widthElements: [0, 40]
	};
	pathMap.pathMap.WINDOW_TIME = {
			d:  'm {mx},{my} l 0,-10 m -1,10 l 7,0'
	};
	pathMap.pathMap.WINDOW_LENGTH = {
			d: 'm {mx},{my} l -5,0 l 0,-4 l 0,8 0,-4 l 10,0 l 0,-4 l 0,8'
	};
	pathMap.pathMap.WINDOW_BATCH = {
			d: 'm {mx},{my} l 6,0 m -6,-1 l 0,21 l 6,0 m -6,-10 l 20,0 m -1,0 l -8,-6 m 8,6 l -6,6'
	};
	pathMap.pathMap.WINDOW_SLIDING = {
			d: 'm {mx},{my} m 0,10 l 20,0 m -1,0 l -8,-6 m 8,6 l -8,6 m 8,-6 m 1,-10 l -6,0 m 6,-1 l 0,21 l -6,0'
	};

	this.getScaledPath = function getScaledPath(pathId, param) {
		return pathMap.getScaledPath(pathId, param);
	};

	this.getRawPath = function getRawPath(pathId, mx, my) {
		var rawPath = pathMap.getRawPath(pathId);
		var path = Snap.format(
			rawPath, {
				mx: mx,
				my: my,
				e: {}
			}
		);
		return path;
	};
}

EqmnPathMap.$inject = ['pathMap'];

module.exports = EqmnPathMap;