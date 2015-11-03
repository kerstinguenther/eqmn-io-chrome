module.exports = {
  __init__: [ 'eqmnRenderer', 'paletteProvider', 'eqmnRules', 'eqmnUpdater', 'eqmnPathMap', 'eqmnContextPadProvider', 'eqmnReplace', /*'eqmnModeling'*/ ],
  elementFactory: [ 'type', require('./EqmnElementFactory') ],
  eqmnRenderer: [ 'type', require('./EqmnRenderer') ],
  paletteProvider: [ 'type', require('./EqmnPalette') ],
  eqmnRules: [ 'type', require('./EqmnRules') ],
  eqmnUpdater: [ 'type', require('./EqmnUpdater') ],
  eqmnPathMap: [ 'type', require('./EqmnPathMap') ],
  eqmnContextPadProvider: [ 'type', require('./EqmnContextPadProvider') ],
  eqmnReplace: ['type', require('./EqmnReplace') ],
  //eqmnModeling: [ 'type', require('./EqmnModeling') ]
};
