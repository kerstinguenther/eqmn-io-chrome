var angular = require('angular'),
    Dialog = require('simple-dialog');

var $ = angular.element,
    assign = angular.extend;

var ngModule = angular.module('ng-simple-dialog', [ ]);

/**
 * A wrapper around SimpleDialog that adds angular js support.
 *
 * Exposes a constructor function that can be used to create dialogs.
 */
ngModule.factory('ngSimpleDialog', [ '$compile', '$rootScope', function($compile, $rootScope) {

  function getBody(dialog) {
    return $('.dlg-body', dialog.element);
  }

  function NgDialog(options) {

    if (!(this instanceof NgDialog)) {
      return new NgDialog(options);
    }

    Dialog.call(this, options);

    var body = getBody(this);
    var linkBody = $compile(body);

    this.on('pre-open', function() {
      var scope = this.scope = $rootScope.$new();

      assign(scope, options.scope || {}, { dialog: this });

      var newBody = linkBody(scope),
          oldBody = getBody(this);

      oldBody.replaceWith(newBody);
    });

    this.on('close', function() {
      var scope = this.scope;

      delete this.scope;

      scope.$applyAsync();
      scope.$destroy();
    });

    this.on('open', function() {
      var element = this.element;

      setTimeout(function() {
        $(element).find('[autofocus]').eq(0).focus();
      }, 0);
    });
  }

  NgDialog.prototype = Object.create(Dialog.prototype);


  /**
   * A simple dialog constructor.
   *
   * @param {Object} options
   * @param {Object} options.scope arguments to be passed to the dialog
   * @param {String} options.template the dialogs contents
   *
   * @return {SimpleDialog} the created dialog instance
   *
   * @example
   * function MyController($scope, ngSimpleDialog) {
   *
   *   var dialogTpl =
   *         '<h1>Choose {{ option.name }}?</h1>' +
   *         '<p><button ng-click="dialog.close(true)">confirm</button></p>';
   *
   *   this.confirmOption = function(option) {
   *
   *     var dialog = ngSimpleDialog({
   *       scope: {
   *         option: option
   *       },
   *       template: dialogTpl
   *     });
   *
   *     dialog.open().on('close', function(result) {
   *       $scope.confirmed = !!result;
   *     });
   *   };
   * }
   */
  return function(options) {
    return NgDialog(options);
  };
}]);

module.exports = ngModule.name;