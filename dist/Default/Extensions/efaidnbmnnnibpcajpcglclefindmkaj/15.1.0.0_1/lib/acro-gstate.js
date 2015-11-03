/*************************************************************************
* ADOBE CONFIDENTIAL
* ___________________
*
*  Copyright 2015 Adobe Systems Incorporated
*  All Rights Reserved.
*
* NOTICE:  All information contained herein is, and remains
* the property of Adobe Systems Incorporated and its suppliers,
* if any.  The intellectual and technical concepts contained
* herein are proprietary to Adobe Systems Incorporated and its
* suppliers and are protected by all applicable intellectual property laws,
* including trade secret and or copyright laws.
* Dissemination of this information or reproduction of this material
* is strictly forbidden unless prior written permission is obtained
* from Adobe Systems Incorporated.
**************************************************************************/

/*global chrome, console, require:true, exports:true, define:true, $, def:true */
/*jslint devel: true, browser: true, nomen: true, maxlen: 93 */

function dependOn() {
    'use strict';
    return [
        require("communicate"),
        require("proxy"),
        require("common"),
        require("util")
    ];
}

var def;
require = function (m) {'use strict'; return m; };
if (window.define) {
    def = window.define;
} else {
    def = function (deps, fn) {
        'use strict';
        return fn.apply(null, [{
            ajax: $.ajax.bind($)
        }]);
    };
}
var exports = acom_analytics = {};

def(dependOn(), function (communicate, proxy, common, util) {
    'use strict';
    var singleton = null, DownloadManager, prop, Gstate;

    Gstate = function () {
        this.proxy = proxy.proxy.bind(this);
        this.LOG = common.LOG;

        //GLOBALS -----------------------------------
        //Global variables to keep track of various timers
        this.CONVERSION_TIMEOUT = 200000;

//Global enums to used to pass conversion settings
        this.web2pdfCaller = { MENU : 0, TOOLBAR : 1, AUTO : 2 };
        this.web2pdfAction = { CONVERT : 0, APPEND : 1 };
        this.web2pdfContext = { PAGE : 0, LINK : 1 };


//---------------------------------------------------------------------------------
//Persistent Preferences set by the user.
        var viewResultsPreferenceVariable = "ViewResultsPref";

        this.getViewResultsPreferenceState = function () {
            return util.getCookie(viewResultsPreferenceVariable) !== "false";
        };

        this.viewPrefIsDefault = function () {
            return !util.getCookie(viewResultsPreferenceVariable);
        };
    };

    if (!singleton) {
        singleton = new Gstate();
        communicate.registerModule("acro-gstate", singleton);
    }
    for (prop in singleton) {
        if (singleton.hasOwnProperty(prop)) {
            if (typeof (singleton[prop]) === "function") {
                exports[prop] = singleton[prop].bind(singleton);
            } else {
                exports[prop] = singleton[prop];
            }
        }
    }

    return singleton;
});
