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

/*global chrome, console, require:true, exports:true, define:true, $, def:true, SETTINGS, FileReader */
/*jslint devel: true, browser: true, nomen: true, maxlen: 80 */

function dependOn() {
    'use strict';
    return [
        require("communicate"),
        require("common"),
        require("util"),
        require("proxy"),
        require("analytics"),
        require("acro-web2pdf")
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
def(dependOn(), function (
    communicate,
    common,
    util,
    proxy,
    analytics,
    acroweb2pdf
) {
    'use strict';
    var singleton = null,
        prop,
        AcroActions;

    AcroActions = function () {
        this.proxy = proxy.proxy.bind(this);
        this.LOG = common.LOG;

        this.getVersion = function (response) {
            // if we don't get a response in three seconds then assume
            // we're dealing with an older shim.
            var timer,
                callback = function (msg) {
                    /* Expecting:
                    {
                        “messageType” : “shimVersionInfo”,
                        “majorVersion” : "15",
                        “minorVersion” : "0"
                    }
                    */
                    if (!msg) {

                        // probably: Native host has exited.
                        return;
                    }
                    clearTimeout(timer);
                    //log shim version analytics
                    if (+msg.majorVersion === 1) {
                        analytics.event(
                            analytics.e.SHIM_VERSION,
                            {VERSION: "Unknown"}
                        );
                    } else if (+msg.majorVersion === 0) {
                        analytics.event(
                            analytics.e.SHIM_VERSION,
                            {VERSION: "None"}
                        );
                    } else {
                        analytics.event(
                            analytics.e.SHIM_VERSION,
                            {VERSION: msg.majorVersion + "." + msg.minorVersion}
                        );

                    }
                    // +convert to integer.
                    communicate.setVersion(+msg.majorVersion);
                    response(+msg.majorVersion);
                };
            timer = setTimeout(
                function () {
                    // 1 === old version (shim doesn't function)
                    callback({
                        "messageType" : "shimVersionInfo",
                        "majorVersion" : "1",
                        "minorVersion" : "0"
                    });
                },
                // if we don't get a response within 2 seconds, we
                // assum it's an old shim.
                2000
            );

            acroweb2pdf.getVersion(callback);
        };

        this.openInAcrobat = function (request) {
            if (communicate.version > 1) {
                request.panel_op = "status";
                request.current_status = "pdf_downloading";
                communicate.sendMessage(request);
                var xhr = util.newXHR();
                xhr.open('GET',  request.url, true);
                xhr.responseType = 'blob';
                xhr.onload = this.proxy(function (e) {
                    var fr;

                    if (xhr.status < 400) {
                        fr = new FileReader();
                        fr.onloadend = function (event) {
                            // populate mime type
                            request.base64PDF = event.target.result;
                            acroweb2pdf.openInAcrobat(request);

                            request.content_op = "status";
                            request.current_status = "pdf_downloaded";
                            communicate.sendMessage(request);
                        };
                        fr.readAsDataURL(xhr.response);
                    } else {
                        request.panel_op = "status";
                        request.current_status = "pdf_failure";
                        communicate.sendMessage(request);
                    }
                });
                xhr.send();
            } else {
                // older shim.  Just download.  In most cases
                // chrome will automatically launch -- but we don't
                // know for sure what the default viewer is
                chrome.downloads.download(
                    {
                        url             : request.url,
                        conflictAction  : "uniquify"
                    }
                );
            }
        };
        this.open_converted_file = function (request) {
            acroweb2pdf.openFile(request);
        };
    };

    if (!singleton) {
        singleton = new AcroActions();
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
    communicate.registerHandlers(
        {
            "open_in_acrobat"       : singleton.proxy(singleton.openInAcrobat),
            "open_converted_file"   :
                singleton.proxy(singleton.open_converted_file)
        }
    );
    return singleton;
});
